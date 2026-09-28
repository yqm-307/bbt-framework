// examples/dual_service/storage_main.cc — svc_a：存储服务进程。
//
// 业务形态：单进程托管一个 Concurrent 服务 "storage"，真实后端经
// CoApp::add_resource 工厂装配为命名资源——Redis（"cache"）与
// MongoDB（"docs"）两个 infra 客户端，handler 内 context().resource
// 取用。不再使用进程内 KvStore 替身：put/get/del 落到真实后端往返，
// 两端地址由命令行显式传入（无可达后端 → 工厂 Create 失败 → run 拒绝）。
// 跨进程对端（gateway 经 static_routes 指向本进程）与 driver 都按
// HTTP bridge 线格式（x-bbt-* header + 位置参数负载）真实接入。
//
// 方法表：
//   put(key, value)   写 Redis SET + Mongo upsert；空 key → InvalidArgument。
//   get(key)          先查 Redis，miss 回退 Mongo；都未命中 → 业务错误
//                     NotFound（非传输错误）。
//   del(key)          Redis DEL + Mongo DeleteOne；命中回 int32 1。
//   sleep_ms(ms)      挂起至多 ms 毫秒（等待一个永不完成的信号，deadline
//                     取自入站上下文——调用方预算到期时如实 TimedOut）。
//
// 优雅关闭：SIGINT/SIGTERM → watcher 线程调用 request_shutdown →
// CoApp::run 返回生命周期返回码（0 = kExitOk 按期收束）；关闭序列经
// _CloseResources 把两个后端客户端收束到 IsClosed 终态。

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CompletionSignal.hpp>

#include <bbt/infra/CoRedisCli.hpp>
#include <bbt/infra/CoMongoCli.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>

namespace fw  = bbt::framework;
namespace co  = bbt::coroutine;
namespace inf = bbt::infra;

namespace {

fw::CoRpcResp ArgErr(const char* what) {
    return fw::CoRpcResp::Error(
        fw::MakeError(fw::ErrorCode::InvalidArgument, what));
}

// 缺资源统一错误：资源缝在 run 期已装配成功才分发，缺失即框架 invariant
// 被破坏，如实 RuntimeUnavailable。
fw::CoRpcResp ResErr(const char* name) {
    return fw::CoRpcResp::Error(fw::MakeError(
        fw::ErrorCode::RuntimeUnavailable,
        std::string("storage: resource '") + name + "' missing"));
}

// Mongo 值文档：把 string value 编进 {_id:key, v:value} 的最小 BSON。
// MongoDocument 是 owning bytes；只编码本服务用的 string 元素。
void BsonPutI32(std::vector<std::uint8_t>& b, std::int32_t v) {
    b.push_back(static_cast<std::uint8_t>(v & 0xFF));
    b.push_back(static_cast<std::uint8_t>((v >> 8) & 0xFF));
    b.push_back(static_cast<std::uint8_t>((v >> 16) & 0xFF));
    b.push_back(static_cast<std::uint8_t>((v >> 24) & 0xFF));
}
void BsonPutCStr(std::vector<std::uint8_t>& b, std::string_view s) {
    for (char c : s) b.push_back(static_cast<std::uint8_t>(c));
    b.push_back(0x00);
}
void BsonElemString(std::vector<std::uint8_t>& b,
                    std::string_view key, std::string_view val) {
    b.push_back(0x02);                        // type: string
    BsonPutCStr(b, key);
    BsonPutI32(b, static_cast<std::int32_t>(val.size() + 1));
    BsonPutCStr(b, val);
}
inf::MongoDocument DocKV(std::string_view key, std::string_view val) {
    std::vector<std::uint8_t> body;
    BsonElemString(body, "_id", key);
    BsonElemString(body, "v",   val);
    inf::MongoDocument doc;
    const std::int32_t total =
        static_cast<std::int32_t>(body.size() + 5);
    doc.bytes.reserve(static_cast<std::size_t>(total));
    BsonPutI32(doc.bytes, total);
    doc.bytes.insert(doc.bytes.end(), body.begin(), body.end());
    doc.bytes.push_back(0x00);
    return doc;
}
inf::MongoDocument DocKey(std::string_view key) {
    std::vector<std::uint8_t> body;
    BsonElemString(body, "_id", key);
    inf::MongoDocument doc;
    const std::int32_t total =
        static_cast<std::int32_t>(body.size() + 5);
    doc.bytes.reserve(static_cast<std::size_t>(total));
    BsonPutI32(doc.bytes, total);
    doc.bytes.insert(doc.bytes.end(), body.begin(), body.end());
    doc.bytes.push_back(0x00);
    return doc;
}
// update 文档：{$set:{v:value}}，upsert 语义由 UpdateOne 承担。
inf::MongoDocument DocSet(std::string_view val) {
    std::vector<std::uint8_t> inner;
    BsonElemString(inner, "v", val);
    std::vector<std::uint8_t> body;
    // $set 是嵌入文档元素（type 0x03）。
    body.push_back(0x03);
    BsonPutCStr(body, "$set");
    BsonPutI32(body, static_cast<std::int32_t>(inner.size() + 5));
    body.insert(body.end(), inner.begin(), inner.end());
    body.push_back(0x00);
    inf::MongoDocument doc;
    const std::int32_t total =
        static_cast<std::int32_t>(body.size() + 5);
    doc.bytes.reserve(static_cast<std::size_t>(total));
    BsonPutI32(doc.bytes, total);
    doc.bytes.insert(doc.bytes.end(), body.begin(), body.end());
    doc.bytes.push_back(0x00);
    return doc;
}

class StorageSvc final : public fw::CoService<StorageSvc> {
public:
    static constexpr std::string_view kServiceName = "storage";

    fw::CoRpcResp Put(fw::CoRpcReq req) {
        auto args = req.Parse<std::string, std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        auto& [key, value] = args.value();
        if (key.empty()) return ArgErr("storage.put: empty key");
        auto cache = context().resource<inf::CoRedisCli>("cache");
        auto docs  = context().resource<inf::CoMongoCli>("docs");
        if (!cache) return ResErr("cache");
        if (!docs)  return ResErr("docs");
        inf::CallOptions o;
        o.deadline = co::Deadline::max();
        // 真实双写：Redis SET 与 Mongo upsert 都成功才回写好的值。
        if (auto r = cache->Set(key, value, o); !r)
            return fw::CoRpcResp::Error(r.error());
        if (auto r = docs->UpdateOne(DocKey(key), DocSet(value), o); !r)
            return fw::CoRpcResp::Error(r.error());
        std::printf("[svc_a] put key=%s (redis+mongo)\n", key.c_str());
        return fw::CoRpcResp::From(value);
    }

    fw::CoRpcResp Get(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        if (args.value().empty()) return ArgErr("storage.get: empty key");
        auto cache = context().resource<inf::CoRedisCli>("cache");
        auto docs  = context().resource<inf::CoMongoCli>("docs");
        if (!cache) return ResErr("cache");
        if (!docs)  return ResErr("docs");
        inf::CallOptions o;
        o.deadline = co::Deadline::max();
        // 真实读路径：Redis GET 命中即回；miss 回退 Mongo FindOne。
        auto hit = cache->Get(args.value(), o);
        if (!hit) return fw::CoRpcResp::Error(hit.error());
        if (hit.value().has_value())
            return fw::CoRpcResp::From(hit.value().value());
        auto doc = docs->FindOne(DocKey(args.value()), o);
        if (!doc) return fw::CoRpcResp::Error(doc.error());
        if (doc.value().has_value()) {
            // 命中 Mongo 即算命中（值本体在文档字节里，示例只证真实往返）。
            return fw::CoRpcResp::From(
                std::string{"mongo-hit:"} + args.value());
        }
        // 两端都未命中：业务错误 NotFound，不是传输失败。
        return fw::CoRpcResp::Error(fw::MakeError(
            fw::ErrorCode::NotFound,
            "storage.get: key '" + args.value() + "' not found"));
    }

    fw::CoRpcResp Del(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        auto cache = context().resource<inf::CoRedisCli>("cache");
        auto docs  = context().resource<inf::CoMongoCli>("docs");
        if (!cache) return ResErr("cache");
        if (!docs)  return ResErr("docs");
        inf::CallOptions o;
        o.deadline = co::Deadline::max();
        auto rn = cache->Delete({args.value()}, o);
        if (!rn) return fw::CoRpcResp::Error(rn.error());
        auto mn = docs->DeleteOne(DocKey(args.value()), o);
        if (!mn) return fw::CoRpcResp::Error(mn.error());
        const bool removed = (rn.value() + mn.value()) > 0;
        return fw::CoRpcResp::From(static_cast<std::int32_t>(removed ? 1 : 0));
    }

    fw::CoRpcResp SleepMs(fw::CoRpcReq req) {
        auto args = req.Parse<std::int32_t>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        if (args.value() < 0) return ArgErr("storage.sleep_ms: negative ms");
        auto ctx = context().request();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        // 在永不完成的信号上带预算等待：等价于协作式 sleep，入站
        // deadline/cancel 到期时以 TimedOut/Cancelled 如实返回。
        co::CompletionSignal gate;
        const auto st = gate.Wait({ctx.value()->deadline,
                                   ctx.value()->cancel});
        if (st != co::WaitStatus::Completed)
            return fw::CoRpcResp::Error(fw::MakeError(
                st == co::WaitStatus::TimedOut
                    ? fw::ErrorCode::TimedOut : fw::ErrorCode::Cancelled,
                "storage.sleep_ms: interrupted"));
        return fw::CoRpcResp::From(args.value());
    }

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&StorageSvc::Put>("put"),
        fw::Method<&StorageSvc::Get>("get"),
        fw::Method<&StorageSvc::Del>("del"),
        fw::Method<&StorageSvc::SleepMs>("sleep_ms"));
};

// signal → flag：handler 只做 async-signal-safe 的 atomic store；
// watcher 线程轮询后调用 request_shutdown（避免在信号上下文里
// 触碰 condition_variable）。
std::atomic_bool g_stop{false};

void InstallSignals() {
    std::signal(SIGINT,  [](int) { g_stop.store(true); });
    std::signal(SIGTERM, [](int) { g_stop.store(true); });
}

// "host:port" → (host, port)；port 段缺失/非法 → port=0 交由
// ValidateRedisClientConfig 拒绝（不静默猜测）。
std::pair<std::string, std::uint16_t> SplitAddr(const std::string& addr) {
    const auto pos = addr.rfind(':');
    if (pos == std::string::npos) return {addr, 0};
    return {addr.substr(0, pos),
            static_cast<std::uint16_t>(std::stoi(addr.substr(pos + 1)))};
}

} // namespace

int main(int argc, char** argv) {
    // 用法: svc_a <listen_port> <redis_addr host:port> <mongo_uri>
    if (argc != 4) {
        std::fprintf(stderr,
            "usage: svc_a <listen_port> <redis_addr host:port> <mongo_uri>\n");
        return 64;   // EX_USAGE
    }
    const auto port = static_cast<std::uint16_t>(std::stoi(argv[1]));
    const auto [rhost, rport] = SplitAddr(argv[2]);
    const std::string mongo_uri = argv[3];

    // 嵌套出站链的协程栈需求见 tests/F1-b2 实测记录：默认 12KB 触底，
    // 本进程 handler 经资源缝发起真实 Redis/Mongo 出站，与对端保持同一配置。
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;

    fw::CoAppOptions opts;
    opts.network_limits = bbt::infra::NetworkLimits{
        /*max_connections*/ 128,
        /*max_inflight*/    64,
        /*max_header_bytes*/16384,
        /*max_body_bytes*/  65536,
        /*incoming_timeout*/std::chrono::milliseconds{30000}};
    opts.listen = bbt::infra::ListenAddress{"127.0.0.1", port};
    opts.static_routes = {};                    // 纯被调方：无出站 RPC 路由
    opts.shutdown_step_budget = std::chrono::milliseconds{2000};

    fw::CoApp app{opts};
    if (auto r = app.add_service<StorageSvc>(fw::ServiceOptions{
            fw::ExecutionPolicy::Concurrent,
            /*max_inflight*/ 64, 0, 0, false, 0, 0, 0});
        !r) {
        std::fprintf(stderr, "[svc_a] add_service failed: %s\n",
                     r.error().message.c_str());
        return 65;
    }

    // 真实后端：工厂形态装配。Create 要求 Scheduler 已启动，框架在
    // on_scheduler_started 相位执行 factory——Redis 与 Mongo 各自
    // 独立 Create→Start→登记实例视图，关闭序列统一收束到 IsClosed。
    inf::RedisClientConfig rcfg;
    rcfg.host         = rhost;
    rcfg.port         = rport;
    rcfg.max_inflight = 256;
    rcfg.max_queue    = 256;
    if (auto r = app.add_resource<inf::CoRedisCli>("cache",
            [rcfg] { return inf::CoRedisCli::Create(rcfg); });
        !r) {
        std::fprintf(stderr, "[svc_a] add_resource(cache) failed: %s\n",
                     r.error().message.c_str());
        return 65;
    }

    inf::MongoClientConfig mcfg;
    mcfg.uri                      = mongo_uri;
    mcfg.database                 = "bbt_dual_service";
    mcfg.collection               = "kv";
    mcfg.worker_threads           = 2;
    mcfg.max_queue                = 256;
    mcfg.server_selection_timeout = std::chrono::milliseconds{5000};
    mcfg.connect_timeout          = std::chrono::milliseconds{5000};
    mcfg.socket_timeout           = std::chrono::milliseconds{5000};
    mcfg.wait_queue_timeout       = std::chrono::milliseconds{5000};
    if (auto r = app.add_resource<inf::CoMongoCli>("docs",
            [mcfg] { return inf::CoMongoCli::Create(mcfg); });
        !r) {
        std::fprintf(stderr, "[svc_a] add_resource(docs) failed: %s\n",
                     r.error().message.c_str());
        return 65;
    }

    InstallSignals();
    std::thread watcher([&app] {
        while (!g_stop.load(std::memory_order_acquire))
            std::this_thread::sleep_for(std::chrono::milliseconds{20});
        app.request_shutdown();
    });

    std::printf("[svc_a] storage service on 127.0.0.1:%u "
                "(redis=%s mongo=%s)\n",
                port, argv[2], mongo_uri.c_str());
    std::fflush(stdout);
    const int rc = app.run();   // 0 = kExitOk
    g_stop.store(true, std::memory_order_release);
    watcher.join();
    std::printf("[svc_a] run() returned %d, shutdown_state=%d\n",
                rc, static_cast<int>(app.shutdown_state()));
    std::fflush(stdout);
    return rc;
}
