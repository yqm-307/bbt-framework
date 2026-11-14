// examples/dual_service/storage_main.cc — svc_a：存储服务进程。
//
// 业务形态：单进程托管一个 Concurrent 服务 "storage"，真实后端经 v2 组件
// 装配为命名能力——Redis（"cache"，v2::RedisComponent：借用共享
// net::Engine 的 CoRedisPool）与 MongoDB（"docs"，v2::MongoDBComponent：
// 自有 worker 的 CoMongoDb + CoMongoColl）。handler 内 GetComponent + Borrow
// 取用。不再使用进程内 KvStore 替身：put/get/del 落到真实后端往返，两端
// 地址由命令行显式传入（无可达后端 → 组件 OnStart 失败 → CoStart 回退）。
// Redis 侧只接受数值 IPv4/IPv6（net::Address::Ip 不解析主机名，非法地址
// 在装配期如实失败）。跨进程对端（gateway 经静态路由指向本进程）与
// driver 都按 V2 会话（SessionFrame + u32be + Hello/HelloAck）真实接入。
//
// 方法表：
//   put(key, value)   写 Redis SET + Mongo upsert；空 key → InvalidArgument。
//   get(key)          先查 Redis，miss 回退 Mongo；都未命中 → 业务错误
//                     NotFound（非传输错误）。
//   del(key)          Redis DEL + Mongo DeleteOne；命中回 int32 1。
//   sleep_ms(ms)      挂起至多 ms 毫秒（等待一个永不完成的信号，deadline
//                     取自入站上下文——调用方预算到期时如实 TimedOut）。
//
// 资源预算：put/get/del 的 Redis/Mongo 出站 deadline 取本请求入站上下文
// deadline（V2 入站已按 min(传输硬看门, now+budget) 收敛），不使用无限
// deadline——调用方预算耗尽时后端调用随之受限，不悬挂在无界等待上。
//
// 优雅关闭：SIGINT/SIGTERM → 信号处理只置位标志，业务主协程在 Co 域观察到后
// RequestShutdown → CoStop（不建 native watcher 线程、不 join）；组件
// OnStop 把同一 CloseOptions 转发给各自 infra 对象（Redis 池 / Mongo owner
// 都只关闭自己拥有的连接与 worker），共享 Engine 由入口 host hook 在主任务
// 结束后 RequestClose，组件不关闭它。
//
// 入口是 runtime 的生成式 `bbtco_main`：进程不写 native main、不启动/停止
// scheduler、不建 runner 线程、不等待 future。参数经环境变量传入：
//   BBT_EXAMPLE_LISTEN_PORT / BBT_EXAMPLE_REDIS_ADDR / BBT_EXAMPLE_MONGO_URI

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

#include <bbt/infra/mongo/Client.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/redis/CoRedisPool.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/HostGate.hpp>
#include <bbt/framework/MongoDBComponent.hpp>
#include <bbt/framework/RedisComponent.hpp>
#include <bbt/framework/V2.hpp>

namespace fw  = bbt::framework;
namespace v2  = bbt::framework::v2;
namespace co  = bbt::coroutine;
namespace inf = bbt::infra;

namespace {

fw::CoRpcResp ArgErr(const char* what) {
    return fw::CoRpcResp::Error(
        fw::MakeError(fw::ErrorCode::InvalidArgument, what));
}

// 组件缺失统一错误：绑定在装配期已建立，GetComponent 缺失即框架 invariant
// 被破坏，如实 RuntimeUnavailable。
fw::CoRpcResp ResErr(const char* name) {
    return fw::CoRpcResp::Error(fw::MakeError(
        fw::ErrorCode::RuntimeUnavailable,
        std::string("storage: component '") + name + "' missing"));
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
inf::mongo::MongoDocument DocKey(std::string_view key) {
    std::vector<std::uint8_t> body;
    BsonElemString(body, "_id", key);
    inf::mongo::MongoDocument doc;
    const std::int32_t total =
        static_cast<std::int32_t>(body.size() + 5);
    doc.bytes.reserve(static_cast<std::size_t>(total));
    BsonPutI32(doc.bytes, total);
    doc.bytes.insert(doc.bytes.end(), body.begin(), body.end());
    doc.bytes.push_back(0x00);
    return doc;
}
// update 文档：{$set:{v:value}}，upsert 语义由 UpdateOne 承担。
inf::mongo::MongoDocument DocSet(std::string_view val) {
    std::vector<std::uint8_t> inner;
    BsonElemString(inner, "v", val);
    std::vector<std::uint8_t> body;
    // $set 是嵌入文档元素（type 0x03）。
    body.push_back(0x03);
    BsonPutCStr(body, "$set");
    BsonPutI32(body, static_cast<std::int32_t>(inner.size() + 5));
    body.insert(body.end(), inner.begin(), inner.end());
    body.push_back(0x00);
    inf::mongo::MongoDocument doc;
    const std::int32_t total =
        static_cast<std::int32_t>(body.size() + 5);
    doc.bytes.reserve(static_cast<std::size_t>(total));
    BsonPutI32(doc.bytes, total);
    doc.bytes.insert(doc.bytes.end(), body.begin(), body.end());
    doc.bytes.push_back(0x00);
    return doc;
}

// 组件借用统一取入站预算：Redis/Mongo 出站不得用无限 deadline——本请求从
// wire 继承来的 deadline（上游已按 min(传输硬看门, now+budget) 收敛）就是
// 后端调用的上限；无受管请求上下文即如实失败，不退化为无限等待。
class StorageSvc final
    : public v2::CoService<v2::RedisComponent, v2::MongoDBComponent> {
public:
    static constexpr std::string_view kServiceName = "storage";

    explicit StorageSvc(fw::detail::ConstructKey k)
        : v2::CoService<v2::RedisComponent, v2::MongoDBComponent>(k) {}

    fw::result<inf::CallOptions> ResourceBudget() {
        auto ctx = context().request();
        if (!ctx)
            return fw::result<inf::CallOptions>::err(fw::MakeError(
                fw::ErrorCode::InvalidContext,
                "storage: no managed request context for resource budget"));
        inf::CallOptions o;
        o.deadline = ctx.value()->deadline;
        return fw::result<inf::CallOptions>::ok(o);
    }

    fw::CoRpcResp Put(fw::CoRpcReq req) {
        auto args = req.Parse<std::string, std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        auto& [key, value] = args.value();
        if (key.empty()) return ArgErr("storage.put: empty key");
        auto cache = GetComponent<v2::RedisComponent>("cache");
        auto docs  = GetComponent<v2::MongoDBComponent>("docs");
        if (!cache) return ResErr("cache");
        if (!docs)  return ResErr("docs");
        auto budget = ResourceBudget();
        if (!budget) return fw::CoRpcResp::Error(budget.error());
        const inf::CallOptions& o = budget.value();
        // 真实双写：Redis SET 与 Mongo upsert 都成功才回写好的值；借用在同一
        // ComponentBorrow 寿命内完成，不持裸指针越过 borrow。
        auto cache_borrow = cache.value().Borrow();
        auto docs_borrow  = docs.value().Borrow();
        if (!cache_borrow || !docs_borrow)
            return ResErr("borrow");
        if (auto r = bbt_co_await cache_borrow.value().Get().Pool().Set(
                key, value, o);
            !r)
            return fw::CoRpcResp::Error(r.error());
        if (auto r = bbt_co_await docs_borrow.value().Get().Collection().UpdateOne(
                DocKey(key), DocSet(value), o);
            !r)
            return fw::CoRpcResp::Error(r.error());
        std::printf("[svc_a] put key=%s (redis+mongo)\n", key.c_str());
        return fw::CoRpcResp::From(value);
    }

    fw::CoRpcResp Get(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        if (args.value().empty()) return ArgErr("storage.get: empty key");
        auto cache = GetComponent<v2::RedisComponent>("cache");
        auto docs  = GetComponent<v2::MongoDBComponent>("docs");
        if (!cache) return ResErr("cache");
        if (!docs)  return ResErr("docs");
        auto budget = ResourceBudget();
        if (!budget) return fw::CoRpcResp::Error(budget.error());
        const inf::CallOptions& o = budget.value();
        // 真实读路径：Redis GET 命中即回；miss 回退 Mongo FindOne。
        auto cache_borrow = cache.value().Borrow();
        auto docs_borrow  = docs.value().Borrow();
        if (!cache_borrow || !docs_borrow)
            return ResErr("borrow");
        auto hit = bbt_co_await cache_borrow.value().Get().Pool().Get(
            args.value(), o);
        if (!hit) return fw::CoRpcResp::Error(hit.error());
        if (hit.value().has_value())
            return fw::CoRpcResp::From(hit.value().value());
        auto doc = bbt_co_await docs_borrow.value().Get().Collection().FindOne(
            DocKey(args.value()), o);
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
        auto cache = GetComponent<v2::RedisComponent>("cache");
        auto docs  = GetComponent<v2::MongoDBComponent>("docs");
        if (!cache) return ResErr("cache");
        if (!docs)  return ResErr("docs");
        auto budget = ResourceBudget();
        if (!budget) return fw::CoRpcResp::Error(budget.error());
        const inf::CallOptions& o = budget.value();
        auto cache_borrow = cache.value().Borrow();
        auto docs_borrow  = docs.value().Borrow();
        if (!cache_borrow || !docs_borrow)
            return ResErr("borrow");
        auto rn = bbt_co_await cache_borrow.value().Get().Pool().Delete(
            {args.value()}, o);
        if (!rn) return fw::CoRpcResp::Error(rn.error());
        auto mn = bbt_co_await docs_borrow.value().Get().Collection().DeleteOne(
            DocKey(args.value()), o);
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
        // 在永不通知的 CoWaiter 上带预算等待：等价于协作式 sleep，入站
        // deadline 到期以 TimedOut 如实返回，协程级 RequestCancel 到达则
        // 返回 Cancelled（deadline 取入站预算，本示例不另叠 ms 上限）。
        auto gate = co::sync::CoWaiter::Create();
        co::WaitOptions wo;
        wo.deadline = ctx.value()->deadline;
        const auto st = gate->Wait(wo);
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

protected:
    ~StorageSvc() noexcept override = default;
    friend class fw::detail::Registry;
};

// signal → flag：handler 只做 async-signal-safe 的 atomic store；
// watcher 线程轮询后调用 RequestShutdown（避免在信号上下文里
// 触碰 condition_variable）。
std::atomic_bool g_stop{false};

void InstallSignals() {
    std::signal(SIGINT,  [](int) { g_stop.store(true); });
    std::signal(SIGTERM, [](int) { g_stop.store(true); });
}

// 十进制端口解析：只接受 1..65535 的数字串。刻意不走 std::stoi——异常未捕获
// 会让进程 terminate，而非法端口是普通用法错误，应给诊断后正常退出。
bool ParsePort(const char* text, std::uint16_t* out) {
    if (text == nullptr || *text == '\0') return false;
    unsigned long value = 0;
    for (const char* p = text; *p != '\0'; ++p) {
        if (*p < '0' || *p > '9') return false;
        value = value * 10 + static_cast<unsigned long>(*p - '0');
        if (value > 65535) return false;
    }
    if (value == 0) return false;
    *out = static_cast<std::uint16_t>(value);
    return true;
}

// "host:port" → (host, port)；port 段缺失/非法 → port=0（调用方据此给诊断退出，
// infra 的 net::Address::Ip 仍是同一个 0/非法值的后端兜底）。不静默猜测、
// 不抛异常。
std::pair<std::string, std::uint16_t> SplitAddr(const std::string& addr) {
    const auto pos = addr.rfind(':');
    if (pos == std::string::npos) return {addr, 0};
    std::uint16_t port = 0;
    if (!ParsePort(addr.c_str() + pos + 1, &port))
        return {addr.substr(0, pos), 0};
    return {addr.substr(0, pos), port};
}

constexpr auto kRedisConnectBudget = std::chrono::milliseconds{5000};

std::string Env(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? std::string{} : std::string{value};
}

// 生命周期结果 → 进程退出码：0 优雅关闭 / 1 校验拒绝 / 2 启动失败回退 /
// 3 关闭曾超预算 / 4 关闭收尾存在失败项。
int CoRc(const fw::result<void>& r) {
    if (r)
        return 0;
    switch (r.error().code) {
    case fw::ErrorCode::InvalidArgument:
        return 1;
    case fw::ErrorCode::TimedOut:
        return 3;
    default:
        return 4;
    }
}

} // namespace

// handler 经组件借用发起真实 Redis/Mongo 出站，嵌套出站链需要更大的协程栈；
// 默认 12KB 触底。static worker 槽位 = 4（最小充分值）：每个 net::Engine 绑定
// slot_count = max(1, num_event_loop) + (server ? 1 : 0)，槽位在 Run 时从 static
// worker 绑定。本进程：共享 Engine 上 RPC server 2 槽（1 sub + 1 main）
// + client 1 槽 = 3。
void setup_runtime() {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
}

int bbtco_main() {
    // 参数（生成式入口不暴露 argv）：BBT_EXAMPLE_LISTEN_PORT /
    // BBT_EXAMPLE_REDIS_ADDR / BBT_EXAMPLE_MONGO_URI。
    const std::string port_text = Env("BBT_EXAMPLE_LISTEN_PORT");
    const std::string redis_text = Env("BBT_EXAMPLE_REDIS_ADDR");
    const std::string mongo_uri = Env("BBT_EXAMPLE_MONGO_URI");
    if (port_text.empty() || redis_text.empty() || mongo_uri.empty()) {
        std::fprintf(stderr,
            "usage: svc_a with env BBT_EXAMPLE_LISTEN_PORT=<port> "
            "BBT_EXAMPLE_REDIS_ADDR=<host:port> BBT_EXAMPLE_MONGO_URI=<uri>\n");
        return 64;   // EX_USAGE
    }
    std::uint16_t port = 0;
    if (!ParsePort(port_text.c_str(), &port)) {
        std::fprintf(stderr,
            "[svc_a] FATAL: 非法 listen_port \"%s\"（需要 1..65535 的十进制端口）\n",
            port_text.c_str());
        return 64;   // EX_USAGE
    }
    const auto [rhost, rport] = SplitAddr(redis_text);
    if (rhost.empty() || rport == 0) {
        std::fprintf(stderr,
            "[svc_a] FATAL: 非法 redis_addr \"%s\"（需要 host:port，端口 1..65535）\n",
            redis_text.c_str());
        return 64;   // EX_USAGE
    }

    fw::AppNetworkOptions network;
    network.SetLimits(inf::NetworkLimits{
                          /*max_connections*/ 128,
                          /*max_inflight*/    64,
                          /*max_header_bytes*/16384,
                          /*max_body_bytes*/  65536,
                          /*incoming_timeout*/
                          std::chrono::milliseconds{30000}})
        .SetListenAddress(inf::ListenAddress{"127.0.0.1", port})
        // V2 会话：入站预算经 wire 的 remaining_budget_ms 与本地
        // incoming_timeout 取 local-min 后才对 handler 可见。
        .SetLocalAppId("dual-storage");

    auto app = v2::CoApp::Create(
        fw::AppOptions{}
            .SetLimits(64, 64, 8, 64)
            .SetShutdownBudget(std::chrono::milliseconds{2000})
            .SetLifecycleBudget(std::chrono::milliseconds{2000})
            .SetNetworkOptions(std::move(network)),
        "dual-storage");

    // 真实后端组件：v2::RedisComponent 借用共享 Engine（app->engine()，由
    // CoApp 持有并在入口 host hook 执行唯一一次 Run；组件不 Run/不关闭它），
    // OnStart 在有限预算内全池建连——不可达后端即 CoStart 失败。Mongo 组件
    // 自有 worker，不借 Engine。
    std::error_code raddr_ec;
    auto redis_address = inf::net::Address::Ip(rhost, rport, raddr_ec);
    if (raddr_ec || !redis_address.Valid()) {
        std::fprintf(stderr,
            "[svc_a] FATAL: 非法 redis_addr \"%s\"（新入口只接受数值 "
            "IPv4/IPv6: %s）\n",
            redis_text.c_str(), raddr_ec.message().c_str());
        return 64;   // EX_USAGE
    }
    v2::RedisComponent::Options redis_options;
    redis_options.engine = app->engine();
    redis_options.address = redis_address;
    redis_options.pool.connections = 2;
    redis_options.connect_timeout = kRedisConnectBudget;
    if (auto r = bbt_co_await app->CoInstallComponent<v2::RedisComponent>(
            "cache", redis_options);
        !r) {
        std::fprintf(stderr, "[svc_a] install cache failed: %s\n",
                     r.error().message.c_str());
        return 65;
    }
    v2::MongoDBComponent::Options mongo_options;
    mongo_options.config.uri = mongo_uri;
    mongo_options.config.worker_threads = 2;
    mongo_options.config.max_queue = 256;
    mongo_options.config.server_selection_timeout =
        std::chrono::milliseconds{5000};
    mongo_options.config.connect_timeout = std::chrono::milliseconds{5000};
    mongo_options.config.socket_timeout = std::chrono::milliseconds{5000};
    mongo_options.config.wait_queue_timeout = std::chrono::milliseconds{5000};
    mongo_options.target.database = "bbt_dual_service";
    mongo_options.target.collection = "kv";
    if (auto r = bbt_co_await app->CoInstallComponent<v2::MongoDBComponent>(
            "docs", mongo_options);
        !r) {
        std::fprintf(stderr, "[svc_a] install docs failed: %s\n",
                     r.error().message.c_str());
        return 65;
    }
    if (auto r = bbt_co_await app->CoInstallService<StorageSvc>(
            fw::ServiceOptions{}
                .SetMaxInflight(64)
                .BindComponent<v2::RedisComponent>("cache")
                .BindComponent<v2::MongoDBComponent>("docs"));
        !r) {
        std::fprintf(stderr, "[svc_a] CoInstallService failed: %s\n",
                     r.error().message.c_str());
        return 65;
    }

    InstallSignals();

    auto started = bbt_co_await app->CoStart();
    if (!started) {
        std::fprintf(stderr, "[svc_a] start failed: %s\n",
                     started.error().message.c_str());
        return CoRc(started);
    }

    std::printf("[svc_a] storage service on 127.0.0.1:%u "
                "(redis=%s mongo=%s)\n",
                port, redis_text.c_str(), mongo_uri.c_str());
    std::fflush(stdout);

    // 关闭触发只有信号：处理函数只置位标志，主协程在 Co 域轮询到后请求关闭。
    while (!g_stop.load(std::memory_order_acquire))
        bbtco_sleep(20);
    app->RequestShutdown();
    auto requested = bbt_co_await app->CoWaitShutdownRequest();
    if (!requested) {
        std::fprintf(stderr, "[svc_a] wait shutdown request failed: %s\n",
                     requested.error().message.c_str());
    }
    auto stopped = bbt_co_await app->CoStop();
    const int rc = CoRc(stopped);
    std::printf("[svc_a] CoStop returned %d, state=%d\n",
                rc, static_cast<int>(app->State()));
    for (const auto& f : app->LifecycleFailures())
        std::fprintf(stderr, "[svc_a] lifecycle_failure: %s\n",
                     f.message.c_str());
    std::fflush(stdout);
    return rc;
}

// 入口 native host hook：共享 Engine 的 Engine::Run 在主线程执行一次。
int bbtco_main_host_hook(bbt::coroutine::HostHookContext& ctx) {
    return bbt::framework::HostGate::RunHostGate(ctx);
}

BBTCO_MAIN_WITH_SETUP_AND_HOST_HOOK();
