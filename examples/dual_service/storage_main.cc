/**
 * examples/dual_service/storage_main.cc — svc_a：存储服务进程。
 *
 * 业务形态：单进程托管一个 Concurrent 服务 "storage"，真实后端经
 * CoApp::CoInstallComponent 装配为受管组件——Redis（"cache"，新 net 入口
 * bbt::infra::redis::CoRedisCli，经框架 RedisNetResource 适配进受管组件
 * 生命周期：OnStart 拉起自有 Engine Run 线程并装配 client，OnStop 同步
 * 物理收口到 Engine Phase==Stopped）与 MongoDB（"docs"，新 owner/句柄入口
 * bbt::infra::mongo::CoMongoDb + CoMongoColl，经框架 MongoDbResource 适配
 * 进同一收口序列；owner Close 返回即 ops 清空 + worker 全退），handler 经
 * GetComponent + Borrow 取用。不再使用进程内 KvStore 替身：put/get/del 落
 * 到真实后端往返，两端地址由命令行显式传入（Redis 不可达 →
 * StorageSvc::OnStart 显式 Connect 失败 → 服务 Failed 并参与启动失败
 * 回退；Mongo 装配失败 → 组件 OnStart 失败同一回退路径）。Redis 侧只接受
 * 数值 IPv4/IPv6（net::Address::Ip 不解析主机名，非法地址在装配期如实
 * 失败）。跨进程对端（gateway 经 static_routes 指向本进程）与 driver 都按
 * V2 会话（SessionFrame + u32be + Hello/HelloAck）真实接入。
 *
 * 方法表：
 * put(key, value)   写 Redis SET + Mongo upsert；空 key → InvalidArgument。
 * get(key)          先查 Redis，miss 回退 Mongo；都未命中 → 业务错误
 * NotFound（非传输错误）。
 * del(key)          Redis DEL + Mongo DeleteOne；命中回 int32 1。
 * sleep_ms(ms)      挂起至多 ms 毫秒（等待一个永不完成的信号，deadline
 * 取自入站上下文——调用方预算到期时如实 TimedOut）。
 *
 * 资源预算：put/get/del 的 Redis/Mongo 出站 deadline 取本请求入站上下文
 * deadline（V2 入站已按 min(传输硬看门, now+budget) 收敛），不使用无限
 * deadline——调用方预算耗尽时后端调用随之受限，不悬挂在无界等待上。
 *
 * 优雅关闭：SIGINT/SIGTERM → watcher 线程调用 RequestShutdown（幂等
 * noexcept，可任意线程）→ 主协程 CoWaitShutdownRequest 返回后亲自
 * CoStop 收口；退出码 0 = CoStop 成功且 AppState==Stopped，4 = 装配/收口
 * 未竟（沿用 getvalue 口径），64 = 用法错误。
 */

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

#include <bbt/infra/mongo/Client.hpp>
#include <bbt/infra/net/Address.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/MongoDbResource.hpp>
#include <bbt/framework/RedisNetResource.hpp>

namespace fw  = bbt::framework;
namespace co  = bbt::coroutine;
namespace inf = bbt::infra;

namespace {

fw::CoRpcResp ArgErr(const char* what) {
    return fw::CoRpcResp::Error(
        fw::MakeError(fw::ErrorCode::InvalidArgument, what));
}

/**
 * 缺资源统一错误：声明式绑定在 CoInstallService 期已展开，缺失即框架
 * invariant 被破坏，如实透出绑定/借用侧的原错误，不降级为泛化错误。
 */
fw::CoRpcResp ResErr(const fw::Error& err) {
    return fw::CoRpcResp::Error(err);
}

/**
 * Mongo 值文档：把 string value 编进 {_id:key, v:value} 的最小 BSON。
 * MongoDocument 是 owning bytes；只编码本服务用的 string 元素。
 */
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
    /**
     * type: string
     */
    b.push_back(0x02);
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
/**
 * update 文档：{$set:{v:value}}，upsert 语义由 UpdateOne 承担。
 */
inf::mongo::MongoDocument DocSet(std::string_view val) {
    std::vector<std::uint8_t> inner;
    BsonElemString(inner, "v", val);
    std::vector<std::uint8_t> body;
    /**
     * $set 是嵌入文档元素（type 0x03）。
     */
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

/**
 * Redis 建连预算：OnStart 内在受管协程经组件命令面显式 Connect
 * （新入口要求 Connect 只在协程上下文调用）。预算只给 Connect 本身；
 * 本步外层还有服务 OnStart 的生命周期期限兜底，失败如实返回让服务
 * Failed 并参与启动失败回退——旧工厂「Connected 才交付」语义由
 * 「Connect 不过即启动失败」等价表达。
 */
constexpr auto kRedisConnectBudget = std::chrono::milliseconds{5000};

/**
 * 资源调用统一取入站预算：Redis/Mongo 出站不得用无限 deadline——本请求从
 * wire 继承来的 deadline（上游已按 min(传输硬看门, now+budget) 收敛）就是
 * 后端调用的上限；无受管请求上下文即如实失败，不退化为无限等待。
 */
class StorageSvc final
    : public fw::CoService<fw::RedisNetResource, fw::MongoDbResource> {
public:
    static constexpr std::string_view kServiceName = "storage";

    explicit StorageSvc(fw::detail::ConstructKey key)
        : fw::CoService<fw::RedisNetResource, fw::MongoDbResource>(key) {}

    /**
     * 服务启动：此时声明绑定的组件已 Running，借 "cache" 显式 Connect
     * ——只有建连成功本服务才算启动；失败原错误透出（组件随启动失败
     * 回退收口，不留半装配句柄）。
     */
    fw::result<void> OnStart() override {
        auto h = GetComponent<fw::RedisNetResource>("cache");
        if (!h) return fw::result<void>::err(std::move(h.error()));
        auto b = h.value().Borrow();
        if (!b) return fw::result<void>::err(std::move(b.error()));
        const inf::CallOptions o{
            std::chrono::steady_clock::now() + kRedisConnectBudget};
        auto connected = bbt_co_await b.value().Get().Connect(o);
        if (!connected)
            return fw::result<void>::err(std::move(connected.error()));
        return fw::result<void>::ok();
    }

    fw::result<inf::CallOptions> ResourceBudget() {
        auto ctx = fw::CurrentRequestContext();
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
        auto cache = GetComponent<fw::RedisNetResource>("cache");
        if (!cache) return ResErr(cache.error());
        auto cache_b = cache.value().Borrow();
        if (!cache_b) return ResErr(cache_b.error());
        auto docs = GetComponent<fw::MongoDbResource>("docs");
        if (!docs) return ResErr(docs.error());
        auto docs_b = docs.value().Borrow();
        if (!docs_b) return ResErr(docs_b.error());
        auto budget = ResourceBudget();
        if (!budget) return fw::CoRpcResp::Error(budget.error());
        const inf::CallOptions& o = budget.value();
        /**
         * 真实双写：Redis SET 与 Mongo upsert 都成功才回写好的值。
         */
        if (auto r = bbt_co_await cache_b.value().Get().Set(key, value, o); !r)
            return fw::CoRpcResp::Error(r.error());
        if (auto r = bbt_co_await docs_b.value().Get().UpdateOne(
                DocKey(key), DocSet(value), o); !r)
            return fw::CoRpcResp::Error(r.error());
        std::printf("[svc_a] put key=%s (redis+mongo)\n", key.c_str());
        return fw::CoRpcResp::From(value);
    }

    fw::CoRpcResp Get(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        if (args.value().empty()) return ArgErr("storage.get: empty key");
        auto cache = GetComponent<fw::RedisNetResource>("cache");
        if (!cache) return ResErr(cache.error());
        auto cache_b = cache.value().Borrow();
        if (!cache_b) return ResErr(cache_b.error());
        auto docs = GetComponent<fw::MongoDbResource>("docs");
        if (!docs) return ResErr(docs.error());
        auto docs_b = docs.value().Borrow();
        if (!docs_b) return ResErr(docs_b.error());
        auto budget = ResourceBudget();
        if (!budget) return fw::CoRpcResp::Error(budget.error());
        const inf::CallOptions& o = budget.value();
        /**
         * 真实读路径：Redis GET 命中即回；miss 回退 Mongo FindOne。
         */
        auto hit = bbt_co_await cache_b.value().Get().Get(args.value(), o);
        if (!hit) return fw::CoRpcResp::Error(hit.error());
        if (hit.value().has_value())
            return fw::CoRpcResp::From(hit.value().value());
        auto doc = bbt_co_await docs_b.value().Get().FindOne(
            DocKey(args.value()), o);
        if (!doc) return fw::CoRpcResp::Error(doc.error());
        if (doc.value().has_value()) {
            /**
             * 命中 Mongo 即算命中（值本体在文档字节里，示例只证真实往返）。
             */
            return fw::CoRpcResp::From(
                std::string{"mongo-hit:"} + args.value());
        }
        /**
         * 两端都未命中：业务错误 NotFound，不是传输失败。
         */
        return fw::CoRpcResp::Error(fw::MakeError(
            fw::ErrorCode::NotFound,
            "storage.get: key '" + args.value() + "' not found"));
    }

    fw::CoRpcResp Del(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        auto cache = GetComponent<fw::RedisNetResource>("cache");
        if (!cache) return ResErr(cache.error());
        auto cache_b = cache.value().Borrow();
        if (!cache_b) return ResErr(cache_b.error());
        auto docs = GetComponent<fw::MongoDbResource>("docs");
        if (!docs) return ResErr(docs.error());
        auto docs_b = docs.value().Borrow();
        if (!docs_b) return ResErr(docs_b.error());
        auto budget = ResourceBudget();
        if (!budget) return fw::CoRpcResp::Error(budget.error());
        const inf::CallOptions& o = budget.value();
        auto rn = bbt_co_await cache_b.value().Get().Delete({args.value()}, o);
        if (!rn) return fw::CoRpcResp::Error(rn.error());
        auto mn = bbt_co_await docs_b.value().Get().DeleteOne(
            DocKey(args.value()), o);
        if (!mn) return fw::CoRpcResp::Error(mn.error());
        const bool removed = (rn.value() + mn.value()) > 0;
        return fw::CoRpcResp::From(static_cast<std::int32_t>(removed ? 1 : 0));
    }

    fw::CoRpcResp SleepMs(fw::CoRpcReq req) {
        auto args = req.Parse<std::int32_t>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        if (args.value() < 0) return ArgErr("storage.sleep_ms: negative ms");
        auto ctx = fw::CurrentRequestContext();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        /**
         * 在永不通知的 CoWaiter 上带预算等待：等价于协作式 sleep，入站
         * deadline 到期以 TimedOut 如实返回，协程级 RequestCancel 到达则
         * 返回 Cancelled（deadline 取入站预算，本示例不另叠 ms 上限）。
         */
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

private:
    friend class bbt::framework::detail::Registry;
};

/**
 * signal → flag：handler 只做 async-signal-safe 的 atomic store；
 * watcher 线程轮询后调用 RequestShutdown（避免在信号上下文里
 * 触碰 condition_variable）。
 */
std::atomic_bool g_stop{false};

void InstallSignals() {
    std::signal(SIGINT,  [](int) { g_stop.store(true); });
    std::signal(SIGTERM, [](int) { g_stop.store(true); });
}

/**
 * 十进制端口解析：只接受 1..65535 的数字串。刻意不走 std::stoi——异常未捕获
 * 会让进程 terminate，而非法端口是普通用法错误，应给诊断后正常退出。
 */
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

/**
 * "host:port" → (host, port)；port 段缺失/非法 → port=0（调用方据此给诊断退出，
 * infra 的 net::Address::Ip 仍是同一个 0/非法值的后端兜底）。不静默猜测、
 * 不抛异常。
 */
std::pair<std::string, std::uint16_t> SplitAddr(const std::string& addr) {
    const auto pos = addr.rfind(':');
    if (pos == std::string::npos) return {addr, 0};
    std::uint16_t port = 0;
    if (!ParsePort(addr.c_str() + pos + 1, &port))
        return {addr.substr(0, pos), 0};
    return {addr.substr(0, pos), port};
}

} // namespace

int main(int argc, char** argv) {
    /**
     * 用法: svc_a <listen_port> <redis_addr host:port> <mongo_uri>
     */
    if (argc != 4) {
        std::fprintf(stderr,
            "usage: svc_a <listen_port> <redis_addr host:port> <mongo_uri>\n");
        /**
         * EX_USAGE
         */
        return 64;
    }
    std::uint16_t port = 0;
    if (!ParsePort(argv[1], &port)) {
        std::fprintf(stderr,
            "[svc_a] FATAL: 非法 listen_port \"%s\"（需要 1..65535 的十进制端口）\n",
            argv[1]);
        /**
         * EX_USAGE
         */
        return 64;
    }
    const auto [rhost, rport] = SplitAddr(argv[2]);
    if (rhost.empty() || rport == 0) {
        std::fprintf(stderr,
            "[svc_a] FATAL: 非法 redis_addr \"%s\"（需要 host:port，端口 1..65535）\n",
            argv[2]);
        /**
         * EX_USAGE
         */
        return 64;
    }
    const std::string mongo_uri = argv[3];

    /**
     * 嵌套出站链的协程栈需求见实测记录：默认 12KB 触底，
     * 本进程 handler 经组件借用发起真实 Redis/Mongo 出站，与对端保持同一配置。
     * static worker 槽位 = 4：RedisNetResource 的 client-only Engine 1 槽 +
     * RPC server Engine 2 槽（1 sub + 1 main）+ 主协程 1 槽——旧 run() 时代
     * 资源工厂在 on_scheduler_started 相位装配，现组件 OnStart 与主协程都在
     * 受管协程内，仍需要同等余量（2 worker 时 bind 报 no_buffer_space）。
     */
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;

    /**
     * Redis 地址只接受数值 IPv4/IPv6：host:port 注入若含主机名/非法段在
     * 装配前如实退出（EX_USAGE），不静默猜测。
     */
    std::error_code raddr_ec;
    auto redis_address = inf::net::Address::Ip(rhost, rport, raddr_ec);
    if (raddr_ec || !redis_address.Valid()) {
        std::fprintf(stderr,
            "[svc_a] FATAL: 非法 redis_addr \"%s\"（新入口只接受数值 "
            "IPv4/IPv6: %s）\n",
            argv[2], raddr_ec.message().c_str());
        /**
         * EX_USAGE
         */
        return 64;
    }

    /**
     * Mongo：新 owner/句柄入口——MongoRuntimeConfig 只携带连接预算，
     * database/collection 目标值由 MongoDbResource::Options 绑定为集合
     * 句柄（组件 OnStart 内 Validate + owner Create/Start + Collection
     * 绑定一次完成；owner Close 是同步物理收口）。
     */
    inf::mongo::MongoRuntimeConfig mcfg;
    mcfg.uri                      = mongo_uri;
    mcfg.worker_threads           = 2;
    mcfg.max_queue                = 256;
    mcfg.server_selection_timeout = std::chrono::milliseconds{5000};
    mcfg.connect_timeout          = std::chrono::milliseconds{5000};
    mcfg.socket_timeout           = std::chrono::milliseconds{5000};
    mcfg.wait_queue_timeout       = std::chrono::milliseconds{5000};
    const inf::mongo::MongoTarget mtarget{"bbt_dual_service", "kv"};

    InstallSignals();
    g_scheduler->Start();

    auto options = fw::AppOptions{}
        .SetLimits(128, 128, 64, 64)
        .SetNetworkLimits(inf::NetworkLimits{
            /*max_connections*/ 128,
            /*max_inflight*/    64,
            /*max_header_bytes*/16384,
            /*max_body_bytes*/  65536,
            /*incoming_timeout*/std::chrono::milliseconds{30000}})
        .Listen("127.0.0.1:" + std::to_string(port))
        .SetLocalIdentity("dual-storage")
        /**
         * 纯被调方：无出站 RPC 路由
         */
        .SetShutdownBudget(std::chrono::milliseconds{2000})
        .SetLifecycleBudget(std::chrono::milliseconds{30000});

    /**
     * 装配/启动/等待关闭/收口全部在受管协程内（Co* 入口的 CoRuntimeOnly
     * 强约束）；App 指针发布给 watcher 线程只做 RequestShutdown；退出码经
     * promise 回传（0 = CoStop 成功且 Stopped，2 = 安装/启动失败，
     * 4 = 收口未竟）。app_done 让 watcher 在「主协程未发布 app 就结束」
     * 时也能退出。
     */
    std::promise<int>                rc_slot;
    std::shared_ptr<fw::CoApp>       app_handle;
    std::mutex                       app_mtx;
    std::atomic_bool                 app_done{false};
    const std::string                redis_desc =
        rhost + ":" + std::to_string(rport);

    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [options = std::move(options), redis_address, mcfg, mtarget,
         redis_desc, &rc_slot, &app_handle, &app_mtx, &app_done]() mutable {
            auto finish = [&](int rc) {
                app_done.store(true, std::memory_order_release);
                rc_slot.set_value(rc);
            };
            auto app = fw::CoApp::Create(std::move(options), "dual-storage");
            if (!app) {
                std::fprintf(stderr, "[svc_a] CoApp::Create failed\n");
                finish(4);
                return;
            }
            {
                std::lock_guard<std::mutex> lk(app_mtx);
                app_handle = app;
            }
            auto cache_install =
                bbt_co_await app->CoInstallComponent<fw::RedisNetResource>(
                    "cache", fw::RedisNetResource::Options{}
                                 .SetAddress(redis_address));
            if (!cache_install) {
                std::fprintf(stderr,
                             "[svc_a] CoInstallComponent(cache) failed: %s\n",
                             cache_install.error().message.c_str());
                finish(2);
                return;
            }
            auto docs_install =
                bbt_co_await app->CoInstallComponent<fw::MongoDbResource>(
                    "docs", fw::MongoDbResource::Options{}
                                .SetConfig(mcfg).SetTarget(mtarget));
            if (!docs_install) {
                std::fprintf(stderr,
                             "[svc_a] CoInstallComponent(docs) failed: %s\n",
                             docs_install.error().message.c_str());
                finish(2);
                return;
            }
            auto svc_install = bbt_co_await app->CoInstallService<StorageSvc>(
                fw::ServiceOptions{}
                    .SetExecution(fw::ExecutionPolicy::Concurrent)
                    .SetMaxInflight(64)
                    .BindComponent<fw::RedisNetResource>("cache")
                    .BindComponent<fw::MongoDbResource>("docs"));
            if (!svc_install) {
                std::fprintf(stderr, "[svc_a] CoInstallService failed: %s\n",
                             svc_install.error().message.c_str());
                finish(2);
                return;
            }
            auto started = bbt_co_await app->CoStart(
                fw::LifecycleOptions{}.SetDeadline(
                    std::chrono::steady_clock::now() +
                    std::chrono::seconds{30}));
            if (!started) {
                std::fprintf(stderr, "[svc_a] CoStart failed: %s\n",
                             started.error().message.c_str());
                for (const auto& f : app->LifecycleFailures())
                    std::fprintf(stderr,
                                 "[svc_a] lifecycle_failure: %s\n",
                                 f.message.c_str());
                finish(2);
                return;
            }
            std::printf("[svc_a] storage service on %s "
                        "(redis=%s mongo=%s)\n",
                        app->BoundEndpoint().c_str(),
                        redis_desc.c_str(), mcfg.uri.c_str());
            std::fflush(stdout);
            const auto waited = bbt_co_await app->CoWaitShutdownRequest();
            (void)waited;
            auto stopped = bbt_co_await app->CoStop(
                fw::LifecycleOptions{}.SetDeadline(
                    std::chrono::steady_clock::now() +
                    std::chrono::seconds{5}));
            for (const auto& f : app->LifecycleFailures())
                std::fprintf(stderr, "[svc_a] lifecycle_failure: %s\n",
                             f.message.c_str());
            finish(stopped && app->State() == fw::AppState::Stopped ? 0 : 4);
        },
        submitted);
    if (!submitted) {
        std::fprintf(stderr, "[svc_a] coroutine task rejected\n");
        return 4;
    }

    /**
     * watcher 等两件事实：app 已发布 且 stop flag 已置位——两者齐备才发一次
     * RequestShutdown（请求标志在 App 内持久，主协程到达
     * CoWaitShutdownRequest 时照常生效）。主协程任一终止路径先置
     * app_done，覆盖「Create/安装失败、app 未发布或已结束」的退出。
     */
    std::thread watcher([&] {
        for (;;) {
            std::shared_ptr<fw::CoApp> h;
            {
                std::lock_guard<std::mutex> lk(app_mtx);
                h = app_handle;
            }
            if (h && g_stop.load(std::memory_order_acquire)) {
                h->RequestShutdown();
                return;
            }
            if (app_done.load(std::memory_order_acquire))
                return;
            std::this_thread::sleep_for(std::chrono::milliseconds{20});
        }
    });

    const int rc = rc_slot.get_future().get();
    g_stop.store(true, std::memory_order_release);
    watcher.join();
    std::printf("[svc_a] exit rc=%d\n", rc);
    std::fflush(stdout);
    return rc;
}
