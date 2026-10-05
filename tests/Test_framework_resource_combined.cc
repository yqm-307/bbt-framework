// framework #8 F8-T3：真实 HTTP loopback → 框架入站 → 命名资源业务 Service
// → 真实 Redis + MongoDB 联合消费验收。
//
// 与单资源 live 的差别（不得以单资源 live 替代联合验收）：
//  - 宿主是框架公开构造 CoApp(CoAppOptions) 默认装配的真实 InfraHttpHost +
//    默认入站线桥（RpcHttpBridge），不是 NullNetHost/DispatchInboundForTest
//    测试缝；请求经真实 TCP/HTTP POST /rpc 进入框架再分发到 Service。
//  - 业务 Service（KvSvc）在独立 TU framework_resource_service.{hpp,cc} 中实现，
//    只 include framework/infra/coroutine 公开头；本文件只作为**测试宿主**另持
//    internal（线桥编码 + 返回码常量）。业务实现与宿主分属不同编译单元——不靠
//    「同一 TU 先 include 公共头」的标签声称隔离。
//
// 覆盖（F8-T3 最小业务链路）：
//  C1 联合业务：cache miss→Mongo 查询→写 cache；hit（经 Service 侧 Mongo
//     读数计数可观察，不靠猜测）；更新先落 Mongo 再失效/刷新 Redis；强制
//     cache miss 后从 Mongo 读回新值（独立证明持久化，非复用已刷新的 cache）；
//     not-found；真实后端错误原样传播（Mongo duplicate key →
//     RemoteError/DuplicateKey）。
//  C2 有限 deadline（派发前过期）：handler 取 context().request()->deadline
//     下传 infra::CallOptions；父预算耗尽后发起资源调用 → infra 在发起 I/O
//     前返回 TimedOut，且后端无副作用（键未写入，可观察），连接保持健康。
//  C3 关闭中在途请求：真实 HTTP handler 停在宿主注入的闸门上 → request_shutdown
//     预算耗尽 → ShutdownIncomplete + pending=WaitHandlersDone、资源尚未 Close
//     → 放行后同步等客户端明确结果 → 完整排空 → kExitShutdownLate。客户端结果按
//     InfraHttpHost.hpp:9-11 契约判定：Close 开始后尚未交给传输层的回复不承诺
//     送达，因此只接受「完整 HELD」或「TransportError(code=8)」，不接受空结果/
//     未知错误（未过期不等于 Close 后必达）。
//  C4a 后端 I/O 在途（服务端停顿）：同一自有实例 CLIENT PAUSE 1500 ALL 后，
//     业务 Get 在 cache 命中路径上被服务端**真实扣住**约 1500ms 才返回正确值
//     ——命令确已发出并被服务端执行（在途证据，非派发前过期）。
//  C4b 后端 I/O 在途 deadline：短父预算 < pause；命令已写出、ReadReply 等待时
//     deadline 先到 → 真实在途 TimedOut；连接被判定不可复用（ConnectStatus
//     ==Failed，走 FailConnAndDrainQueue，与 C2 派发前保留健康连接形成判别）；
//     恢复由测试装配控制层显式 Connect（不 Service 重连、不自动重试）。
//
// 环境变量（缺前两项任一 → 进程 exit 77 标 Skipped）：
//   BBT_TEST_REDIS_ADDR=host:port
//   BBT_TEST_MONGO_URI=mongodb://h:p/?directConnection=true
//   BBT_TEST_REDIS_CLI=/abs/path/redis-cli  （仅 C4a/C4b 需要；路径由
//     环境/runner 传入，仓内不写私有路径）
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <sys/wait.h>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/sync/WaitTypes.hpp>

// ── 业务 Service：独立 TU，仅 framework/infra/coroutine 公开头 ──
#include "framework_resource_service.hpp"

// ── 测试宿主面（internal 允许）：线桥编码 + 返回码常量 ──
#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/ShutdownState.hpp>
#include <bbt/framework/internal/HostLifecycle.hpp>
#include <bbt/framework/internal/RpcHttpBridge.hpp>

namespace fw  = bbt::framework;
namespace co  = bbt::coroutine;
namespace inf = bbt::infra;
using Clock = std::chrono::steady_clock;

namespace {

using fw8test::KvSvc;

constexpr std::chrono::milliseconds kWait{20000};

const char* EnvOr(const char* k) { return std::getenv(k); }
bool RedisEnabled() {
    const char* a = EnvOr("BBT_TEST_REDIS_ADDR");
    return a != nullptr && *a != '\0';
}
bool MongoEnabled() {
    const char* u = EnvOr("BBT_TEST_MONGO_URI");
    return u != nullptr && *u != '\0';
}

// 缺环境必须显式标记 Skipped（CTest SKIP_RETURN_CODE 77）：联合 target 要求
// Redis 与 Mongo 同时可用，缺任一即 skip，不以退出码 0 冒充通过。
struct RequireBothEnv {
    RequireBothEnv() {
        if (!RedisEnabled() || !MongoEnabled()) {
            std::fputs("combined: 需要 BBT_TEST_REDIS_ADDR + "
                       "BBT_TEST_MONGO_URI，标记 Skipped\n", stderr);
            std::exit(77);
        }
    }
};
BOOST_GLOBAL_FIXTURE(RequireBothEnv);

std::pair<std::string, std::uint16_t> SplitAddr(const std::string& addr) {
    const auto pos = addr.rfind(':');
    if (pos == std::string::npos) return {addr, 0};
    return {addr.substr(0, pos),
            static_cast<std::uint16_t>(std::stoi(addr.substr(pos + 1)))};
}

// 每次运行唯一的前缀：后端是保留状态的容器，避免与上次运行的文档/LRU 冲突。
std::string RunPrefix() {
    return "f8c-" + std::to_string(
        Clock::now().time_since_epoch().count()) + "-";
}

class TestLatch {
public:
    explicit TestLatch(int n) : m_count(n) {}
    void CountDown() {
        { std::lock_guard<std::mutex> lk(m_mtx); if (--m_count > 0) return; }
        m_cv.notify_all();
    }
    bool WaitFor(std::chrono::milliseconds ms) {
        std::unique_lock<std::mutex> lk(m_mtx);
        return m_cv.wait_for(lk, ms, [this] { return m_count == 0; });
    }
private:
    std::mutex              m_mtx;
    std::condition_variable m_cv;
    int                     m_count;
};

// ── 协程闸门（CoWaiter 单次放行；无 sleep 凑时序）──
class CoGate {
public:
    co::WaitStatus Wait(co::Deadline deadline) {
        auto waiter = co::sync::CoWaiter::Create();
        co::WaitOptions wo;
        wo.deadline = deadline;
        return waiter->WaitWithCallback(wo, [&]() -> bool {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (m_open) waiter->Notify();
            else        m_waiter = waiter;
            return true;
        });
    }
    void Open() {
        co::sync::CoWaiter::SPtr w;
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_open = true;
            w = m_waiter;
        }
        if (w) w->Notify();
    }
private:
    std::mutex                 m_mtx;
    bool                       m_open = false;
    co::sync::CoWaiter::SPtr   m_waiter;
};

// ── redis-cli（路径由环境/runner 传入；仅用于对同一自有实例造真实服务端停顿）──
std::string RedisCliPath() {
    const char* p = EnvOr("BBT_TEST_REDIS_CLI");
    return p ? std::string{p} : std::string{};
}

struct CliResult {
    int         rc{-1};
    std::string out;
};

CliResult RunRedisCli(const std::vector<std::string>& args) {
    CliResult res;
    const std::string cli = RedisCliPath();
    if (cli.empty()) return res;
    const auto [host, port] = SplitAddr(EnvOr("BBT_TEST_REDIS_ADDR"));
    std::string cmd = "'" + cli + "' -h " + host + " -p " +
        std::to_string(port);
    for (const auto& a : args) cmd += " " + a;
    cmd += " 2>&1";
    FILE* f = ::popen(cmd.c_str(), "r");
    if (f == nullptr) return res;
    char buf[256];
    std::size_t n = 0;
    while ((n = std::fread(buf, 1, sizeof(buf), f)) > 0)
        res.out.append(buf, n);
    const int st = ::pclose(f);
    res.rc = WIFEXITED(st) ? WEXITSTATUS(st) : -1;
    return res;
}

// ── 宿主 + 测试侧 HTTP 客户端 runtime。CoApp 用公开构造（默认真实 HTTP
//    装配）；测试进程内同刻至多一个 run 内 CoApp，用例串行。──
inf::NetworkLimits Limits(std::chrono::milliseconds incoming_timeout) {
    return inf::NetworkLimits{64, 64, 16384, 65536, incoming_timeout};
}

struct RunHandle {
    std::thread       th;
    fw::CoApp*        app{nullptr};
    std::atomic<bool> done{false};
    int               rc{-1};
    ~RunHandle() {
        if (!th.joinable()) return;
        if (!done.load(std::memory_order_acquire)) app->request_shutdown();
        th.join();
    }
};

struct CombinedBox {
    std::unique_ptr<fw::CoApp>           app;
    std::unique_ptr<RunHandle>           run;
    std::shared_ptr<inf::NetworkRuntime> client_rt;
    std::shared_ptr<inf::HttpClient>     client;
    std::string                          endpoint;

    CombinedBox() = default;
    ~CombinedBox() { Stop(); }
    CombinedBox(CombinedBox&&)                 = default;
    CombinedBox& operator=(CombinedBox&&)      = default;
    CombinedBox(const CombinedBox&)            = delete;
    CombinedBox& operator=(const CombinedBox&) = delete;

    void Stop() {
        if (client)    client->Close();
        if (client_rt) client_rt->Close();
        if (app)       app->request_shutdown();
        if (run && run->th.joinable()) run->th.join();
    }
};

// Redis 工厂：Create 在启动控制线程、显式 Connect 投递到 Scheduler 后限时
// 等待；只有 Connected 实例才交框架绑定（ready 先于接纳）。成功交付前的
// 任何退出路径（含等待超时/抛出）都由 RAII 同步 Close，不留半装配句柄。
fw::result<std::shared_ptr<inf::CoRedisCli>> MakeRedis(
    const inf::RedisClientConfig& cfg) {
    auto created = inf::CoRedisCli::Create(cfg);
    if (!created) return created;
    auto cli = created.value();
    bool delivered = false;
    struct Guard {
        std::shared_ptr<inf::CoRedisCli> cli;
        const bool*                      released;
        ~Guard() { if (!*released) cli->Close(); }
    } guard{cli, &delivered};
    auto promise = std::make_shared<std::promise<fw::result<void>>>();
    auto done    = promise->get_future();
    inf::CallOptions call_options{Clock::now() + kWait};
    bool submitted = false;
    g_scheduler->RegistCoroutineTask([cli, promise, call_options] {
        try {
            promise->set_value(cli->Connect(call_options));
        } catch (...) {
            promise->set_exception(std::current_exception());
        }
    }, submitted);
    if (!submitted)
        return fw::result<std::shared_ptr<inf::CoRedisCli>>::err(
            fw::MakeError(inf::ErrorCode::Unavailable,
                "redis connect task registration failed"));
    if (done.wait_for(kWait) != std::future_status::ready)
        return fw::result<std::shared_ptr<inf::CoRedisCli>>::err(
            fw::MakeError(inf::ErrorCode::TimedOut,
                "redis connect wait timed out"));
    auto connected = done.get();   // 协程异常经 future 重新抛出
    if (!connected)
        return inf::result<std::shared_ptr<inf::CoRedisCli>>::err(
            connected.error());
    delivered = true;
    return created;
}

void RegisterResources(fw::CoApp& app) {
    const auto [rhost, rport] = SplitAddr(EnvOr("BBT_TEST_REDIS_ADDR"));
    inf::RedisClientConfig rcfg;
    rcfg.host = rhost;
    rcfg.port = rport;
    rcfg.max_inflight = 256;
    rcfg.max_queue    = 256;
    BOOST_REQUIRE(app.add_resource<inf::CoRedisCli>("cache",
        [rcfg] { return MakeRedis(rcfg); }));

    inf::MongoClientConfig mcfg;
    mcfg.uri                      = EnvOr("BBT_TEST_MONGO_URI");
    mcfg.database                 = "bbt_f8_combined";
    mcfg.collection               = "kv";
    mcfg.worker_threads           = 2;
    mcfg.max_queue                = 256;
    mcfg.server_selection_timeout = std::chrono::milliseconds{5000};
    mcfg.connect_timeout          = std::chrono::milliseconds{5000};
    mcfg.socket_timeout           = std::chrono::milliseconds{5000};
    mcfg.wait_queue_timeout       = std::chrono::milliseconds{5000};
    BOOST_REQUIRE(app.add_resource<inf::CoMongoCli>("docs",
        [mcfg] { return inf::CoMongoCli::Create(mcfg); }));
}

// 起真实 HTTP app：公开 CoApp 构造 + 资源装配 + KV service，等 bound_endpoint
// 就绪，再建测试侧 client runtime。
CombinedBox StartCombined(std::chrono::milliseconds incoming_timeout,
                          std::chrono::milliseconds shutdown_budget) {
    CombinedBox box;
    fw::CoAppOptions opts;
    opts.network_limits       = Limits(incoming_timeout);
    opts.listen               = inf::ListenAddress{"127.0.0.1", 0};
    opts.shutdown_step_budget = shutdown_budget;
    box.app = std::make_unique<fw::CoApp>(opts);
    BOOST_REQUIRE(box.app->add_service<KvSvc>(fw::ServiceOptions{
        fw::ExecutionPolicy::Concurrent, 64, 0, 0, false, 0, 0, 0}));
    RegisterResources(*box.app);

    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
    auto run = std::make_unique<RunHandle>();
    run->app = box.app.get();
    RunHandle* rp = run.get();
    run->th = std::thread([rp, a = box.app.get()] {
        rp->rc = a->run();
        rp->done.store(true, std::memory_order_release);
    });
    box.run = std::move(run);

    const auto until = Clock::now() + kWait;
    while (box.endpoint.empty()) {
        BOOST_REQUIRE_MESSAGE(!box.run->done.load(),
            "app.run exited before listen, rc=" << box.run->rc);
        BOOST_REQUIRE(Clock::now() < until);
        box.endpoint = box.app->bound_endpoint();
        std::this_thread::yield();
    }
    BOOST_REQUIRE(box.app->shutdown_state() == fw::ShutdownState::Running);

    auto crt = inf::NetworkRuntime::Create(Limits(incoming_timeout));
    BOOST_REQUIRE(crt);
    box.client_rt = crt.value();
    BOOST_REQUIRE(box.client_rt->Start());
    auto cl = box.client_rt->CreateHttpClient();
    BOOST_REQUIRE(cl);
    box.client = cl.value();
    return box;
}

namespace Wire = fw::http_bridge;

inf::RpcEnvelope MakeEnv(const char* service, const char* method,
                         std::string rid, const fw::CoRpcReq& req) {
    inf::RpcEnvelope env;
    env.service         = service;
    env.method          = method;
    env.request_id      = std::move(rid);
    env.request_schema  = std::string(fw::kCoRpcPositionalSchema);
    env.response_schema = std::string(fw::kCoRpcPositionalSchema);
    env.payload         = req.payload();
    return env;
}

fw::result<inf::RpcEnvelope> SendRpc(
    const std::shared_ptr<inf::HttpClient>& client,
    const std::string& endpoint, const inf::RpcEnvelope& env,
    std::chrono::milliseconds budget) {
    inf::CallOptions o;
    o.deadline = Clock::now() + budget;
    auto res = client->Request(
        Wire::ToHttpRequest(env, "http://" + endpoint + "/rpc"), o);
    if (!res) return fw::result<inf::RpcEnvelope>::err(std::move(res.error()));
    return Wire::EnvelopeFromResponse(res.value());
}

// 控制线程发起一次真实 HTTP RPC 并限时等待；协程内异常也通知等待者。
fw::result<inf::RpcEnvelope> CallRpc(
    const CombinedBox& box, const inf::RpcEnvelope& env,
    std::chrono::milliseconds budget = kWait) {
    struct State {
        std::optional<fw::result<inf::RpcEnvelope>> out;
        std::exception_ptr                          eptr;
        TestLatch                                   done{1};
    };
    auto st = std::make_shared<State>();
    // client/endpoint 按值捕获：超预算提前返回时，在途协程不持有测试栈对象。
    auto client = box.client;
    auto endpoint = box.endpoint;
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [st, client, endpoint, env, budget] {
            try {
                st->out.emplace(SendRpc(client, endpoint, env, budget));
            } catch (...) {
                st->eptr = std::current_exception();
            }
            st->done.CountDown();
        },
        succ);
    if (!succ || !st->done.WaitFor(budget))
        return fw::result<inf::RpcEnvelope>::err(fw::MakeError(
            fw::ErrorCode::InternalError, "CallRpc did not complete"));
    if (st->eptr) std::rethrow_exception(st->eptr);
    return std::move(*st->out);
}

// 受管协程内执行任意返回 result 的调用（资源命令只能在协程上下文调用）。
template <class Fn>
auto RunInCoroutine(Fn&& fn, std::chrono::milliseconds budget = kWait)
    -> std::optional<decltype(fn())> {
    using Out = decltype(fn());
    struct State {
        std::optional<Out> out;
        std::exception_ptr eptr;
        TestLatch          done{1};
    };
    auto st = std::make_shared<State>();
    auto f  = std::decay_t<Fn>(std::forward<Fn>(fn));
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [st, f] {
            try { st->out.emplace(f()); }
            catch (...) { st->eptr = std::current_exception(); }
            st->done.CountDown();
        },
        succ);
    if (!succ || !st->done.WaitFor(budget)) return std::nullopt;
    if (st->eptr) std::rethrow_exception(st->eptr);
    return std::move(st->out);
}

// 解码 (int32, string) 回复。
struct Reply2 {
    std::int32_t status{0};
    std::string  value;
};
fw::result<Reply2> Decode2(const fw::result<inf::RpcEnvelope>& r) {
    if (!r) return fw::result<Reply2>::err(r.error());
    auto d = fw::CoRpcReq(r.value().payload).Parse<std::int32_t, std::string>();
    if (!d) return fw::result<Reply2>::err(d.error());
    return fw::result<Reply2>::ok(Reply2{std::get<0>(d.value()),
                                         std::get<1>(d.value())});
}

fw::CoRpcReq Req1(std::string a) {
    auto req = fw::CoRpcReq::From(std::move(a));
    if (!req) throw std::logic_error("encode failed");
    return std::move(req).value();
}
fw::CoRpcReq Req2(std::string a, std::string b) {
    auto req = fw::CoRpcReq::From(std::tuple{std::move(a), std::move(b)});
    if (!req) throw std::logic_error("encode failed");
    return std::move(req).value();
}

KvSvc* SvcOf(fw::CoApp& app) {
    auto s = app.find_service("kv");
    BOOST_REQUIRE(s);
    return static_cast<KvSvc*>(s.value().get());
}

BOOST_AUTO_TEST_SUITE(framework_resource_combined)

// C1：真实 HTTP→Service→Redis/Mongo 联合业务 + 持久化独立证明 + 错误原样传播。
BOOST_AUTO_TEST_CASE(combined_cache_mongo_business) {
    auto box = StartCombined(/*incoming*/std::chrono::milliseconds{30000},
                             /*shutdown*/std::chrono::milliseconds{2000});
    const std::string prefix = RunPrefix();
    const std::string id     = prefix + "k1";
    auto* svc = SvcOf(*box.app);

    auto call = [&box](const char* method, const fw::CoRpcReq& req)
        -> fw::result<inf::RpcEnvelope> {
        return CallRpc(box, MakeEnv("kv", method,
            std::string("c1-") + method, req));
    };

    // (1) not-found：cache 空 + Mongo 无文档。
    {
        auto r = call("get", Req1(id));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_EQUAL(rep.value().status, 1);          // NOTFOUND
    }
    const int reads_after_miss = svc->mongo_reads.load();

    // (2) 落 Mongo 持久文档。
    {
        auto r = call("insert", Req2(id, "v1"));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_EQUAL(rep.value().value, "v1");
    }

    // (3) miss→Mongo 查询→写 cache：值来自 Mongo，Mongo 读数 +1。
    {
        auto r = call("get", Req1(id));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_EQUAL(rep.value().status, 0);
        BOOST_CHECK_EQUAL(rep.value().value, "v1");
    }
    const int reads_after_fill = svc->mongo_reads.load();
    BOOST_CHECK_EQUAL(reads_after_fill, reads_after_miss + 1);

    // (4) hit：再读同键直接命中 cache，Mongo 读数不再增长（可观察，非猜测）。
    {
        auto r = call("get", Req1(id));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_EQUAL(rep.value().status, 0);
        BOOST_CHECK_EQUAL(rep.value().value, "v1");
    }
    BOOST_CHECK_EQUAL(svc->mongo_reads.load(), reads_after_fill);

    // (5) 更新：先落 Mongo（matched=1），再失效/刷新 Redis。
    {
        auto r = call("update", Req2(id, "v2"));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_EQUAL(rep.value().status, 1);          // matched
        BOOST_CHECK_EQUAL(rep.value().value, "v2");
    }

    // (6) 再读得到新值（cache 已刷新）；Mongo 读数仍不增。
    {
        auto r = call("get", Req1(id));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_EQUAL(rep.value().status, 0);
        BOOST_CHECK_EQUAL(rep.value().value, "v2");
    }
    BOOST_CHECK_EQUAL(svc->mongo_reads.load(), reads_after_fill);

    // (7) 持久化独立证明：强制 cache miss（真实 DEL）后从 Mongo 读回 v2，
    //     且 Mongo 读数 +1。仅靠 matched=1 + 已刷新的 cache 不能证明持久化。
    auto cache = svc->Cache();
    BOOST_REQUIRE(cache);
    auto del = RunInCoroutine([cache, &id] {
        inf::CallOptions o{Clock::now() + kWait};
        return cache->Delete({"kv:" + id}, o);
    });
    BOOST_REQUIRE(del);
    BOOST_CHECK_EQUAL(del.value().value(), std::uint64_t{1});  // 键真实存在并被删
    const int reads_before_reread = svc->mongo_reads.load();
    {
        auto r = call("get", Req1(id));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_EQUAL(rep.value().status, 0);
        BOOST_CHECK_EQUAL(rep.value().value, "v2");         // 来自 Mongo，非 cache
    }
    BOOST_CHECK_EQUAL(svc->mongo_reads.load(), reads_before_reread + 1);

    // (8) 真实后端错误原样传播：重复 _id 插入 → RemoteError/DuplicateKey。
    {
        auto r = call("insert", Req2(id, "v3"));
        BOOST_REQUIRE(!r);
        BOOST_CHECK_EQUAL(static_cast<int>(r.error().code),
                          static_cast<int>(fw::ErrorCode::RemoteError));
        BOOST_CHECK_EQUAL(r.error().domain_code, "DuplicateKey");
    }

    // 关闭：资源同步收束到 IsClosed 终态。
    auto docs = svc->Docs();
    BOOST_REQUIRE(docs);

    box.app->request_shutdown();
    box.Stop();
    BOOST_CHECK_EQUAL(box.run->rc, fw::HostLifecycle::kExitOk);
    BOOST_CHECK(cache->IsClosed());
    BOOST_CHECK(docs->IsClosed());
}

// C2：有限 deadline（派发前过期）——handler 取 context().request()->deadline
// 下传资源；父预算耗尽后资源调用在 I/O 前 TimedOut，且后端无副作用。
// 边界：这是过期请求派发前被拒，不是底层 I/O 真实在途超时（后者见 C4a/C4b）。
BOOST_AUTO_TEST_CASE(combined_finite_deadline_propagation) {
    auto box = StartCombined(/*incoming*/std::chrono::milliseconds{700},
                             /*shutdown*/std::chrono::milliseconds{2000});

    // handler 观察到的是有限父预算（非 Deadline::max()）。
    {
        auto r = CallRpc(box, MakeEnv("kv", "probe_deadline", "c2-p",
                                      Req1("x")));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_GT(rep.value().status, 0);
        BOOST_CHECK_LE(rep.value().status, 700);
    }

    // 过期请求：Set 期望 TimedOut（handler 正确下传才失败）。
    const std::string id = RunPrefix() + "exp";
    {
        auto r = CallRpc(box, MakeEnv("kv", "expired_set", "c2-x",
                                      Req2(id, "v")),
                         std::chrono::milliseconds{5000});
        BOOST_REQUIRE(!r);
        BOOST_CHECK_EQUAL(static_cast<int>(r.error().code),
                          static_cast<int>(fw::ErrorCode::TimedOut));
    }
    // 无副作用：过期 Set 未触达后端，键不存在。
    {
        auto r = CallRpc(box, MakeEnv("kv", "exists", "c2-e", Req1(id)));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_EQUAL(rep.value().status, 0);
    }

    // 派发前过期不判连接失败：连接保持健康（infra 65a0a CoRedisCliImpl.cc:674/688
    // 「保持健康连接可复用」）。与 C4b 的在途失败（ConnectStatus==Failed）判别。
    auto cache = SvcOf(*box.app)->Cache();
    BOOST_REQUIRE(cache);
    BOOST_CHECK(cache->ConnectStatus() == inf::ConnectState::Connected);

    box.app->request_shutdown();
    box.Stop();
    BOOST_CHECK_EQUAL(box.run->rc, fw::HostLifecycle::kExitOk);
}

// C3：关闭中在途请求——真实 HTTP handler 未排空 → ShutdownIncomplete（资源尚未
// Close）→ 放行后同步等客户端明确回包 → 完整排空 → kExitShutdownLate。
BOOST_AUTO_TEST_CASE(combined_shutdown_drains_inflight) {
    auto box = StartCombined(/*incoming*/std::chrono::milliseconds{10000},
                             /*shutdown*/std::chrono::milliseconds{300});
    auto* svc = SvcOf(*box.app);
    auto gate     = std::make_shared<CoGate>();
    auto arrived  = std::make_shared<TestLatch>(1);
    auto released = std::make_shared<TestLatch>(1);
    // 显式测试钩子：闸门由宿主测试装配持有/放行（业务实现不持全局测试状态）。
    svc->hold_gate = [gate, arrived, released] {
        arrived->CountDown();                     // handler 已到闸门（在途）
        gate->Wait(Clock::now() + std::chrono::milliseconds{15000});
        released->CountDown();                    // 放行后 handler 继续产出回复
    };
    // RAII：任何早退/断言失败都先放行闸门，避免排空窗口上悬挂。
    struct ReleaseGuard {
        std::shared_ptr<CoGate> gate;
        ~ReleaseGuard() { if (gate) gate->Open(); }
    } release{gate};

    // 在途响应 sink：显式等待并断言客户端得到**明确结果**（完整回包或契约允许
    // 的传输错误）——删掉发送/响应即失败，不能只发不等仍绿。
    auto sink      = std::make_shared<std::optional<fw::result<inf::RpcEnvelope>>>();
    auto sink_done = std::make_shared<TestLatch>(1);
    auto client    = box.client;
    auto endpoint  = box.endpoint;
    auto hold_env  = MakeEnv("kv", "hold", "c3-h", Req1("h"));
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [client, endpoint, sink, sink_done, hold_env] {
            try {
                sink->emplace(SendRpc(client, endpoint, hold_env, kWait));
            } catch (...) {
                // 保持等待者必被通知；异常时 sink 为空，由下方断言捕获。
            }
            sink_done->CountDown();
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(arrived->WaitFor(kWait));   // handler 已在闸门上（真实 HTTP 在途）

    auto cache = svc->Cache();
    auto docs  = svc->Docs();
    BOOST_REQUIRE(cache);
    BOOST_REQUIRE(docs);

    const auto shutdown_req_at = Clock::now();
    box.app->request_shutdown();
    const auto until = Clock::now() + kWait;
    while (box.app->shutdown_state() != fw::ShutdownState::ShutdownIncomplete) {
        BOOST_REQUIRE(Clock::now() < until);
        std::this_thread::yield();
    }
    const auto incomplete_at = Clock::now();
    const auto pending = box.app->pending_cleanup();
    BOOST_REQUIRE_EQUAL(pending.size(), std::size_t{1});
    BOOST_CHECK_EQUAL(pending[0], "WaitHandlersDone");
    BOOST_CHECK(!box.run->done.load());
    // ShutdownIncomplete（handler 未排空）期间资源尚未 Close。
    BOOST_CHECK(!cache->IsClosed());
    BOOST_CHECK(!docs->IsClosed());

    gate->Open();                              // 迟到排空
    BOOST_REQUIRE(released->WaitFor(kWait));   // 放行后 handler 真实继续执行
    const auto released_at = Clock::now();
    BOOST_REQUIRE(sink_done->WaitFor(kWait));  // 同步等客户端明确结果（不悬挂）
    BOOST_REQUIRE(sink->has_value());          // 必须有明确回包/错误，不接受“无结果”静默

    box.Stop();
    // ── 计时证据：本请求 incoming 预算 10000ms，关闭步预算 300ms；放行发生在
    //    incoming 到期之前。注意「未过期」只说明 handler 侧预算未尽，并不等于
    //    Close 后必达（见下方契约判定）。
    BOOST_TEST_MESSAGE("C3 计时：shutdown→ShutdownIncomplete="
        << std::chrono::duration_cast<std::chrono::milliseconds>(
               incomplete_at - shutdown_req_at).count()
        << "ms, release→client结果="
        << std::chrono::duration_cast<std::chrono::milliseconds>(
               released_at - incomplete_at).count()
        << "ms, incoming预算=10000ms（未到期）");
    BOOST_CHECK_EQUAL(box.run->rc, fw::HostLifecycle::kExitShutdownLate);
    BOOST_CHECK(box.app->shutdown_state() == fw::ShutdownState::Closed);
    BOOST_CHECK(box.app->pending_cleanup().empty());
    BOOST_CHECK(cache->IsClosed());
    BOOST_CHECK(docs->IsClosed());

    // ── 客户端可见结果门禁（按契约，不按“允许任意错误”放宽）──
    // 该请求 incoming 预算 10000ms、关闭步预算 300ms：放行发生在 incoming 到期
    // 之前。但「未过期」不等于「Close 后必达」：InfraHttpHost.hpp:9-11 明文规定
    // 「Close 开始后仍在途且尚未交给传输层的回复不承诺送达原客户端」；infra
    // HttpServer Close 亦明确丢弃未发送数据。故客户端结果只接受契约允许的两种：
    //   (a) 完整 HELD —— 回复在 Close 生效前已交给传输层送达；
    //   (b) TransportError(code=8) —— Close 开始后在途、尚未交给传输层而被截断。
    // 空结果或其它未知错误仍判失败（不静默、不放宽为任意错误）。
    const auto& outcome = sink->value();
    if (!outcome) {
        const int code = static_cast<int>(outcome.error().code);
        BOOST_CHECK_MESSAGE(
            code == static_cast<int>(fw::ErrorCode::TransportError),
            "C3：客户端结果既非完整 HELD 也非契约允许的 TransportError"
            "（拒绝空结果/未知错误）: code=" << code
            << " msg=\"" << outcome.error().message << "\"");
        BOOST_TEST_MESSAGE("C3：Close 开始后未交给传输层的回复按契约被截断 → "
            "TransportError code=" << code
            << " msg=\"" << outcome.error().message << "\"");
    } else {
        auto rep = Decode2(outcome);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_EQUAL(rep.value().status, 0);
        BOOST_CHECK_EQUAL(rep.value().value, "HELD");
    }
}

// C4a：后端 I/O 在途（服务端停顿）——同一自有实例 CLIENT PAUSE 后，业务 Get
// 在 cache 命中路径上被真实服务端扣住 ~pause 才返回正确值：命令确已发出并被
// 服务端执行（在途证据；若未发出不可能返回该值，也不以单纯 TimedOut 反证）。
BOOST_AUTO_TEST_CASE(combined_backend_inflight_held_by_pause) {
    BOOST_REQUIRE_MESSAGE(!RedisCliPath().empty(),
        "需要 BBT_TEST_REDIS_CLI（redis-cli 路径由环境/runner 传入）");
    auto box = StartCombined(/*incoming*/std::chrono::milliseconds{6000},
                             /*shutdown*/std::chrono::milliseconds{2000});
    auto* svc = SvcOf(*box.app);
    const std::string id = RunPrefix() + "inflight";
    auto call = [&box](const char* method, const fw::CoRpcReq& req)
        -> fw::result<inf::RpcEnvelope> {
        return CallRpc(box, MakeEnv("kv", method,
            std::string("c4a-") + method, req));
    };

    // 预热：Mongo 落库 + 首次 get 填充 cache（此后为 cache 命中路径）。
    {
        auto r = call("insert", Req2(id, "v1"));
        BOOST_REQUIRE(r);
    }
    {
        auto r = call("get", Req1(id));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_EQUAL(rep.value().value, "v1");
    }
    auto cache = svc->Cache();
    BOOST_REQUIRE(cache);
    BOOST_REQUIRE(cache->ConnectStatus() == inf::ConnectState::Connected);

    // 真实服务端停顿：普通 GET 被停住（ALL）。
    auto pause = RunRedisCli({"CLIENT", "PAUSE", "1500", "ALL"});
    BOOST_REQUIRE_MESSAGE(pause.rc == 0 &&
        pause.out.find("OK") != std::string::npos,
        "CLIENT PAUSE 失败: rc=" << pause.rc << " out=" << pause.out);

    const auto t0 = Clock::now();
    auto r = call("get", Req1(id));    // 命中路径：单条真实 Redis GET
    const auto held_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now() - t0).count();
    BOOST_REQUIRE(r);
    auto rep = Decode2(r);
    BOOST_REQUIRE(rep);
    BOOST_CHECK_EQUAL(rep.value().status, 0);
    BOOST_CHECK_EQUAL(rep.value().value, "v1");
    BOOST_CHECK_MESSAGE(held_ms >= 1000,
        "Get 未被服务端 pause 扣住（held=" << held_ms << "ms），在途证据不成立");
    BOOST_CHECK_LT(held_ms, 5000);
    BOOST_CHECK(cache->ConnectStatus() == inf::ConnectState::Connected);

    box.app->request_shutdown();
    box.Stop();
    BOOST_CHECK_EQUAL(box.run->rc, fw::HostLifecycle::kExitOk);
}

// C4b：后端 I/O 在途 deadline——短父预算 < pause；命令已写出、ReadReply 等待时
// deadline 先到 → 真实在途 TimedOut；连接被判不可复用（与 C2 派发前保留健康连接
// 判别）；恢复由测试装配控制层显式 Connect（不 Service 重连、不自动重试）。
BOOST_AUTO_TEST_CASE(combined_backend_inflight_deadline) {
    BOOST_REQUIRE_MESSAGE(!RedisCliPath().empty(),
        "需要 BBT_TEST_REDIS_CLI（redis-cli 路径由环境/runner 传入）");
    auto box = StartCombined(/*incoming*/std::chrono::milliseconds{900},
                             /*shutdown*/std::chrono::milliseconds{2000});
    auto* svc = SvcOf(*box.app);
    auto cache = svc->Cache();
    BOOST_REQUIRE(cache);
    BOOST_REQUIRE(cache->ConnectStatus() == inf::ConnectState::Connected);

    auto pause = RunRedisCli({"CLIENT", "PAUSE", "1500", "ALL"});
    BOOST_REQUIRE_MESSAGE(pause.rc == 0 &&
        pause.out.find("OK") != std::string::npos,
        "CLIENT PAUSE 失败: rc=" << pause.rc << " out=" << pause.out);

    auto r = CallRpc(box, MakeEnv("kv", "ping", "c4b-p", Req1("x")),
                     std::chrono::milliseconds{5000});
    BOOST_REQUIRE(!r);
    BOOST_CHECK_EQUAL(static_cast<int>(r.error().code),
                      static_cast<int>(fw::ErrorCode::TimedOut));
    // 在途证据（区别于派发前过期）：ReadReply 超时走 FailConnAndDrainQueue
    // （infra 65a0a CoRedisCliImpl.cc:710-714）→ 连接判不可复用 → Failed；
    // 派发前过期在 :674/:688 返回且保留健康连接（C2 已断言 Connected）。
    BOOST_CHECK_MESSAGE(cache->ConnectStatus() == inf::ConnectState::Failed,
        "期望连接被判不可复用(Failed)，实际="
            << inf::ConnectStateName(cache->ConnectStatus()));

    // 等 pause 结束：用同一实例的真实命令做确定性屏障（非固定 sleep）。
    auto barrier = RunRedisCli({"PING"});
    BOOST_CHECK_EQUAL(barrier.rc, 0);

    // 恢复：测试装配控制层显式 Connect（不 Service 重连、不自动重试、
    // 不放宽 Error、不宣称逻辑 timeout 等于远端未执行）。
    auto conn = RunInCoroutine([cache] {
        inf::CallOptions o{Clock::now() + std::chrono::milliseconds{5000}};
        return cache->Connect(o);
    });
    BOOST_REQUIRE(conn.has_value());                 // 协程已返回
    BOOST_REQUIRE(static_cast<bool>(*conn));         // Connect 成功
    BOOST_CHECK(cache->ConnectStatus() == inf::ConnectState::Connected);

    auto ok = CallRpc(box, MakeEnv("kv", "ping", "c4b-ok", Req1("x")));
    BOOST_REQUIRE(ok);
    auto rep = Decode2(ok);
    BOOST_REQUIRE(rep);
    BOOST_CHECK_EQUAL(rep.value().status, 0);
    BOOST_CHECK_EQUAL(rep.value().value, "PONG");

    box.app->request_shutdown();
    box.Stop();
    BOOST_CHECK_EQUAL(box.run->rc, fw::HostLifecycle::kExitOk);
}

BOOST_AUTO_TEST_SUITE_END()
} // namespace
