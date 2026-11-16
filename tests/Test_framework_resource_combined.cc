/**
 * 联合 live 验收：真实 TCP V2 会话 → 框架入站 → 命名资源业务 Service
 * → 真实 Redis + MongoDB 联合消费。
 *
 * 与单资源 live 的差别（不得以单资源 live 替代联合验收）：
 * - 宿主是框架公开构造 CoApp::Create(AppOptions) 默认装配的真实
 *   InfraHttpHost + InstallRpcAdmission 入站接线，不是 NullNetHost/
 *   DispatchInboundForTest 测试缝；请求经真实 TCP V2 会话
 *   （SessionFrame + Hello/HelloAck）进入框架再分发到 Service。
 * - 业务 Service（KvSvc）在独立 TU framework_resource_service.{hpp,cc}
 *   中实现，只 include framework/infra/coroutine 公开头；本文件只作为
 *   **测试宿主**另持 internal（V2 出站接线 + seam）。业务实现与宿主
 *   分属不同编译单元——不靠「同一 TU 先 include 公共头」的标签声称
 *   隔离。
 *
 * 生命周期范式（迁移语义，无 run()/HostLifecycle）：测试进程自带
 * coroutine runtime；Create/装配/CoStart/CoWaitShutdownRequest/CoStop
 * 全部在受管协程内执行（RunInManagedCoroutine 投递），外部线程
 * RequestShutdown 唤醒等待。
 *
 * 覆盖（最小业务链路）：
 * C1 联合业务：cache miss→Mongo 查询→写 cache；hit（经 Service 侧 Mongo
 *   读数计数可观察，不靠猜测）；更新先落 Mongo 再失效/刷新 Redis；强制
 *   cache miss 后从 Mongo 读回新值（独立证明持久化，非复用已刷新的
 *   cache）；not-found；真实后端错误原样传播（Mongo duplicate key →
 *   RemoteError/DuplicateKey）。
 * C2 有限 deadline（派发前过期）：handler 取 CurrentRequestContext()
 *   的 deadline 下传 infra::CallOptions；父预算耗尽后发起资源调用 →
 *   infra 在发起 I/O 前返回 TimedOut，且后端无副作用（键未写入，可
 *   观察），连接保持健康。
 * C3 关闭中在途请求：真实 V2 handler 停在宿主注入的闸门上 →
 *   RequestShutdown → CoStop 预算耗尽 → StopIncomplete + 排空失败记录、
 *   资源尚未收口（borrow 仍活、真实命令仍通）→ 放行后 handler 在
 *   StopIncomplete 期间经组件借用取 Redis/Mongo 执行真实操作成功 →
 *   同步等客户端明确结果 → 再次 CoStop 推进完整收口 → Stopped；
 *   最终对象销毁后 Service 句柄失效。客户端结果按 InfraHttpHost.hpp
 *   「Close 开始后尚未交给传输层的回复不承诺送达」契约判定：只接受
 *   「完整 HELD」或「TransportError(code=8)」，不接受空结果/未知
 *   错误（未过期不等于 Close 后必达）。观察者由外部状态独立持有，
 *   测试不持 Service 实例（新模型无公开实例出口）。
 * C4a 后端 I/O 在途（服务端停顿）：同一自有实例 CLIENT PAUSE 1500 ALL
 *   后，业务 Get 在 cache 命中路径上被服务端**真实扣住**约 1500ms
 *   才返回正确值——命令确已发出并被服务端执行（在途证据，非派发前
 *   过期）。
 * C4b 后端 I/O 在途 deadline：短父预算 < pause；命令已写出、等待
 *   回复时 deadline 先到 → 真实在途 TimedOut；新入口把该流退役为
 *   broken（写已接纳后 deadline 同样走 Fail，后续命令报
 *   「explicit Connect required」，不经 ConnectStatus 暴露——由后续
 *   Ping 的如实失败证明，与 C2 派发前保留健康连接形成判别）；恢复
 *   由测试装配控制层显式 Connect（不 Service 重连、不自动重试）。
 *
 * 环境变量（缺前两项任一 → 进程 exit 77 标 Skipped）：
 * BBT_TEST_REDIS_ADDR=host:port
 * BBT_TEST_MONGO_URI=mongodb://h:p/?directConnection=true
 * BBT_TEST_REDIS_CLI=/abs/path/redis-cli  （仅 C4a/C4b 需要；路径由
 * 环境/runner 传入，仓内不写私有路径）
 */
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
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

#include <bbt/infra/net/Address.hpp>

/**
 * ── 业务 Service：独立 TU，仅 framework/infra/coroutine 公开头 ──
 */
#include "framework_resource_service.hpp"

/**
 * ── 测试宿主面（internal 允许）：V2 出站接线 ──
 */
#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>
#include <bbt/framework/internal/RpcBridge.hpp>

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

/**
 * 缺环境必须显式标记 Skipped（CTest SKIP_RETURN_CODE 77）：联合 target 要求
 * Redis 与 Mongo 同时可用，缺任一即 skip，不以退出码 0 冒充通过。
 */
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

/**
 * 每次运行唯一的前缀：后端是保留状态的容器，避免与上次运行的文档/LRU 冲突。
 */
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

/**
 * ── 协程闸门（CoWaiter 单次放行；无 sleep 凑时序）──
 */
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

/**
 * ── redis-cli（路径由环境/runner 传入；仅用于对同一自有实例造真实服务端停顿）──
 */
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

/**
 * ── 测试侧借用服务：声明集合含两类资源但不声明 BindComponent——
 * 用例在运行期经 CoBindComponent 生成 binding（同 subject 同组件的
 * 声明式绑定被 duplicate binding 拒绝，KvSvc 的声明式绑定已占边，
 * 故测试借用必须走独立 subject）。借出的 ComponentBorrow 持有对象
 * 版本 lease：跨 CoStop 持有即「资源未收口」的正面证据（borrow 排空
 * 先于 OnStop，owner 保留落 StopIncomplete），同时保活对象本体供
 * IsClosed/engine_stopped/coll_closed 观测口读取。──
 */
class CombinedPeek final : public fw::CoService<fw::RedisNetResource,
                                                fw::MongoDbResource> {
public:
    static constexpr std::string_view kServiceName = "peek";
    explicit CombinedPeek(fw::detail::ConstructKey k)
        : fw::CoService<fw::RedisNetResource, fw::MongoDbResource>(k) {}
protected:
    ~CombinedPeek() noexcept override = default;
private:
    friend class fw::detail::Registry;
};

fw::ServiceOptions ConcurrentOpts() {
    return fw::ServiceOptions{}
        .SetExecution(fw::ExecutionPolicy::Concurrent)
        .SetMaxInflight(64);
}

/**
 * ── 宿主 + 测试侧 V2 客户端 runtime。CoApp 用公开 Create（默认真实 V2
 * 装配）；app 生命周期任务在受管协程内执行。──
 */
inf::NetworkLimits Limits(std::chrono::milliseconds incoming_timeout) {
    return inf::NetworkLimits{64, 64, 16384, 65536, incoming_timeout};
}

fw::AppOptions AppOpts(std::chrono::milliseconds incoming_timeout,
                       std::chrono::milliseconds shutdown_budget) {
    return fw::AppOptions{}
        .SetLimits(64, 64, 64, 64)
        .SetShutdownBudget(shutdown_budget)
        .SetLifecycleBudget(std::chrono::milliseconds{30000})
        .SetNetworkLimits(Limits(incoming_timeout))
        .Listen("127.0.0.1:0")
        .SetLocalIdentity("combined-server");
}

/**
 * 在受管协程内执行 fn：经 g_scheduler 投递协程任务，宿主线程用限时
 * std::cv 等其完成（宿主线程的 CV 等待不违反 worker 阻塞禁令）。
 * 返回 false 表示任务未在预算内完成（防挂死测试进程）。
 */
template <class F>
bool RunInManagedCoroutine(F&& fn,
    std::chrono::milliseconds budget = std::chrono::milliseconds{30000}) {
    if (bbt::coroutine::IsInCoroutineContext()) { fn(); return true; }
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 5;
        /**
         * 嵌套出站链的协程栈需求（infra hiredis/asio 出站命令）。
         */
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
        g_scheduler->Start();
    }
    struct Slot {
        std::mutex              mtx;
        std::condition_variable cv;
        bool                    done = false;
        std::exception_ptr      error;
    };
    auto slot = std::make_shared<Slot>();
    auto f = std::forward<F>(fn);
    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [slot, f = std::move(f)]() mutable {
            try { f(); } catch (...) { slot->error = std::current_exception(); }
            std::lock_guard<std::mutex> lk(slot->mtx);
            slot->done = true;
            slot->cv.notify_all();
        }, submitted);
    if (!submitted) throw std::runtime_error("test coroutine submission rejected");
    std::unique_lock<std::mutex> lk(slot->mtx);
    const bool done = slot->cv.wait_for(lk, budget, [&] { return slot->done; });
    if (!done)
        std::terminate();
    if (slot->error) std::rethrow_exception(slot->error);
    return done;
}

struct CombinedBox {
    std::shared_ptr<fw::CoApp>                                   app;
    fw::ObjectHandle                                             svc_handle;
    std::shared_ptr<fw::InfraHttpHost>                           client_rt;
    std::shared_ptr<fw::RpcEgress>                               client;
    std::string                                                  endpoint;
    /**
     * 测试侧组件借用（PeekSvc subject）：持有 lease 即保活资源对象
     * 版本，跨 CoStop 持有阻止收口（如实 StopIncomplete）。
     */
    std::optional<fw::ComponentBorrow<fw::RedisNetResource>>     cache;
    std::optional<fw::ComponentBorrow<fw::MongoDbResource>>      docs;
    std::optional<fw::ComponentHandle<fw::RedisNetResource>>     cache_h;
    std::optional<fw::ComponentHandle<fw::MongoDbResource>>      docs_h;

    CombinedBox() = default;
    ~CombinedBox() { Stop(); }
    CombinedBox(CombinedBox&&)                 = default;
    CombinedBox& operator=(CombinedBox&&)      = default;
    CombinedBox(const CombinedBox&)            = delete;
    CombinedBox& operator=(const CombinedBox&) = delete;

    /**
     * 完整收尾：归还借用 → 外部 RequestShutdown → 受管协程内
     * CoWaitShutdownRequest + CoStop（可带再次推进）→ client runtime
     * 收口。任何一步失败如实断言，不伪造终态。
     */
    void Stop() {
        cache.reset(); docs.reset(); cache_h.reset(); docs_h.reset();
        if (client_rt) client_rt->Close();
        if (app) {
            app->RequestShutdown();
            auto a = app;
            RunInManagedCoroutine([a] {
                bbt::coroutine::WaitOptions wait;
                wait.deadline = Clock::now() + std::chrono::seconds(10);
                auto waited = bbt_co_await a->CoWaitShutdownRequest(wait);
                (void)waited;
                auto stopped = bbt_co_await a->CoStop(
                    fw::LifecycleOptions{}.SetDeadline(
                        Clock::now() + std::chrono::seconds(10)));
                (void)stopped;
            });
        }
    }
};

/**
 * 资源装配（受管协程内调用）：Redis 数值 IP（不解析主机名，含主机名
 * 如实 InvalidArgument）+ Mongo 独占库 bbt_f8_combined 沿用旧口径。
 */
void RegisterResources(const std::shared_ptr<fw::CoApp>& app) {
    const auto [rhost, rport] = SplitAddr(EnvOr("BBT_TEST_REDIS_ADDR"));
    std::error_code addr_ec;
    auto address = inf::net::Address::Ip(rhost, rport, addr_ec);
    BOOST_REQUIRE_MESSAGE(!addr_ec,
        "BBT_TEST_REDIS_ADDR 需为数值 IPv4/IPv6: " << addr_ec.message());
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<fw::RedisNetResource>(
        "cache", fw::RedisNetResource::Options{}.SetAddress(address)));

    inf::mongo::MongoRuntimeConfig rcfg;
    rcfg.uri                      = EnvOr("BBT_TEST_MONGO_URI");
    rcfg.worker_threads           = 2;
    rcfg.max_queue                = 256;
    rcfg.server_selection_timeout = std::chrono::milliseconds{5000};
    rcfg.connect_timeout          = std::chrono::milliseconds{5000};
    rcfg.socket_timeout           = std::chrono::milliseconds{5000};
    rcfg.wait_queue_timeout       = std::chrono::milliseconds{5000};
    const inf::mongo::MongoTarget target{"bbt_f8_combined", "kv"};
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<fw::MongoDbResource>(
        "docs", fw::MongoDbResource::Options{}
                    .SetConfig(rcfg).SetTarget(target)));
}

/**
 * 给 PeekSvc 生成两条 binding 并发放 handle（受管协程内调用）。
 */
void BindPeek(CombinedBox& box) {
    auto peek = box.app->FindService("peek");
    BOOST_REQUIRE(peek);
    auto ch = bbt_co_await box.app->CoBindComponent<fw::RedisNetResource>(
        peek.value(), "cache");
    BOOST_REQUIRE(ch);
    box.cache_h.emplace(std::move(ch).value());
    auto dh = bbt_co_await box.app->CoBindComponent<fw::MongoDbResource>(
        peek.value(), "docs");
    BOOST_REQUIRE(dh);
    box.docs_h.emplace(std::move(dh).value());
    auto cb = box.cache_h->Borrow();
    BOOST_REQUIRE(cb);
    box.cache.emplace(std::move(cb).value());
    auto db = box.docs_h->Borrow();
    BOOST_REQUIRE(db);
    box.docs.emplace(std::move(db).value());
}

/**
 * 起真实 V2 app：公开 CoApp::Create + 资源装配 + KV/Peek service +
 * 显式 Connect（协程上下文）+ 测试侧借用，等 BoundEndpoint 就绪，
 * 再建测试侧 client runtime。
 */
CombinedBox StartCombined(std::chrono::milliseconds incoming_timeout,
                          std::chrono::milliseconds shutdown_budget) {
    CombinedBox box;
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        box.app = fw::CoApp::Create(
            AppOpts(incoming_timeout, shutdown_budget), "combined-server");
        RegisterResources(box.app);
        BOOST_REQUIRE(bbt_co_await box.app->CoInstallService<KvSvc>(
            ConcurrentOpts()
                .BindComponent<fw::RedisNetResource>("cache")
                .BindComponent<fw::MongoDbResource>("docs")));
        BOOST_REQUIRE(bbt_co_await box.app->CoInstallService<CombinedPeek>(
            ConcurrentOpts()));
        BOOST_REQUIRE(bbt_co_await box.app->CoStart(
            fw::LifecycleOptions{}.SetDeadline(
                Clock::now() + std::chrono::seconds(30))));
        BindPeek(box);
        /**
         * 显式 Connect（新入口协程上下文契约）：只有 Connected 实例才允许
         * 后续命令面（ready 先于入站接纳）；deadline 有界、无自动重试。
         */
        inf::CallOptions connect_options{Clock::now() + kWait};
        auto connected = bbt_co_await box.cache->Get().Connect(connect_options);
        BOOST_REQUIRE_MESSAGE(connected,
            "redis explicit Connect 失败: " << connected.error().message);
        box.svc_handle = box.app->FindService("kv").value();
        box.endpoint = box.app->BoundEndpoint();
    }));
    BOOST_REQUIRE(!box.endpoint.empty());
    BOOST_CHECK(box.app->State() == fw::AppState::Running);

    /**
     * 测试客户端是互异 identity 的第二个会话 owner：复用目标服务自己的
     * owner 会被 V2 self-connection 拒绝。
     */
    auto crt = std::make_shared<fw::InfraHttpHost>(
        Limits(incoming_timeout), inf::ListenAddress{"127.0.0.1", 0},
        "combined-client");
    BOOST_REQUIRE(crt);
    box.client_rt = crt;
    BOOST_REQUIRE(box.client_rt->Create());
    BOOST_REQUIRE(box.client_rt->Start());
    BOOST_REQUIRE(box.client_rt->rpc_client() != nullptr);
    box.client = std::make_shared<fw::RpcEgress>(box.client_rt);
    return box;
}

inf::rpc::Request MakeEnv(const char* service, const char* method,
                          std::string rid, const fw::CoRpcReq& req) {
    inf::rpc::Request env;
    env.SetService(service)
        .SetMethod(method)
        .SetRequestId(std::move(rid))
        .SetRequestSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetResponseSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetReply(inf::rpc::ReplyMode::Required)
        .SetTraffic(inf::rpc::TrafficClass::Service)
        .SetPayload(req.payload());
    return env;
}

/**
 * 真实 V2 Call：target 是完整 tcp://host:port，预算由 CallOptions.deadline
 * 唯一承载（同时是客户端等待上限与 wire remaining_budget_ms 来源）。
 */
fw::result<inf::rpc::Response> SendRpc(
    const std::shared_ptr<fw::RpcEgress>& egress,
    const std::string& endpoint, const inf::rpc::Request& env,
    std::chrono::milliseconds budget) {
    inf::CallOptions o;
    o.deadline = Clock::now() + budget;
    return bbt_co_await egress->Call("tcp://" + endpoint, env, o);
}

/**
 * 控制线程发起一次真实 V2 RPC 并限时等待；协程内异常也通知等待者。
 */
fw::result<inf::rpc::Response> CallRpc(
    const CombinedBox& box, const inf::rpc::Request& env,
    std::chrono::milliseconds budget = kWait) {
    struct State {
        std::optional<fw::result<inf::rpc::Response>> out;
        std::exception_ptr                          eptr;
        TestLatch                                   done{1};
    };
    auto st = std::make_shared<State>();
    /**
     * client/endpoint 按值捕获：超预算提前返回时，在途协程不持有测试栈对象。
     */
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
        return fw::result<inf::rpc::Response>::err(fw::MakeError(
            fw::ErrorCode::InternalError, "CallRpc did not complete"));
    if (st->eptr) std::rethrow_exception(st->eptr);
    return std::move(*st->out);
}

/**
 * 受管协程内执行任意返回 result 的调用（资源命令只能在协程上下文调用）。
 */
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

/**
 * 解码 (int32, string) 回复。
 */
struct Reply2 {
    std::int32_t status{0};
    std::string  value;
};
fw::result<Reply2> Decode2(const fw::result<inf::rpc::Response>& r) {
    if (!r) return fw::result<Reply2>::err(r.error());
    auto d = fw::CoRpcReq(r.value().Payload()).Parse<std::int32_t, std::string>();
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

BOOST_AUTO_TEST_SUITE(framework_resource_combined)

/**
 * C1：真实 V2→Service→Redis/Mongo 联合业务 + 持久化独立证明 + 错误原样传播。
 * 关闭断言映射：borrow 持有时 CoStop 落 StopIncomplete（资源未收口、
 * IsClosed==false、真实命令仍通）→ 归还后再次 CoStop 推进至 Stopped、
 * 组件销毁（同步 OnStop 已落定）。新模型无 Service 实例访问口：
 * mongo_reads 计数与借用观察口改为 fixture 静态成员/PeekSvc 绑定。
 */
BOOST_AUTO_TEST_CASE(combined_cache_mongo_business) {
    auto box = StartCombined(/*incoming*/std::chrono::milliseconds{30000},
                             /*shutdown*/std::chrono::milliseconds{300});
    const std::string prefix = RunPrefix();
    const std::string id     = prefix + "k1";

    auto call = [&box](const char* method, const fw::CoRpcReq& req)
        -> fw::result<inf::rpc::Response> {
        return CallRpc(box, MakeEnv("kv", method,
            std::string("c1-") + method, req));
    };

    /**
     * (1) not-found：cache 空 + Mongo 无文档。
     */
    {
        auto r = call("get", Req1(id));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        /**
         * NOTFOUND
         */
        BOOST_CHECK_EQUAL(rep.value().status, 1);
    }
    const int reads_after_miss = KvSvc::s_mongo_reads.load();

    /**
     * (2) 落 Mongo 持久文档。
     */
    {
        auto r = call("insert", Req2(id, "v1"));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_EQUAL(rep.value().value, "v1");
    }

    /**
     * (3) miss→Mongo 查询→写 cache：值来自 Mongo，Mongo 读数 +1。
     */
    {
        auto r = call("get", Req1(id));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_EQUAL(rep.value().status, 0);
        BOOST_CHECK_EQUAL(rep.value().value, "v1");
    }
    const int reads_after_fill = KvSvc::s_mongo_reads.load();
    BOOST_CHECK_EQUAL(reads_after_fill, reads_after_miss + 1);

    /**
     * (4) hit：再读同键直接命中 cache，Mongo 读数不再增长（可观察，非猜测）。
     */
    {
        auto r = call("get", Req1(id));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_EQUAL(rep.value().status, 0);
        BOOST_CHECK_EQUAL(rep.value().value, "v1");
    }
    BOOST_CHECK_EQUAL(KvSvc::s_mongo_reads.load(), reads_after_fill);

    /**
     * (5) 更新：先落 Mongo（matched=1），再失效/刷新 Redis。
     */
    {
        auto r = call("update", Req2(id, "v2"));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        /**
         * matched
         */
        BOOST_CHECK_EQUAL(rep.value().status, 1);
        BOOST_CHECK_EQUAL(rep.value().value, "v2");
    }

    /**
     * (6) 再读得到新值（cache 已刷新）；Mongo 读数仍不增。
     */
    {
        auto r = call("get", Req1(id));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_EQUAL(rep.value().status, 0);
        BOOST_CHECK_EQUAL(rep.value().value, "v2");
    }
    BOOST_CHECK_EQUAL(KvSvc::s_mongo_reads.load(), reads_after_fill);

    /**
     * (7) 持久化独立证明：强制 cache miss（真实 DEL）后从 Mongo 读回 v2，
     * 且 Mongo 读数 +1。仅靠 matched=1 + 已刷新的 cache 不能证明持久化。
     */
    auto del = RunInCoroutine([&box, &id] {
        inf::CallOptions o{Clock::now() + kWait};
        return bbt_co_await box.cache->Get().Delete({"kv:" + id}, o);
    });
    BOOST_REQUIRE(del);
    /**
     * 键真实存在并被删
     */
    BOOST_CHECK_EQUAL(del.value().value(), std::uint64_t{1});
    const int reads_before_reread = KvSvc::s_mongo_reads.load();
    {
        auto r = call("get", Req1(id));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_EQUAL(rep.value().status, 0);
        /**
         * 来自 Mongo，非 cache
         */
        BOOST_CHECK_EQUAL(rep.value().value, "v2");
    }
    BOOST_CHECK_EQUAL(KvSvc::s_mongo_reads.load(), reads_before_reread + 1);

    /**
     * (8) 真实后端错误原样传播：重复 _id 插入 → RemoteError/DuplicateKey。
     */
    {
        auto r = call("insert", Req2(id, "v3"));
        BOOST_REQUIRE(!r);
        BOOST_CHECK_EQUAL(static_cast<int>(r.error().code),
            static_cast<int>(fw::ErrorCode::RemoteError));
        BOOST_CHECK_EQUAL(r.error().domain_code, "DuplicateKey");
    }

    /**
     * 关闭：借用持有期间 CoStop 如实落 StopIncomplete——owner/资源版本
     * 保留（IsClosed 仍为 false、真实命令仍通，不提前收口的正面证据，
     * 即旧「资源仍活跃」断言）；归还借用后再次 CoStop 推进至 Stopped，
     * 组件销毁意味着同步 OnStop 已落定（FindComponent→NotFound）。
     */
    box.app->RequestShutdown();
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        bbt::coroutine::WaitOptions wait;
        wait.deadline = Clock::now() + std::chrono::seconds(10);
        BOOST_REQUIRE(bbt_co_await box.app->CoWaitShutdownRequest(wait));
        BOOST_REQUIRE(bbt_co_await box.app->CoStop());
    }));
    BOOST_CHECK(box.app->State() == fw::AppState::StopIncomplete);
    BOOST_CHECK(!box.cache->Get().IsClosed());
    BOOST_CHECK(!box.docs->Get().IsClosed());
    /**
     * owner 保留窗口的真实命令仍通。
     */
    {
        auto alive = RunInCoroutine([&box] {
            inf::CallOptions o{Clock::now() + std::chrono::milliseconds{5000}};
            return bbt_co_await box.cache->Get().Ping(o);
        });
        BOOST_REQUIRE_MESSAGE(alive.has_value() && *alive,
            "StopIncomplete 期间真实 Ping 应成功（owner 保留）");
    }
    box.cache.reset();
    box.docs.reset();
    box.cache_h.reset();
    box.docs_h.reset();
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        BOOST_REQUIRE(bbt_co_await box.app->CoStop(
            fw::LifecycleOptions{}.SetDeadline(
                Clock::now() + std::chrono::seconds(10))));
    }));
    BOOST_CHECK(box.app->State() == fw::AppState::Stopped);
    BOOST_CHECK(box.app->PendingCleanup().empty());
    {
        auto gone = box.app->FindComponent<fw::RedisNetResource>("cache");
        BOOST_CHECK(!gone);
        auto gone2 = box.app->FindComponent<fw::MongoDbResource>("docs");
        BOOST_CHECK(!gone2);
    }
    if (box.client_rt) box.client_rt->Close();
    box.app.reset();
}

/**
 * C2：有限 deadline（派发前过期）——handler 取 CurrentRequestContext()
 * 的 deadline 下传资源；父预算耗尽后资源调用在 I/O 前 TimedOut，且
 * 后端无副作用。边界：这是过期请求派发前被拒，不是底层 I/O 真实
 * 在途超时（后者见 C4a/C4b）。
 */
BOOST_AUTO_TEST_CASE(combined_finite_deadline_propagation) {
    auto box = StartCombined(/*incoming*/std::chrono::milliseconds{700},
                             /*shutdown*/std::chrono::milliseconds{2000});

    /**
     * handler 观察到的是有限父预算（非 Deadline::max()）。
     */
    {
        auto r = CallRpc(box, MakeEnv("kv", "probe_deadline", "c2-p",
                                      Req1("x")));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_GT(rep.value().status, 0);
        BOOST_CHECK_LE(rep.value().status, 700);
    }

    /**
     * 过期请求：Set 期望在发起 I/O 前被拒。新入口的错误诚实性：deadline 在
     * 写被接纳前到期 → TimedOut（零接纳，无副作用）；写已接纳后到期 →
     * OutcomeUnknown（如实不推断 Redis 未执行）。两路都无「成功但键未写」，
     * 真正副作用判别在下方 exists 断言。
     */
    const std::string id = RunPrefix() + "exp";
    {
        auto r = CallRpc(box, MakeEnv("kv", "expired_set", "c2-x",
                                      Req2(id, "v")),
                         std::chrono::milliseconds{5000});
        BOOST_REQUIRE(!r);
        const int code = static_cast<int>(r.error().code);
        BOOST_CHECK_MESSAGE(code ==
                static_cast<int>(fw::ErrorCode::TimedOut) ||
            code == static_cast<int>(fw::ErrorCode::OutcomeUnknown),
            "C2：过期 Set 应返回 TimedOut 或 OutcomeUnknown，实际=" << code
                << " msg=\"" << r.error().message << "\"");
    }
    /**
     * 无副作用：过期 Set 未触达后端，键不存在。注意 OutcomeUnknown 路径下
     * 连接可能已被退役（写已接纳后到期）——先用管理面探测；若连接已退役
     * 则显式 Connect 重建再发 exists（新入口「explicit Connect required」语义），
     * 不放宽「键不存在」这一副作用判别本身。
     */
    {
        auto probe = RunInCoroutine([&box] {
            inf::CallOptions o{Clock::now() + std::chrono::milliseconds{5000}};
            return bbt_co_await box.cache->Get().Ping(o);
        });
        if (!(probe.has_value() && *probe)) {
            auto conn = RunInCoroutine([&box] {
                inf::CallOptions o{Clock::now() + std::chrono::milliseconds{5000}};
                return bbt_co_await box.cache->Get().Connect(o);
            });
            BOOST_REQUIRE_MESSAGE(conn.has_value() && *conn,
                "C2：连接退役后显式 Connect 重建失败");
        }
    }
    {
        auto r = CallRpc(box, MakeEnv("kv", "exists", "c2-e", Req1(id)));
        BOOST_REQUIRE(r);
        auto rep = Decode2(r);
        BOOST_REQUIRE(rep);
        BOOST_CHECK_EQUAL(rep.value().status, 0);
    }

    /**
     * 过期 Set 不判连接失败：连接经显式 Connect 重建后健康（Ping 成功）；
     * 与 C4b 的在途失败判别（那里 Ping/命令如实为错误路径直到显式 Connect）。
     */
    {
        auto ping = RunInCoroutine([&box] {
            inf::CallOptions o{Clock::now() + std::chrono::milliseconds{5000}};
            return bbt_co_await box.cache->Get().Ping(o);
        });
        BOOST_REQUIRE_MESSAGE(ping.has_value() && *ping,
            "C2：显式 Connect 重建后 Ping 应成功");
    }

    box.Stop();
    BOOST_CHECK(box.app->State() == fw::AppState::Stopped);
}

/**
 * C3：关闭中在途请求——真实 V2 handler 未排空 → CoStop 预算耗尽
 * （shutdown_budget 消费）→ StopIncomplete + 排空失败记录（资源尚未
 * 收口：borrow 仍活、真实命令仍通）→ 放行后 handler 在 StopIncomplete
 * 期间经组件借用取 Redis/Mongo 执行真实操作成功（Service owner 仍
 * 活跃）→ 同步等客户端明确回包 → 归还借用再次 CoStop 推进收口至
 * Stopped；最终对象销毁后 Service 句柄失效。观察者由外部状态独立
 * 持有（Service 钩子捕获其 shared_ptr，lambda 寿命安全）；测试不持
 * Service 实例（新模型无公开实例出口，经静态成员注入）。
 */
BOOST_AUTO_TEST_CASE(combined_shutdown_drains_inflight) {
    auto box = StartCombined(/*incoming*/std::chrono::milliseconds{10000},
                             /*shutdown*/std::chrono::milliseconds{300});

    /**
     * 独立观察者：测试持有 RunState；Service 的两个钩子各捕获其 shared_ptr
     * （即使收口完成、Service 释放，lambda 拷贝仍安全），断言只读
     * RunState，不与关闭释放竞争。
     */
    struct RunState {
        std::shared_ptr<CoGate> gate = std::make_shared<CoGate>();
        TestLatch               arrived{1};
        TestLatch               released{1};
        TestLatch               op_done{1};
        std::atomic<bool>       op_ok{false};
    };
    auto st = std::make_shared<RunState>();

    /**
     * Service 句柄（非拥有）：收口后 IsValid=false 即「实例已释放」证据。
     */
    auto svc_handle = box.svc_handle;

    /**
     * 显式测试钩子：闸门与「放行后真实资源操作完成」报告由宿主测试装配
     * 注入（fixture 静态成员；新模型无 Service 实例访问口）。
     */
    KvSvc::s_hold_gate = [st] {
        /**
         * handler 已到闸门（在途）
         */
        st->arrived.CountDown();
        st->gate->Wait(Clock::now() + std::chrono::milliseconds{15000});
        /**
         * 放行后 handler 继续产出回复
         */
        st->released.CountDown();
    };
    KvSvc::s_hold_op_report = [st](bool ok) {
        st->op_ok.store(ok, std::memory_order_release);
        /**
         * 放行后资源操作已落定
         */
        st->op_done.CountDown();
    };
    /**
     * RAII：任何早退/断言失败都先放行闸门，避免排空窗口上悬挂。
     */
    struct ReleaseGuard {
        std::shared_ptr<RunState> st;
        ~ReleaseGuard() { if (st) st->gate->Open(); }
    } release{st};

    /**
     * 在途响应 sink：显式等待并断言客户端得到**明确结果**（完整回包或契约
     * 允许的传输错误）——删掉发送/响应即失败，不能只发不等仍绿。
     */
    auto sink = std::make_shared<std::optional<fw::result<inf::rpc::Response>>>();
    auto sink_done = std::make_shared<TestLatch>(1);
    auto client    = box.client;
    auto endpoint  = box.endpoint;
    /**
     * hold 请求 id 每次运行唯一：Mongo 文档键不跨运行冲突（InsertOne 否则
     * 重复键）。
     */
    auto hold_env  = MakeEnv("kv", "hold", "c3-h", Req1(RunPrefix() + "hold"));
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [client, endpoint, sink, sink_done, hold_env] {
            try {
                sink->emplace(SendRpc(client, endpoint, hold_env, kWait));
            } catch (...) {
                /**
                 * 保持等待者必被通知；异常时 sink 为空，由下方断言捕获。
                 */
            }
            sink_done->CountDown();
        },
        succ);
    BOOST_REQUIRE(succ);
    /**
     * handler 已在闸门上（真实 V2 在途）
     */
    BOOST_REQUIRE(st->arrived.WaitFor(kWait));

    const auto shutdown_req_at = Clock::now();
    box.app->RequestShutdown();
    /**
     * 首轮 CoStop（shutdown_budget=300ms）：在途 handler 让排空未证实 →
     * StopIncomplete + wait_handlers_done 失败记录；宿主已同步 Close。
     */
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        bbt::coroutine::WaitOptions wait;
        wait.deadline = Clock::now() + std::chrono::seconds(10);
        BOOST_REQUIRE(bbt_co_await box.app->CoWaitShutdownRequest(wait));
        BOOST_REQUIRE(bbt_co_await box.app->CoStop());
    }));
    const auto incomplete_at = Clock::now();
    BOOST_CHECK(box.app->State() == fw::AppState::StopIncomplete);
    /**
     * 排空未竟如实留档。
     */
    {
        bool saw_drain = false;
        for (const auto& f : box.app->LifecycleFailures())
            if (f.phase == "wait_handlers_done") saw_drain = true;
        BOOST_CHECK_MESSAGE(saw_drain,
            "StopIncomplete 应有 wait_handlers_done 失败记录");
    }
    /**
     * StopIncomplete（handler 未排空）期间：资源尚未收口（borrow 保活
     * 即 owner 保留证据），Service 条目仍登记（句柄有效），真实命令
     * 仍通——owner/资源版本保留。
     */
    BOOST_CHECK(!box.cache->Get().IsClosed());
    BOOST_CHECK(!box.docs->Get().IsClosed());
    BOOST_CHECK(svc_handle.IsValid());
    {
        auto alive = RunInCoroutine([&box] {
            inf::CallOptions o{Clock::now() + std::chrono::milliseconds{5000}};
            return bbt_co_await box.cache->Get().Ping(o);
        });
        BOOST_REQUIRE_MESSAGE(alive.has_value() && *alive,
            "StopIncomplete 期间真实 Ping 应成功（资源未收口）");
    }

    /**
     * 迟到排空
     */
    st->gate->Open();
    /**
     * 放行后 handler 真实继续执行
     */
    BOOST_REQUIRE(st->released.WaitFor(kWait));
    const auto released_at = Clock::now();
    /**
     * 放行后（仍在 StopIncomplete、资源未收束）handler 经组件借用取
     * Redis/Mongo 执行真实操作成功——由独立 RunState 观察，不读可能被
     * 释放的 Service。
     */
    BOOST_REQUIRE(st->op_done.WaitFor(kWait));
    BOOST_CHECK_MESSAGE(st->op_ok.load(),
        "StopIncomplete 期间经组件借用的真实 Redis/Mongo 操作未成功");
    /**
     * 同步等客户端明确结果（不悬挂）
     */
    BOOST_REQUIRE(sink_done->WaitFor(kWait));
    /**
     * 必须有明确回包/错误，不接受“无结果”静默
     */
    BOOST_REQUIRE(sink->has_value());

    /**
     * 归还借用 → 再次 CoStop（已 Close 宿主跳过重排空）推进收口。
     */
    box.cache.reset();
    box.docs.reset();
    box.cache_h.reset();
    box.docs_h.reset();
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        BOOST_REQUIRE(bbt_co_await box.app->CoStop(
            fw::LifecycleOptions{}.SetDeadline(
                Clock::now() + std::chrono::seconds(10))));
    }));
    /**
     * ── 计时证据：本请求 incoming 预算 10000ms，关闭预算 300ms；放行
     * 发生在 incoming 到期之前。注意「未过期」只说明 handler 侧预算
     * 未尽，并不等于 Close 后必达（见下方契约判定）。
     */
    BOOST_TEST_MESSAGE("C3 计时：shutdown→StopIncomplete="
        << std::chrono::duration_cast<std::chrono::milliseconds>(
               incomplete_at - shutdown_req_at).count()
        << "ms, release→client结果="
        << std::chrono::duration_cast<std::chrono::milliseconds>(
               released_at - incomplete_at).count()
        << "ms, incoming预算=10000ms（未到期）");
    BOOST_CHECK(box.app->State() == fw::AppState::Stopped);
    BOOST_CHECK(box.app->PendingCleanup().empty());
    /**
     * 完整收口后 Service 条目销毁：句柄失效（测试全程未持实例
     * owner，故失效只能来自框架销毁）。
     */
    BOOST_CHECK_MESSAGE(!svc_handle.IsValid(),
        "收口完成（对象销毁）后 Service 句柄应失效");
    if (box.client_rt) box.client_rt->Close();
    box.app.reset();

    /**
     * ── 客户端可见结果门禁（按契约，不按“允许任意错误”放宽）──
     * 该请求 incoming 预算 10000ms、关闭预算 300ms：放行发生在 incoming
     * 到期之前。但「未过期」不等于「Close 后必达」：InfraHttpHost 契约明文
     * 规定「Close 开始后仍在途且尚未交给传输层的回复不承诺送达原客户端」；
     * infra rpc::CoRpcServer 的 Shutdown 亦明确丢弃未发送数据。故客户端
     * 结果只接受契约允许的两种：
     * (a) 完整 HELD —— 回复在 Close 生效前已交给传输层送达；
     * (b) TransportError(code=8) —— Close 开始后在途、尚未交给传输层而被截断。
     * 空结果或其它未知错误仍判失败（不静默、不放宽为任意错误）。
     */
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
    KvSvc::s_hold_gate = nullptr;
    KvSvc::s_hold_op_report = nullptr;
}

/**
 * C4a：后端 I/O 在途（服务端停顿）——同一自有实例 CLIENT PAUSE 后，业务
 * Get 在 cache 命中路径上被真实服务端扣住 ~pause 才返回正确值：命令确已
 * 发出并被服务端执行（在途证据；若未发出不可能返回该值，也不以单纯
 * TimedOut 反证）。
 */
BOOST_AUTO_TEST_CASE(combined_backend_inflight_held_by_pause) {
    BOOST_REQUIRE_MESSAGE(!RedisCliPath().empty(),
        "需要 BBT_TEST_REDIS_CLI（redis-cli 路径由环境/runner 传入）");
    auto box = StartCombined(/*incoming*/std::chrono::milliseconds{6000},
                             /*shutdown*/std::chrono::milliseconds{2000});
    const std::string id = RunPrefix() + "inflight";
    auto call = [&box](const char* method, const fw::CoRpcReq& req)
        -> fw::result<inf::rpc::Response> {
        return CallRpc(box, MakeEnv("kv", method,
            std::string("c4a-") + method, req));
    };

    /**
     * 预热：Mongo 落库 + 首次 get 填充 cache（此后为 cache 命中路径）。
     */
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
    {
        auto ping = RunInCoroutine([&box] {
            inf::CallOptions o{Clock::now() + std::chrono::milliseconds{5000}};
            return bbt_co_await box.cache->Get().Ping(o);
        });
        BOOST_REQUIRE_MESSAGE(ping.has_value() && *ping,
            "C4a：预热后 Ping 应成功（连接健康）");
    }

    /**
     * 真实服务端停顿：普通 GET 被停住（ALL）。
     */
    auto pause = RunRedisCli({"CLIENT", "PAUSE", "1500", "ALL"});
    BOOST_REQUIRE_MESSAGE(pause.rc == 0 &&
        pause.out.find("OK") != std::string::npos,
        "CLIENT PAUSE 失败: rc=" << pause.rc << " out=" << pause.out);

    const auto t0 = Clock::now();
    /**
     * 命中路径：单条真实 Redis GET
     */
    auto r = call("get", Req1(id));
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
    /**
     * 新入口无 ConnectStatus；在途期间连接未被退役的事实由「pause 结束后
     * Get 仍返回正确值」本身证明（坏流会被退役且后续命令必失败）。
     */
    {
        auto ping = RunInCoroutine([&box] {
            inf::CallOptions o{Clock::now() + std::chrono::milliseconds{5000}};
            return bbt_co_await box.cache->Get().Ping(o);
        });
        BOOST_REQUIRE_MESSAGE(ping.has_value() && *ping,
            "C4a：pause 放开后连接应仍健康（Ping 应成功）");
    }

    box.Stop();
    BOOST_CHECK(box.app->State() == fw::AppState::Stopped);
}

/**
 * C4b：后端 I/O 在途 deadline——短父预算 < pause；命令已写出、ReadReply
 * 等待时 deadline 先到 → 真实在途 TimedOut；连接被判不可复用（与 C2
 * 派发前保留健康连接判别）；恢复由测试装配控制层显式 Connect（不 Service
 * 重连、不自动重试）。
 */
BOOST_AUTO_TEST_CASE(combined_backend_inflight_deadline) {
    BOOST_REQUIRE_MESSAGE(!RedisCliPath().empty(),
        "需要 BBT_TEST_REDIS_CLI（redis-cli 路径由环境/runner 传入）");
    auto box = StartCombined(/*incoming*/std::chrono::milliseconds{900},
                             /*shutdown*/std::chrono::milliseconds{2000});
    {
        auto ping = RunInCoroutine([&box] {
            inf::CallOptions o{Clock::now() + std::chrono::milliseconds{5000}};
            return bbt_co_await box.cache->Get().Ping(o);
        });
        BOOST_REQUIRE_MESSAGE(ping.has_value() && *ping,
            "C4b：装配后 Ping 应成功（连接健康）");
    }

    auto pause = RunRedisCli({"CLIENT", "PAUSE", "1500", "ALL"});
    BOOST_REQUIRE_MESSAGE(pause.rc == 0 &&
        pause.out.find("OK") != std::string::npos,
        "CLIENT PAUSE 失败: rc=" << pause.rc << " out=" << pause.out);

    auto r = CallRpc(box, MakeEnv("kv", "ping", "c4b-p", Req1("x")),
                     std::chrono::milliseconds{5000});
    /**
     * 契约证据（区分于「任意错误」）：本调用 incoming 预算 900ms、redis-cli
     * pause 1500ms——handler 的 deadline 在服务端扣住期间先到。新入口如实
     * 返回 TimedOut（写未接纳即到期）或 OutcomeUnknown（写已接纳、没有可信
     * 完整回复——不推断 Redis 未执行）。两路都证明「命令真实在途」。
     */
    BOOST_REQUIRE(!r);
    {
        const int code = static_cast<int>(r.error().code);
        BOOST_CHECK_MESSAGE(code ==
                static_cast<int>(fw::ErrorCode::TimedOut) ||
            code == static_cast<int>(fw::ErrorCode::OutcomeUnknown),
            "C4b：在途 deadline 应返回 TimedOut 或 OutcomeUnknown，实际=" << code
                << " msg=\"" << r.error().message << "\"");
    }
    /**
     * 新入口语义（redis-net.md）：写已接纳、没有可信完整回复 →
     * OutcomeUnknown 并退役整条流；deadline/取消在写已接纳后同样走
     * Fail（broken，后续命令报「explicit Connect required」），与旧栈的
     * Failed 态等价、但不经 ConnectStatus 暴露——由后续命令的如实失败
     * 与显式 Connect 恢复路径证明。
     */
    {
        auto after = RunInCoroutine([&box] {
            inf::CallOptions o{Clock::now() + std::chrono::milliseconds{5000}};
            return bbt_co_await box.cache->Get().Ping(o);
        });
        BOOST_REQUIRE_MESSAGE(after.has_value() && !*after,
            "C4b：在途 deadline 后连接应已退役（Ping 应失败），实际仍可用");
    }

    /**
     * 等 pause 结束：用同一实例的真实命令做确定性屏障（非固定 sleep）。
     */
    auto barrier = RunRedisCli({"PING"});
    BOOST_CHECK_EQUAL(barrier.rc, 0);

    /**
     * 恢复：测试装配控制层显式 Connect（不 Service 重连、不自动重试、
     * 不放宽 Error、不宣称逻辑 timeout 等于远端未执行）。新入口故障后须
     * 显式 Connect——已退役连接经显式 Connect 重建后才恢复命令面。
     */
    auto conn = RunInCoroutine([&box] {
        inf::CallOptions o{Clock::now() + std::chrono::milliseconds{5000}};
        return bbt_co_await box.cache->Get().Connect(o);
    });
    /**
     * 协程已返回
     */
    BOOST_REQUIRE(conn.has_value());
    /**
     * Connect 成功
     */
    BOOST_REQUIRE(static_cast<bool>(*conn));
    {
        auto ping = RunInCoroutine([&box] {
            inf::CallOptions o{Clock::now() + std::chrono::milliseconds{5000}};
            return bbt_co_await box.cache->Get().Ping(o);
        });
        BOOST_REQUIRE_MESSAGE(ping.has_value() && *ping,
            "C4b：显式 Connect 重建后 Ping 应成功");
    }

    auto ok = CallRpc(box, MakeEnv("kv", "ping", "c4b-ok", Req1("x")));
    BOOST_REQUIRE(ok);
    auto rep = Decode2(ok);
    BOOST_REQUIRE(rep);
    BOOST_CHECK_EQUAL(rep.value().status, 0);
    BOOST_CHECK_EQUAL(rep.value().value, "PONG");

    box.Stop();
    BOOST_CHECK(box.app->State() == fw::AppState::Stopped);
}

BOOST_AUTO_TEST_SUITE_END()
} // namespace
