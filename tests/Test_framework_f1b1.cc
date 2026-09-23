// service-actor/v2 F1-b1 切片验证：CoApp 装配、run 前校验、生命周期
// 启动/关闭顺序、启动中途失败回退、关闭超时 ShutdownIncomplete、多宿主
// 拒绝。网络组件全部走 INetworkHost 桩（经 internal 测试缝注入）：断言
// 真实调用序列、每步发生时的调度器代际、超时与异常路径；无真实 socket、
// 无 sleep 凑时序、无空断言。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/object/CoObject.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>

// 机器面（宿主桩/生命周期/注册表/测试缝构造）显式走 internal 头。
#include <bbt/framework/internal/ActorRegistry.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/HostLifecycle.hpp>

namespace fw = bbt::framework;
namespace co = bbt::coroutine;
using Clock = std::chrono::steady_clock;

namespace {

// ── 测试屏障原语（与 F2-a 同一形态：std CV，无 sleep 凑时序）──

class TestGate {
public:
    void Open() {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_open = true;
        }
        m_cv.notify_all();
    }
    void Wait() {
        std::unique_lock<std::mutex> lk(m_mtx);
        m_cv.wait(lk, [this] { return m_open; });
    }
    bool WaitFor(std::chrono::milliseconds ms) {
        std::unique_lock<std::mutex> lk(m_mtx);
        return m_cv.wait_for(lk, ms, [this] { return m_open; });
    }
private:
    std::mutex              m_mtx;
    std::condition_variable m_cv;
    bool                    m_open = false;
};

class TestLatch {
public:
    explicit TestLatch(int n) : m_count(n) {}
    void CountDown() {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (--m_count > 0) return;
        }
        m_cv.notify_all();
    }
    void Wait() {
        std::unique_lock<std::mutex> lk(m_mtx);
        m_cv.wait(lk, [this] { return m_count == 0; });
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

constexpr std::chrono::milliseconds kWait{5000};

// ── INetworkHost 桩：记录每次调用的名字与当时调度器运行代际；代际 >0 即
//    「Scheduler 已启动且未 Stop」，借此把真实调度器位置纳入同一序列断言。──

struct NetCall {
    std::string   name;
    std::uint64_t sched_gen;
};

class StubNetHost final : public fw::INetworkHost {
public:
    fw::result<void> create_rc = fw::result<void>::ok();
    fw::result<void> start_rc  = fw::result<void>::ok();
    std::function<fw::result<void>(co::Deadline, co::CancellationToken)>
        on_wait_handlers;
    std::function<bbt::infra::CloseStatus(co::Deadline, co::CancellationToken)>
        on_wait_closed;
    std::function<void()> on_started;   // Start 成功后回调（控制线程）
    TestLatch start_seen{1};   // Start() 被调用（启动完成、开始接纳）时倒计时

    fw::result<void> Create() override {
        _Record("net.create");
        return create_rc;
    }
    fw::result<void> Start() override {
        _Record("net.start");
        start_seen.CountDown();
        auto rc = start_rc;
        if (rc && on_started) on_started();
        return rc;
    }
    void StopAccepting() noexcept override { _Record("net.stop_accepting"); }
    fw::result<void> WaitHandlersDone(co::Deadline d,
                                      co::CancellationToken t) override {
        _Record("net.wait_handlers");
        if (on_wait_handlers) return on_wait_handlers(d, t);
        return fw::result<void>::ok();
    }
    void RequestClose() noexcept override { _Record("net.request_close"); }
    bbt::infra::CloseStatus WaitClosed(co::Deadline d,
                                       co::CancellationToken t) override {
        _Record("net.wait_closed");
        if (on_wait_closed) return on_wait_closed(d, t);
        return bbt::infra::CloseStatus::Closed;
    }
    void ReleaseClosed() noexcept override { _Record("net.release"); }

    std::vector<NetCall> Snapshot() const {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_calls;
    }
    std::vector<std::string> Names() const {
        std::lock_guard<std::mutex> lk(m_mtx);
        std::vector<std::string> out;
        for (const auto& c : m_calls) out.push_back(c.name);
        return out;
    }
    std::size_t TotalCalls() const {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_calls.size();
    }

private:
    void _Record(const char* name) {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_calls.push_back(NetCall{name, g_scheduler->GetRunGeneration()});
    }
    mutable std::mutex    m_mtx;
    std::vector<NetCall>  m_calls;
};

// ── 业务桩类型：统一 CoRpcReq/CoRpcResp 公共面。──

class EchoSvc final : public fw::CoService<EchoSvc> {
public:
    static constexpr std::string_view kServiceName = "echo";
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value) return fw::CoRpcResp::Error(value.error());
        return fw::CoRpcResp::From(value.value());
    }
    // 资源缝观察口（业务侧 protected context() 的测试出口）。
    template <class R>
    std::shared_ptr<R> Resource() { return context().resource<R>(); }
    template <class R>
    std::shared_ptr<R> Resource(std::string_view name) {
        return context().resource<R>(name);
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EchoSvc::Ping>("ping"));
};

// 与 EchoSvc 同名：重复注册拒绝用。
class EchoDupSvc final : public fw::CoService<EchoDupSvc> {
public:
    static constexpr std::string_view kServiceName = "echo";
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value) return fw::CoRpcResp::Error(value.error());
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EchoDupSvc::Ping>("ping"));
};

class KeyedSvc final : public fw::CoService<KeyedSvc> {
public:
    static constexpr std::string_view kServiceName = "keyed";
    fw::CoRpcResp Get(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value) return fw::CoRpcResp::Error(value.error());
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::ActorMethodAt<&KeyedSvc::Get, std::int32_t>("get"));
};

// ActorSerial 缺 key 提取器（声明为普通 Method）：启动期校验失败用。
class UnkeyedSvc final : public fw::CoService<UnkeyedSvc> {
public:
    static constexpr std::string_view kServiceName = "unkeyed";
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value) return fw::CoRpcResp::Error(value.error());
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&UnkeyedSvc::Ping>("ping"));
};

// 资源缝验证类型（F0 已覆盖装配校验；此处验证运行期可取回）。
struct DummyResource { int n = 7; };

fw::ServiceOptions ConcurrentOpts() {
    return fw::ServiceOptions{
        fw::ExecutionPolicy::Concurrent,
        /*max_inflight*/ 64,
        /*mailbox_capacity*/ 0,
        /*max_actors*/ 0,
        /*ordered_ingress*/ false,
        /*max_ordered_streams*/ 0,
        /*max_cached_results*/ 0,
        /*max_cached_result_bytes*/ 0};
}

fw::ServiceOptions ActorOpts() {
    return fw::ServiceOptions{
        fw::ExecutionPolicy::ActorSerial,
        /*max_inflight*/ 64,
        /*mailbox_capacity*/ 16,
        /*max_actors*/ 8,
        /*ordered_ingress*/ false,
        /*max_ordered_streams*/ 0,
        /*max_cached_results*/ 0,
        /*max_cached_result_bytes*/ 0};
}

fw::CoAppOptions AppOpts(std::vector<fw::StaticRoute> routes = {}) {
    return fw::CoAppOptions{
        bbt::infra::NetworkLimits{
            /*max_connections*/ 64,
            /*max_inflight*/ 64,
            /*max_header_bytes*/ 8192,
            /*max_body_bytes*/ 65536,
            /*incoming_timeout*/ std::chrono::milliseconds{30000}},
        bbt::infra::ListenAddress{"127.0.0.1", 0},
        std::move(routes),
        /*shutdown_step_budget*/ std::chrono::milliseconds{2000}};
}

// 测试缝构造：宿主桩 + 无出站发送（受管出站如实 RuntimeUnavailable）。
std::unique_ptr<fw::CoApp> MakeApp(
    const fw::CoAppOptions& opts, const std::shared_ptr<StubNetHost>& host) {
    return fw::MakeCoAppForTest(opts, fw::CoAppSeam{host, {}});
}

// 在线程上跑 run()；返回的句柄不可移动（atomic 成员），以 unique_ptr 持有。
struct RunHandle {
    std::thread      th;
    std::atomic<bool> done{false};
    int              rc{-1};
};

std::unique_ptr<RunHandle> RunApp(fw::CoApp& app) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    auto h = std::make_unique<RunHandle>();
    RunHandle* p = h.get();
    h->th = std::thread([p, &app] {
        p->rc = app.run();
        p->done.store(true, std::memory_order_release);
    });
    return h;
}

void JoinRun(const std::unique_ptr<RunHandle>& h) {
    if (h->th.joinable()) h->th.join();
}

const std::vector<std::string> kCleanOrder{
    "net.create", "net.start", "net.stop_accepting", "net.wait_handlers",
    "net.request_close", "net.wait_closed", "net.release"};

BOOST_AUTO_TEST_SUITE(framework_f1b1)

// 用例 1：add_service 注册成功；同服务名重复注册被明确拒绝；服务身份在
// 启动期经 _bind_runtime 绑定（id/generation != 0）；资源缝在受管实例上
// 可取回已装配对象。
BOOST_AUTO_TEST_CASE(add_service_registers_and_rejects_duplicate) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);

    BOOST_REQUIRE(app->add_service<EchoSvc>(ConcurrentOpts()));
    BOOST_REQUIRE(app->add_service<KeyedSvc>(ActorOpts()));
    auto res = std::make_shared<DummyResource>();
    auto primary = std::make_shared<DummyResource>();
    primary->n = 11;
    auto replica = std::make_shared<DummyResource>();
    replica->n = 22;
    BOOST_REQUIRE(app->add_resource(res));
    BOOST_REQUIRE(app->add_resource<DummyResource>("primary", primary));
    BOOST_REQUIRE(app->add_resource<DummyResource>("replica", replica));

    auto dup = app->add_service<EchoDupSvc>(ConcurrentOpts());
    BOOST_REQUIRE(!dup);
    BOOST_CHECK(dup.error().code == fw::ErrorCode::InvalidArgument);
    BOOST_TEST(dup.error().message.find("duplicate") != std::string::npos);

    auto h = RunApp(*app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

    // Concurrent：启动期绑定的单实例可读回对象身份。
    auto svc = app->find_service("echo");
    BOOST_REQUIRE(svc);
    BOOST_TEST(svc.value()->GetObjectInfo().id != 0);
    BOOST_TEST(svc.value()->GetObjectInfo().generation != 0);
    BOOST_TEST(svc.value()->GetObjectInfo().kind == "service");
    BOOST_TEST(svc.value()->GetObjectInfo().name == "echo");

    // 资源缝正路径：装配进宿主的资源经服务上下文可取回同一对象。
    auto* echo = static_cast<EchoSvc*>(svc.value().get());
    auto got = echo->Resource<DummyResource>();
    BOOST_REQUIRE(got != nullptr);
    BOOST_TEST(got.get() == res.get());
    BOOST_TEST(got->n == 7);
    auto got_primary = echo->Resource<DummyResource>("primary");
    auto got_replica = echo->Resource<DummyResource>("replica");
    BOOST_REQUIRE(got_primary != nullptr);
    BOOST_REQUIRE(got_replica != nullptr);
    BOOST_TEST(got_primary.get() == primary.get());
    BOOST_TEST(got_replica.get() == replica.get());
    BOOST_TEST(got_primary->n == 11);
    BOOST_TEST(got_replica->n == 22);
    BOOST_CHECK(echo->Resource<DummyResource>("missing") == nullptr);

    // ActorSerial：注册表激活时绑定身份 + actor key。
    auto* reg = app->actor_registry();
    BOOST_REQUIRE(reg != nullptr);
    auto actor = reg->GetOrCreate("keyed", "k-1");
    BOOST_REQUIRE(actor);
    BOOST_TEST(actor.value()->GetObjectInfo().id != 0);
    BOOST_REQUIRE(actor.value()->co_actor_key().has_value());
    BOOST_TEST(actor.value()->co_actor_key().value() == "k-1");

    // 未注册名/按 key 服务走 find_service 的明确错误。
    BOOST_CHECK(!app->find_service("no-such"));
    BOOST_CHECK(!app->find_service("keyed"));

    app->request_shutdown();
    JoinRun(h);
    BOOST_TEST(h->rc == fw::HostLifecycle::kExitOk);
}

// 用例 2：ServiceOptions 违反 F2-a 校验 → add_service 失败，任何组件未
// 启动（桩计数 0、调度器代际 0）；run 自身配置非法同样 run 前失败。
BOOST_AUTO_TEST_CASE(invalid_options_fail_before_any_startup) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);

    {   // max_inflight == 0
        auto o = ConcurrentOpts(); o.max_inflight = 0;
        auto r = app->add_service<EchoSvc>(o);
        BOOST_REQUIRE(!r);
        BOOST_CHECK(r.error().code == fw::ErrorCode::InvalidArgument);
    }
    {   // ActorSerial 缺 mailbox_capacity
        auto o = ActorOpts(); o.mailbox_capacity = 0;
        auto r = app->add_service<KeyedSvc>(o);
        BOOST_REQUIRE(!r);
        BOOST_CHECK(r.error().code == fw::ErrorCode::InvalidArgument);
    }
    {   // ActorSerial 服务存在无 key 提取器方法 → 装配期失败
        auto r = app->add_service<UnkeyedSvc>(ActorOpts());
        BOOST_REQUIRE(!r);
        BOOST_CHECK(r.error().code == fw::ErrorCode::InvalidArgument);
        BOOST_TEST(r.error().message.find("ping") != std::string::npos);
    }

    // 注册失败不登记：同名/同服务仍可重新合法注册。
    BOOST_REQUIRE(app->add_service<EchoSvc>(ConcurrentOpts()));

    // run 自身校验失败（network_limits 非法）→ 不启动任何组件。
    auto host2 = std::make_shared<StubNetHost>();
    auto bad_opts = AppOpts();
    bad_opts.network_limits.max_inflight = 0;
    auto app2 = MakeApp(bad_opts, host2);
    BOOST_CHECK(app2->run() == fw::HostLifecycle::kExitRejected);
    BOOST_TEST(host2->TotalCalls() == std::size_t{0});
    BOOST_TEST(g_scheduler->GetRunGeneration() == 0);
    BOOST_TEST(app2->lifecycle_failures().size() == std::size_t{1});
}

// 用例 3：未显式配置的出站目标被拒绝——不存在「自动连任意服务」；
// 拒绝发生在任何网络动作之前（桩计数 0）。
BOOST_AUTO_TEST_CASE(unconfigured_route_target_rejected) {
    auto host = std::make_shared<StubNetHost>();
    fw::CoAppOptions opts = AppOpts({
        fw::StaticRoute{"svc.allowed",
                        bbt::infra::RpcAddress{"tcp", "127.0.0.1:9001"}}});
    auto app = MakeApp(opts, host);

    auto bad = app->find_route("svc.unknown");
    BOOST_REQUIRE(!bad);
    BOOST_CHECK(bad.error().code == fw::ErrorCode::NotFound);
    BOOST_TEST(bad.error().message.find("svc.unknown") != std::string::npos);

    auto good = app->find_route("svc.allowed");
    BOOST_REQUIRE(good);
    BOOST_TEST(good.value().transport == "tcp");
    BOOST_TEST(good.value().endpoint == "127.0.0.1:9001");

    // 拒绝发生在网络组件启动之前：桩一次都未被调用。
    BOOST_TEST(host->TotalCalls() == std::size_t{0});
}

// 用例 4：启动顺序 = Scheduler::Start → 网络 Create/Start（开始接纳）→
// 运行；桩调用点记录代际证明调度器先于网络、且整个使用期保持运行。
BOOST_AUTO_TEST_CASE(startup_order_scheduler_then_network) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app->add_service<EchoSvc>(ConcurrentOpts()));

    auto h = RunApp(*app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Running);

    app->request_shutdown();
    JoinRun(h);
    BOOST_TEST(h->rc == fw::HostLifecycle::kExitOk);

    const auto calls = host->Snapshot();
    std::vector<std::string> names;
    for (const auto& c : calls) names.push_back(c.name);
    BOOST_TEST(names == kCleanOrder);
    // 全部网络调用都发生在调度器运行代际内（Scheduler::Start 先于
    // net.create，Scheduler::Stop 晚于 net.release）。
    for (const auto& c : calls)
        BOOST_TEST(c.sched_gen != 0);
    BOOST_TEST(g_scheduler->GetRunGeneration() == 0);   // Stop 最后
}

// 用例 5：关闭顺序 = StopAccepting → 等 handler 结束 → RequestClose →
// WaitClosed → 释放 → Scheduler::Stop；StopAccepting 严格先于
// RequestClose/WaitClosed。
BOOST_AUTO_TEST_CASE(shutdown_order_stop_accepting_before_close) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app->add_service<EchoSvc>(ConcurrentOpts()));

    auto h = RunApp(*app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));
    app->request_shutdown();
    JoinRun(h);
    BOOST_TEST(h->rc == fw::HostLifecycle::kExitOk);

    const auto calls = host->Snapshot();
    auto pos = [&calls](const char* n) -> std::size_t {
        for (std::size_t i = 0; i < calls.size(); ++i)
            if (calls[i].name == n) return i;
        return calls.size();
    };
    BOOST_TEST(pos("net.stop_accepting") < pos("net.wait_handlers"));
    BOOST_TEST(pos("net.wait_handlers") < pos("net.request_close"));
    BOOST_TEST(pos("net.request_close") < pos("net.wait_closed"));
    BOOST_TEST(pos("net.wait_closed") < pos("net.release"));
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Closed);
    BOOST_TEST(g_scheduler->GetRunGeneration() == 0);
}

// 用例 6：启动中途失败——Create 失败只回退 Scheduler；Start 失败按同一
// 固定顺序回退已立起的网络组件；不留半启动状态（代际归零、run 返回）。
BOOST_AUTO_TEST_CASE(startup_failure_rolls_back_in_order) {
    {   // 失败在第 1 个网络步（Create）
        auto host = std::make_shared<StubNetHost>();
        host->create_rc = fw::result<void>::err(
            fw::MakeError(fw::ErrorCode::InternalError, "create boom"));
        auto app = MakeApp(AppOpts(), host);
        BOOST_REQUIRE(app->add_service<EchoSvc>(ConcurrentOpts()));

        BOOST_TEST(app->run() == fw::HostLifecycle::kExitStartFailed);
        // Create 未成功 → 网络关闭序列不适用，只回退 Scheduler。
        BOOST_TEST(host->Names() == std::vector<std::string>{"net.create"});
        BOOST_TEST(g_scheduler->GetRunGeneration() == 0);
        BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Closed);
        BOOST_TEST(!app->lifecycle_failures().empty());
    }
    {   // 失败在第 2 个网络步（Start）：Create 已立起 → 全序列回退
        auto host = std::make_shared<StubNetHost>();
        host->start_rc = fw::result<void>::err(
            fw::MakeError(fw::ErrorCode::Unavailable, "start boom"));
        auto app = MakeApp(AppOpts(), host);
        BOOST_REQUIRE(app->add_service<EchoSvc>(ConcurrentOpts()));

        BOOST_TEST(app->run() == fw::HostLifecycle::kExitStartFailed);
        const auto calls = host->Snapshot();
        std::vector<std::string> names;
        for (const auto& c : calls) names.push_back(c.name);
        // create/start 失败步 + 同一固定顺序的回退序列。
        BOOST_TEST(names == std::vector<std::string>({
            "net.create", "net.start", "net.stop_accepting",
            "net.wait_handlers", "net.request_close", "net.wait_closed",
            "net.release"}));
        for (const auto& c : calls)
            BOOST_TEST(c.sched_gen != 0);   // 回退期间调度器仍在驱动
        BOOST_TEST(g_scheduler->GetRunGeneration() == 0);
    }
}

// 用例 7：关闭等待超时——桩让 handler 在预算内不结束：进入
// ShutdownIncomplete、run 不返回、不伪装成功；迟到收尾完成后 run 返回
// 非零（kExitShutdownLate），进程不退出、无 _Exit/abort。
BOOST_AUTO_TEST_CASE(shutdown_timeout_reports_incomplete_not_success) {
    auto host = std::make_shared<StubNetHost>();
    TestLatch waiting_unbounded{1};   // 第二次（无界）等待已进入
    TestGate  allow_drain;            // 迟到收尾放行
    std::atomic<int> drain_calls{0};

    host->on_wait_handlers =
        [&](co::Deadline, co::CancellationToken) -> fw::result<void> {
            if (drain_calls.fetch_add(1) == 0) {
                // 预算内不结束 → 明确超时（不用 sleep：直接报 TimedOut，
                // 由状态机决定后续）。
                return fw::result<void>::err(fw::MakeError(
                    fw::ErrorCode::TimedOut, "handlers still running"));
            }
            // 预算耗尽后的无界续等：挂到测试放行——这就是「未完成的清理」
            // 仍在被等待的实据。
            waiting_unbounded.CountDown();
            allow_drain.Wait();
            return fw::result<void>::ok();
        };

    auto app = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app->add_service<EchoSvc>(ConcurrentOpts()));

    auto h = RunApp(*app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));
    app->request_shutdown();

    // 等状态机进入无界续等：此时可断言未完成清理的真实状态。
    BOOST_REQUIRE(waiting_unbounded.WaitFor(kWait));
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::ShutdownIncomplete);
    const auto pending = app->pending_cleanup();
    BOOST_REQUIRE(pending.size() == 1);
    BOOST_TEST(pending[0] == "WaitHandlersDone");
    // run 不返回（栈上 CoApp 不被析构）；StopAccepting 已在超时前发生。
    BOOST_TEST(!h->done.load());
    const auto names = host->Names();
    BOOST_CHECK(std::find(names.begin(), names.end(),
                          "net.stop_accepting") != names.end());
    BOOST_CHECK(std::find(names.begin(), names.end(),
                          "net.release") == names.end());   // 未提前释放

    // 迟到收尾完成：完整走完后序步骤，run 返回非零（曾超预算）。
    allow_drain.Open();
    JoinRun(h);
    BOOST_TEST(h->rc == fw::HostLifecycle::kExitShutdownLate);
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Closed);
    BOOST_TEST(app->pending_cleanup().empty());
    BOOST_TEST(g_scheduler->GetRunGeneration() == 0);
    const auto after = host->Names();
    BOOST_CHECK(std::find(after.begin(), after.end(),
                          "net.release") != after.end());
}

// 用例 8：多 CoApp——另一宿主活跃时第二个 run() 被明确拒绝；同一实例
// 重复 run 同样拒绝；完整收束后新实例可顺序复用单例调度器。
// （契约未冻结多实例口径 → 本实现口径，报告待澄清。）
BOOST_AUTO_TEST_CASE(second_active_app_run_rejected) {
    auto host1 = std::make_shared<StubNetHost>();
    auto app1 = MakeApp(AppOpts(), host1);
    BOOST_REQUIRE(app1->add_service<EchoSvc>(ConcurrentOpts()));

    auto h1 = RunApp(*app1);
    BOOST_REQUIRE(host1->start_seen.WaitFor(kWait));

    // 第二个活跃 run() 被拒；其网络组件一次未被调用。
    auto host2 = std::make_shared<StubNetHost>();
    auto app2 = MakeApp(AppOpts(), host2);
    BOOST_TEST(app2->run() == fw::HostLifecycle::kExitRejected);
    BOOST_TEST(host2->TotalCalls() == std::size_t{0});
    BOOST_CHECK(app2->shutdown_state() == fw::ShutdownState::Running);

    app1->request_shutdown();
    JoinRun(h1);
    BOOST_TEST(h1->rc == fw::HostLifecycle::kExitOk);

    // 同一实例不可重入 run()。
    BOOST_TEST(app1->run() == fw::HostLifecycle::kExitRejected);

    // 顺序复用单例调度器：收束后新实例可正常 run。
    auto host3 = std::make_shared<StubNetHost>();
    auto app3 = MakeApp(AppOpts(), host3);
    BOOST_REQUIRE(app3->add_service<EchoSvc>(ConcurrentOpts()));
    auto h3 = RunApp(*app3);
    BOOST_REQUIRE(host3->start_seen.WaitFor(kWait));
    app3->request_shutdown();
    JoinRun(h3);
    BOOST_TEST(h3->rc == fw::HostLifecycle::kExitOk);
    BOOST_TEST(g_scheduler->GetRunGeneration() == 0);
}

// 补充：早于 run 的 request_shutdown → 完成启动后立即进入关闭序列。
BOOST_AUTO_TEST_CASE(shutdown_requested_before_run_closes_immediately) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app->add_service<EchoSvc>(ConcurrentOpts()));
    app->request_shutdown();
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Closing);

    BOOST_TEST(app->run() == fw::HostLifecycle::kExitOk);
    const auto names = host->Names();
    BOOST_TEST(names == kCleanOrder);
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Closed);
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace
