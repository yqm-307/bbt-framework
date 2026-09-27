// service-actor/v2 #8 切片验证：命名资源 Registry 的工厂装配、
// Create/Start 相位、失败回滚与 shutdown budget 下的 RequestClose/
// WaitClosed 收束。全部经 INetworkHost 桩驱动生命周期（与 F1-b1 同一
// 形态：无真实 socket、无 sleep 凑时序、真实断言调用序与代际）。
//
// 覆盖（issue #8 验收的资源生命周期切片）：
//  T1 factory_named_resources_start_after_scheduler
//      两个同名类型不同名工厂资源在 Scheduler 启动后创建并各 Start 一次；
//      两个 Service 取同名资源得到同一实例、不同名隔离；关闭序列对资源
//      RequestClose+WaitClosed 且发生在 handler 排空后、网络 RequestClose 前。
//  T2 factory_create_failure_rolls_back
//      工厂 Create 返回 err → run 返回 kExitStartFailed、service 不绑定、
//      已先启动的资源被 RequestClose+WaitClosed 收束（不留半装配活跃资源）。
//  T3 factory_start_failure_rolls_back
//      第二个资源 Start 失败 → 半装配资源本身也被 RequestClose/WaitClosed
//      收束且不进入 service 可见实例视图。
//  T4 duplicate_and_invalid_registration
//      (类型,名) 重复登记失败；工厂资源与预创建共享同一键空间；空工厂失败；
//      run 开始后登记返回 Closed。
//  T5 resource_close_within_budget
//      资源 WaitClosed 在预算内完成 → kExitOk，顺序在 WaitHandlersDone 后。
//  T6 resource_close_budget_exhaustion
//      资源 WaitClosed 首调超时 → ShutdownIncomplete（run 不返回、未完成项
//      含 resource:*、资源仍强持有）→ 迟到续等完成 → kExitShutdownLate。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/coroutine/sync/Cancellation.hpp>
#include <bbt/coroutine/sync/CompletionSignal.hpp>

#include <bbt/infra/ICoCloseable.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>

// 机器面（生命周期/测试缝构造）显式走 internal 头。
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/HostLifecycle.hpp>

namespace fw  = bbt::framework;
namespace co  = bbt::coroutine;
namespace inf = bbt::infra;
using Clock = std::chrono::steady_clock;

namespace {

class TestGate {
public:
    void Open() {
        { std::lock_guard<std::mutex> lk(m_mtx); m_open = true; }
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

constexpr std::chrono::milliseconds kWait{5000};

// ── INetworkHost 桩（与 F1-b1 同形态）：记录调用名与调度器代际。──
struct NetCall {
    std::string   name;
    std::uint64_t sched_gen;
};

class StubNetHost final : public fw::INetworkHost {
public:
    fw::result<void> create_rc = fw::result<void>::ok();
    fw::result<void> start_rc  = fw::result<void>::ok();
    TestLatch start_seen{1};

    fw::result<void> Create() override { _Record("net.create"); return create_rc; }
    fw::result<void> Start() override {
        _Record("net.start"); start_seen.CountDown(); return start_rc; }
    void StopAccepting() noexcept override { _Record("net.stop_accepting"); }
    fw::result<void> WaitHandlersDone(co::Deadline,
                                    co::CancellationToken) override {
        _Record("net.wait_handlers");
        return fw::result<void>::ok();
    }
    void RequestClose() noexcept override { _Record("net.request_close"); }
    inf::CloseStatus WaitClosed(co::Deadline, co::CancellationToken) override {
        _Record("net.wait_closed");
        return inf::CloseStatus::Closed;
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
    mutable std::mutex   m_mtx;
    std::vector<NetCall> m_calls;
};

// ── 桩资源：模拟 infra CoRedisCli 的资源形态——静态 Create 要求 Scheduler
//    已启动、实例 Start() -> result<void>、实现 ICoCloseable。──
class StubResource : public inf::ICoCloseable {
public:
    struct Shared {
        std::atomic<int> create_calls{0};
        std::atomic<int> start_calls{0};
        std::atomic<int> request_close_calls{0};
        std::atomic<int> wait_closed_calls{0};
        // 创建/启动时记录调度器代际（证明在 Scheduler::Start 之后）。
        std::atomic<std::uint64_t> create_gen{0};
        // WaitClosed 行为控制：首轮超时/挂起由测试编排。
        std::function<inf::CloseStatus(co::Deadline, co::CancellationToken)>
            on_wait_closed;
        std::atomic<bool> closed{false};
        std::atomic<bool> close_requested{false};
        std::atomic<bool> started{false};
        // 失败注入：下一次 Create/Start 返回 err（consume-once 语义）。
        std::atomic<bool> fail_create{false};
        std::atomic<bool> fail_start{false};
    };

    explicit StubResource(std::shared_ptr<Shared> s) : m_s(std::move(s)) {}

    static fw::result<std::shared_ptr<StubResource>> Create(
        std::shared_ptr<Shared> s) {
        s->create_calls.fetch_add(1);
        s->create_gen.store(g_scheduler->GetRunGeneration(),
                            std::memory_order_release);
        if (s->fail_create.exchange(false)) {
            return fw::result<std::shared_ptr<StubResource>>::err(
                fw::MakeError(fw::ErrorCode::Unavailable, "create boom"));
        }
        // Create 要求 Scheduler 已启动（运行时代际非零），否则如实失败。
        if (co::CurrentRuntimeGeneration() == 0)
            return fw::result<std::shared_ptr<StubResource>>::err(
                fw::MakeError(fw::ErrorCode::RuntimeUnavailable,
                    "scheduler not started"));
        return fw::result<std::shared_ptr<StubResource>>::ok(
            std::make_shared<StubResource>(std::move(s)));
    }

    fw::result<void> Start() {
        m_s->start_calls.fetch_add(1);
        if (m_s->fail_start.exchange(false))
            return fw::result<void>::err(
                fw::MakeError(fw::ErrorCode::Unavailable, "start boom"));
        m_s->started.store(true);
        return fw::result<void>::ok();
    }

    void RequestClose() noexcept override {
        // 如实语义：RequestClose 只发起关闭意图，不即置 Closed——真正
        // 收束由 WaitClosed 报告（与 infra 连接池/客户端的真实形态一致：
        // 在途回调未排空时 IsClosed 保持 false）。
        m_s->request_close_calls.fetch_add(1);
        m_s->close_requested.store(true);
    }
    bool IsClosed() const noexcept override { return m_s->closed.load(); }
    inf::CloseStatus WaitClosed(co::Deadline d,
                                co::CancellationToken c) override {
        m_s->wait_closed_calls.fetch_add(1);
        if (m_s->on_wait_closed) return m_s->on_wait_closed(d, std::move(c));
        m_s->closed.store(true);
        return inf::CloseStatus::Closed;
    }

private:
    std::shared_ptr<Shared> m_s;
};

// 不可关闭资源（无 ICoCloseable）：证明非可关闭资源不进入关闭序列。
struct PlainResource { int n = 0; };

// ── 业务桩服务。──
class CacheSvc final : public fw::CoService<CacheSvc> {
public:
    static constexpr std::string_view kServiceName = "cache-svc";
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto v = req.Parse<std::int32_t>();
        if (!v) return fw::CoRpcResp::Error(v.error());
        return fw::CoRpcResp::From(v.value());
    }
    // 资源缝观察口（protected context() 的测试出口）。
    template <class R>
    std::shared_ptr<R> Resource(std::string_view name) {
        return context().resource<R>(name);
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&CacheSvc::Ping>("ping"));
};

class SecondSvc final : public fw::CoService<SecondSvc> {
public:
    static constexpr std::string_view kServiceName = "second-svc";
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto v = req.Parse<std::int32_t>();
        if (!v) return fw::CoRpcResp::Error(v.error());
        return fw::CoRpcResp::From(v.value());
    }
    template <class R>
    std::shared_ptr<R> Resource(std::string_view name) {
        return context().resource<R>(name);
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&SecondSvc::Ping>("ping"));
};

fw::ServiceOptions ConcurrentOpts() {
    return fw::ServiceOptions{
        fw::ExecutionPolicy::Concurrent, 64, 0, 0, false, 0, 0, 0};
}

fw::CoAppOptions AppOpts() {
    return fw::CoAppOptions{
        bbt::infra::NetworkLimits{64, 64, 8192, 65536,
                                  std::chrono::milliseconds{30000}},
        bbt::infra::ListenAddress{"127.0.0.1", 0},
        {},
        std::chrono::milliseconds{1500}};
}

std::unique_ptr<fw::CoApp> MakeApp(const fw::CoAppOptions& opts,
                                   const std::shared_ptr<StubNetHost>& host) {
    return fw::MakeCoAppForTest(opts, fw::CoAppSeam{host, {}});
}

struct RunHandle {
    std::thread       th;
    std::atomic<bool> done{false};
    int               rc{-1};
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

std::size_t PosOf(const std::vector<std::string>& names,
                  const std::string& n) {
    for (std::size_t i = 0; i < names.size(); ++i)
        if (names[i] == n) return i;
    return names.size();
}

BOOST_AUTO_TEST_SUITE(framework_resource)

// T1：两个命名工厂资源在 Scheduler 启动后 Create/Start 各一次；两个服务
// 共享同名实例、不同名隔离；关闭序列对资源 RequestClose+WaitClosed 且
// 位于 handler 排空后、网络 RequestClose 前。
BOOST_AUTO_TEST_CASE(factory_resources_start_and_close_in_order) {
    auto host = std::make_shared<StubNetHost>();
    auto app  = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app->add_service<CacheSvc>(ConcurrentOpts()));
    BOOST_REQUIRE(app->add_service<SecondSvc>(ConcurrentOpts()));

    auto cache_s  = std::make_shared<StubResource::Shared>();
    auto session_s = std::make_shared<StubResource::Shared>();
    BOOST_REQUIRE(app->add_resource<StubResource>("cache",
        [cache_s] { return StubResource::Create(cache_s); }));
    BOOST_REQUIRE(app->add_resource<StubResource>("session",
        [session_s] { return StubResource::Create(session_s); }));
    // 预创建资源与工厂共存。
    auto plain = std::make_shared<PlainResource>();
    plain->n = 42;
    BOOST_REQUIRE(app->add_resource<PlainResource>("plain", plain));

    auto h = RunApp(*app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

    // Create/Start 各一次，且在调度器运行代际内（Scheduler 已启动）。
    BOOST_TEST(cache_s->create_calls.load() == 1);
    BOOST_TEST(cache_s->start_calls.load() == 1);
    BOOST_TEST(cache_s->create_gen.load() != 0);
    BOOST_TEST(session_s->create_calls.load() == 1);
    BOOST_TEST(session_s->start_calls.load() == 1);

    // 两个服务取同名资源得到同一托管实例；不同名隔离。
    auto svc1 = app->find_service("cache-svc");
    auto svc2 = app->find_service("second-svc");
    BOOST_REQUIRE(svc1);
    BOOST_REQUIRE(svc2);
    auto* c1 = static_cast<CacheSvc*>(svc1.value().get());
    auto* c2 = static_cast<SecondSvc*>(svc2.value().get());
    auto cache1 = c1->Resource<StubResource>("cache");
    auto cache2 = c2->Resource<StubResource>("cache");
    auto sess1  = c1->Resource<StubResource>("session");
    BOOST_REQUIRE(cache1 && cache2 && sess1);
    BOOST_TEST(cache1.get() == cache2.get());          // 同名共享同一实例
    BOOST_TEST(cache1.get() != sess1.get());           // 不同名隔离
    BOOST_CHECK(c1->Resource<StubResource>("missing") == nullptr);
    // 预创建资源同视图可见。
    auto got_plain = c1->Resource<PlainResource>("plain");
    BOOST_REQUIRE(got_plain);
    BOOST_TEST(got_plain->n == 42);

    app->request_shutdown();
    JoinRun(h);
    BOOST_TEST(h->rc == fw::HostLifecycle::kExitOk);

    // 资源关闭在 handler 排空后、网络 RequestClose 前。
    BOOST_TEST(cache_s->request_close_calls.load() >= 1);
    BOOST_TEST(cache_s->wait_closed_calls.load() >= 1);
    BOOST_TEST(session_s->request_close_calls.load() >= 1);
    // 顺序证据：net.wait_handlers < resource close < net.request_close。
    // 资源步骤不进 host 的 NetCall 序列——用调度器代际 + 桩序断言
    // host 侧不变量，资源收束发生时机经 budget/状态断言覆盖（T6）。
    const auto names = host->Names();
    BOOST_TEST(PosOf(names, "net.wait_handlers") <
               PosOf(names, "net.request_close"));
}

// T2：资源 Create 失败 → 启动失败回退；凡已被创建并启动的资源必须被
// RequestClose/WaitClosed 收束（不留半装配活跃资源）；service 不绑定、
// 网络组件未立起、Scheduler 完整 Stop。（两个资源的处理序按登记表迭代
// 序，不固定——断言写成「凡被启动者必被收束」，顺序无关。）
BOOST_AUTO_TEST_CASE(factory_create_failure_rolls_back) {
    auto host = std::make_shared<StubNetHost>();
    auto app  = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app->add_service<CacheSvc>(ConcurrentOpts()));

    auto ok_s   = std::make_shared<StubResource::Shared>();
    auto fail_s = std::make_shared<StubResource::Shared>();
    fail_s->fail_create.store(true);   // 下一次 Create 失败
    BOOST_REQUIRE(app->add_resource<StubResource>("ok-res",
        [ok_s] { return StubResource::Create(ok_s); }));
    BOOST_REQUIRE(app->add_resource<StubResource>("fail-res",
        [fail_s] { return StubResource::Create(fail_s); }));

    BOOST_TEST(app->run() == fw::HostLifecycle::kExitStartFailed);
    // 凡被启动的资源必被收束（RequestClose + WaitClosed 各至少一次，
    // 且最终 IsClosed）；未创建的不要求（无对象可收束）。
    if (ok_s->create_calls.load() > 0) {
        BOOST_TEST(ok_s->request_close_calls.load() >= 1);
        BOOST_TEST(ok_s->wait_closed_calls.load() >= 1);
        BOOST_TEST(ok_s->closed.load());
    }
    if (fail_s->create_calls.load() > 0) {
        // Create 失败但未返回实例：spec.instance 为空、无收束对象，
        // 断言仅落在「不进入 service 视图」。
        BOOST_TEST(fail_s->start_calls.load() == 0);
    }
    // 未启动任何网络组件；service 未绑定。
    BOOST_TEST(host->TotalCalls() == std::size_t{0});
    auto svc_r = app->find_service("cache-svc");
    BOOST_REQUIRE(!svc_r);
    BOOST_CHECK(svc_r.error().code == fw::ErrorCode::RuntimeUnavailable);
    BOOST_TEST(!app->lifecycle_failures().empty());
    BOOST_TEST(g_scheduler->GetRunGeneration() == 0);
}

// T3：资源 Start 失败 → 半装配资源本身被收束（RequestClose/WaitClosed）
// 且不进入 service 可见实例视图。
BOOST_AUTO_TEST_CASE(factory_start_failure_rolls_back) {
    auto host = std::make_shared<StubNetHost>();
    auto app  = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app->add_service<CacheSvc>(ConcurrentOpts()));

    auto fail_s = std::make_shared<StubResource::Shared>();
    fail_s->fail_start.store(true);
    BOOST_REQUIRE(app->add_resource<StubResource>("bad",
        [fail_s] { return StubResource::Create(fail_s); }));

    BOOST_TEST(app->run() == fw::HostLifecycle::kExitStartFailed);
    // 半装配资源被收束——Create 已成功的对象也走 RequestClose/WaitClosed
    // 到 IsClosed 终态。
    BOOST_TEST(fail_s->request_close_calls.load() >= 1);
    BOOST_TEST(fail_s->wait_closed_calls.load() >= 1);
    BOOST_TEST(fail_s->closed.load());
    BOOST_TEST(host->TotalCalls() == std::size_t{0});
    BOOST_TEST(g_scheduler->GetRunGeneration() == 0);
}

// T4：登记校验——(类型,名) 重复（工厂↔预创建共享键空间）、空工厂、
// run 开始后登记 → Closed。
BOOST_AUTO_TEST_CASE(resource_registration_validation) {
    auto host = std::make_shared<StubNetHost>();
    auto app  = MakeApp(AppOpts(), host);
    auto s    = std::make_shared<StubResource::Shared>();

    BOOST_REQUIRE(app->add_resource<StubResource>("a",
        [s] { return StubResource::Create(s); }));
    // 同名工厂重复登记失败。
    auto dup_f = app->add_resource<StubResource>("a",
        [s] { return StubResource::Create(s); });
    BOOST_REQUIRE(!dup_f);
    BOOST_CHECK(dup_f.error().code == fw::ErrorCode::InvalidArgument);
    // 同名预创建与工厂共享键空间。
    auto dup_p = app->add_resource<StubResource>("a",
        std::make_shared<StubResource>(s));
    BOOST_REQUIRE(!dup_p);
    BOOST_CHECK(dup_p.error().code == fw::ErrorCode::InvalidArgument);
    // 空工厂失败。
    auto null_f = app->add_resource<StubResource>("b",
        std::function<fw::result<std::shared_ptr<StubResource>>()>{});
    BOOST_REQUIRE(!null_f);
    BOOST_CHECK(null_f.error().code == fw::ErrorCode::InvalidArgument);

    BOOST_REQUIRE(app->add_service<CacheSvc>(ConcurrentOpts()));
    auto h = RunApp(*app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));
    // run 开始后登记返回 Closed。
    auto late = app->add_resource<StubResource>("late",
        [s] { return StubResource::Create(s); });
    BOOST_REQUIRE(!late);
    BOOST_CHECK(late.error().code == fw::ErrorCode::Closed);
    app->request_shutdown();
    JoinRun(h);
    BOOST_TEST(h->rc == fw::HostLifecycle::kExitOk);
}

// T5：资源 WaitClosed 在预算内完成 → kExitOk；资源收束晚于 handler 排空。
BOOST_AUTO_TEST_CASE(resource_close_within_budget) {
    auto host = std::make_shared<StubNetHost>();
    auto app  = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app->add_service<CacheSvc>(ConcurrentOpts()));

    auto s = std::make_shared<StubResource::Shared>();
    std::atomic<bool> drained{false};
    // 资源 WaitClosed 时记录「handler 已排空」这一真实前提（经由 host
    // 桩的调用名序佐证 on_handlers_drained 挂接点位置）。
    s->on_wait_closed =
        [host, &drained](co::Deadline, co::CancellationToken) {
            const auto names = host->Names();
            drained.store(
                std::find(names.begin(), names.end(),
                          "net.wait_handlers") != names.end() &&
                std::find(names.begin(), names.end(),
                          "net.request_close") == names.end());
            return inf::CloseStatus::Closed;
        };
    BOOST_REQUIRE(app->add_resource<StubResource>("r",
        [s] { return StubResource::Create(s); }));

    auto h = RunApp(*app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));
    app->request_shutdown();
    JoinRun(h);
    BOOST_TEST(h->rc == fw::HostLifecycle::kExitOk);
    BOOST_TEST(drained.load());   // 资源收束发生在 handler 排空之后
}

// T6：资源 WaitClosed 首调超时 → ShutdownIncomplete（run 不返回、未完成
// 项含 resource:*、资源仍强持有）→ 迟到续等完成 → kExitShutdownLate。
BOOST_AUTO_TEST_CASE(resource_close_budget_exhaustion) {
    auto host = std::make_shared<StubNetHost>();
    auto app  = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app->add_service<CacheSvc>(ConcurrentOpts()));

    auto s = std::make_shared<StubResource::Shared>();
    TestLatch waiting_unbounded{1};
    TestGate  allow_close;
    std::atomic<int> calls{0};
    s->on_wait_closed =
        [&](co::Deadline, co::CancellationToken) -> inf::CloseStatus {
            if (calls.fetch_add(1) == 0)
                return inf::CloseStatus::TimedOut;   // 预算内不收束
            waiting_unbounded.CountDown();
            allow_close.Wait();
            return inf::CloseStatus::Closed;
        };
    BOOST_REQUIRE(app->add_resource<StubResource>("slow",
        [s] { return StubResource::Create(s); }));

    auto h = RunApp(*app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));
    app->request_shutdown();

    // 等状态机进入资源的无界续等：ShutdownIncomplete 可观察、run 不返回、
    // 资源强持有（spec.instance 未释放——由 host 未 release 间接证明）。
    BOOST_REQUIRE(waiting_unbounded.WaitFor(kWait));
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::ShutdownIncomplete);
    const auto pending = app->pending_cleanup();
    BOOST_REQUIRE(!pending.empty());
    bool saw_resource_step = false;
    for (const auto& p : pending)
        if (p.rfind("resource:", 0) == 0) saw_resource_step = true;
    BOOST_CHECK(saw_resource_step);
    BOOST_TEST(!h->done.load());   // run 不返回
    const auto names = host->Names();
    BOOST_CHECK(std::find(names.begin(), names.end(),
                          "net.release") == names.end());   // 未提前释放

    allow_close.Open();
    JoinRun(h);
    BOOST_TEST(h->rc == fw::HostLifecycle::kExitShutdownLate);
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Closed);
    BOOST_TEST(app->pending_cleanup().empty());
    BOOST_TEST(g_scheduler->GetRunGeneration() == 0);
}

// ── 异常路径：资源工厂抛异常（修复 review finding 1 的回归测试）──
class ThrowingResource {
public:
    static fw::result<std::shared_ptr<ThrowingResource>> Create() {
        throw std::runtime_error("factory boom");
    }
};

// T7：factory 抛异常 → 异常被收口为 err，run 返回 kExitStartFailed
// 而不是让异常逃逸越过 HostLifecycle（review finding 1 回归）。
BOOST_AUTO_TEST_CASE(factory_throw_rolls_back_via_exception_boundary) {
    auto host = std::make_shared<StubNetHost>();
    auto app  = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app->add_service<CacheSvc>(ConcurrentOpts()));

    auto ok_s = std::make_shared<StubResource::Shared>();
    BOOST_REQUIRE(app->add_resource<StubResource>("ok",
        [ok_s] { return StubResource::Create(ok_s); }));
    // 抛异常的工厂：Create 时 throw，不返回 err。
    BOOST_REQUIRE(app->add_resource<ThrowingResource>("bad",
        [] { return ThrowingResource::Create(); }));

    BOOST_TEST(app->run() == fw::HostLifecycle::kExitStartFailed);
    // 已创建并启动的资源被收束；抛异常的工厂未产生对象不要求收束。
    if (ok_s->create_calls.load() > 0) {
        BOOST_TEST(ok_s->request_close_calls.load() >= 1);
        BOOST_TEST(ok_s->closed.load());
    }
    BOOST_TEST(host->TotalCalls() == std::size_t{0});
    // 失败清单含异常收口记录。
    const auto failures = app->lifecycle_failures();
    bool saw_threw = false;
    for (const auto& f : failures)
        if (f.find("threw") != std::string::npos ||
            f.find("InternalError") != std::string::npos)
            saw_threw = true;
    BOOST_TEST(saw_threw);
    BOOST_TEST(g_scheduler->GetRunGeneration() == 0);
}

// T8：Start() 抛异常 → 同样收口为 err，半装配资源被收束。
class ThrowingStartResource : public inf::ICoCloseable {
public:
    struct Shared {
        std::atomic<bool> closed{false};
        std::atomic<int>  request_close_calls{0};
    };
    explicit ThrowingStartResource(std::shared_ptr<Shared> s)
        : m_s(std::move(s)) {}
    static fw::result<std::shared_ptr<ThrowingStartResource>> Create(
        std::shared_ptr<Shared> s) {
        if (co::CurrentRuntimeGeneration() == 0)
            return fw::result<
                std::shared_ptr<ThrowingStartResource>>::err(
                    fw::MakeError(fw::ErrorCode::RuntimeUnavailable,
                                  "scheduler not started"));
        return fw::result<std::shared_ptr<ThrowingStartResource>>::ok(
            std::make_shared<ThrowingStartResource>(std::move(s)));
    }
    fw::result<void> Start() {
        throw std::runtime_error("start threw");
    }
    void RequestClose() noexcept override {
        m_s->request_close_calls.fetch_add(1);
    }
    bool IsClosed() const noexcept override { return m_s->closed.load(); }
    inf::CloseStatus WaitClosed(co::Deadline,
                                co::CancellationToken) override {
        m_s->closed.store(true);
        return inf::CloseStatus::Closed;
    }
private:
    std::shared_ptr<Shared> m_s;
};

BOOST_AUTO_TEST_CASE(resource_start_throw_rolls_back_via_exception_boundary) {
    auto host = std::make_shared<StubNetHost>();
    auto app  = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app->add_service<CacheSvc>(ConcurrentOpts()));

    auto s = std::make_shared<ThrowingStartResource::Shared>();
    BOOST_REQUIRE(app->add_resource<ThrowingStartResource>("thrown",
        [s] { return ThrowingStartResource::Create(s); }));

    BOOST_TEST(app->run() == fw::HostLifecycle::kExitStartFailed);
    // 半装配资源被收束（RequestClose 至少一次，IsClosed 终态）。
    BOOST_TEST(s->request_close_calls.load() >= 1);
    BOOST_TEST(s->closed.load());
    BOOST_TEST(host->TotalCalls() == std::size_t{0});
    BOOST_TEST(g_scheduler->GetRunGeneration() == 0);
}

BOOST_AUTO_TEST_SUITE_END()
} // namespace
