// v2::CoApp 装配、CoStart 前校验、生命周期
// 启动/关闭顺序、启动中途失败回退、关闭超时 StopIncomplete。
// 网络组件全部走 INetworkHost 桩（经 internal 测试缝注入）：断言
// 真实调用序列、每步发生时 coroutine runtime 已初始化、超时与异常路径；
// 无真实 socket、无 sleep 凑时序、无空断言。
#define BOOST_TEST_NO_MAIN
#include <boost/test/included/unit_test.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <string>
#include <string_view>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

extern char** environ;

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/object/CoObject.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/V2.hpp>

// 机器面（宿主桩/注册表/测试缝构造）显式走 internal 头。
#include <bbt/framework/internal/ActorRegistry.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/HostLifecycle.hpp>

#include "framework_co_harness.hpp"

namespace fw = bbt::framework;
namespace v2 = bbt::framework::v2;
namespace co = bbt::coroutine;
using Clock = std::chrono::steady_clock;

namespace {

// ── 测试屏障原语（std CV，无 sleep 凑时序）──

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

// ── INetworkHost 桩：记录每次调用的名字与当时 runtime 是否已初始化（初始化
//    在进程寿命内恒为 true）；借此把「调度器先于网络、使用期保持运行」纳入
//    同一序列断言。──

struct NetCall {
    std::string name;
    bool        sched_ready;   // 调用发生时 coroutine runtime 已初始化
};

class StubNetHost final : public fw::INetworkHost {
public:
    fw::result<void> create_rc = fw::result<void>::ok();
    fw::result<void> start_rc  = fw::result<void>::ok();
    fw::result<void> close_rc   = fw::result<void>::ok();
    fw::result<void> release_rc = fw::result<void>::ok();
    std::function<fw::result<void>(const co::WaitOptions&)> on_wait_handlers;
    std::function<void()> on_started;   // Start 成功后回调（控制线程）
    std::function<void()> on_closed;    // Close() 调用时回调（控制线程）
    TestLatch start_seen{1};   // Start() 被调用（启动完成、开始接纳）时倒计时

    fw::result<void> Create() override {
        _Record("net.create");
        return create_rc;
    }
    bbt_co_async(fw::result<void>) Start(const co::WaitOptions&) override {
        _Record("net.start");
        start_seen.CountDown();
        auto rc = start_rc;
        if (rc && on_started) on_started();
        return rc;
    }
    void StopAccepting() noexcept override { _Record("net.stop_accepting"); }
    bbt_co_async(fw::result<void>) WaitHandlersDone(
        const co::WaitOptions& d) override {
        _Record("net.wait_handlers");
        if (on_wait_handlers) return on_wait_handlers(d);
        return fw::result<void>::ok();
    }
    bbt_co_async(fw::result<void>) Close(const bbt::infra::CloseOptions&) override {
        _Record("net.close");
        if (on_closed) on_closed();
        return close_rc;
    }
    bbt_co_async(fw::result<void>) WaitPhysicalReleased(
        const co::WaitOptions&) override {
        _Record("net.wait_released");
        return release_rc;
    }
    std::optional<bbt::infra::Error> CloseReason() const override {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_close_reason;
    }

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
    void PublishCloseReason(bbt::infra::Error reason) {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (!m_close_reason)
            m_close_reason = std::move(reason);
    }

private:
    void _Record(const char* name) {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_calls.push_back(NetCall{name, g_scheduler->IsInitialized()});
    }
    mutable std::mutex m_mtx;
    std::vector<NetCall> m_calls;
    std::optional<bbt::infra::Error> m_close_reason;
};

// ── 业务桩类型：统一 CoRpcReq/CoRpcResp 公共面。──

class EchoSvc final : public v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "echo";
    explicit EchoSvc(fw::detail::ConstructKey k) : v2::CoService<>(k) {}
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value) return fw::CoRpcResp::Error(value.error());
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EchoSvc::Ping>("ping"));
protected:
    ~EchoSvc() noexcept override = default;
    friend class fw::detail::Registry;
};

// 与 EchoSvc 同名：重复注册拒绝用。
class EchoDupSvc final : public v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "echo";
    explicit EchoDupSvc(fw::detail::ConstructKey k) : v2::CoService<>(k) {}
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value) return fw::CoRpcResp::Error(value.error());
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EchoDupSvc::Ping>("ping"));
protected:
    ~EchoDupSvc() noexcept override = default;
    friend class fw::detail::Registry;
};

class KeyedSvc final : public v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "keyed";
    explicit KeyedSvc(fw::detail::ConstructKey k) : v2::CoService<>(k) {}
    fw::CoRpcResp Get(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value) return fw::CoRpcResp::Error(value.error());
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::ActorMethodAt<&KeyedSvc::Get, std::int32_t>("get"));
protected:
    ~KeyedSvc() noexcept override = default;
    friend class fw::detail::Registry;
};

// ActorSerial 缺 key 提取器（声明为普通 Method）：启动期校验失败用。
class UnkeyedSvc final : public v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "unkeyed";
    explicit UnkeyedSvc(fw::detail::ConstructKey k) : v2::CoService<>(k) {}
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value) return fw::CoRpcResp::Error(value.error());
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&UnkeyedSvc::Ping>("ping"));
protected:
    ~UnkeyedSvc() noexcept override = default;
    friend class fw::detail::Registry;
};

fw::ServiceOptions ConcurrentOpts() {
    return fw::ServiceOptions{}.SetMaxInflight(64);
}

fw::ServiceOptions ActorOpts() {
    return fw::ServiceOptions{}
        .SetMaxInflight(64)
        .SetActorSerial()
        .SetSerialQueueCapacity(16)
        .SetMaxSerialKeys(8);
}

fw::AppOptions AppOpts() {
    return fw::AppOptions{}
        .SetLimits(64, 64, 8, 64)
        .SetShutdownBudget(std::chrono::milliseconds{2000})
        .SetLifecycleBudget(std::chrono::milliseconds{2000});
}

// 测试缝构造：宿主桩 + 无出站发送（受管出站如实 RuntimeUnavailable）。
// MakeCoAppForTest 与 v2::CoApp::Create 同受「受管协程内创建」约束——
// CoRuntimeOnly 强校验，native 测试线程经 RunInCoTask 进入受管域。
std::unique_ptr<v2::CoApp> MakeApp(
    const fw::AppOptions& opts, const std::shared_ptr<StubNetHost>& host) {
    std::unique_ptr<v2::CoApp> out;
    bool ran = framework_test::RunInCoTask([&] {
        out = fw::MakeCoAppForTest(opts, fw::CoAppSeam{host, {}});
    });
    if (!ran)
        return nullptr;
    return out;
}

// 在 native 线程上驱动完整 Co 生命周期（CoStart → 等关闭请求 → CoStop）；
// 返回的句柄不可移动（atomic 成员），以 unique_ptr 持有。
struct RunHandle {
    std::thread      th;
    std::atomic<bool> done{false};
    fw::result<void> rc = fw::result<void>::err(
        fw::MakeError(fw::ErrorCode::RuntimeUnavailable, "not run"));
};

std::unique_ptr<RunHandle> RunApp(v2::CoApp& app) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 3;
    auto h = std::make_unique<RunHandle>();
    RunHandle* p = h.get();
    h->th = std::thread([p, &app] {
        p->rc = framework_test::RunAppUntilStopped(app);
        p->done.store(true, std::memory_order_release);
    });
    return h;
}

void JoinRun(const std::unique_ptr<RunHandle>& h) {
    if (h->th.joinable()) h->th.join();
}

const std::vector<std::string> kCleanOrder{
    "net.create", "net.start", "net.stop_accepting", "net.wait_handlers",
    "net.close", "net.wait_released"};

// 永久排空失败在该 Co 生命周期里是「诚实未完成」而不是永不返回：
// CoStop 用调用方给的同一绝对期限收排空；期限耗尽或排空返回非超时错误都表示
// 排空未被证实 → 状态 StopIncomplete、保留 owner/资源、如实返回错误，
// 不进入资源收束挂接点、不网络 Close、不释放；再次以更晚期限 CoStop 可推进。
enum class DrainFailure {
    NonTimeoutError,  // 初次即非超时错误 → 排空未被证实
    TimeoutThenError, // 初次 TimedOut、再次非超时错误 → 仍未证实
};

BOOST_AUTO_TEST_SUITE(framework_f1b1)

// 用例 0：永久排空失败 → 诚实未完成（不是永不返回）。排空未被证实时不进入
// 资源收束挂接点、不网络 Close、不释放 owner；状态保持 StopIncomplete，
// 未完成清理项与原始排空错误都可观察。两条路径：初次即非超时错误；先 TimedOut
// 再非超时错误。Co 生命周期不会把控制线程永久挂在等待上。
BOOST_AUTO_TEST_CASE(drain_failure_reports_incomplete_never_closes) {
    for (const DrainFailure mode :
         {DrainFailure::NonTimeoutError, DrainFailure::TimeoutThenError}) {
        auto wait_calls = std::make_shared<std::atomic<int>>(0);
        auto host = std::make_shared<StubNetHost>();
        host->on_wait_handlers =
            [mode, wait_calls](const co::WaitOptions&) -> fw::result<void> {
            const int n =
                wait_calls->fetch_add(1, std::memory_order_acq_rel) + 1;
            if (mode == DrainFailure::NonTimeoutError)
                return fw::result<void>::err(fw::MakeError(
                    fw::ErrorCode::InternalError, "permanent drain failure"));
            if (n == 1)
                return fw::result<void>::err(fw::MakeError(
                    fw::ErrorCode::TimedOut, "handlers still running"));
            return fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::InternalError, "drain failed after timeout"));
        };
        auto app = MakeApp(AppOpts(), host);
        BOOST_REQUIRE(app);
        bool ran = framework_test::RunInCoTask([&] {
            auto r = bbt_co_await app->CoInstallService<EchoSvc>(
                ConcurrentOpts());
            if (!r)
                throw std::runtime_error("install failed");
            app->RequestShutdown(); // 启动后立即进入关闭序列
        });
        BOOST_REQUIRE(ran);

        const auto rc = framework_test::RunAppUntilStopped(*app);
        BOOST_CHECK(!framework_test::CoOk(rc));
        BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);
        // 排空未证实：资源收束挂接点/网络 Close/物理释放都不得发生。
        const auto names = host->Names();
        BOOST_CHECK(std::find(names.begin(), names.end(), "net.stop_accepting") !=
                    names.end());
        BOOST_CHECK(std::find(names.begin(), names.end(), "net.close") ==
                    names.end());
        BOOST_CHECK(std::find(names.begin(), names.end(), "net.wait_released") ==
                    names.end());
        // 原始排空错误如实透出：非超时错误进错误返回；超时按 TimedOut/原码返回。
        BOOST_TEST(host->TotalCalls() > std::size_t{0});
    }
}

// 用例 0b：非超时排空错误 + 迟到收尾——初次 CoStop 不得当作已排空（保留 owner、
// 不 Close、如实返回错误并进入 StopIncomplete）；放行迟到排空后，再次以更晚
// 期限显式 CoStop 才推进到真实终态（资源收束 → 网络 Close → 物理释放）。
BOOST_AUTO_TEST_CASE(drain_error_enters_incomplete_then_second_stop_succeeds) {
    auto host = std::make_shared<StubNetHost>();
    TestGate allow_drain; // 迟到收尾放行
    std::atomic<int> calls{0};
    host->on_wait_handlers =
        [&](const co::WaitOptions&) -> fw::result<void> {
            if (calls.fetch_add(1, std::memory_order_acq_rel) == 0)
                return fw::result<void>::err(fw::MakeError(
                    fw::ErrorCode::InternalError, "drain probe failed"));
            allow_drain.Wait();
            return fw::result<void>::ok();
        };
    auto app = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app);
    bool ran = framework_test::RunInCoTask([&] {
        auto r = bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
        if (!r)
            throw std::runtime_error("install failed");
        app->RequestShutdown();
    });
    BOOST_REQUIRE(ran);

    const auto first = framework_test::RunAppUntilStopped(*app);
    BOOST_CHECK(!framework_test::CoOk(first));
    BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);
    { // 未提前网络 Close
        const auto names = host->Names();
        BOOST_CHECK(std::find(names.begin(), names.end(), "net.close") ==
                    names.end());
    }

    // 迟到排空完成 → 以新的显式期限再次收口，走完收束到 Stopped。
    allow_drain.Open();
    const auto second = framework_test::CoStop(*app);
    BOOST_TEST(framework_test::CoOk(second));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_TEST(app->PendingCleanup().empty());
    const auto after = host->Names();
    BOOST_CHECK(std::find(after.begin(), after.end(), "net.close") !=
                after.end());
    BOOST_CHECK(std::find(after.begin(), after.end(), "net.wait_released") !=
                after.end());
}

// 用例 1：CoInstallService 注册成功；同服务名重复注册被明确拒绝；服务对象
// 的受管句柄可读回（kind/generation/name）。
BOOST_AUTO_TEST_CASE(install_service_registers_and_rejects_duplicate) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app);

    fw::result<void> r2 = fw::result<void>::ok();
    fw::result<void> dup = fw::result<void>::ok();
    bool ran = framework_test::RunInCoTask([&] {
        auto a = bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
        auto b = bbt_co_await app->CoInstallService<KeyedSvc>(ActorOpts());
        if (!a)
            r2 = std::move(a);
        else if (!b)
            r2 = std::move(b);
        else
            dup = bbt_co_await app->CoInstallService<EchoDupSvc>(
                ConcurrentOpts());
    });
    BOOST_REQUIRE(ran);
    BOOST_REQUIRE(framework_test::CoOk(r2));
    BOOST_REQUIRE(!framework_test::CoOk(dup));
    BOOST_CHECK(dup.error().code == fw::ErrorCode::InvalidArgument);

    // Concurrent：服务句柄以注册名取回，ObjectInfo 快照读回 kind/name。
    auto svc = app->FindService("echo");
    BOOST_REQUIRE(static_cast<bool>(svc));
    BOOST_TEST(svc.value().IsValid());
    auto info = svc.value().Info();
    BOOST_REQUIRE(static_cast<bool>(info));
    BOOST_CHECK(info.value().kind == fw::ObjectKind::Service);
    BOOST_TEST(info.value().name == "echo");

    // ActorSerial：服务类型登记成功；v2 的 FindActor 按 (type,key) 查实例，
    // 未激活的 key 如实 NotFound（Find 不激活对象）。
    auto missing = app->FindActor(fw::ActorId{"keyed", "k-1"});
    BOOST_REQUIRE(!static_cast<bool>(missing));
    BOOST_CHECK(missing.error().code == fw::ErrorCode::NotFound);

    // 未注册名取回明确错误。
    BOOST_CHECK(!app->FindService("no-such"));
}

// 用例 2：ServiceOptions 违反执行策略校验 → CoInstallService 失败，任何组件未
// 启动（桩计数 0）。
BOOST_AUTO_TEST_CASE(invalid_options_fail_before_any_startup) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app);

    fw::result<void> r1 = fw::result<void>::ok();
    fw::result<void> r3 = fw::result<void>::ok();
    fw::result<void> r4 = fw::result<void>::ok();
    fw::result<void> r5 = fw::result<void>::ok();
    bool ran = framework_test::RunInCoTask([&] {
        r1 = bbt_co_await app->CoInstallService<EchoSvc>(
            fw::ServiceOptions{}.SetMaxInflight(0));
        // max_inflight == 0 → InvalidArgument
        r3 = bbt_co_await app->CoInstallService<KeyedSvc>(
            fw::ServiceOptions{}.SetMaxInflight(64).SetActorSerial());
        // ActorSerial 缺 mailbox/key 额度 → InvalidArgument
        r4 = bbt_co_await app->CoInstallService<UnkeyedSvc>(ActorOpts());
        // ActorSerial 服务存在无 key 提取器方法 → 装配期失败
        r5 = bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
    });
    BOOST_REQUIRE(ran);
    BOOST_REQUIRE(!framework_test::CoOk(r1));
    BOOST_CHECK(r1.error().code == fw::ErrorCode::InvalidArgument);
    BOOST_REQUIRE(!framework_test::CoOk(r3));
    BOOST_CHECK(r3.error().code == fw::ErrorCode::InvalidArgument);
    BOOST_REQUIRE(!framework_test::CoOk(r4));
    BOOST_CHECK(r4.error().code == fw::ErrorCode::InvalidArgument);
    BOOST_REQUIRE(framework_test::CoOk(r5));

    // 装配期拒绝不启动任何网络组件：桩计数为 0。
    BOOST_TEST(host->TotalCalls() == std::size_t{0});
}

// 用例 3：未显式配置的出站目标被拒绝——不存在「自动连任意服务」；
// 拒绝发生在任何网络动作之前（桩计数 0）。
BOOST_AUTO_TEST_CASE(unconfigured_route_target_rejected) {
    auto host = std::make_shared<StubNetHost>();
    auto options = AppOpts();
    options.AddRoute("svc.allowed", "127.0.0.1:9001");
    auto app = MakeApp(options, host);
    BOOST_REQUIRE(app);

    auto bad = app->FindRoute("svc.unknown");
    BOOST_REQUIRE(!bad);
    BOOST_CHECK(bad.error().code == fw::ErrorCode::NotFound);
    BOOST_TEST(bad.error().message.find("svc.unknown") != std::string::npos);

    auto good = app->FindRoute("svc.allowed");
    BOOST_REQUIRE(good);
    BOOST_TEST(good.value() == "127.0.0.1:9001");

    // 拒绝发生在网络组件启动之前：桩一次都未被调用。
    BOOST_TEST(host->TotalCalls() == std::size_t{0});
}

// 用例 4：启动顺序 = runtime 初始化 → 网络 Create/Start（开始接纳）→ 运行；
// 桩调用点记录「runtime 已初始化」证明调度器先于网络、且整个使用期保持运行。
BOOST_AUTO_TEST_CASE(startup_order_scheduler_then_network) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app);
    bool ran = framework_test::RunInCoTask([&] {
        auto r = bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
        if (!r)
            throw std::runtime_error("install failed");
    });
    BOOST_REQUIRE(ran);

    auto h = RunApp(*app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));
    BOOST_CHECK(app->State() == fw::AppState::Running);

    app->RequestShutdown();
    JoinRun(h);
    BOOST_TEST(framework_test::CoOk(h->rc));

    const auto calls = host->Snapshot();
    std::vector<std::string> names;
    for (const auto& c : calls) names.push_back(c.name);
    BOOST_TEST(names == kCleanOrder);
    // 全部网络调用都发生在 runtime 已初始化之后（初始化先于 net.create，
    // 且进程寿命内不复位）。
    for (const auto& c : calls)
        BOOST_TEST(c.sched_ready);
}

// 用例 5：关闭顺序 = StopAccepting → 等 handler 结束 → Close；StopAccepting
// 严格先于 Close。
BOOST_AUTO_TEST_CASE(shutdown_order_stop_accepting_before_close) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app);
    bool ran = framework_test::RunInCoTask([&] {
        auto r = bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
        if (!r)
            throw std::runtime_error("install failed");
    });
    BOOST_REQUIRE(ran);

    auto h = RunApp(*app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));
    app->RequestShutdown();
    JoinRun(h);
    BOOST_TEST(framework_test::CoOk(h->rc));

    const auto calls = host->Snapshot();
    auto pos = [&calls](const char* n) -> std::size_t {
        for (std::size_t i = 0; i < calls.size(); ++i)
            if (calls[i].name == n) return i;
        return calls.size();
    };
    BOOST_TEST(pos("net.stop_accepting") < pos("net.wait_handlers"));
    BOOST_TEST(pos("net.wait_handlers") < pos("net.close"));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
}

// 用例 6：启动中途失败——Create 失败只回退（网络关闭序列不适用）；Start
// 失败如实返回，不留半启动状态（CoStart 返回错误）。
BOOST_AUTO_TEST_CASE(startup_failure_rolls_back_in_order) {
    {   // 失败在第 1 个网络步（Create）
        auto host = std::make_shared<StubNetHost>();
        host->create_rc = fw::result<void>::err(
            fw::MakeError(fw::ErrorCode::InternalError, "create boom"));
        auto app = MakeApp(AppOpts(), host);
        BOOST_REQUIRE(app);
        bool ran = framework_test::RunInCoTask([&] {
            auto r = bbt_co_await app->CoInstallService<EchoSvc>(
                ConcurrentOpts());
            if (!r)
                throw std::runtime_error("install failed");
        });
        BOOST_REQUIRE(ran);

        BOOST_TEST(!framework_test::CoOk(framework_test::CoStart(*app)));
        // Create 未成功 → 网络关闭序列不适用。
        BOOST_TEST(host->Names() == std::vector<std::string>{"net.create"});
    }
    {   // 失败在第 2 个网络步（Start）：Create 已立起 → Start 如实失败
        auto host = std::make_shared<StubNetHost>();
        host->start_rc = fw::result<void>::err(
            fw::MakeError(fw::ErrorCode::Unavailable, "start boom"));
        auto app = MakeApp(AppOpts(), host);
        BOOST_REQUIRE(app);
        bool ran = framework_test::RunInCoTask([&] {
            auto r = bbt_co_await app->CoInstallService<EchoSvc>(
                ConcurrentOpts());
            if (!r)
                throw std::runtime_error("install failed");
        });
        BOOST_REQUIRE(ran);

        BOOST_TEST(!framework_test::CoOk(framework_test::CoStart(*app)));
        const auto calls = host->Snapshot();
        for (const auto& c : calls)
            BOOST_TEST(c.sched_ready);   // 启动期间 runtime 已初始化
    }
}

// 用例 7：关闭等待超时——桩让 handler 在预算内不结束：CoStop 如实返回
// TimedOut/错误、状态 StopIncomplete、保留 owner、不伪装成功；不进入
// 资源收束/网络 Close/物理释放。迟到收尾完成后，再次以更晚期限显式收口才
// 推进到 Stopped。进程不退出、无 _Exit/abort、无永久保活。
BOOST_AUTO_TEST_CASE(shutdown_timeout_reports_incomplete_not_success) {
    auto host = std::make_shared<StubNetHost>();
    TestGate allow_drain; // 迟到收尾放行
    std::atomic<int> drain_calls{0};

    host->on_wait_handlers =
        [&](const co::WaitOptions&) -> fw::result<void> {
            if (drain_calls.fetch_add(1) == 0) {
                // 预算内不结束 → 明确超时（不用 sleep：直接报 TimedOut，
                // 由状态机决定后续）。
                return fw::result<void>::err(fw::MakeError(
                    fw::ErrorCode::TimedOut, "handlers still running"));
            }
            allow_drain.Wait();
            return fw::result<void>::ok();
        };

    auto app = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app);
    bool ran = framework_test::RunInCoTask([&] {
        auto r = bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
        if (!r)
            throw std::runtime_error("install failed");
        app->RequestShutdown();
    });
    BOOST_REQUIRE(ran);

    const auto first = framework_test::RunAppUntilStopped(*app);
    BOOST_REQUIRE(!framework_test::CoOk(first));
    BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);
    // StopAccepting 已在超时前发生；网络 Close / 物理释放未提前发生。
    const auto names = host->Names();
    BOOST_CHECK(std::find(names.begin(), names.end(),
                          "net.stop_accepting") != names.end());
    BOOST_CHECK(std::find(names.begin(), names.end(),
                          "net.close") == names.end());
    BOOST_CHECK(std::find(names.begin(), names.end(),
                          "net.wait_released") == names.end());

    // 迟到收尾完成：再次以新的显式期限收口，完整走完后序步骤。
    allow_drain.Open();
    const auto second = framework_test::CoStop(*app);
    BOOST_TEST(framework_test::CoOk(second));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_TEST(app->PendingCleanup().empty());
    const auto after = host->Names();
    BOOST_CHECK(std::find(after.begin(), after.end(),
                          "net.close") != after.end());
    BOOST_CHECK(std::find(after.begin(), after.end(),
                          "net.wait_released") != after.end());
}

// 用例 8：两个 seam app 各自独立装配——v2 没有「至多一个活跃 CoApp」的全局
// 约束；进程单槽的只读入口闸门只承载一次真实网络代（HostGate），两个 app
// 各自经 seam host 完成启动/收束。
BOOST_AUTO_TEST_CASE(two_seam_apps_start_and_stop_independently) {
    auto host1 = std::make_shared<StubNetHost>();
    auto app1 = MakeApp(AppOpts(), host1);
    BOOST_REQUIRE(app1);
    auto host2 = std::make_shared<StubNetHost>();
    auto app2 = MakeApp(AppOpts(), host2);
    BOOST_REQUIRE(app2);

    bool ran = framework_test::RunInCoTask([&] {
        auto r1 = bbt_co_await app1->CoInstallService<EchoSvc>(
            ConcurrentOpts());
        auto r2 = bbt_co_await app2->CoInstallService<EchoSvc>(
            ConcurrentOpts());
        if (!r1 || !r2)
            throw std::runtime_error("install failed");
    });
    BOOST_REQUIRE(ran);

    auto h1 = RunApp(*app1);
    BOOST_REQUIRE(host1->start_seen.WaitFor(kWait));
    auto h2 = RunApp(*app2);
    BOOST_REQUIRE(host2->start_seen.WaitFor(kWait));

    app1->RequestShutdown();
    app2->RequestShutdown();
    JoinRun(h1);
    JoinRun(h2);
    BOOST_TEST(framework_test::CoOk(h1->rc));
    BOOST_TEST(framework_test::CoOk(h2->rc));

    // 同一实例不可重入 CoStart。
    BOOST_TEST(!framework_test::CoOk(framework_test::CoStart(*app1)));

    // 顺序复用单例调度器：收束后新实例可正常 Start/Stop。
    auto host3 = std::make_shared<StubNetHost>();
    auto app3 = MakeApp(AppOpts(), host3);
    BOOST_REQUIRE(app3);
    ran = framework_test::RunInCoTask([&] {
        auto r = bbt_co_await app3->CoInstallService<EchoSvc>(
            ConcurrentOpts());
        if (!r)
            throw std::runtime_error("install failed");
    });
    BOOST_REQUIRE(ran);
    auto h3 = RunApp(*app3);
    BOOST_REQUIRE(host3->start_seen.WaitFor(kWait));
    app3->RequestShutdown();
    JoinRun(h3);
    BOOST_TEST(framework_test::CoOk(h3->rc));
}

// 补充：早于 CoStart 的 RequestShutdown → 完成启动后立即进入关闭序列。
BOOST_AUTO_TEST_CASE(shutdown_requested_before_start_closes_immediately) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app);
    bool ran = framework_test::RunInCoTask([&] {
        auto r = bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
        if (!r)
            throw std::runtime_error("install failed");
        app->RequestShutdown();
    });
    BOOST_REQUIRE(ran);

    BOOST_TEST(framework_test::CoOk(framework_test::RunAppUntilStopped(*app)));
    const auto names = host->Names();
    BOOST_TEST(names == kCleanOrder);
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace

boost::unit_test::test_suite* init_unit_test(int, char**) { return nullptr; }

int main(int argc, char* argv[]) {
    return ::boost::unit_test::unit_test_main(&init_unit_test, argc, argv);
}
