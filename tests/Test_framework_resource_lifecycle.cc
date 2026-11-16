/**
 * 命名资源（受管组件）的装配相位、启动失败回滚与关闭收束语义
 * （framework.resource_lifecycle）。
 *
 * 旧 add_resource/ServiceContext 资源缝已删除；资源在新对象模型中是
 * CoComponent：Create 相位 = 组件 OnStart 内的资源创建（经工厂在测试内
 * 模拟 infra CoRedisCli 资源形态——静态 Create 要求运行时已初始化、实例
 * Start() -> result<void>），Close 相位 = 组件 OnStop 内的同步 Close
 * （infra 关闭是同步契约：Close() 返回即物理释放，无等待）。
 * 生命周期全部经 MakeCoAppForTest + INetworkHost 桩驱动（无真实 socket、
 * 无 sleep 凑时序、真实断言调用序与运行时就绪），断言运行在真实受管协程。
 *
 * 覆盖（沿用旧用例名，语义逐条对应）：
 * T1 factory_resources_start_and_close_in_order
 * 两个同名类型不同名工厂资源在运行时就绪后创建并各 Start 一次；两个
 * Service 借同名资源得到同一实例、不同名隔离；资源同步 Close 发生在
 * handler 排空之后。
 * T2 factory_create_failure_rolls_back
 * 资源创建失败 → CoStart 返回错误、App 仍置 Running（冻结契约：首个
 * hook 失败对象记 Failed、部分启动诚实反映）；已先启动的资源保留，
 * 最终由显式 CoStop 同步 Close 收束（不留半装配活跃资源）；网络组件
 * 未启动、服务未进入 Running。
 * T3 factory_start_failure_rolls_back
 * 第二个资源 Start 失败 → CoStart 返回错误、App 仍置 Running；失败对象
 * 保留 owner（Failed），经显式卸载收口被同步 Close 收束且服务未 Running。
 * T4 resource_registration_validation
 * (类型,名) 重复登记失败（工厂/预创建键空间合并为同一受管键空间）；
 * App 已 Stopped 后登记返回 Closed。
 * T5 resource_close_after_handler_drain
 * 资源同步 Close 晚于 handler 排空；运行收束完整。
 * T6 resource_close_synchronous_and_idempotent
 * 一次关闭序列里每个资源只 Close 一次，无未完成收口项。
 * T7 factory_throw_rolls_back_via_exception_boundary
 * 资源创建抛异常 → 收口为 err，已启动资源保留、最终显式 CoStop 收束，
 * 失败记录可观测。
 * T8 resource_start_throw_rolls_back_via_exception_boundary
 * 资源 Start 抛异常 → 同样收口为 err；失败对象保留 owner，经显式卸载
 * 收口被同步 Close 收束。
 */
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/framework/Framework.hpp>

/**
 * 机器面（测试缝构造/桩宿主）显式走 internal 头。
 */
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/NetworkHost.hpp>

namespace fw  = bbt::framework;
using bbt::framework::result;
using bbt::framework::ErrorCode;
using Clock = std::chrono::steady_clock;

namespace {

/**
 * 旧 run() 退出码在本测试内的对应映射：kExitOk = CoStart/CoWaitShutdown
 * /CoStop 全部 ok 且 App 终态 Stopped；kExitStartFailed = CoStart 返回错误。
 */
constexpr int kExitOk = 0;
constexpr int kExitStartFailed = 1;

/**
 * ── INetworkHost 桩：记录调用名。──
 */
class StubNetHost final : public fw::INetworkHost {
public:
    fw::result<void> Create() override { _Record("net.create");
        return fw::result<void>::ok(); }
    fw::result<void> Start() override { _Record("net.start");
        return fw::result<void>::ok(); }
    void StopAccepting() noexcept override { _Record("net.stop_accepting"); }
    fw::result<void> WaitHandlersDone(bbt::coroutine::Deadline) override {
        _Record("net.wait_handlers");
        return fw::result<void>::ok();
    }
    void Close() noexcept override { _Record("net.close"); }

    std::vector<std::string> Names() const {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_calls;
    }
    std::size_t CountOf(const std::string& name) const {
        std::lock_guard<std::mutex> lk(m_mtx);
        return static_cast<std::size_t>(
            std::count(m_calls.begin(), m_calls.end(), name));
    }
    bool Saw(const std::string& name) const { return CountOf(name) > 0; }
private:
    void _Record(const char* name) {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_calls.emplace_back(name);
    }
    mutable std::mutex      m_mtx;
    std::vector<std::string> m_calls;
};

/**
 * 桩资源共享观测：模拟旧 StubResource::Shared——create/start/close 计数、
 * 「创建时运行时已就绪」、Close 时观测钩子（顺序证据）与失败注入
 * （consume-once 语义）。
 */
struct ResourceShared {
    std::atomic<int> create_calls{0};
    std::atomic<int> start_calls{0};
    std::atomic<int> close_calls{0};
    /**
     * 创建时记录「运行时就绪」（证明在 scheduler Start 之后）。
     */
    std::atomic<bool> created_after_scheduler{false};
    /**
     * Close() 观测钩子（顺序证据：在 Close 时读 host 调用序）。
     */
    std::function<void()> on_close;
    std::atomic<bool> closed{false};
    std::atomic<bool> started{false};
    /**
     * 失败注入：下一次创建/启动返回 err（consume-once 语义）。
     */
    std::atomic<bool> fail_create{false};
    std::atomic<bool> fail_start{false};
};

/**
 * 桩资源组件：新模型中的「命名资源」= App scope CoComponent。
 * Create/Start 相位在 OnStart（受管协程内，运行时必然就绪）；
 * Close 相位在 OnStop（收口序列同步执行，幂等记账靠 Shared.closed）。
 */
class StubResource final : public fw::CoComponent {
public:
    struct Options { std::shared_ptr<ResourceShared> s; };

    explicit StubResource(fw::detail::ConstructKey k, Options o)
        : fw::CoComponent(k), m_s(std::move(o.s)) {}

protected:
    ~StubResource() noexcept override = default;

    result<void> OnStart() override {
        m_s->create_calls.fetch_add(1);
        if (m_s->fail_create.exchange(false))
            return result<void>::err(fw::MakeError(
                ErrorCode::Unavailable, "create boom"));
        /**
         * Create 要求运行时已初始化，否则如实失败。
         */
        if (!g_scheduler->IsInitialized())
            return result<void>::err(fw::MakeError(
                ErrorCode::RuntimeUnavailable, "scheduler not started"));
        m_s->created_after_scheduler.store(true, std::memory_order_release);
        /**
         * 资源 Start 相位
         */
        m_s->start_calls.fetch_add(1);
        if (m_s->fail_start.exchange(false))
            return result<void>::err(fw::MakeError(
                ErrorCode::Unavailable, "start boom"));
        m_s->started.store(true);
        return result<void>::ok();
    }

    /**
     * infra 关闭是同步契约：Close() 返回即物理释放、幂等、noexcept。
     */
    result<void> OnStop() override {
        m_s->close_calls.fetch_add(1);
        if (m_s->on_close) m_s->on_close();
        m_s->closed.store(true);
        return result<void>::ok();
    }

private:
    std::shared_ptr<ResourceShared> m_s;
    friend class fw::detail::Registry;
};

/**
 * 不可关闭资源（无 Close 记账）：证明非可关闭资源不产生 Close 记录。
 */
class PlainResource final : public fw::CoComponent {
public:
    struct Options { int n = 0; };
    explicit PlainResource(fw::detail::ConstructKey k, Options o)
        : fw::CoComponent(k), m_n(o.n) {}
    int Value() const noexcept { return m_n; }
protected:
    ~PlainResource() noexcept override = default;
private:
    int m_n;
    friend class fw::detail::Registry;
};

/**
 * ── 业务桩服务：经声明式绑定 + Borrow 消费命名资源。──
 */
class CacheSvc final : public fw::CoService<StubResource, PlainResource> {
public:
    static constexpr std::string_view kServiceName = "cache-svc";
    explicit CacheSvc(fw::detail::ConstructKey k)
        : fw::CoService<StubResource, PlainResource>(k) {}
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto v = req.Parse<std::int32_t>();
        if (!v) return fw::CoRpcResp::Error(v.error());
        return fw::CoRpcResp::From(v.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&CacheSvc::Ping>("ping"));

protected:
    ~CacheSvc() noexcept override = default;

private:
    friend class fw::detail::Registry;
};

class SecondSvc final : public fw::CoService<StubResource> {
public:
    static constexpr std::string_view kServiceName = "second-svc";
    explicit SecondSvc(fw::detail::ConstructKey k)
        : fw::CoService<StubResource>(k) {}
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto v = req.Parse<std::int32_t>();
        if (!v) return fw::CoRpcResp::Error(v.error());
        return fw::CoRpcResp::From(v.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&SecondSvc::Ping>("ping"));

protected:
    ~SecondSvc() noexcept override = default;

private:
    friend class fw::detail::Registry;
};

fw::ServiceOptions ServiceOpts() {
    return fw::ServiceOptions{}
        .SetExecution(fw::ExecutionPolicy::Concurrent)
        .SetMaxInflight(64);
}

fw::AppOptions AppOpts() {
    return fw::AppOptions{}
        .SetLimits(64, 64, 64, 64)
        .SetShutdownBudget(std::chrono::milliseconds{1500})
        .SetLifecycleBudget(std::chrono::milliseconds{1500})
        .SetNetworkLimits(bbt::infra::NetworkLimits{
            64, 64, 8192, 65536, std::chrono::milliseconds{30000}})
        .SetLocalIdentity(std::string{"resource-lifecycle-test"});
}

std::shared_ptr<fw::CoApp> MakeApp(const fw::AppOptions& opts,
                                   const std::shared_ptr<StubNetHost>& host) {
    return fw::MakeCoAppForTest(opts, fw::CoAppSeam{host, {}});
}

/**
 * 网络宿主未被启动的证据：无 create/start/stop_accepting/wait_handlers
 * 记录（启动失败回退会同步 Close 未启动宿主，net.close 允许出现）。
 */
void CheckNetworkNeverStarted(const StubNetHost& host) {
    for (const char* step : {"net.create", "net.start",
                             "net.stop_accepting", "net.wait_handlers"})
        BOOST_TEST(host.Saw(step) == false,
                   "unexpected network step " << step);
}

/**
 * 在受管协程内执行 fn：经 g_scheduler 投递协程任务，宿主线程用
 * 限时 std::cv 等其完成（宿主线程的 CV 等待不违反 worker 阻塞禁令）。
 * 返回 false 表示任务未在预算内完成（防挂死测试进程）。
 */
template <class F>
bool RunInManagedCoroutine(F&& fn,
    std::chrono::milliseconds budget = std::chrono::milliseconds{10000}) {
    if (bbt::coroutine::IsInCoroutineContext()) { fn(); return true; }
    /**
     * native 测试宿主负责初始化唯一 runtime，产品 App 不启动 runtime。
     */
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 3;
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
    /**
     * 不让仍在途任务使用已退出的测试栈。
     */
    if (!done)
        std::terminate();
    if (slot->error) std::rethrow_exception(slot->error);
    return done;
}

#define CO_TEST_CASE(name) \
    static void name##_body(); \
    BOOST_AUTO_TEST_CASE(name) { BOOST_REQUIRE(RunInManagedCoroutine(name##_body)); } \
    static void name##_body()

BOOST_AUTO_TEST_SUITE(framework_resource)

/**
 * T1：两个命名工厂资源在运行时就绪后 Create/Start 各一次；两个服务借
 * 同名实例一致、不同名隔离；资源同步 Close 发生在 handler 排空之后。
 */
CO_TEST_CASE(factory_resources_start_and_close_in_order) {
    auto host = std::make_shared<StubNetHost>();
    auto app  = MakeApp(AppOpts(), host);

    auto cache_s   = std::make_shared<ResourceShared>();
    auto session_s = std::make_shared<ResourceShared>();
    /**
     * Close 时观测：handler 排空已记录、网络宿主尚未 Close（顺序证据）。
     */
    std::atomic<bool> cache_drained_first{false};
    std::atomic<bool> session_drained_first{false};
    cache_s->on_close = [&host, &cache_drained_first]() {
        cache_drained_first.store(host->Saw("net.wait_handlers") &&
                                  !host->Saw("net.close"));
    };
    session_s->on_close = [&host, &session_drained_first]() {
        session_drained_first.store(host->Saw("net.wait_handlers") &&
                                    !host->Saw("net.close"));
    };

    /**
     * 资源（组件）先安装，服务按模板声明集合允许绑定（服务安装不展开
     * 绑定，共享/隔离断言走用例内显式 CoBindComponent——与旧
     * 「两个服务都按名取同一资源」等价）。
     */
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<StubResource>("cache",
        StubResource::Options{cache_s}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<StubResource>("session",
        StubResource::Options{session_s}));
    /**
     * 预创建资源与工厂共存（旧 plain 预创建共享资源：非可关闭形态）。
     */
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<PlainResource>("plain",
        PlainResource::Options{42}));

    BOOST_REQUIRE(bbt_co_await app->CoInstallService<CacheSvc>(ServiceOpts()));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<SecondSvc>(ServiceOpts()));

    auto started = bbt_co_await app->CoStart();
    BOOST_REQUIRE(started);

    /**
     * Create/Start 各一次，且发生在运行时就绪之后。
     */
    BOOST_TEST(cache_s->create_calls.load() == 1);
    BOOST_TEST(cache_s->start_calls.load() == 1);
    BOOST_TEST(cache_s->created_after_scheduler.load());
    BOOST_TEST(session_s->create_calls.load() == 1);
    BOOST_TEST(session_s->start_calls.load() == 1);

    /**
     * 两个服务借同名资源得到同一托管实例；不同名隔离。
     * （旧 context().resource<R>(name) 的对应物：声明绑定 + Borrow，
     * borrow 用后即弃、不存出。）
     */
    {
        auto svc1 = app->FindService("cache-svc");
        auto svc2 = app->FindService("second-svc");
        BOOST_REQUIRE(svc1 && svc2);
        auto cache1 = bbt_co_await app->CoBindComponent<StubResource>(svc1.value(), "cache");
        auto cache2 = bbt_co_await app->CoBindComponent<StubResource>(svc2.value(), "cache");
        auto sess1  = bbt_co_await app->CoBindComponent<StubResource>(svc1.value(), "session");
        BOOST_REQUIRE(cache1 && cache2 && sess1);
        auto b1 = cache1.value().Borrow();
        auto b2 = cache2.value().Borrow();
        auto bs = sess1.value().Borrow();
        BOOST_REQUIRE(b1 && b2 && bs);
        /**
         * 同名共享同一实例
         */
        BOOST_CHECK(&b1.value().Get() == &b2.value().Get());
        /**
         * 不同名隔离
         */
        BOOST_CHECK(&b1.value().Get() != &bs.value().Get());

        auto missing = bbt_co_await app->CoBindComponent<StubResource>(svc1.value(), "missing");
        BOOST_CHECK(!missing && missing.error().code == ErrorCode::NotFound);

        /**
         * 预创建资源同视图可见。
         */
        auto plain_h = bbt_co_await app->CoBindComponent<PlainResource>(svc1.value(), "plain");
        BOOST_REQUIRE(plain_h);
        auto bp = plain_h.value().Borrow();
        BOOST_REQUIRE(bp);
        BOOST_TEST(bp.value().Get().Value() == 42);
    }

    /**
     * 外部线程请求关闭（RequestShutdown 跨线程幂等），协程等待并收口。
     */
    std::thread req([&app] { app->RequestShutdown(); });
    req.join();
    bbt::coroutine::WaitOptions wait;
    wait.deadline = Clock::now() + std::chrono::seconds(5);
    BOOST_REQUIRE(bbt_co_await app->CoWaitShutdownRequest(wait));
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_CHECK(app->State() == fw::AppState::Stopped);

    /**
     * 资源同步 Close 各一次；顺序证据：资源 Close 时 handler 排空已记录、
     * 网络宿主尚未 Close（handler 排空后、网络 Close 前）。
     */
    BOOST_TEST(cache_s->close_calls.load() == 1);
    BOOST_TEST(session_s->close_calls.load() == 1);
    BOOST_TEST(cache_s->closed.load());
    BOOST_TEST(session_s->closed.load());
    BOOST_TEST(cache_drained_first.load());
    BOOST_TEST(session_drained_first.load());
}

/**
 * T2：资源 Create 失败 → 启动失败回退；凡已被创建并启动的资源必须被
 * 同步 Close 收束（不留半装配活跃资源）；网络组件未启动、服务未进入
 * Running。（两个资源的处理序按拓扑迭代序，不固定——断言写成
 * 「凡被启动者必被收束」，顺序无关。）
 */
CO_TEST_CASE(factory_create_failure_rolls_back) {
    auto host = std::make_shared<StubNetHost>();
    auto app  = MakeApp(AppOpts(), host);

    auto ok_s   = std::make_shared<ResourceShared>();
    auto fail_s = std::make_shared<ResourceShared>();
    /**
     * 下一次 Create 失败
     */
    fail_s->fail_create.store(true);
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<StubResource>("ok-res",
        StubResource::Options{ok_s}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<StubResource>("fail-res",
        StubResource::Options{fail_s}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<CacheSvc>(ServiceOpts()));

    auto started = bbt_co_await app->CoStart();
    BOOST_CHECK(!started);
    BOOST_CHECK(started.error().code == ErrorCode::Unavailable);
    /**
     * 冻结契约：首个 hook 失败原错误透出、失败对象记 Failed，App 仍置
     * Running（部分启动诚实反映）；已启动对象保留，不虚报事务回滚。
     */
    BOOST_CHECK(app->State() == fw::AppState::Running);
    /**
     * 已启动资源保留（不被本次失败收口）；失败对象 owner 保留（Failed，
     * 仍可 Find/卸载）。创建失败的资源未进入 Start 相位。
     */
    if (ok_s->create_calls.load() > 0) {
        BOOST_TEST(ok_s->close_calls.load() == 0);
        BOOST_TEST(!ok_s->closed.load());
    }
    if (fail_s->create_calls.load() > 0) {
        /**
         * Create 失败：Start 相位未执行。
         */
        BOOST_TEST(fail_s->start_calls.load() == 0);
    }
    /**
     * 未启动任何网络组件；服务未进入 Running。
     */
    CheckNetworkNeverStarted(*host);
    auto svc_h = app->FindService("cache-svc");
    BOOST_REQUIRE(svc_h);
    auto info = svc_h.value().Info();
    BOOST_REQUIRE(info);
    BOOST_CHECK(info.value().state == fw::ObjectState::Installed);
    BOOST_TEST(!app->LifecycleFailures().empty());
    /**
     * 已启动资源不留活跃状态的收尾由显式 CoStop 完成：OnStop 同步 Close
     * 到 closed 终态。
     */
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    if (ok_s->create_calls.load() > 0) {
        BOOST_TEST(ok_s->close_calls.load() >= 1);
        BOOST_TEST(ok_s->closed.load());
    }
}

/**
 * T3：资源 Start 失败 → 半装配资源本身被同步 Close 收束且服务未进入
 * Running。
 */
CO_TEST_CASE(factory_start_failure_rolls_back) {
    auto host = std::make_shared<StubNetHost>();
    auto app  = MakeApp(AppOpts(), host);

    auto fail_s = std::make_shared<ResourceShared>();
    fail_s->fail_start.store(true);
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<StubResource>("bad",
        StubResource::Options{fail_s}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<CacheSvc>(ServiceOpts()));

    auto started = bbt_co_await app->CoStart();
    BOOST_CHECK(!started);
    BOOST_CHECK(started.error().code == ErrorCode::Unavailable);
    /**
     * 冻结契约：首个 hook 失败原错误透出、失败对象记 Failed，App 仍置
     * Running（部分启动诚实反映）；已启动对象保留，不回退。
     */
    BOOST_CHECK(app->State() == fw::AppState::Running);
    /**
     * 失败对象保留 owner（Failed，仍可 Find/卸载）。「半装配资源不留
     * 活跃状态」由显式卸载收口完成：OnStop 同步 Close 到 closed 终态。
     */
    auto bad_h = app->FindComponent<StubResource>("bad");
    BOOST_REQUIRE(bad_h);
    auto bad_info = bad_h.value().Info();
    BOOST_REQUIRE(bad_info);
    BOOST_CHECK(bad_info.value().state == fw::ObjectState::Failed);
    BOOST_REQUIRE(bbt_co_await app->CoUninstallComponent<StubResource>("bad"));
    BOOST_TEST(fail_s->close_calls.load() >= 1);
    BOOST_TEST(fail_s->closed.load());
    CheckNetworkNeverStarted(*host);
}

/**
 * T4：登记校验——(类型,名) 重复登记失败；App 已 Stopped 后登记返回
 * Closed。（旧「空工厂失败」与「工厂↔预创建共享键空间」随工厂/预创建
 * 形态删除：受管组件只有 Options 构造一种装配形态，键空间语义保留为
 * 同 (类型,名) 唯一。）
 */
CO_TEST_CASE(resource_registration_validation) {
    auto host = std::make_shared<StubNetHost>();
    auto app  = MakeApp(AppOpts(), host);
    auto s    = std::make_shared<ResourceShared>();

    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<StubResource>("a",
        StubResource::Options{s}));
    /**
     * 同名重复登记失败。
     */
    auto dup = bbt_co_await app->CoInstallComponent<StubResource>("a",
        StubResource::Options{s});
    BOOST_REQUIRE(!dup);
    BOOST_CHECK(dup.error().code == ErrorCode::InvalidArgument);

    BOOST_REQUIRE(bbt_co_await app->CoInstallService<CacheSvc>(ServiceOpts()));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    BOOST_CHECK(app->State() == fw::AppState::Running);
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    /**
     * App 已 Stopped：登记返回 Closed。
     */
    auto late = bbt_co_await app->CoInstallComponent<StubResource>("late",
        StubResource::Options{s});
    BOOST_REQUIRE(!late);
    BOOST_CHECK(late.error().code == ErrorCode::Closed);
}

/**
 * T5：资源 Close 是同步契约——Close 返回即物理释放；资源收束晚于
 * handler 排空。
 */
CO_TEST_CASE(resource_close_after_handler_drain) {
    auto host = std::make_shared<StubNetHost>();
    auto app  = MakeApp(AppOpts(), host);

    auto s = std::make_shared<ResourceShared>();
    std::atomic<bool> drained{false};
    /**
     * 资源 Close 时记录「handler 已排空、网络尚未 Close」这一真实前提。
     */
    s->on_close = [host, &drained]() {
        drained.store(host->Saw("net.wait_handlers") && !host->Saw("net.close"));
    };
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<StubResource>("r",
        StubResource::Options{s}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<CacheSvc>(ServiceOpts()));

    BOOST_REQUIRE(bbt_co_await app->CoStart());
    std::thread req([&app] { app->RequestShutdown(); });
    req.join();
    bbt::coroutine::WaitOptions wait;
    wait.deadline = Clock::now() + std::chrono::seconds(5);
    BOOST_REQUIRE(bbt_co_await app->CoWaitShutdownRequest(wait));
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    /**
     * 资源收束发生在 handler 排空之后
     */
    BOOST_TEST(drained.load());
    BOOST_TEST(s->closed.load());
}

/**
 * T6：资源 Close 同步、幂等——一次关闭序列里每个资源只 Close 一次；
 * 无未完成收口项（收口完整 → App Stopped 而非 StopIncomplete）。
 */
CO_TEST_CASE(resource_close_synchronous_and_idempotent) {
    auto host = std::make_shared<StubNetHost>();
    auto app  = MakeApp(AppOpts(), host);

    auto s = std::make_shared<ResourceShared>();
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<StubResource>("r",
        StubResource::Options{s}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<CacheSvc>(ServiceOpts()));

    BOOST_REQUIRE(bbt_co_await app->CoStart());
    std::thread req([&app] { app->RequestShutdown(); });
    req.join();
    bbt::coroutine::WaitOptions wait;
    wait.deadline = Clock::now() + std::chrono::seconds(5);
    BOOST_REQUIRE(bbt_co_await app->CoWaitShutdownRequest(wait));
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    /**
     * 恰好一次（幂等）
     */
    BOOST_TEST(s->close_calls.load() == 1);
    BOOST_TEST(s->closed.load());
    /**
     * 无未完成收口项
     */
    BOOST_TEST(app->PendingCleanup().empty());
}

/**
 * ── 异常路径：资源创建抛异常 ──
 */
class ThrowingResource final : public fw::CoComponent {
public:
    struct Options {};
    explicit ThrowingResource(fw::detail::ConstructKey k, Options)
        : fw::CoComponent(k) {}
protected:
    ~ThrowingResource() noexcept override = default;
    result<void> OnStart() override {
        throw std::runtime_error("factory boom");
    }
private:
    friend class fw::detail::Registry;
};

/**
 * T7：创建相位抛异常 → 异常被收口为 err，CoStart 返回错误且 App 置
 * Running（而不是让异常逃逸；冻结契约：失败对象记 Failed、App 仍置
 * Running——部分启动诚实反映），已启动资源保留待显式收口，失败记录可观测。
 */
CO_TEST_CASE(factory_throw_rolls_back_via_exception_boundary) {
    auto host = std::make_shared<StubNetHost>();
    auto app  = MakeApp(AppOpts(), host);

    auto ok_s = std::make_shared<ResourceShared>();
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<StubResource>("ok",
        StubResource::Options{ok_s}));
    /**
     * 抛异常的工厂：创建时 throw，不返回 err。
     */
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<ThrowingResource>("bad",
        ThrowingResource::Options{}));

    auto started = bbt_co_await app->CoStart();
    BOOST_CHECK(!started);
    BOOST_CHECK(started.error().code == ErrorCode::InternalError);
    /**
     * 冻结契约：hook 失败对象记 Failed、App 仍置 Running；已启动资源
     * 保留（不被本次失败收口）。
     */
    BOOST_CHECK(app->State() == fw::AppState::Running);
    if (ok_s->create_calls.load() > 0) {
        BOOST_TEST(ok_s->close_calls.load() == 0);
        BOOST_TEST(!ok_s->closed.load());
    }
    CheckNetworkNeverStarted(*host);
    /**
     * 失败清单含异常收口记录。
     */
    const auto failures = app->LifecycleFailures();
    bool saw_threw = false;
    for (const auto& f : failures)
        if (f.code == ErrorCode::InternalError ||
            f.message.find("threw") != std::string::npos)
            saw_threw = true;
    BOOST_TEST(saw_threw);
    /**
     * 已启动资源最终由显式 CoStop 收口（OnStop 同步 Close 到 closed）。
     */
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    if (ok_s->create_calls.load() > 0) {
        BOOST_TEST(ok_s->close_calls.load() >= 1);
        BOOST_TEST(ok_s->closed.load());
    }
}

/**
 * T8：Start 相位抛异常 → 同样收口为 err；失败对象保留 owner，经显式卸载
 * 收口被同步 Close 收束。
 */
class ThrowingStartResource final : public fw::CoComponent {
public:
    struct Options { std::shared_ptr<ResourceShared> s; };
    explicit ThrowingStartResource(fw::detail::ConstructKey k, Options o)
        : fw::CoComponent(k), m_s(std::move(o.s)) {}
protected:
    ~ThrowingStartResource() noexcept override = default;
    result<void> OnStart() override {
        m_s->create_calls.fetch_add(1);
        if (!g_scheduler->IsInitialized())
            return result<void>::err(fw::MakeError(
                ErrorCode::RuntimeUnavailable, "scheduler not started"));
        m_s->created_after_scheduler.store(true);
        m_s->start_calls.fetch_add(1);
        throw std::runtime_error("start threw");
    }
    result<void> OnStop() override {
        m_s->close_calls.fetch_add(1);
        m_s->closed.store(true);
        return result<void>::ok();
    }
private:
    std::shared_ptr<ResourceShared> m_s;
    friend class fw::detail::Registry;
};

CO_TEST_CASE(resource_start_throw_rolls_back_via_exception_boundary) {
    auto host = std::make_shared<StubNetHost>();
    auto app  = MakeApp(AppOpts(), host);

    auto s = std::make_shared<ResourceShared>();
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<ThrowingStartResource>(
        "thrown", ThrowingStartResource::Options{s}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<CacheSvc>(ServiceOpts()));

    auto started = bbt_co_await app->CoStart();
    BOOST_CHECK(!started);
    BOOST_CHECK(started.error().code == ErrorCode::InternalError);
    /**
     * 冻结契约：hook 失败对象记 Failed、App 仍置 Running；已启动对象
     * 保留（不回退）。
     */
    BOOST_CHECK(app->State() == fw::AppState::Running);
    /**
     * Start 相位抛异常的对象保留 owner（Failed）；「半装配资源被同步
     * Close 收束」由显式卸载收口完成（Close 至少一次，closed 终态）。
     */
    BOOST_REQUIRE(bbt_co_await
        app->CoUninstallComponent<ThrowingStartResource>("thrown"));
    BOOST_TEST(s->close_calls.load() >= 1);
    BOOST_TEST(s->closed.load());
    CheckNetworkNeverStarted(*host);
}

BOOST_AUTO_TEST_SUITE_END()
} // namespace
