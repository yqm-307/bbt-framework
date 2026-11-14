// 组件化资源路径验收（framework-resource-lifecycle）：
//  - RedisComponent/MongoDBComponent 经 v2::CoComponent 安装，Engine 由应用根
//    共享借用；组件不 Run/不关闭共享 Engine。
//  - 5 个 Service 按 3+2 绑定两个 RedisComponent：对象身份、地址/池配置隔离、
//    解绑隔离与逆拓扑关闭均走真实 Registry/Handle/Borrow。
//  - CoStop(CloseOptions) 把同一个值转发给组件 OnStop（模式与绝对期限逐项
//    断言）；非法组合在副作用前返回 InvalidArgument（F6）。
//  - OnStop 首次失败保留 owner 与未完成步骤，第二次显式 CoStop 才用新预算推进
//    至 Destroyed（F2）。
// 本文件不连接真实 Redis/MongoDB，也不冒充 live：
//  - 装配用例（resource_lifecycle）只 Install，App 停在 Created——断言的是装配
//    身份/隔离/转发，不宣称组件已启动；
//  - components_co_start_and_co_stop_same_close_options 真实执行 CoStart/CoStop：
//    Engine 由测试线程以等价入口 hook 跑起来（loopback/无监听），Mongo owner 的
//    OnStart 真实成功；Redis 池的 OnStart 真实执行并因无后端如实失败（不伪报
//    connected）；OnStop 收到同一个 CloseOptions 值（mode/deadline/wait_release）。
// 真实 Redis/Mongo 成功连接证据只在 live 目标（需后端授权），本文件不覆盖。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <cstdint>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <thread>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/infra/CloseOptions.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Engine.hpp>

#include <bbt/framework/V2.hpp>
#include <bbt/framework/MongoDBComponent.hpp>
#include <bbt/framework/RedisComponent.hpp>

#include "framework_co_harness.hpp"

namespace fw = bbt::framework;
namespace v2 = bbt::framework::v2;
namespace inf = bbt::infra;
namespace co = bbt::coroutine;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

namespace {

void EnsureRuntime() {
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
        g_scheduler->Start();
    }
}

struct Latch {
    std::mutex mtx;
    std::condition_variable cv;
    bool done{false};
    void Settle() {
        {
            std::lock_guard<std::mutex> lk(mtx);
            done = true;
        }
        cv.notify_all();
    }
    bool Wait(std::chrono::milliseconds ms) {
        std::unique_lock<std::mutex> lk(mtx);
        return cv.wait_for(lk, ms, [this] { return done; });
    }
};

// 在受管协程内执行 fn；native 测试线程有界等待。返回 false 必须当失败。
bool RunInCoTask(std::function<void()> fn,
                 std::chrono::milliseconds timeout = 30s) {
    EnsureRuntime();
    auto box = std::make_shared<Latch>();
    bool registered = false;
    g_scheduler->RegistCoroutineTask(
        [box, fn = std::move(fn)] {
            try {
                fn();
            } catch (...) {}
            box->Settle();
        },
        registered);
    return registered && box->Wait(timeout);
}

// 跨服务/组件的事件与借用观测。
struct Capture {
    std::mutex mtx;
    std::vector<std::string> order;                       // "svc:X"/"probe:stop"
    std::vector<std::pair<std::string, int>> bound_ports; // service → port
    std::vector<std::pair<std::string, std::size_t>> bound_pools;
    std::vector<std::string> unbinds;
    std::vector<fw::ComponentHandle<v2::RedisComponent>> handles;
    std::atomic<int> borrow_ok{0};
    std::atomic<int> borrow_fail{0};

    void Add(std::string event) {
        std::lock_guard<std::mutex> lk(mtx);
        order.push_back(std::move(event));
    }
    void Bound(std::string service, int port, std::size_t pool) {
        std::lock_guard<std::mutex> lk(mtx);
        bound_ports.emplace_back(service, port);
        bound_pools.emplace_back(std::move(service), pool);
    }
    void Unbound(std::string service) {
        std::lock_guard<std::mutex> lk(mtx);
        unbinds.push_back(std::move(service));
    }
    std::size_t Index(const std::string& event) {
        std::lock_guard<std::mutex> lk(mtx);
        for (std::size_t i = 0; i < order.size(); ++i)
            if (order[i] == event)
                return i;
        return order.size();
    }
    bool HasUnbind(const std::string& service) {
        std::lock_guard<std::mutex> lk(mtx);
        for (const auto& s : unbinds)
            if (s == service)
                return true;
        return false;
    }
    int PortOf(const std::string& service) {
        std::lock_guard<std::mutex> lk(mtx);
        for (const auto& [s, p] : bound_ports)
            if (s == service)
                return p;
        return 0;
    }
    std::size_t PoolOf(const std::string& service) {
        std::lock_guard<std::mutex> lk(mtx);
        for (const auto& [s, p] : bound_pools)
            if (s == service)
                return p;
        return 0;
    }
};

std::shared_ptr<Capture> g_cap;

#define DEFINE_REDIS_SERVICE(CLASS, NAME, COMP)                             \
    class CLASS final : public v2::CoService<v2::RedisComponent> {          \
    public:                                                                 \
        static constexpr std::string_view kServiceName = NAME;              \
        explicit CLASS(fw::detail::ConstructKey k)                          \
            : v2::CoService<v2::RedisComponent>(k) {}                       \
                                                                            \
    protected:                                                              \
        ~CLASS() noexcept override = default;                               \
        fw::result<void> OnStop() override {                                \
            if (g_cap)                                                      \
                g_cap->Add("svc:" NAME);                                    \
            return fw::result<void>::ok();                                  \
        }                                                                   \
        void OnBindingChanged(const fw::BindingEvent& ev) override {        \
            if (!g_cap)                                                     \
                return;                                                     \
            if (!ev.bound) {                                                \
                g_cap->Unbound(NAME);                                       \
                return;                                                     \
            }                                                               \
            auto h = this->template GetComponent<v2::RedisComponent>(COMP); \
            if (!h) {                                                       \
                g_cap->borrow_fail.fetch_add(1);                            \
                return;                                                     \
            }                                                               \
            auto b = h.value().Borrow();                                    \
            if (!b) {                                                       \
                g_cap->borrow_fail.fetch_add(1);                            \
                return;                                                     \
            }                                                               \
            g_cap->borrow_ok.fetch_add(1);                                  \
            g_cap->handles.push_back(h.value());                            \
            g_cap->Bound(NAME, b.value().Get().Config().address.Port(),     \
                         b.value().Get().Config().pool.connections);        \
        }                                                                   \
        friend class fw::detail::Registry;                                  \
    }

DEFINE_REDIS_SERVICE(SvcA, "svc-a", "redis-a");
DEFINE_REDIS_SERVICE(SvcB, "svc-b", "redis-a");
DEFINE_REDIS_SERVICE(SvcC, "svc-c", "redis-a");
DEFINE_REDIS_SERVICE(SvcD, "svc-d", "redis-b");
DEFINE_REDIS_SERVICE(SvcE, "svc-e", "redis-b");

#undef DEFINE_REDIS_SERVICE

// App scope 组件：只记录 OnStop 与收到的 CloseOptions，用于逆拓扑次序断言。
class ProbeComponent final : public v2::CoComponent {
public:
    struct Options {};
    explicit ProbeComponent(fw::detail::ConstructKey k, Options)
        : v2::CoComponent(k) {}
    static std::atomic<int> s_last_mode;
    static std::atomic<long long> s_last_deadline_ns;
    static std::atomic<int> s_last_wait_release;

protected:
    ~ProbeComponent() noexcept override = default;
    fw::result<void> OnStop(const inf::CloseOptions& options) override {
        s_last_mode.store(static_cast<int>(options.mode));
        s_last_deadline_ns.store(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                options.deadline.time_since_epoch())
                .count());
        s_last_wait_release.store(options.wait_release ? 1 : 0);
        if (g_cap)
            g_cap->Add("probe:stop");
        return fw::result<void>::ok();
    }
    friend class fw::detail::Registry;
};
std::atomic<int> ProbeComponent::s_last_mode{-1};
std::atomic<long long> ProbeComponent::s_last_deadline_ns{0};
std::atomic<int> ProbeComponent::s_last_wait_release{-1};

// F2：首次 OnStop 失败，owner/步骤保留；第二次显式 CoStop 才完成。
class FlakyComponent final : public v2::CoComponent {
public:
    struct Options {};
    explicit FlakyComponent(fw::detail::ConstructKey k, Options)
        : v2::CoComponent(k) {}
    static std::atomic<int> stop_calls;
    static std::atomic<bool> fail_first;

protected:
    ~FlakyComponent() noexcept override = default;
    fw::result<void> OnStop(const inf::CloseOptions&) override {
        const int n = stop_calls.fetch_add(1) + 1;
        if (n == 1 && fail_first.load()) {
            return fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::RuntimeUnavailable,
                "flaky component: first stop fails"));
        }
        return fw::result<void>::ok();
    }
    friend class fw::detail::Registry;
};
std::atomic<int> FlakyComponent::stop_calls{0};
std::atomic<bool> FlakyComponent::fail_first{true};

fw::ServiceOptions RedisSvcOptions(std::string_view comp) {
    fw::ServiceOptions options;
    options.SetMaxInflight(8);
    if (comp == "redis-a")
        options.BindComponent<v2::RedisComponent>("redis-a");
    else
        options.BindComponent<v2::RedisComponent>("redis-b");
    return options;
}

v2::RedisComponent::Options RedisOpts(
    const std::shared_ptr<inf::net::Engine>& engine, std::uint16_t port,
    std::size_t connections) {
    std::error_code ec;
    auto address = inf::net::Address::Ip("127.0.0.1", port, ec);
    BOOST_REQUIRE_MESSAGE(!ec, "bad test address: " << ec.message());
    v2::RedisComponent::Options options;
    options.engine = engine;
    options.address = address;
    options.pool.connections = connections;
    options.pool.max_waiters = 0;
    return options;
}

v2::MongoDBComponent::Options MongoOpts(std::string db, std::string coll) {
    v2::MongoDBComponent::Options options;
    options.config.uri = "mongodb://127.0.0.1:1/";
    options.config.worker_threads = 1;
    options.config.max_queue = 4;
    options.config.server_selection_timeout = 500ms;
    options.config.connect_timeout = 500ms;
    options.config.socket_timeout = 500ms;
    options.config.wait_queue_timeout = 500ms;
    options.target.database = std::move(db);
    options.target.collection = std::move(coll);
    return options;
}

bool Ok(const fw::result<void>& r) {
    return static_cast<bool>(r);
}

} // namespace

BOOST_AUTO_TEST_CASE(redis_two_components_five_services_identity_and_isolation) {
    EnsureRuntime();
    g_cap = std::make_shared<Capture>();
    FlakyComponent::stop_calls.store(0);
    FlakyComponent::fail_first.store(true);

    std::shared_ptr<v2::CoApp> app;
    std::shared_ptr<inf::net::Engine> engine;
    fw::result<void> install_rc = fw::result<void>::err(
        fw::MakeError(fw::ErrorCode::RuntimeUnavailable, "install not run"));
    bool ran = RunInCoTask([&] {
        fw::AppOptions opts;
        opts.SetLimits(64, 64, 8, 16)
            .SetShutdownBudget(2000ms)
            .SetLifecycleBudget(2000ms);
        app = v2::CoApp::Create(opts, "resource-lifecycle");
        engine = inf::net::Engine::Create(nullptr);
        if (!engine) {
            install_rc = fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::RuntimeUnavailable, "engine create failed"));
            return;
        }
        if (!Ok(bbt_co_await app->CoInstallComponent<v2::RedisComponent>(
                "redis-a", RedisOpts(engine, 16379, 2)))) {
            install_rc = fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::InternalError, "install redis-a failed"));
            return;
        }
        if (!Ok(bbt_co_await app->CoInstallComponent<v2::RedisComponent>(
                "redis-b", RedisOpts(engine, 16380, 3)))) {
            install_rc = fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::InternalError, "install redis-b failed"));
            return;
        }
        if (!Ok(bbt_co_await app->CoInstallComponent<ProbeComponent>(
                "probe", ProbeComponent::Options{}))) {
            install_rc = fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::InternalError, "install probe failed"));
            return;
        }
        if (!Ok(bbt_co_await app->CoInstallComponent<FlakyComponent>(
                "flaky", FlakyComponent::Options{}))) {
            install_rc = fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::InternalError, "install flaky failed"));
            return;
        }
        if (!Ok(bbt_co_await app->CoInstallComponent<v2::MongoDBComponent>(
                "docs", MongoOpts("app", "records")))) {
            install_rc = fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::InternalError, "install docs failed"));
            return;
        }
        if (!Ok(bbt_co_await app->CoInstallService<SvcA>(
                RedisSvcOptions("redis-a"))) ||
            !Ok(bbt_co_await app->CoInstallService<SvcB>(
                RedisSvcOptions("redis-a"))) ||
            !Ok(bbt_co_await app->CoInstallService<SvcC>(
                RedisSvcOptions("redis-a"))) ||
            !Ok(bbt_co_await app->CoInstallService<SvcD>(
                RedisSvcOptions("redis-b"))) ||
            !Ok(bbt_co_await app->CoInstallService<SvcE>(
                RedisSvcOptions("redis-b")))) {
            install_rc = fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::InternalError, "install service failed"));
            return;
        }
        install_rc = fw::result<void>::ok();
    });
    BOOST_REQUIRE(ran);
    if (!Ok(install_rc))
        BOOST_FAIL("install failed: " + install_rc.error().message);
    BOOST_REQUIRE(Ok(install_rc));
    BOOST_REQUIRE(app);

    // 对象身份：两个 Redis 组件是不同的 Registry 对象；Mongo 组件也在场。
    auto a = app->FindComponent<v2::RedisComponent>("redis-a");
    auto b = app->FindComponent<v2::RedisComponent>("redis-b");
    auto docs = app->FindComponent<v2::MongoDBComponent>("docs");
    BOOST_REQUIRE(a);
    BOOST_REQUIRE(b);
    BOOST_REQUIRE(docs);
    BOOST_CHECK_NE(a.value().Id(), b.value().Id());
    BOOST_CHECK(a.value().IsValid());
    BOOST_CHECK(b.value().IsValid());
    BOOST_CHECK(docs.value().IsValid());

    // 3+2 绑定经真实 Borrow；地址配置互不串用。
    BOOST_CHECK_EQUAL(g_cap->borrow_ok.load(), 5);
    BOOST_CHECK_EQUAL(g_cap->borrow_fail.load(), 0);
    BOOST_CHECK_EQUAL(g_cap->PortOf("svc-a"), 16379);
    BOOST_CHECK_EQUAL(g_cap->PortOf("svc-b"), 16379);
    BOOST_CHECK_EQUAL(g_cap->PortOf("svc-c"), 16379);
    BOOST_CHECK_EQUAL(g_cap->PortOf("svc-d"), 16380);
    BOOST_CHECK_EQUAL(g_cap->PortOf("svc-e"), 16380);
    BOOST_CHECK_EQUAL(g_cap->PoolOf("svc-a"), 2);
    BOOST_CHECK_EQUAL(g_cap->PoolOf("svc-b"), 2);
    BOOST_CHECK_EQUAL(g_cap->PoolOf("svc-c"), 2);
    BOOST_CHECK_EQUAL(g_cap->PoolOf("svc-d"), 3);
    BOOST_CHECK_EQUAL(g_cap->PoolOf("svc-e"), 3);

    // 非受管协程直接 Borrow：在副作用前以 InvalidContext 拒绝。
    {
        BOOST_REQUIRE(!g_cap->handles.empty());
        auto b = g_cap->handles.front().Borrow();
        BOOST_CHECK(!b);
        if (!b)
            BOOST_CHECK(b.error().code == fw::ErrorCode::InvalidContext);
    }

    // 解绑一个 Service 不影响另一个 Service 的绑定或组件本身。
    auto d = app->FindService("svc-d");
    BOOST_REQUIRE(d);
    fw::result<void> unbind_rc = fw::result<void>::err(
        fw::MakeError(fw::ErrorCode::RuntimeUnavailable, "unbind not run"));
    ran = RunInCoTask([&] {
        unbind_rc = bbt_co_await app->CoUnbindComponent<v2::RedisComponent>(
            d.value(), "redis-b");
    });
    BOOST_REQUIRE(ran);
    if (!Ok(unbind_rc))
        BOOST_FAIL("unbind failed: " + unbind_rc.error().message);
    BOOST_REQUIRE(Ok(unbind_rc));
    BOOST_CHECK(g_cap->HasUnbind("svc-d"));
    BOOST_CHECK(!g_cap->HasUnbind("svc-e"));
    auto b_after = app->FindComponent<v2::RedisComponent>("redis-b");
    BOOST_REQUIRE(b_after);
    BOOST_CHECK(b_after.value().IsValid());

    // F6：非法关闭选项在副作用前拒绝；App 状态与组件停止计数不变。
    {
        fw::result<void> bad_rc = fw::result<void>::ok();
        ran = RunInCoTask([&] {
            inf::CloseOptions bad =
                inf::CloseOptions::Graceful(co::Deadline::max());
            bad_rc = bbt_co_await app->CoStop(bad);
        });
        BOOST_REQUIRE(ran);
        BOOST_CHECK(!Ok(bad_rc));
        BOOST_CHECK_EQUAL(static_cast<int>(bad_rc.error().code),
                          static_cast<int>(fw::ErrorCode::InvalidArgument));
        BOOST_CHECK(app->State() == fw::AppState::Created);
        BOOST_CHECK_EQUAL(FlakyComponent::stop_calls.load(), 0);
    }

    // F2：首次停止失败保留 owner/未完成步骤；第二次显式 CoStop 才销毁。
    fw::result<void> first_stop = fw::result<void>::ok();
    ran = RunInCoTask([&] {
        inf::CloseOptions options =
            inf::CloseOptions::Graceful(Clock::now() + 2s);
        first_stop = bbt_co_await app->CoStop(options);
    });
    BOOST_REQUIRE(ran);
    BOOST_CHECK(Ok(first_stop));
    BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);
    BOOST_CHECK(!app->PendingCleanup().empty());
    BOOST_CHECK_EQUAL(FlakyComponent::stop_calls.load(), 1);
    BOOST_CHECK(app->FindComponent<FlakyComponent>("flaky").value().IsValid());

    FlakyComponent::fail_first.store(false);
    fw::result<void> second_stop = fw::result<void>::ok();
    ran = RunInCoTask([&] {
        inf::CloseOptions options =
            inf::CloseOptions::Graceful(Clock::now() + 2s);
        second_stop = bbt_co_await app->CoStop(options);
    });
    BOOST_REQUIRE(ran);
    BOOST_CHECK(Ok(second_stop));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_CHECK(app->PendingCleanup().empty());
    BOOST_CHECK_EQUAL(FlakyComponent::stop_calls.load(), 2);
    BOOST_CHECK(!app->FindComponent<FlakyComponent>("flaky"));

    // 逆拓扑：Service OnStop 全部先于组件 OnStop。
    BOOST_CHECK_LT(g_cap->Index("svc:svc-a"), g_cap->Index("probe:stop"));
    BOOST_CHECK_LT(g_cap->Index("svc:svc-e"), g_cap->Index("probe:stop"));
    // 组件 OnStop 收到当次同一个 CloseOptions（模式 + 绝对期限）。
    BOOST_CHECK_EQUAL(ProbeComponent::s_last_mode.load(),
                      static_cast<int>(inf::CloseMode::Graceful));
    // 组件随停止销毁；Mongo 组件同样收口（未连接真实后端）。
    BOOST_CHECK(!app->FindComponent<v2::RedisComponent>("redis-a"));
    BOOST_CHECK(!app->FindComponent<v2::MongoDBComponent>("docs"));

    // 清理：释放 app/engine（测试线程不持有在途协程）。
    app.reset();
    engine.reset();
}

namespace {

// 组件 OnStart/OnStop 的可执行观察面：Service 在自己的 hook 内经
// ComponentHandle::Borrow 读组件状态（不持裸指针越过 borrow）。
struct StatedProbe {
    std::atomic<int> borrow_ok{0};
    std::atomic<int> borrow_fail{0};
    std::atomic<int> mongo_started{0};
    std::atomic<int> redis_connected{-1};
    const void* engine_a{nullptr};
    const void* engine_b{nullptr};
};
StatedProbe kStated;

// Mongo 服务：OnStart 在组件 OnStart 之后执行，读 owner 是否真的 Running。
class MongoStatedSvc final : public v2::CoService<v2::MongoDBComponent> {
public:
    static constexpr std::string_view kServiceName = "svc-mongo-stated";
    explicit MongoStatedSvc(fw::detail::ConstructKey k)
        : v2::CoService<v2::MongoDBComponent>(k) {}

protected:
    ~MongoStatedSvc() noexcept override = default;
    fw::result<void> OnStart() override {
        auto handle = this->template GetComponent<v2::MongoDBComponent>("docs");
        if (!handle) {
            kStated.borrow_fail.fetch_add(1);
            return fw::result<void>::ok();
        }
        auto borrow = handle.value().Borrow();
        if (!borrow) {
            kStated.borrow_fail.fetch_add(1);
            return fw::result<void>::ok();
        }
        kStated.mongo_started.store(
            borrow.value().Get().started() ? 1 : 0);
        kStated.borrow_ok.fetch_add(1);
        return fw::result<void>::ok();
    }
    friend class fw::detail::Registry;
};

// Redis 观察服务：声明绑定两个 Redis 组件；绑定在安装窗口（subject 处于
// Installing）落地，v2 只在该窗口允许借 Installed 依赖做纯内存检查。两条绑定
// 都落地后读取各自 Config 的共享 Engine 身份与 connected()。
class RedisStatedSvc final : public v2::CoService<v2::RedisComponent> {
public:
    static constexpr std::string_view kServiceName = "observer";
    explicit RedisStatedSvc(fw::detail::ConstructKey k)
        : v2::CoService<v2::RedisComponent>(k) {}

protected:
    ~RedisStatedSvc() noexcept override = default;
    void OnBindingChanged(const fw::BindingEvent& ev) override {
        if (!ev.bound || m_observed)
            return;
        auto cache = this->template GetComponent<v2::RedisComponent>("cache");
        auto cache2 =
            this->template GetComponent<v2::RedisComponent>("cache2");
        if (!cache || !cache2)
            return; // 另一条绑定尚未落地
        m_observed = true;
        for (const auto& handle : {cache.value(), cache2.value()}) {
            auto borrow = handle.Borrow();
            if (!borrow) {
                kStated.borrow_fail.fetch_add(1);
                continue;
            }
            auto& component = borrow.value().Get();
            if (kStated.engine_a == nullptr)
                kStated.engine_a = component.Config().engine.get();
            else
                kStated.engine_b = component.Config().engine.get();
            kStated.redis_connected.store(component.connected() ? 1 : 0);
            kStated.borrow_ok.fetch_add(1);
        }
    }
    friend class fw::detail::Registry;

private:
    bool m_observed = false;
};

} // namespace

// 已启动生命周期（可成功路径）：Engine 由测试线程以等价入口 host hook 跑起来
// （客户端布局，无监听、无后端），组件真实执行 OnStart/OnStop。
//
// 断言（全部来自真实执行，不冒充 live）：
//  - MongoDBComponent::OnStart 真实成功（owner Start + 绑定集合，不连后端），
//    Service OnStart 经 Borrow 读到 started()==true；
//  - CoStop(CloseOptions{wait_release=true}) 把同一个值传给组件 OnStop：
//    ProbeComponent 记到同一 mode/绝对期限/wait_release；
//  - 组件与 App 都不关闭共享 Engine：CoStop 后 Engine 仍 Running，由入口
//    hook 的 RequestClose 收口到 Stopped。
BOOST_AUTO_TEST_CASE(components_co_start_and_co_stop_same_close_options) {
    EnsureRuntime();
    g_cap = std::make_shared<Capture>();
    ProbeComponent::s_last_mode.store(-1);
    ProbeComponent::s_last_wait_release.store(-1);

    std::shared_ptr<v2::CoApp> app;
    std::shared_ptr<inf::net::Engine> engine;
    std::thread run_thread;
    fw::result<void> install_rc = fw::result<void>::err(
        fw::MakeError(fw::ErrorCode::RuntimeUnavailable, "install not run"));

    BOOST_REQUIRE(RunInCoTask([&] {
        fw::AppOptions opts;
        opts.SetLimits(64, 64, 8, 16)
            .SetShutdownBudget(2000ms)
            .SetLifecycleBudget(3000ms);
        app = v2::CoApp::Create(opts, "component-started");
        // Idle 装配：Engine 此时必须仍 Idle（Redis 池的 Create 要求）。
        engine = inf::net::Engine::Create(nullptr);
        if (!engine) {
            install_rc = fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::RuntimeUnavailable, "engine create failed"));
            return;
        }
        if (!Ok(bbt_co_await app->CoInstallComponent<v2::MongoDBComponent>(
                "docs", MongoOpts("app", "records")))) {
            install_rc = fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::InternalError, "install docs failed"));
            return;
        }
        if (!Ok(bbt_co_await app->CoInstallComponent<ProbeComponent>(
                "probe", ProbeComponent::Options{}))) {
            install_rc = fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::InternalError, "install probe failed"));
            return;
        }
        fw::ServiceOptions svc;
        svc.SetMaxInflight(8);
        svc.BindComponent<v2::MongoDBComponent>("docs");
        if (!Ok(bbt_co_await app->CoInstallService<MongoStatedSvc>(svc))) {
            install_rc = fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::InternalError, "install service failed"));
            return;
        }
        install_rc = fw::result<void>::ok();
    }));
    if (!Ok(install_rc))
        BOOST_FAIL("install failed: " + install_rc.error().message);
    BOOST_REQUIRE(app);
    BOOST_CHECK(engine->Phase() == inf::net::EnginePhase::Idle);

    // 等价入口 hook：Engine 的 native 执行只由该线程调用一次 Run。
    inf::net::EngineConfig config;
    config.runtime_progress = inf::net::RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    run_thread = std::thread([engine, config] { (void)engine->Run(config); });

    bool running = false;
    BOOST_REQUIRE(RunInCoTask([&] {
        co::WaitOptions wait;
        wait.deadline = Clock::now() + 10s;
        running = Ok(engine->CoWaitRunning(wait));
    }));
    BOOST_REQUIRE_MESSAGE(running, "engine did not reach Running");

    bool started = false;
    BOOST_REQUIRE(RunInCoTask([&] {
        started = Ok(bbt_co_await app->CoStart());
    }));
    BOOST_REQUIRE(started);
    BOOST_CHECK(app->State() == fw::AppState::Running);
    BOOST_CHECK_EQUAL(kStated.borrow_fail.load(), 0);
    BOOST_CHECK_EQUAL(kStated.mongo_started.load(), 1);

    fw::result<void> stop_rc = fw::result<void>::err(
        fw::MakeError(fw::ErrorCode::RuntimeUnavailable, "stop not run"));
    const auto deadline = Clock::now() + 2s;
    BOOST_REQUIRE(RunInCoTask([&] {
        inf::CloseOptions options = inf::CloseOptions::Graceful(deadline);
        options.wait_release = true;
        stop_rc = bbt_co_await app->CoStop(options);
    }));
    BOOST_REQUIRE(Ok(stop_rc));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    // 同一个值到达组件 OnStop（mode/绝对期限/wait_release）。
    BOOST_CHECK_EQUAL(ProbeComponent::s_last_mode.load(),
                      static_cast<int>(inf::CloseMode::Graceful));
    BOOST_CHECK_EQUAL(ProbeComponent::s_last_wait_release.load(), 1);
    BOOST_CHECK_EQUAL(ProbeComponent::s_last_deadline_ns.load(),
                      std::chrono::duration_cast<std::chrono::nanoseconds>(
                          deadline.time_since_epoch())
                          .count());
    // 共享 Engine 不被组件或 App 关闭：仍在 Running，由入口 hook 收口。
    BOOST_CHECK(engine->Phase() == inf::net::EnginePhase::Running);
    engine->RequestClose();
    BOOST_REQUIRE(run_thread.joinable());
    run_thread.join();
    BOOST_CHECK(engine->Phase() == inf::net::EnginePhase::Stopped);
    app.reset();
    engine.reset();
}

// 已启动生命周期（诚实失败路径）：Redis 池 OnStart 真实执行并因无后端失败。
// 断言 App 仍 Running（部分启动诚实反映）、失败项可观察、组件不伪报 connected、
// 且两个 RedisComponent 借用同一个 Engine 实例（共享 Engine 的可执行证据）。
BOOST_AUTO_TEST_CASE(redis_on_start_fails_honestly_without_backend) {
    EnsureRuntime();
    g_cap = std::make_shared<Capture>();
    kStated.borrow_ok.store(0);
    kStated.borrow_fail.store(0);
    kStated.redis_connected.store(-1);
    kStated.engine_a = nullptr;
    kStated.engine_b = nullptr;

    std::shared_ptr<v2::CoApp> app;
    std::shared_ptr<inf::net::Engine> engine;
    std::thread run_thread;
    fw::result<void> install_rc = fw::result<void>::err(
        fw::MakeError(fw::ErrorCode::RuntimeUnavailable, "install not run"));

    BOOST_REQUIRE(RunInCoTask([&] {
        fw::AppOptions opts;
        opts.SetLimits(64, 64, 8, 16)
            .SetShutdownBudget(2000ms)
            .SetLifecycleBudget(3000ms);
        app = v2::CoApp::Create(opts, "component-redis-fail");
        engine = inf::net::Engine::Create(nullptr);
        if (!engine) {
            install_rc = fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::RuntimeUnavailable, "engine create failed"));
            return;
        }
        if (!Ok(bbt_co_await app->CoInstallComponent<v2::RedisComponent>(
                "cache", RedisOpts(engine, 16379, 1))) ||
            !Ok(bbt_co_await app->CoInstallComponent<v2::RedisComponent>(
                "cache2", RedisOpts(engine, 16380, 2)))) {
            install_rc = fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::InternalError, "install redis failed"));
            return;
        }
        // 观察服务在安装窗口绑定两个 Redis 组件；绑定落地时依赖仍 Installed，
        // 由装配窗口借用读取 Engine 身份（不依赖启动/停止顺序或组件拓扑）。
        fw::ServiceOptions observer_options;
        observer_options.SetMaxInflight(4)
            .BindComponent<v2::RedisComponent>("cache")
            .BindComponent<v2::RedisComponent>("cache2");
        if (!Ok(bbt_co_await
                    app->CoInstallService<RedisStatedSvc>(observer_options))) {
            install_rc = fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::InternalError, "install observer failed"));
            return;
        }
        install_rc = fw::result<void>::ok();
    }));
    if (!Ok(install_rc))
        BOOST_FAIL("install failed: " + install_rc.error().message);
    BOOST_REQUIRE(app);

    inf::net::EngineConfig config;
    config.runtime_progress = inf::net::RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    run_thread = std::thread([engine, config] { (void)engine->Run(config); });

    bool running = false;
    BOOST_REQUIRE(RunInCoTask([&] {
        co::WaitOptions wait;
        wait.deadline = Clock::now() + 10s;
        running = Ok(engine->CoWaitRunning(wait));
    }));
    BOOST_REQUIRE_MESSAGE(running, "engine did not reach Running");

    // Redis OnStart 真实执行：无后端 → Connect 失败 → 首错透出，App 仍 Running。
    fw::result<void> start_rc = fw::result<void>::ok();
    BOOST_REQUIRE(RunInCoTask([&] { start_rc = bbt_co_await app->CoStart(); }));
    BOOST_CHECK(!Ok(start_rc));
    BOOST_CHECK(app->State() == fw::AppState::Running);
    BOOST_CHECK(!app->LifecycleFailures().empty());

    fw::result<void> stop_rc = fw::result<void>::err(
        fw::MakeError(fw::ErrorCode::RuntimeUnavailable, "stop not run"));
    BOOST_REQUIRE(RunInCoTask([&] {
        stop_rc = bbt_co_await app->CoStop(
            inf::CloseOptions::Graceful(Clock::now() + 2s));
    }));
    BOOST_REQUIRE(Ok(stop_rc));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    // 两个组件借用同一个 Engine 实例，且都没有伪报已连接。
    BOOST_CHECK_EQUAL(kStated.borrow_fail.load(), 0);
    BOOST_CHECK_EQUAL(kStated.borrow_ok.load(), 2);
    BOOST_CHECK(kStated.engine_a != nullptr);
    BOOST_CHECK_EQUAL(kStated.engine_a, kStated.engine_b);
    BOOST_CHECK_EQUAL(kStated.redis_connected.load(), 0);
    BOOST_CHECK(engine->Phase() == inf::net::EnginePhase::Running);
    engine->RequestClose();
    BOOST_REQUIRE(run_thread.joinable());
    run_thread.join();
    app.reset();
    engine.reset();
}

namespace {

// T（挂起借用边界）：handler 先取真实 ComponentHandle 再停在放行门上；
// 挂起期间对同一组件执行 CoUnbindComponent（关闭绑定边）+ CoUninstallComponent
// （移除登记）；放行后旧 binding 代句柄的 Borrow 必返 Closed，下一次实时
// GetComponent 必返 NotFound——只验证借用边界，不引入 Service 执行租约或
// 自动组件依赖。
class CountComponent final : public v2::CoComponent {
public:
    struct Options {};
    static std::atomic<int> borrows;

    explicit CountComponent(fw::detail::ConstructKey k, Options)
        : v2::CoComponent(k) {}

    std::int32_t Plus(std::int32_t v) const { return v + 1; }

protected:
    ~CountComponent() noexcept override = default;
    friend class fw::detail::Registry;
};
std::atomic<int> CountComponent::borrows{0};

class GatedSvc final : public v2::CoService<CountComponent> {
public:
    static constexpr std::string_view kServiceName = "gated-svc";
    static std::shared_ptr<co::sync::CoWaiter> waiter;
    static std::atomic<int> entered;
    static std::atomic<int> closed_seen;
    static std::atomic<int> notfound_seen;

    explicit GatedSvc(fw::detail::ConstructKey k)
        : v2::CoService<CountComponent>(k) {}

    // 先取真实绑定句柄（带 binding 代）→ 报进入 → 挂起在 waiter 上；
    // 测试侧在解绑/卸载后 Notify → 恢复后借同一旧句柄（必 Closed），
    // 再实时 GetComponent（必 NotFound）。
    fw::result<std::int32_t> Probe() {
        auto h = GetComponent<CountComponent>("count");
        if (!h)
            return fw::result<std::int32_t>::err(h.error());
        entered.fetch_add(1);
        if (waiter) {
            co::WaitOptions wo;
            wo.deadline = Clock::now() + 10s;
            const auto st = waiter->Wait(wo);
            if (st != co::WaitStatus::Completed)
                return fw::result<std::int32_t>::err(fw::MakeError(
                    fw::ErrorCode::TimedOut, "gated borrow wait"));
        }
        auto b = h.value().Borrow();
        if (!b && b.error().code == fw::ErrorCode::Closed)
            closed_seen.fetch_add(1);
        // 同一恢复点再做下一次实时查询：组件已卸载 → NotFound。
        auto again = GetComponent<CountComponent>("count");
        if (!again) {
            if (again.error().code == fw::ErrorCode::NotFound)
                notfound_seen.fetch_add(1);
            return fw::result<std::int32_t>::err(again.error());
        }
        if (!b)
            return fw::result<std::int32_t>::err(b.error());
        return fw::result<std::int32_t>::ok(
            b.value().Get().Plus(0));
    }
protected:
    ~GatedSvc() noexcept override = default;
    friend class fw::detail::Registry;
};
std::shared_ptr<co::sync::CoWaiter> GatedSvc::waiter;
std::atomic<int> GatedSvc::entered{0};
std::atomic<int> GatedSvc::closed_seen{0};
std::atomic<int> GatedSvc::notfound_seen{0};

} // namespace

BOOST_AUTO_TEST_CASE(suspended_component_close_then_borrow_fails) {
    EnsureRuntime();
    CountComponent::borrows.store(0);
    GatedSvc::entered.store(0);
    GatedSvc::closed_seen.store(0);
    GatedSvc::notfound_seen.store(0);
    auto waiter = co::sync::CoWaiter::Create();
    GatedSvc::waiter = waiter;

    std::shared_ptr<v2::CoApp> app;
    fw::result<void> rc = fw::result<void>::ok();
    bool ran = RunInCoTask([&] {
        fw::AppOptions opts;
        opts.SetLimits(64, 64, 8, 16)
            .SetShutdownBudget(2000ms)
            .SetLifecycleBudget(3000ms);
        app = v2::CoApp::Create(opts, "suspend-borrow");
        auto a = bbt_co_await app->CoInstallComponent<CountComponent>(
            "count", CountComponent::Options{});
        auto b = bbt_co_await app->CoInstallService<GatedSvc>(
            fw::ServiceOptions{}.SetMaxInflight(8)
                                 .BindComponent<CountComponent>("count"));
        auto c = bbt_co_await app->CoStart();
        if (!a) rc = std::move(a);
        else if (!b) rc = std::move(b);
        else rc = std::move(c);
    });
    BOOST_REQUIRE(ran);
    BOOST_REQUIRE(Ok(rc));

    auto svc_h = app->FindService("gated-svc");
    BOOST_REQUIRE(svc_h);

    // 先入 handler：取真实绑定句柄并停在 waiter 上（挂起期间组件被解绑/卸载）。
    // Probe 不能经 RunInCoTask 的「有界等任务返回」驱动——等待本身就是要挂起
    // 的事件，否则先死锁。这里经独立协程任务跑，native 侧只等 entered。
    auto svc = framework_test::ServiceInstance(*app, "gated-svc");
    BOOST_REQUIRE(svc);
    auto probe_done = std::make_shared<Latch>();
    std::shared_ptr<fw::result<std::int32_t>> probe_out =
        std::make_shared<fw::result<std::int32_t>>(
            fw::result<std::int32_t>::err(fw::MakeError(
                fw::ErrorCode::RuntimeUnavailable, "not run")));
    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [svc, probe_done, probe_out] {
            auto* s = static_cast<GatedSvc*>(svc.get());
            *probe_out = s->Probe();
            probe_done->Settle();
        },
        submitted);
    BOOST_REQUIRE(submitted);
    const auto until = Clock::now() + 30s;
    while (GatedSvc::entered.load() == 0) {
        BOOST_REQUIRE(Clock::now() < until);
        std::this_thread::yield();
    }

    // 挂起期间解绑（关闭该绑定边的新 borrow 并等旧 borrow 排空）。
    fw::result<void> unbind = fw::result<void>::err(
        fw::MakeError(fw::ErrorCode::RuntimeUnavailable, "not run"));
    ran = RunInCoTask([&] {
        unbind = bbt_co_await app->CoUnbindComponent<CountComponent>(
            svc_h.value(), "count");
    });
    BOOST_REQUIRE(ran);
    BOOST_REQUIRE(Ok(unbind));

    // 解绑后卸载组件（移除登记）。
    fw::result<void> uninstall = fw::result<void>::err(
        fw::MakeError(fw::ErrorCode::RuntimeUnavailable, "not run"));
    ran = RunInCoTask([&] {
        uninstall = bbt_co_await app->CoUninstallComponent<CountComponent>(
            "count");
    });
    BOOST_REQUIRE(ran);
    BOOST_REQUIRE(Ok(uninstall));

    // 放行 handler：旧 binding 代句柄的 Borrow 必返 Closed；下一次实时
    // GetComponent 必返 NotFound。
    GatedSvc::waiter->Notify();
    BOOST_REQUIRE(probe_done->Wait(10s));
    BOOST_CHECK(GatedSvc::closed_seen.load() == 1);
    BOOST_CHECK(GatedSvc::notfound_seen.load() == 1);
    BOOST_CHECK(!app->FindComponent<CountComponent>("count"));

    auto stopped = framework_test::CoStop(*app);
    BOOST_CHECK(framework_test::CoOk(stopped));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
}
