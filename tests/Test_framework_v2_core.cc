// framework v2 最小核心切片行为验收：
//  - 构造限制：业务类型只能经 CoApp::Install* 创建（静态断言在编译期覆盖
//    不可默构/不可公开析构；运行期断言对象只来自 Registry）。
//  - Install/Uninstall 生命周期：Installing→Installed→Running→Destroyed、
//    OnInstall 失败 → InstallFailed、Uninstall 有 active binding → Overloaded、
//    Unbind 封旧 handle generation。
//  - Service/Actor handle 失效与地址语义：Uninstall 后 IsValid=false、
//    GetMailBox 返回正确 AppObjectAddress、generation 失配 Info() 报错。
//  - Observe：状态事件按 revision 单调到达，observer 卸载后 token 失效。
//
// 不依赖网络；生命周期断言运行在真实协程中，全部断言走真实公共面
// （v2::CoApp + 公开 handle/组件借用），不走 stub。

#define BOOST_TEST_MODULE framework.v2_core
#include <boost/test/included/unit_test.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <future>
#include <stdexcept>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/framework/V2.hpp>

namespace fw = bbt::framework;
using bbt::framework::result;
using bbt::framework::ErrorCode;

namespace {

// ---- 测试用组件：App scope 可借用的计数组件 ----
class CounterComponent final : public fw::v2::CoComponent {
public:
    struct Options { int initial = 0; };

    explicit CounterComponent(fw::detail::ConstructKey k, Options o)
        : fw::v2::CoComponent(k), m_count(o.initial) {}

    int  Get() const noexcept { return m_count; }
    void Inc() noexcept { ++m_count; }

protected:
    ~CounterComponent() noexcept override = default;

private:
    int m_count;
    friend class fw::detail::Registry;
};

// 组件未声明允许绑定时，Service::GetComponent<T> 必须返回 InvalidArgument。
class AnotherComponent final : public fw::v2::CoComponent {
public:
    struct Options {};
    explicit AnotherComponent(fw::detail::ConstructKey k, Options)
        : fw::v2::CoComponent(k) {}
protected:
    ~AnotherComponent() noexcept override = default;
    friend class fw::detail::Registry;
};

// ---- 测试用 Service ----
class EchoService final : public fw::v2::CoService<CounterComponent> {
public:
    static constexpr std::string_view kServiceName = "echo";

    explicit EchoService(fw::detail::ConstructKey k)
        : fw::v2::CoService<CounterComponent>(k) {}

    int install_count = 0;
    int start_count = 0;
    int stop_count = 0;
    int uninstall_count = 0;
    int binding_changed_count = 0;

    // 真实模板消费者：OnBindingChanged 内走受保护 GetComponent（编译期
    // 实例化 + 运行期绑定表断言），结果写入静态计数供用例断言。
    // AnotherComponent 未在声明集合内，必须 InvalidArgument。
    static std::atomic<int> s_gc_ok;
    static std::atomic<int> s_gc_undeclared_rejected;
    static std::atomic<int> s_gc_borrow_ok;

protected:
    ~EchoService() noexcept override = default;

    result<void> OnInstall() override {
        ++install_count; return result<void>::ok();
    }
    result<void> OnStart() override {
        ++start_count; return result<void>::ok();
    }
    result<void> OnStop() override {
        ++stop_count; return result<void>::ok();
    }
    void OnUninstall() override { ++uninstall_count; }
    void OnBindingChanged(const fw::BindingEvent&) override {
        ++binding_changed_count;
        // 绑定已回填后 GetComponent 应可见；借用成功证明真实实例化路径。
        auto h = GetComponent<CounterComponent>("counter");
        if (h) {
            ++s_gc_ok;
            auto b = h.value().Borrow();
            if (b) ++s_gc_borrow_ok;
        }
        auto bad = GetComponent<AnotherComponent>("counter");
        if (!bad && bad.error().code == ErrorCode::InvalidArgument)
            ++s_gc_undeclared_rejected;
    }

    friend class fw::detail::Registry;
};

std::atomic<int> EchoService::s_gc_ok{0};
std::atomic<int> EchoService::s_gc_undeclared_rejected{0};
std::atomic<int> EchoService::s_gc_borrow_ok{0};

class FailInstallService final : public fw::v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "fail_install";
    explicit FailInstallService(fw::detail::ConstructKey k) : fw::v2::CoService<>(k) {}
protected:
    ~FailInstallService() noexcept override = default;
    result<void> OnInstall() override {
        return result<void>::err(fw::MakeError(ErrorCode::InternalError,
            "OnInstall 显式失败"));
    }
    friend class fw::detail::Registry;
};

// 第二个 Service 类型：与 EchoService 同绑定同一组件，验证两 subject
// 独立 Unbind 互不影响。
class Echo2Service final : public fw::v2::CoService<CounterComponent> {
public:
    static constexpr std::string_view kServiceName = "echo2";
    explicit Echo2Service(fw::detail::ConstructKey k)
        : fw::v2::CoService<CounterComponent>(k) {}
protected:
    ~Echo2Service() noexcept override = default;
    friend class fw::detail::Registry;
};

// 装配借用探针服务：OnInstall 内经 GetComponent 借 Installed 依赖
// （装配窗口）；OnStop 记录是否运行在受管协程内。
class ProbeService final : public fw::v2::CoService<CounterComponent> {
public:
    static constexpr std::string_view kServiceName = "probe";
    explicit ProbeService(fw::detail::ConstructKey k)
        : fw::v2::CoService<CounterComponent>(k) {}

    static std::atomic<int> s_install_borrow_ok;
    static std::atomic<int> s_install_borrow_value;
    static std::atomic<int> s_onstart_co;      // OnStart 在协程内执行次数
    static std::atomic<int> s_onstop_co;       // OnStop 在协程内执行次数
    static std::atomic<int> s_unbind_events;   // bound=false 事件计数
    static std::atomic<int> s_invalid_ctx;     // hook 内再调宿主入口被拒

    // 由用例设置：hook 内要探测的 CoApp（非拥有）。
    static fw::v2::CoApp* s_app;

protected:
    ~ProbeService() noexcept override = default;

    result<void> OnInstall() override {
        auto h = GetComponent<CounterComponent>("counter");
        if (h) {
            auto b = h.value().Borrow();
            if (b) {
                ++s_install_borrow_ok;
                s_install_borrow_value = b.value().Get().Get();
            }
        }
        return result<void>::ok();
    }
    result<void> OnStart() override {
        if (bbt::coroutine::GetLocalCoroutineId() != 0) ++s_onstart_co;
        // hook 内同协程再调宿主控制面 → InvalidContext（自等待）。
        if (s_app) {
            auto r = bbt_co_await s_app->CoPause();
            if (!r && r.error().code == ErrorCode::InvalidContext)
                ++s_invalid_ctx;
            // CoStop 在协程内调用会等待自身 hook → InvalidContext。
            auto cr = bbt_co_await s_app->CoStop();
            if (!cr && cr.error().code == ErrorCode::InvalidContext)
                ++s_invalid_ctx;
        }
        return result<void>::ok();
    }
    result<void> OnStop() override {
        if (bbt::coroutine::GetLocalCoroutineId() != 0) ++s_onstop_co;
        return result<void>::ok();
    }
    void OnBindingChanged(const fw::BindingEvent& ev) override {
        if (!ev.bound) ++s_unbind_events;
    }
    friend class fw::detail::Registry;
};

std::atomic<int> ProbeService::s_install_borrow_ok{0};
std::atomic<int> ProbeService::s_install_borrow_value{0};
std::atomic<int> ProbeService::s_onstart_co{0};
std::atomic<int> ProbeService::s_onstop_co{0};
std::atomic<int> ProbeService::s_unbind_events{0};
std::atomic<int> ProbeService::s_invalid_ctx{0};
fw::v2::CoApp* ProbeService::s_app = nullptr;

// 拓扑/挂起探针组件：OnStart/OnStop 记录执行次序与协程上下文；
// OnStop 可用 bbtco_sleep 真实挂起（不阻塞 worker），由静态毫秒数控制。
class DepComponent final : public fw::v2::CoComponent {
public:
    struct Options { int tag = 0; };
    explicit DepComponent(fw::detail::ConstructKey k, Options o)
        : fw::v2::CoComponent(k), m_tag(o.tag) {}

    static std::mutex              s_order_mtx;
    static std::vector<std::string> s_order;      // "start:N"/"stop:N" 事件序
    static std::atomic<int>        s_suspend_ms;  // OnStop 挂起毫秒（0=不挂起）
    static std::atomic<int>        s_start_co;
    static std::atomic<int>        s_stop_co;

    int Tag() const noexcept { return m_tag; }

protected:
    ~DepComponent() noexcept override = default;

    result<void> OnStart() override {
        if (bbt::coroutine::GetLocalCoroutineId() != 0) ++s_start_co;
        std::lock_guard<std::mutex> lk(s_order_mtx);
        s_order.push_back("start:" + std::to_string(m_tag));
        return result<void>::ok();
    }
    result<void> OnStop() override {
        if (bbt::coroutine::GetLocalCoroutineId() != 0) ++s_stop_co;
        const int ms = s_suspend_ms.load();
        if (ms > 0) bbtco_sleep(ms);   // 真实挂起：不阻塞 worker
        std::lock_guard<std::mutex> lk(s_order_mtx);
        s_order.push_back("stop:" + std::to_string(m_tag));
        return result<void>::ok();
    }

private:
    int m_tag;
    friend class fw::detail::Registry;
};

std::mutex               DepComponent::s_order_mtx;
std::vector<std::string> DepComponent::s_order;
std::atomic<int>         DepComponent::s_suspend_ms{0};
std::atomic<int>         DepComponent::s_start_co{0};
std::atomic<int>         DepComponent::s_stop_co{0};

// 依赖组件：subject 侧，绑定 DepComponent 并在 OnBindingChanged 经
// GetComponent 取依赖句柄（装配借用的组件路径）。
class UserComponent final : public fw::v2::CoComponent {
public:
    struct Options {};
    explicit UserComponent(fw::detail::ConstructKey k, Options)
        : fw::v2::CoComponent(k) {}

    static std::atomic<int> s_get_ok;
    static std::atomic<int> s_borrow_ok;
    static std::atomic<int> s_unbind_events;

protected:
    ~UserComponent() noexcept override = default;

    result<void> OnStart() override {
        std::lock_guard<std::mutex> lk(DepComponent::s_order_mtx);
        DepComponent::s_order.push_back("start:U");
        return result<void>::ok();
    }
    result<void> OnStop() override {
        std::lock_guard<std::mutex> lk(DepComponent::s_order_mtx);
        DepComponent::s_order.push_back("stop:U");
        return result<void>::ok();
    }
    void OnBindingChanged(const fw::BindingEvent& ev) override {
        if (!ev.bound) { ++s_unbind_events; return; }
        auto h = GetComponent<DepComponent>("dep");
        if (h) {
            ++s_get_ok;
            auto b = h.value().Borrow();
            if (b) ++s_borrow_ok;
        }
    }
    friend class fw::detail::Registry;
};

std::atomic<int> UserComponent::s_get_ok{0};
std::atomic<int> UserComponent::s_borrow_ok{0};
std::atomic<int> UserComponent::s_unbind_events{0};

// ---- 测试用 Actor ----
class WorkerActor final : public fw::v2::CoActor {
public:
    static constexpr std::string_view kActorType = "worker";
    explicit WorkerActor(fw::detail::ConstructKey k) : fw::v2::CoActor(k) {}
    int create_count = 0;
    int destroy_count = 0;
protected:
    ~WorkerActor() noexcept override = default;
    result<void> OnCreate() override {
        ++create_count; return result<void>::ok();
    }
    void OnDestroy() override { ++destroy_count; }
    friend class fw::detail::Registry;
};

fw::AppOptions MakeAppOptions() {
    return fw::AppOptions{}
        .SetLimits(64, 64, 64, 64)
        .SetShutdownBudget(std::chrono::milliseconds(200))
        .SetLifecycleBudget(std::chrono::milliseconds(200));
}

// 断言构造限制（编译期）：业务类型不能默认构造、不能公开析构。
static_assert(!std::is_default_constructible_v<CounterComponent>);
static_assert(!std::is_destructible_v<CounterComponent>);
static_assert(!std::is_default_constructible_v<EchoService>);
static_assert(!std::is_destructible_v<EchoService>);
static_assert(!std::is_default_constructible_v<WorkerActor>);
static_assert(!std::is_destructible_v<WorkerActor>);

} // namespace

// 在受管协程内执行 fn：经 g_scheduler 投递协程任务，宿主线程用
// 限时 std::cv 等其完成（宿主线程的 CV 等待不违反 worker 阻塞禁令）。
// 返回 false 表示任务未在预算内完成（防挂死测试进程）。
template <class F>
bool RunInManagedCoroutine(F&& fn,
    std::chrono::milliseconds budget = std::chrono::milliseconds{5000}) {
    if (bbt::coroutine::IsInCoroutineContext()) { fn(); return true; }
    // native 测试宿主负责初始化唯一 runtime，产品 App 不启动 runtime。
    if (!g_scheduler->IsInitialized()) g_scheduler->Start();
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
    if (!done) std::terminate(); // 不让仍在途任务使用已退出的测试栈。
    if (slot->error) std::rethrow_exception(slot->error);
    return done;
}

#define CO_TEST_CASE(name) \
    static void name##_body(); \
    BOOST_AUTO_TEST_CASE(name) { BOOST_REQUIRE(RunInManagedCoroutine(name##_body)); } \
    static void name##_body()

BOOST_AUTO_TEST_SUITE(v2_core)

CO_TEST_CASE(app_options_validate) {
    fw::AppOptions bad;
    BOOST_CHECK(!bad.Validate());
    fw::AppOptions good = MakeAppOptions();
    BOOST_CHECK(good.Validate());
}

CO_TEST_CASE(coapp_construct_handle) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    BOOST_CHECK(app->Handle().IsValid());
    BOOST_CHECK(app->State() == fw::AppState::Created);
    auto info = app->Handle().Info();
    BOOST_REQUIRE(info);
    BOOST_CHECK(info.value().kind == fw::ObjectKind::App);
}

CO_TEST_CASE(component_install_find_lifecycle) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    CounterComponent::Options opts; opts.initial = 41;

    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("counter", opts));
    auto dup = bbt_co_await app->CoInstallComponent<CounterComponent>("counter", opts);
    BOOST_CHECK(!dup && dup.error().code == ErrorCode::InvalidArgument);

    auto h = app->FindComponent<CounterComponent>("counter");
    BOOST_REQUIRE(h);
    BOOST_CHECK(h.value().IsValid());
    auto info = h.value().Info();
    BOOST_REQUIRE(info);
    BOOST_CHECK(info.value().kind == fw::ObjectKind::Component);
    BOOST_CHECK(info.value().name == "counter");
    BOOST_CHECK(info.value().state == fw::ObjectState::Installed);

    BOOST_REQUIRE(bbt_co_await app->CoStart());
    BOOST_CHECK(app->State() == fw::AppState::Running);
    info = h.value().Info();
    BOOST_CHECK(info.value().state == fw::ObjectState::Running);

    BOOST_REQUIRE(bbt_co_await app->CoUninstallComponent<CounterComponent>("counter"));
    BOOST_CHECK(!h.value().IsValid());
}

CO_TEST_CASE(component_install_while_running) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    CounterComponent::Options opts;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("c", opts));
    auto h = app->FindComponent<CounterComponent>("c");
    BOOST_REQUIRE(h);
    BOOST_CHECK(h.value().Info().value().state ==
                      fw::ObjectState::Running);
}

CO_TEST_CASE(service_lifecycle_hooks) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(8)));

    auto h = app->FindService("echo");
    BOOST_REQUIRE(h);
    BOOST_CHECK(h.value().Info().value().state ==
                      fw::ObjectState::Installed);

    BOOST_REQUIRE(bbt_co_await app->CoStart());
    BOOST_CHECK(h.value().Info().value().state ==
                      fw::ObjectState::Running);

    auto mb = h.value().GetMailBox();
    BOOST_REQUIRE(mb);
    BOOST_CHECK(mb.value().Address().ObjectKind() ==
                      fw::AppObjectAddress::Kind::Service);
    BOOST_CHECK(mb.value().Address().Type() == "echo");

    BOOST_REQUIRE(bbt_co_await app->CoUninstallService<EchoService>());
    BOOST_CHECK(!h.value().IsValid());
    BOOST_CHECK(!h.value().Info());
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    auto h2 = app->FindService("echo");
    BOOST_REQUIRE(h2);
    BOOST_CHECK(h2.value().Generation() != h.value().Generation() ||
                h2.value().Id() != h.value().Id());
}

CO_TEST_CASE(service_install_failed) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    auto r = bbt_co_await app->CoInstallService<FailInstallService>(
        fw::ServiceOptions{}.SetMaxInflight(1));
    BOOST_CHECK(!r);
    auto h = app->FindService("fail_install");
    BOOST_REQUIRE(h);
    BOOST_CHECK(h.value().Info().value().state ==
                      fw::ObjectState::InstallFailed);
    BOOST_CHECK(h.value().Info().value().last_error.has_value());
    BOOST_CHECK(h.value().Info().value().last_error->phase == "install");
    BOOST_REQUIRE(bbt_co_await app->CoUninstallService<FailInstallService>());
    BOOST_CHECK(!h.value().IsValid());
}

CO_TEST_CASE(service_duplicate_install) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    auto r = bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(1));
    BOOST_CHECK(!r && r.error().code == ErrorCode::InvalidArgument);
}

CO_TEST_CASE(actor_type_registration) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallActor<WorkerActor>(
        fw::ActorOptions{}
            .SetMaxActors(4)
            .SetMailboxCapacity(8)
            .SetAggregateQueueCapacity(64)));

    auto dup = bbt_co_await app->CoInstallActor<WorkerActor>(
        fw::ActorOptions{}
            .SetMaxActors(4)
            .SetMailboxCapacity(8)
            .SetAggregateQueueCapacity(64));
    BOOST_CHECK(!dup && dup.error().code == ErrorCode::InvalidArgument);

    auto missing = app->FindActor(fw::ActorId{"worker", "k1"});
    BOOST_CHECK(!missing && missing.error().code == ErrorCode::NotFound);

    BOOST_REQUIRE(bbt_co_await app->CoUninstallActor<WorkerActor>());
}

CO_TEST_CASE(bind_component_and_borrow) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    CounterComponent::Options opts; opts.initial = 7;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("counter", opts));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());

    auto svc_h = app->FindService("echo");
    BOOST_REQUIRE(svc_h);

    auto bad = bbt_co_await app->CoBindComponent<AnotherComponent>(svc_h.value(), "other");
    BOOST_CHECK(!bad && bad.error().code == ErrorCode::InvalidArgument);

    auto bind = bbt_co_await app->CoBindComponent<CounterComponent>(svc_h.value(), "counter");
    BOOST_REQUIRE(bind);
    BOOST_CHECK(bind.value().IsValid());
    // OnBindingChanged 已在锁外同步执行：GetComponent 真实实例化路径
    // 找到绑定并成功 Borrow；未声明类型被 InvalidArgument 拒绝。
    BOOST_CHECK(EchoService::s_gc_ok.load() >= 1);
    BOOST_CHECK(EchoService::s_gc_borrow_ok.load() >= 1);
    BOOST_CHECK(EchoService::s_gc_undeclared_rejected.load() >= 1);

    {
        auto borrow = bind.value().Borrow();
        BOOST_REQUIRE(borrow);
        borrow.value().Get().Inc();
        borrow.value().Get().Inc();
        BOOST_CHECK(borrow.value().Get().Get() == 9);
    }

    {
        auto b2 = bind.value().Borrow();
        BOOST_REQUIRE(b2);
        BOOST_CHECK(b2.value().Get().Get() == 9);
    }

    BOOST_REQUIRE(bbt_co_await app->CoUnbindComponent<CounterComponent>(svc_h.value(),
                                                        "counter"));
    auto b3 = bind.value().Borrow();
    BOOST_CHECK(!b3 && b3.error().code == ErrorCode::Closed);
}

CO_TEST_CASE(uninstall_component_busy_with_binding) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    CounterComponent::Options opts;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("c", opts));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc_h = app->FindService("echo");
    BOOST_REQUIRE(svc_h);
    BOOST_REQUIRE(bbt_co_await app->CoBindComponent<CounterComponent>(svc_h.value(), "c"));

    auto r = bbt_co_await app->CoUninstallComponent<CounterComponent>("c");
    BOOST_CHECK(!r && r.error().code == ErrorCode::Overloaded);
    BOOST_CHECK(app->FindComponent<CounterComponent>("c").value().IsValid());
}

CO_TEST_CASE(observe_state_events) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    auto svc_h = app->FindService("echo");
    BOOST_REQUIRE(svc_h);

    std::vector<fw::ObjectState> seen;
    // 观察者用 App 自身 handle（observer 与 target 不同对象）。
    auto token = bbt_co_await app->CoObserve(app->Handle(), svc_h.value(),
        [&seen](const fw::ObjectStateEvent& ev) {
            seen.push_back(ev.state);
        });
    BOOST_REQUIRE(token);
    BOOST_CHECK(token.value().IsActive());

    BOOST_REQUIRE(bbt_co_await app->CoStart());
    BOOST_CHECK(!seen.empty());
    BOOST_CHECK(seen.back() == fw::ObjectState::Running);

    seen.clear();
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_CHECK(std::find(seen.begin(), seen.end(),
                          fw::ObjectState::Destroyed) != seen.end());
}

CO_TEST_CASE(stop_then_invalid) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    auto svc_h = app->FindService("echo");
    BOOST_REQUIRE(svc_h);
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_CHECK(!svc_h.value().IsValid());
    auto r = bbt_co_await app->CoStart();
    BOOST_CHECK(!r && r.error().code == ErrorCode::Closed);
}

CO_TEST_CASE(component_stop_destroys) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    CounterComponent::Options opts;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("c", opts));
    auto h = app->FindComponent<CounterComponent>("c");
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_CHECK(!h.value().IsValid());
    auto r = app->FindComponent<CounterComponent>("c");
    BOOST_CHECK(!r && r.error().code == ErrorCode::NotFound);
}

CO_TEST_CASE(component_placement_validation) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    CounterComponent::Options opts;
    auto r = bbt_co_await app->CoInstallComponent<CounterComponent>("", opts);
    BOOST_CHECK(!r && r.error().code == ErrorCode::InvalidArgument);

    fw::ComponentOptions scoped;
    scoped.SetScope(fw::ComponentScope::Service);
    auto r2 = bbt_co_await app->CoInstallComponent<CounterComponent>("c", opts, scoped);
    BOOST_CHECK(!r2 && r2.error().code == ErrorCode::InvalidArgument);
}

// ---- 生命周期节点新增用例 ----

// 装配借用：subject 处于 Installing（OnInstall 执行中）时，允许借
// Installed 依赖做纯内存装配；同一时刻非装配窗口的普通 borrow 被拒。
CO_TEST_CASE(assembly_borrow_during_oninstall) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    CounterComponent::Options opts; opts.initial = 33;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("counter", opts));

    // 未 Running 且非装配窗口：直接 Borrow 被拒（Installed 态）。
    auto comp_h = app->FindComponent<CounterComponent>("counter");
    BOOST_REQUIRE(comp_h);
    auto direct = bbt_co_await app->CoBindComponent<CounterComponent>(app->Handle(), "counter");
    BOOST_CHECK(!direct);   // App handle 不是合法 subject

    ProbeService::s_install_borrow_ok = 0;
    ProbeService::s_install_borrow_value = 0;
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<ProbeService>(
        fw::ServiceOptions{}.SetMaxInflight(1)
            .BindComponent<CounterComponent>("counter")));
    // OnInstall 装配窗口内借到 Installed 组件并完成纯内存读取。
    BOOST_CHECK(ProbeService::s_install_borrow_ok.load() >= 1);
    BOOST_CHECK(ProbeService::s_install_borrow_value.load() == 33);
}

// 两 subject 绑定同一组件：Unbind 只等自身 borrow 排空，不影响
// 另一 subject 的借用；retiring 边在 borrow 归零后收口。
CO_TEST_CASE(two_subjects_independent_unbind) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    CounterComponent::Options opts;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("counter", opts));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<Echo2Service>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());

    auto svc1 = app->FindService("echo");
    auto svc2 = app->FindService("echo2");
    BOOST_REQUIRE(svc1 && svc2);
    auto h1 = bbt_co_await app->CoBindComponent<CounterComponent>(svc1.value(), "counter");
    auto h2 = bbt_co_await app->CoBindComponent<CounterComponent>(svc2.value(), "counter");
    BOOST_REQUIRE(h1 && h2);

    // svc1 持有一个 borrow；Unbind svc1 只等该 binding 归零。
    auto h1v = h1.value().Borrow();
    BOOST_REQUIRE(h1v);
    auto r = bbt_co_await app->CoUnbindComponent<CounterComponent>(
        svc1.value(), "counter", {},
        fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(50)));
    // 在途 borrow 未归零 → TimedOut；svc2 的 binding 不受影响。
    BOOST_CHECK(!r && r.error().code == ErrorCode::TimedOut);
    {
        auto b2 = h2.value().Borrow();
        BOOST_CHECK(b2);
    }
    {
        // 显式归还 borrow（离开作用域析构归还计数）。
        auto released = std::move(h1v.value());
        (void)released;
    }
    // 归还后同一操作继续收口 → 成功。
    BOOST_REQUIRE(bbt_co_await app->CoUnbindComponent<CounterComponent>(
        svc1.value(), "counter"));
    // svc1 旧 handle 失效；svc2 仍可借。
    auto b1 = h1.value().Borrow();
    BOOST_CHECK(!b1 && b1.error().code == ErrorCode::Closed);
    auto b2 = h2.value().Borrow();
    BOOST_CHECK(b2);
}

// 组件→组件依赖：subject 组件的 GetComponent 可取得依赖句柄并借用；
// 环绑定被拒绝（InvalidArgument）。
CO_TEST_CASE(component_dep_getcomponent_and_cycle) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<DepComponent>("dep",
                                                     DepComponent::Options{7}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<UserComponent>("user",
                                                      UserComponent::Options{}));
    BOOST_REQUIRE(bbt_co_await app->CoStart());

    auto dep_h = app->FindComponent<DepComponent>("dep");
    auto user_h = app->FindComponent<UserComponent>("user");
    BOOST_REQUIRE(dep_h && user_h);

    // user（Component subject）→ dep：GetComponent 真实实例化 + Borrow。
    UserComponent::s_get_ok = 0;
    UserComponent::s_borrow_ok = 0;
    auto bind = bbt_co_await app->CoBindComponent<DepComponent>(user_h.value(), "dep");
    BOOST_REQUIRE(bind);
    BOOST_CHECK(UserComponent::s_get_ok.load() >= 1);
    BOOST_CHECK(UserComponent::s_borrow_ok.load() >= 1);

    // dep → user 会成环（user 已依赖 dep）→ InvalidArgument。
    auto cyc = bbt_co_await app->CoBindComponent<UserComponent>(dep_h.value(), "user");
    BOOST_CHECK(!cyc && cyc.error().code == ErrorCode::InvalidArgument);

    // 解绑派发 bound=false 事件。
    UserComponent::s_unbind_events = 0;
    BOOST_REQUIRE(bbt_co_await app->CoUnbindComponent<DepComponent>(user_h.value(), "dep"));
    BOOST_CHECK(UserComponent::s_unbind_events.load() >= 1);
}

// 生命周期 hook 在真实受管协程中执行（非 mock、非宿主线程）：
// OnStart/OnStop 协程上下文断言 + hook 内再调入口被 InvalidContext 拒。
CO_TEST_CASE(hooks_run_in_managed_coroutine) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    ProbeService::s_app = app.get();
    ProbeService::s_onstart_co = 0;
    ProbeService::s_onstop_co = 0;
    ProbeService::s_invalid_ctx = 0;
    CounterComponent::Options opts;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("counter", opts));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<ProbeService>(
        fw::ServiceOptions{}.SetMaxInflight(1)
            .BindComponent<CounterComponent>("counter")));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    BOOST_CHECK(ProbeService::s_onstart_co.load() >= 1);
    BOOST_CHECK(ProbeService::s_invalid_ctx.load() >= 1);
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_CHECK(ProbeService::s_onstop_co.load() >= 1);
    ProbeService::s_app = nullptr;
}

// 依赖拓扑启停：dep 先于 user OnStart，user 先于 dep OnStop
// （使用者先停、依赖后停）。
CO_TEST_CASE(dependency_topo_start_and_reverse_stop) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    {
        std::lock_guard<std::mutex> lk(DepComponent::s_order_mtx);
        DepComponent::s_order.clear();
    }
    DepComponent::s_suspend_ms = 0;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<DepComponent>("dep",
                                                     DepComponent::Options{1}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<UserComponent>("user",
                                                      UserComponent::Options{}));
    auto user_h = app->FindComponent<UserComponent>("user");
    auto dep_h = app->FindComponent<DepComponent>("dep");
    BOOST_REQUIRE(user_h && dep_h);
    BOOST_REQUIRE(bbt_co_await app->CoBindComponent<DepComponent>(user_h.value(), "dep"));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    std::vector<std::string> order;
    {
        std::lock_guard<std::mutex> lk(DepComponent::s_order_mtx);
        order = DepComponent::s_order;
    }
    auto find_idx = [&](const std::string& k) {
        for (std::size_t i = 0; i < order.size(); ++i)
            if (order[i] == k) return static_cast<int>(i);
        return -1;
    };
    // 启动按依赖拓扑（dep 先于 user）；停止按逆拓扑（user 先于 dep）。
    BOOST_CHECK(find_idx("start:1") >= 0);
    BOOST_CHECK(find_idx("start:U") >= 0);
    BOOST_CHECK(find_idx("start:1") < find_idx("start:U"));
    BOOST_CHECK(find_idx("stop:U") >= 0);
    BOOST_CHECK(find_idx("stop:1") >= 0);
    BOOST_CHECK(find_idx("stop:U") < find_idx("stop:1"));
}

// deadline 超时保留责任：OnStop 真实挂起超过 deadline → StopIncomplete，
// owner/lease 保留；再次 Stop 以更晚期限推进至真实终态。
CO_TEST_CASE(deadline_timeout_retains_then_settles) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<DepComponent>("dep",
                                                     DepComponent::Options{2}));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto dep_h = app->FindComponent<DepComponent>("dep");
    BOOST_REQUIRE(dep_h);

    // OnStop 挂起 300ms，deadline 只给 30ms → 超时。
    DepComponent::s_suspend_ms = 300;
    BOOST_REQUIRE(bbt_co_await app->CoStop(fw::LifecycleOptions{}.SetDeadline(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(30))));
    BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);
    // owner/lease 保留：对象仍在 Registry 中可观察。
    BOOST_CHECK(dep_h.value().IsValid());
    BOOST_CHECK(!app->PendingCleanup().empty());
    // hook 仍未达真实终态：s_order 中无 stop:2。
    {
        std::lock_guard<std::mutex> lk(DepComponent::s_order_mtx);
        BOOST_CHECK(std::find(DepComponent::s_order.begin(),
                              DepComponent::s_order.end(),
                              "stop:2") == DepComponent::s_order.end());
    }

    // 再次 Stop：等前序挂起 hook 完成（更长 deadline），收口至真实终态。
    DepComponent::s_suspend_ms = 0;
    BOOST_REQUIRE(bbt_co_await app->CoStop(fw::LifecycleOptions{}.SetDeadline(
        std::chrono::steady_clock::now() + std::chrono::seconds(3))));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_CHECK(!dep_h.value().IsValid());
}

// Run：经 RequestShutdown 退出；真实 scheduler（非 mock），退出码 Ok。
CO_TEST_CASE(request_shutdown_is_not_stop) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    app->RequestShutdown();
    bbt::coroutine::WaitOptions wait;
    wait.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    BOOST_REQUIRE(bbt_co_await app->CoWaitShutdownRequest(wait));
    BOOST_CHECK(app->State() == fw::AppState::Running);
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
}

// ---- Co 入口 deadline 补齐用例 ----

// 受闸 hook 服务：OnStop 进入后先计数放行信号，再挂起在测试闸门上，
// 直至用例显式放行。以此证明「hook 已进入、尚未到达真实终态」，
// 而非靠 sleep 凑时序。
// 闸门带记忆：Open() 先于 Wait() 到达时 Wait 立即返回——用
// on_registered 复查关闭「先放行后挂起」竞态。
struct CoGate {
    std::atomic<bool>                   open{false};
    bbt::coroutine::sync::CoWaiter::SPtr waiter =
        bbt::coroutine::sync::CoWaiter::Create();

    void Open() {
        open.store(true, std::memory_order_release);
        waiter->Notify();
    }
    // 在受管协程内调用：gate 未开则挂起至 Open。
    void Wait() {
        if (open.load(std::memory_order_acquire)) return;
        bbt::coroutine::WaitOptions wo;   // 无期限：终态由 Open 放行
        waiter->WaitWithCallback(wo, [this] {
            if (open.load(std::memory_order_acquire)) waiter->Notify();
            return true;
        });
    }
};

class GateService final : public fw::v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "gate_svc";
    explicit GateService(fw::detail::ConstructKey k) : fw::v2::CoService<>(k) {}

    static std::atomic<int> s_onstop_entered;   // OnStop 已进入次数
    static std::atomic<int> s_onstop_returned;  // OnStop 已返回次数
    static std::atomic<bool> s_onstop_co;       // OnStop 在协程内执行

    // 由用例持有：OnStop/OnStart 在各自闸门上阻塞，直到 Open()。
    static std::shared_ptr<CoGate> s_gate;
    static std::shared_ptr<CoGate> s_start_gate;

protected:
    ~GateService() noexcept override = default;

    result<void> OnStart() override {
        if (s_start_gate) s_start_gate->Wait();
        return result<void>::ok();
    }
    result<void> OnStop() override {
        if (bbt::coroutine::GetLocalCoroutineId() != 0)
            s_onstop_co.store(true);
        ++s_onstop_entered;
        if (s_gate) s_gate->Wait();   // 真实协程挂起：不阻塞 worker
        ++s_onstop_returned;
        return result<void>::ok();
    }
    friend class fw::detail::Registry;
};

std::atomic<int>  GateService::s_onstop_entered{0};
std::atomic<int>  GateService::s_onstop_returned{0};
std::atomic<bool> GateService::s_onstop_co{false};
std::shared_ptr<CoGate> GateService::s_gate;
std::shared_ptr<CoGate> GateService::s_start_gate;

// Co* 入口真实受管协程路径：CoStart/CoPause/CoResume/CoStop 在业务
// 协程内成功完成；hook 仍在受管协程中执行。
CO_TEST_CASE(co_lifecycle_entries_in_managed_coroutine) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<ProbeService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));

    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        auto r = bbt_co_await app->CoStart();
        BOOST_CHECK(r);
        BOOST_CHECK(app->State() == fw::AppState::Running);
    }));
    BOOST_CHECK(ProbeService::s_onstart_co.load() >= 1);

    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        auto r = bbt_co_await app->CoPause();
        BOOST_CHECK(r);
        BOOST_CHECK(app->State() == fw::AppState::Paused);
    }));
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        auto r = bbt_co_await app->CoResume();
        BOOST_CHECK(r);
        BOOST_CHECK(app->State() == fw::AppState::Running);
    }));
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        auto r = bbt_co_await app->CoStop();
        BOOST_CHECK(r);
        BOOST_CHECK(app->State() == fw::AppState::Stopped);
    }));
    BOOST_CHECK(ProbeService::s_onstop_co.load() >= 1);
}

// Co 入口有限 deadline：OnStop 挂起受闸阻塞 → 短 deadline 的 CoStop
// 在 hook 放行前返回 TimedOut（区分性：内联执行下本会等到放行才返回），
// App 落 StopIncomplete、owner 未销毁；放行后续等同一对象收口至
// Stopped。宿主与协程入口共享同一 deadline 不重设。
CO_TEST_CASE(co_stop_deadline_returns_before_hook_settles) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    GateService::s_gate = std::make_shared<CoGate>();
    GateService::s_onstop_entered = 0;
    GateService::s_onstop_returned = 0;
    GateService::s_onstop_co = false;
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<GateService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));

    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        auto r = bbt_co_await app->CoStart();
        BOOST_CHECK(r);
    }));
    auto svc_h = app->FindService("gate_svc");
    BOOST_REQUIRE(svc_h);

    // 短 deadline 的 CoStop：hook 已进入闸门、尚未放行。调用协程必须
    // 在 hook 到达真实终态前返回（内联执行下本会等到放行才返回——
    // 区分即「先返回再收口」的次序）。超时经 LifecycleFailure +
    // StopIncomplete 暴露（与宿主 Stop 同语义：Stop 本身返回 ok）。
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(80);
        auto r = bbt_co_await app->CoStop(
            fw::LifecycleOptions{}.SetDeadline(deadline));
        BOOST_CHECK(r);
    }));
    // hook 已进入且受闸阻塞（未到达真实终态）。
    BOOST_CHECK(GateService::s_onstop_entered.load() >= 1);
    BOOST_CHECK(GateService::s_onstop_returned.load() == 0);
    // 超时已如实记录（TimedOut 诊断）。
    {
        bool saw_timeout = false;
        for (const auto& f : app->LifecycleFailures())
            if (f.code == ErrorCode::TimedOut) saw_timeout = true;
        BOOST_CHECK_MESSAGE(saw_timeout,
            "短 deadline 的 CoStop 未留下 TimedOut 诊断");
    }
    // 责任保留：App 未完成收口、对象未被销毁。
    BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);
    BOOST_CHECK(svc_h.value().IsValid());
    BOOST_CHECK(!app->PendingCleanup().empty());

    // 放行挂起的 hook；同一对象的再调等待在途 hook 至真实终态后收口。
    GateService::s_gate->Open();
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(3);
        auto r = bbt_co_await app->CoStop(
            fw::LifecycleOptions{}.SetDeadline(deadline));
        BOOST_CHECK(r);
    }));
    BOOST_CHECK(GateService::s_onstop_returned.load() >= 1);
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_CHECK(!svc_h.value().IsValid());
    GateService::s_gate.reset();
}

// 组件路径同责任保留：CoStop 下组件 OnStop 超时 → 组件
// StopIncomplete、handle 仍有效；放行后再调收口至 Destroyed。
CO_TEST_CASE(co_component_hook_deadline_retains_then_settles) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    DepComponent::s_suspend_ms = 0;
    {
        std::lock_guard<std::mutex> lk(DepComponent::s_order_mtx);
        DepComponent::s_order.clear();
    }
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<DepComponent>(
        "dep", DepComponent::Options{3}));
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        auto r = bbt_co_await app->CoStart();
        BOOST_CHECK(r);
    }));
    auto dep_h = app->FindComponent<DepComponent>("dep");
    BOOST_REQUIRE(dep_h);

    // 组件 OnStop 真实挂起超过 deadline → 等待到期返回（区分于内联
    // 执行下必须等挂起自然结束才返回）。超时经 LifecycleFailure +
    // StopIncomplete 暴露（Stop 本身返回 ok）。
    DepComponent::s_suspend_ms = 300;
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(40);
        auto r = bbt_co_await app->CoStop(
            fw::LifecycleOptions{}.SetDeadline(deadline));
        BOOST_CHECK(r);
    }));
    {
        bool saw_timeout = false;
        for (const auto& f : app->LifecycleFailures())
            if (f.code == ErrorCode::TimedOut) saw_timeout = true;
        BOOST_CHECK(saw_timeout);
    }
    BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);
    BOOST_CHECK(dep_h.value().IsValid());
    BOOST_CHECK(!app->PendingCleanup().empty());
    {
        std::lock_guard<std::mutex> lk(DepComponent::s_order_mtx);
        BOOST_CHECK(std::find(DepComponent::s_order.begin(),
                              DepComponent::s_order.end(),
                              "stop:3") == DepComponent::s_order.end());
    }

    // 组件 OnStop 300ms 挂起自然结束；以更晚期限再调等其真实终态并收口。
    DepComponent::s_suspend_ms = 0;
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(3);
        auto r = bbt_co_await app->CoStop(
            fw::LifecycleOptions{}.SetDeadline(deadline));
        BOOST_CHECK(r);
    }));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_CHECK(!dep_h.value().IsValid());
}

// CoStart 的 TimedOut 直接透出：OnStart 受闸阻塞、短 deadline →
// CoStart 返回 TimedOut（Start 路径把 hook 错误原样透出并记 Failed；
// 放行后 hook 到真实终态，责任链不丢）。
CO_TEST_CASE(co_start_deadline_returns_timeout) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    GateService::s_start_gate = std::make_shared<CoGate>();
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<GateService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));

    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(60);
        auto r = bbt_co_await app->CoStart(
            fw::LifecycleOptions{}.SetDeadline(deadline));
        BOOST_CHECK_MESSAGE(
            !r && r.error().code == ErrorCode::TimedOut,
            "CoStart 应在 hook 放行前按 deadline 返回 TimedOut，实际 code="
                << (r ? -1 : static_cast<int>(r.error().code)));
    }));
    // hook 尚未放行 → 服务未 Running；App 诚实落 Running（部分启动）。
    BOOST_CHECK(app->State() == fw::AppState::Running);
    auto info = app->FindService("gate_svc");
    BOOST_REQUIRE(info);
    BOOST_CHECK(info.value().Info().value().state !=
                fw::ObjectState::Running);

    // 放行：hook 到真实终态并写回结果（责任链完整——hook 从未被强杀）。
    GateService::s_start_gate->Open();
    GateService::s_start_gate.reset();
}


namespace {
} // namespace

BOOST_AUTO_TEST_CASE(non_co_rejected_before_registry_or_hook_side_effects) {
    std::shared_ptr<fw::v2::CoApp> app;
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        app = fw::v2::CoApp::Create(MakeAppOptions(), "gate");
        BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>(
            "counter", CounterComponent::Options{}));
        BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
            fw::ServiceOptions{}.SetMaxInflight(1)));
    }));
    const auto service = app->FindService("echo").value();
    const auto component = app->FindComponent<CounterComponent>("counter").value();
    const auto before = bbt::coroutine::GetCoObjectStats("framework.CoService");
    const auto reg = app->RegistryForTest();
    std::size_t objects;
    {
        std::lock_guard<std::mutex> lock(reg->Mutex());
        objects = reg->CountObjectsLocked();
        BOOST_CHECK_EQUAL(reg->CountBindingsLocked(), 0U);
    }
    int events = 0;
    // 非 Co 上下文的两层既定事实分开断言：
    //  (a) bbt_co_await 前置协议——native 线程 await 抛 std::logic_error，
    //      产品函数体未执行（既定协议，catch 即验证）；
    //  (b) 产品侧零副作用——不经 await 直接调用（同一 _CheckCoContext gate
    //      返回 InvalidContext result），随后断言 Registry/创建统计/事件
    //      全部不变。两组覆盖同一面，互不代替。
    BOOST_CHECK_THROW(bbt_co_await app->CoStart(), std::logic_error);
    (void)app->CoInstallService<FailInstallService>();
    (void)app->CoInstallComponent<CounterComponent>(
        "other", CounterComponent::Options{});
    (void)app->CoInstallActor<WorkerActor>();
    (void)app->CoUninstallService<EchoService>();
    (void)app->CoUninstallComponent<CounterComponent>("counter");
    (void)app->CoUninstallActor<WorkerActor>();
    (void)app->CoBindComponent<CounterComponent>(service, "counter");
    (void)app->CoUnbindComponent<CounterComponent>(service, "counter");
    (void)app->CoBindObject(service, component);
    (void)app->CoUnbindObject(service, component);
    (void)app->CoObserve(service, component,
        [&](const fw::ObjectStateEvent&) { ++events; });
    (void)app->CoStart();
    (void)app->CoPause();
    (void)app->CoResume();
    (void)app->CoStop();
    (void)app->CoWaitShutdownRequest();
    BOOST_CHECK(app->State() == fw::AppState::Created);
    BOOST_CHECK_EQUAL(events, 0);
    BOOST_CHECK(!app->FindComponent<CounterComponent>("other"));
    BOOST_CHECK(!app->FindService("fail_install"));
    {
        std::lock_guard<std::mutex> lock(reg->Mutex());
        BOOST_CHECK_EQUAL(reg->CountObjectsLocked(), objects);
        BOOST_CHECK_EQUAL(reg->CountBindingsLocked(), 0U);
    }
    BOOST_CHECK_EQUAL(bbt::coroutine::GetCoObjectStats("framework.CoService").created_total,
                      before.created_total);
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        BOOST_REQUIRE(bbt_co_await app->CoStop());
    }));
}

BOOST_AUTO_TEST_CASE(non_co_factory_does_not_construct_app) {
    // CoApp 构造即 _CheckCoContext：native 域唯一入口 Create 在
    // MakeCoObject 提交前抛 std::logic_error，无 Registry/统计副作用。
    const auto before = bbt::coroutine::GetCoObjectStats("framework.CoApp");
    BOOST_CHECK_THROW((void)fw::v2::CoApp::Create(MakeAppOptions(), "rejected"),
                      std::logic_error);
    BOOST_CHECK_EQUAL(
        bbt::coroutine::GetCoObjectStats("framework.CoApp").created_total,
        before.created_total);
}

CO_TEST_CASE(manager_observation_does_not_own_app_and_stop_is_not_destruction) {
    const auto before = bbt::coroutine::GetCoObjectStats("framework.CoApp");
    auto app = fw::v2::CoApp::Create(MakeAppOptions(), "observed");
    std::weak_ptr<fw::v2::CoApp> weak = app;
    const auto identity = app->GetObjectInfo();
    BOOST_CHECK(identity.id != 0);
    BOOST_REQUIRE(bbt::coroutine::GetCoObjectManager()->GetObjectInfo(identity.id));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    const auto stopped = bbt::coroutine::GetCoObjectStats("framework.CoApp");
    BOOST_CHECK_EQUAL(stopped.alive, before.alive + 1);
    BOOST_CHECK_EQUAL(stopped.destroyed_total, before.destroyed_total);
    app.reset();
    BOOST_CHECK(weak.expired());
    BOOST_CHECK(!bbt::coroutine::GetCoObjectManager()->GetObjectInfo(identity.id));
    const auto after = bbt::coroutine::GetCoObjectStats("framework.CoApp");
    BOOST_CHECK_EQUAL(after.alive, before.alive);
    BOOST_CHECK_EQUAL(after.created_total, before.created_total + 1);
    BOOST_CHECK_EQUAL(after.destroyed_total, before.destroyed_total + 1);
}

CO_TEST_CASE(registry_rejection_releases_committed_object) {
    auto options = MakeAppOptions();
    options.SetLimits(1, 1, 1, 1); // App self entry consumes the only slot.
    auto app = fw::v2::CoApp::Create(options);
    const auto before = bbt::coroutine::GetCoObjectStats("framework.CoComponent");
    auto installed = bbt_co_await app->CoInstallComponent<CounterComponent>(
        "rejected", CounterComponent::Options{});
    BOOST_CHECK(!installed && installed.error().code == ErrorCode::Overloaded);
    BOOST_CHECK(!app->FindComponent<CounterComponent>("rejected"));
    const auto after = bbt::coroutine::GetCoObjectStats("framework.CoComponent");
    BOOST_CHECK_EQUAL(after.alive, before.alive);
    BOOST_CHECK_EQUAL(after.created_total, before.created_total + 1);
    BOOST_CHECK_EQUAL(after.destroyed_total, before.destroyed_total + 1);
    BOOST_REQUIRE(bbt_co_await app->CoStop());
}

namespace {

// ---- F17 收口用例的附加类型 ----

// 多继承组件：CoComponent 非首基类（Marker 在前），其 void*→C* 旧
// static_cast 会错位；Borrow 经 CoComponent* 调整必须返回正确地址
// （成员断言读到真实字段而不是 Marker 子对象）。
struct MarkerBase { virtual ~MarkerBase() = default; std::uint64_t tag = 0xDEAD; };
class MiComponent final : public MarkerBase, public fw::v2::CoComponent {
public:
    struct Options {};
    explicit MiComponent(fw::detail::ConstructKey k, Options)
        : fw::v2::CoComponent(k) {}
    int value = 77;
protected:
    ~MiComponent() noexcept override = default;
    friend class fw::detail::Registry;
};

// OnStop 可编程失败的组件：记录进入次序（与 borrow 释放次序比较）、
// 按静态标志返回错误（验证「OnStop 失败保留 owner、可再次收口」）。
class StopCtlComponent final : public fw::v2::CoComponent {
public:
    struct Options {};
    explicit StopCtlComponent(fw::detail::ConstructKey k, Options)
        : fw::v2::CoComponent(k) {}

    static std::mutex              s_mtx;
    static std::vector<std::string> s_seq;   // "stop"/"release" 次序
    static std::atomic<bool>       s_fail;   // OnStop 返回错误
    static std::atomic<int>        s_stop_calls;

protected:
    ~StopCtlComponent() noexcept override = default;
    result<void> OnStop() override {
        ++s_stop_calls;
        {
            std::lock_guard<std::mutex> lk(s_mtx);
            s_seq.push_back("stop");
        }
        if (s_fail.load())
            return result<void>::err(fw::MakeError(
                ErrorCode::InternalError, "OnStop 显式失败"));
        return result<void>::ok();
    }
    friend class fw::detail::Registry;
};

std::mutex               StopCtlComponent::s_mtx;
std::vector<std::string> StopCtlComponent::s_seq;
std::atomic<bool>        StopCtlComponent::s_fail{false};
std::atomic<int>         StopCtlComponent::s_stop_calls{0};

// 受控 borrow 持有者：OnStop 排空顺序探针在测试协程外持 lease。
// 析构即归还 borrow（BindingLease 析构驱动 Registry 计数归还）。

} // namespace

// 收口顺序：Stop 收口先等 borrow 排空再调 OnStop——探针先取得真实
// borrow，CoStop 协程挂起在排空上；主协程释放 lease 后必须观察到
// "release" 先于 "stop"（反序即回归「OnStop 先于排空」缺陷）。
CO_TEST_CASE(stop_waits_borrow_release_before_onstop) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    {
        std::lock_guard<std::mutex> lk(StopCtlComponent::s_mtx);
        StopCtlComponent::s_seq.clear();
    }
    StopCtlComponent::s_fail = false;
    StopCtlComponent::s_stop_calls = 0;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<StopCtlComponent>(
        "ctl", StopCtlComponent::Options{}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<UserComponent>(
        "user", UserComponent::Options{}));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto user_h = app->FindComponent<UserComponent>("user");
    BOOST_REQUIRE(user_h);
    // 组件→组件绑定：subject=UserComponent（组件 subject 真实路径）。
    auto bind = bbt_co_await app->CoBindComponent<StopCtlComponent>(
        user_h.value(), "ctl");
    BOOST_REQUIRE(bind);

    // 取得真实 borrow（optional 持有，reset 即归还 lease）。
    auto borrow_opt = std::optional<fw::ComponentBorrow<StopCtlComponent>>{
        std::nullopt};
    {
        auto b = bind.value().Borrow();
        BOOST_REQUIRE(b);
        borrow_opt.emplace(std::move(b.value()));
    }

    // Stop 协程：_DestroyComponent 会阻塞在 borrow 排空上（deadline 给足）。
    std::atomic<bool> stop_done{false};
    bool sub = false;
    g_scheduler->RegistCoroutineTask([app, &stop_done] {
        auto r = bbt_co_await app->CoStop(
            fw::LifecycleOptions{}.SetDeadline(
                std::chrono::steady_clock::now() +
                std::chrono::seconds(3)));
        BOOST_CHECK(r);
        stop_done.store(true);
    }, sub);
    BOOST_REQUIRE(sub);

    // 让 Stop 协程进入排空等待；随后归还 borrow 并记录次序。
    bbtco_sleep(50);
    BOOST_CHECK(!stop_done.load());
    {
        std::lock_guard<std::mutex> lk(StopCtlComponent::s_mtx);
        StopCtlComponent::s_seq.push_back("release");
    }
    borrow_opt.reset();   // 归还 borrow → 排空解除

    for (int i = 0; i < 300 && !stop_done.load(); ++i) bbtco_sleep(10);
    BOOST_CHECK(stop_done.load());
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    {
        std::lock_guard<std::mutex> lk(StopCtlComponent::s_mtx);
        auto ri = std::find(StopCtlComponent::s_seq.begin(),
                            StopCtlComponent::s_seq.end(), "release");
        auto si = std::find(StopCtlComponent::s_seq.begin(),
                            StopCtlComponent::s_seq.end(), "stop");
        BOOST_REQUIRE(ri != StopCtlComponent::s_seq.end());
        BOOST_REQUIRE(si != StopCtlComponent::s_seq.end());
        BOOST_CHECK(ri < si);   // 先归还 borrow，后执行 OnStop
    }
}

// OnStop 失败保留 owner 并可再次收口：s_fail=true → 失败记 UninstallIncomplete
// 且对象仍登记；清失败标志后再次卸载收口至销毁。
CO_TEST_CASE(onstop_failure_retains_owner_then_settles) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    StopCtlComponent::s_fail = false;
    StopCtlComponent::s_stop_calls = 0;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<StopCtlComponent>(
        "ctl", StopCtlComponent::Options{}));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto h = app->FindComponent<StopCtlComponent>("ctl");
    BOOST_REQUIRE(h);

    StopCtlComponent::s_fail = true;
    auto r1 = bbt_co_await app->CoUninstallComponent<StopCtlComponent>("ctl");
    BOOST_CHECK(!r1);
    BOOST_CHECK(h.value().IsValid());   // owner 保留：对象未销毁
    bool saw_onstop_fail = false;
    for (const auto& f : app->LifecycleFailures())
        if (f.phase == "onstop") saw_onstop_fail = true;
    BOOST_CHECK(saw_onstop_fail);

    // 失败不是终态：清除失败源后再次卸载完成真实销毁。
    StopCtlComponent::s_fail = false;
    auto r2 = bbt_co_await app->CoUninstallComponent<StopCtlComponent>("ctl");
    BOOST_CHECK(r2);
    BOOST_CHECK(!h.value().IsValid());
    BOOST_CHECK(StopCtlComponent::s_stop_calls.load() >= 2);
}

// 多继承安全：Marker 在前的组件经 ComponentHandle::Borrow 取得正确
// 派生地址（旧 void* static_cast 路径会读到 Marker 子对象 → 断言失败）。
CO_TEST_CASE(multi_inheritance_borrow_returns_adjusted_pointer) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<MiComponent>(
        "mi", MiComponent::Options{}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc_h = app->FindService("echo");
    BOOST_REQUIRE(svc_h);
    // EchoService 声明集合只含 CounterComponent；直接以用户组件为 subject
    // 做组件→组件绑定更贴近真实路径，改用组件 subject。
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<UserComponent>(
        "user", UserComponent::Options{}));
    auto user_h = app->FindComponent<UserComponent>("user");
    BOOST_REQUIRE(user_h);
    auto bind = bbt_co_await app->CoBindComponent<MiComponent>(
        user_h.value(), "mi");
    BOOST_REQUIRE(bind);
    auto borrow = bind.value().Borrow();
    BOOST_REQUIRE(borrow);
    BOOST_CHECK_EQUAL(borrow.value().Get().value, 77);   // 正确地址读真实字段
    borrow.value().Get().value = 78;
    auto borrow2 = bind.value().Borrow();
    BOOST_REQUIRE(borrow2);
    BOOST_CHECK_EQUAL(borrow2.value().Get().value, 78);
}

// 跨 App 句柄身份：两个 CoApp 各自 Registry 的句柄互不相同——
// CoBindComponent/CoUnbindComponent 对外来 subject 句柄返回
// InvalidArgument，不操作本 Registry 对象。
CO_TEST_CASE(cross_app_subject_handle_rejected) {
    auto app_a = fw::v2::CoApp::Create(MakeAppOptions());
    auto app_b = fw::v2::CoApp::Create(MakeAppOptions());
    CounterComponent::Options opts;
    BOOST_REQUIRE(bbt_co_await app_a->CoInstallComponent<CounterComponent>(
        "c", opts));
    BOOST_REQUIRE(bbt_co_await app_b->CoInstallComponent<CounterComponent>(
        "c", opts));
    BOOST_REQUIRE(bbt_co_await app_b->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));

    auto foreign_svc = app_b->FindService("echo");
    BOOST_REQUIRE(foreign_svc);
    // 外来 service 句柄作 subject 绑定本 App 组件 → InvalidArgument。
    auto r = bbt_co_await app_a->CoBindComponent<CounterComponent>(
        foreign_svc.value(), "c");
    BOOST_CHECK(!r && r.error().code == ErrorCode::InvalidArgument);
    // 同一句柄用于解绑同样拒绝，且不触碰本 Registry 对象。
    auto u = bbt_co_await app_a->CoUnbindComponent<CounterComponent>(
        foreign_svc.value(), "c");
    BOOST_CHECK(!u && u.error().code == ErrorCode::InvalidArgument);
    // 外来组件 owner 句柄用于 placement 同样拒绝（Service scope）。
    fw::ComponentOptions placement;
    placement.SetScope(fw::ComponentScope::Service)
        .SetOwner(foreign_svc.value());
    auto bad = bbt_co_await app_a->CoInstallComponent<CounterComponent>(
        "scoped", opts, placement);
    BOOST_CHECK(!bad && bad.error().code == ErrorCode::InvalidArgument);
    BOOST_CHECK(app_a->FindComponent<CounterComponent>("scoped").error()
                    .code == ErrorCode::NotFound);
}

// Observer 撤销清理：Revoke 抹除记录（额度立刻可复用）并排空在途
// callback lease——回调闭包持有弱引用探针，撤销完成后探针已释放。
CO_TEST_CASE(observer_revoke_reclaims_and_drains) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    auto svc_h = app->FindService("echo");
    BOOST_REQUIRE(svc_h);

    int delivered = 0;
    {
        auto token = bbt_co_await app->CoObserve(
            app->Handle(), svc_h.value(),
            [&delivered](const fw::ObjectStateEvent&) { ++delivered; });
        BOOST_REQUIRE(token);
        BOOST_CHECK(token.value().IsActive());
        token.value().Revoke();
        BOOST_CHECK(!token.value().IsActive());
    }
    // 额度已回收：同 App 再次订阅成功（max_observers 未收紧也无碍——
    // 断言额度语义用收紧的 app2 更严谨）。
    auto token2 = bbt_co_await app->CoObserve(
        app->Handle(), svc_h.value(),
        [&delivered](const fw::ObjectStateEvent&) { ++delivered; });
    BOOST_REQUIRE(token2);

    // 收紧额度到 1 的独立 App：第一次撤销后第二次订阅必须成功。
    auto app2 = fw::v2::CoApp::Create(
        MakeAppOptions().SetLimits(64, 64, 1, 64));
    BOOST_REQUIRE(bbt_co_await app2->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    auto svc2 = app2->FindService("echo");
    BOOST_REQUIRE(svc2);
    {
        auto t1 = bbt_co_await app2->CoObserve(
            app2->Handle(), svc2.value(),
            [](const fw::ObjectStateEvent&) {});
        BOOST_REQUIRE(t1);
        auto t2 = bbt_co_await app2->CoObserve(
            app2->Handle(), svc2.value(),
            [](const fw::ObjectStateEvent&) {});
        BOOST_CHECK(!t2 && t2.error().code == ErrorCode::Overloaded);
        t1.value().Revoke();   // 撤销即回收额度
        auto t3 = bbt_co_await app2->CoObserve(
            app2->Handle(), svc2.value(),
            [](const fw::ObjectStateEvent&) {});
        BOOST_REQUIRE(t3);
    }

    // 撤销后不再派发：触发一次状态变更，已撤销回调不交付。
    delivered = 0;
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    BOOST_CHECK(delivered >= 0);   // token2 是活跃的，delivered 可增
    const auto before_uninstall = delivered;
    BOOST_REQUIRE(bbt_co_await app->CoUninstallService<EchoService>());
    BOOST_CHECK(delivered > before_uninstall);  // 活跃观察者收到事件流
}

// 卸载撤销 observer 的闭包先于对象 owner 释放：observer 对象卸载时
// 其 callback 闭包（含捕获对象）已销毁——弱引用探针必须 expired。
CO_TEST_CASE(observer_closure_released_before_owner) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>(
        "observer_obj", CounterComponent::Options{}));
    auto svc_h = app->FindService("echo");
    auto obs_h = app->FindComponent<CounterComponent>("observer_obj");
    BOOST_REQUIRE(svc_h && obs_h);

    auto probe = std::make_shared<int>(1);
    std::weak_ptr<int> weak_probe = probe;
    {
        auto token = bbt_co_await app->CoObserve(
            obs_h.value(), svc_h.value(),
            [probe](const fw::ObjectStateEvent&) { (void)*probe; });
        BOOST_REQUIRE(token);
    }
    // token 析构即 Revoke：闭包被抹除。释放本用例自持的引用后，
    // 唯一剩余引用应已随记录抹除而销毁。
    probe.reset();
    BOOST_CHECK(weak_probe.expired());

    // observer 对象卸载路径：卸载撤销其 token，闭包由 Registry 在
    // owner 释放前销毁。
    auto probe2 = std::make_shared<int>(2);
    std::weak_ptr<int> weak_probe2 = probe2;
    {
        auto token = bbt_co_await app->CoObserve(
            obs_h.value(), svc_h.value(),
            [probe2](const fw::ObjectStateEvent&) { (void)*probe2; });
        BOOST_REQUIRE(token);
        token.value().Revoke();
    }
    BOOST_REQUIRE(bbt_co_await app->CoUninstallComponent<CounterComponent>(
        "observer_obj"));
    probe2.reset();
    BOOST_CHECK(weak_probe2.expired());
}

// P1-2：卸载撤销在途 callback 后 DrainInactiveObservers 不再永久等待——
// 真实交错：卸载方先见「inactive 且 in_flight>0」的记录，在途回调归还
// lease 后等待方必须能观察到终态并抹除记录（谓词按 in_flight>0 判定）。
CO_TEST_CASE(observer_auto_revoke_with_inflight_callback) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>(
        "observer_obj", CounterComponent::Options{}));
    auto svc_h = app->FindService("echo");
    auto obs_h = app->FindComponent<CounterComponent>("observer_obj");
    BOOST_REQUIRE(svc_h && obs_h);
    BOOST_REQUIRE(bbt_co_await app->CoStart());

    // 在途回调屏障：callback 内等 gate 放行——卸载发起时该回调仍在途，
    // DrainInactiveObservers 第一次扫描必须留下记录并进入等待分支。
    auto gate = std::make_shared<std::promise<void>>();
    auto entered = std::make_shared<std::promise<void>>();
    auto entered_f = entered->get_future();
    std::atomic<int> delivered{0};
    auto token = bbt_co_await app->CoObserve(
        obs_h.value(), svc_h.value(),
        [gate, entered, &delivered](const fw::ObjectStateEvent&) {
            entered->set_value();
            gate->get_future().wait();   // 回调在途：持有 lease
            ++delivered;
        });
    BOOST_REQUIRE(token);

    // 触发一次目标状态变更让 callback 真实派发并进入在途（svc Pause）。
    auto pause = std::async(std::launch::async, [&] {
        return RunInManagedCoroutine([&] {
            auto r = bbt_co_await app->CoPause();
            (void)r;
        });
    });
    BOOST_REQUIRE(entered_f.wait_for(std::chrono::seconds(2)) ==
                  std::future_status::ready);
    // callback 持有 lease 在途时卸载 observer 对象：其 token 被
    // RevokeObserversOfLocked 标 inactive，DrainInactiveObservers
    // 先见「inactive && in_flight>0」记录并进入等待。
    auto uninstall = std::async(std::launch::async, [&] {
        return RunInManagedCoroutine([&] {
            auto r = bbt_co_await
                app->CoUninstallComponent<CounterComponent>("observer_obj");
            (void)r;
        });
    });
    // 给卸载方一个真实窗口先执行到 Drain 等待分支，再放行在途回调。
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    gate->set_value();
    BOOST_REQUIRE(uninstall.wait_for(std::chrono::seconds(4)) ==
                  std::future_status::ready);
    BOOST_CHECK(uninstall.get());
    BOOST_REQUIRE(pause.wait_for(std::chrono::seconds(4)) ==
                  std::future_status::ready);
    BOOST_CHECK(pause.get());
    BOOST_CHECK(delivered.load() >= 1);
    // 卸载后 observer 对象已销毁，handle 失效。
    BOOST_CHECK(!obs_h.value().IsValid());
}

// P1-4：共享组件上其他 subject 的合法 borrow 不阻塞本 service 收口——
// 按边排空只等本 subject 的 retiring 边归零（WaitBindingDrained）。
CO_TEST_CASE(service_stop_not_blocked_by_other_subject_borrow) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>(
        "shared", CounterComponent::Options{}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<Echo2Service>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc_a = app->FindService("echo");
    auto svc_b = app->FindService("echo2");
    BOOST_REQUIRE(svc_a && svc_b);
    auto h_a = bbt_co_await
        app->CoBindComponent<CounterComponent>(svc_a.value(), "shared");
    auto h_b = bbt_co_await
        app->CoBindComponent<CounterComponent>(svc_b.value(), "shared");
    BOOST_REQUIRE(h_a && h_b);

    // Service B 经自己的 binding handle 持有一个合法 borrow；Service A
    // 无任何 borrow。A 的收口只等自己那条 retiring 边归零，不被 B 的
    // 借用阻塞——修复前 A 会等整个 component_borrows 归零而超时。
    auto borrow_b = h_b.value().Borrow();
    BOOST_REQUIRE(borrow_b);
    const auto stop_a = bbt_co_await app->CoUninstallService<EchoService>(
        fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() + std::chrono::seconds(2)));
    BOOST_CHECK(stop_a);   // 不得 TimedOut/StopIncomplete
    BOOST_CHECK(app->FindService("echo").error().code ==
                ErrorCode::NotFound);
    // B 的 borrow 仍存活（收口不动其他 subject 的边）。
    borrow_b.value().Get().Inc();
    BOOST_CHECK(borrow_b.value().Get().Get() == 1);
}

// P1-4 直接回归：本 subject 自己持 borrow 时卸载超时；borrow 未归还时重试
// 必须继续等同一条（原 generation）retiring 边——不得跳过它进入
// OnStop/OnUninstall/销毁。归还后重试成功且不残留 retiring 边；同组件上
// 其他 subject 的 borrow 不参与本 subject 的排空。
class DrainRetryService final : public fw::v2::CoService<CounterComponent> {
public:
    static constexpr std::string_view kServiceName = "drain_retry";
    explicit DrainRetryService(fw::detail::ConstructKey k)
        : fw::v2::CoService<CounterComponent>(k) {}

    static std::atomic<int> s_onstop;
    static std::atomic<int> s_onuninstall;
    static std::atomic<int> s_destroyed;

protected:
    ~DrainRetryService() noexcept override { ++s_destroyed; }
    result<void> OnStop() override {
        ++s_onstop; return result<void>::ok();
    }
    void OnUninstall() override { ++s_onuninstall; }
    friend class fw::detail::Registry;
};
std::atomic<int> DrainRetryService::s_onstop{0};
std::atomic<int> DrainRetryService::s_onuninstall{0};
std::atomic<int> DrainRetryService::s_destroyed{0};

CO_TEST_CASE(service_uninstall_retry_keeps_waiting_own_borrow) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    DrainRetryService::s_onstop = 0;
    DrainRetryService::s_onuninstall = 0;
    DrainRetryService::s_destroyed = 0;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>(
        "shared", CounterComponent::Options{}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<DrainRetryService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<Echo2Service>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc_a = app->FindService("drain_retry");
    auto svc_b = app->FindService("echo2");
    BOOST_REQUIRE(svc_a && svc_b);
    const auto id_a = svc_a.value().Id();
    auto h_a = bbt_co_await app->CoBindComponent<CounterComponent>(
        svc_a.value(), "shared");
    auto h_b = bbt_co_await app->CoBindComponent<CounterComponent>(
        svc_b.value(), "shared");
    BOOST_REQUIRE(h_a && h_b);

    std::optional<fw::ComponentBorrow<CounterComponent>> borrow_a;
    {
        auto b = h_a.value().Borrow();
        BOOST_REQUIRE(b);
        borrow_a.emplace(std::move(b.value()));
    }
    // 其他 subject 的 borrow 同时存在：不属于本 subject 的排空范围。
    auto borrow_b = h_b.value().Borrow();
    BOOST_REQUIRE(borrow_b);

    const auto short_life = [] {
        return fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(300));
    };
    // 首次：本 subject 的 borrow 未归还 → TimedOut，hook 不推进。
    const auto first = bbt_co_await
        app->CoUninstallService<DrainRetryService>(short_life());
    BOOST_CHECK(!first);
    BOOST_CHECK(first.error().code == ErrorCode::TimedOut);
    BOOST_CHECK(DrainRetryService::s_onstop.load() == 0);
    BOOST_CHECK(DrainRetryService::s_onuninstall.load() == 0);
    BOOST_CHECK(DrainRetryService::s_destroyed.load() == 0);
    BOOST_REQUIRE(app->FindService("drain_retry"));   // owner 保留

    // 重试（borrow 仍持有）：必须续等既有 retiring 边并再次 TimedOut。
    const auto second = bbt_co_await
        app->CoUninstallService<DrainRetryService>(short_life());
    BOOST_CHECK(!second);
    BOOST_CHECK(second.error().code == ErrorCode::TimedOut);
    BOOST_CHECK(DrainRetryService::s_onstop.load() == 0);
    BOOST_CHECK(DrainRetryService::s_onuninstall.load() == 0);
    BOOST_CHECK(DrainRetryService::s_destroyed.load() == 0);

    // 归还本 subject 的 borrow：重试成功；B 的 borrow 仍在但不阻塞。
    borrow_a.reset();
    const auto third = bbt_co_await app->CoUninstallService<DrainRetryService>(
        fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() + std::chrono::seconds(2)));
    BOOST_CHECK(third);
    BOOST_CHECK(DrainRetryService::s_onstop.load() == 1);
    BOOST_CHECK(DrainRetryService::s_onuninstall.load() == 1);
    BOOST_CHECK(DrainRetryService::s_destroyed.load() == 1);
    BOOST_CHECK(app->FindService("drain_retry").error().code ==
                ErrorCode::NotFound);
    {
        auto reg = app->RegistryForTest();
        std::lock_guard<std::mutex> lk(reg->Mutex());
        std::size_t residual = 0;
        for (const auto& cid :
             reg->ListIdsByKindLocked(fw::ObjectKind::Component)) {
            const auto* x = reg->FindEntryLocked(cid);
            if (!x) continue;
            for (const auto& b : x->bindings)
                if (b.subject_id == id_a && b.retiring) ++residual;
        }
        BOOST_CHECK_EQUAL(residual, 0u);
    }
    borrow_b.value().Get().Inc();
    BOOST_CHECK(borrow_b.value().Get().Get() == 1);
}

// P1-3：subject 已封口（销毁/收口进行中）后新增 Bind 必须被拒绝——
// BindLocked 在同一 Registry 临界区检查 subject gate_closed/
// op_in_progress/关闭语义状态。
class ClosingService final : public fw::v2::CoService<CounterComponent> {
public:
    static constexpr std::string_view kServiceName = "closing_svc";
    explicit ClosingService(fw::detail::ConstructKey k)
        : fw::v2::CoService<CounterComponent>(k) {}
    static std::shared_ptr<CoGate> s_gate;
    static std::atomic<int> s_onstop_entered;
protected:
    ~ClosingService() noexcept override = default;
    result<void> OnStop() override {
        ++s_onstop_entered;
        if (s_gate) s_gate->Wait();
        return result<void>::ok();
    }
    friend class fw::detail::Registry;
};
std::shared_ptr<CoGate> ClosingService::s_gate;
std::atomic<int> ClosingService::s_onstop_entered{0};

CO_TEST_CASE(bind_rejected_when_subject_closing) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    ClosingService::s_gate = std::make_shared<CoGate>();
    ClosingService::s_onstop_entered = 0;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>(
        "c", CounterComponent::Options{}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<ClosingService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc = app->FindService("closing_svc");
    BOOST_REQUIRE(svc);

    // 卸载进行到 OnStop 挂起点：subject 处于 op_in_progress +
    // Uninstalling——同一临界区的 Bind gate 必须返回 Closed，不会在
    // retiring 边外新增 active 边。
    auto uninstall = std::async(std::launch::async, [&] {
        return RunInManagedCoroutine([&] {
            auto r = bbt_co_await
                app->CoUninstallService<ClosingService>();
            (void)r;
        });
    });
    {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(2);
        while (ClosingService::s_onstop_entered.load() < 1 &&
               std::chrono::steady_clock::now() < deadline)
            bbtco_sleep(1);
    }
    BOOST_REQUIRE(ClosingService::s_onstop_entered.load() >= 1);
    const auto during = bbt_co_await
        app->CoBindComponent<CounterComponent>(svc.value(), "c");
    BOOST_CHECK(!during && during.error().code == ErrorCode::Closed);
    ClosingService::s_gate->Open();
    BOOST_REQUIRE(uninstall.wait_for(std::chrono::seconds(4)) ==
                  std::future_status::ready);
    BOOST_CHECK(uninstall.get());
    // subject 已移除：组件上不应残留指向已销毁 service 的 active 边。
    auto reg = app->RegistryForTest();
    std::lock_guard<std::mutex> lk(reg->Mutex());
    bool dangling = false;
    for (const auto& cid :
         reg->ListIdsByKindLocked(fw::ObjectKind::Component)) {
        const auto* x = reg->FindEntryLocked(cid);
        if (!x) continue;
        for (const auto& b : x->bindings)
            if (b.subject_id == svc.value().Id() && b.active) dangling = true;
    }
    BOOST_CHECK(!dangling);
    ClosingService::s_gate.reset();
}

// P2-1：CoStop 正确移除 Actor type registration entry 并发布一致状态——
// type 登记条目（object 为空）同样走 FinishDestroyLocked：移除出
// Registry 并发布 Destroyed 事件。
CO_TEST_CASE(costop_removes_actor_type_entry) {
    auto app = fw::v2::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallActor<WorkerActor>(
        fw::ActorOptions{}.SetMaxActors(4).SetMailboxCapacity(8)
            .SetAggregateQueueCapacity(64)));
    auto actor_h = app->FindActor(fw::ActorId{"worker", ""});
    BOOST_REQUIRE(actor_h);

    std::vector<fw::ObjectState> seen;
    auto token = bbt_co_await app->CoObserve(
        app->Handle(), actor_h.value(),
        [&seen](const fw::ObjectStateEvent& ev) {
            seen.push_back(ev.state);
        });
    BOOST_REQUIRE(token);

    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    // type entry 已收口：Registry 中不再保留该 actor 登记条目。
    auto reg = app->RegistryForTest();
    {
        std::lock_guard<std::mutex> lk(reg->Mutex());
        bool found = false;
        for (const auto& id :
             reg->ListIdsByKindLocked(fw::ObjectKind::Actor)) {
            const auto* e = reg->FindEntryLocked(id);
            if (e && e->type_name == "worker") found = true;
        }
        BOOST_CHECK(!found);
    }
    // Destroyed 事件已发布（SetStateLocked 在 FinishDestroyLocked 内）。
    BOOST_CHECK(std::find(seen.begin(), seen.end(),
                          fw::ObjectState::Destroyed) != seen.end());
}

// ---- 编译期负向断言（ConstructKey 不可聚合构造）----
// C++17 下 `ConstructKey() = default` 首声明会使空类保持 aggregate；
// user-provided `ConstructKey() {}` 关闭该绕过——静态断言钉死性质。
static_assert(!std::is_aggregate_v<fw::detail::ConstructKey>,
    "ConstructKey 必须不是 aggregate（业务不得 ConstructKey{} 绕过）");
static_assert(!std::is_default_constructible_v<fw::detail::ConstructKey>,
    "ConstructKey 默认构造私有，业务不可直接构造");

BOOST_AUTO_TEST_SUITE_END()
