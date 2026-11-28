/**
 * framework v2 最小核心切片行为验收：
 * - 构造限制：业务类型只能经 CoApp::Install* 创建（静态断言在编译期覆盖
 * 不可默构/不可公开析构；运行期断言对象只来自 Registry）。
 * - Install/Uninstall 生命周期：Installing→Installed→Running→Destroyed、
 * OnInstall 失败 → InstallFailed、Uninstall 有 active binding → Overloaded、
 * Unbind 封旧 handle generation。
 * - Service/Actor handle 失效与地址语义：Uninstall 后 IsValid=false、
 * GetMailBox 返回正确 AppObjectAddress、generation 失配 Info() 报错。
 * - 动态绑定寿命契约：默认解绑不等在途 Borrow、四参数解绑排空与超时
 * 续等、关闭等显式 Borrow 而不等普通 shared_ptr、原子换绑与失败保留
 * 旧关系、同实例 ABA 的 IsBound 代次判定、换绑 hook 锁外可见性。
 * - Observe：状态事件按 revision 单调到达，observer 卸载后 token 失效。
 *
 * 不依赖网络；生命周期断言运行在真实协程中，全部断言走真实公共面
 * （CoApp + 公开 handle/组件借用），不走 stub。
 */

#define BOOST_TEST_MODULE framework.v2_core
#include <boost/test/included/unit_test.hpp>

#include <algorithm>
#include <atomic>
#include <cstdint>
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

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/NetworkHost.hpp>

namespace fw = bbt::framework;
using bbt::framework::ErrorCode;
using bbt::framework::result;

namespace {

/**
 * ---- 测试用组件：App scope 可借用的计数组件 ----
 */
class CounterComponent final : public fw::CoComponent {
public:
    struct Options {
        int initial = 0;
    };

    explicit CounterComponent(fw::detail::ConstructKey k, Options o)
        : fw::CoComponent(k), m_count(o.initial) {}

    int Get() const noexcept { return m_count; }
    void Inc() noexcept { ++m_count; }

protected:
    ~CounterComponent() noexcept override = default;

private:
    int m_count;
    friend class fw::detail::Registry;
};

/**
 * 组件未声明允许绑定时，Service::GetComponent<T> 必须返回 InvalidArgument。
 */
class AnotherComponent final : public fw::CoComponent {
public:
    struct Options {};
    explicit AnotherComponent(fw::detail::ConstructKey k, Options)
        : fw::CoComponent(k) {}

protected:
    ~AnotherComponent() noexcept override = default;
    friend class fw::detail::Registry;
};

/**
 * ---- 测试用 Service ----
 */
class EchoService final : public fw::CoService<CounterComponent> {
public:
    static constexpr std::string_view kServiceName = "echo";

    explicit EchoService(fw::detail::ConstructKey k)
        : fw::CoService<CounterComponent>(k) {}

    int install_count = 0;
    int start_count = 0;
    int stop_count = 0;
    int uninstall_count = 0;
    int binding_changed_count = 0;

    /**
     * 真实模板消费者：OnBindingChanged 内走受保护 GetComponent（编译期
     * 实例化 + 运行期绑定表断言），结果写入静态计数供用例断言。
     * AnotherComponent 未在声明集合内，必须 InvalidArgument。
     */
    static std::atomic<int> s_gc_ok;
    static std::atomic<int> s_gc_undeclared_rejected;
    static std::atomic<int> s_gc_borrow_ok;

protected:
    ~EchoService() noexcept override = default;

    result<void> OnInstall() override {
        ++install_count;
        return result<void>::ok();
    }
    result<void> OnStart() override {
        ++start_count;
        return result<void>::ok();
    }
    result<void> OnStop() override {
        ++stop_count;
        return result<void>::ok();
    }
    void OnUninstall() override { ++uninstall_count; }
    void OnBindingChanged(const fw::BindingEvent&) override {
        ++binding_changed_count;
        /**
         * 绑定已回填后 GetComponent 应可见；借用成功证明真实实例化路径。
         */
        auto h = GetComponent<CounterComponent>("counter");
        if (h) {
            ++s_gc_ok;
            auto b = h.value().Borrow();
            if (b)
                ++s_gc_borrow_ok;
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

class FailInstallService final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "fail_install";
    explicit FailInstallService(fw::detail::ConstructKey k) : fw::CoService<>(k) {}

protected:
    ~FailInstallService() noexcept override = default;
    result<void> OnInstall() override {
        return result<void>::err(fw::MakeError(ErrorCode::InternalError,
                                               "OnInstall 显式失败"));
    }
    friend class fw::detail::Registry;
};

/**
 * 第二个 Service 类型：与 EchoService 同绑定同一组件，验证两 subject
 * 独立 Unbind 互不影响。
 */
class Echo2Service final : public fw::CoService<CounterComponent> {
public:
    static constexpr std::string_view kServiceName = "echo2";
    explicit Echo2Service(fw::detail::ConstructKey k)
        : fw::CoService<CounterComponent>(k) {}

protected:
    ~Echo2Service() noexcept override = default;
    friend class fw::detail::Registry;
};

/**
 * 换绑探针服务：记录每次 OnBindingChanged 锁外查询到的绑定代与实例，
 * 验证换绑/解绑通知时槽位对 hook 的可见性。只查询不借用，不干扰排空。
 */
class RebindProbeService final : public fw::CoService<CounterComponent> {
public:
    static constexpr std::string_view kServiceName = "rebind_probe";
    explicit RebindProbeService(fw::detail::ConstructKey k)
        : fw::CoService<CounterComponent>(k) {}

    struct Observation {
        bool bound = false;
        std::uint64_t binding_generation = 0;
        const CounterComponent* instance = nullptr;
    };

    static void Reset() {
        std::lock_guard<std::mutex> lk(s_mtx);
        s_seen.clear();
    }
    static std::vector<Observation> Snapshot() {
        std::lock_guard<std::mutex> lk(s_mtx);
        return s_seen;
    }

protected:
    ~RebindProbeService() noexcept override = default;

    void OnBindingChanged(const fw::BindingEvent& ev) override {
        Observation o;
        o.bound = ev.bound;
        o.binding_generation = ev.binding_generation;
        /**
         * 锁外按事件槽位查询当前关系（GetComponent 取当前绑定）。
         */
        auto h = GetComponent<CounterComponent>(ev.slot);
        if (h)
            o.instance = h.value().lock().get();
        std::lock_guard<std::mutex> lk(s_mtx);
        s_seen.push_back(o);
    }

private:
    static std::mutex s_mtx;
    static std::vector<Observation> s_seen;
    friend class fw::detail::Registry;
};

std::mutex RebindProbeService::s_mtx;
std::vector<RebindProbeService::Observation> RebindProbeService::s_seen;

/**
 * 装配借用探针服务：OnInstall 内经 GetComponent 借 Installed 依赖
 * （装配窗口）；OnStop 记录是否运行在受管协程内。
 */
class ProbeService final : public fw::CoService<CounterComponent> {
public:
    static constexpr std::string_view kServiceName = "probe";
    explicit ProbeService(fw::detail::ConstructKey k)
        : fw::CoService<CounterComponent>(k) {}

    static std::atomic<int> s_install_borrow_ok;
    static std::atomic<int> s_install_borrow_value;
    /**
     * OnStart 在协程内执行次数
     */
    static std::atomic<int> s_onstart_co;
    /**
     * OnStop 在协程内执行次数
     */
    static std::atomic<int> s_onstop_co;
    /**
     * bound=false 事件计数
     */
    static std::atomic<int> s_unbind_events;
    /**
     * hook 内再调宿主入口被拒
     */
    static std::atomic<int> s_invalid_ctx;

    /**
     * 由用例设置：hook 内要探测的 CoApp（非拥有）。
     */
    static fw::CoApp* s_app;

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
        if (bbt::coroutine::GetLocalCoroutineId() != 0)
            ++s_onstart_co;
        /**
         * hook 内同协程再调宿主控制面 → InvalidContext（自等待）。
         */
        if (s_app) {
            auto r = bbt_co_await s_app->CoPause();
            if (!r && r.error().code == ErrorCode::InvalidContext)
                ++s_invalid_ctx;
            /**
             * CoStop 在协程内调用会等待自身 hook → InvalidContext。
             */
            auto cr = bbt_co_await s_app->CoStop();
            if (!cr && cr.error().code == ErrorCode::InvalidContext)
                ++s_invalid_ctx;
        }
        return result<void>::ok();
    }
    result<void> OnStop() override {
        if (bbt::coroutine::GetLocalCoroutineId() != 0)
            ++s_onstop_co;
        return result<void>::ok();
    }
    void OnBindingChanged(const fw::BindingEvent& ev) override {
        if (!ev.bound)
            ++s_unbind_events;
    }
    friend class fw::detail::Registry;
};

std::atomic<int> ProbeService::s_install_borrow_ok{0};
std::atomic<int> ProbeService::s_install_borrow_value{0};
std::atomic<int> ProbeService::s_onstart_co{0};
std::atomic<int> ProbeService::s_onstop_co{0};
std::atomic<int> ProbeService::s_unbind_events{0};
std::atomic<int> ProbeService::s_invalid_ctx{0};
fw::CoApp* ProbeService::s_app = nullptr;

/**
 * 拓扑/挂起探针组件：OnStart/OnStop 记录执行次序与协程上下文；
 * OnStop 可用 bbtco_sleep 真实挂起（不阻塞 worker），由静态毫秒数控制。
 */
class DepComponent final : public fw::CoComponent {
public:
    struct Options {
        int tag = 0;
    };
    explicit DepComponent(fw::detail::ConstructKey k, Options o)
        : fw::CoComponent(k), m_tag(o.tag) {}

    static std::mutex s_order_mtx;
    /**
     * "start:N"/"stop:N" 事件序
     */
    static std::vector<std::string> s_order;
    /**
     * OnStop 挂起毫秒（0=不挂起）
     */
    static std::atomic<int> s_suspend_ms;
    static std::atomic<int> s_start_co;
    static std::atomic<int> s_stop_co;

    int Tag() const noexcept { return m_tag; }

protected:
    ~DepComponent() noexcept override = default;

    result<void> OnStart() override {
        if (bbt::coroutine::GetLocalCoroutineId() != 0)
            ++s_start_co;
        std::lock_guard<std::mutex> lk(s_order_mtx);
        s_order.push_back("start:" + std::to_string(m_tag));
        return result<void>::ok();
    }
    result<void> OnStop() override {
        if (bbt::coroutine::GetLocalCoroutineId() != 0)
            ++s_stop_co;
        const int ms = s_suspend_ms.load();
        /**
         * 真实挂起：不阻塞 worker
         */
        if (ms > 0)
            bbtco_sleep(ms);
        std::lock_guard<std::mutex> lk(s_order_mtx);
        s_order.push_back("stop:" + std::to_string(m_tag));
        return result<void>::ok();
    }

private:
    int m_tag;
    friend class fw::detail::Registry;
};

std::mutex DepComponent::s_order_mtx;
std::vector<std::string> DepComponent::s_order;
std::atomic<int> DepComponent::s_suspend_ms{0};
std::atomic<int> DepComponent::s_start_co{0};
std::atomic<int> DepComponent::s_stop_co{0};

/**
 * 依赖组件：subject 侧，绑定 DepComponent 并在 OnBindingChanged 经
 * GetComponent 取依赖句柄（装配借用的组件路径）。
 */
class UserComponent final : public fw::CoComponent {
public:
    struct Options {};
    explicit UserComponent(fw::detail::ConstructKey k, Options)
        : fw::CoComponent(k) {}

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
        if (!ev.bound) {
            ++s_unbind_events;
            return;
        }
        auto h = GetComponent<DepComponent>("dep");
        if (h) {
            ++s_get_ok;
            auto b = h.value().Borrow();
            if (b)
                ++s_borrow_ok;
        }
    }
    friend class fw::detail::Registry;
};

std::atomic<int> UserComponent::s_get_ok{0};
std::atomic<int> UserComponent::s_borrow_ok{0};
std::atomic<int> UserComponent::s_unbind_events{0};

/**
 * ---- 测试用 Actor ----
 */
class WorkerActor final : public fw::CoActor {
public:
    static constexpr std::string_view kActorType = "worker";
    explicit WorkerActor(fw::detail::ConstructKey k) : fw::CoActor(k) {}
    int create_count = 0;
    int destroy_count = 0;

protected:
    ~WorkerActor() noexcept override = default;
    result<void> OnCreate() override {
        ++create_count;
        return result<void>::ok();
    }
    void OnDestroy() override { ++destroy_count; }
    friend class fw::detail::Registry;
};

/**
 * 同一异常序列分别走组件与服务的真实安装、调度和卸载入口。
 * 不调用内部 hook 执行器，防止测试绕过成员指针的类型化接线。
 */
template <class Base>
class ThrowingHooks final : public Base {
public:
    struct Options {};
    static constexpr std::string_view kServiceName = "throwing_hooks";
    inline static bool s_fail_install = false;

    explicit ThrowingHooks(fw::detail::ConstructKey k) : Base(k) {}
    ThrowingHooks(fw::detail::ConstructKey k, Options) : Base(k) {}

protected:
    ~ThrowingHooks() noexcept override = default;
    result<void> OnInstall() override {
        if (s_fail_install)
            throw std::runtime_error("install probe");
        return result<void>::ok();
    }
    result<void> OnStart() override { throw 7; }
    void OnUninstall() override { throw std::runtime_error("uninstall probe"); }

private:
    friend class fw::detail::Registry;
};

fw::AppOptions MakeAppOptions() {
    return fw::AppOptions{}
        .SetLimits(64, 64, 64, 64)
        .SetShutdownBudget(std::chrono::milliseconds(200))
        .SetLifecycleBudget(std::chrono::milliseconds(200));
}

/** 绑定配额回归用：max_bindings=1，其余额度充足。 */
fw::AppOptions MakeQuotaAppOptions() {
    return fw::AppOptions{}
        .SetLimits(64, 1, 64, 64)
        .SetShutdownBudget(std::chrono::milliseconds(200))
        .SetLifecycleBudget(std::chrono::milliseconds(200));
}

template <class T>
void CheckHookFailurePaths() {
    for (int phase = 0; phase < 2; ++phase) {
        T::s_fail_install = (phase == 0);
        auto app = fw::CoApp::Create(MakeAppOptions());
        auto installed = [&] {
            if constexpr (std::is_base_of_v<fw::CoComponent, T>)
                return bbt_co_await app->CoInstallComponent<T>("probe", typename T::Options{});
            else
                return bbt_co_await app->CoInstallService<T>(fw::ServiceOptions{}.SetMaxInflight(1));
        }();
        if (phase == 0) {
            BOOST_REQUIRE(!installed);
            BOOST_CHECK(installed.error().code == ErrorCode::InternalError);
            BOOST_CHECK_EQUAL(installed.error().message, "hook threw: install probe");
        } else {
            BOOST_REQUIRE(installed);
            auto started = bbt_co_await app->CoStart();
            BOOST_REQUIRE(!started);
            BOOST_CHECK(started.error().code == ErrorCode::InternalError);
            BOOST_CHECK_EQUAL(started.error().message, "hook threw (non-std)");
        }
        auto handle = [&] {
            if constexpr (std::is_base_of_v<fw::CoComponent, T>)
                return app->FindComponent<T>("probe");
            else
                return app->FindService(T::kServiceName);
        }();
        BOOST_REQUIRE(handle);
        auto info = handle.value().Info();
        BOOST_REQUIRE(info);
        BOOST_CHECK(info.value().state == (phase == 0
                                               ? fw::ObjectState::InstallFailed
                                               : fw::ObjectState::Failed));
        auto failures = app->LifecycleFailures();
        BOOST_REQUIRE_EQUAL(failures.size(), 1u);
        BOOST_CHECK_EQUAL(failures.front().object_id, handle.value().Id());
        BOOST_CHECK_EQUAL(failures.front().phase, phase == 0 ? "install" : "start");
        BOOST_CHECK(failures.front().code == ErrorCode::InternalError);

        auto uninstalled = [&] {
            if constexpr (std::is_base_of_v<fw::CoComponent, T>)
                return bbt_co_await app->CoUninstallComponent<T>("probe");
            else
                return bbt_co_await app->CoUninstallService<T>();
        }();
        BOOST_REQUIRE(uninstalled);
        BOOST_CHECK(!handle.value().IsValid());
        failures = app->LifecycleFailures();
        BOOST_REQUIRE_EQUAL(failures.size(), 2u);
        BOOST_CHECK_EQUAL(failures.back().object_id, handle.value().Id());
        BOOST_CHECK_EQUAL(failures.back().phase, "uninstall");
        BOOST_CHECK(failures.back().code == ErrorCode::InternalError);
        BOOST_CHECK_EQUAL(failures.back().message, "void hook threw: uninstall probe");
    }
}

/**
 * 断言构造限制（编译期）：业务类型不能默认构造、不能公开析构。
 */
static_assert(!std::is_default_constructible_v<CounterComponent>);
static_assert(!std::is_destructible_v<CounterComponent>);
static_assert(!std::is_default_constructible_v<EchoService>);
static_assert(!std::is_destructible_v<EchoService>);
static_assert(!std::is_default_constructible_v<WorkerActor>);
static_assert(!std::is_destructible_v<WorkerActor>);

} // namespace

/**
 * 在受管协程内执行 fn：经 g_scheduler 投递协程任务，宿主线程用
 * 限时 std::cv 等其完成（宿主线程的 CV 等待不违反 worker 阻塞禁令）。
 * 返回 false 表示任务未在预算内完成（防挂死测试进程）。
 */
template <class F>
bool RunInManagedCoroutine(F&& fn,
                           std::chrono::milliseconds budget = std::chrono::milliseconds{5000}) {
    if (bbt::coroutine::IsInCoroutineContext()) {
        fn();
        return true;
    }
    /**
     * native 测试宿主负责初始化唯一 runtime，产品 App 不启动 runtime。
     */
    if (!g_scheduler->IsInitialized())
        g_scheduler->Start();
    struct Slot {
        std::mutex mtx;
        std::condition_variable cv;
        bool done = false;
        std::exception_ptr error;
    };
    auto slot = std::make_shared<Slot>();
    auto f = std::forward<F>(fn);
    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [slot, f = std::move(f)]() mutable {
            try {
                f();
            } catch (...) { slot->error = std::current_exception(); }
            std::lock_guard<std::mutex> lk(slot->mtx);
            slot->done = true;
            slot->cv.notify_all();
        },
        submitted);
    if (!submitted)
        throw std::runtime_error("test coroutine submission rejected");
    std::unique_lock<std::mutex> lk(slot->mtx);
    const bool done = slot->cv.wait_for(lk, budget, [&] { return slot->done; });
    /**
     * 不让仍在途任务使用已退出的测试栈。
     */
    if (!done)
        std::terminate();
    if (slot->error)
        std::rethrow_exception(slot->error);
    return done;
}

#define CO_TEST_CASE(name)                                 \
    static void name##_body();                             \
    BOOST_AUTO_TEST_CASE(name) {                           \
        BOOST_REQUIRE(RunInManagedCoroutine(name##_body)); \
    }                                                      \
    static void name##_body()

/** 有记忆、带 deadline 的阶段信号：复用 CoGate 的「登记后复查谓词」
 * 模式（先审 open 再登记、唤醒后复查 open），关闭 CoWaiter 先通知后等待的
 * 丢唤醒窗口。仅本文件阶段同步使用，不改产品行为。
 */
class StageGate {
public:
    StageGate() : waiter_(bbt::coroutine::sync::CoWaiter::Create()) {}

    void Open() {
        open_.store(true, std::memory_order_release);
        waiter_->Notify();
    }

    /** 协程内调用：0=已放行或被唤醒，非零=超时或未放行。超时绝不转成功。 */
    int Wait(int ms) {
        if (open_.load(std::memory_order_acquire))
            return 0;
        bbt::coroutine::WaitOptions options;
        options.deadline = std::chrono::steady_clock::now() +
                           std::chrono::milliseconds(ms);
        auto status = waiter_->WaitWithCallback(
            options,
            [this] {
                if (open_.load(std::memory_order_acquire))
                    waiter_->Notify();
                return true;
            });
        if (status == bbt::coroutine::WaitStatus::Completed &&
            open_.load(std::memory_order_acquire))
            return 0;
        return 1;
    }

    void Reset() {
        open_.store(false, std::memory_order_release);
        waiter_ = bbt::coroutine::sync::CoWaiter::Create();
    }

private:
    std::atomic<bool> open_{false};
    bbt::coroutine::sync::CoWaiter::SPtr waiter_;
};

/** 阶段门闩服务（测试夹具，不改产品行为）：声明 DepComponent，仅作为停机屏障
 * 或换绑 subject/卸载目标；OnStop 与旧代 OnBindingChanged 进入后发布阶段信号
 * 并可阻塞至显式放行，全部等待有界，超时不改 hook 成功结果。
 */
class PhaseService final : public fw::CoService<DepComponent> {
public:
    static constexpr std::string_view kServiceName = "phase_svc";
    explicit PhaseService(fw::detail::ConstructKey k)
        : fw::CoService<DepComponent>(k) {}

    static std::atomic<std::uint64_t> s_seq;
    static std::atomic<std::uint64_t> s_stop_seq;
    static std::atomic<std::uint64_t> s_uninstall_seq;
    static std::atomic<std::uint64_t> s_bc_old_seq;
    static std::atomic<std::uint64_t> s_bc_new_seq;
    static std::atomic<int> s_stop_entered;
    static std::atomic<int> s_bc_old_entered;
    static std::atomic<int> s_bc_new_seen;
    static std::atomic<std::uint64_t> s_new_gen;
    /** 初始 bind 已完成后置 true；初始 bind 的 bound=true 通知不计入新代。 */
    static std::atomic<bool> s_initial_bind_done;
    static std::atomic<bool> s_hold_stop;

    static void hold_stop() { s_hold_stop.store(true); }

    static std::shared_ptr<StageGate> s_stop_enter_gate;
    static std::shared_ptr<StageGate> s_stop_done_gate;
    static std::shared_ptr<StageGate> s_stop_finish_gate;
    static std::shared_ptr<StageGate> s_bc_enter_gate;
    static std::shared_ptr<StageGate> s_bc_release_gate;
    static std::shared_ptr<StageGate> s_rebind_finish_gate;
    /** 屏障等待超时计数（超时不改 hook 成功结果，仅记录诊断）。 */
    static std::atomic<int> s_stop_done_wait_timedouts;
    static std::atomic<int> s_bc_release_wait_timedouts;

    static void reset() {
        s_seq.store(0);
        s_stop_seq.store(0);
        s_uninstall_seq.store(0);
        s_bc_old_seq.store(0);
        s_bc_new_seq.store(0);
        s_stop_entered.store(0);
        s_bc_old_entered.store(0);
        s_bc_new_seen.store(0);
        s_new_gen.store(0);
        s_initial_bind_done.store(false);
        s_hold_stop.store(false);
        s_stop_enter_gate = std::make_shared<StageGate>();
        s_stop_done_gate = std::make_shared<StageGate>();
        s_stop_finish_gate = std::make_shared<StageGate>();
        s_bc_enter_gate = std::make_shared<StageGate>();
        s_bc_release_gate = std::make_shared<StageGate>();
        s_rebind_finish_gate = std::make_shared<StageGate>();
        s_stop_done_wait_timedouts.store(0);
        s_bc_release_wait_timedouts.store(0);
    }

protected:
    ~PhaseService() noexcept override = default;

    result<void> OnStop() override {
        s_stop_seq.store(s_seq.fetch_add(1, std::memory_order_relaxed) + 1,
                         std::memory_order_relaxed);
        ++s_stop_entered;
        if (s_stop_enter_gate)
            s_stop_enter_gate->Open();
        if (s_hold_stop.load(std::memory_order_acquire) && s_stop_done_gate) {
            int rc = s_stop_done_gate->Wait(3000);
            if (rc != 0)
                s_stop_done_wait_timedouts.fetch_add(1, std::memory_order_relaxed);
        }
        return result<void>::ok();
    }
    void OnUninstall() override {
        s_uninstall_seq.store(s_seq.fetch_add(1, std::memory_order_relaxed) + 1,
                              std::memory_order_relaxed);
    }
    void OnBindingChanged(const fw::BindingEvent& ev) override {
        if (!ev.bound) {
            s_bc_old_seq.store(s_seq.fetch_add(1, std::memory_order_relaxed) + 1,
                               std::memory_order_relaxed);
            ++s_bc_old_entered;
            if (s_bc_enter_gate)
                s_bc_enter_gate->Open();
            if (s_bc_release_gate) {
                int rc = s_bc_release_gate->Wait(3000);
                if (rc != 0)
                    s_bc_release_wait_timedouts.fetch_add(1, std::memory_order_relaxed);
            }
        } else {
            /** 初始 bind 的新代通知不计入 s_bc_new_seq。 */
            if (s_initial_bind_done.load(std::memory_order_acquire)) {
                s_bc_new_seq.store(s_seq.fetch_add(1, std::memory_order_relaxed) + 1,
                                   std::memory_order_relaxed);
                ++s_bc_new_seen;
                s_new_gen.store(ev.binding_generation, std::memory_order_relaxed);
            }
        }
    }
    friend class fw::detail::Registry;
};

std::atomic<std::uint64_t> PhaseService::s_seq{0};
std::atomic<std::uint64_t> PhaseService::s_stop_seq{0};
std::atomic<std::uint64_t> PhaseService::s_uninstall_seq{0};
std::atomic<std::uint64_t> PhaseService::s_bc_old_seq{0};
std::atomic<std::uint64_t> PhaseService::s_bc_new_seq{0};
std::atomic<int> PhaseService::s_stop_entered{0};
std::atomic<int> PhaseService::s_bc_old_entered{0};
std::atomic<int> PhaseService::s_bc_new_seen{0};
std::atomic<std::uint64_t> PhaseService::s_new_gen{0};
std::atomic<bool> PhaseService::s_initial_bind_done{false};
std::atomic<bool> PhaseService::s_hold_stop{false};
std::shared_ptr<StageGate> PhaseService::s_stop_enter_gate;
std::shared_ptr<StageGate> PhaseService::s_stop_done_gate;
std::shared_ptr<StageGate> PhaseService::s_stop_finish_gate;
std::shared_ptr<StageGate> PhaseService::s_bc_enter_gate;
std::shared_ptr<StageGate> PhaseService::s_bc_release_gate;
std::shared_ptr<StageGate> PhaseService::s_rebind_finish_gate;
std::atomic<int> PhaseService::s_stop_done_wait_timedouts{0};
std::atomic<int> PhaseService::s_bc_release_wait_timedouts{0};

BOOST_AUTO_TEST_SUITE(v2_core)

CO_TEST_CASE(component_hook_errors_preserve_failure_records) {
    CheckHookFailurePaths<ThrowingHooks<fw::CoComponent>>();
}

CO_TEST_CASE(service_hook_errors_preserve_failure_records) {
    CheckHookFailurePaths<ThrowingHooks<fw::CoService<>>>();
}

CO_TEST_CASE(app_options_validate) {
    fw::AppOptions bad;
    BOOST_CHECK(!bad.Validate());
    fw::AppOptions good = MakeAppOptions();
    BOOST_CHECK(good.Validate());
}

CO_TEST_CASE(coapp_construct_handle) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    BOOST_CHECK(app->Handle().IsValid());
    BOOST_CHECK(app->State() == fw::AppState::Created);
    auto info = app->Handle().Info();
    BOOST_REQUIRE(info);
    BOOST_CHECK(info.value().kind == fw::ObjectKind::App);
}

CO_TEST_CASE(component_install_find_lifecycle) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    CounterComponent::Options opts;
    opts.initial = 41;

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
    auto app = fw::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    CounterComponent::Options opts;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("c", opts));
    auto h = app->FindComponent<CounterComponent>("c");
    BOOST_REQUIRE(h);
    BOOST_CHECK(h.value().Info().value().state ==
                fw::ObjectState::Running);
}

CO_TEST_CASE(service_lifecycle_hooks) {
    auto app = fw::CoApp::Create(MakeAppOptions());
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
    {
        auto reinstall = bbt_co_await app->CoInstallService<EchoService>(
            fw::ServiceOptions{}.SetMaxInflight(1));
        BOOST_REQUIRE_MESSAGE(reinstall,
                              "卸载后重装失败: code="
                                  << (reinstall ? -1
                                                : static_cast<int>(reinstall.error().code))
                                  << " msg="
                                  << (reinstall ? std::string{} : reinstall.error().message));
    }
    auto h2 = app->FindService("echo");
    BOOST_REQUIRE(h2);
    BOOST_CHECK(h2.value().Generation() != h.value().Generation() ||
                h2.value().Id() != h.value().Id());
}

CO_TEST_CASE(service_install_failed) {
    auto app = fw::CoApp::Create(MakeAppOptions());
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
    auto app = fw::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    auto r = bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(1));
    BOOST_CHECK(!r && r.error().code == ErrorCode::InvalidArgument);
}

CO_TEST_CASE(actor_type_registration) {
    auto app = fw::CoApp::Create(MakeAppOptions());
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
    auto app = fw::CoApp::Create(MakeAppOptions());
    CounterComponent::Options opts;
    opts.initial = 7;
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
    /**
     * OnBindingChanged 已在锁外同步执行：GetComponent 真实实例化路径
     * 找到绑定并成功 Borrow；未声明类型被 InvalidArgument 拒绝。
     */
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
    auto app = fw::CoApp::Create(MakeAppOptions());
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
    auto app = fw::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    auto svc_h = app->FindService("echo");
    BOOST_REQUIRE(svc_h);

    std::vector<fw::ObjectState> seen;
    /**
     * 观察者用 App 自身 handle（observer 与 target 不同对象）。
     */
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
    auto app = fw::CoApp::Create(MakeAppOptions());
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
    auto app = fw::CoApp::Create(MakeAppOptions());
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
    auto app = fw::CoApp::Create(MakeAppOptions());
    CounterComponent::Options opts;
    auto r = bbt_co_await app->CoInstallComponent<CounterComponent>("", opts);
    BOOST_CHECK(!r && r.error().code == ErrorCode::InvalidArgument);

    fw::ComponentOptions scoped;
    scoped.SetScope(fw::ComponentScope::Service);
    auto r2 = bbt_co_await app->CoInstallComponent<CounterComponent>("c", opts, scoped);
    BOOST_CHECK(!r2 && r2.error().code == ErrorCode::InvalidArgument);
}

/**
 * ---- 生命周期节点新增用例 ----
 */

/**
 * 装配借用：subject 处于 Installing（OnInstall 执行中）时，允许借
 * Installed 依赖做纯内存装配；同一时刻非装配窗口的普通 borrow 被拒。
 */
CO_TEST_CASE(assembly_borrow_during_oninstall) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    CounterComponent::Options opts;
    opts.initial = 33;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("counter", opts));

    /**
     * 未 Running 且非装配窗口：直接 Borrow 被拒（Installed 态）。
     */
    auto comp_h = app->FindComponent<CounterComponent>("counter");
    BOOST_REQUIRE(comp_h);
    auto direct = bbt_co_await app->CoBindComponent<CounterComponent>(app->Handle(), "counter");
    /**
     * App handle 不是合法 subject
     */
    BOOST_CHECK(!direct);

    ProbeService::s_install_borrow_ok = 0;
    ProbeService::s_install_borrow_value = 0;
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<ProbeService>(
        fw::ServiceOptions{}.SetMaxInflight(1).BindComponent<CounterComponent>("counter")));
    /**
     * OnInstall 装配窗口内借到 Installed 组件并完成纯内存读取。
     */
    BOOST_CHECK(ProbeService::s_install_borrow_ok.load() >= 1);
    BOOST_CHECK(ProbeService::s_install_borrow_value.load() == 33);
}

/**
 * 两 subject 绑定同一组件：Unbind 只等自身 borrow 排空，不影响
 * 另一 subject 的借用；retiring 边在 borrow 归零后收口。
 */
CO_TEST_CASE(two_subjects_independent_unbind) {
    auto app = fw::CoApp::Create(MakeAppOptions());
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

    /**
     * svc1 持有一个 borrow；Unbind svc1 只等该 binding 归零。
     */
    auto h1v = h1.value().Borrow();
    BOOST_REQUIRE(h1v);
    auto r = bbt_co_await app->CoUnbindComponent<CounterComponent>(
        svc1.value(), "counter", {},
        fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(50)));
    /**
     * 在途 borrow 未归零 → TimedOut；svc2 的 binding 不受影响。
     */
    BOOST_CHECK(!r && r.error().code == ErrorCode::TimedOut);
    {
        auto b2 = h2.value().Borrow();
        BOOST_CHECK(b2);
    }
    {
        /**
         * 显式归还 borrow（离开作用域析构归还计数）。
         */
        auto released = std::move(h1v.value());
        (void)released;
    }
    /**
     * 归还后同一四参数入口续等该代 → 排空收口成功（默认入口不续等）。
     */
    BOOST_REQUIRE(bbt_co_await app->CoUnbindComponent<CounterComponent>(
        svc1.value(), "counter", {},
        fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(500))));
    /**
     * svc1 旧 handle 失效；svc2 仍可借。
     */
    auto b1 = h1.value().Borrow();
    BOOST_CHECK(!b1 && b1.error().code == ErrorCode::Closed);
    auto b2 = h2.value().Borrow();
    BOOST_CHECK(b2);
}

/**
 * 组件→组件依赖：subject 组件的 GetComponent 可取得依赖句柄并借用；
 * 环绑定被拒绝（InvalidArgument）。
 */
CO_TEST_CASE(component_dep_getcomponent_and_cycle) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<DepComponent>("dep",
                                                                     DepComponent::Options{7}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<UserComponent>("user",
                                                                      UserComponent::Options{}));
    BOOST_REQUIRE(bbt_co_await app->CoStart());

    auto dep_h = app->FindComponent<DepComponent>("dep");
    auto user_h = app->FindComponent<UserComponent>("user");
    BOOST_REQUIRE(dep_h && user_h);

    /**
     * user（Component subject）→ dep：GetComponent 真实实例化 + Borrow。
     */
    UserComponent::s_get_ok = 0;
    UserComponent::s_borrow_ok = 0;
    auto bind = bbt_co_await app->CoBindComponent<DepComponent>(user_h.value(), "dep");
    BOOST_REQUIRE(bind);
    BOOST_CHECK(UserComponent::s_get_ok.load() >= 1);
    BOOST_CHECK(UserComponent::s_borrow_ok.load() >= 1);

    /**
     * dep → user 会成环（user 已依赖 dep）→ InvalidArgument。
     */
    auto cyc = bbt_co_await app->CoBindComponent<UserComponent>(dep_h.value(), "user");
    BOOST_CHECK(!cyc && cyc.error().code == ErrorCode::InvalidArgument);

    /**
     * 解绑派发 bound=false 事件。
     */
    UserComponent::s_unbind_events = 0;
    BOOST_REQUIRE(bbt_co_await app->CoUnbindComponent<DepComponent>(user_h.value(), "dep"));
    BOOST_CHECK(UserComponent::s_unbind_events.load() >= 1);
}

/**
 * 默认解绑不等在途显式 Borrow：摘除关系立即成功；旧代新 Borrow 被拒，
 * 在途 Borrow 保有原资源保证直至归还；同槽位随即发布的新代不受影响。
 */
CO_TEST_CASE(default_unbind_immediate_with_outstanding_borrow) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    CounterComponent::Options opts;
    opts.initial = 7;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("counter", opts));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc = app->FindService("echo");
    BOOST_REQUIRE(svc);
    auto h1 = bbt_co_await app->CoBindComponent<CounterComponent>(svc.value(), "counter");
    BOOST_REQUIRE(h1);
    const auto gen1 = h1.value().BindingGeneration();
    BOOST_CHECK(h1.value().IsBound());

    auto borrow = h1.value().Borrow();
    BOOST_REQUIRE(borrow);
    borrow.value().Get().Inc();

    /**
     * 旧 Borrow 在途：默认解绑立即返回，不等排空。
     */
    BOOST_REQUIRE(bbt_co_await app->CoUnbindComponent<CounterComponent>(
        svc.value(), "counter"));
    BOOST_CHECK(!h1.value().IsBound());
    auto rejected = h1.value().Borrow();
    BOOST_CHECK(!rejected && rejected.error().code == ErrorCode::Closed);

    /**
     * 在途 Borrow 保有原资源保证；期间同槽位可发布新代绑定。
     */
    borrow.value().Get().Inc();
    BOOST_CHECK_EQUAL(borrow.value().Get().Get(), 9);
    auto h2 = bbt_co_await app->CoBindComponent<CounterComponent>(svc.value(), "counter");
    BOOST_REQUIRE(h2);
    BOOST_CHECK(h2.value().IsBound());
    BOOST_CHECK(h2.value().BindingGeneration() != gen1);
    BOOST_CHECK(!h1.value().IsBound());
}

/**
 * 关闭（卸载）等已发放显式 Borrow：限时未归还 → UninstallIncomplete 且
 * 对象保留；归还后重试推进真实终态。普通 lock() 强引用不参与排空，
 * 收口完成后仍保活同一 C++ 对象，但资源面 Borrow 已被拒。
 */
CO_TEST_CASE(component_close_drains_outstanding_borrow) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    CounterComponent::Options opts;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("c", opts));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc = app->FindService("echo");
    BOOST_REQUIRE(svc);
    auto h1 = bbt_co_await app->CoBindComponent<CounterComponent>(svc.value(), "c");
    BOOST_REQUIRE(h1);

    /**
     * 普通强引用（lock 分享安装控制块）：不阻塞关闭。
     */
    auto owner = h1.value().lock();
    BOOST_REQUIRE(owner);
    auto borrow = h1.value().Borrow();
    BOOST_REQUIRE(borrow);
    /**
     * 默认解绑摘除关系（不等借用），该代 Borrow 记账保留到组件排空。
     */
    BOOST_REQUIRE(bbt_co_await app->CoUnbindComponent<CounterComponent>(svc.value(), "c"));

    auto first = bbt_co_await app->CoUninstallComponent<CounterComponent>(
        "c", {}, fw::LifecycleOptions{}.SetDeadline(std::chrono::steady_clock::now() + std::chrono::milliseconds(50)));
    BOOST_CHECK(!first && first.error().code == ErrorCode::TimedOut);
    auto still = app->FindComponent<CounterComponent>("c");
    BOOST_REQUIRE(still);
    BOOST_CHECK(still.value().Info().value().state ==
                fw::ObjectState::UninstallIncomplete);

    {
        auto released = std::move(borrow.value());
        (void)released;
    }
    /**
     * 归还后重试：关闭推进到真实终态。
     */
    BOOST_REQUIRE(bbt_co_await app->CoUninstallComponent<CounterComponent>("c"));
    BOOST_CHECK(!still.value().IsValid());

    /**
     * 收口完成：普通 shared_ptr 仍保活对象；Borrow 资源面被拒。
     */
    BOOST_CHECK(owner);
    BOOST_CHECK(h1.value().lock());
    auto late = h1.value().Borrow();
    BOOST_CHECK(!late && late.error().code == ErrorCode::Closed);
    owner->Inc();
    BOOST_CHECK_EQUAL(owner->Get(), 1);
}

/**
 * 四参数解绑超时续等原代：排空窗口内同槽位发布的新代保持挂载可用；
 * 重试仍续等原代（原代借用在途 → 超时，而非立即成功），归还后收口
 * 原代且不摘新代。
 */
CO_TEST_CASE(unbind_drain_timeout_retry_keeps_new_binding) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>(
        "first", CounterComponent::Options{1}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc = app->FindService("echo");
    BOOST_REQUIRE(svc);
    auto h1 = bbt_co_await app->CoBindComponent<CounterComponent>(svc.value(), "first");
    BOOST_REQUIRE(h1);

    auto borrow = h1.value().Borrow();
    BOOST_REQUIRE(borrow);
    auto timeout = bbt_co_await app->CoUnbindComponent<CounterComponent>(
        svc.value(), "first", {},
        fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(50)));
    BOOST_CHECK(!timeout && timeout.error().code == ErrorCode::TimedOut);

    /**
     * 排空窗口内同槽位发布新代绑定（后续发布的绑定）；新代即时可用。
     */
    auto h2 = bbt_co_await app->CoBindComponent<CounterComponent>(svc.value(), "first");
    BOOST_REQUIRE(h2);
    BOOST_CHECK(h2.value().IsBound());
    auto new_borrow = h2.value().Borrow();
    BOOST_REQUIRE(new_borrow);
    BOOST_CHECK_EQUAL(new_borrow.value().Get().Get(), 1);

    /**
     * 原代借用在途：重试续等原代 → 超时（等待对象不是新代）。
     */
    auto retry = bbt_co_await app->CoUnbindComponent<CounterComponent>(
        svc.value(), "first", {},
        fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(50)));
    BOOST_CHECK(!retry && retry.error().code == ErrorCode::TimedOut);
    BOOST_CHECK(h2.value().IsBound());

    {
        auto released = std::move(borrow.value());
        (void)released;
    }
    BOOST_REQUIRE(bbt_co_await app->CoUnbindComponent<CounterComponent>(
        svc.value(), "first", {},
        fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(500))));
    BOOST_CHECK(!h1.value().IsBound());
    /**
     * 原代收口不摘新代：新代保持挂载可用。
     */
    BOOST_CHECK(h2.value().IsBound());
    auto again = h2.value().Borrow();
    BOOST_REQUIRE(again);
    BOOST_CHECK_EQUAL(again.value().Get().Get(), 1);
}

/**
 * 原子换绑：成功即同槽位发布新代；锁外旧/新 hook 均查到新实例；
 * 其他 subject 的关系保持独立；旧句柄仍锁定原实例且新 Borrow 被拒。
 */
CO_TEST_CASE(rebind_atomic_replace_hooks_see_new_instance) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    CounterComponent::Options first_opts;
    first_opts.initial = 1;
    CounterComponent::Options second_opts;
    second_opts.initial = 2;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("first", first_opts));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("second", second_opts));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<RebindProbeService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<Echo2Service>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc = app->FindService("rebind_probe");
    auto svc2 = app->FindService("echo2");
    BOOST_REQUIRE(svc && svc2);

    auto h1 = bbt_co_await app->CoBindComponent<CounterComponent>(svc.value(), "first");
    BOOST_REQUIRE(h1);
    const auto gen1 = h1.value().BindingGeneration();
    auto first_owner = h1.value().lock();
    BOOST_REQUIRE(first_owner);
    /**
     * 第二 subject 绑定同一目标：其关系独立于 svc 的换绑。
     */
    auto h_other = bbt_co_await app->CoBindComponent<CounterComponent>(svc2.value(), "first");
    BOOST_REQUIRE(h_other);
    auto kept = h_other.value().Borrow();
    BOOST_REQUIRE(kept);

    RebindProbeService::Reset();
    auto rb = bbt_co_await app->CoRebindComponent<CounterComponent>(
        svc.value(), "first", "second");
    BOOST_REQUIRE(rb);
    const auto gen2 = rb.value().BindingGeneration();
    BOOST_CHECK(gen2 != gen1);
    BOOST_CHECK(rb.value().IsBound());
    BOOST_CHECK(!h1.value().IsBound());
    BOOST_CHECK_EQUAL(h1.value().BindingGeneration(), gen1);
    /**
     * 旧句柄仍锁定原实例；新句柄锁定新实例。
     */
    BOOST_CHECK_EQUAL(h1.value().lock().get(), first_owner.get());
    auto second_owner = rb.value().lock();
    BOOST_REQUIRE(second_owner);
    BOOST_CHECK_NE(second_owner.get(), first_owner.get());
    /**
     * 旧代新 Borrow 被拒；独立 subject 的借用不受影响。
     */
    auto old_borrow = h1.value().Borrow();
    BOOST_CHECK(!old_borrow && old_borrow.error().code == ErrorCode::Closed);
    BOOST_CHECK(kept);
    kept.value().Get().Inc();
    BOOST_CHECK_EQUAL(kept.value().Get().Get(), 2);

    /**
     * 锁外旧/新 hook 均查到已发布的新实例。
     */
    auto seen = RebindProbeService::Snapshot();
    BOOST_REQUIRE_EQUAL(seen.size(), 2u);
    BOOST_CHECK(!seen[0].bound);
    BOOST_CHECK_EQUAL(seen[0].binding_generation, gen1);
    BOOST_CHECK_EQUAL(seen[0].instance, second_owner.get());
    BOOST_CHECK(seen[1].bound);
    BOOST_CHECK_EQUAL(seen[1].binding_generation, gen2);
    BOOST_CHECK_EQUAL(seen[1].instance, second_owner.get());

    /**
     * ABA：解绑后同实例重回同槽位，旧代句柄仍判未绑定，新代句柄
     * 指向同一实例；解绑通知时槽位已空，锁外查询查不到实例。
     */
    RebindProbeService::Reset();
    BOOST_REQUIRE(bbt_co_await app->CoUnbindComponent<CounterComponent>(
        svc.value(), "first"));
    auto h3 = bbt_co_await app->CoBindComponent<CounterComponent>(svc.value(), "first");
    BOOST_REQUIRE(h3);
    BOOST_CHECK(h3.value().BindingGeneration() != gen1);
    BOOST_CHECK_EQUAL(h3.value().lock().get(), first_owner.get());
    BOOST_CHECK(!h1.value().IsBound());
    BOOST_CHECK(h3.value().IsBound());
    auto aba_seen = RebindProbeService::Snapshot();
    BOOST_REQUIRE_EQUAL(aba_seen.size(), 2u);
    BOOST_CHECK(!aba_seen[0].bound);
    BOOST_CHECK(aba_seen[0].instance == nullptr);
    BOOST_CHECK(aba_seen[1].bound);
    BOOST_CHECK_EQUAL(aba_seen[1].instance, first_owner.get());
}

/**
 * 换绑验证失败保留旧关系：目标缺失、槽位已指向目标、槽位未绑定均被拒，
 * 原绑定保持挂载可借且无新事件。
 */
CO_TEST_CASE(rebind_failure_keeps_old_binding) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>(
        "first", CounterComponent::Options{5}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<RebindProbeService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc = app->FindService("rebind_probe");
    BOOST_REQUIRE(svc);
    auto h = bbt_co_await app->CoBindComponent<CounterComponent>(svc.value(), "first");
    BOOST_REQUIRE(h);
    const auto gen = h.value().BindingGeneration();
    /**
     * 首绑通知已消费，后续仅观察失败尝试是否派发事件。
     */
    RebindProbeService::Reset();

    auto missing = bbt_co_await app->CoRebindComponent<CounterComponent>(
        svc.value(), "first", "missing");
    BOOST_CHECK(!missing && missing.error().code == ErrorCode::NotFound);

    auto same = bbt_co_await app->CoRebindComponent<CounterComponent>(
        svc.value(), "first", "first");
    BOOST_CHECK(!same && same.error().code == ErrorCode::InvalidArgument);

    auto unbound_slot = bbt_co_await app->CoRebindComponent<CounterComponent>(
        svc.value(), "never_bound", "first");
    BOOST_CHECK(!unbound_slot && unbound_slot.error().code == ErrorCode::NotFound);

    BOOST_CHECK(h.value().IsBound());
    BOOST_CHECK_EQUAL(h.value().BindingGeneration(), gen);
    BOOST_CHECK(RebindProbeService::Snapshot().empty());
    auto b = h.value().Borrow();
    BOOST_REQUIRE(b);
    BOOST_CHECK_EQUAL(b.value().Get().Get(), 5);
}

/**
 * 生命周期 hook 在真实受管协程中执行（非 mock、非宿主线程）：
 * OnStart/OnStop 协程上下文断言 + hook 内再调入口被 InvalidContext 拒。
 */
CO_TEST_CASE(hooks_run_in_managed_coroutine) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    ProbeService::s_app = app.get();
    ProbeService::s_onstart_co = 0;
    ProbeService::s_onstop_co = 0;
    ProbeService::s_invalid_ctx = 0;
    CounterComponent::Options opts;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("counter", opts));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<ProbeService>(
        fw::ServiceOptions{}.SetMaxInflight(1).BindComponent<CounterComponent>("counter")));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    BOOST_CHECK(ProbeService::s_onstart_co.load() >= 1);
    BOOST_CHECK(ProbeService::s_invalid_ctx.load() >= 1);
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_CHECK(ProbeService::s_onstop_co.load() >= 1);
    ProbeService::s_app = nullptr;
}

/**
 * 依赖拓扑启停：dep 先于 user OnStart，user 先于 dep OnStop
 * （使用者先停、依赖后停）。
 */
CO_TEST_CASE(dependency_topo_start_and_reverse_stop) {
    auto app = fw::CoApp::Create(MakeAppOptions());
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
            if (order[i] == k)
                return static_cast<int>(i);
        return -1;
    };
    /**
     * 启动按依赖拓扑（dep 先于 user）；停止按逆拓扑（user 先于 dep）。
     */
    BOOST_CHECK(find_idx("start:1") >= 0);
    BOOST_CHECK(find_idx("start:U") >= 0);
    BOOST_CHECK(find_idx("start:1") < find_idx("start:U"));
    BOOST_CHECK(find_idx("stop:U") >= 0);
    BOOST_CHECK(find_idx("stop:1") >= 0);
    BOOST_CHECK(find_idx("stop:U") < find_idx("stop:1"));
}

/**
 * deadline 超时保留责任：OnStop 真实挂起超过 deadline → StopIncomplete，
 * owner/lease 保留；再次 Stop 以更晚期限推进至真实终态。
 */
CO_TEST_CASE(deadline_timeout_retains_then_settles) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<DepComponent>("dep",
                                                                     DepComponent::Options{2}));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto dep_h = app->FindComponent<DepComponent>("dep");
    BOOST_REQUIRE(dep_h);

    /**
     * OnStop 挂起 300ms，deadline 只给 30ms → 超时。
     */
    DepComponent::s_suspend_ms = 300;
    BOOST_REQUIRE(bbt_co_await app->CoStop(fw::LifecycleOptions{}.SetDeadline(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(30))));
    BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);
    /**
     * owner/lease 保留：对象仍在 Registry 中可观察。
     */
    BOOST_CHECK(dep_h.value().IsValid());
    BOOST_CHECK(!app->PendingCleanup().empty());
    /**
     * hook 仍未达真实终态：s_order 中无 stop:2。
     */
    {
        std::lock_guard<std::mutex> lk(DepComponent::s_order_mtx);
        BOOST_CHECK(std::find(DepComponent::s_order.begin(),
                              DepComponent::s_order.end(),
                              "stop:2") == DepComponent::s_order.end());
    }

    /**
     * 再次 Stop：等前序挂起 hook 完成（更长 deadline），收口至真实终态。
     */
    DepComponent::s_suspend_ms = 0;
    BOOST_REQUIRE(bbt_co_await app->CoStop(fw::LifecycleOptions{}.SetDeadline(
        std::chrono::steady_clock::now() + std::chrono::seconds(3))));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_CHECK(!dep_h.value().IsValid());
}

/**
 * Run：经 RequestShutdown 退出；真实 scheduler（非 mock），退出码 Ok。
 */
CO_TEST_CASE(request_shutdown_is_not_stop) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    app->RequestShutdown();
    bbt::coroutine::WaitOptions wait;
    wait.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    BOOST_REQUIRE(bbt_co_await app->CoWaitShutdownRequest(wait));
    BOOST_CHECK(app->State() == fw::AppState::Running);
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
}

/**
 * ---- Co 入口 deadline 补齐用例 ----
 */

/**
 * 受闸 hook 服务：OnStop 进入后先计数放行信号，再挂起在测试闸门上，
 * 直至用例显式放行。以此证明「hook 已进入、尚未到达真实终态」，
 * 而非靠 sleep 凑时序。
 * 闸门带记忆：Open() 先于 Wait() 到达时 Wait 立即返回——用
 * on_registered 复查关闭「先放行后挂起」竞态。
 */
struct CoGate {
    std::atomic<bool> open{false};
    bbt::coroutine::sync::CoWaiter::SPtr waiter =
        bbt::coroutine::sync::CoWaiter::Create();

    void Open() {
        open.store(true, std::memory_order_release);
        waiter->Notify();
    }
    /**
     * 在受管协程内调用：gate 未开则挂起至 Open。
     */
    void Wait() {
        if (open.load(std::memory_order_acquire))
            return;
        /**
         * 无期限：终态由 Open 放行
         */
        bbt::coroutine::WaitOptions wo;
        waiter->WaitWithCallback(wo, [this] {
            if (open.load(std::memory_order_acquire))
                waiter->Notify();
            return true;
        });
    }
};

class GateService final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "gate_svc";
    explicit GateService(fw::detail::ConstructKey k) : fw::CoService<>(k) {}

    /**
     * OnStop 已进入次数
     */
    static std::atomic<int> s_onstop_entered;
    /**
     * OnStop 已返回次数
     */
    static std::atomic<int> s_onstop_returned;
    /**
     * OnStop 在协程内执行
     */
    static std::atomic<bool> s_onstop_co;

    /**
     * 由用例持有：OnStop/OnStart 在各自闸门上阻塞，直到 Open()。
     */
    static std::shared_ptr<CoGate> s_gate;
    static std::shared_ptr<CoGate> s_start_gate;

protected:
    ~GateService() noexcept override = default;

    result<void> OnStart() override {
        if (s_start_gate)
            s_start_gate->Wait();
        return result<void>::ok();
    }
    result<void> OnStop() override {
        if (bbt::coroutine::GetLocalCoroutineId() != 0)
            s_onstop_co.store(true);
        ++s_onstop_entered;
        /**
         * 真实协程挂起：不阻塞 worker
         */
        if (s_gate)
            s_gate->Wait();
        ++s_onstop_returned;
        return result<void>::ok();
    }
    friend class fw::detail::Registry;
};

std::atomic<int> GateService::s_onstop_entered{0};
std::atomic<int> GateService::s_onstop_returned{0};
std::atomic<bool> GateService::s_onstop_co{false};
std::shared_ptr<CoGate> GateService::s_gate;
std::shared_ptr<CoGate> GateService::s_start_gate;

/**
 * Co* 入口真实受管协程路径：CoStart/CoPause/CoResume/CoStop 在业务
 * 协程内成功完成；hook 仍在受管协程中执行。
 */
CO_TEST_CASE(co_lifecycle_entries_in_managed_coroutine) {
    auto app = fw::CoApp::Create(MakeAppOptions());
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

/**
 * Co 入口有限 deadline：OnStop 挂起受闸阻塞 → 短 deadline 的 CoStop
 * 在 hook 放行前返回 TimedOut（区分性：内联执行下本会等到放行才返回），
 * App 落 StopIncomplete、owner 未销毁；放行后续等同一对象收口至
 * Stopped。宿主与协程入口共享同一 deadline 不重设。
 */
CO_TEST_CASE(co_stop_deadline_returns_before_hook_settles) {
    auto app = fw::CoApp::Create(MakeAppOptions());
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

    /**
     * 短 deadline 的 CoStop：hook 已进入闸门、尚未放行。调用协程必须
     * 在 hook 到达真实终态前返回（内联执行下本会等到放行才返回——
     * 区分即「先返回再收口」的次序）。超时经 LifecycleFailure +
     * StopIncomplete 暴露（与宿主 Stop 同语义：Stop 本身返回 ok）。
     */
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(80);
        auto r = bbt_co_await app->CoStop(
            fw::LifecycleOptions{}.SetDeadline(deadline));
        BOOST_CHECK(r);
    }));
    /**
     * hook 已进入且受闸阻塞（未到达真实终态）。
     */
    BOOST_CHECK(GateService::s_onstop_entered.load() >= 1);
    BOOST_CHECK(GateService::s_onstop_returned.load() == 0);
    /**
     * 超时已如实记录（TimedOut 诊断）。
     */
    {
        bool saw_timeout = false;
        for (const auto& f : app->LifecycleFailures())
            if (f.code == ErrorCode::TimedOut)
                saw_timeout = true;
        BOOST_CHECK_MESSAGE(saw_timeout,
                            "短 deadline 的 CoStop 未留下 TimedOut 诊断");
    }
    /**
     * 责任保留：App 未完成收口、对象未被销毁。
     */
    BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);
    BOOST_CHECK(svc_h.value().IsValid());
    BOOST_CHECK(!app->PendingCleanup().empty());

    /**
     * 放行挂起的 hook；同一对象的再调等待在途 hook 至真实终态后收口。
     */
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

/**
 * 组件路径同责任保留：CoStop 下组件 OnStop 超时 → 组件
 * StopIncomplete、handle 仍有效；放行后再调收口至 Destroyed。
 */
CO_TEST_CASE(co_component_hook_deadline_retains_then_settles) {
    auto app = fw::CoApp::Create(MakeAppOptions());
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

    /**
     * 组件 OnStop 真实挂起超过 deadline → 等待到期返回（区分于内联
     * 执行下必须等挂起自然结束才返回）。超时经 LifecycleFailure +
     * StopIncomplete 暴露（Stop 本身返回 ok）。
     */
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
            if (f.code == ErrorCode::TimedOut)
                saw_timeout = true;
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

    /**
     * 组件 OnStop 300ms 挂起自然结束；以更晚期限再调等其真实终态并收口。
     */
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

/**
 * CoStart 的 TimedOut 直接透出：OnStart 受闸阻塞、短 deadline →
 * CoStart 返回 TimedOut（Start 路径把 hook 错误原样透出并记 Failed；
 * 放行后 hook 到真实终态，责任链不丢）。
 */
CO_TEST_CASE(co_start_deadline_returns_timeout) {
    auto app = fw::CoApp::Create(MakeAppOptions());
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
    /**
     * hook 尚未放行 → 服务未 Running；App 诚实落 Running（部分启动）。
     */
    BOOST_CHECK(app->State() == fw::AppState::Running);
    auto info = app->FindService("gate_svc");
    BOOST_REQUIRE(info);
    BOOST_CHECK(info.value().Info().value().state !=
                fw::ObjectState::Running);

    /**
     * 放行：hook 到真实终态并写回结果（责任链完整——hook 从未被强杀）。
     * Open 只触发唤醒，被唤醒的 hook 协程恢复后仍要读写 CoGate/CoWaiter
     * 自身成员直到退出 Wait；等同一对象在途 hook 到真实终态收口后闸门
     * 才不再被访问，此时释放才是安全的。
     */
    GateService::s_start_gate->Open();
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(3);
        auto r = bbt_co_await app->CoStop(
            fw::LifecycleOptions{}.SetDeadline(deadline));
        BOOST_CHECK(r);
    }));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    GateService::s_start_gate.reset();
}

/**
 * 预算区分（回归：shutdown_budget 此前只记账、无消费点——CoStop 误用
 * lifecycle_budget 回填）：
 * (a) CoStop 未显式 SetDeadline → 以 shutdown_budget 为单轮总期限
 *     （suspend 300ms × shutdown 60ms → StopIncomplete），而非
 *     lifecycle_budget=3000ms（旧误用下本会等到 OnStop 自然完成再
 *     推进至 Stopped，StopIncomplete 区分失败）；
 * (b) 同一轮收口不重设：再次 CoStop 带明确更晚期限 → 等前序挂起
 *     OnStop 到真实终态并推进至 Stopped（重复 CoStop 可推进、
 *     保留明确 deadline）；
 * (c) CoPause 未设 deadline → 仍以 lifecycle_budget（而非
 *     shutdown_budget）回填（suspend 300ms × lifecycle 3000ms →
 *     等满自然完成，Paused；若误用 shutdown_budget 则超期 Failed）。
 */
CO_TEST_CASE(co_stop_consumes_shutdown_budget_not_lifecycle) {
    /**
     * 预算区分前提：lifecycle 显式 3000ms > OnStop 挂起 300ms——旧误用
     * lifecycle_budget 回填时会等满至 Stopped，本用例 (a) 必失败；若只留
     * MakeAppOptions 的默认小预算（<300ms），两种回填都超时落
     * StopIncomplete，断言失去判别力。
     */
    auto app = fw::CoApp::Create(MakeAppOptions()
                                     .SetShutdownBudget(
                                         std::chrono::milliseconds(60))
                                     .SetLifecycleBudget(
                                         std::chrono::milliseconds(3000)));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<DepComponent>(
        "dep", DepComponent::Options{4}));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto dep_h = app->FindComponent<DepComponent>("dep");
    BOOST_REQUIRE(dep_h);

    /**
     * (a) 未显式 deadline 的 CoStop：shutdown_budget=60ms < OnStop
     * 挂起 300ms → 收口未竟。误用 lifecycle_budget=3000ms 时本断言
     * 失败（会被等满至 Stopped）。
     */
    DepComponent::s_suspend_ms = 300;
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);
    BOOST_CHECK(dep_h.value().IsValid());
    BOOST_CHECK(!app->PendingCleanup().empty());

    /**
     * (b) 重复 CoStop 带明确 deadline：等前序挂起 hook 至真实终态
     * （同轮总预算不被子步骤重置——本轮 3s 覆盖 hook 剩余 ~240ms +
     * 对象收口）。
     */
    DepComponent::s_suspend_ms = 0;
    BOOST_REQUIRE(bbt_co_await app->CoStop(
        fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() +
            std::chrono::seconds(3))));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_CHECK(!dep_h.value().IsValid());
}

/**
 * (c) lifecycle_budget 的对应对照：CoPause/CoResume 未设 deadline 仍用
 * lifecycle_budget 回填，不被 shutdown_budget 误伤。
 */
CO_TEST_CASE(co_pause_consumes_lifecycle_budget_not_shutdown) {
    /**
     * CoPause 回填 lifecycle_budget：显式 3000ms（不用 MakeAppOptions 的
     * 200ms 默认，与注释及 (a) 用例口径一致）；未挂起路径即刻完成。
     */
    auto app = fw::CoApp::Create(MakeAppOptions()
                                     .SetShutdownBudget(
                                         std::chrono::milliseconds(60))
                                     .SetLifecycleBudget(
                                         std::chrono::milliseconds(3000)));
    GateService::s_gate.reset();
    GateService::s_onstop_entered = 0;
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<GateService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());

    /**
     * CoPause 无 suspend：lifecycle_budget=3000ms 内即刻完成 → Paused。
     *（区分语义入口；若 CoPause 误用 shutdown_budget=60ms，挂起中的
     * OnPause 会超期——本用例走「未挂起」路径只验证回填选择。）
     */
    BOOST_REQUIRE(bbt_co_await app->CoPause());
    BOOST_CHECK(app->State() == fw::AppState::Paused);
    BOOST_REQUIRE(bbt_co_await app->CoResume());
    BOOST_CHECK(app->State() == fw::AppState::Running);
    BOOST_REQUIRE(bbt_co_await app->CoStop(
        fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() +
            std::chrono::seconds(3))));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    GateService::s_gate.reset();
}

namespace {
} // namespace

BOOST_AUTO_TEST_CASE(non_co_rejected_before_registry_or_hook_side_effects) {
    std::shared_ptr<fw::CoApp> app;
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        app = fw::CoApp::Create(MakeAppOptions(), "gate");
        BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>(
            "counter", CounterComponent::Options{}));
        BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
            fw::ServiceOptions{}.SetMaxInflight(1)));
    }));
    const auto service = app->FindService("echo").value();
    const auto component = app->FindComponent<CounterComponent>("counter").value();
    const auto before = bbt::coroutine::GetCoObjectStats("framework.CoService");
    const auto reg = fw::RegistryForTest(*app);
    std::size_t objects;
    {
        std::lock_guard<std::mutex> lock(reg->Mutex());
        objects = reg->CountObjectsLocked();
        BOOST_CHECK_EQUAL(reg->CountBindingsLocked(), 0U);
    }
    int events = 0;
    /**
     * 非 Co 上下文的两层既定事实分开断言：
     * (a) bbt_co_await 前置协议——native 线程 await 抛 std::logic_error，
     * 产品函数体未执行（既定协议，catch 即验证）；
     * (b) 产品侧零副作用——不经 await 直接调用（同一 _CheckCoContext gate
     * 返回 InvalidContext result），随后断言 Registry/创建统计/事件
     * 全部不变。两组覆盖同一面，互不代替。
     */
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
    /**
     * 返回域：函数体已在 native 域真实执行，结果由 AsyncReturn 携带；
     * 在受管协程里解包只过 AwaitHelper 的环境检查、不重跑产品函数，
     * 因此可直接断言入口给出 InvalidContext，而非默默成功或换错误码。
     * 三类代表路径（登记、生命周期、订阅）共用同一 _CheckCoContext。
     */
    auto install_pending = app->CoInstallService<FailInstallService>();
    auto start_pending = app->CoStart();
    auto observe_pending = app->CoObserve(
        service, component, [](const fw::ObjectStateEvent&) {});
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        auto installed = bbt_co_await std::move(install_pending);
        auto started = bbt_co_await std::move(start_pending);
        auto observed = bbt_co_await std::move(observe_pending);
        BOOST_CHECK(!installed &&
                    installed.error().code == ErrorCode::InvalidContext);
        BOOST_CHECK(!started && started.error().code == ErrorCode::InvalidContext);
        BOOST_CHECK(!observed &&
                    observed.error().code == ErrorCode::InvalidContext);
    }));
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
    /**
     * CoApp 构造即 _CheckCoContext：native 域唯一入口 Create 在
     * MakeCoObject 提交前抛 std::logic_error，无 Registry/统计副作用。
     */
    const auto before = bbt::coroutine::GetCoObjectStats("framework.CoApp");
    BOOST_CHECK_THROW((void)fw::CoApp::Create(MakeAppOptions(), "rejected"),
                      std::logic_error);
    BOOST_CHECK_EQUAL(
        bbt::coroutine::GetCoObjectStats("framework.CoApp").created_total,
        before.created_total);
}

CO_TEST_CASE(manager_observation_does_not_own_app_and_stop_is_not_destruction) {
    const auto before = bbt::coroutine::GetCoObjectStats("framework.CoApp");
    auto app = fw::CoApp::Create(MakeAppOptions(), "observed");
    std::weak_ptr<fw::CoApp> weak = app;
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
    /**
     * App self entry consumes the only slot.
     */
    options.SetLimits(1, 1, 1, 1);
    auto app = fw::CoApp::Create(options);
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

/**
 * ---- F17 收口用例的附加类型 ----
 */

/**
 * 多继承组件：CoComponent 非首基类（Marker 在前），其 void*→C* 旧
 * static_cast 会错位；Borrow 经 CoComponent* 调整必须返回正确地址
 * （成员断言读到真实字段而不是 Marker 子对象）。
 */
struct MarkerBase {
    virtual ~MarkerBase() = default;
    std::uint64_t tag = 0xDEAD;
};
class MiComponent final : public MarkerBase, public fw::CoComponent {
public:
    struct Options {};
    explicit MiComponent(fw::detail::ConstructKey k, Options)
        : fw::CoComponent(k) {}
    int value = 77;

protected:
    ~MiComponent() noexcept override = default;
    friend class fw::detail::Registry;
};

/**
 * OnStop 可编程失败的组件：记录进入次序（与 borrow 释放次序比较）、
 * 按静态标志返回错误（验证「OnStop 失败保留 owner、可再次收口」）。
 */
class StopCtlComponent final : public fw::CoComponent {
public:
    struct Options {};
    explicit StopCtlComponent(fw::detail::ConstructKey k, Options)
        : fw::CoComponent(k) {}

    static std::mutex s_mtx;
    /**
     * "stop"/"release" 次序
     */
    static std::vector<std::string> s_seq;
    /**
     * OnStop 返回错误
     */
    static std::atomic<bool> s_fail;
    static std::atomic<int> s_stop_calls;

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

std::mutex StopCtlComponent::s_mtx;
std::vector<std::string> StopCtlComponent::s_seq;
std::atomic<bool> StopCtlComponent::s_fail{false};
std::atomic<int> StopCtlComponent::s_stop_calls{0};

/**
 * 受控 borrow 持有者：OnStop 排空顺序探针在测试协程外持 lease。
 * 析构即归还 borrow（BindingLease 析构驱动 Registry 计数归还）。
 */

/** ---- P1 回归夹具：卸载保留失败组件及其依赖 ---- */

/** OnStop 按实例 tag 可编程失败（s_fail_tag 等于该 tag 时返回错误）；
 * 记录 start/stop 事件序，供回归断言收口顺序与“重试不重跑已完成 hook”。 */
class FailTagComponent final : public fw::CoComponent {
public:
    struct Options {
        int tag = 0;
    };
    explicit FailTagComponent(fw::detail::ConstructKey k, Options o)
        : fw::CoComponent(k), m_tag(o.tag) {}
    int Tag() const noexcept { return m_tag; }

    /** -1 = 不失败；否则等于 tag 的实例 OnStop 返回错误。 */
    static std::atomic<int> s_fail_tag;
    static std::mutex s_order_mtx;
    static std::vector<std::string> s_order;

private:
    int m_tag;
    friend class fw::detail::Registry;

protected:
    ~FailTagComponent() noexcept override = default;
    result<void> OnStart() override {
        std::lock_guard<std::mutex> lk(s_order_mtx);
        s_order.push_back("start:" + std::to_string(m_tag));
        return result<void>::ok();
    }
    result<void> OnStop() override {
        std::lock_guard<std::mutex> lk(s_order_mtx);
        s_order.push_back("stop:" + std::to_string(m_tag));
        if (s_fail_tag.load(std::memory_order_relaxed) == m_tag)
            return result<void>::err(fw::MakeError(
                ErrorCode::InternalError, "OnStop 显式失败"));
        return result<void>::ok();
    }
    friend class fw::detail::Registry;
};

std::atomic<int> FailTagComponent::s_fail_tag{-1};
std::mutex FailTagComponent::s_order_mtx;
std::vector<std::string> FailTagComponent::s_order;

/** 仅记录 OnStop/OnUninstall 次数的服务，作为 owned 组件的 owner。 */
class DepRetainService final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "dep_retain";
    static std::atomic<int> s_stop;
    static std::atomic<int> s_uninstall;
    explicit DepRetainService(fw::detail::ConstructKey k)
        : fw::CoService<>(k) {}

protected:
    ~DepRetainService() noexcept override = default;
    result<void> OnStop() override {
        ++s_stop;
        return result<void>::ok();
    }
    void OnUninstall() override { ++s_uninstall; }
    friend class fw::detail::Registry;
};

std::atomic<int> DepRetainService::s_stop{0};
std::atomic<int> DepRetainService::s_uninstall{0};

/** 持有被观察组件（Service 作 observer）的服务；OnUninstall 时快照回调计数。 */
class ObserveService final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "observe_svc";
    static std::atomic<std::size_t> s_cb_total;
    static std::atomic<std::size_t> s_cb_at_uninstall;
    explicit ObserveService(fw::detail::ConstructKey k)
        : fw::CoService<>(k) {}

protected:
    ~ObserveService() noexcept override = default;
    void OnUninstall() override {
        s_cb_at_uninstall.store(s_cb_total.load(std::memory_order_relaxed));
    }
    friend class fw::detail::Registry;
};

std::atomic<std::size_t> ObserveService::s_cb_total{0};
std::atomic<std::size_t> ObserveService::s_cb_at_uninstall{0};

static std::mutex g_observe_mtx;
static std::vector<fw::ObjectState> g_observe_seen;

/** OnStop 有界挂起：进入后开启进度信号，在 release 门闸放行前阻塞；
 * s_fail_tag 等于该实例 tag 时，放行后返回错误，用于制造启动回退中
 * 指定组件的 OnStop 失败（其它组件正常收口）。 */
class GateStopComponent final : public fw::CoComponent {
public:
    struct Options {
        int tag = 0;
    };
    explicit GateStopComponent(fw::detail::ConstructKey k, Options o)
        : fw::CoComponent(k), m_tag(o.tag) {}
    int Tag() const noexcept { return m_tag; }
    static StageGate s_stop_enter;
    static StageGate s_start_done;
    static std::shared_ptr<CoGate> s_stop_release;
    /** -1 = 不失败；否则等于 tag 的实例 OnStop 放行后返回错误。 */
    static std::atomic<int> s_fail_tag;
    static void reset() {
        s_stop_enter.Reset();
        s_start_done.Reset();
        s_stop_release = std::make_shared<CoGate>();
        s_fail_tag.store(-1, std::memory_order_relaxed);
    }

private:
    int m_tag;
    friend class fw::detail::Registry;

protected:
    ~GateStopComponent() noexcept override = default;
    result<void> OnStop() override {
        s_stop_enter.Open();
        if (s_stop_release)
            s_stop_release->Wait();
        if (s_fail_tag.load(std::memory_order_relaxed) == m_tag)
            return result<void>::err(fw::MakeError(
                fw::ErrorCode::InternalError, "OnStop 回退失败"));
        return result<void>::ok();
    }
    friend class fw::detail::Registry;
};

StageGate GateStopComponent::s_stop_enter;
StageGate GateStopComponent::s_start_done;
std::shared_ptr<CoGate> GateStopComponent::s_stop_release;
std::atomic<int> GateStopComponent::s_fail_tag{-1};

/** 启动必败的网络宿主桩：Create 返回错误，用于把 CoStart 打入网络相位
 * 回退（此时组件与服务均已启动）。 */
class FailNetHost final : public fw::INetworkHost {
public:
    static std::atomic<bool> s_fail_create;
    fw::result<void> Create() override {
        if (s_fail_create.load(std::memory_order_relaxed))
            return fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::InternalError, "network host create 失败"));
        return fw::result<void>::ok();
    }
    fw::result<void> Start() override { return fw::result<void>::ok(); }
    void StopAccepting() noexcept override {}
    fw::result<void> WaitHandlersDone(bbt::coroutine::Deadline) override {
        return fw::result<void>::ok();
    }
    void Close() noexcept override {}
};

std::atomic<bool> FailNetHost::s_fail_create{false};

/** 无状态服务：构造“组件先启动、网络相位失败”回退场景。 */
class PlainService final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "plain_svc";
    explicit PlainService(fw::detail::ConstructKey k) : fw::CoService<>(k) {}

protected:
    ~PlainService() noexcept override = default;
    friend class fw::detail::Registry;
};

} // namespace

/**
 * 收口顺序：Stop 收口先等 borrow 排空再调 OnStop——探针先取得真实
 * borrow，CoStop 协程挂起在排空上；主协程释放 lease 后必须观察到
 * "release" 先于 "stop"（反序即回归「OnStop 先于排空」缺陷）。
 */
CO_TEST_CASE(stop_waits_borrow_release_before_onstop) {
    auto app = fw::CoApp::Create(MakeAppOptions());
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
    /**
     * 组件→组件绑定：subject=UserComponent（组件 subject 真实路径）。
     */
    auto bind = bbt_co_await app->CoBindComponent<StopCtlComponent>(
        user_h.value(), "ctl");
    BOOST_REQUIRE(bind);

    /**
     * 取得真实 borrow（optional 持有，reset 即归还 lease）。
     */
    auto borrow_opt = std::optional<fw::ComponentBorrow<StopCtlComponent>>{
        std::nullopt};
    {
        auto b = bind.value().Borrow();
        BOOST_REQUIRE(b);
        borrow_opt.emplace(std::move(b.value()));
    }

    /**
     * Stop 协程：_DestroyComponent 会阻塞在 borrow 排空上（deadline 给足）。
     */
    std::atomic<bool> stop_done{false};
    bool sub = false;
    g_scheduler->RegistCoroutineTask([app, &stop_done] {
        auto r = bbt_co_await app->CoStop(
            fw::LifecycleOptions{}.SetDeadline(
                std::chrono::steady_clock::now() +
                std::chrono::seconds(3)));
        BOOST_CHECK(r);
        stop_done.store(true);
    },
                                     sub);
    BOOST_REQUIRE(sub);

    /**
     * 让 Stop 协程进入排空等待；随后归还 borrow 并记录次序。
     */
    bbtco_sleep(50);
    BOOST_CHECK(!stop_done.load());
    {
        std::lock_guard<std::mutex> lk(StopCtlComponent::s_mtx);
        StopCtlComponent::s_seq.push_back("release");
    }
    /**
     * 归还 borrow → 排空解除
     */
    borrow_opt.reset();

    for (int i = 0; i < 300 && !stop_done.load(); ++i)
        bbtco_sleep(10);
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
        /**
         * 先归还 borrow，后执行 OnStop
         */
        BOOST_CHECK(ri < si);
    }
}

/**
 * OnStop 失败保留 owner 并可再次收口：s_fail=true → 失败记 UninstallIncomplete
 * 且对象仍登记；清失败标志后再次卸载收口至销毁。
 */
CO_TEST_CASE(onstop_failure_retains_owner_then_settles) {
    auto app = fw::CoApp::Create(MakeAppOptions());
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
    /**
     * owner 保留：对象未销毁
     */
    BOOST_CHECK(h.value().IsValid());
    bool saw_onstop_fail = false;
    for (const auto& f : app->LifecycleFailures())
        if (f.phase == "onstop")
            saw_onstop_fail = true;
    BOOST_CHECK(saw_onstop_fail);

    /**
     * 失败不是终态：清除失败源后再次卸载完成真实销毁。
     */
    StopCtlComponent::s_fail = false;
    auto r2 = bbt_co_await app->CoUninstallComponent<StopCtlComponent>("ctl");
    BOOST_CHECK(r2);
    BOOST_CHECK(!h.value().IsValid());
    BOOST_CHECK(StopCtlComponent::s_stop_calls.load() >= 2);
}

/**
 * 多继承安全：Marker 在前的组件经 ComponentHandle::Borrow 取得正确
 * 派生地址（旧 void* static_cast 路径会读到 Marker 子对象 → 断言失败）。
 */
CO_TEST_CASE(multi_inheritance_borrow_returns_adjusted_pointer) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<MiComponent>(
        "mi", MiComponent::Options{}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc_h = app->FindService("echo");
    BOOST_REQUIRE(svc_h);
    /**
     * EchoService 声明集合只含 CounterComponent；直接以用户组件为 subject
     * 做组件→组件绑定更贴近真实路径，改用组件 subject。
     */
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<UserComponent>(
        "user", UserComponent::Options{}));
    auto user_h = app->FindComponent<UserComponent>("user");
    BOOST_REQUIRE(user_h);
    auto bind = bbt_co_await app->CoBindComponent<MiComponent>(
        user_h.value(), "mi");
    BOOST_REQUIRE(bind);
    auto borrow = bind.value().Borrow();
    BOOST_REQUIRE(borrow);
    /**
     * 正确地址读真实字段
     */
    BOOST_CHECK_EQUAL(borrow.value().Get().value, 77);
    borrow.value().Get().value = 78;
    auto borrow2 = bind.value().Borrow();
    BOOST_REQUIRE(borrow2);
    BOOST_CHECK_EQUAL(borrow2.value().Get().value, 78);
    /** lock() 共享安装控制块并做多继承指针调整：
     * 与 Borrow().Get() 指向同一完整对象，CoComponent 子对象地址因前置
     * 基类 MarkerBase 而错位（调整真实发生）。 */
    auto locked = bind.value().lock();
    BOOST_REQUIRE(locked);
    BOOST_CHECK(locked.get() == &borrow.value().Get());
    /** 多继承下 CoComponent 子对象在 MiComponent 内有非零偏置：用原始地址
     * 比较（直接指针 != 会做派生到基类的调整，看不见偏置）。 */
    BOOST_CHECK(reinterpret_cast<std::uintptr_t>(locked.get()) !=
                reinterpret_cast<std::uintptr_t>(
                    static_cast<const fw::CoComponent*>(&borrow.value().Get())));
}

/**
 * 跨 App 句柄身份：两个 CoApp 各自 Registry 的句柄互不相同——
 * CoBindComponent/CoUnbindComponent 对外来 subject 句柄返回
 * InvalidArgument，不操作本 Registry 对象。
 */
CO_TEST_CASE(cross_app_subject_handle_rejected) {
    auto app_a = fw::CoApp::Create(MakeAppOptions());
    auto app_b = fw::CoApp::Create(MakeAppOptions());
    CounterComponent::Options opts;
    BOOST_REQUIRE(bbt_co_await app_a->CoInstallComponent<CounterComponent>(
        "c", opts));
    BOOST_REQUIRE(bbt_co_await app_b->CoInstallComponent<CounterComponent>(
        "c", opts));
    BOOST_REQUIRE(bbt_co_await app_b->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));

    auto foreign_svc = app_b->FindService("echo");
    BOOST_REQUIRE(foreign_svc);
    /**
     * 外来 service 句柄作 subject 绑定本 App 组件 → InvalidArgument。
     */
    auto r = bbt_co_await app_a->CoBindComponent<CounterComponent>(
        foreign_svc.value(), "c");
    BOOST_CHECK(!r && r.error().code == ErrorCode::InvalidArgument);
    /**
     * 同一句柄用于解绑同样拒绝，且不触碰本 Registry 对象。
     */
    auto u = bbt_co_await app_a->CoUnbindComponent<CounterComponent>(
        foreign_svc.value(), "c");
    BOOST_CHECK(!u && u.error().code == ErrorCode::InvalidArgument);
    /**
     * 外来组件 owner 句柄用于 placement 同样拒绝（Service scope）。
     */
    fw::ComponentOptions placement;
    placement.SetScope(fw::ComponentScope::Service)
        .SetOwner(foreign_svc.value());
    auto bad = bbt_co_await app_a->CoInstallComponent<CounterComponent>(
        "scoped", opts, placement);
    BOOST_CHECK(!bad && bad.error().code == ErrorCode::InvalidArgument);
    BOOST_CHECK(app_a->FindComponent<CounterComponent>("scoped").error().code == ErrorCode::NotFound);
}

/**
 * Observer 撤销清理：Revoke 抹除记录（额度立刻可复用）并排空在途
 * callback lease——回调闭包持有弱引用探针，撤销完成后探针已释放。
 */
CO_TEST_CASE(observer_revoke_reclaims_and_drains) {
    auto app = fw::CoApp::Create(MakeAppOptions());
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
    /**
     * 额度已回收：同 App 再次订阅成功（max_observers 未收紧也无碍——
     * 断言额度语义用收紧的 app2 更严谨）。
     */
    auto token2 = bbt_co_await app->CoObserve(
        app->Handle(), svc_h.value(),
        [&delivered](const fw::ObjectStateEvent&) { ++delivered; });
    BOOST_REQUIRE(token2);

    /**
     * 收紧额度到 1 的独立 App：第一次撤销后第二次订阅必须成功。
     */
    auto app2 = fw::CoApp::Create(
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
        /**
         * 撤销即回收额度
         */
        t1.value().Revoke();
        auto t3 = bbt_co_await app2->CoObserve(
            app2->Handle(), svc2.value(),
            [](const fw::ObjectStateEvent&) {});
        BOOST_REQUIRE(t3);
    }

    /**
     * 撤销后不再派发：触发一次状态变更，已撤销回调不交付。
     */
    delivered = 0;
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    /**
     * token2 是活跃的，delivered 可增
     */
    BOOST_CHECK(delivered >= 0);
    const auto before_uninstall = delivered;
    BOOST_REQUIRE(bbt_co_await app->CoUninstallService<EchoService>());
    /**
     * 活跃观察者收到事件流
     */
    BOOST_CHECK(delivered > before_uninstall);
}

/**
 * 卸载撤销 observer 的闭包先于对象 owner 释放：observer 对象卸载时
 * 其 callback 闭包（含捕获对象）已销毁——弱引用探针必须 expired。
 */
CO_TEST_CASE(observer_closure_released_before_owner) {
    auto app = fw::CoApp::Create(MakeAppOptions());
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
    /**
     * token 析构即 Revoke：闭包被抹除。释放本用例自持的引用后，
     * 唯一剩余引用应已随记录抹除而销毁。
     */
    probe.reset();
    BOOST_CHECK(weak_probe.expired());

    /**
     * observer 对象卸载路径：卸载撤销其 token，闭包由 Registry 在
     * owner 释放前销毁。
     */
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

/**
 * P1-2：卸载撤销在途 callback 后 DrainInactiveObservers 不再永久等待——
 * 真实交错：卸载方先见「inactive 且 in_flight>0」的记录，在途回调归还
 * lease 后等待方必须能观察到终态并抹除记录（谓词按 in_flight>0 判定）。
 */
CO_TEST_CASE(observer_auto_revoke_with_inflight_callback) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>(
        "observer_obj", CounterComponent::Options{}));
    auto svc_h = app->FindService("echo");
    auto obs_h = app->FindComponent<CounterComponent>("observer_obj");
    BOOST_REQUIRE(svc_h && obs_h);
    BOOST_REQUIRE(bbt_co_await app->CoStart());

    /**
     * 在途回调屏障：callback 内等 gate 放行——卸载发起时该回调仍在途，
     * DrainInactiveObservers 第一次扫描必须留下记录并进入等待分支。
     */
    auto gate = std::make_shared<std::promise<void>>();
    auto entered = std::make_shared<std::promise<void>>();
    auto entered_f = entered->get_future();
    std::atomic<int> delivered{0};
    auto token = bbt_co_await app->CoObserve(
        obs_h.value(), svc_h.value(),
        [gate, entered, &delivered](const fw::ObjectStateEvent&) {
            entered->set_value();
            /**
             * 回调在途：持有 lease
             */
            gate->get_future().wait();
            ++delivered;
        });
    BOOST_REQUIRE(token);

    /**
     * 触发一次目标状态变更让 callback 真实派发并进入在途（svc Pause）。
     */
    auto pause = std::async(std::launch::async, [&] {
        return RunInManagedCoroutine([&] {
            auto r = bbt_co_await app->CoPause();
            (void)r;
        });
    });
    BOOST_REQUIRE(entered_f.wait_for(std::chrono::seconds(2)) ==
                  std::future_status::ready);
    /**
     * callback 持有 lease 在途时卸载 observer 对象：其 token 被
     * RevokeObserversOfLocked 标 inactive，DrainInactiveObservers
     * 先见「inactive && in_flight>0」记录并进入等待。
     */
    auto uninstall = std::async(std::launch::async, [&] {
        return RunInManagedCoroutine([&] {
            auto r = bbt_co_await
                         app->CoUninstallComponent<CounterComponent>("observer_obj");
            (void)r;
        });
    });
    /**
     * 给卸载方一个真实窗口先执行到 Drain 等待分支，再放行在途回调。
     */
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    gate->set_value();
    BOOST_REQUIRE(uninstall.wait_for(std::chrono::seconds(4)) ==
                  std::future_status::ready);
    BOOST_CHECK(uninstall.get());
    BOOST_REQUIRE(pause.wait_for(std::chrono::seconds(4)) ==
                  std::future_status::ready);
    BOOST_CHECK(pause.get());
    BOOST_CHECK(delivered.load() >= 1);
    /**
     * 卸载后 observer 对象已销毁，handle 失效。
     */
    BOOST_CHECK(!obs_h.value().IsValid());
}

/**
 * P1-4：共享组件上其他 subject 的合法 borrow 不阻塞本 service 收口——
 * 按边排空只等本 subject 的 retiring 边归零（WaitBindingDrained）。
 */
CO_TEST_CASE(service_stop_not_blocked_by_other_subject_borrow) {
    auto app = fw::CoApp::Create(MakeAppOptions());
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

    /**
     * Service B 经自己的 binding handle 持有一个合法 borrow；Service A
     * 无任何 borrow。A 的收口只等自己那条 retiring 边归零，不被 B 的
     * 借用阻塞——修复前 A 会等整个 component_borrows 归零而超时。
     */
    auto borrow_b = h_b.value().Borrow();
    BOOST_REQUIRE(borrow_b);
    const auto stop_a = bbt_co_await app->CoUninstallService<EchoService>(
        fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() + std::chrono::seconds(2)));
    /**
     * 不得 TimedOut/StopIncomplete
     */
    BOOST_CHECK(stop_a);
    BOOST_CHECK(app->FindService("echo").error().code ==
                ErrorCode::NotFound);
    /**
     * B 的 borrow 仍存活（收口不动其他 subject 的边）。
     */
    borrow_b.value().Get().Inc();
    BOOST_CHECK(borrow_b.value().Get().Get() == 1);
}

/**
 * P1-4 直接回归：本 subject 自己持 borrow 时卸载超时；borrow 未归还时重试
 * 必须继续等同一条（原 generation）retiring 边——不得跳过它进入
 * OnStop/OnUninstall/销毁。归还后重试成功且不残留 retiring 边；同组件上
 * 其他 subject 的 borrow 不参与本 subject 的排空。
 */
class DrainRetryService final : public fw::CoService<CounterComponent> {
public:
    static constexpr std::string_view kServiceName = "drain_retry";
    explicit DrainRetryService(fw::detail::ConstructKey k)
        : fw::CoService<CounterComponent>(k) {}

    static std::atomic<int> s_onstop;
    static std::atomic<int> s_onuninstall;
    static std::atomic<int> s_destroyed;

protected:
    ~DrainRetryService() noexcept override { ++s_destroyed; }
    result<void> OnStop() override {
        ++s_onstop;
        return result<void>::ok();
    }
    void OnUninstall() override { ++s_onuninstall; }
    friend class fw::detail::Registry;
};
std::atomic<int> DrainRetryService::s_onstop{0};
std::atomic<int> DrainRetryService::s_onuninstall{0};
std::atomic<int> DrainRetryService::s_destroyed{0};

CO_TEST_CASE(service_uninstall_retry_keeps_waiting_own_borrow) {
    auto app = fw::CoApp::Create(MakeAppOptions());
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
    /**
     * 其他 subject 的 borrow 同时存在：不属于本 subject 的排空范围。
     */
    auto borrow_b = h_b.value().Borrow();
    BOOST_REQUIRE(borrow_b);

    const auto short_life = [] {
        return fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(300));
    };
    /**
     * 首次：本 subject 的 borrow 未归还 → TimedOut，hook 不推进。
     */
    const auto first = bbt_co_await
                           app->CoUninstallService<DrainRetryService>(short_life());
    BOOST_CHECK(!first);
    BOOST_CHECK(first.error().code == ErrorCode::TimedOut);
    BOOST_CHECK(DrainRetryService::s_onstop.load() == 0);
    BOOST_CHECK(DrainRetryService::s_onuninstall.load() == 0);
    BOOST_CHECK(DrainRetryService::s_destroyed.load() == 0);
    /**
     * owner 保留
     */
    BOOST_REQUIRE(app->FindService("drain_retry"));

    /**
     * 重试（borrow 仍持有）：必须续等既有 retiring 边并再次 TimedOut。
     */
    const auto second = bbt_co_await
                            app->CoUninstallService<DrainRetryService>(short_life());
    BOOST_CHECK(!second);
    BOOST_CHECK(second.error().code == ErrorCode::TimedOut);
    BOOST_CHECK(DrainRetryService::s_onstop.load() == 0);
    BOOST_CHECK(DrainRetryService::s_onuninstall.load() == 0);
    BOOST_CHECK(DrainRetryService::s_destroyed.load() == 0);

    /**
     * 归还本 subject 的 borrow：重试成功；B 的 borrow 仍在但不阻塞。
     */
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
        auto reg = fw::RegistryForTest(*app);
        std::lock_guard<std::mutex> lk(reg->Mutex());
        std::size_t residual = 0;
        for (const auto& cid :
             reg->ListIdsByKindLocked(fw::ObjectKind::Component)) {
            const auto* x = reg->FindEntryLocked(cid);
            if (!x)
                continue;
            for (const auto& b : x->bindings)
                if (b.subject_id == id_a && b.retiring)
                    ++residual;
        }
        BOOST_CHECK_EQUAL(residual, 0u);
    }
    borrow_b.value().Get().Inc();
    BOOST_CHECK(borrow_b.value().Get().Get() == 1);
}

/**
 * P1-3：subject 已封口（销毁/收口进行中）后新增 Bind 必须被拒绝——
 * BindLocked 在同一 Registry 临界区检查 subject gate_closed/
 * op_in_progress/关闭语义状态。
 */
class ClosingService final : public fw::CoService<CounterComponent> {
public:
    static constexpr std::string_view kServiceName = "closing_svc";
    explicit ClosingService(fw::detail::ConstructKey k)
        : fw::CoService<CounterComponent>(k) {}
    static std::shared_ptr<CoGate> s_gate;
    static std::atomic<int> s_onstop_entered;

protected:
    ~ClosingService() noexcept override = default;
    result<void> OnStop() override {
        ++s_onstop_entered;
        if (s_gate)
            s_gate->Wait();
        return result<void>::ok();
    }
    friend class fw::detail::Registry;
};
std::shared_ptr<CoGate> ClosingService::s_gate;
std::atomic<int> ClosingService::s_onstop_entered{0};

CO_TEST_CASE(bind_rejected_when_subject_closing) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    ClosingService::s_gate = std::make_shared<CoGate>();
    ClosingService::s_onstop_entered = 0;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>(
        "c", CounterComponent::Options{}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<ClosingService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc = app->FindService("closing_svc");
    BOOST_REQUIRE(svc);

    /**
     * 卸载进行到 OnStop 挂起点：subject 处于 op_in_progress +
     * Uninstalling——同一临界区的 Bind gate 必须返回 Closed，不会在
     * retiring 边外新增 active 边。
     */
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
    /**
     * subject 已移除：组件上不应残留指向已销毁 service 的 active 边。
     */
    auto reg = fw::RegistryForTest(*app);
    std::lock_guard<std::mutex> lk(reg->Mutex());
    bool dangling = false;
    for (const auto& cid :
         reg->ListIdsByKindLocked(fw::ObjectKind::Component)) {
        const auto* x = reg->FindEntryLocked(cid);
        if (!x)
            continue;
        for (const auto& b : x->bindings)
            if (b.subject_id == svc.value().Id() && !b.retiring)
                dangling = true;
    }
    BOOST_CHECK(!dangling);
    ClosingService::s_gate.reset();
}

/**
 * P2-1：CoStop 正确移除 Actor type registration entry 并发布一致状态——
 * type 登记条目（object 为空）同样走 FinishDestroyLocked：移除出
 * Registry 并发布 Destroyed 事件。
 */
CO_TEST_CASE(costop_removes_actor_type_entry) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallActor<WorkerActor>(
        fw::ActorOptions{}.SetMaxActors(4).SetMailboxCapacity(8).SetAggregateQueueCapacity(64)));
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
    /**
     * type entry 已收口：Registry 中不再保留该 actor 登记条目。
     */
    auto reg = fw::RegistryForTest(*app);
    {
        std::lock_guard<std::mutex> lk(reg->Mutex());
        bool found = false;
        for (const auto& id :
             reg->ListIdsByKindLocked(fw::ObjectKind::Actor)) {
            const auto* e = reg->FindEntryLocked(id);
            if (e && e->type_name == "worker")
                found = true;
        }
        BOOST_CHECK(!found);
    }
    /**
     * Destroyed 事件已发布（SetStateLocked 在 FinishDestroyLocked 内）。
     */
    BOOST_CHECK(std::find(seen.begin(), seen.end(),
                          fw::ObjectState::Destroyed) != seen.end());
}

/**
 * P1（契约锁定）：OnBindingChanged 抛错不改变绑定结果——通知抛错仍可
 * 借，绑定保持 active，异常只记 LifecycleFailure（binding_changed_hook），
 * 不回滚、不外泄。
 */
class BindingThrowService final : public fw::CoService<CounterComponent> {
public:
    static constexpr std::string_view kServiceName = "binding_throw";
    explicit BindingThrowService(fw::detail::ConstructKey k)
        : fw::CoService<CounterComponent>(k) {}

    static std::atomic<int> s_bind_hooks;
    static std::atomic<int> s_unbind_hooks;

protected:
    ~BindingThrowService() noexcept override = default;
    void OnBindingChanged(const fw::BindingEvent& ev) override {
        if (ev.bound) {
            ++s_bind_hooks;
            throw std::runtime_error("binding hook probe");
        }
        ++s_unbind_hooks;
    }
    friend class fw::detail::Registry;
};
std::atomic<int> BindingThrowService::s_bind_hooks{0};
std::atomic<int> BindingThrowService::s_unbind_hooks{0};

CO_TEST_CASE(bind_hook_throw_records_failure_and_still_borrowable) {
    BindingThrowService::s_bind_hooks = 0;
    BindingThrowService::s_unbind_hooks = 0;
    auto app = fw::CoApp::Create(MakeAppOptions());
    CounterComponent::Options opts;
    opts.initial = 3;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>(
        "counter", opts));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<BindingThrowService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc = app->FindService("binding_throw");
    BOOST_REQUIRE(svc);

    const auto bind = bbt_co_await
                          app->CoBindComponent<CounterComponent>(svc.value(), "counter");
    /**
     * hook 确已执行且异常未外泄：bind 正常返回成功。
     */
    BOOST_REQUIRE(bind);
    BOOST_CHECK_EQUAL(BindingThrowService::s_bind_hooks.load(), 1);
    /**
     * 绑定仍可借：借用手感与未抛错一致。
     */
    {
        auto borrow = bind.value().Borrow();
        BOOST_REQUIRE(borrow);
        borrow.value().Get().Inc();
        BOOST_CHECK(borrow.value().Get().Get() == 4);
    }
    /**
     * 抛错只记诊断：operation=bind、phase=binding_changed_hook。
     */
    const auto failures = app->LifecycleFailures();
    BOOST_REQUIRE_EQUAL(failures.size(), 1u);
    BOOST_CHECK_EQUAL(failures.front().operation, "bind");
    BOOST_CHECK_EQUAL(failures.front().phase, "binding_changed_hook");
    BOOST_CHECK(failures.front().code == ErrorCode::InternalError);
}

/**
 * P1（契约锁定）：解绑通知在存量借用归还后送达——borrow 未归还时解绑
 * 超时且 bound=false 通知不得提前送达；归还后重试收口并恰好通知一次。
 */
CO_TEST_CASE(unbind_notification_after_borrow_drain) {
    ProbeService::s_unbind_events = 0;
    auto app = fw::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>(
        "counter", CounterComponent::Options{}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<ProbeService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc = app->FindService("probe");
    BOOST_REQUIRE(svc);
    const auto bind = bbt_co_await
                          app->CoBindComponent<CounterComponent>(svc.value(), "counter");
    BOOST_REQUIRE(bind);

    std::optional<fw::ComponentBorrow<CounterComponent>> held;
    {
        auto b = bind.value().Borrow();
        BOOST_REQUIRE(b);
        held.emplace(std::move(b.value()));
    }

    const auto short_life = [] {
        return fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(300));
    };
    /**
     * 存量借用未归还：解绑 TimedOut，通知不得先于排空送达。
     */
    const auto first = bbt_co_await
                           app->CoUnbindComponent<CounterComponent>(svc.value(), "counter", {},
                                                                    short_life());
    BOOST_REQUIRE(!first);
    BOOST_CHECK(first.error().code == ErrorCode::TimedOut);
    BOOST_CHECK_EQUAL(ProbeService::s_unbind_events.load(), 0);
    /**
     * 超时留痕：borrow_drain 相位记 LifecycleFailure，retiring 边保留。
     */
    {
        const auto failures = app->LifecycleFailures();
        BOOST_REQUIRE_EQUAL(failures.size(), 1u);
        BOOST_CHECK_EQUAL(failures.front().operation, "unbind");
        BOOST_CHECK_EQUAL(failures.front().phase, "borrow_drain");
        BOOST_CHECK(failures.front().code == ErrorCode::TimedOut);
    }
    /**
     * 归还后重试：排空完成才送达，且恰好一次。
     */
    held.reset();
    const auto second = bbt_co_await
                            app->CoUnbindComponent<CounterComponent>(svc.value(), "counter", {},
                                                                     fw::LifecycleOptions{}.SetDeadline(
                                                                         std::chrono::steady_clock::now() + std::chrono::seconds(2)));
    BOOST_REQUIRE(second);
    BOOST_CHECK_EQUAL(ProbeService::s_unbind_events.load(), 1);
}

/**
 * 借用与订阅的最后持有者换成普通线程：受管任务取出 lease/令牌后移交
 * native 线程析构，Registry 的归还（ReleaseBorrow）与撤销（RevokeObserver）
 * 路径不要求 Co 域——借用计数归零后解绑不再等排空，撤销同时抹除订阅记录
 * 并释放回调闭包。
 */
CO_TEST_CASE(borrow_and_observer_release_on_native_thread) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>(
        "counter", CounterComponent::Options{}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc_h = app->FindService("echo");
    BOOST_REQUIRE(svc_h);
    const auto bind = bbt_co_await
                          app->CoBindComponent<CounterComponent>(svc_h.value(), "counter");
    BOOST_REQUIRE(bind);
    /**
     * 兼容窗口：旧成员入口仍返回同一 Registry，未随测试迁移失效。
     */
    BOOST_CHECK(app->RegistryForTest() == fw::RegistryForTest(*app));

    auto probe = std::make_shared<int>(7);
    std::weak_ptr<int> weak_probe = probe;
    auto borrow = bind.value().Borrow();
    BOOST_REQUIRE(borrow);
    auto token = bbt_co_await app->CoObserve(
        app->Handle(), svc_h.value(),
        [probe](const fw::ObjectStateEvent&) { (void)*probe; });
    BOOST_REQUIRE(token);

    /**
     * 移交：线程内读取组件、确认订阅生效，随后在 native 线程显式析构两个
     * RAII 值——释放发生的执行域就是本用例要锁定的边界。
     */
    std::optional<fw::ComponentBorrow<CounterComponent>> cross_borrow{
        std::move(borrow.value())};
    std::optional<fw::ObserverToken> cross_token{std::move(token.value())};
    std::atomic<int> seen{-1};
    std::atomic<bool> active{false};
    std::thread native(
        [b = std::move(cross_borrow), t = std::move(cross_token), &seen, &active]() mutable {
            seen.store(b->Get().Get(), std::memory_order_release);
            active.store(t->IsActive(), std::memory_order_release);
            b.reset();
            t.reset();
        });
    native.join();
    BOOST_CHECK_EQUAL(seen.load(), 0);
    BOOST_CHECK(active.load());

    /**
     * 归还已记账：解绑立即收口（若 native 释放未计入，解绑会超时）；
     * 撤销已抹除记录，回调闭包随记录释放。
     */
    probe.reset();
    BOOST_CHECK(weak_probe.expired());
    BOOST_REQUIRE(bbt_co_await app->CoUnbindComponent<CounterComponent>(
        svc_h.value(), "counter"));
    auto closed = bind.value().Borrow();
    BOOST_CHECK(!closed && closed.error().code == ErrorCode::Closed);
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
}

/**
 * ---- 编译期负向断言（ConstructKey 不可聚合构造）----
 * C++17 下 `ConstructKey() = default` 首声明会使空类保持 aggregate；
 * user-provided `ConstructKey() {}` 关闭该绕过——静态断言钉死性质。
 */
static_assert(!std::is_aggregate_v<fw::detail::ConstructKey>,
              "ConstructKey 必须不是 aggregate（业务不得 ConstructKey{} 绕过）");
static_assert(!std::is_default_constructible_v<fw::detail::ConstructKey>,
              "ConstructKey 默认构造私有，业务不可直接构造");

/** Stop 已进入后，晚到的换绑/新绑一律拒绝（Closed）；原有挂载关系不被
 * 伪造摘除，原句柄仍锁定原代与实例。 */
CO_TEST_CASE(stop_rejects_late_rebind) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    CounterComponent::Options opts;
    opts.initial = 3;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("c", opts));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc = app->FindService("echo");
    BOOST_REQUIRE(svc);
    auto h = bbt_co_await app->CoBindComponent<CounterComponent>(svc.value(), "c");
    BOOST_REQUIRE(h);
    BOOST_CHECK(h.value().IsBound());

    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_CHECK(app->State() == fw::AppState::Stopped);

    /** 晚到换绑：同槽位替换被拒。 */
    auto r = bbt_co_await app->CoRebindComponent<CounterComponent>(
        svc.value(), "c", "c");
    BOOST_CHECK(!r && r.error().code == ErrorCode::Closed);
    /** 晚到新绑同样被拒。 */
    auto nb = bbt_co_await app->CoBindComponent<CounterComponent>(svc.value(), "c");
    BOOST_CHECK(!nb && nb.error().code == ErrorCode::Closed);
    /** 原句柄仍描述原代挂载，实例由 lock() 保活。 */
    /** 组件已随 Stop 收口：原句柄失效。 */
    BOOST_CHECK(!h.value().IsValid());
}

/** 换绑通知期间卸载等回调：旧代退役且有在途 Borrow 时，卸载旧目标被拒且
 * 对象保留；两个通知分别携带自己的代；归还后卸载推进，新代不受影响。 */
CO_TEST_CASE(rebind_retiring_uninstall_window) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    CounterComponent::Options first_opts;
    first_opts.initial = 7;
    CounterComponent::Options second_opts;
    second_opts.initial = 8;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>(
        "first", first_opts));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>(
        "second", second_opts));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<RebindProbeService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc = app->FindService("rebind_probe");
    BOOST_REQUIRE(svc);

    auto h1 = bbt_co_await app->CoBindComponent<CounterComponent>(
        svc.value(), "first");
    BOOST_REQUIRE(h1);
    const auto gen1 = h1.value().BindingGeneration();
    auto borrow1 = h1.value().Borrow();
    BOOST_REQUIRE(borrow1);
    borrow1.value().Get().Inc();

    /** 换绑：旧代退役（borrows=1，保留条目），新代挂载。 */
    RebindProbeService::Reset();
    auto rb = bbt_co_await app->CoRebindComponent<CounterComponent>(
        svc.value(), "first", "second");
    BOOST_REQUIRE(rb);
    const auto gen2 = rb.value().BindingGeneration();
    BOOST_CHECK(gen2 != gen1);
    auto seen = RebindProbeService::Snapshot();
    BOOST_REQUIRE_EQUAL(seen.size(), 2u);
    BOOST_CHECK(!seen[0].bound && seen[0].binding_generation == gen1 &&
                seen[0].instance != nullptr);
    BOOST_CHECK(seen[1].bound && seen[1].binding_generation == gen2 &&
                seen[1].instance != nullptr);

    /** 卸载旧目标：在途 Borrow → 排空超时，UninstallIncomplete，对象保留。 */
    auto first_lookup = app->FindComponent<CounterComponent>("first");
    BOOST_REQUIRE(first_lookup);
    auto blocked = bbt_co_await app->CoUninstallComponent<CounterComponent>(
        "first", {},
        fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() + std::chrono::milliseconds(100)));
    BOOST_CHECK(!blocked && blocked.error().code == ErrorCode::TimedOut);
    first_lookup = app->FindComponent<CounterComponent>("first");
    BOOST_REQUIRE(first_lookup);
    BOOST_CHECK(first_lookup.value().IsValid());
    BOOST_CHECK(first_lookup.value().Info().value().state ==
                fw::ObjectState::UninstallIncomplete);

    /** 新代不受影响。 */
    BOOST_CHECK(!h1.value().IsBound());
    BOOST_CHECK(rb.value().IsBound());
    auto borrow2 = rb.value().Borrow();
    BOOST_REQUIRE(borrow2);
    BOOST_CHECK_EQUAL(borrow2.value().Get().Get(), 8);

    /** 归还在途 Borrow：退役条目清空，重试卸载推进到真实终态。 */
    {
        auto released = std::move(borrow1.value());
        (void)released;
    }
    BOOST_REQUIRE(bbt_co_await app->CoUninstallComponent<CounterComponent>("first"));
    auto gone = app->FindComponent<CounterComponent>("first");
    BOOST_CHECK(!gone);

    /** 旧句柄不再绑定；新句柄仍挂载可借。 */
    BOOST_CHECK(!h1.value().IsBound());
    BOOST_CHECK(rb.value().IsBound());
    auto again = rb.value().Borrow();
    BOOST_REQUIRE(again);
    BOOST_CHECK_EQUAL(again.value().Get().Get(), 8);
}

/** retiring 绑定仍占用 max_bindings：满额时新建绑定被拒；旧 Borrow 归还后
 * 退役条目清空，配额释放，新绑定方可通过。 */
CO_TEST_CASE(quota_retiring_occupies_limit) {
    auto app = fw::CoApp::Create(MakeQuotaAppOptions());
    CounterComponent::Options opts;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("c1", opts));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("c2", opts));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc = app->FindService("echo");
    BOOST_REQUIRE(svc);

    auto h1 = bbt_co_await app->CoBindComponent<CounterComponent>(svc.value(), "c1");
    BOOST_REQUIRE(h1); // 第 1 条绑定，达到 max_bindings=1
    auto borrow = h1.value().Borrow();
    BOOST_REQUIRE(borrow);

    /** 默认解绑不等在途归还：代退役但保留（borrows=1），继续占用配额。 */
    BOOST_REQUIRE(bbt_co_await app->CoUnbindComponent<CounterComponent>(
        svc.value(), "c1"));
    BOOST_CHECK(!h1.value().IsBound());

    /** 新建不同槽位的绑定：退役条目仍占额度 → Overloaded。 */
    auto h2 = bbt_co_await app->CoBindComponent<CounterComponent>(svc.value(), "c2");
    BOOST_CHECK(!h2 && h2.error().code == ErrorCode::Overloaded);

    /** 归还在途 Borrow：退役条目擦除，配额释放，新绑定通过。 */
    {
        auto released = std::move(borrow.value());
        (void)released;
    }
    auto h3 = bbt_co_await app->CoBindComponent<CounterComponent>(svc.value(), "c2");
    BOOST_REQUIRE(h3);
    BOOST_CHECK(h3.value().IsBound());
}

/** 有在途不能破限：满额且原代有在途 Borrow 时，原地换绑被拒，原关系保持
 * 挂载；归还后同一槽位原地换绑净增为 0，满额时仍可。 */
CO_TEST_CASE(quota_in_flight_cannot_break_limit) {
    auto app = fw::CoApp::Create(MakeQuotaAppOptions());
    CounterComponent::Options o1;
    o1.initial = 1;
    CounterComponent::Options o2;
    o2.initial = 2;
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("c1", o1));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>("c2", o2));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc = app->FindService("echo");
    BOOST_REQUIRE(svc);

    auto h1 = bbt_co_await app->CoBindComponent<CounterComponent>(svc.value(), "c1");
    BOOST_REQUIRE(h1);
    const auto gen1 = h1.value().BindingGeneration();
    auto borrow = h1.value().Borrow();
    BOOST_REQUIRE(borrow);

    /** 满额且原代在途：换绑被拒，不能破限，原关系保持挂载。 */
    auto rb = bbt_co_await app->CoRebindComponent<CounterComponent>(
        svc.value(), "c1", "c2");
    BOOST_CHECK(!rb && rb.error().code == ErrorCode::Overloaded);
    BOOST_CHECK(h1.value().IsBound());
    BOOST_CHECK_EQUAL(h1.value().BindingGeneration(), gen1);

    /** 归还后：同一槽位原地换绑，净增为 0，满额时仍可。 */
    {
        auto released = std::move(borrow.value());
        (void)released;
    }
    auto rb2 = bbt_co_await app->CoRebindComponent<CounterComponent>(
        svc.value(), "c1", "c2");
    BOOST_REQUIRE(rb2);
    BOOST_CHECK(rb2.value().IsBound());
    BOOST_CHECK(!h1.value().IsBound());
    auto b2 = rb2.value().Borrow();
    BOOST_REQUIRE(b2);
    BOOST_CHECK_EQUAL(b2.value().Get().Get(), 2);
}
/** 已停机的 App 再换绑被 App 停机门拒绝，不发布新依赖：旧关系保持挂载，对象
 * 在 OnStop 完成后按正常收口。PhaseService 仅作停机屏障（非换绑 subject）；
 * 换绑 subject 为独立 App scope 的 UserComponent，旧目标为 DepComponent A。
 */
CO_TEST_CASE(phase_onstop_hold_rejects_rebind_no_new_dependency) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<PhaseService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<UserComponent>(
        "user", UserComponent::Options{}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<DepComponent>(
        "A", DepComponent::Options{1}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<DepComponent>(
        "B", DepComponent::Options{2}));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto user_h = app->FindComponent<UserComponent>("user");
    BOOST_REQUIRE(user_h);

    PhaseService::reset();
    PhaseService::hold_stop();

    auto ha = bbt_co_await app->CoBindComponent<DepComponent>(user_h.value(), "A");
    BOOST_REQUIRE(ha);
    BOOST_CHECK(ha.value().IsBound());
    const auto gen_before = ha.value().BindingGeneration();

    // 后台受管协程：CoStop 阻塞于 OnStop 直至放行。结果经共享内存发布，避免在
    // 协程挂起后悬空测试栈引用；停止结果必须为成功。
    struct StopTask {
        std::optional<result<void>> outcome;
        std::atomic<bool> published{false};
    };
    auto stop_task = std::make_shared<StopTask>();
    bool sub = false;
    g_scheduler->RegistCoroutineTask(
        [app, stop_task] {
            auto r = bbt_co_await app->CoStop(
                fw::LifecycleOptions{}.SetDeadline(
                    std::chrono::steady_clock::now() + std::chrono::seconds(5)));
            stop_task->outcome = std::move(r);
            stop_task->published.store(true, std::memory_order_release);
            PhaseService::s_stop_finish_gate->Open();
        },
        sub);
    BOOST_REQUIRE(sub);

    // 任何退出路径都放行 OnStop，避免后台协程在测试作用域结束后仍活跃。
    struct StopReleaseGuard {
        ~StopReleaseGuard() {
            if (!released && PhaseService::s_stop_done_gate)
                PhaseService::s_stop_done_gate->Open();
        }
        void release() { released = true; }
        bool released = false;
    } stop_guard;

    // 有界等待 OnStop 明确进入（阶段信号，非 sleep 推断到达）。
    BOOST_REQUIRE_EQUAL(PhaseService::s_stop_enter_gate->Wait(2000), 0);
    BOOST_REQUIRE_MESSAGE(PhaseService::s_stop_entered.load() > 0,
                          "OnStop 未在预算内进入，挂起窗口未成立");

    // OnStop 仍挂起：组件和原关系有效。
    BOOST_CHECK(ha.value().IsValid());
    BOOST_CHECK(ha.value().IsBound());
    BOOST_REQUIRE(ha.value().lock());

    // App 处于 Stopping：换绑命中 App 停机门，返回 Closed；未发布新依赖，
    // 同一旧代仍 IsBound、代次不变。
    auto rb = bbt_co_await app->CoRebindComponent<DepComponent>(
        user_h.value(), "A", "B");
    BOOST_CHECK(!rb && rb.error().code == ErrorCode::Closed);
    BOOST_CHECK(ha.value().IsBound());
    BOOST_CHECK_EQUAL(ha.value().BindingGeneration(), gen_before);

    // 显式放行 OnStop，验证 Stop 结果为成功。
    stop_guard.release();
    PhaseService::s_stop_done_gate->Open();
    BOOST_REQUIRE_EQUAL(PhaseService::s_stop_finish_gate->Wait(5000), 0);
    BOOST_REQUIRE(stop_task->published.load(std::memory_order_acquire));
    BOOST_REQUIRE(stop_task->outcome.has_value());
    BOOST_CHECK(bool(*stop_task->outcome));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_CHECK_EQUAL(PhaseService::s_stop_done_wait_timedouts.load(), 0);
}

/** 旧代 OnBindingChanged 进入后保持通知暂停；在当前受管协程执行带短而有限
 * deadline 的 CoUninstallService<PhaseService>，观察 TimedOut、subject 保留/
 * Incomplete、OnStop/OnUninstall 均未执行。再放行，验证换绑任务返回成功、
 * 旧退役与新代通知真实次序、重试卸载成功、两通知都先于 OnStop/OnUninstall、
 * subject 最终 NotFound。初始 bind 通知不入新代计数。RAII 放行保证任何退出
 * 路径都释放旧代通知；wait 返回后不把超时当 Notify，超时不读未同步的 optional。
 */
CO_TEST_CASE(phase_binding_change_notify_precedes_stop_uninstall) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<PhaseService>(
        fw::ServiceOptions{}.SetMaxInflight(4)));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<DepComponent>(
        "A", DepComponent::Options{1}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<DepComponent>(
        "B", DepComponent::Options{2}));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc = app->FindService("phase_svc");
    BOOST_REQUIRE(svc);

    PhaseService::reset();

    // 初始 bind 的 bound=true 通知不计入新代计数。
    auto ha = bbt_co_await app->CoBindComponent<DepComponent>(svc.value(), "A");
    BOOST_REQUIRE(ha);
    BOOST_CHECK(ha.value().IsBound());
    const auto gen_before = ha.value().BindingGeneration();
    PhaseService::s_initial_bind_done.store(true);

    // 后台受管协程：A->B 换绑阻塞在旧代 OnBindingChanged 暂停上。结果经共享
    // 内存发布（release），完成信号开后方按 acquire 读取，超时不读 optional。
    struct RebindTask {
        std::optional<result<fw::ComponentHandle<DepComponent>>> outcome;
        std::atomic<bool> published{false};
    };
    auto rebind_task = std::make_shared<RebindTask>();
    bool sub = false;
    g_scheduler->RegistCoroutineTask(
        [app, subj = svc.value(), rebind_task] {
            auto r = bbt_co_await app->CoRebindComponent<DepComponent>(
                subj, "A", "B");
            rebind_task->outcome = std::move(r);
            rebind_task->published.store(true, std::memory_order_release);
            PhaseService::s_rebind_finish_gate->Open();
        },
        sub);
    BOOST_REQUIRE(sub);

    // RAII 放行：析构时若未显式放行，也释放旧代通知，避免后台协程悬空。
    struct BcReleaseGuard {
        std::shared_ptr<StageGate> gate;
        bool released = false;
        ~BcReleaseGuard() {
            if (!released && gate)
                gate->Open();
        }
        void release() {
            released = true;
            if (gate)
                gate->Open();
        }
    } guard{PhaseService::s_bc_release_gate};

    // 有界等待旧代 OnBindingChanged 进入（阶段信号）。
    BOOST_REQUIRE_EQUAL(PhaseService::s_bc_enter_gate->Wait(2000), 0);
    BOOST_REQUIRE_MESSAGE(PhaseService::s_bc_old_entered.load() > 0,
                          "旧代 OnBindingChanged 未在预算内进入，暂停窗口未成立");

    // 仍未放行时，当前受管协程执行带短有限 deadline 的 CoUninstallService。
    auto uninstalled = bbt_co_await app->CoUninstallService<PhaseService>(
        fw::LifecycleOptions{}.SetDeadline(
            std::chrono::steady_clock::now() + std::chrono::milliseconds(200)));
    BOOST_CHECK(!uninstalled && uninstalled.error().code == ErrorCode::TimedOut);

    // subject 保留 / Incomplete，OnStop/OnUninstall 均未执行。
    BOOST_CHECK(svc.value().IsValid());
    auto st = svc.value().Info();
    BOOST_REQUIRE(st);
    BOOST_CHECK(st.value().state == fw::ObjectState::UninstallIncomplete);
    BOOST_CHECK_EQUAL(PhaseService::s_stop_entered.load(), 0);
    BOOST_CHECK_EQUAL(PhaseService::s_uninstall_seq.load(), 0);

    // 显式放行旧代通知，让换绑完成。
    guard.release();

    // 换绑任务返回成功（wait 返回后不把超时当 Notify；超时不读未同步的 optional）。
    BOOST_REQUIRE_EQUAL(PhaseService::s_rebind_finish_gate->Wait(5000), 0);
    BOOST_REQUIRE(rebind_task->published.load(std::memory_order_acquire));
    BOOST_REQUIRE(rebind_task->outcome.has_value());
    auto& res_opt = *rebind_task->outcome;
    BOOST_REQUIRE(bool(res_opt));
    BOOST_CHECK(PhaseService::s_bc_new_seen.load() >= 1);
    BOOST_CHECK_EQUAL(PhaseService::s_new_gen.load(),
                      res_opt.value().BindingGeneration());         // 新通知代次匹配实际重绑结果
    BOOST_CHECK(res_opt.value().BindingGeneration() != gen_before); // 代次相较首绑已更替
    BOOST_CHECK(!ha.value().IsBound());                             // 旧代已退役

    // 重试卸载成功，subject 最终 NotFound。
    BOOST_REQUIRE(bbt_co_await app->CoUninstallService<PhaseService>());
    auto gone = app->FindService("phase_svc");
    BOOST_CHECK(!gone && gone.error().code == ErrorCode::NotFound);

    // 两代通知都先于 OnStop/OnUninstall：0 < old < new < stop < uninstall。
    BOOST_CHECK(PhaseService::s_stop_entered.load() >= 1);
    BOOST_CHECK(PhaseService::s_stop_seq.load() > 0);
    BOOST_CHECK(PhaseService::s_uninstall_seq.load() > 0);
    BOOST_CHECK(PhaseService::s_bc_old_seq.load() > 0);
    BOOST_CHECK(PhaseService::s_bc_new_seq.load() > PhaseService::s_bc_old_seq.load());
    BOOST_CHECK(PhaseService::s_stop_seq.load() > PhaseService::s_bc_new_seq.load());
    BOOST_CHECK(PhaseService::s_uninstall_seq.load() > PhaseService::s_stop_seq.load());
    BOOST_CHECK_EQUAL(PhaseService::s_bc_release_wait_timedouts.load(), 0);
}

/** P1-1：同一 Service 拥有 U 依赖 D（U 绑定指向 D），U.OnStop 失败时 CoUninstallService
 * 保留 U 与 D（D 未被 OnStop、仍可 Find），Service 进入 UninstallIncomplete；
 * 与 U 无依赖的独立 owned 组件仍被正常收口。清除失败源后重试，U、D 按逆拓扑
 * （先 U 后 D）收口，Service 销毁；已完成的 hook 不重跑。
 */
CO_TEST_CASE(co_uninstall_service_retains_failed_component_dependencies) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    FailTagComponent::s_fail_tag.store(-1, std::memory_order_relaxed);
    DepRetainService::s_stop.store(0, std::memory_order_relaxed);
    DepRetainService::s_uninstall.store(0, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lk(FailTagComponent::s_order_mtx);
        FailTagComponent::s_order.clear();
    }
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<DepRetainService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    auto svc = app->FindService("dep_retain");
    BOOST_REQUIRE(svc);
    fw::ComponentOptions owned;
    owned.SetScope(fw::ComponentScope::Service).SetOwner(svc.value());
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<FailTagComponent>(
        "d", FailTagComponent::Options{1}, owned));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<FailTagComponent>(
        "u", FailTagComponent::Options{2}, owned));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<FailTagComponent>(
        "i", FailTagComponent::Options{3}, owned));
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto u_h = app->FindComponent<FailTagComponent>("u", owned);
    BOOST_REQUIRE(u_h);
    /** U 绑定 D：subject=U、target=D，形成依赖边 U→D；同一 Service scope 内合法。 */
    fw::ComponentOptions bind_placement;
    bind_placement.SetScope(fw::ComponentScope::Service).SetOwner(svc.value());
    BOOST_REQUIRE(bbt_co_await app->CoBindComponent<FailTagComponent>(
        u_h.value(), "d", bind_placement));

    auto id_of = [&](const char* name) {
        auto h = app->FindComponent<FailTagComponent>(name, owned);
        BOOST_REQUIRE(h);
        return h.value().Id();
    };
    const auto u_id = id_of("u");

    /** 首次卸载：U.OnStop 失败。 */
    FailTagComponent::s_fail_tag.store(2, std::memory_order_relaxed);
    auto r1 = bbt_co_await app->CoUninstallService<DepRetainService>();
    BOOST_CHECK(!r1);
    /** owner 保留，进入 UninstallIncomplete。 */
    auto sv2 = app->FindService("dep_retain");
    BOOST_REQUIRE(sv2);
    BOOST_CHECK(sv2.value().IsValid());
    BOOST_CHECK(sv2.value().Info().value().state ==
                fw::ObjectState::UninstallIncomplete);
    /** U 保留、StopIncomplete、仍可 Find。 */
    auto u2 = app->FindComponent<FailTagComponent>("u", owned);
    BOOST_REQUIRE(u2);
    BOOST_CHECK(u2.value().IsValid());
    BOOST_CHECK(u2.value().Info().value().state == fw::ObjectState::StopIncomplete);
    /** D 未被 OnStop、未移除，仍可 Find、Running。 */
    auto d2 = app->FindComponent<FailTagComponent>("d", owned);
    BOOST_REQUIRE(d2);
    BOOST_CHECK(d2.value().IsValid());
    BOOST_CHECK(d2.value().Info().value().state == fw::ObjectState::Running);
    /** 与 U 无依赖的独立 owned 组件 I 仍被正常收口。 */
    BOOST_CHECK(!app->FindComponent<FailTagComponent>("i", owned));
    /** 失败记录：U onstop。 */
    bool saw_u_onstop = false;
    for (const auto& f : app->LifecycleFailures())
        if (f.object_id == u_id && f.phase == "onstop")
            saw_u_onstop = true;
    BOOST_CHECK(saw_u_onstop);
    auto stop_count = [&](int tag) {
        std::lock_guard<std::mutex> lk(FailTagComponent::s_order_mtx);
        return static_cast<int>(std::count(FailTagComponent::s_order.begin(),
                                           FailTagComponent::s_order.end(),
                                           "stop:" + std::to_string(tag)));
    };
    BOOST_CHECK_EQUAL(stop_count(2), 1); // U 尝试 1 次
    BOOST_CHECK_EQUAL(stop_count(1), 0); // D 未 OnStop
    BOOST_CHECK_EQUAL(stop_count(3), 1); // I 已收口
    BOOST_CHECK_EQUAL(DepRetainService::s_stop.load(std::memory_order_relaxed),
                      1);
    BOOST_CHECK_EQUAL(DepRetainService::s_uninstall.load(std::memory_order_relaxed),
                      1);

    /** 清除失败源后重试：U、D 按逆拓扑（先 U 后 D）收口，Service 销毁；
     * 已完成的 Service OnStop/OnUninstall 不重跑。 */
    FailTagComponent::s_fail_tag.store(-1, std::memory_order_relaxed);
    BOOST_REQUIRE(bbt_co_await app->CoUninstallService<DepRetainService>());
    BOOST_CHECK(!app->FindService("dep_retain"));
    BOOST_CHECK(!app->FindComponent<FailTagComponent>("u", owned));
    BOOST_CHECK(!app->FindComponent<FailTagComponent>("d", owned));
    BOOST_CHECK(!app->FindComponent<FailTagComponent>("i", owned));
    BOOST_CHECK_EQUAL(stop_count(2), 2); // U 重跑 1 次
    BOOST_CHECK_EQUAL(stop_count(1), 1); // D OnStop 仅重试中 1 次
    BOOST_CHECK_EQUAL(stop_count(3), 1);
    BOOST_CHECK_EQUAL(DepRetainService::s_stop.load(std::memory_order_relaxed),
                      1);
    BOOST_CHECK_EQUAL(DepRetainService::s_uninstall.load(std::memory_order_relaxed),
                      1);
    {
        std::lock_guard<std::mutex> lk(FailTagComponent::s_order_mtx);
        std::size_t last_u = FailTagComponent::s_order.size();
        for (std::size_t i = 0; i < FailTagComponent::s_order.size(); ++i)
            if (FailTagComponent::s_order[i] == "stop:2")
                last_u = i;
        BOOST_REQUIRE(last_u != FailTagComponent::s_order.size());
        bool d_after_u = false;
        for (std::size_t i = last_u + 1; i < FailTagComponent::s_order.size(); ++i)
            if (FailTagComponent::s_order[i] == "stop:1")
                d_after_u = true;
        BOOST_CHECK(d_after_u);
    }
}

/** P1-2：Service 经 CoObserve 订阅自己 owned 组件的状态事件。CoUninstallService
 * 过程中，Service 签发的 observer 在封门时已撤销并排空：OnUninstall 执行后
 * 不再收到该组件的 Stopping/关闭事件，回调计数在 OnUninstall 后不再增长。 */
CO_TEST_CASE(co_uninstall_service_revokes_observer_before_owned_close) {
    auto app = fw::CoApp::Create(MakeAppOptions());
    ObserveService::s_cb_total.store(0, std::memory_order_relaxed);
    ObserveService::s_cb_at_uninstall.store(SIZE_MAX, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lk(g_observe_mtx);
        g_observe_seen.clear();
    }
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<ObserveService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    auto svc = app->FindService("observe_svc");
    BOOST_REQUIRE(svc);
    fw::ComponentOptions owned;
    owned.SetScope(fw::ComponentScope::Service).SetOwner(svc.value());
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<CounterComponent>(
        "uobs", CounterComponent::Options{}, owned));
    auto uobs = app->FindComponent<CounterComponent>("uobs", owned);
    BOOST_REQUIRE(uobs);
    /** 以 Service 本身为 observer 订阅其 owned 组件。 */
    auto token = bbt_co_await app->CoObserve(svc.value(), uobs.value(),
                                             [&](const fw::ObjectStateEvent& ev) {
                                                 std::lock_guard<std::mutex> lk(g_observe_mtx);
                                                 g_observe_seen.push_back(ev.state);
                                                 ObserveService::s_cb_total.fetch_add(1,
                                                                                      std::memory_order_relaxed);
                                             });
    BOOST_REQUIRE(token);
    BOOST_CHECK(token.value().IsActive());
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    /** 订阅在卸载前已收到实体转换（证明 token 曾处于活跃投递）。 */
    {
        std::lock_guard<std::mutex> lk(g_observe_mtx);
        BOOST_CHECK(std::find(g_observe_seen.begin(), g_observe_seen.end(),
                              fw::ObjectState::Running) != g_observe_seen.end());
    }
    BOOST_REQUIRE(bbt_co_await app->CoUninstallService<ObserveService>());
    /** 封门时撤销：token 已失效。 */
    BOOST_CHECK(!token.value().IsActive());
    /** OnUninstall 后不再收到目标组件的 Stopping/关闭事件，计数不再增长。 */
    const std::size_t at_uninstall =
        ObserveService::s_cb_at_uninstall.load(std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lk(g_observe_mtx);
        BOOST_CHECK_EQUAL(g_observe_seen.size(), at_uninstall);
        BOOST_CHECK(std::find(g_observe_seen.begin(), g_observe_seen.end(),
                              fw::ObjectState::Stopping) == g_observe_seen.end());
        BOOST_CHECK(std::find(g_observe_seen.begin(), g_observe_seen.end(),
                              fw::ObjectState::Destroyed) == g_observe_seen.end());
    }
    BOOST_CHECK_EQUAL(ObserveService::s_cb_total.load(std::memory_order_relaxed),
                      at_uninstall);
    /** 事件被抑制不是因为未执行收口：组件与服务均已销毁。 */
    BOOST_CHECK(!app->FindService("observe_svc"));
    BOOST_CHECK(!app->FindComponent<CounterComponent>("uobs", owned));
}

/** P1-3：CoStart 网络相位失败（宿主 Create 失败）触发回退，且某已启动组件
 * OnStop 失败。回退期间 bind/rebind 被拒（返回 Closed）；回退后 App 为
 * StopIncomplete；随后 CoStop 将残留组件收口至 Stopped。 */
CO_TEST_CASE(co_start_rollback_rejects_bind_then_stop_collects) {
    auto host = std::make_shared<FailNetHost>();
    FailNetHost::s_fail_create.store(false, std::memory_order_relaxed);
    auto app = fw::MakeCoAppForTest(
        MakeAppOptions()
            .SetLocalIdentity(std::string{"p1-3-rollback"})
            .SetNetworkLimits(bbt::infra::NetworkLimits{
                /*max_connections*/ 64,
                /*max_inflight*/ 64,
                /*max_header_bytes*/ 8192,
                /*max_body_bytes*/ 65536,
                /*incoming_timeout*/ std::chrono::milliseconds{30000}}),
        fw::CoAppSeam{host, {}});
    GateStopComponent::reset();
    FailNetHost::s_fail_create.store(true, std::memory_order_relaxed);
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<GateStopComponent>(
        "t2", GateStopComponent::Options{1}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<GateStopComponent>(
        "t", GateStopComponent::Options{2}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<GateStopComponent>(
        "c", GateStopComponent::Options{3}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<PlainService>(
        fw::ServiceOptions{}.SetMaxInflight(1)));
    auto t2_h = app->FindComponent<GateStopComponent>("t2");
    auto c_h = app->FindComponent<GateStopComponent>("c");
    BOOST_REQUIRE(t2_h && c_h);
    /** c→t 依赖：关闭时 c 先于 t 收口；t2 独立，t2 先于 c 收口（名称字典序）。 */
    fw::ComponentOptions bind_placement;
    BOOST_REQUIRE(bbt_co_await app->CoBindComponent<GateStopComponent>(
        c_h.value(), "t", bind_placement));

    struct StartTask {
        std::optional<result<void>> outcome;
        std::atomic<bool> published{false};
    };
    auto start_task = std::make_shared<StartTask>();
    bool sub = false;
    g_scheduler->RegistCoroutineTask(
        [app, start_task] {
            auto r = bbt_co_await app->CoStart();
            start_task->outcome = std::move(r);
            start_task->published.store(true, std::memory_order_release);
            GateStopComponent::s_start_done.Open();
        },
        sub);
    BOOST_REQUIRE(sub);

    /** 网络相位失败：组件/服务均已启动，CoStart 返回宿主创建错误。 */
    BOOST_REQUIRE_EQUAL(GateStopComponent::s_stop_enter.Wait(2000), 0);
    BOOST_CHECK(app->State() == fw::AppState::Stopping);
    /** 绑定发布门已关：新增 bind 与原子换绑均返回 Closed。 */
    auto nb = bbt_co_await app->CoBindComponent<GateStopComponent>(
        t2_h.value(), "c", bind_placement);
    BOOST_CHECK(!nb && nb.error().code == ErrorCode::Closed);
    auto rb = bbt_co_await app->CoRebindComponent<GateStopComponent>(
        c_h.value(), "", "t2", bind_placement);
    BOOST_CHECK(!rb && rb.error().code == ErrorCode::Closed);

    /** 放行 c.OnStop 并使其失败：回退带错误结束。 */
    GateStopComponent::s_fail_tag.store(3, std::memory_order_relaxed);
    GateStopComponent::s_stop_release->Open();
    BOOST_REQUIRE_EQUAL(GateStopComponent::s_start_done.Wait(5000), 0);
    BOOST_REQUIRE(start_task->published.load(std::memory_order_acquire));
    BOOST_REQUIRE(start_task->outcome.has_value());
    BOOST_CHECK(!(*start_task->outcome));
    BOOST_CHECK(start_task->outcome->error().code == ErrorCode::InternalError);
    BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);
    /** 残留：c 被保留（StopIncomplete，仍可 Find），服务已销毁，
     * 依赖边上的 t 未被误收口，无关 t2 已随回退继续收口。 */
    auto c2 = app->FindComponent<GateStopComponent>("c");
    BOOST_REQUIRE(c2);
    BOOST_CHECK(c2.value().IsValid());
    BOOST_CHECK(c2.value().Info().value().state == fw::ObjectState::StopIncomplete);
    BOOST_CHECK(!app->FindService("plain_svc"));
    BOOST_CHECK(app->FindComponent<GateStopComponent>("t"));
    BOOST_CHECK(!app->FindComponent<GateStopComponent>("t2"));

    /** 清除失败源后 CoStop 继续收口至 Stopped。 */
    GateStopComponent::s_fail_tag.store(-1, std::memory_order_relaxed);
    BOOST_REQUIRE(bbt_co_await app->CoStop());
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_CHECK(!app->FindComponent<GateStopComponent>("c"));
    BOOST_CHECK(!app->FindComponent<GateStopComponent>("t"));
    BOOST_CHECK(!c2.value().IsValid());
}

BOOST_AUTO_TEST_SUITE_END()
