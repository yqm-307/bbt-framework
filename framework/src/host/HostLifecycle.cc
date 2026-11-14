#include <bbt/framework/internal/HostLifecycle.hpp>

#include <algorithm>
#include <chrono>
#include <utility>

#include <boost/scope/scope_exit.hpp>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/framework/internal/CloseOptionsCheck.hpp>

namespace bbt::framework {

namespace {

namespace co = bbt::coroutine;
constexpr std::chrono::milliseconds kWaitSlice{50};

// 下一片等待的绝对时间点：min(deadline, now+50ms)；无期限时固定 50ms。
co::Deadline NextSlice(co::Deadline deadline) noexcept {
    const auto now = std::chrono::steady_clock::now();
    if (deadline == co::Deadline::max())
        return now + kWaitSlice;
    return std::min(deadline, now + kWaitSlice);
}

// 单次 Co 分片等待：Completed/TimedOut 用 bool 区分是否命中唤醒；
// AlreadyWaiting 表示同一等待位另有等待者，让出 5ms 后按未命中处理，由调用方
// 复查谓词。取消、非法域与登记失败如实映射为错误。
result<bool> WaitSlice(const std::shared_ptr<co::sync::CoWaiter>& signal,
                       co::Deadline until) {
    using WS = co::WaitStatus;
    if (!signal)
        return result<bool>::err(MakeError(ErrorCode::RuntimeUnavailable,
                                           "host lifecycle: wait signal missing"));
    co::WaitOptions options;
    options.deadline = until;
    switch (signal->Wait(options)) {
    case WS::Completed:
        return result<bool>::ok(true);
    case WS::TimedOut:
        return result<bool>::ok(false);
    case WS::AlreadyWaiting:
        bbtco_sleep(5);
        return result<bool>::ok(false);
    case WS::Cancelled:
        return result<bool>::err(MakeError(ErrorCode::Cancelled,
                                           "host lifecycle: wait cancelled"));
    case WS::InvalidContext:
        return result<bool>::err(MakeError(ErrorCode::InvalidContext,
                                           "host lifecycle: wait requires coroutine context"));
    case WS::RuntimeUnavailable:
        return result<bool>::err(MakeError(ErrorCode::RuntimeUnavailable,
                                           "host lifecycle: coroutine wait unavailable"));
    }
    return result<bool>::err(MakeError(ErrorCode::RuntimeUnavailable,
                                       "host lifecycle: coroutine wait unavailable"));
}

result<void> FailInvalid(std::string what) {
    return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                       std::move(what)));
}

} // namespace

HostLifecycle::HostLifecycle(std::shared_ptr<INetworkHost> host,
                             std::chrono::milliseconds step_budget)
    : m_host(std::move(host)),
      m_step_budget(step_budget) {}

result<void> HostLifecycle::_CheckCoContext(const char* entry) const {
    // 生命周期入口只在受管协程内执行：native 域没有「业务排空」的等待语义，
    // 在产生任何副作用前拒绝。
    if (!co::IsInCoroutineContext())
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
                                           std::string("host lifecycle: ") + entry +
                                               " requires a managed coroutine context"));
    return result<void>::ok();
}

bbt_co_async(result<void>) HostLifecycle::Start(const Hooks& hooks) {
    if (auto r = _CheckCoContext("Start"); !r)
        return r;
    if (!m_host)
        return FailInvalid("host lifecycle: network host is not configured");
    if (m_step_budget <= std::chrono::milliseconds{0})
        return FailInvalid("host lifecycle: step budget must be > 0");
    if (m_started.exchange(true, std::memory_order_acq_rel))
        return result<void>::err(MakeError(ErrorCode::Overloaded,
                                           "host lifecycle: start already attempted"));

    _SetPhase(Phase::Starting);
    // 固定顺序：装配钩子（资源/业务对象）→ 网络 Create → 网络 Start 等 Ready。
    // 失败一律按同一固定顺序回退，异常也收口为启动失败，不越过本入口。
    // 同一有限期限贯穿资源 CoStart 与网络 Start，不另设第二套预算。
    co::WaitOptions wait;
    wait.deadline = std::chrono::steady_clock::now() + m_step_budget;
    result<void> startup = result<void>::ok();
    try {
        if (hooks.on_scheduler_started)
            startup = hooks.on_scheduler_started(wait);
        if (startup) {
            if (auto created = m_host->Create(); !created) {
                startup = result<void>::err(std::move(created.error()));
            } else {
                m_host_created.store(true, std::memory_order_release);
                if (hooks.on_host_created) {
                    try {
                        hooks.on_host_created();
                    } catch (...) {
                        startup = result<void>::err(MakeError(
                            ErrorCode::InternalError,
                            "host engine start hook threw"));
                    }
                }
            }
        }
        if (startup) {
            startup = bbt_co_await m_host->Start(wait);
        }
    } catch (const std::exception& e) {
        startup = result<void>::err(MakeError(ErrorCode::InternalError,
                                              std::string("startup threw: ") + e.what()));
    } catch (...) {
        startup = result<void>::err(MakeError(ErrorCode::InternalError,
                                              "startup threw (non-std::exception)"));
    }
    if (!startup) {
        _AddFailure("startup: " + startup.error().message);
        _Rollback(hooks);
        _SetPhase(Phase::Failed);
        return startup;
    }

    _SetPhase(Phase::Running);
    return result<void>::ok();
}

void HostLifecycle::_Rollback(const Hooks& hooks) noexcept {
    // 启动中途失败：按固定顺序回退已立起的组件，不留半启动状态；资源收束由
    // on_release 兜底（资源 Close 幂等），网络按 Immediate 封口。
    if (m_host_created.load(std::memory_order_acquire) && m_host) {
        try {
            m_host->StopAccepting();
            bbt::infra::CloseOptions options = bbt::infra::CloseOptions::Immediate(
                MakeError(ErrorCode::Cancelled, "host lifecycle: startup failed"));
            const auto closed = bbt_co_await m_host->Close(options);
            (void)closed;
        } catch (...) {
            _AddFailure("rollback: host close threw");
        }
    }
    if (hooks.on_release) {
        try {
            hooks.on_release();
        } catch (...) {
            _AddFailure("rollback: on_release threw");
        }
    }
}

bbt_co_async(result<void>) HostLifecycle::Stop(
    const bbt::infra::CloseOptions& options, const Hooks& hooks) {
    if (auto r = _CheckCoContext("Stop"); !r)
        return r;
    if (!m_host)
        return FailInvalid("host lifecycle: network host is not configured");
    // 参数校验先于任何副作用：非法组合不得改变宿主排空/关闭状态。
    if (auto valid = bbt::framework::internal::ValidateCloseValue(options);
        !valid)
        return result<void>::err(std::move(valid.error()));
    return _Stop(options, hooks);
}

result<void> HostLifecycle::_Stop(const bbt::infra::CloseOptions& options,
                                  const Hooks& hooks) {
    // 宿主/hook 抛出的异常不得越过生命周期入口：收口未完成时保持
    // ShutdownIncomplete（owner/资源保留），异常收口为可观察失败项。
    try {
        return _StopInner(options, hooks);
    } catch (const std::exception& e) {
        _AddFailure(std::string("stop threw: ") + e.what());
        _EnterIncomplete("WaitHandlersDone");
        _SetPhase(Phase::ShutdownIncomplete);
        return result<void>::err(MakeError(ErrorCode::InternalError,
                                           std::string("stop threw: ") + e.what()));
    } catch (...) {
        _AddFailure("stop threw (non-std::exception)");
        _EnterIncomplete("WaitHandlersDone");
        _SetPhase(Phase::ShutdownIncomplete);
        return result<void>::err(MakeError(ErrorCode::InternalError,
                                           "stop threw (non-std::exception)"));
    }
}

result<void> HostLifecycle::_StopInner(const bbt::infra::CloseOptions& options,
                                       const Hooks& hooks) {
    _SetPhase(Phase::Stopping);
    bool interrupted = false;
    bool incomplete = false;
    result<void> first_error = result<void>::ok();
    // 只记录首个错误；后续失败仍记入 Failures 观察面。
    const auto remember = [&first_error](result<void>& slot, Error error) {
        if (slot)
            slot = result<void>::err(std::move(error));
    };

    if (m_host_created.load(std::memory_order_acquire)) {
        // 顺序：StopAccepting → 等排空 → 资源同步 Close → 网络 Co Close →
        // 等 PhysicalReleased。排空未被证实时不进入后三步：保留 owner/资源，
        // 记 StopIncomplete 并如实返回。
        m_host->StopAccepting();
        co::WaitOptions wait;
        wait.deadline = options.mode == bbt::infra::CloseMode::Graceful
                            ? options.deadline
                            : std::chrono::steady_clock::now() + m_step_budget;
        auto drained = bbt_co_await m_host->WaitHandlersDone(wait);
        if (!drained) {
            if (m_host->CloseReason()) {
                // Close 打断：handler 可能仍在借用业务对象，不发「已排空」
                // 信号（跳过资源收束挂接点），只走网络 Close 与释放。
                interrupted = true;
                _AddFailure("WaitHandlersDone: interrupted by host close");
                remember(first_error, *m_host->CloseReason());
            } else {
                const bool timed_out =
                    drained.error().code == ErrorCode::TimedOut;
                if (!timed_out) {
                    _AddFailure("WaitHandlersDone: " + drained.error().message);
                    remember(first_error, std::move(drained.error()));
                } else {
                    remember(first_error, MakeError(ErrorCode::TimedOut,
                                                    "host lifecycle: handlers not drained before deadline"));
                }
                _EnterIncomplete("WaitHandlersDone");
                _SetPhase(Phase::ShutdownIncomplete);
                return first_error;
            }
        } else {
            auto resources = _OnHandlersDrained(hooks, options);
            if (!resources) {
                _AddFailure("on_handlers_drained: " +
                            resources.error().message);
                remember(first_error, std::move(resources.error()));
                // F2：资源/组件收束失败不进入网络关闭与释放路径——owner、
                // 资源版本与未完成步骤保留，下一次显式 Stop 才用新预算重试；
                // 本次不换关闭模式、不执行 on_release。
                _EnterIncomplete("on_handlers_drained");
                _SetPhase(Phase::ShutdownIncomplete);
                return first_error;
            }
        }

        auto closed = bbt_co_await m_host->Close(options);
        if (!closed) {
            _AddFailure("host close: " + closed.error().message);
            remember(first_error, std::move(closed.error()));
            incomplete = true;
        }
        if (!incomplete) {
            auto released = bbt_co_await m_host->WaitPhysicalReleased(wait);
            if (!released) {
                _AddFailure("WaitPhysicalReleased: " +
                            released.error().message);
                remember(first_error, std::move(released.error()));
                incomplete = true;
            }
        }
    }

    if (incomplete) {
        // 失败/超时保留 owner 与未完成步骤；状态用 ShutdownIncomplete，
        // 使后续显式 CoStop 能真正重试，而不是被 Failed→Closed 短路。
        _EnterIncomplete("stop");
        _SetPhase(Phase::ShutdownIncomplete);
        return first_error;
    }

    if (hooks.on_release) {
        try {
            hooks.on_release();
        } catch (...) {
            _AddFailure("on_release hook threw");
        }
    }

    _LeaveIncomplete("WaitHandlersDone");
    _LeaveIncomplete("on_handlers_drained");
    _LeaveIncomplete("stop");
    // result 的显式 bool 表示「成功」，首个错误用 !first_error 判定。
    if (!first_error) {
        _SetPhase(interrupted ? Phase::Closed : Phase::Failed);
        return first_error;
    }
    _SetPhase(Phase::Closed);
    return result<void>::ok();
}

result<void> HostLifecycle::_OnHandlersDrained(
    const Hooks& hooks, const bbt::infra::CloseOptions& options) {
    if (!hooks.on_handlers_drained)
        return result<void>::ok();
    try {
        return hooks.on_handlers_drained(options);
    } catch (const std::exception& e) {
        return result<void>::err(MakeError(
            ErrorCode::InternalError,
            std::string("on_handlers_drained: ") + e.what()));
    } catch (...) {
        return result<void>::err(MakeError(
            ErrorCode::InternalError, "on_handlers_drained threw"));
    }
}

bbt_co_async(result<void>) HostLifecycle::WaitShutdownRequest(
    const bbt::coroutine::WaitOptions& wait) {
    if (auto r = _CheckCoContext("WaitShutdownRequest"); !r)
        return r;
    if (m_shutdown_req.load(std::memory_order_acquire))
        return result<void>::ok();

    auto signal = co::sync::CoWaiter::Create();
    {
        std::lock_guard<std::mutex> lk(m_waiter_mtx);
        if (m_waiter_taken)
            return result<void>::err(MakeError(ErrorCode::Overloaded,
                                               "host lifecycle: a shutdown waiter is already active"));
        m_waiter = signal;
        m_waiter_taken = true;
    }
    // 退出（含超时/取消）都必须归还唯一等待槽。
    auto release = boost::scope::make_scope_exit([this, &signal]() noexcept {
        std::lock_guard<std::mutex> lk(m_waiter_mtx);
        if (m_waiter == signal) {
            m_waiter.reset();
            m_waiter_taken = false;
        }
    });

    for (;;) {
        if (m_shutdown_req.load(std::memory_order_acquire))
            return result<void>::ok();
        const auto until = NextSlice(wait.deadline);
        auto woken = WaitSlice(signal, until);
        if (!woken)
            return result<void>::err(std::move(woken.error()));
        if (m_shutdown_req.load(std::memory_order_acquire))
            return result<void>::ok();
        if (wait.deadline != co::Deadline::max() &&
            std::chrono::steady_clock::now() >= wait.deadline)
            return result<void>::err(MakeError(ErrorCode::TimedOut,
                                               "host lifecycle: shutdown request not observed before deadline"));
    }
}

void HostLifecycle::RequestShutdown() noexcept {
    m_shutdown_req.store(true, std::memory_order_release);
    std::shared_ptr<co::sync::CoWaiter> signal;
    {
        std::lock_guard<std::mutex> lk(m_waiter_mtx);
        signal = m_waiter;
    }
    if (signal)
        signal->Notify();
}

HostLifecycle::Phase HostLifecycle::PhaseNow() const noexcept {
    return m_phase.load(std::memory_order_acquire);
}

ShutdownState HostLifecycle::State() const noexcept {
    ShutdownState s;
    switch (m_phase.load(std::memory_order_acquire)) {
    case Phase::Idle:
    case Phase::Starting:
    case Phase::Running:
        s = ShutdownState::Running;
        break;
    case Phase::Stopping:
        s = ShutdownState::Closing;
        break;
    case Phase::ShutdownIncomplete:
        s = ShutdownState::ShutdownIncomplete;
        break;
    case Phase::Closed:
    case Phase::Failed:
        s = ShutdownState::Closed;
        break;
    default:
        s = ShutdownState::Running;
        break;
    }
    return s;
}

std::vector<std::string> HostLifecycle::IncompleteSteps() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_incomplete;
}

std::vector<std::string> HostLifecycle::Failures() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_failures;
}

void HostLifecycle::_EnterIncomplete(const char* step) {
    std::lock_guard<std::mutex> lk(m_mtx);
    m_incomplete.emplace_back(step);
}

void HostLifecycle::_LeaveIncomplete(const char* step) {
    std::lock_guard<std::mutex> lk(m_mtx);
    for (auto it = m_incomplete.begin(); it != m_incomplete.end(); ++it) {
        if (*it == step) {
            m_incomplete.erase(it);
            break;
        }
    }
}

void HostLifecycle::_AddFailure(std::string what) {
    std::lock_guard<std::mutex> lk(m_mtx);
    m_failures.push_back(std::move(what));
}

void HostLifecycle::_SetPhase(Phase p) noexcept {
    m_phase.store(p, std::memory_order_release);
}

} // namespace bbt::framework
