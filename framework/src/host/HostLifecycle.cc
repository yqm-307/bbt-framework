#include <bbt/framework/internal/HostLifecycle.hpp>

#include <utility>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/object/CoObject.hpp>

namespace bbt::framework {

HostLifecycle::HostLifecycle(std::shared_ptr<INetworkHost> host,
                             std::chrono::milliseconds step_budget,
                             const std::atomic_bool* external_request)
    : m_host(std::move(host)),
      m_step_budget(step_budget),
      m_external_request(external_request) {}

int HostLifecycle::Run(const Hooks& hooks) {
    if (m_started.exchange(true, std::memory_order_acq_rel))
        return kExitRejected;
    if (!m_host) {
        _AddFailure("network host is not configured");
        _SetPhase(Phase::Failed);
        return kExitRejected;
    }
    if (m_step_budget <= std::chrono::milliseconds{0}) {
        _AddFailure("step_budget must be > 0");
        _SetPhase(Phase::Failed);
        return kExitRejected;
    }

    _SetPhase(Phase::Starting);

    // 固定顺序第 1 步：Scheduler::Start。宿主以单例调度器为运行时；
    // 已由他人启动（generation != 0）时所有权不明，拒绝而不是叠加 Start。
    if (bbt::coroutine::CurrentRuntimeGeneration() != 0) {
        _AddFailure("scheduler already running: ownership ambiguous");
        _SetPhase(Phase::Failed);
        return kExitRejected;
    }
    g_scheduler->Start();

    if (auto r = _StartUp(hooks); !r) {
        _AddFailure("startup: " + r.error().message);
        // 启动中途失败：按同一固定顺序回退已立起的组件，不留半启动状态。
        (void)_ShutDown(hooks);
        _SetPhase(Phase::Failed);
        return kExitStartFailed;
    }

    _SetPhase(Phase::Running);
    _WaitShutdownRequest();
    const int rc = _ShutDown(hooks);
    _SetPhase(rc == kExitShutdownFailed ? Phase::Failed : Phase::Closed);
    return rc;
}

result<void> HostLifecycle::_StartUp(const Hooks& hooks) {
    // 服务绑定等装配动作：在 Scheduler::Start 之后、网络 Create 之前。
    if (hooks.on_scheduler_started)
        if (auto r = hooks.on_scheduler_started(); !r)
            return r;
    if (auto r = m_host->Create(); !r)
        return r;
    m_host_created.store(true, std::memory_order_release);
    if (auto r = m_host->Start(); !r)
        return r;
    return result<void>::ok();
}

void HostLifecycle::_WaitShutdownRequest() {
    if (m_shutdown_req.load(std::memory_order_acquire) ||
        (m_external_request != nullptr &&
         m_external_request->load(std::memory_order_acquire)))
        return;
    std::unique_lock<std::mutex> lk(m_mtx);
    // 谓词同时看自掛请求位与外部请求位；RequestShutdown 的 notify 不持锁，
    // 靠置位→唤醒→复查谓词保证不丢唤醒。
    m_cv.wait(lk, [this] {
        return m_shutdown_req.load(std::memory_order_acquire) ||
               (m_external_request != nullptr &&
                m_external_request->load(std::memory_order_acquire));
    });
}

int HostLifecycle::_ShutDown(const Hooks& hooks) {
    _SetPhase(Phase::Closing);

    if (m_host_created.load(std::memory_order_acquire)) {
        // 顺序：StopAccepting → 等 handler 结束 → RequestClose/WaitClosed
        // → 释放已关闭网络对象。任何一步超时都不跳过、不提前释放。
        m_host->StopAccepting();
        _WaitHandlersDone();
        m_host->RequestClose();
        _WaitClosed();
        m_host->ReleaseClosed();
    }
    // 业务对象回收与「释放已关闭网络对象」同组，均在 Scheduler::Stop 前。
    if (hooks.on_release) {
        try {
            hooks.on_release();
        } catch (...) {
            _AddFailure("on_release hook threw");
        }
    }
    g_scheduler->Stop();

    {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (!m_failures.empty())
            return kExitShutdownFailed;
    }
    return m_exceeded_budget.load(std::memory_order_acquire)
               ? kExitShutdownLate
               : kExitOk;
}

void HostLifecycle::_WaitHandlersDone() {
    auto r = m_host->WaitHandlersDone(_StepDeadline(), m_close_cancel.Token());
    if (r)
        return;
    if (r.error().code == ErrorCode::TimedOut) {
        // 预算耗尽：标记 ShutdownIncomplete 后以无界期限续等——期限约束的
        // 是优雅关闭是否成功，不是进程退出的硬时限；不得跳过释放仍在被
        // 回调访问的资源。
        _EnterIncomplete("WaitHandlersDone");
        r = m_host->WaitHandlersDone(bbt::coroutine::Deadline::max(),
                                     m_close_cancel.Token());
        _LeaveIncomplete("WaitHandlersDone");
        if (r) {
            m_exceeded_budget.store(true, std::memory_order_release);
            return;
        }
    }
    _AddFailure("WaitHandlersDone: " + r.error().message);
}

void HostLifecycle::_WaitClosed() {
    auto st = m_host->WaitClosed(_StepDeadline(), m_close_cancel.Token());
    if (st == bbt::infra::CloseStatus::Closed)
        return;
    if (st == bbt::infra::CloseStatus::TimedOut) {
        _EnterIncomplete("WaitClosed");
        st = m_host->WaitClosed(bbt::coroutine::Deadline::max(),
                                m_close_cancel.Token());
        _LeaveIncomplete("WaitClosed");
        if (st == bbt::infra::CloseStatus::Closed) {
            m_exceeded_budget.store(true, std::memory_order_release);
            return;
        }
    }
    _AddFailure("WaitClosed: status " + std::to_string(static_cast<int>(st)));
}

bbt::coroutine::Deadline HostLifecycle::_StepDeadline() const {
    return std::chrono::steady_clock::now() + m_step_budget;
}

void HostLifecycle::_EnterIncomplete(const char* step) {
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_incomplete.emplace_back(step);
    }
    _SetPhase(Phase::ShutdownIncomplete);
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
    ShutdownState s;
    switch (p) {
    case Phase::Closing:            s = ShutdownState::Closing; break;
    case Phase::ShutdownIncomplete: s = ShutdownState::ShutdownIncomplete; break;
    case Phase::Closed:
    case Phase::Failed:             s = ShutdownState::Closed; break;
    default:                        s = ShutdownState::Running; break;
    }
    m_state.store(s, std::memory_order_release);
}

void HostLifecycle::RequestShutdown() noexcept {
    m_shutdown_req.store(true, std::memory_order_release);
    m_cv.notify_all();
}

HostLifecycle::Phase HostLifecycle::PhaseNow() const noexcept {
    return m_phase.load(std::memory_order_acquire);
}

ShutdownState HostLifecycle::State() const noexcept {
    return m_state.load(std::memory_order_acquire);
}

std::vector<std::string> HostLifecycle::IncompleteSteps() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_incomplete;
}

std::vector<std::string> HostLifecycle::Failures() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_failures;
}

} // namespace bbt::framework
