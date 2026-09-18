#pragma once
// co-service-actor/v1 F1-b1：宿主生命周期状态机与网络宿主注入点。
// 落实契约 F3（decisions/0001 第 231/233/235 行）的固定资源顺序：
//   Scheduler::Start → 网络组件 Create/Start（开始接纳）→ 运行
//   → StopAccepting → 等 handler 结束 → RequestClose/WaitClosed
//   → 释放已关闭网络对象（及业务对象）→ Scheduler::Stop → run 返回。
// 每一步等待有有限预算；预算耗尽进入 ShutdownIncomplete——控制线程仍在
// Run 内继续等待迟到收尾，不提前走「释放/Stop/返回」三步，不假装成功，
// 不 _Exit/abort、不强杀线程。迟到收尾最终完成后照常走完整收束，
// Run 返回非零（kExitShutdownLate）。
//
// 可测性：网络组件的创建/启动/停止接纳/等待排空/关闭/等待收束/释放全部
// 经过 INetworkHost 抽象接口（注入点），测试可用桩组件断言调用顺序、
// 异常路径与超时路径，不需要真实 socket。真实 HTTP 适配属 F1-b2。

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <bbt/coroutine/sync/Cancellation.hpp>
#include <bbt/coroutine/sync/CompletionSignal.hpp>

#include <bbt/infra/ICoCloseable.hpp>

#include <bbt/framework/Result.hpp>
#include <bbt/framework/ShutdownState.hpp>

namespace bbt::framework {

// INetworkHost：CoApp 的网络依赖组件（契约第 137 行——Server/Client/连接池
// 由它拥有，不是 Service 的父类）。全部方法在宿主控制线程（Run 所在线程）
// 调用；实现不得依赖「运行在协程内」。真实实现（F1-b2）在内部桥接 infra
// NetworkRuntime/HttpServer 的协程语义；桩实现用于断言顺序与异常路径。
//
// 实现约束：
//  - Create 成功后，无论 Start 是否成功，关闭序列（StopAccepting →
//    WaitHandlersDone → RequestClose → WaitClosed → ReleaseClosed）必须
//    安全可调（未启动部分视为空操作）；
//  - WaitHandlersDone/WaitClosed 在 deadline 耗尽时必须返回超时
//    （TimedOut / CloseStatus::TimedOut），不得无限阻塞；宿主在预算耗尽后
//    会以 Deadline::max() 再次调用同一入口等待迟到收尾；
//  - StopAccepting/RequestClose/ReleaseClosed 幂等、noexcept。
class INetworkHost {
public:
    virtual ~INetworkHost() = default;

    // 创建网络组件（真实实现对应 NetworkRuntime::Create + 监听装配）。
    virtual result<void> Create() = 0;
    // 启动组件并开始接纳。同一实例至多成功一次。
    virtual result<void> Start() = 0;
    // 停止新入站接纳；不取消已接纳 handler、不断开其回复路径。
    virtual void StopAccepting() noexcept = 0;
    // 等已接纳 handler 结束（排空/取消）。超时 → err(TimedOut)。
    virtual result<void> WaitHandlersDone(
        bbt::coroutine::Deadline deadline,
        bbt::coroutine::CancellationToken cancel) = 0;
    // 请求关闭整个网络组件。幂等。
    virtual void RequestClose() noexcept = 0;
    // 等待组件收束。语义与 infra::ICoCloseable::WaitClosed 相同，
    // 但在控制线程调用（真实实现内部桥接协程等待）。
    virtual bbt::infra::CloseStatus WaitClosed(
        bbt::coroutine::Deadline deadline,
        bbt::coroutine::CancellationToken cancel) = 0;
    // 释放已关闭的网络对象。仅在 WaitClosed 报告 Closed 后调用。
    virtual void ReleaseClosed() noexcept = 0;
};

// HostLifecycle：把启动/关闭顺序实现为可断言状态机。单实例、单线程使用
// （Run 在控制线程调用；RequestShutdown/State/IncompleteSteps 可由外部
// 监督线程调用）。
class HostLifecycle {
public:
    // 比契约枚举更细的可观测相位；State() 归并到契约四态。
    // Failed：启动失败已回退、或关闭收尾存在硬性失败项；对外归并为 Closed
    // （不再运行），真实原因经 Failures()/IncompleteSteps() 与返回码表达。
    enum class Phase {
        Idle,
        Starting,
        Running,
        Closing,
        ShutdownIncomplete,
        Closed,
        Failed,
    };

    // Run 返回码（CoApp::run 沿用同一组）。
    static constexpr int kExitOk             = 0;  // 按期优雅关闭
    static constexpr int kExitRejected       = 1;  // 前置/校验失败，未启动任何组件
    static constexpr int kExitStartFailed    = 2;  // 启动中途失败，已按序回退
    static constexpr int kExitShutdownLate   = 3;  // 关闭曾超预算，迟到收尾已全部完成
    static constexpr int kExitShutdownFailed = 4;  // 关闭收尾存在失败项

    // 启动/关闭期挂接点（CoApp 用）：
    //  - on_scheduler_started：Scheduler::Start 之后、网络 Create 之前执行
    //    （服务身份绑定等）；失败按启动失败回退；
    //  - on_release：网络对象释放之后、Scheduler::Stop 之前执行
    //    （业务对象回收）；不得抛异常。
    struct Hooks {
        std::function<result<void>()> on_scheduler_started;
        std::function<void()>         on_release;
    };

    // step_budget：每个等待步的有限预算（WaitHandlersDone/WaitClosed 各自
    // 获得一次 now()+step_budget 的期限）。external_request 为可选外部
    // 关闭请求位（CoApp::request_shutdown 的早于 Run 的调用经它传达）；
    // 非空时其置位与 RequestShutdown 等效。
    HostLifecycle(std::shared_ptr<INetworkHost> host,
                  std::chrono::milliseconds step_budget,
                  const std::atomic_bool* external_request = nullptr);

    // 控制线程入口：执行完整生命周期。至多重入一次（重复调用 → kExitRejected）。
    // 阻塞至关闭请求到来并完成收束；ShutdownIncomplete 期间不返回。
    int Run(const Hooks& hooks);

    // 幂等、noexcept：请求关闭。可在 Run 之前/之中由任意线程调用。
    void RequestShutdown() noexcept;

    Phase         PhaseNow() const noexcept;
    ShutdownState State() const noexcept;
    // 曾进入且尚未完成的清理步名（可观察未完成项）。
    std::vector<std::string> IncompleteSteps() const;
    // 收尾失败步名与原因（预算超时不算失败，算 kExitShutdownLate）。
    std::vector<std::string> Failures() const;

private:
    result<void> _StartUp(const Hooks& hooks);
    void         _WaitShutdownRequest();
    int          _ShutDown(const Hooks& hooks);   // 返回 kExit*，固定顺序收束
    void         _WaitHandlersDone();             // 预算 + 迟到续等
    void         _WaitClosed();                   // 同上（CloseStatus 形态）
    bbt::coroutine::Deadline _StepDeadline() const;
    void         _EnterIncomplete(const char* step);
    void         _LeaveIncomplete(const char* step);
    void         _AddFailure(std::string what);
    void         _SetPhase(Phase p) noexcept;

    std::shared_ptr<INetworkHost>      m_host;
    const std::chrono::milliseconds    m_step_budget;
    const std::atomic_bool* const      m_external_request;

    std::atomic_bool                   m_started{false};
    std::atomic_bool                   m_shutdown_req{false};
    std::atomic_bool                   m_host_created{false};
    std::atomic_bool                   m_exceeded_budget{false};
    std::atomic<Phase>                 m_phase{Phase::Idle};
    std::atomic<ShutdownState>         m_state{ShutdownState::Running};

    bbt::coroutine::CancellationSource m_close_cancel;

    mutable std::mutex                 m_mtx;      // 仅护未完成项/失败清单与 CV
    std::condition_variable            m_cv;
    std::vector<std::string>           m_incomplete;
    std::vector<std::string>           m_failures;
};

} // namespace bbt::framework
