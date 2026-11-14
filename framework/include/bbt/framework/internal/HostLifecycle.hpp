#pragma once
// Co 宿主生命周期状态机与网络宿主注入点（机器面，internal/）。
//
// 固定资源顺序（本仓验收矩阵与 HostLifecycle 实现为真源）：
//   coroutine runtime 已由 runtime main 启动（本状态机不 Start/不 Stop runtime）
//   → 资源创建/启动 + 业务对象绑定（on_scheduler_started）
//   → 网络组件 Create（Engine Idle 装配）→ 网络 Start 并经
//     Engine::CoWaitRunning 等入口 host hook 在主线程执行的 Run 发布 Ready
//     （开始接纳）→ 运行
//   → StopAccepting（封业务门）→ 等 handler 排空 → on_handlers_drained
//     （调用方自己的收束挂接点）→ 网络 Co Close
//   → 等 PhysicalReleased → 释放业务对象。
//
// 全部步骤在调用方的受管协程内执行：等待一律 CoWaiter + 持久谓词（50ms 分片由
// 各等待点实现），不建 control/reaper 线程，不使用 std::thread / future /
// promise / condition_variable，也不在业务路径阻塞 OS 线程。Engine 的 native
// 执行只由应用入口 host hook 调一次 `Engine::Run`；本状态机只观察
// `CoWaitRunning`/`Phase()`，不持有 runner/线程/join。
//
// 关闭语义（诚实终态）：CoStop 用调用方给的同一绝对期限收口；排空未被证实
// （期限耗尽）时进入 StopIncomplete、保留 owner/资源并如实返回 TimedOut，
// 允许再次以更晚期限调用推进。首因固定、步骤幂等；不发「已排空」信号，也不把
// 网络 Close 的成功当作业务排空。
//
// 关闭打断：网络宿主已发布关闭原因（INetworkHost::CloseReason）时，排空立即
// 结束且不进入资源收束挂接点（业务 handler 可能仍借用业务对象），只走网络
// Close 与释放，记失败项并如实返回原因——这不是「排空被证实」。
//
// 可测性：宿主组件的全部步骤经过 INetworkHost 抽象接口（注入点），测试可用
// Co 桩组件断言调用顺序与失败路径，不需要真实 socket；真实 HTTP/RPC 适配见
// InfraHttpHost。

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

#include <bbt/infra/CloseOptions.hpp>

#include <bbt/framework/Result.hpp>
#include <bbt/framework/ShutdownState.hpp>

namespace bbt::framework {

// INetworkHost：CoApp 的网络依赖组件（Engine/Server/Client 由它拥有，不是
// Service 的父类）。全部方法只允许在宿主控制协程（co_start/co_stop 调用者）
// 内调用；实现不得依赖「运行在 native 控制线程上」。真实实现在内部消费
// infra net::Engine（CoWaitRunning/Phase/物理凭据）/ http::Server /
// rpc::CoRpcServer 的 Co 面；
// 桩实现用于断言顺序与失败路径。
//
// 实现约束：
//  - Create 成功后，无论 Start 是否成功，关闭序列（StopAccepting →
//    WaitHandlersDone → Close → WaitPhysicalReleased）必须安全可调（未启动
//    部分视为空操作）；
//  - Start/WaitHandlersDone/Close/WaitPhysicalReleased 先做 Co 域 gate：非受管
//    协程返回 err(InvalidContext)，且无副作用；等待用 CoWaiter 与持久谓词按
//    min(deadline, now+50ms) 分片，绝不退回 native CV；
//  - WaitHandlersDone 在期限耗尽返回 TimedOut，不得无界阻塞；非预算性错误按
//    err(其它 code) 如实返回，宿主不得把任何错误当作「已排空」；
//  - StopAccepting 幂等、noexcept（只封业务门，不等在途）；
//  - Close 是逻辑封口 + 首因发布（幂等、不可重开），不证明物理释放；
//    WaitPhysicalReleased 才是物理凭据。
class INetworkHost {
public:
    virtual ~INetworkHost() = default;

    // 创建网络组件（真实实现对应 Engine Idle 装配 + listener 安装）。
    virtual result<void> Create() = 0;
    // 启动并等待真实 Ready（真实实现经 Engine::CoWaitRunning；Run 由应用
    // 入口 host hook 执行一次）。
    virtual bbt_co_async(result<void>) Start(
        const bbt::coroutine::WaitOptions& wait) = 0;
    // 停止新入站接纳；不取消已接纳 handler、不断开其回复路径。
    virtual void StopAccepting() noexcept = 0;
    // 等已接纳 handler 结束（排空）。超时 → err(TimedOut)；宿主已被
    // Close 打断 → 立即返回关闭原因，不等满期限。
    virtual bbt_co_async(result<void>) WaitHandlersDone(
        const bbt::coroutine::WaitOptions& wait) = 0;
    // 逻辑封口并发布首因；不等待业务 handler 返回，也不等物理释放。
    virtual bbt_co_async(result<void>) Close(
        const bbt::infra::CloseOptions& options) = 0;
    // 等本代网络资源与 native runner 责任都收口（infra 物理凭据）。
    virtual bbt_co_async(result<void>) WaitPhysicalReleased(
        const bbt::coroutine::WaitOptions& wait) = 0;
    // Close 发布的原因；未 Close 为空。HostLifecycle 用「原因是否存在」这个
    // 明确状态判断「排空被关闭打断」，不按具体 ErrorCode 猜测。
    virtual std::optional<bbt::infra::Error> CloseReason() const = 0;
};

// HostLifecycle：把启动/关闭顺序实现为可断言的 Co 状态机。单实例、单控制
// 协程使用（Start/Stop 在同一 owner 协程序列调用）；RequestShutdown/State 可由
// 任意线程观察。
class HostLifecycle {
public:
    // 比对外枚举更细的可观测相位；State() 归并到 ShutdownState 四态。
    // Failed：启动失败已回退、或关闭收尾存在硬性失败项；对外归并为 Closed。
    enum class Phase {
        Idle,
        Starting,
        Running,
        Stopping,
        ShutdownIncomplete,
        Closed,
        Failed,
    };

    // 启动/关闭期挂接点（CoApp 用）：
    //  - on_scheduler_started：网络 Create 之前执行（资源创建/启动 + 业务身份
    //    绑定等）；失败按启动失败回退。wait 是本次 Start 的有限期限，资源
    //    CoStart 必须消费同一预算，不另设第二套；
    //  - on_handlers_drained：StopAccepting → WaitHandlersDone 之后、网络
    //    Close 之前执行（资源收束）。仅在排空被证实时调用；CloseOptions 与
    //    本次收口相同，失败如实返回而不是吞掉；不得抛异常（抛出按失败项记录）；
    //  - on_release：网络 Close 之后执行（业务对象回收 + 启动失败回退时的资源
    //    收束兜底）。不得抛异常。
    struct Hooks {
        std::function<result<void>(const bbt::coroutine::WaitOptions&)>
            on_scheduler_started;
        // 网络 Create 成功后、Start 之前执行（测试/入口在宿主原生线程启动共享
        // Engine 的等价 host hook）；不得抛异常。
        std::function<void()> on_host_created;
        std::function<result<void>(const bbt::infra::CloseOptions&)>
            on_handlers_drained;
        std::function<void()> on_release;
    };

    // step_budget：单次 Co Stop 未显式给期限时的有限上限（默认收口预算）。
    HostLifecycle(std::shared_ptr<INetworkHost> host,
                  std::chrono::milliseconds step_budget);

    // 控制协程入口：装配 → Start → 等 Ready。已在启动/运行/已关闭 → err(Busy/
    // Closed)。启动失败按固定顺序回退并返回原错误（不留半启动状态）。
    bbt_co_async(result<void>) Start(const Hooks& hooks);

    // 控制协程入口：按 options 的绝对期限收口。Graceful 必须给有限期限；
    // 排空未被证实 → ShutdownIncomplete + err(TimedOut)，owner/资源保留。
    bbt_co_async(result<void>) Stop(const bbt::infra::CloseOptions& options,
                                    const Hooks& hooks);

    // 单一 shutdown-request 等待槽：持久请求谓词，先请求后等待不丢；第二个
    // 活动 waiter → err(Busy)；期限耗尽 → err(TimedOut) 且不清请求。
    bbt_co_async(result<void>) WaitShutdownRequest(
        const bbt::coroutine::WaitOptions& wait);

    // 幂等、noexcept：请求关闭。可在任意线程/域调用；只置位并唤醒等待槽，
    // 不执行 hook、不调用 Stop、不等待自身。
    void RequestShutdown() noexcept;

    Phase PhaseNow() const noexcept;
    ShutdownState State() const noexcept;
    // 曾进入且尚未完成的清理步名（可观察未完成项）。
    std::vector<std::string> IncompleteSteps() const;
    // 收尾失败步名与原因（期限耗尽不算失败，按 TimedOut 返回）。
    std::vector<std::string> Failures() const;

private:
    result<void> _CheckCoContext(const char* entry) const;
    result<void> _StartUp(const Hooks& hooks);
    result<void> _Stop(const bbt::infra::CloseOptions& options,
                       const Hooks& hooks);
    // 收口实现（不含异常边界）：_Stop 把宿主/hook 抛出的异常收口为可观察失败项。
    result<void> _StopInner(const bbt::infra::CloseOptions& options,
                            const Hooks& hooks);
    // 启动失败回退：固定顺序、Immediate 封口、不掩盖原错误。
    void _Rollback(const Hooks& hooks) noexcept;
    result<void> _OnHandlersDrained(const Hooks& hooks,
                                    const bbt::infra::CloseOptions& options);
    bool _CoPending() const noexcept;
    void _EnterIncomplete(const char* step);
    void _LeaveIncomplete(const char* step);
    void _AddFailure(std::string what);
    void _SetPhase(Phase p) noexcept;

    const std::shared_ptr<INetworkHost> m_host;
    const std::chrono::milliseconds m_step_budget;

    // 请求位与等待槽：位是持久事实，槽只容纳一个活动 waiter。
    std::atomic_bool m_shutdown_req{false};
    mutable std::mutex m_waiter_mtx;
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> m_waiter;
    bool m_waiter_taken{false};

    std::atomic<bool> m_started{false};
    std::atomic<bool> m_host_created{false};
    std::atomic<Phase> m_phase{Phase::Idle};

    mutable std::mutex m_mtx; // 仅护未完成项/失败清单
    std::vector<std::string> m_incomplete;
    std::vector<std::string> m_failures;
};

} // namespace bbt::framework
