#pragma once
// 入口宿主闸门（framework 入口 host gate）：进程单槽位，只承载一次运行。
//
// 主协程完成 Idle 阶段装配（宿主 Create 之后、等待 Ready 之前）把应用根持有的
// 共享 `net::Engine` 与 `Run` 输入发布给本闸门；应用入口的提交后 native host
// hook（`bbtco_main_host_hook`）调用 `RunHostGate`，在主线程对**同一** Engine
// 执行恰好一次 `Engine::Run`。业务协程不调用 `Run`，framework 不持有
// runner/thread/future/join。
//
// 两向退出：
//  - 主任务返回/异常 → runtime 兑现恰好一次 `OnMainTaskDone` → 本闸门对 Engine
//    调一次 `RequestClose`，`Run` 收口返回；
//  - `Run` 提前失败 → Engine 进入 `Stopped` 并保留 `LastError`：Ready 前失败由
//    主任务的 `Engine::CoWaitRunning` 如实返回，运行中失败由发布时登记的唤醒口
//    通知主任务自行收口。
//
// 本闸门不是通用管理器：一个槽位、一次发布、一次取走，没有队列/重试/注册表，
// 也不创建线程；主任务在发布前结束时不启动网络代，hook 不会永久阻塞。
//
// 用法（应用入口，见 `docs/architecture/framework-contract.md` §12.5/§13）：
// @code
//   int bbtco_main_host_hook(bbt::coroutine::HostHookContext& ctx) {
//       return bbt::framework::RunHostGate(ctx);
//   }
// @endcode

#include <functional>
#include <memory>
#include <mutex>

#include <bbt/coroutine/runtime/RuntimeMain.hpp>

#include <bbt/infra/net/Engine.hpp>

namespace bbt::framework {

/**
 * @brief 入口宿主闸门：共享 Engine 的唯一发布/执行点。
 *
 * @par 职责
 * 接收应用根发布的 `{engine, config, wake_main}`，并在入口 host hook 内对同一
 * Engine 执行一次 `Run`；主任务结束时请求关闭，`Run` 失败时唤醒主任务。
 *
 * @par 不负责
 * 不创建/销毁 Engine、不决定监听配置、不等待 Ready、不关闭组件或业务对象、
 * 不创建线程。
 *
 * @par 约束
 * 进程内单槽位、单次运行；`Publish` 只能由持有共享 Engine 的应用根在 Idle 装配
 * 完成后调用一次，`RunHostGate` 只能作为提交后 host hook 调用一次。
 */
class HostGate final {
public:
    /// @brief 发布内容：共享 Engine、其 `Run` 输入与主任务唤醒口。
    struct Publication {
        /// 应用根持有的共享 Engine（Idle 装配完成，未 Run）。
        std::shared_ptr<bbt::infra::net::Engine> engine;
        /// 该 Engine 的 `Run` 输入（监听地址/网络选项/推进方式）。
        bbt::infra::net::EngineConfig config;
        /// `Run` 提前失败时唤醒主任务（幂等、任意 native 线程可调用；为空表示
        /// 主任务只经 `CoWaitRunning` 观察失败）。
        std::function<void()> wake_main;
    };

    /**
     * @brief 发布共享 Engine（应用根在主协程内、宿主 Create 之后调用一次）。
     *
     * 槽位已取走（本进程已执行过 `Run`）时忽略本次发布——同一进程不做第二次
     * 运行；替换语义只对「尚未取走」的槽位成立。
     */
    static void Publish(Publication publication) noexcept;

    /// @brief 槽位是否有尚未取走的发布（测试与诊断观察，不参与运行路径）。
    static bool Pending() noexcept;

    /// @brief 观察当前待运行的发布副本（测试与诊断；不取走、不改变槽位）。
    static std::optional<Publication> Peek() noexcept;

    /**
     * @brief 入口 host hook 本体：执行一次 `Engine::Run`。
     *
     * @param ctx runtime 提供的提交后控制面；本函数在其上登记恰好一次
     *        `OnMainTaskDone`（→ `RequestClose`）。
     * @return 0 表示本代正常收口（含「主任务在发布前结束，未启动网络代」）；
     *         非零表示 `Run` 失败（退出码按失败上报）。
     */
    static int RunHostGate(bbt::coroutine::HostHookContext& ctx) noexcept;

    HostGate() = delete;
};

/// @brief 主任务唤醒口：`CoApp` 在析构时断开，闸门不会调用已销毁的 App。
///
/// `Wake` 与 `Detach` 在同一把锁下串行：`Detach` 返回后 `Wake` 一定是空操作，
/// 因此 App 可以在析构体里先断开再释放成员。
class HostGateWakeLink final {
public:
    /// @brief 安装唤醒动作（App 构造期调用一次；覆盖旧动作）。
    void Attach(std::function<void()> wake) noexcept;
    /// @brief 唤醒主任务；未安装或已断开时为空操作。
    void Wake() noexcept;
    /// @brief 断开（App 析构体首步）；阻塞到在途 `Wake` 返回后才返回。
    void Detach() noexcept;

private:
    std::mutex m_mtx;
    std::function<void()> m_wake;
};

} // namespace bbt::framework
