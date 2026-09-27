#pragma once
// service-actor/v2 #8：资源关闭的控制线程桥接（机器面，internal/）。
//
// infra::ICoCloseable::WaitClosed 只在协程内合法；HostLifecycle 的关闭
// 序列与资源收束都跑在 CoApp 控制线程（run() 所在线程）。本函数做与
// InfraHttpHost::WaitClosed 同一形态的桥接：向 Scheduler 注册一次性
// 等待协程执行真实 WaitClosed，控制线程在条件变量上收结果；deadline/
// cancel 原样传入 infra，不放宽语义。
//
// 只桥接「等待收束」这一步；RequestClose（任意线程可调、幂等）由调用方
// 自己发起。Scheduler 停止排空时等待协程可能被整队丢弃而永不运行——
// 以活性复查退出返回 RuntimeUnavailable，不把控制线程永久挂住。

#include <memory>

#include <bbt/coroutine/sync/Cancellation.hpp>
#include <bbt/coroutine/sync/CompletionSignal.hpp>

#include <bbt/infra/ICoCloseable.hpp>

namespace bbt::framework::internal {

// 在控制线程等待 closeable 收束：内部注册一次性协程调用
// closeable.WaitClosed(deadline, cancel)，本线程阻塞至结果就绪。
// 目标已 IsClosed → 直接 Closed；协程无法注册或 Scheduler 不再驱动 →
// RuntimeUnavailable；其余状态原样返回（含 TimedOut/Cancelled）。
bbt::infra::CloseStatus WaitClosedOnControlThread(
    std::shared_ptr<bbt::infra::ICoCloseable> closeable,
    bbt::coroutine::Deadline deadline,
    bbt::coroutine::CancellationToken cancel);

} // namespace bbt::framework::internal
