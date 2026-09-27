#include <bbt/framework/internal/ResourceClose.hpp>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/object/CoObject.hpp>

namespace bbt::framework::internal {

namespace {

// 控制线程等待的上限粒度：谓词之外的活性复查（Scheduler 意外停止）按此
// 间隔重估，不是时序凑数。与 InfraHttpHost::WaitClosed 同口径。
constexpr std::chrono::milliseconds kRecheck{50};

} // namespace

bbt::infra::CloseStatus WaitClosedOnControlThread(
    std::shared_ptr<bbt::infra::ICoCloseable> closeable,
    bbt::coroutine::Deadline deadline,
    bbt::coroutine::CancellationToken cancel) {
    if (!closeable || closeable->IsClosed())
        return bbt::infra::CloseStatus::Closed;

    // 桥接：ICoCloseable::WaitClosed 只在协程内合法。注册一次性协程执行
    // 真实 WaitClosed（deadline/cancel 原样传递），控制线程经条件变量收
    // 结果。
    struct Slot {
        std::mutex                             m;
        std::condition_variable                cv;
        std::optional<bbt::infra::CloseStatus> st;
    };
    auto slot = std::make_shared<Slot>();
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [closeable, deadline, cancel, slot]() mutable {
            auto st = closeable->WaitClosed(deadline, std::move(cancel));
            {
                std::lock_guard<std::mutex> lk(slot->m);
                slot->st = st;
            }
            slot->cv.notify_all();
        },
        succ);
    if (!succ)
        return bbt::infra::CloseStatus::RuntimeUnavailable;

    std::unique_lock<std::mutex> lk(slot->m);
    while (!slot->st.has_value()) {
        // 协程在 Scheduler Stop 排空时可能被整队丢弃而永不运行；
        // 以活性复查退出，不把控制线程永久挂住。
        if (!g_scheduler->IsRunning())
            return bbt::infra::CloseStatus::RuntimeUnavailable;
        slot->cv.wait_for(lk, kRecheck);
    }
    return *slot->st;
}

} // namespace bbt::framework::internal
