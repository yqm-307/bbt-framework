#include <bbt/framework/internal/ActorMailbox.hpp>

#include <utility>

#include <bbt/coroutine/coroutine.hpp>

namespace bbt::framework {

ActorMailbox::SPtr ActorMailbox::Create(std::size_t mailbox_capacity,
                                        ErrorHook on_abnormal) {
    return SPtr(new ActorMailbox(mailbox_capacity, std::move(on_abnormal)));
}

ActorMailbox::ActorMailbox(std::size_t capacity, ErrorHook on_abnormal)
    : m_capacity(capacity), m_on_abnormal(std::move(on_abnormal)) {}

result<void> ActorMailbox::TryEnqueue(Task task) {
    if (!task)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "ActorMailbox::TryEnqueue: empty task"));

    std::lock_guard<std::mutex> lock(m_mtx);
    if (m_closed)
        return result<void>::err(MakeError(ErrorCode::Closed,
            "ActorMailbox is closed"));
    if (m_queue.size() >= m_capacity)
        return result<void>::err(MakeError(ErrorCode::Overloaded,
            "ActorMailbox waiting queue full (mailbox_capacity)"));
    m_queue.push_back(std::move(task));
    if (m_draining)
        return result<void>::ok();

    // 空转→活跃边沿：注册本邮箱唯一 drain 协程。注册放在持锁临界区内，
    // 保证 m_draining 置位先于协程起跑被观测；注册失败回滚本次接纳
    // （刚 push 的项仍在队尾、draining 尚未置位，pop_back 即收回），
    // 不留孤儿项、不占用容量。
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [self = shared_from_this()]() { self->_Drain(); }, succ);
    if (!succ) {
        m_queue.pop_back();
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "ActorMailbox: coroutine scheduler unavailable"));
    }
    m_draining = true;
    return result<void>::ok();
}

void ActorMailbox::Close() {
    std::lock_guard<std::mutex> lock(m_mtx);
    m_closed = true;
}

bool ActorMailbox::IsClosed() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_closed;
}

std::size_t ActorMailbox::Pending() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_queue.size();
}

std::size_t ActorMailbox::Capacity() const {
    return m_capacity;
}

bool ActorMailbox::IsDraining() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_draining;
}

void ActorMailbox::_Drain() {
    // 唯一消费者：m_draining 保证同一时刻至多一个 _Drain 协程。
    // 每条 task 内联在本协程执行——task 挂起（等待远端/依赖项）时本协程
    // 随之挂起，worker 被让出，但执行资格（m_draining）仍被持有，
    // 下一条不会被取出。handler 正常或异常退出后才取下一条。
    for (;;) {
        Task task;
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_queue.empty()) {
                // 释放执行资格：此后首个 TryEnqueue 会注册新 drain。
                m_draining = false;
                return;
            }
            task = std::move(m_queue.front());
            m_queue.pop_front();
        }
        try {
            task();
        } catch (...) {
            if (m_on_abnormal) {
                try {
                    m_on_abnormal(std::current_exception());
                } catch (...) {
                }
            }
        }
    }
}

} // namespace bbt::framework
