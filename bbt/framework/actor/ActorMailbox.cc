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

    // 持锁注册唯一 drain，使它取队前看到 m_draining。注册失败时只回滚队尾。
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
    // task 挂起时让出 worker，但仍持有执行资格，直到 task 返回或抛出。
    for (;;) {
        Task task;
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_queue.empty()) {
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
