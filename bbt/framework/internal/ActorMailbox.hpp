#pragma once
/**
 * @brief 按入队顺序逐个执行同一邮箱的任务。
 * 任务在锁外执行；即使它挂起，同一邮箱也不会开始下一项，其他邮箱仍可继续。
 * 容量只计算等待项，不含正在执行的任务。满时返回 Overloaded，不等待也不挤掉旧任务。
 * Close 后不再接收新任务，已入队的继续执行。队列和 m_draining 共用一把锁，
 * 保证最多只有一个协程取任务；该协程持有邮箱直到队列清空。
 */

#include <cstddef>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

class ActorMailbox : public std::enable_shared_from_this<ActorMailbox> {
public:
    using Task = std::function<void()>;
    using SPtr = std::shared_ptr<ActorMailbox>;
    /**
     * handler 异常观测口：框架保证异常不外溢、drain 继续推进；
     * 装配层可挂指标/日志。hook 自身异常同样吞掉，不反向影响队列。
     */
    using ErrorHook = std::function<void(std::exception_ptr)>;

    /**
     * mailbox_capacity 为等待队列上限（不含执行中项），须 > 0；
     * 语义由 ValidateServiceOptions 在装配期把关，此处不重复校验。
     */
    static SPtr Create(std::size_t mailbox_capacity,
                       ErrorHook on_abnormal = nullptr);

    /**
     * 接纳一条任务。按调用临界区先后定序；满 → err(Overloaded)；
     * 已 Close → err(Closed)；空任务 → err(InvalidArgument)；
     * 调度器不可用 → err(RuntimeUnavailable) 且不占有队列位。
     */
    result<void> TryEnqueue(Task task);

    /**
     * 仅停止接纳：已接纳的等待项仍由 drain 消费完（停机排空/取消语义
     * 由关闭序列负责，本组件不丢弃在途项）。
     */
    void Close();
    bool IsClosed() const;

    /**
     * 等待中项数（不含执行中）
     */
    std::size_t Pending() const;
    /**
     * mailbox_capacity
     */
    std::size_t Capacity() const;
    /**
     * 观测用：是否存在活跃 drain 协程
     */
    bool IsDraining() const;

private:
    ActorMailbox(std::size_t capacity, ErrorHook on_abnormal);
    /**
     * 唯一消费者，在协程上下文执行
     */
    void _Drain();

    mutable std::mutex m_mtx;
    std::deque<Task> m_queue;
    bool m_draining = false;
    bool m_closed = false;
    const std::size_t m_capacity;
    ErrorHook m_on_abnormal;
};

} // namespace bbt::framework
