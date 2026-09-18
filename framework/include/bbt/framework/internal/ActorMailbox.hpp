#pragma once
// co-service-actor/v1 F2-a：Actor 邮箱——单 Actor 的串行执行资格 + 有界等待队列。
//
// 语义（契约 §F2 保序承诺，第 173-175 行）：
//  - 接纳顺序：TryEnqueue 成功的项按接纳先后排队；容量仅计等待中项，
//    不含执行中项；满则拒绝并返回 Overloaded——不排队等待、不淘汰已接纳项。
//  - 非重入：同一邮箱同一时刻至多一个 handler 在执行；前一条 handler
//    正常或异常退出后才取下一条。
//  - handler 在邮箱的 drain 协程内联执行：handler 挂起等待远端/被依赖项时
//    让出 worker，但 drain 协程仍持有本邮箱的业务执行资格，下一条不会被取出。
//  - 不绑定原生线程；不同邮箱的 drain 协程相互独立，可并发推进。
//
// 复用与自兜边界（不从组件名推断保证）：
//  - g_scheduler->RegistCoroutineTask：仅提供「把 drain 闭包交给某个 worker
//    协程执行」，不承诺同一闭包不被并发执行。唯一消费者由本类自保证：
//    m_draining 占位标志与队列共用 m_mtx，空转→活跃边沿至多注册一个 drain。
//  - 销毁竞争：drain 闭包按值捕获本对象 shared_ptr，协程存活期间邮箱不死；
//    邮箱析构不发生在 drain 运行中途。Scheduler::Stop 遗弃未跑完的协程时
//    随闭包一起释放引用，不解引用。
//  - 队列与容量用 std::deque + std::mutex 自建：coroutine 的 Chan<T,Max>
//    容量是模板常量，放不下运行期 mailbox_capacity；且其读端挂起语义
//    （m_is_reading 单读者）虽同形，本类需要在持锁临界区内完成「满则拒绝
//    + 边沿注册」的原子判定，故不套接。

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
    // handler 异常观测口：框架保证异常不外溢、drain 继续推进；
    // 装配层可挂指标/日志。hook 自身异常同样吞掉，不反向影响队列。
    using ErrorHook = std::function<void(std::exception_ptr)>;

    // mailbox_capacity 为等待队列上限（不含执行中项），须 > 0；
    // 语义由 ValidateServiceOptions 在装配期把关，此处不重复校验。
    static SPtr Create(std::size_t mailbox_capacity,
                       ErrorHook on_abnormal = nullptr);

    // 接纳一条任务。按调用临界区先后定序；满 → err(Overloaded)；
    // 已 Close → err(Closed)；空任务 → err(InvalidArgument)；
    // 调度器不可用 → err(RuntimeUnavailable) 且不占有队列位。
    result<void> TryEnqueue(Task task);

    // 仅停止接纳：已接纳的等待项仍由 drain 消费完（v1 的停机排空/取消
    // 属 F3 语义，本切片不丢弃在途项）。
    void Close();
    bool IsClosed() const;

    std::size_t Pending() const;   // 等待中项数（不含执行中）
    std::size_t Capacity() const;  // mailbox_capacity
    bool IsDraining() const;       // 观测用：是否存在活跃 drain 协程

private:
    ActorMailbox(std::size_t capacity, ErrorHook on_abnormal);
    void _Drain();  // 唯一消费者，在协程上下文执行

    mutable std::mutex m_mtx;
    std::deque<Task> m_queue;
    bool             m_draining = false;
    bool             m_closed   = false;
    const std::size_t m_capacity;
    ErrorHook        m_on_abnormal;
};

} // namespace bbt::framework
