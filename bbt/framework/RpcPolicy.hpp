#pragma once
/** @brief 对经过路由解析的受管出站调用做只读策略判断。 */

#include <optional>
#include <string>

#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/infra/rpc/Request.hpp>

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

/**
 * 受管出站调用的只读事实视图：已解析目标、请求 envelope（五字段 + payload +
 * metadata 只读）、已适配的绝对期限（父预算取 min 后的有效值，循环上界）。
 */
struct RpcCallView {
    const std::string& target;
    const bbt::infra::rpc::Request& request;
    bbt::coroutine::Deadline deadline;
};

/**
 * 策略结论：Allow（放行，可选收紧期限）或 Reject（拒绝，零 I/O）。
 */
struct RpcPolicyDecision {
    enum class Kind {
        Allow,
        Reject
    };

    Kind kind = Kind::Allow;

    /**
     * 仅当 kind==Allow 且已设置时生效；框架取 min(原期限, 本值)，绝不放大。
     * 收紧值 ≤ now → 框架在发起任何 I/O 之前按 TimedOut 拒绝。
     */
    std::optional<bbt::coroutine::Deadline> tightened_deadline;

    /**
     * kind==Reject 时回给调用方的稳定错误（经框架错误边界上报）。
     */
    Error reject_error;
};

/**
 * 公开出站策略抽象：可继承实现，也可组合包装（策略包策略 = 装饰器）。
 */
class RpcPolicy {
public:
    virtual ~RpcPolicy() = default;

    /**
     * 静态或动态路由解析成功后、发送前调用一次。解析失败不进入策略。
     * 在发起调用的协程内执行，同一实例可被并发调用。实现须线程安全，
     * 不得阻塞、执行 I/O 或重入 CoApp/服务。视图中的引用仅此次调用有效。
     * 策略错误或拒绝直接返回调用方。允许时只能缩短期限，过期返回 TimedOut。
     */
    virtual result<RpcPolicyDecision> Preflight(const RpcCallView& call) = 0;
};

} // namespace bbt::framework
