#pragma once
// service-actor/v2 / R8（3-5）：公开出站 RPC 策略（受控虚拟点）。
//
// 只暴露「只读调用事实 + Allow/Reject + 只收紧期限」这一可被框架验证的窄面：
// 不交出发送实现、可变 envelope、codec、路由目标选择或 infra 内部类型。策略
// 因此既不能自组 wire/HTTP，也不能绕过 owner/限额/关联/Close 契约。若产品要把
// 「任意 next 包装 / 替换发送」做成公开入口，属超出本点 design §3 口径的范围
// 变更，须先修订 design/proposal——本头不提供该面。
//
// 调用契约（不可放宽）：
//  - 位置：每次受管出站调用在静态路由授权（CoApp::find_route）之后、发送之前
//    被调用一次；未命中静态路由的目标不会询问策略。
//  - 线程：在调用发起协程内执行；Concurrent 服务可并发出站，故同一策略实例
//    会被并发调用，实现必须线程安全。
//  - 禁止：不得阻塞、不得发起 I/O、不得回调 CoApp/服务（避免重入）。
//  - 只读：target/request 为本次调用的只读引用，生命周期仅限本次调用，不得保存。
//  - 期限：只能收紧（框架取 min），不得放大；收紧到已过期 → 框架在发起任何 I/O
//    之前按 TimedOut 拒绝。
//  - 失败：返回 err → 按框架既有错误边界上报，与今天的 egress 失败同形。

#include <optional>

#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/infra/NetworkTypes.hpp>

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

// 受管出站调用的只读事实视图：已解析目标、请求 envelope（五字段 + payload +
// metadata 只读）、已适配的绝对期限（父预算取 min 后的有效值，循环上界）。
struct RpcCallView {
    const bbt::infra::RpcAddress& target;
    const bbt::infra::RpcEnvelope& request;
    bbt::coroutine::Deadline deadline;
};

// 策略结论：Allow（放行，可选收紧期限）或 Reject（拒绝，零 I/O）。
struct RpcPolicyDecision {
    enum class Kind { Allow, Reject };

    Kind kind = Kind::Allow;

    // 仅当 kind==Allow 且已设置时生效；框架取 min(原期限, 本值)，绝不放大。
    // 收紧值 ≤ now → 框架在发起任何 I/O 之前按 TimedOut 拒绝。
    std::optional<bbt::coroutine::Deadline> tightened_deadline;

    // kind==Reject 时回给调用方的稳定错误（经框架错误边界上报）。
    Error reject_error;
};

// 公开出站策略抽象：可继承实现，也可组合包装（策略包策略 = 装饰器）。
class RpcPolicy {
public:
    virtual ~RpcPolicy() = default;

    // 每次受管出站调用调用一次（契约见文件头）。
    virtual result<RpcPolicyDecision> Preflight(const RpcCallView& call) = 0;
};

} // namespace bbt::framework
