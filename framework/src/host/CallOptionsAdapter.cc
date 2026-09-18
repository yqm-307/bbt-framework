#include <bbt/framework/internal/CallOptionsAdapter.hpp>

#include <algorithm>
#include <chrono>
#include <exception>

namespace bbt::framework {

result<bbt::infra::CallOptions> AdaptCallOptions(
    const RequestContext*  parent,
    const CallOptions&     options,
    const CallEgressHooks& hooks)
{
    using bbt::coroutine::CancellationToken;
    using bbt::coroutine::Deadline;

    // 1. 有效期限：缺省继承父剩余预算；显式期限与父预算取最小值，
    //    不得延长父预算；顶层入口必须自带有限期限。
    Deadline effective;
    if (parent == nullptr) {
        if (!options.deadline.has_value() ||
            *options.deadline == Deadline::max())
            return result<bbt::infra::CallOptions>::err(MakeError(
                ErrorCode::InvalidArgument,
                "top-level call requires a finite deadline"));
        effective = *options.deadline;
    } else {
        effective = options.deadline.has_value()
            ? std::min(*options.deadline, parent->deadline)
            : parent->deadline;
        if (effective == Deadline::max())
            return result<bbt::infra::CallOptions>::err(MakeError(
                ErrorCode::InvalidArgument,
                "no finite call budget inherited from parent request"));
    }

    // 2. 已过期 → 发起任何 I/O 之前返回 TimedOut。
    if (effective <= std::chrono::steady_clock::now())
        return result<bbt::infra::CallOptions>::err(MakeError(
            ErrorCode::TimedOut,
            "call budget already expired before egress io"));

    // 3. cancel：父 token（顶层为永不可取消的空 token）与 options.cancel
    //    合并，合并后原样传入 infra；装配抛异常 → InternalError，
    //    此时尚未发起 I/O、未消耗序号、票据保持未发送。
    CancellationToken combined;
    try {
        const CancellationToken parent_cancel =
            (parent != nullptr) ? parent->cancel : CancellationToken{};
        combined = hooks.combine_cancel
            ? hooks.combine_cancel(parent_cancel, options.cancel)
            : CancellationToken::Combine(parent_cancel, options.cancel);
    } catch (const std::exception& e) {
        return result<bbt::infra::CallOptions>::err(MakeError(
            ErrorCode::InternalError,
            std::string("context assembly failed: ") + e.what()));
    } catch (...) {
        return result<bbt::infra::CallOptions>::err(MakeError(
            ErrorCode::InternalError,
            "context assembly failed with non-std exception"));
    }

    bbt::infra::CallOptions out;
    out.deadline = effective;
    out.cancel   = combined;

    // 4. 发起 I/O 的能力检查先于任何副作用：无出站注入点 → 不消耗序号。
    if (!hooks.initiate_io)
        return result<bbt::infra::CallOptions>::err(MakeError(
            ErrorCode::RuntimeUnavailable,
            "egress io hook not installed"));
    if (options.ordered.has_value() && !hooks.consume_sequence)
        return result<bbt::infra::CallOptions>::err(MakeError(
            ErrorCode::InvalidArgument,
            "ordered call requires a sequence consumer hook"));

    // 5. 消耗接收端序号（有序票据首次发送绑定），随后发起 I/O。
    if (options.ordered.has_value()) {
        auto seq = hooks.consume_sequence(*options.ordered);
        if (!seq)
            return result<bbt::infra::CallOptions>::err(seq.error());
    }
    auto io = hooks.initiate_io(out);
    if (!io)
        return result<bbt::infra::CallOptions>::err(io.error());

    return result<bbt::infra::CallOptions>::ok(out);
}

} // namespace bbt::framework
