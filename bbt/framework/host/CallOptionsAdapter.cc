#include <bbt/framework/internal/CallOptionsAdapter.hpp>

#include <algorithm>

namespace bbt::framework {

result<bbt::coroutine::Deadline> ResolveCallDeadline(
    const RequestContext* parent,
    std::optional<bbt::coroutine::Deadline> deadline,
    bbt::coroutine::Deadline now) {
    using bbt::coroutine::Deadline;

    auto effective = deadline.value_or(Deadline::max());
    if (parent)
        effective = std::min(effective, parent->deadline);
    if (effective == Deadline::max())
        return result<Deadline>::err(MakeError(
            ErrorCode::InvalidArgument,
            parent ? "no finite call budget inherited from parent request"
                   : "top-level call requires a finite deadline"));
    if (effective <= now)
        return result<Deadline>::err(MakeError(
            ErrorCode::TimedOut,
            "call budget already expired before egress io"));
    return result<Deadline>::ok(effective);
}

} // namespace bbt::framework
