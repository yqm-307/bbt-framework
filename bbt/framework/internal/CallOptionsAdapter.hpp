#pragma once
/** @brief 解析受管出站调用的单调时钟绝对期限。 */

#include <optional>

#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework {

/// 取显式期限和父期限中的较早者。要求有限预算，已过期返回 TimedOut。
result<bbt::coroutine::Deadline> ResolveCallDeadline(
    const RequestContext* parent,
    std::optional<bbt::coroutine::Deadline> deadline,
    bbt::coroutine::Deadline now);

} // namespace bbt::framework
