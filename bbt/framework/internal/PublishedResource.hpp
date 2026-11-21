#pragma once
/** @brief 从组件取得已发布的客户端，在一次调用及其挂起期间保活。 */

#include <memory>

#include <bbt/framework/Result.hpp>

namespace bbt::framework::detail {

template <class T, class F>
auto WithPublishedResource(const std::shared_ptr<T>& slot, F call) {
    const auto resource = std::atomic_load(&slot);
    if (!resource) {
        using Result = decltype(call(*resource));
        return Result::err(MakeError(ErrorCode::Closed, "component resource is not open"));
    }
    return call(*resource);
}

} // namespace bbt::framework::detail
