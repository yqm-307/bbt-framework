#pragma once
// 目录的四个消费视图与主目录在同一 App 绑定两个 Redis 组件，不创建私有池。
#include <array>
#include <cstdint>
#include <string>
#include <bbt/framework/RedisComponent.hpp>
#include <bbt/framework/Service.hpp>
#include <bbt/framework/RpcMethods.hpp>
#include "services/common/proto_codec.hpp"
#include "services/common/node_identity.hpp"

namespace ecommerce {
namespace v1 = bbt::example::ecommerce::v1;

inline bbt::framework::CoRpcResp InspectCache(
    bbt::framework::result<bbt::framework::ComponentHandle<bbt::framework::v2::RedisComponent>> handle) {
    using namespace bbt::framework;
    if (!handle)
        return CoRpcResp::Error(handle.error());
    auto borrow = handle.value().Borrow();
    if (!borrow)
        return CoRpcResp::Error(borrow.error());
    v1::ComponentView view;
    view.set_component_id(handle.value().Object().Id());
    view.set_generation(handle.value().Object().Generation());
    view.set_pool_identity(std::to_string(reinterpret_cast<std::uintptr_t>(&borrow.value().Get().Pool())));
    view.set_process_id(Node().process_id);
    view.set_connected(borrow.value().Get().connected());
    return CoRpcResp::FromProto(view);
}

/** @brief 同一目录进程的独立消费视图；前两视图复用主缓存，后两视图复用辅助缓存。 */
template <int Index>
class CacheViewService final : public bbt::framework::v2::CoService<bbt::framework::v2::RedisComponent> {
public:
    inline static constexpr std::array<std::string_view, 4> kNames = {
        "ecommerce.catalog.search", "ecommerce.catalog.recommendations",
        "ecommerce.catalog.stock", "ecommerce.catalog.audit"};
    static constexpr std::string_view kServiceName = kNames[Index];
    explicit CacheViewService(bbt::framework::detail::ConstructKey key) : CoService(key) {}
    bbt::framework::CoRpcResp Inspect(bbt::framework::CoRpcReq) {
        return InspectCache(this->template GetComponent<bbt::framework::v2::RedisComponent>(
            Index < 2 ? "cache" : "aux-cache"));
    }
    static constexpr auto kRpcMethods = bbt::framework::RpcMethods(
        bbt::framework::ProtoMethod<&CacheViewService::Inspect, v1::LookupRequest, v1::ComponentView>("Inspect"));

protected:
    ~CacheViewService() noexcept override = default;
    friend class bbt::framework::detail::Registry;
};
} // namespace ecommerce
