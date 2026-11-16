#pragma once
/**
 * 订单服务：按 sku/qty 向 catalog 取单价并算出总额。
 *
 * 本头只声明业务面，实现见 order_service.cc。出站目标是逻辑服务名
 * kCatalogService/kSettlementService，地址由宿主路由门按动态路由表解析。
 * 存储模式下声明绑定 orders 集合组件（MongoDbResource）；非存储构建
 * 不引用该资源类型。
 */

#include <string>
#include <string_view>

#include <bbt/example/ecommerce/v1/ecommerce.pb.h>

#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/RpcMethods.hpp>
#ifdef EC_STORAGE_AVAILABLE
#include <bbt/framework/MongoDbResource.hpp>
#endif

#include "services/common/service_names.hpp"

namespace ecommerce {

namespace v1 = bbt::example::ecommerce::v1;

using bbt::framework::CoRpcReq;
using bbt::framework::CoRpcResp;
using bbt::framework::CoService;
using bbt::framework::ProtoMethod;
using bbt::framework::RpcMethods;

/**
 * @brief 订单服务：`Quote` 调用 catalog.Price 并回带两侧实例身份。
 */
class OrderService final
#ifdef EC_STORAGE_AVAILABLE
    : public CoService<bbt::framework::MongoDbResource> {
public:
    explicit OrderService(bbt::framework::detail::ConstructKey key)
        : CoService<bbt::framework::MongoDbResource>(key) {}

    /**
     * 存储模式启动探针：orders 组件已绑定后读一次空文档，Mongo 不可达即
     * 启动失败（fail-closed，不带着坏后端进入 ready）。
     */
    bbt::framework::result<void> OnStart() override;
#else
    : public CoService<> {
public:
    explicit OrderService(bbt::framework::detail::ConstructKey key)
        : CoService<>(key) {}
#endif

    CoRpcResp Quote(CoRpcReq req);

    CoRpcResp CreateOrder(CoRpcReq req);
    CoRpcResp GetOrder(CoRpcReq req);

    static constexpr auto kRpcMethods = RpcMethods(
        ProtoMethod<&OrderService::Quote, v1::QuoteRequest, v1::QuoteResponse>(
            "Quote"),
        ProtoMethod<&OrderService::CreateOrder, v1::OrderRequest, v1::Record>("CreateOrder"),
        ProtoMethod<&OrderService::GetOrder, v1::LookupRequest, v1::Record>("GetOrder"));

protected:
    ~OrderService() noexcept override = default;

private:
    friend class bbt::framework::detail::Registry;
};

} // namespace ecommerce
