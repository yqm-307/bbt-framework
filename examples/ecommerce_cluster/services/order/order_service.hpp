#pragma once
// 订单服务：按 sku/qty 向 catalog 取单价并算出总额。
//
// 本头只声明业务面，实现见 order_service.cc。出站目标是逻辑服务名
// kCatalogService，地址由宿主 find_route 按动态路由表解析。

#include <string>
#include <string_view>

#include <bbt/example/ecommerce/v1/ecommerce.pb.h>

#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/RpcMethods.hpp>

#include "services/common/service_names.hpp"

namespace ecommerce {

namespace v1 = bbt::example::ecommerce::v1;

using bbt::framework::CoRpcReq;
using bbt::framework::CoRpcResp;
using bbt::framework::CoService;
using bbt::framework::ProtoMethod;
using bbt::framework::RpcMethods;

/// @brief 订单服务：`Quote` 调用 catalog.Price 并回带两侧实例身份。
class OrderService final : public CoService<OrderService> {
public:
    static constexpr std::string_view kServiceName = kOrderService;

    CoRpcResp Quote(CoRpcReq req);

    CoRpcResp CreateOrder(CoRpcReq req);
    CoRpcResp GetOrder(CoRpcReq req);

    static constexpr auto kRpcMethods = RpcMethods(
        ProtoMethod<&OrderService::Quote, v1::QuoteRequest, v1::QuoteResponse>(
            "Quote"),
        ProtoMethod<&OrderService::CreateOrder, v1::OrderRequest, v1::Record>("CreateOrder"),
        ProtoMethod<&OrderService::GetOrder, v1::LookupRequest, v1::Record>("GetOrder"));
};

} // namespace ecommerce
