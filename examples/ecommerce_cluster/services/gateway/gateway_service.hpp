#pragma once
// 网关服务：对外唯一入口，把结算请求转给 order。
//
// 本头只声明业务面，实现见 gateway_service.cc。出站目标是逻辑服务名
// kOrderService，地址由宿主 find_route 按动态路由表解析。

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

/// @brief 网关服务：`Checkout` 调用 order.Quote 并回带三跳实例身份。
class GatewayService final : public CoService<GatewayService> {
public:
    static constexpr std::string_view kServiceName = kGatewayService;

    CoRpcResp Checkout(CoRpcReq req);

    CoRpcResp CreateOrder(CoRpcReq req);
    CoRpcResp GetOrder(CoRpcReq req);
    CoRpcResp GetBill(CoRpcReq req);

    static constexpr auto kRpcMethods = RpcMethods(
        ProtoMethod<&GatewayService::Checkout, v1::CheckoutRequest,
                    v1::CheckoutResponse>("Checkout"),
        ProtoMethod<&GatewayService::CreateOrder, v1::OrderRequest, v1::Record>("CreateOrder"),
        ProtoMethod<&GatewayService::GetOrder, v1::LookupRequest, v1::Record>("GetOrder"),
        ProtoMethod<&GatewayService::GetBill, v1::LookupRequest, v1::Record>("GetBill"));
};

} // namespace ecommerce
