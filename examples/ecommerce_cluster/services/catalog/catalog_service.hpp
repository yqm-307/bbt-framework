#pragma once
// 目录服务：按 sku 返回单价（分）。
//
// 本头只声明业务面，实现见 catalog_service.cc。被 order 经动态发现调用；
// 进程里没有实例地址，落点由宿主 find_route 解析。

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

/// @brief 目录服务：`Price` 返回 sku 单价与本实例身份。
class CatalogService final : public CoService<CatalogService> {
public:
    static constexpr std::string_view kServiceName = kCatalogService;

    CoRpcResp Price(CoRpcReq req);

    static constexpr auto kRpcMethods = RpcMethods(
        ProtoMethod<&CatalogService::Price, v1::PriceRequest, v1::PriceResponse>(
            "Price"));
};

} // namespace ecommerce
