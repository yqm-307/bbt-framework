#pragma once
// 逻辑服务名：调用方只知这三个名字，Consul 里注册的也是同样的名字。
//
// 名字是业务与注册中心之间唯一的约定；调用点不得出现实例地址，
// 落点由宿主按动态路由表解析。

#include <string_view>

namespace ecommerce {

inline constexpr std::string_view kCatalogService =
    "bbt.example.ecommerce.v1.CatalogService";
inline constexpr std::string_view kOrderService =
    "bbt.example.ecommerce.v1.OrderService";
inline constexpr std::string_view kGatewayService =
    "bbt.example.ecommerce.v1.GatewayService";

inline constexpr std::string_view kSettlementService = "ecommerce.settlement";

} // namespace ecommerce
