#pragma once
// 目录服务：按 sku 返回单价（分）。
//
// 本头只声明业务面，实现见 catalog_service.cc。被 order 经动态发现调用；
// 进程里没有实例地址，落点由宿主路由门解析。

#include <string>
#include <string_view>

#include <bbt/example/ecommerce/v1/ecommerce.pb.h>

#include <bbt/framework/CoRpc.hpp>
#ifdef EC_STORAGE_AVAILABLE
#include <bbt/framework/RedisComponent.hpp>
#endif
#include <bbt/framework/RpcMethods.hpp>
#include <bbt/framework/Service.hpp>

#include "services/common/service_names.hpp"

namespace ecommerce {

namespace v1 = bbt::example::ecommerce::v1;

using bbt::framework::CoRpcReq;
using bbt::framework::CoRpcResp;
using bbt::framework::ProtoMethod;
using bbt::framework::RpcMethods;

// 存储模式才声明 Redis 能力：v2::CoService<Components...> 的允许集合是
// 编译期声明，infra redis_net 缺失时组件类型不存在，不能出现在模板实参里。
#ifdef EC_STORAGE_AVAILABLE
using CatalogBase =
    bbt::framework::v2::CoService<bbt::framework::v2::RedisComponent>;
#else
using CatalogBase = bbt::framework::v2::CoService<>;
#endif

/// @brief 目录服务：`Price` 返回 sku 单价与本实例身份。
class CatalogService final : public CatalogBase {
public:
    static constexpr std::string_view kServiceName = kCatalogService;

    explicit CatalogService(bbt::framework::detail::ConstructKey key)
        : CatalogBase(key) {}

    CoRpcResp Price(CoRpcReq req);
#ifdef EC_STORAGE_AVAILABLE
    CoRpcResp Inspect(CoRpcReq req);
#endif

    static constexpr auto kRpcMethods = RpcMethods(
        ProtoMethod<&CatalogService::Price, v1::PriceRequest, v1::PriceResponse>(
            "Price")
#ifdef EC_STORAGE_AVAILABLE
            ,
        ProtoMethod<&CatalogService::Inspect, v1::LookupRequest, v1::ComponentView>("Inspect")
#endif
    );

protected:
    ~CatalogService() noexcept override = default;
    friend class bbt::framework::detail::Registry;
};

} // namespace ecommerce
