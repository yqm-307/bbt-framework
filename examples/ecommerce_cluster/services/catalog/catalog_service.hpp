#pragma once
/**
 * 目录服务：按 sku 返回单价（分）。
 *
 * 本头只声明业务面，实现见 catalog_service.cc。被 order 经动态发现调用；
 * 进程里没有实例地址，落点由宿主路由门解析。存储模式下声明绑定 cache
 * 组件（RedisNetResource）；非存储构建不引用该资源类型。
 */

#include <string>
#include <string_view>

#include <bbt/example/ecommerce/v1/ecommerce.pb.h>

#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/RpcMethods.hpp>
#ifdef EC_STORAGE_AVAILABLE
#include <bbt/framework/RedisNetResource.hpp>
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
 * @brief 目录服务：`Price` 返回 sku 单价与本实例身份。
 */
class CatalogService final
#ifdef EC_STORAGE_AVAILABLE
    : public CoService<bbt::framework::RedisNetResource> {
public:
    explicit CatalogService(bbt::framework::detail::ConstructKey key)
        : CoService<bbt::framework::RedisNetResource>(key) {}

    /**
     * 存储模式启动连接：cache 组件已绑定后显式建连，Redis 不可达即启动
     * 失败（fail-closed，等价旧工厂「Connected 才交付」语义）。
     */
    bbt::framework::result<void> OnStart() override;
#else
    : public CoService<> {
public:
    explicit CatalogService(bbt::framework::detail::ConstructKey key)
        : CoService<>(key) {}
#endif

    CoRpcResp Price(CoRpcReq req);

    static constexpr auto kRpcMethods = RpcMethods(
        ProtoMethod<&CatalogService::Price, v1::PriceRequest, v1::PriceResponse>(
            "Price"));

protected:
    ~CatalogService() noexcept override = default;

private:
    friend class bbt::framework::detail::Registry;
};

} // namespace ecommerce
