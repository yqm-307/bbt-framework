#pragma once
/**
 * 独立结算服务，拥有账单集合；不接入真实支付。
 * 存储模式下声明绑定 bills 集合组件（MongoDbResource）；非存储构建
 * 不引用该资源类型。
 */
#include <string_view>
#include <bbt/example/ecommerce/v1/ecommerce.pb.h>
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/CoRpc.hpp>
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
/** @brief 按业务 ID 保存唯一账单，提供查询。 */
class SettlementService final
#ifdef EC_STORAGE_AVAILABLE
    : public CoService<bbt::framework::MongoDbResource> {
public:
    explicit SettlementService(bbt::framework::detail::ConstructKey key)
        : CoService<bbt::framework::MongoDbResource>(key) {}

    /**
     * 存储模式启动探针：bills 组件已绑定后读一次空文档，Mongo 不可达即
     * 启动失败（fail-closed）。
     */
    bbt::framework::result<void> OnStart() override;
#else
    : public CoService<> {
public:
    explicit SettlementService(bbt::framework::detail::ConstructKey key)
        : CoService<>(key) {}
#endif

    CoRpcResp Settle(CoRpcReq req);
    CoRpcResp GetBill(CoRpcReq req);
    static constexpr auto kRpcMethods = RpcMethods(
        ProtoMethod<&SettlementService::Settle, v1::BillRequest, v1::Record>("Settle"),
        ProtoMethod<&SettlementService::GetBill, v1::LookupRequest, v1::Record>("GetBill"));

protected:
    ~SettlementService() noexcept override = default;

private:
    friend class bbt::framework::detail::Registry;
};
} // namespace ecommerce
