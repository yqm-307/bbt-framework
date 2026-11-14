#pragma once
// 独立结算服务，拥有账单集合；不接入真实支付。
#include <string_view>
#include <bbt/example/ecommerce/v1/ecommerce.pb.h>
#include <bbt/framework/CoRpc.hpp>
#ifdef EC_STORAGE_AVAILABLE
#include <bbt/framework/MongoDBComponent.hpp>
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

// 存储模式才声明 Mongo 能力（infra mongo 缺失时组件类型不存在）。
#ifdef EC_STORAGE_AVAILABLE
using SettlementBase =
    bbt::framework::v2::CoService<bbt::framework::v2::MongoDBComponent>;
#else
using SettlementBase = bbt::framework::v2::CoService<>;
#endif

/** @brief 按业务 ID 保存唯一账单，提供查询。 */
class SettlementService final : public SettlementBase {
public:
    static constexpr std::string_view kServiceName = kSettlementService;

    explicit SettlementService(bbt::framework::detail::ConstructKey key)
        : SettlementBase(key) {}

    CoRpcResp Settle(CoRpcReq req);
    CoRpcResp GetBill(CoRpcReq req);
    static constexpr auto kRpcMethods = RpcMethods(
        ProtoMethod<&SettlementService::Settle, v1::BillRequest, v1::Record>("Settle"),
        ProtoMethod<&SettlementService::GetBill, v1::LookupRequest, v1::Record>("GetBill"));

protected:
    bbt::framework::result<void> OnStart() override;
    ~SettlementService() noexcept override = default;
    friend class bbt::framework::detail::Registry;
};
} // namespace ecommerce
