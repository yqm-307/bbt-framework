#pragma once
// 独立结算服务，拥有账单集合；不接入真实支付。
#include <string_view>
#include <bbt/example/ecommerce/v1/ecommerce.pb.h>
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/RpcMethods.hpp>
#include "services/common/service_names.hpp"
namespace ecommerce {
namespace v1 = bbt::example::ecommerce::v1;
using bbt::framework::CoRpcReq;
using bbt::framework::CoRpcResp;
using bbt::framework::CoService;
using bbt::framework::ProtoMethod;
using bbt::framework::RpcMethods;
/** @brief 按业务 ID 保存唯一账单，提供查询。 */
class SettlementService final : public CoService<SettlementService> {
public:
    static constexpr std::string_view kServiceName = kSettlementService;
    CoRpcResp Settle(CoRpcReq req);
    CoRpcResp GetBill(CoRpcReq req);
    static constexpr auto kRpcMethods = RpcMethods(
        ProtoMethod<&SettlementService::Settle, v1::BillRequest, v1::Record>("Settle"),
        ProtoMethod<&SettlementService::GetBill, v1::LookupRequest, v1::Record>("GetBill"));
};
} // namespace ecommerce
