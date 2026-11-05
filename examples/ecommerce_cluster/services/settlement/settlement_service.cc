#include "services/settlement/settlement_service.hpp"
#include "services/common/storage.hpp"
#include "services/common/proto_codec.hpp"
#include "services/common/node_identity.hpp"
namespace ecommerce {
using namespace bbt::framework;
CoRpcResp SettlementService::Settle(CoRpcReq req) {
    auto q = req.ParseProto<v1::BillRequest>();
    if (!q)
        return CoRpcResp::Error(q.error());
    const auto& order = q.value().order();
    auto fail = [&](Error error) {
        StorageHop(context(), "settlement", "Settle", order.order_id(), false, static_cast<int>(error.code));
        return CoRpcResp::Error(std::move(error));
    };
    auto valid = ValidateOrder(order);
    if (!valid)
        return fail(valid.error());
    if (q.value().total_cents() <= 0 || order.discount_cents() > q.value().total_cents())
        return fail(MakeError(ErrorCode::InvalidArgument, "invalid settlement amount"));
#ifdef EC_STORAGE_AVAILABLE
    auto db = context().resource<MongoDbResource>("bills");
    if (!Storage().enabled || !db)
        return fail(MakeError(ErrorCode::RuntimeUnavailable, "storage mode required"));
    auto budget = StorageBudget(context());
    auto old = ReadRecord(*db, order.order_id(), budget);
    if (!old && old.error().code != ErrorCode::NotFound)
        return fail(old.error());
    v1::Record bill;
    bill.set_order_id(order.order_id());
    bill.set_sku(order.sku());
    bill.set_qty(order.qty());
    bill.set_total_cents(q.value().total_cents());
    bill.set_discount_cents(order.discount_cents());
    bill.set_payable_cents(q.value().total_cents() - order.discount_cents());
    bill.set_state("settled");
    bill.set_settlement_instance(Node().instance_id);
    auto saved = old ? old : InsertRecord(*db, bill, budget);
    if (!saved)
        return fail(saved.error());
    if (!SamePayload(saved.value(), order) || saved.value().total_cents() != bill.total_cents())
        return fail(MakeError(ErrorCode::InvalidArgument, "id_conflict: settlement payload differs"));
    StorageHop(context(), "settlement", "Settle", order.order_id(), true);
    return CoRpcResp::FromProto(saved.value());
#else
    return fail(MakeError(ErrorCode::RuntimeUnavailable, "storage dependencies unavailable"));
#endif
}
CoRpcResp SettlementService::GetBill(CoRpcReq req) {
    auto q = req.ParseProto<v1::LookupRequest>();
    if (!q)
        return CoRpcResp::Error(q.error());
#ifdef EC_STORAGE_AVAILABLE
    auto db = context().resource<MongoDbResource>("bills");
    if (Storage().enabled && db) {
        auto r = ReadRecord(*db, q.value().order_id(), StorageBudget(context()));
        if (!r)
            return CoRpcResp::Error(r.error());
        StorageHop(context(), "settlement", "GetBill", q.value().order_id(), true);
        return CoRpcResp::FromProto(r.value());
    }
#endif
    return CoRpcResp::Error(MakeError(ErrorCode::RuntimeUnavailable, "storage mode required"));
}
} // namespace ecommerce
