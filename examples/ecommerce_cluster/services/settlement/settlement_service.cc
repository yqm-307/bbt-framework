#include "services/settlement/settlement_service.hpp"
#include "services/common/storage.hpp"
#include "services/common/proto_codec.hpp"
#include "services/common/node_identity.hpp"

#include <chrono>
#include <string>
#include <utility>

#include <bbt/coroutine/syntax/Async.hpp>

namespace ecommerce {
using namespace bbt::framework;

#ifdef EC_STORAGE_AVAILABLE
result<void> SettlementService::OnStart() {
    if (!Storage().enabled) return result<void>::ok();
    /**
     * 启动探针：读一次空文档，Mongo 不可达即启动失败（fail-closed）。
     */
    auto db = GetComponent<MongoDbResource>("bills");
    if (!db) return result<void>::err(std::move(db.error()));
    auto borrow = db.value().Borrow();
    if (!borrow) return result<void>::err(std::move(borrow.error()));
    auto filter = bbt::infra::mongo::MongoDocument::FromJson("{}");
    if (!filter) return result<void>::err(std::move(filter.error()));
    auto read = bbt_co_await borrow.value().Get().FindOne(
        filter.value(),
        bbt::infra::CallOptions{std::chrono::steady_clock::now() + std::chrono::seconds{3}});
    if (!read)
        return result<void>::err(MakeError(ErrorCode::Unavailable,
                                           "mongo startup: " + read.error().message));
    return result<void>::ok();
}
#endif // EC_STORAGE_AVAILABLE

CoRpcResp SettlementService::Settle(CoRpcReq req) {
    auto q = req.ParseProto<v1::BillRequest>();
    if (!q)
        return CoRpcResp::Error(q.error());
    const auto& order = q.value().order();
    auto fail = [&](Error error) {
        StorageHop("settlement", "Settle", order.order_id(), false, static_cast<int>(error.code));
        return CoRpcResp::Error(std::move(error));
    };
    auto valid = ValidateOrder(order);
    if (!valid)
        return fail(valid.error());
    if (q.value().total_cents() <= 0 || order.discount_cents() > q.value().total_cents())
        return fail(MakeError(ErrorCode::InvalidArgument, "invalid settlement amount"));
#ifdef EC_STORAGE_AVAILABLE
    if (Storage().enabled) {
        auto db = GetComponent<MongoDbResource>("bills");
        if (!db) return fail(db.error());
        auto borrow = db.value().Borrow();
        if (!borrow) return fail(borrow.error());
        auto budget = StorageBudget();
        if (!budget) return fail(budget.error());
        MongoDbResource& coll = borrow.value().Get();
        auto old = ReadRecord(coll, order.order_id(), budget.value());
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
        auto saved = old ? old : InsertRecord(coll, bill, budget.value());
        if (!saved)
            return fail(saved.error());
        if (!SamePayload(saved.value(), order) || saved.value().total_cents() != bill.total_cents())
            return fail(MakeError(ErrorCode::InvalidArgument, "id_conflict: settlement payload differs"));
        StorageHop("settlement", "Settle", order.order_id(), true);
        return CoRpcResp::FromProto(saved.value());
    }
#endif
    return fail(MakeError(ErrorCode::RuntimeUnavailable, "storage mode required"));
}
CoRpcResp SettlementService::GetBill(CoRpcReq req) {
    auto q = req.ParseProto<v1::LookupRequest>();
    if (!q)
        return CoRpcResp::Error(q.error());
#ifdef EC_STORAGE_AVAILABLE
    if (Storage().enabled) {
        auto db = GetComponent<MongoDbResource>("bills");
        if (!db)
            return CoRpcResp::Error(db.error());
        auto borrow = db.value().Borrow();
        if (!borrow)
            return CoRpcResp::Error(borrow.error());
        auto budget = StorageBudget();
        if (!budget)
            return CoRpcResp::Error(budget.error());
        auto r = ReadRecord(borrow.value().Get(), q.value().order_id(), budget.value());
        if (!r)
            return CoRpcResp::Error(r.error());
        StorageHop("settlement", "GetBill", q.value().order_id(), true);
        return CoRpcResp::FromProto(r.value());
    }
#endif
    return CoRpcResp::Error(MakeError(ErrorCode::RuntimeUnavailable, "storage mode required"));
}
} // namespace ecommerce
