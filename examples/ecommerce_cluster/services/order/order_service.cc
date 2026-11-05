#include "services/order/order_service.hpp"

#include <cstdint>
#include <limits>
#include "services/common/storage.hpp"
#include <string>
#include <utility>

#include "services/common/hop_log.hpp"
#include "services/common/node_identity.hpp"
#include "services/common/proto_codec.hpp"

namespace ecommerce {

using namespace bbt::framework;

CoRpcResp OrderService::Quote(CoRpcReq req) {
    HopRecord hop{kOrderService, "Quote", InboundRequestId(context()),
                  kCatalogService, "Price", true, 0};
    auto fail = [&hop](Error error) {
        hop.ok = false;
        hop.error_code = static_cast<int>(error.code);
        LogHop(hop);
        return CoRpcResp::Error(std::move(error));
    };

    auto parsed = req.ParseProto<v1::QuoteRequest>();
    if (!parsed) return fail(parsed.error());

    const std::string sku = parsed.value().sku();
    const std::int32_t qty = parsed.value().qty();
    if (sku.empty())
        return fail(MakeError(ErrorCode::InvalidArgument,
                              "order: sku must be non-empty"));
    if (qty <= 0)
        return fail(MakeError(ErrorCode::InvalidArgument,
                              "order: qty must be positive"));

    v1::PriceRequest price_req;
    price_req.set_sku(sku);
    // 只给逻辑服务名：目标地址由宿主 find_route（动态表）决定，业务不注入。
    auto price = this->call<v1::PriceRequest, v1::PriceResponse>(
        std::string(kCatalogService), "Price", price_req);
    if (!price) return fail(price.error());

    v1::QuoteResponse out;
    out.set_sku(sku);
    out.set_qty(qty);
    out.set_total_cents(price.value().unit_cents() *
                        static_cast<std::int64_t>(qty));
    out.set_catalog_instance(price.value().instance_id());
    out.set_order_instance(Node().instance_id);
    LogHop(hop);
    return CoRpcResp::FromProto(out);
}

CoRpcResp OrderService::CreateOrder(CoRpcReq req) {
    auto q = req.ParseProto<v1::OrderRequest>();
    if (!q)
        return CoRpcResp::Error(q.error());
    const auto& order = q.value();
    auto fail = [&](Error error) {
        StorageHop(context(), "order", "CreateOrder", order.order_id(), false, static_cast<int>(error.code));
        return CoRpcResp::Error(std::move(error));
    };
    auto valid = ValidateOrder(order);
    if (!valid)
        return fail(valid.error());
#ifdef EC_STORAGE_AVAILABLE
    auto db = context().resource<MongoDbResource>("orders");
    if (!Storage().enabled || !db)
        return fail(MakeError(ErrorCode::RuntimeUnavailable, "storage mode required"));
    const auto budget = StorageBudget(context());
    auto saved = ReadRecord(*db, order.order_id(), budget);
    if (!saved && saved.error().code != ErrorCode::NotFound)
        return fail(saved.error());
    if (!saved) {
        v1::PriceRequest price_req;
        price_req.set_sku(order.sku());
        price_req.set_order_id(order.order_id());
        auto price = this->call<v1::PriceRequest, v1::PriceResponse>(
            std::string(kCatalogService), "Price", price_req);
        if (!price)
            return fail(price.error());
        const auto unit = price.value().unit_cents();
        if (unit <= 0 || order.qty() > std::numeric_limits<std::int64_t>::max() / unit)
            return fail(MakeError(ErrorCode::InvalidArgument, "order amount overflow"));
        const auto total = order.qty() * unit;
        if (order.discount_cents() > total)
            return fail(MakeError(ErrorCode::InvalidArgument, "discount exceeds total"));
        v1::Record pending;
        pending.set_order_id(order.order_id());
        pending.set_sku(order.sku());
        pending.set_qty(order.qty());
        pending.set_total_cents(total);
        pending.set_discount_cents(order.discount_cents());
        pending.set_payable_cents(total - order.discount_cents());
        pending.set_state("pending");
        pending.set_catalog_instance(price.value().instance_id());
        pending.set_order_instance(Node().instance_id);
        saved = InsertRecord(*db, pending, budget);
        if (!saved)
            return fail(saved.error());
    }
    if (!SamePayload(saved.value(), order))
        return fail(MakeError(ErrorCode::InvalidArgument, "id_conflict: order payload differs"));
    if (saved.value().state() != "settled") {
        v1::BillRequest bill_req;
        *bill_req.mutable_order() = order;
        bill_req.set_total_cents(saved.value().total_cents());
        // 一次显式业务调用只发送一次写 RPC；失败保留 pending，不盲重试。
        auto bill = this->call<v1::BillRequest, v1::Record>(
            std::string(kSettlementService), "Settle", bill_req);
        if (!bill)
            return fail(bill.error());
        if (!SamePayload(bill.value(), order) || bill.value().state() != "settled" ||
            bill.value().total_cents() != saved.value().total_cents() ||
            bill.value().payable_cents() != saved.value().payable_cents())
            return fail(MakeError(ErrorCode::ProtocolError, "inconsistent settlement reply"));
        auto settled = saved.value();
        settled.set_state("settled");
        settled.set_settlement_instance(bill.value().settlement_instance());
        auto updated = MarkSettled(*db, settled, budget);
        if (!updated)
            return fail(updated.error());
        saved = ReadRecord(*db, order.order_id(), budget);
        if (!saved)
            return fail(saved.error());
    }
    StorageHop(context(), "order", "CreateOrder", order.order_id(), true);
    return CoRpcResp::FromProto(saved.value());
#else
    return fail(MakeError(ErrorCode::RuntimeUnavailable, "storage dependencies unavailable"));
#endif
}
CoRpcResp OrderService::GetOrder(CoRpcReq req) {
    auto q = req.ParseProto<v1::LookupRequest>();
    if (!q)
        return CoRpcResp::Error(q.error());
#ifdef EC_STORAGE_AVAILABLE
    auto db = context().resource<MongoDbResource>("orders");
    if (Storage().enabled && db) {
        auto r = ReadRecord(*db, q.value().order_id(), StorageBudget(context()));
        if (!r)
            return CoRpcResp::Error(r.error());
        StorageHop(context(), "order", "GetOrder", q.value().order_id(), true);
        return CoRpcResp::FromProto(r.value());
    }
#endif
    return CoRpcResp::Error(MakeError(ErrorCode::RuntimeUnavailable, "storage mode required"));
}
} // namespace ecommerce
