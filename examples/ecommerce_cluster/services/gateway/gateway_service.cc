#include "services/gateway/gateway_service.hpp"

#include <cstdint>
#include "services/common/storage.hpp"
#include <string>
#include <utility>

#include "services/common/hop_log.hpp"
#include "services/common/node_identity.hpp"
#include "services/common/proto_codec.hpp"

namespace ecommerce {

using namespace bbt::framework;

CoRpcResp GatewayService::Checkout(CoRpcReq req) {
    HopRecord hop{kGatewayService, "Checkout", InboundRequestId(context()),
                  kOrderService, "Quote", true, 0};
    auto fail = [&hop](Error error) {
        hop.ok = false;
        hop.error_code = static_cast<int>(error.code);
        LogHop(hop);
        return CoRpcResp::Error(std::move(error));
    };

    auto parsed = req.ParseProto<v1::CheckoutRequest>();
    if (!parsed) return fail(parsed.error());

    const std::string sku = parsed.value().sku();
    const std::int32_t qty = parsed.value().qty();
    if (sku.empty())
        return fail(MakeError(ErrorCode::InvalidArgument,
                              "gateway: sku must be non-empty"));
    if (qty <= 0)
        return fail(MakeError(ErrorCode::InvalidArgument,
                              "gateway: qty must be positive"));

    v1::QuoteRequest quote_req;
    quote_req.set_sku(sku);
    quote_req.set_qty(qty);
    auto quote = this->call<v1::QuoteRequest, v1::QuoteResponse>(
        std::string(kOrderService), "Quote", quote_req);
    if (!quote) return fail(quote.error());

    v1::CheckoutResponse out;
    out.set_sku(sku);
    out.set_qty(qty);
    out.set_total_cents(quote.value().total_cents());
    out.set_catalog_instance(quote.value().catalog_instance());
    out.set_order_instance(quote.value().order_instance());
    out.set_gateway_instance(Node().instance_id);
    LogHop(hop);
    return CoRpcResp::FromProto(out);
}

CoRpcResp GatewayService::CreateOrder(CoRpcReq req) {
    auto q = req.ParseProto<v1::OrderRequest>();
    if (!q)
        return CoRpcResp::Error(q.error());
    if (!Storage().enabled)
        return CoRpcResp::Error(MakeError(ErrorCode::RuntimeUnavailable, "storage mode required"));
    auto reply = this->call<v1::OrderRequest, v1::Record>(std::string(kOrderService), "CreateOrder", q.value());
    StorageHop(context(), "gateway", "CreateOrder", q.value().order_id(), bool(reply),
               reply ? 0 : static_cast<int>(reply.error().code));
    if (!reply)
        return CoRpcResp::Error(reply.error());
    return CoRpcResp::FromProto(reply.value());
}
CoRpcResp GatewayService::GetOrder(CoRpcReq req) {
    auto q = req.ParseProto<v1::LookupRequest>();
    if (!q)
        return CoRpcResp::Error(q.error());
    if (!Storage().enabled)
        return CoRpcResp::Error(MakeError(ErrorCode::RuntimeUnavailable, "storage mode required"));
    auto reply = this->call<v1::LookupRequest, v1::Record>(std::string(kOrderService), "GetOrder", q.value());
    StorageHop(context(), "gateway", "GetOrder", q.value().order_id(), bool(reply),
               reply ? 0 : static_cast<int>(reply.error().code));
    if (!reply)
        return CoRpcResp::Error(reply.error());
    return CoRpcResp::FromProto(reply.value());
}
CoRpcResp GatewayService::GetBill(CoRpcReq req) {
    auto q = req.ParseProto<v1::LookupRequest>();
    if (!q)
        return CoRpcResp::Error(q.error());
    if (!Storage().enabled)
        return CoRpcResp::Error(MakeError(ErrorCode::RuntimeUnavailable, "storage mode required"));
    auto reply = this->call<v1::LookupRequest, v1::Record>(std::string(kSettlementService), "GetBill", q.value());
    StorageHop(context(), "gateway", "GetBill", q.value().order_id(), bool(reply),
               reply ? 0 : static_cast<int>(reply.error().code));
    if (!reply)
        return CoRpcResp::Error(reply.error());
    return CoRpcResp::FromProto(reply.value());
}
} // namespace ecommerce
