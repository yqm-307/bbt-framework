#include "services/catalog/catalog_service.hpp"

#include <cstdint>
#include <charconv>
#include <cstdio>
#include "services/common/storage.hpp"
#ifdef EC_STORAGE_AVAILABLE
#include <bbt/framework/RedisNetResource.hpp>
#endif
#include <string>
#include <string_view>
#include <utility>

#include "services/common/hop_log.hpp"
#include "services/common/node_identity.hpp"
#include "services/common/proto_codec.hpp"

namespace ecommerce {

using namespace bbt::framework;

namespace {

// 目录价（分）：示例数据。未知 sku 返回 0，由 handler 转 NotFound。
std::int64_t UnitPriceCents(std::string_view sku) {
    if (sku == "sku-book") return 1299;
    if (sku == "sku-mug") return 899;
    if (sku == "sku-pen") return 259;
    return 0;
}

} // namespace

CoRpcResp CatalogService::Price(CoRpcReq req) {
    HopRecord hop{kCatalogService, "Price", InboundRequestId(context()), {}, {},
                  true, 0};
    auto fail = [&hop](Error error) {
        hop.ok = false;
        hop.error_code = static_cast<int>(error.code);
        LogHop(hop);
        return CoRpcResp::Error(std::move(error));
    };

    auto parsed = req.ParseProto<v1::PriceRequest>();
    if (!parsed) return fail(parsed.error());

    const std::string& sku = parsed.value().sku();
    if (sku.empty())
        return fail(MakeError(ErrorCode::InvalidArgument,
                              "catalog: sku must be non-empty"));

    std::int64_t unit = UnitPriceCents(sku);
#ifdef EC_STORAGE_AVAILABLE
    if (Storage().enabled && unit > 0) {
        auto cache = context().resource<RedisNetResource>("cache");
        if (!cache)
            return fail(MakeError(ErrorCode::RuntimeUnavailable, "cache resource missing"));
        const auto budget = StorageBudget(context());
        const std::string key = Storage().key_prefix + ":price:" + sku;
        auto value = bbt_co_await cache->Get(key, budget);
        if (!value)
            return fail(value.error());
        if (value.value()) {
            const auto& text = *value.value();
            auto parsed_price = std::from_chars(text.data(), text.data() + text.size(), unit);
            if (parsed_price.ec != std::errc{} || parsed_price.ptr != text.data() + text.size() || unit <= 0)
                return fail(MakeError(ErrorCode::ProtocolError, "invalid cached price"));
        } else {
            auto set = bbt_co_await cache->Set(key, std::to_string(unit), budget);
            if (!set)
                return fail(set.error());
        }
        std::printf("event=cache result=%s key=%s order_id=%s pid=%s\n",
                    value.value() ? "hit" : "miss", key.c_str(), parsed.value().order_id().c_str(), Node().process_id.c_str());
        std::fflush(stdout);
        StorageHop(context(), "catalog", "Price", parsed.value().order_id(), true);
    }
#endif
    if (unit <= 0)
        return fail(MakeError(ErrorCode::NotFound,
                              "catalog: unknown sku '" + sku + "'"));

    v1::PriceResponse out;
    out.set_sku(sku);
    out.set_unit_cents(unit);
    out.set_instance_id(Node().instance_id);
    out.set_process_id(Node().process_id);
    LogHop(hop);
    return CoRpcResp::FromProto(out);
}

} // namespace ecommerce
