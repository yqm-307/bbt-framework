#include "services/common/storage.hpp"
#include <algorithm>
#include <cctype>
#include <cstdio>
#include "services/common/hop_log.hpp"
#include "services/common/node_identity.hpp"
namespace ecommerce {
using namespace bbt::framework;
StorageConfig& Storage() {
    static StorageConfig config;
    return config;
}
namespace {
bool SafeToken(const std::string& value) {
    return !value.empty() && value.size() <= 80 &&
           std::all_of(value.begin(), value.end(), [](unsigned char c) {
               return std::isalnum(c) || c == '-' || c == '_';
           });
}
} // namespace
result<void> ValidateOrder(const v1::OrderRequest& r) {
    if (!SafeToken(r.order_id()) || !SafeToken(r.sku()) || r.qty() <= 0 || r.discount_cents() < 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "invalid id, sku, quantity or discount"));
    return result<void>::ok();
}
bool SamePayload(const v1::Record& r, const v1::OrderRequest& q) {
    return r.order_id() == q.order_id() && r.sku() == q.sku() && r.qty() == q.qty() &&
           r.discount_cents() == q.discount_cents();
}
void StorageHop(const ServiceContext& ctx, const char* service, const char* method,
                const std::string& id, bool ok, int code) {
    std::printf("event=storage_hop service=%s method=%s order_id=%s request_id=%s pid=%s instance=%s ok=%d code=%d\n",
                service, method, SafeToken(id) ? id.c_str() : "invalid", InboundRequestId(ctx).c_str(),
                Node().process_id.c_str(), Node().instance_id.c_str(), ok ? 1 : 0, code);
    std::fflush(stdout);
}
#ifdef EC_STORAGE_AVAILABLE
using bbt::infra::mongo::MongoDocument;
bbt::infra::CallOptions StorageBudget(const ServiceContext& ctx) {
    return bbt::infra::CallOptions{ctx.request().value()->deadline};
}
namespace {
// protobuf 记录以十六进制字符串存在真实 Mongo，避免引入第二套业务 schema。
std::string Encode(const v1::Record& r) {
    const auto digit = [](int n) { return static_cast<char>(n < 10 ? '0' + n : 'a' + n - 10); };
    std::string out;
    for (unsigned char c : r.SerializeAsString()) {
        out += digit(c >> 4);
        out += digit(c & 15);
    }
    return out;
}
result<v1::Record> Decode(const MongoDocument& d) {
    auto value = d.GetString("record");
    if (!value)
        return result<v1::Record>::err(value.error());
    std::string bytes;
    const auto& hex = value.value();
    const auto digit = [](char c) { return c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10
                                                                                                 : -1; };
    bool valid = hex.size() % 2 == 0;
    for (std::size_t i = 0; valid && i < hex.size(); i += 2) {
        const int a = digit(hex[i]), b = digit(hex[i + 1]);
        if (a < 0 || b < 0)
            valid = false;
        else
            bytes += static_cast<char>((a << 4) | b);
    }
    v1::Record record;
    if (!valid || !record.ParseFromString(bytes))
        return result<v1::Record>::err(MakeError(ErrorCode::ProtocolError, "corrupt persisted record"));
    return result<v1::Record>::ok(std::move(record));
}
result<MongoDocument> Filter(const std::string& id) {
    if (!SafeToken(id))
        return result<MongoDocument>::err(MakeError(ErrorCode::InvalidArgument, "invalid order id"));
    return MongoDocument::FromJson("{\"_id\":\"" + id + "\"}");
}
} // namespace
result<v1::Record> ReadRecord(MongoDbResource& db, const std::string& id,
                              const bbt::infra::CallOptions& options) {
    auto filter = Filter(id);
    if (!filter)
        return result<v1::Record>::err(filter.error());
    auto found = bbt_co_await db.FindOne(filter.value(), options);
    if (!found)
        return result<v1::Record>::err(found.error());
    if (!found.value())
        return result<v1::Record>::err(MakeError(ErrorCode::NotFound, "record not found"));
    return Decode(*found.value());
}
result<v1::Record> InsertRecord(MongoDbResource& db, const v1::Record& r,
                                const bbt::infra::CallOptions& options) {
    auto filter = Filter(r.order_id());
    if (!filter)
        return result<v1::Record>::err(filter.error());
    auto doc = MongoDocument::FromJson("{\"_id\":\"" + r.order_id() + "\",\"state\":\"" + r.state() +
                                       "\",\"record\":\"" + Encode(r) + "\"}");
    if (!doc)
        return result<v1::Record>::err(doc.error());
    auto inserted = bbt_co_await db.InsertOne(doc.value(), options);
    if (inserted)
        return result<v1::Record>::ok(r);
    // 只在确认重复键时读回，未知结果和其他错误原样返回。
    if (inserted.error().backend_code != 11000)
        return result<v1::Record>::err(inserted.error());
    return ReadRecord(db, r.order_id(), options);
}
result<void> MarkSettled(MongoDbResource& db, const v1::Record& r,
                         const bbt::infra::CallOptions& options) {
    auto filter = Filter(r.order_id());
    if (!filter)
        return result<void>::err(filter.error());
    auto update = MongoDocument::FromJson("{\"$set\":{\"state\":\"settled\",\"record\":\"" + Encode(r) + "\"}}");
    if (!update)
        return result<void>::err(update.error());
    auto changed = bbt_co_await db.UpdateOne(filter.value(), update.value(), options);
    if (!changed)
        return result<void>::err(changed.error());
    if (changed.value().matched != 1)
        return result<void>::err(MakeError(ErrorCode::NotFound, "pending order missing"));
    return result<void>::ok();
}
#endif
} // namespace ecommerce
