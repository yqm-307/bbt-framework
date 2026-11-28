#include <bbt/framework/MongoDbResource.hpp>
#include <bbt/framework/internal/PublishedResource.hpp>

namespace bbt::framework {

MongoDbResource::MongoDbResource(detail::ConstructKey key, Options options)
    : CoComponent(std::move(key)), m_options(std::move(options)) {}

bbt_co_async(bbt::infra::result<void>) MongoDbResource::InsertOne(
    const bbt::infra::mongo::MongoDocument& doc,
    const bbt::infra::CallOptions& options) {
    bbt_co_return detail::WithPublishedResource(m_handles, [&](auto& h) { return bbt_co_await h.coll->InsertOne(doc, options); });
}

bbt_co_async(
    bbt::infra::result<std::optional<bbt::infra::mongo::MongoDocument>>)
    MongoDbResource::FindOne(const bbt::infra::mongo::MongoDocument& filter,
                             const bbt::infra::CallOptions& options) {
    bbt_co_return detail::WithPublishedResource(m_handles, [&](auto& h) { return bbt_co_await h.coll->FindOne(filter, options); });
}

bbt_co_async(bbt::infra::result<bbt::infra::mongo::MongoUpdateResult>)
    MongoDbResource::UpdateOne(const bbt::infra::mongo::MongoDocument& filter,
                               const bbt::infra::mongo::MongoDocument& update,
                               const bbt::infra::CallOptions& options) {
    bbt_co_return detail::WithPublishedResource(m_handles, [&](auto& h) { return bbt_co_await h.coll->UpdateOne(filter, update, options); });
}

bbt_co_async(bbt::infra::result<std::uint64_t>) MongoDbResource::DeleteOne(
    const bbt::infra::mongo::MongoDocument& filter,
    const bbt::infra::CallOptions& options) {
    bbt_co_return detail::WithPublishedResource(m_handles, [&](auto& h) { return bbt_co_await h.coll->DeleteOne(filter, options); });
}

bbt_co_async(bbt::infra::result<bbt::infra::mongo::MongoInsertManyResult>)
    MongoDbResource::InsertMany(const std::vector<bbt::infra::mongo::MongoDocument>& docs,
                                const bbt::infra::CallOptions& options) {
    bbt_co_return detail::WithPublishedResource(m_handles, [&](auto& h) { return bbt_co_await h.coll->InsertMany(docs, options); });
}

bbt_co_async(
    bbt::infra::result<std::vector<bbt::infra::mongo::MongoDocument>>)
    MongoDbResource::Find(const bbt::infra::mongo::MongoDocument& filter,
                          const bbt::infra::mongo::FindOptions& find_options,
                          const bbt::infra::CallOptions& options) {
    bbt_co_return detail::WithPublishedResource(m_handles, [&](auto& h) { return bbt_co_await h.coll->Find(filter, find_options, options); });
}

bbt_co_async(bbt::infra::result<bbt::infra::mongo::MongoUpdateResult>)
    MongoDbResource::UpdateMany(const bbt::infra::mongo::MongoDocument& filter,
                                const bbt::infra::mongo::MongoDocument& update,
                                const bbt::infra::CallOptions& options) {
    bbt_co_return detail::WithPublishedResource(m_handles, [&](auto& h) { return bbt_co_await h.coll->UpdateMany(filter, update, options); });
}

bbt_co_async(bbt::infra::result<std::uint64_t>) MongoDbResource::DeleteMany(
    const bbt::infra::mongo::MongoDocument& filter,
    const bbt::infra::CallOptions& options) {
    bbt_co_return detail::WithPublishedResource(m_handles, [&](auto& h) { return bbt_co_await h.coll->DeleteMany(filter, options); });
}

bbt_co_async(bbt::infra::result<std::uint64_t>) MongoDbResource::CountDocuments(
    const bbt::infra::mongo::MongoDocument& filter,
    const bbt::infra::CallOptions& options) {
    bbt_co_return detail::WithPublishedResource(m_handles, [&](auto& h) { return bbt_co_await h.coll->CountDocuments(filter, options); });
}

bool MongoDbResource::IsClosed() const noexcept {
    return m_closure.load() != Closure::Open;
}

bool MongoDbResource::coll_closed() const noexcept {
    return m_closure.load() == Closure::CollectionClosed;
}

bbt::infra::result<std::shared_ptr<bbt::infra::mongo::CoMongoColl>>
MongoDbResource::SiblingCollection(bbt::infra::mongo::MongoTarget target) {
    return detail::WithPublishedResource(m_handles, [&](auto& h) { return h.db->Collection(std::move(target)); });
}

MongoDbResource::~MongoDbResource() noexcept {
    Close();
}

result<void> MongoDbResource::OnStart() {
    if (auto v = bbt::infra::mongo::ValidateMongoRuntimeConfig(
            m_options.Config());
        !v)
        return result<void>::err(std::move(v.error()));
    if (auto v = bbt::infra::mongo::ValidateMongoTarget(
            m_options.Target());
        !v)
        return result<void>::err(std::move(v.error()));
    auto created = bbt::infra::mongo::CoMongoDb::Create(
        m_options.Config());
    if (!created)
        return result<void>::err(std::move(created.error()));
    auto db = std::move(created).value();
    if (auto started = db->Start(); !started)
        return started;
    auto coll = db->Collection(m_options.Target());
    if (!coll)
        return result<void>::err(std::move(coll.error()));
    std::atomic_store(&m_handles, std::make_shared<Handles>(Handles{std::move(db), std::move(coll).value()}));
    return result<void>::ok();
}

result<void> MongoDbResource::OnStop() {
    Close();
    return result<void>::ok();
}

void MongoDbResource::Close() noexcept {
    if (IsClosed())
        return;
    auto handles = std::atomic_exchange(&m_handles, decltype(m_handles){});
    if (!handles) {
        m_closure.store(Closure::NoCollection);
        return;
    }
    handles->coll->Close();
    m_closure.store(Closure::CollectionClosed);
    handles->db->Close();
}

} // namespace bbt::framework
