#include <bbt/framework/MongoDbResource.hpp>

namespace bbt::framework {

MongoDbResource::MongoDbResource(detail::ConstructKey key, Options options)
    : CoComponent(std::move(key)), m_options(std::move(options)) {}

bbt_co_async(bbt::infra::result<void>) MongoDbResource::InsertOne(
    const bbt::infra::mongo::MongoDocument& doc,
    const bbt::infra::CallOptions& options) {
    return bbt_co_await m_coll->InsertOne(doc, options);
}

bbt_co_async(
    bbt::infra::result<std::optional<bbt::infra::mongo::MongoDocument>>)
    MongoDbResource::FindOne(const bbt::infra::mongo::MongoDocument& filter,
                             const bbt::infra::CallOptions& options) {
    return bbt_co_await m_coll->FindOne(filter, options);
}

bbt_co_async(bbt::infra::result<bbt::infra::mongo::MongoUpdateResult>)
    MongoDbResource::UpdateOne(const bbt::infra::mongo::MongoDocument& filter,
                               const bbt::infra::mongo::MongoDocument& update,
                               const bbt::infra::CallOptions& options) {
    return bbt_co_await m_coll->UpdateOne(filter, update, options);
}

bbt_co_async(bbt::infra::result<std::uint64_t>) MongoDbResource::DeleteOne(
    const bbt::infra::mongo::MongoDocument& filter,
    const bbt::infra::CallOptions& options) {
    return bbt_co_await m_coll->DeleteOne(filter, options);
}

bbt_co_async(bbt::infra::result<bbt::infra::mongo::MongoInsertManyResult>)
    MongoDbResource::InsertMany(const std::vector<bbt::infra::mongo::MongoDocument>& docs,
                                const bbt::infra::CallOptions& options) {
    return bbt_co_await m_coll->InsertMany(docs, options);
}

bbt_co_async(
    bbt::infra::result<std::vector<bbt::infra::mongo::MongoDocument>>)
    MongoDbResource::Find(const bbt::infra::mongo::MongoDocument& filter,
                          const bbt::infra::mongo::FindOptions& find_options,
                          const bbt::infra::CallOptions& options) {
    return bbt_co_await m_coll->Find(filter, find_options, options);
}

bbt_co_async(bbt::infra::result<bbt::infra::mongo::MongoUpdateResult>)
    MongoDbResource::UpdateMany(const bbt::infra::mongo::MongoDocument& filter,
                                const bbt::infra::mongo::MongoDocument& update,
                                const bbt::infra::CallOptions& options) {
    return bbt_co_await m_coll->UpdateMany(filter, update, options);
}

bbt_co_async(bbt::infra::result<std::uint64_t>) MongoDbResource::DeleteMany(
    const bbt::infra::mongo::MongoDocument& filter,
    const bbt::infra::CallOptions& options) {
    return bbt_co_await m_coll->DeleteMany(filter, options);
}

bbt_co_async(bbt::infra::result<std::uint64_t>) MongoDbResource::CountDocuments(
    const bbt::infra::mongo::MongoDocument& filter,
    const bbt::infra::CallOptions& options) {
    return bbt_co_await m_coll->CountDocuments(filter, options);
}

bool MongoDbResource::IsClosed() const noexcept {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_closed;
}

bool MongoDbResource::coll_closed() const noexcept {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_coll_closed;
}

bbt::infra::result<std::shared_ptr<bbt::infra::mongo::CoMongoColl>>
MongoDbResource::SiblingCollection(bbt::infra::mongo::MongoTarget target) {
    return m_db->Collection(std::move(target));
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
    m_db = std::move(created).value();
    if (auto started = m_db->Start(); !started) {
        m_db.reset();
        return result<void>::err(std::move(started.error()));
    }
    auto coll = m_db->Collection(m_options.Target());
    if (!coll) {
        Close();
        return result<void>::err(std::move(coll.error()));
    }
    m_coll = std::move(coll).value();
    return result<void>::ok();
}

result<void> MongoDbResource::OnStop() {
    Close();
    return result<void>::ok();
}

void MongoDbResource::Close() noexcept {
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (m_closed)
            return;
        m_closed = true;
    }
    if (m_coll) {
        m_coll->Close();
        std::lock_guard<std::mutex> lk(m_mtx);
        m_coll_closed = true;
    }
    if (m_db)
        m_db->Close();
}

} // namespace bbt::framework
