#include <bbt/framework/RedisNetResource.hpp>

namespace bbt::framework {

RedisNetResource::RedisNetResource(detail::ConstructKey key, Options options)
    : CoComponent(std::move(key)), m_options(std::move(options)) {}

bbt_co_async(bbt::infra::result<void>) RedisNetResource::Connect(
    const bbt::infra::CallOptions& options) {
    return bbt_co_await m_client->Connect(options);
}

bbt_co_async(bbt::infra::result<void>) RedisNetResource::Ping(
    const bbt::infra::CallOptions& options) {
    return bbt_co_await m_client->Ping(options);
}

bbt_co_async(bbt::infra::result<std::optional<std::string>>) RedisNetResource::Get(
    std::string_view key,
    const bbt::infra::CallOptions& options) {
    return bbt_co_await m_client->Get(key, options);
}

bbt_co_async(bbt::infra::result<void>) RedisNetResource::Set(
    std::string_view key, std::string_view value,
    const bbt::infra::CallOptions& options) {
    return bbt_co_await m_client->Set(key, value, options);
}

bbt_co_async(bbt::infra::result<bool>) RedisNetResource::Exists(
    std::string_view key,
    const bbt::infra::CallOptions& options) {
    return bbt_co_await m_client->Exists(key, options);
}

bbt_co_async(bbt::infra::result<std::uint64_t>) RedisNetResource::Delete(
    std::vector<std::string> keys,
    const bbt::infra::CallOptions& options) {
    return bbt_co_await m_client->Delete(std::move(keys), options);
}

bool RedisNetResource::IsClosed() const noexcept {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_closed;
}

bool RedisNetResource::engine_stopped() const noexcept {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_engine_stopped;
}

RedisNetResource::~RedisNetResource() noexcept {
    Close();
}

result<void> RedisNetResource::OnStart() {
    m_engine = bbt::infra::net::Engine::Create(nullptr);
    std::promise<std::error_code> done;
    auto run_result = done.get_future();
    m_engine_thread = std::thread(
        [engine = m_engine, done = std::move(done)]() mutable {
            std::error_code ec;
            try {
                bbt::infra::net::EngineConfig config;
                config.runtime_progress =
                    bbt::infra::net::RuntimeProgress::Independent;
                ec = engine->Run(config);
            } catch (...) {
                ec = std::make_error_code(std::errc::io_error);
            }
            done.set_value(ec);
        });
    m_engine_result = std::move(run_result);
    /**
     * 等网络线程就绪再创建客户端。线程提前退出或超过启动期限，都不再等待。
     */
    const auto until = std::chrono::steady_clock::now() + kEngineRunWait;
    while (m_engine->Phase() != bbt::infra::net::EnginePhase::Running) {
        if (m_engine_result.wait_for(std::chrono::milliseconds{0}) ==
            std::future_status::ready) {
            break;
        }
        if (std::chrono::steady_clock::now() >= until)
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    if (m_engine->Phase() != bbt::infra::net::EnginePhase::Running) {
        Close();
        return result<void>::err(MakeError(
            ErrorCode::RuntimeUnavailable,
            "redis-net: engine not running"));
    }
    auto created = bbt::infra::redis::CoRedisCli::Create(
        m_engine, m_options.Address());
    if (!created) {
        Close();
        return result<void>::err(std::move(created.error()));
    }
    m_client = std::move(created.value());
    return result<void>::ok();
}

result<void> RedisNetResource::OnStop() {
    Close();
    return result<void>::ok();
}

void RedisNetResource::Close() noexcept {
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (m_closed)
            return;
        m_closed = true;
    }
    /**
     * client 层先停：不再接纳新命令，已发布连接 RequestClose。
     */
    if (m_client)
        m_client->Close();
    /**
     * 线程退出不等于正常停止，还要检查 Run 的结果和 Engine 的最终状态。
     */
    if (m_engine)
        m_engine->RequestClose();
    if (m_engine_thread.joinable())
        m_engine_thread.join();
    std::error_code ec{};
    if (m_engine_result.valid())
        ec = m_engine_result.get();
    const bool stopped = !ec && m_engine &&
                         (m_engine->Phase() == bbt::infra::net::EnginePhase::Stopped);
    std::lock_guard<std::mutex> lk(m_mtx);
    m_engine_stopped = stopped;
}

} // namespace bbt::framework
