#include <bbt/framework/RedisNetResource.hpp>
#include <bbt/framework/internal/PublishedResource.hpp>

#include <new>
#include <thread>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

namespace bbt::framework {

struct RedisNetResource::EngineExit {
    const std::shared_ptr<bbt::coroutine::sync::CoWaiter> waiter = bbt::coroutine::sync::CoWaiter::Create();
    std::atomic_bool done{false};
};

RedisNetResource::RedisNetResource(detail::ConstructKey key, Options options)
    : CoComponent(std::move(key)), m_options(std::move(options)) {}

bbt_co_async(bbt::infra::result<void>) RedisNetResource::Connect(
    const bbt::infra::CallOptions& options) {
    return detail::WithPublishedResource(m_client, [&](auto& cli) { return bbt_co_await cli.Connect(options); });
}

bbt_co_async(bbt::infra::result<void>) RedisNetResource::Ping(
    const bbt::infra::CallOptions& options) {
    return detail::WithPublishedResource(m_client, [&](auto& cli) { return bbt_co_await cli.Ping(options); });
}

bbt_co_async(bbt::infra::result<std::optional<std::string>>) RedisNetResource::Get(
    std::string_view key,
    const bbt::infra::CallOptions& options) {
    return detail::WithPublishedResource(m_client, [&](auto& cli) { return bbt_co_await cli.Get(key, options); });
}

bbt_co_async(bbt::infra::result<void>) RedisNetResource::Set(
    std::string_view key, std::string_view value,
    const bbt::infra::CallOptions& options) {
    return detail::WithPublishedResource(m_client, [&](auto& cli) { return bbt_co_await cli.Set(key, value, options); });
}

bbt_co_async(bbt::infra::result<bool>) RedisNetResource::Exists(
    std::string_view key,
    const bbt::infra::CallOptions& options) {
    return detail::WithPublishedResource(m_client, [&](auto& cli) { return bbt_co_await cli.Exists(key, options); });
}

bbt_co_async(bbt::infra::result<std::uint64_t>) RedisNetResource::Delete(
    std::vector<std::string> keys,
    const bbt::infra::CallOptions& options) {
    return detail::WithPublishedResource(m_client, [&](auto& cli) { return bbt_co_await cli.Delete(std::move(keys), options); });
}

bool RedisNetResource::IsClosed() const noexcept {
    return m_closure.load() != Closure::Open;
}

bool RedisNetResource::engine_stopped() const noexcept {
    return m_closure.load() == Closure::Stopped;
}

RedisNetResource::~RedisNetResource() noexcept {
    Close();
}

result<void> RedisNetResource::OnStart() {
    m_engine = bbt::infra::net::Engine::Create(nullptr);
    m_engine_exit = std::make_shared<EngineExit>();
    m_engine_result = std::async(std::launch::async, [engine = m_engine, exit = m_engine_exit] {
        bbt::infra::net::EngineConfig config;
        config.runtime_progress = bbt::infra::net::RuntimeProgress::Independent;
        std::error_code error;
        try {
            error = engine->Run(config);
        } catch (...) {
            error = std::make_error_code(std::errc::io_error);
        }
        exit->done.store(true);
        exit->waiter->Notify();
        return error;
    });
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
    auto fail_start = [this](Error error) {
        if (auto close_error = Close())
            error.message += "; engine shutdown: " + close_error.message();
        return result<void>::err(std::move(error));
    };
    if (m_engine->Phase() != bbt::infra::net::EnginePhase::Running)
        return fail_start(MakeError(ErrorCode::RuntimeUnavailable, "redis-net: engine not running"));
    auto created = bbt::infra::redis::CoRedisCli::Create(
        m_engine, m_options.Address());
    if (!created)
        return fail_start(std::move(created.error()));
    std::atomic_store(&m_client, std::move(created.value()));
    return result<void>::ok();
}

result<void> RedisNetResource::OnStop() {
    if (auto error = Close())
        return result<void>::err(MakeError(ErrorCode::InternalError, "redis-net: " + error.message()));
    return result<void>::ok();
}

std::error_code RedisNetResource::Close() noexcept try {
    auto client = std::atomic_exchange(&m_client, decltype(m_client){});
    if (!IsClosed())
        m_closure.store(Closure::Sealed);
    if (client)
        client->Close();
    if (!m_engine)
        return {};
    m_engine->Close();
    const bool joined = m_engine_result.valid();
    // Engine::Run 收口仍需其 reactor worker；OnStop 可能就在这个 worker 上。
    // Run 真正返回后才取 future，宿主线程则仍可同步等待。
    if (joined && bbt::coroutine::IsInCoroutineContext() && !m_engine_exit->done.load()) {
        const auto status = m_engine_exit->waiter->WaitWithCallback({}, [exit = m_engine_exit] {
            if (exit->done.load())
                exit->waiter->Notify();
            return true;
        });
        if (status != bbt::coroutine::WaitStatus::Completed)
            return std::make_error_code(status == bbt::coroutine::WaitStatus::Cancelled
                                            ? std::errc::operation_canceled
                                            : std::errc::resource_unavailable_try_again);
    }
    const auto run_error = joined ? m_engine_result.get() : std::error_code{};
    const auto phase = m_engine->Phase();
    using bbt::infra::net::EnginePhase;
    if (phase == EnginePhase::Closing || m_engine_finalize.valid()) {
        // Run 已返回但保留 Closing（解绑失败/安装未结）：首次发现保留引擎对象
        // 与原错误，交下一次卸载重试；重试在原生域补一次同代物理收口。
        if (joined)
            return run_error ? run_error : std::make_error_code(std::errc::device_or_resource_busy);
        if (!m_engine_finalize.valid()) {
            m_engine_finalize_exit = std::make_shared<EngineExit>();
            m_engine_finalize = std::async(std::launch::async,
                                           [engine = m_engine, exit = m_engine_finalize_exit] {
                                               std::error_code error;
                                               try {
                                                   error = engine->FinalizeNative();
                                               } catch (...) {
                                                   error = std::make_error_code(std::errc::io_error);
                                               }
                                               exit->done.store(true);
                                               exit->waiter->Notify();
                                               return error;
                                           });
        }
        // FinalizeNative 拒绝协程域：收口线程独立于 reactor worker，协程侧只
        // 用既有等待原语等完成信号。等待被取消时线程继续收口，结果由下一次
        // Close 收取；这里不做无界自动重试。
        if (bbt::coroutine::IsInCoroutineContext() && !m_engine_finalize_exit->done.load()) {
            const auto status = m_engine_finalize_exit->waiter->WaitWithCallback({}, [exit = m_engine_finalize_exit] {
                if (exit->done.load())
                    exit->waiter->Notify();
                return true;
            });
            if (status != bbt::coroutine::WaitStatus::Completed)
                return std::make_error_code(status == bbt::coroutine::WaitStatus::Cancelled
                                                ? std::errc::operation_canceled
                                                : std::errc::resource_unavailable_try_again);
        }
        const auto finalize_error = m_engine_finalize.get();
        m_engine_finalize_exit.reset();
        if (m_engine->Phase() != EnginePhase::Stopped)
            return finalize_error ? finalize_error : std::make_error_code(std::errc::device_or_resource_busy);
        // Run 的错误已由首次卸载上报并记入生命周期诊断。重试返回本次收口结果。
        m_engine.reset();
        return finalize_error;
    }
    if (phase != EnginePhase::Stopped && phase != EnginePhase::Idle)
        return run_error ? run_error : std::make_error_code(std::errc::device_or_resource_busy);
    // Run 的错误交给本次调用。引擎已收口后，后续卸载可以继续。
    if (joined && !run_error && phase == EnginePhase::Stopped)
        m_closure.store(Closure::Stopped);
    m_engine.reset();
    return run_error;
} catch (const std::bad_alloc&) {
    // 等待登记失败时，future 与引擎仍由组件持有，生命周期重试可继续等待。
    return std::make_error_code(std::errc::not_enough_memory);
} catch (const std::system_error& error) {
    return error.code();
} catch (...) {
    return std::make_error_code(std::errc::io_error);
}

} // namespace bbt::framework
