#include <bbt/framework/internal/InfraHttpHost.hpp>

#include <algorithm>
#include <chrono>
#include <utility>
#include <vector>
#include <boost/scope/scope_exit.hpp>
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
#include <bbt/infra/rpc/HttpTransport.hpp>
#endif
#if defined(BBT_FRAMEWORK_HAS_RPC_TCP)
#include <bbt/infra/rpc/TcpTransport.hpp>
#endif

namespace bbt::framework {
namespace {
namespace net = bbt::infra::net;
constexpr std::chrono::milliseconds kEngineRunWait{5000};
bbt::infra::Error EngineError(std::error_code ec, const char* what) {
    return MakeError(ErrorCode::RuntimeUnavailable,
                     std::string(what) + ": " + (ec ? ec.message() : "invalid engine phase"));
}
result<void> RunEngine(const std::shared_ptr<net::Engine>& engine,
                       net::EngineConfig config, std::thread& runner,
                       std::shared_future<std::error_code>& finished) {
    std::promise<std::error_code> done;
    finished = done.get_future().share();
    runner = std::thread([engine, config = std::move(config), done = std::move(done)]() mutable {
        try {
            done.set_value(engine->Run(std::move(config)));
        } catch (...) { done.set_value(std::make_error_code(std::errc::io_error)); }
    });
    const auto deadline = std::chrono::steady_clock::now() + kEngineRunWait;
    while (engine->Phase() != net::EnginePhase::Running) {
        if (finished.wait_for(std::chrono::milliseconds{0}) == std::future_status::ready)
            return result<void>::err(EngineError({}, "Engine Run failed before Running"));
        if (std::chrono::steady_clock::now() >= deadline)
            return result<void>::err(EngineError(std::make_error_code(std::errc::timed_out), "Engine startup"));
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    return result<void>::ok();
}
} // namespace

InfraHttpHost::InfraHttpHost(bbt::infra::NetworkLimits limits, bbt::infra::ListenAddress listen)
    : m_limits(std::move(limits)), m_listen(std::move(listen)), m_state(std::make_shared<HandlerState>()) {}
void InfraHttpHost::InstallHandler(bbt::infra::http::ServerHandler handler) {
    m_state->http = std::move(handler);
}
void InfraHttpHost::InstallRpcHandler(bbt::infra::RpcHandler handler) {
    m_state->rpc = std::move(handler);
}
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
void InfraHttpHost::SetRpcInboundTransport(RpcInboundBridge bridge) {
    m_inbound_bridge = bridge;
}
#endif

result<void> InfraHttpHost::Create() {
    if (auto valid = bbt::infra::ValidateNetworkLimits(m_limits); !valid)
        return valid;
    auto egress = bbt::infra::net::Engine::Create(std::shared_ptr<bbt::infra::net::RawHandler>{});
    bbt::infra::http::ClientOptions client_options;
    // A single host pool is shared by all framework HTTP profiles. Bound active
    // sockets by both quotas; framework ingress/governance remains above transport.
    client_options.limits.max_connections = std::min(m_limits.max_connections, m_limits.max_inflight);
    client_options.limits.max_keepalive_connections = std::min<std::size_t>(
        client_options.limits.max_keepalive_connections, client_options.limits.max_connections);
    client_options.limits.max_queue = 0;
    client_options.max_body_bytes = m_limits.max_body_bytes;
    client_options.max_request_body_bytes = m_limits.max_body_bytes;
    client_options.max_header_bytes = m_limits.max_header_bytes;
    client_options.timeout = bbt::infra::http::Timeout::Uniform(
        bbt::infra::http::PhaseTimeout::Milliseconds(m_limits.incoming_timeout));
    auto client = bbt::infra::http::Client::Create(egress, client_options);
    if (!client)
        return result<void>::err(std::move(client.error()));
    bbt::infra::http::ServerOptions options;
    options.limits = m_limits;
    options.max_target_bytes = std::min(options.max_target_bytes, m_limits.max_header_bytes);
    if (m_state->http || m_state->rpc) {
        auto engine = bbt::infra::net::Engine::Create(std::shared_ptr<bbt::infra::net::RawHandler>{});
        if (m_state->http && !m_state->rpc) {
            auto server = bbt::infra::http::Server::Create(engine, [state = m_state](bbt::infra::IncomingCallContext ctx, bbt::infra::http::IncomingRequest req) { return _InvokeHttp(state, std::move(ctx), std::move(req)); }, options);
            if (!server)
                return result<void>::err(std::move(server.error()));
            std::lock_guard<std::mutex> lock(m_mtx);
            m_server = std::move(server).value();
        } else {
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
            std::shared_ptr<bbt::infra::rpc::IRpcInboundTransport> inbound;
            if (m_inbound_bridge == RpcInboundBridge::TcpV1) {
#if defined(BBT_FRAMEWORK_HAS_RPC_TCP)
                bbt::infra::rpc::TcpRpcServerOptions tcp;
                tcp.limits = m_limits;
                auto made = bbt::infra::rpc::TcpRpcInboundTransport::Create(engine, tcp);
                if (!made)
                    return result<void>::err(std::move(made.error()));
                inbound = std::move(made).value();
#else
                return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable, "TcpV1 requires bbt::infra_rpc_tcp"));
#endif
            } else {
                auto made = bbt::infra::rpc::HttpRpcInboundTransport::Create(engine, options);
                if (!made)
                    return result<void>::err(std::move(made.error()));
                inbound = std::move(made).value();
            }
            auto server = bbt::infra::rpc::Server::Create(std::move(inbound),
                                                          [state = m_state](bbt::infra::IncomingCallContext ctx, bbt::infra::RpcEnvelope req) {
                                                              return _InvokeRpc(state, std::move(ctx), std::move(req));
                                                          });
            if (!server)
                return result<void>::err(std::move(server.error()));
            std::lock_guard<std::mutex> lock(m_mtx);
            m_rpc_server = std::move(server).value();
#else
            return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable, "RPC wire requires bbt::infra_rpc_net"));
#endif
        }
        std::lock_guard<std::mutex> lock(m_mtx);
        m_engine = std::move(engine);
    }
    std::lock_guard<std::mutex> lock(m_mtx);
    m_egress_engine = std::move(egress);
    m_client = std::move(client).value();
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    // Keep the RPC adapter owner with the host: destroying an individual egress
    // bridge must not close the HTTP pool shared with other profiles.
    auto rpc_client = bbt::infra::rpc::Client::Create(m_client);
    if (!rpc_client) return result<void>::err(std::move(rpc_client.error()));
    m_rpc_http_client = std::move(rpc_client).value();
#endif
    m_created = true;
    return result<void>::ok();
}

result<bbt::infra::http::Response> InfraHttpHost::_InvokeHttp(
    const std::shared_ptr<HandlerState>& state, bbt::infra::IncomingCallContext ctx,
    bbt::infra::http::IncomingRequest request) {
    {
        std::lock_guard<std::mutex> lock(state->mtx);
        if (!state->open) {
            return result<bbt::infra::http::Response>::err(
                state->close_reason ? *state->close_reason
                                    : MakeError(ErrorCode::RuntimeUnavailable,
                                                "inbound handler: host closed"));
        }
        ++state->inflight;
    }
    auto guard = boost::scope::make_scope_exit([state] {
        {
            std::lock_guard<std::mutex> lock(state->mtx);
            --state->inflight;
        }
        state->cv.notify_all();
    });
    return state->http(std::move(ctx), std::move(request));
}
result<bbt::infra::RpcEnvelope> InfraHttpHost::_InvokeRpc(
    const std::shared_ptr<HandlerState>& state, bbt::infra::IncomingCallContext ctx,
    bbt::infra::RpcEnvelope request) {
    {
        std::lock_guard<std::mutex> lock(state->mtx);
        if (!state->open) {
            return result<bbt::infra::RpcEnvelope>::err(
                state->close_reason ? *state->close_reason
                                    : MakeError(ErrorCode::RuntimeUnavailable,
                                                "inbound rpc handler: host closed"));
        }
        ++state->inflight;
    }
    auto guard = boost::scope::make_scope_exit([state] {
        {
            std::lock_guard<std::mutex> lock(state->mtx);
            --state->inflight;
        }
        state->cv.notify_all();
    });
    return state->rpc(std::move(ctx), std::move(request));
}

result<void> InfraHttpHost::Start() {
    if (!m_created)
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable, "network host not created"));
    bbt::infra::net::EngineConfig config;
    config.runtime_progress = bbt::infra::net::RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    // Client-only engine has no accept consumer and uses one registered driver.
    if (auto started = RunEngine(m_egress_engine, config, m_egress_thread, m_egress_result); !started)
        return started;
    if (!m_engine)
        return result<void>::ok();
    std::error_code ec;
    auto address = bbt::infra::net::Address::Ip(m_listen.host, m_listen.port, ec);
    if (ec)
        return result<void>::err(EngineError(ec, "invalid listen address"));
    config.listen_addresses = {address};
    if (auto started = RunEngine(m_engine, config, m_engine_thread, m_engine_result); !started)
        return started;
    std::vector<bbt::infra::net::Address> addresses;
    if (auto error = m_engine->LocalAddresses(addresses); error || addresses.empty())
        return result<void>::err(EngineError(error, "LocalAddresses failed"));
    if (m_server) {
        if (auto r = m_server->Start(); !r)
            return r;
    }
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    if (m_rpc_server) {
        if (auto r = m_rpc_server->Start(); !r)
            return r;
    }
#endif
    std::lock_guard<std::mutex> lock(m_mtx);
    m_bound = {m_listen.host, addresses.front().Port()};
    m_bound_ready = true;
    return result<void>::ok();
}

void InfraHttpHost::StopAccepting() noexcept {
    {
        std::lock_guard<std::mutex> lock(m_state->mtx);
        m_state->open = false;
    }
    if (auto server = http_server())
        server->StopAccepting();
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    if (auto server = rpc_server())
        server->StopAccepting();
#endif
}
result<void> InfraHttpHost::WaitHandlersDone(bbt::coroutine::Deadline deadline) {
    {
        std::unique_lock<std::mutex> lock(m_state->mtx);
        // Close 先到：立即把关闭原因交给等待者，不进入排空/收口。
        if (m_state->close_reason) { return result<void>::err(*m_state->close_reason); }
        const auto done = m_state->cv.wait_until(lock, deadline, [this] {
            return m_state->inflight == 0 || m_state->close_reason.has_value();
        });
        if (m_state->inflight != 0) {
            if (m_state->close_reason) { return result<void>::err(*m_state->close_reason); }
            (void)done;
            return result<void>::err(
                MakeError(ErrorCode::TimedOut, "inbound handlers still running"));
        }
    }
    // 排空被证实：封口并等 server 的真实排空（拒新 + 已授权 reply 写终态），
    // 再做物理收口（listener/callback/连接 Released）。Close 打断时 Shutdown
    // 直接返回其关闭原因，后续物理收口不再进行。
    // infra 新契约：Shutdown 曾超时后重复调用（仍 Draining 且有在途）返回
    // Overloaded 而非续等——本宿主把该 Busy 续等转接到可选 Drain（同一排空
    // 谓词），保留「预算内一次 Shutdown + 无界续等一次 Drain」的两段语义。
    auto shutdown_or_drain = [](auto& server, bbt::coroutine::Deadline deadline)
        -> result<void> {
        if (auto r = server->Shutdown(deadline); !r) {
            if (r.error().code == ErrorCode::Overloaded)
                return server->Drain(deadline);
            return r;
        }
        return result<void>::ok();
    };
    if (auto server = http_server()) {
        if (auto r = shutdown_or_drain(server, deadline); !r) { return r; }
        server->Close();
        if (auto r = server->WaitStopped(deadline); !r) { return r; }
    }
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    if (auto server = rpc_server()) {
        if (auto r = shutdown_or_drain(server, deadline); !r) { return r; }
        server->Close();
        if (auto r = server->WaitStopped(deadline); !r) { return r; }
    }
#endif
    return result<void>::ok();
}
bool InfraHttpHost::_JoinEngine(const std::shared_ptr<bbt::infra::net::Engine>& engine,
                                std::thread& runner, std::shared_future<std::error_code>& finished) {
    if (!engine)
        return true;
    engine->RequestClose();
    if (!runner.joinable())
        return true;
    // 短同步 join：驱动线程自身退出是毫秒级；但 Engine 的 owner 收口可能排在
    // 仍在业务 handler 里的协程之后，不能在这里等业务完成。未就绪就把 join
    // 责任留给析构（绝不 detach、不提前 reset owner）。
    if (finished.valid() &&
        finished.wait_for(std::chrono::milliseconds{200}) != std::future_status::ready)
        return false;
    runner.join();
    if (!finished.valid())
        return engine->Phase() == bbt::infra::net::EnginePhase::Idle ||
               engine->Phase() == bbt::infra::net::EnginePhase::Stopped;
    const auto ec = finished.get();
    if (ec || engine->Phase() != bbt::infra::net::EnginePhase::Stopped) {
        std::lock_guard<std::mutex> lock(m_mtx);
        m_close_error = EngineError(ec, "Engine did not reach Stopped");
        return false;
    }
    return true;
}
void InfraHttpHost::Close() noexcept {
    Close(MakeError(ErrorCode::Closed, "framework host: closed by owner"));
}

void InfraHttpHost::Close(bbt::infra::Error reason) noexcept {
    // call_once：并发/重复关闭阻塞到首次完成，析构再调为空操作——不会留下
    // 仍 joinable 的 std::thread 让析构 terminate。Close 本身不等待业务
    // handler：只封口、发原因、RequestClose 并 join 自有驱动线程。
    std::call_once(m_close_once, [this, reason = std::move(reason)]() mutable {
        _Close(std::move(reason));
    });
}

void InfraHttpHost::_Close(bbt::infra::Error reason) noexcept {
    try {
        {
            std::lock_guard<std::mutex> lock(m_state->mtx);
            m_state->open = false;
            if (!m_state->close_reason) { m_state->close_reason = reason; }
        }
        m_state->cv.notify_all();   // 唤醒 WaitHandlersDone 的排空等待
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (!m_close_reason) { m_close_reason = reason; }
        }
        StopAccepting();
        if (auto server = http_server()) { server->Close(reason); }
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
        if (auto server = rpc_server()) { server->Close(reason); }
#endif
        if (auto client = http_client()) { client->Close(reason); }
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
        if (auto client = rpc_http_client()) { client->Close(reason); }
#endif
        // 无条件 join：旧路径失败即 return 会保留 joinable 线程，析构时
        // std::terminate。join 只在驱动线程可即时退出时完成；仍被业务占用
        // 时把 join 留给析构（见 _JoinEngine / ~InfraHttpHost）。
        const bool inbound_stopped = _JoinEngine(m_engine, m_engine_thread, m_engine_result);
        const bool outbound_stopped = _JoinEngine(m_egress_engine, m_egress_thread, m_egress_result);
        std::lock_guard<std::mutex> lock(m_mtx);
        // 已 Close 的 server/client owner 与驱动线程寿命解耦，可立即释放。
        m_server.reset();
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
        m_rpc_server.reset();
        m_rpc_http_client.reset();
#endif
        m_client.reset();
        if (inbound_stopped) { m_engine.reset(); }
        if (outbound_stopped) { m_egress_engine.reset(); }
        m_bound_ready = false;
        m_bound = {};
        m_created = false;
    } catch (...) {
        // noexcept 契约：Close 必须可安全重入/析构调用，不让分配失败逃逸。
    }
}

InfraHttpHost::~InfraHttpHost() noexcept {
    Close();
    // Close 只在驱动线程可即时退出时 join；业务仍占用驱动 owner 时把 join
    // 责任留到这里。必须 join 而不是 detach：析构不能留下仍访问本对象的线程，
    // 也不能因仍 joinable 的 std::thread terminate。
    if (m_engine_thread.joinable()) { m_engine_thread.join(); }
    if (m_egress_thread.joinable()) { m_egress_thread.join(); }
}

std::optional<bbt::infra::Error> InfraHttpHost::CloseReason() const noexcept {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_close_reason;
}
std::optional<bbt::infra::ListenAddress> InfraHttpHost::bound_address() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_bound_ready ? std::optional<bbt::infra::ListenAddress>{m_bound} : std::nullopt;
}
std::string InfraHttpHost::bound_endpoint() const {
    const auto address = bound_address();
    return address ? address->host + ":" + std::to_string(address->port) : std::string{};
}
std::shared_ptr<bbt::infra::http::Server> InfraHttpHost::http_server() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_server;
}
std::shared_ptr<bbt::infra::net::Engine> InfraHttpHost::egress_engine() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_egress_engine;
}
std::shared_ptr<bbt::infra::http::Client> InfraHttpHost::http_client() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_client;
}
std::optional<bbt::infra::Error> InfraHttpHost::close_error() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_close_error;
}
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
std::shared_ptr<bbt::infra::rpc::Client> InfraHttpHost::rpc_http_client() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_rpc_http_client;
}
std::shared_ptr<bbt::infra::rpc::Server> InfraHttpHost::rpc_server() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_rpc_server;
}
#endif
} // namespace bbt::framework
