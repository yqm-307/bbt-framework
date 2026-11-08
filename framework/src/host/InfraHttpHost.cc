#include <bbt/framework/internal/InfraHttpHost.hpp>

#include <algorithm>
#include <chrono>
#include <exception>
#include <utility>
#include <vector>
#include <boost/scope/scope_exit.hpp>
#include <bbt/coroutine/coroutine.hpp>
#include <bbt/framework/internal/ErrorDomainRule.hpp>
#include <bbt/infra/rpc/Options.hpp>

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

InfraHttpHost::InfraHttpHost(bbt::infra::NetworkLimits limits, bbt::infra::ListenAddress listen,
                             std::string local_identity)
    : m_limits(std::move(limits)), m_listen(std::move(listen)), m_local_identity(std::move(local_identity)), m_state(std::make_shared<HandlerState>()) {
    m_state->max_inflight = m_limits.max_inflight;
}
void InfraHttpHost::InstallHandler(bbt::infra::http::ServerHandler handler) {
    m_state->http = std::move(handler);
}
void InfraHttpHost::InstallRpcHandler(RpcDispatch handler) {
    m_state->rpc = std::move(handler);
}

result<void> InfraHttpHost::Create() {
    namespace rpc = bbt::infra::rpc;
    if (auto valid = bbt::infra::ValidateNetworkLimits(m_limits); !valid)
        return valid;
    // 一个端口只能安装一种接纳协议；HTTP 能力不承载 RPC 信封。
    if (m_state->http && m_state->rpc)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "host: HTTP and RPC cannot share a listener"));
    auto engine = net::Engine::Create(std::shared_ptr<net::RawHandler>{});
    rpc::ClientOptions rpc_options;
    rpc_options.SetLocalIdentity(m_local_identity).SetTrustedNetworkPlaintext(true).SetMaxSessions(std::min(m_limits.max_connections, rpc::kMaxMaxSessions)).SetMaxFrameBytes(m_limits.max_body_bytes);
    auto rpc_client = rpc::CoRpcClient::Create(engine, rpc_options);
    if (!rpc_client)
        return result<void>::err(std::move(rpc_client.error()));
    bbt::infra::http::ClientOptions client_options;
    client_options.limits.max_connections = std::min(m_limits.max_connections, m_limits.max_inflight);
    client_options.limits.max_keepalive_connections = std::min<std::size_t>(
        client_options.limits.max_keepalive_connections, client_options.limits.max_connections);
    client_options.limits.max_queue = 0;
    client_options.max_body_bytes = m_limits.max_body_bytes;
    client_options.max_request_body_bytes = m_limits.max_body_bytes;
    client_options.max_header_bytes = m_limits.max_header_bytes;
    client_options.timeout = bbt::infra::http::Timeout::Uniform(
        bbt::infra::http::PhaseTimeout::Milliseconds(m_limits.incoming_timeout));
    auto client = bbt::infra::http::Client::Create(engine, client_options);
    if (!client)
        return result<void>::err(std::move(client.error()));
    std::shared_ptr<bbt::infra::http::Server> http_server;
    std::shared_ptr<rpc::CoRpcServer> rpc_server;
    if (m_state->http) {
        bbt::infra::http::ServerOptions options;
        options.limits = m_limits;
        options.max_target_bytes = std::min(options.max_target_bytes, m_limits.max_header_bytes);
        auto server = bbt::infra::http::Server::Create(engine, [state = m_state](bbt::infra::IncomingCallContext ctx, bbt::infra::http::IncomingRequest req) { return _InvokeHttp(state, std::move(ctx), std::move(req)); }, options);
        if (!server)
            return result<void>::err(std::move(server.error()));
        http_server = std::move(server).value();
    } else if (m_state->rpc) {
        std::error_code ec;
        auto address = net::Address::Ip(m_listen.host, m_listen.port, ec);
        if (ec)
            return result<void>::err(EngineError(ec, "invalid listen address"));
        rpc::ServerOptions options;
        options.SetListenAddress(address).SetMaxFrameBytes(m_limits.max_body_bytes).SetMaxConnections(m_limits.max_connections).SetMaxInflight(m_limits.max_inflight).SetMaxHandlerBytes(m_limits.max_body_bytes).SetIncomingTimeout(m_limits.incoming_timeout);
        auto server = rpc::CoRpcServer::Create(engine, rpc_client.value(), [state = m_state](rpc::AdmissionItem&& item) { return _AdmitRpc(state, std::move(item)); }, options);
        if (!server)
            return result<void>::err(std::move(server.error()));
        rpc_server = std::move(server).value();
    }
    std::lock_guard<std::mutex> lock(m_mtx);
    m_engine = std::move(engine);
    m_client = std::move(client).value();
    m_rpc_client = std::move(rpc_client).value();
    m_server = std::move(http_server);
    m_rpc_server = std::move(rpc_server);
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
result<void> InfraHttpHost::_AdmitRpc(const std::shared_ptr<HandlerState>& state,
                                      bbt::infra::rpc::AdmissionItem&& item) {
    // 同步回调只转移拥有权，不运行业务，也不等待回复写终态。
    std::lock_guard<std::mutex> lock(state->mtx);
    if (!state->open)
        return result<void>::err(state->close_reason.value_or(
            MakeError(ErrorCode::Closed, "inbound rpc: host sealed")));
    if (state->inflight >= state->max_inflight)
        return result<void>::err(MakeError(ErrorCode::Overloaded, "inbound rpc: queue full"));
    state->queue.push_back(std::move(item));
    bool registered = false;
    g_scheduler->RegistCoroutineTask(256 * 1024, [state] { _RunRpc(state); }, registered);
    if (!registered) {
        // 回滚转移；infra 仍负责拒绝终态和 lease，不留下孤儿项。
        item = std::move(state->queue.back());
        state->queue.pop_back();
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
                                           "inbound rpc: scheduler unavailable"));
    }
    ++state->inflight;
    return result<void>::ok();
}

void InfraHttpHost::_RunRpc(const std::shared_ptr<HandlerState>& state) {
    namespace rpc = bbt::infra::rpc;
    // 取队列与同步派发共用门，避免多 worker 把同一有序流的后项先入邮箱。
    // on_dispatched 在任何业务等待之前解锁，不持系统锁跨协程挂起。
    std::unique_lock<std::mutex> dispatch_lock(state->dispatch_mtx);
    const auto dispatched = [&] {
        if (dispatch_lock.owns_lock())
            dispatch_lock.unlock();
    };
    rpc::AdmissionItem item;
    std::optional<Error> closed;
    {
        std::lock_guard<std::mutex> lock(state->mtx);
        item = std::move(state->queue.front());
        state->queue.pop_front();
        closed = state->close_reason;
    }
    const bool call = item.request.Reply() == rpc::ReplyMode::Required;
    // 业务和回复实际返回后才减计数；Close 不提前宣布排空。
    auto done = boost::scope::make_scope_exit([state, lease = item.lease, call] {
        if (call && lease)
            (void)lease->Release();
        {
            std::lock_guard<std::mutex> lock(state->mtx);
            --state->inflight;
        }
        state->cv.notify_all();
    });
    auto reply = result<rpc::Response>::err(MakeError(ErrorCode::InternalError,
                                                      "inbound rpc: handler failed"));
    try {
        if (closed)
            reply = result<rpc::Response>::err(*closed);
        else if (item.request.Context().deadline <= std::chrono::steady_clock::now())
            reply = result<rpc::Response>::err(MakeError(ErrorCode::TimedOut,
                                                         "inbound rpc: expired while queued"));
        else
            reply = state->rpc(item.request, dispatched);
    } catch (const std::exception& e) {
        reply = result<rpc::Response>::err(MakeError(ErrorCode::InternalError, e.what()));
    } catch (...) {
        reply = result<rpc::Response>::err(MakeError(ErrorCode::InternalError,
                                                     "inbound rpc: non-standard handler exception"));
    }
    dispatched();
    // SendAccepted/Send lease 属 infra；业务完成不是第二次接纳终态。
    if (!call || !item.reply)
        return;
    if (!reply) {
        auto error = std::move(reply.error());
        if (auto valid = ValidateErrorAtBoundary(error); !valid)
            error = std::move(valid.error());
        auto written = bbt_co_await item.reply->Fail(std::move(error));
        (void)written; // 断链/超时已由 infra 记录；无论写结果如何均须归还 lease。
    } else {
        auto written = bbt_co_await item.reply->Complete(std::move(reply).value());
        (void)written;
    }
}

result<void> InfraHttpHost::Start() {
    if (!m_created)
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable, "network host not created"));
    bbt::infra::net::EngineConfig config;
    config.runtime_progress = bbt::infra::net::RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    std::error_code ec;
    auto address = bbt::infra::net::Address::Ip(m_listen.host, m_listen.port, ec);
    if (ec)
        return result<void>::err(EngineError(ec, "invalid listen address"));
    if (m_server || m_rpc_server)
        config.listen_addresses = {address};
    if (auto started = RunEngine(m_engine, config, m_engine_thread, m_engine_result); !started)
        return started;
    std::vector<bbt::infra::net::Address> addresses;
    if (auto error = m_engine->LocalAddresses(addresses); error || ((m_server || m_rpc_server) && addresses.empty()))
        return result<void>::err(EngineError(error, "LocalAddresses failed"));
    if (m_server) {
        if (auto r = m_server->Start(); !r)
            return r;
    }
    if (m_rpc_server) {
        if (auto r = m_rpc_server->Start(); !r)
            return r;
    }
    std::lock_guard<std::mutex> lock(m_mtx);
    if (!addresses.empty()) {
        m_bound = {m_listen.host, addresses.front().Port()};
        m_bound_ready = true;
    }
    return result<void>::ok();
}

void InfraHttpHost::StopAccepting() noexcept {
    {
        std::lock_guard<std::mutex> lock(m_state->mtx);
        m_state->open = false;
    }
    // RPC 会话层的新契约没有独立的 StopAccepting 阶段：业务门关闭后
    // 新入站由 _AdmitRpc 同步拒绝；会话/监听封口只在 WaitHandlersDone 的
    // Shutdown 或 Close 发生（提前封 Engine 接纳会让会话提前退役，迟到
    // 请求拿不到业务门的 Closed 终态）。HTTP 栈仍是旧生命周期，保留其
    // 停止接纳步骤。
    if (auto server = http_server())
        server->StopAccepting();
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
        // 排空已证实：同一把业务门内拒绝后续接纳，避免「检查归零 → 封口」
        // 之间又进入一个 handler。
        m_state->open = false;
    }
    // 排空被证实：HTTP 仍按旧契约做拒新 + 已授权 reply 写终态 + 物理收口；
    // Close 打断时 Shutdown 直接返回其关闭原因，后续物理收口不再进行。
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
    // RPC 新契约只有统一 Close/Shutdown 入口；本入口在 native 控制线程，
    // 不在协程域。业务 handler 与 reply 写终态已由 inflight 归零证明，
    // 剩余只是会话/连接责任，用同步 Shutdown 封口；物理回收由 Close 的
    // Engine 收口证明。
    if (auto server = rpc_server()) {
        server->Shutdown();
    }
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
        // RPC 新契约的同步强制关闭：任意域、幂等、不等 handler。业务完成后
        // 的物理回收由下面的 Engine 收口统一证明。
        if (auto server = rpc_server()) {
            server->Shutdown(reason);
        }
        if (auto client = http_client()) { client->Close(reason); }
        if (auto client = rpc_client()) {
            client->Shutdown(reason);
        }
        // 无条件 join：旧路径失败即 return 会保留 joinable 线程，析构时
        // std::terminate。join 只在驱动线程可即时退出时完成；仍被业务占用
        // 时把 join 留给析构（见 _JoinEngine / ~InfraHttpHost）。
        const bool inbound_stopped = _JoinEngine(m_engine, m_engine_thread, m_engine_result);
        std::lock_guard<std::mutex> lock(m_mtx);
        // 已 Close 的 server/client owner 与驱动线程寿命解耦，可立即释放。
        m_server.reset();
        m_rpc_server.reset();
        m_rpc_client.reset();
        m_client.reset();
        if (inbound_stopped) { m_engine.reset(); }
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
    return m_engine;
}
std::shared_ptr<bbt::infra::http::Client> InfraHttpHost::http_client() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_client;
}
std::optional<bbt::infra::Error> InfraHttpHost::close_error() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_close_error;
}
std::shared_ptr<bbt::infra::rpc::CoRpcClient> InfraHttpHost::rpc_client() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_rpc_client;
}
std::shared_ptr<bbt::infra::rpc::CoRpcServer> InfraHttpHost::rpc_server() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_rpc_server;
}
} // namespace bbt::framework
