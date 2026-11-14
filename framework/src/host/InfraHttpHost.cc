#include <bbt/framework/internal/InfraHttpHost.hpp>

#include <algorithm>
#include <chrono>
#include <exception>
#include <utility>
#include <vector>

#include <boost/scope/scope_exit.hpp>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/framework/internal/CloseOptionsCheck.hpp>
#include <bbt/framework/internal/ErrorDomainRule.hpp>
#include <bbt/infra/rpc/Options.hpp>

namespace bbt::framework {
namespace {
namespace net = bbt::infra::net;
namespace co = bbt::coroutine;

constexpr std::chrono::milliseconds kWaitSlice{50};

bbt::infra::Error EngineError(std::error_code ec, const char* what) {
    return MakeError(ErrorCode::RuntimeUnavailable,
                     std::string(what) + ": " +
                         (ec ? ec.message() : "invalid engine phase"));
}

// 下一片等待的绝对时间点：min(deadline, now+50ms)；无期限时固定 50ms。
co::Deadline NextSlice(co::Deadline deadline) noexcept {
    const auto now = std::chrono::steady_clock::now();
    if (deadline == co::Deadline::max())
        return now + kWaitSlice;
    return std::min(deadline, now + kWaitSlice);
}

// 单次 Co 分片等待；AlreadyWaiting 让出 5ms 后按未命中处理（由调用方复查谓词）。
result<bool> WaitSlice(const std::shared_ptr<co::sync::CoWaiter>& signal,
                       co::Deadline until) {
    using WS = co::WaitStatus;
    if (!signal)
        return result<bool>::err(MakeError(ErrorCode::RuntimeUnavailable,
                                           "host: drain wait signal missing"));
    co::WaitOptions options;
    options.deadline = until;
    switch (signal->Wait(options)) {
    case WS::Completed:
        return result<bool>::ok(true);
    case WS::TimedOut:
        return result<bool>::ok(false);
    case WS::AlreadyWaiting:
        bbtco_sleep(5);
        return result<bool>::ok(false);
    case WS::Cancelled:
        return result<bool>::err(MakeError(ErrorCode::Cancelled,
                                           "host: drain wait cancelled"));
    case WS::InvalidContext:
        return result<bool>::err(MakeError(ErrorCode::InvalidContext,
                                           "host: drain wait requires coroutine context"));
    case WS::RuntimeUnavailable:
        return result<bool>::err(MakeError(ErrorCode::RuntimeUnavailable,
                                           "host: drain wait unavailable"));
    }
    return result<bool>::err(MakeError(ErrorCode::RuntimeUnavailable,
                                       "host: drain wait unavailable"));
}

} // namespace

InfraHttpHost::InfraHttpHost(bbt::infra::NetworkLimits limits,
                             bbt::infra::ListenAddress listen,
                             std::string local_identity)
    : m_limits(std::move(limits)),
      m_listen(std::move(listen)),
      m_local_identity(std::move(local_identity)),
      m_state(std::make_shared<HandlerState>()) {
    m_state->max_inflight = m_limits.max_inflight;
}

InfraHttpHost::InfraHttpHost(
    std::shared_ptr<bbt::infra::net::Engine> engine,
    bbt::infra::NetworkLimits limits, bbt::infra::ListenAddress listen,
    std::string local_identity)
    : m_limits(std::move(limits)),
      m_listen(std::move(listen)),
      m_local_identity(std::move(local_identity)),
      m_state(std::make_shared<HandlerState>()),
      m_engine(std::move(engine)),
      m_engine_borrowed(true) {
    m_state->max_inflight = m_limits.max_inflight;
}

void InfraHttpHost::InstallHandler(bbt::infra::http::ServerHandler handler) {
    m_state->http = std::move(handler);
}

void InfraHttpHost::InstallRpcHandler(RpcDispatch handler) {
    m_state->rpc = std::move(handler);
}

bool InfraHttpHost::http_handler_installed() const {
    std::lock_guard<std::mutex> lock(m_state->mtx);
    return static_cast<bool>(m_state->http);
}

bool InfraHttpHost::rpc_handler_installed() const {
    std::lock_guard<std::mutex> lock(m_state->mtx);
    return static_cast<bool>(m_state->rpc);
}

result<void> InfraHttpHost::Create() {
    namespace rpc = bbt::infra::rpc;
    if (auto valid = bbt::infra::ValidateNetworkLimits(m_limits); !valid)
        return valid;
    // 一个端口只能安装一种接纳协议；HTTP 能力不承载 RPC 信封。
    if (m_state->http && m_state->rpc)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "host: HTTP and RPC cannot share a listener"));
    // Engine 在 Idle 阶段装配：不 Run、不建线程；Run 由应用入口 host hook
    // 在主线程执行一次，本宿主只在 Start 中观察 Ready。借用形态直接使用应用根
    // 的共享实例（必须已存在），不自建第二个 Engine。
    std::shared_ptr<net::Engine> engine = m_engine;
    if (!engine) {
        if (m_engine_borrowed)
            return result<void>::err(MakeError(
                ErrorCode::RuntimeUnavailable,
                "host: borrowed shared engine is missing"));
        engine = net::Engine::Create(std::shared_ptr<net::RawHandler>{});
    }
    rpc::ClientOptions rpc_options;
    rpc_options.SetLocalIdentity(m_local_identity)
        .SetTrustedNetworkPlaintext(true)
        .SetMaxSessions(std::min(m_limits.max_connections, rpc::kMaxMaxSessions))
        .SetMaxFrameBytes(m_limits.max_body_bytes);
    auto rpc_client = rpc::CoRpcClient::Create(engine, rpc_options);
    if (!rpc_client)
        return result<void>::err(std::move(rpc_client.error()));
    bbt::infra::http::ClientOptions client_options;
    client_options.limits.max_connections =
        std::min(m_limits.max_connections, m_limits.max_inflight);
    client_options.limits.max_keepalive_connections = std::min<std::size_t>(
        client_options.limits.max_keepalive_connections,
        client_options.limits.max_connections);
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
        options.max_target_bytes =
            std::min(options.max_target_bytes, m_limits.max_header_bytes);
        auto server = bbt::infra::http::Server::Create(
            engine,
            [state = m_state](bbt::infra::IncomingCallContext ctx,
                              bbt::infra::http::IncomingRequest req) {
                return _InvokeHttp(state, std::move(ctx), std::move(req));
            },
            options);
        if (!server)
            return result<void>::err(std::move(server.error()));
        http_server = std::move(server).value();
    } else if (m_state->rpc) {
        std::error_code ec;
        auto address = net::Address::Ip(m_listen.host, m_listen.port, ec);
        if (ec)
            return result<void>::err(EngineError(ec, "invalid listen address"));
        rpc::ServerOptions options;
        options.SetListenAddress(address)
            .SetMaxFrameBytes(m_limits.max_body_bytes)
            .SetMaxConnections(m_limits.max_connections)
            .SetMaxInflight(m_limits.max_inflight)
            .SetMaxHandlerBytes(m_limits.max_body_bytes)
            .SetIncomingTimeout(m_limits.incoming_timeout);
        auto server = rpc::CoRpcServer::Create(
            engine, rpc_client.value(),
            [state = m_state](rpc::AdmissionItem&& item) {
                return _AdmitRpc(state, std::move(item));
            },
            options);
        if (!server)
            return result<void>::err(std::move(server.error()));
        rpc_server = std::move(server).value();
    }
    // Run 的输入由持有 Engine 的入口 host hook 消费；本宿主只装配并保存。
    bbt::infra::net::EngineConfig engine_config;
    engine_config.runtime_progress = net::RuntimeProgress::Independent;
    engine_config.options.num_event_loop = 1;
    std::error_code listen_ec;
    auto listen_address =
        net::Address::Ip(m_listen.host, m_listen.port, listen_ec);
    if (listen_ec)
        return result<void>::err(EngineError(listen_ec, "invalid listen address"));
    if (http_server || rpc_server)
        engine_config.listen_addresses = {listen_address};
    std::lock_guard<std::mutex> lock(m_mtx);
    m_engine = std::move(engine);
    m_client = std::move(client).value();
    m_rpc_client = std::move(rpc_client).value();
    m_server = std::move(http_server);
    m_rpc_server = std::move(rpc_server);
    m_engine_config = std::move(engine_config);
    m_created = true;
    return result<void>::ok();
}

result<void> InfraHttpHost::_CheckCoContext(const char* entry) const {
    if (!co::IsInCoroutineContext())
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
                                           std::string("host: ") + entry +
                                               " requires a managed coroutine context"));
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
    // 减一由构造于 ++ 之后的 noexcept 作用域守卫承担：正常返回与异常栈展开
    // 都走同一条「锁内 --inflight + 锁外唤醒」。
    auto guard = boost::scope::make_scope_exit([state] {
        {
            std::lock_guard<std::mutex> lock(state->mtx);
            --state->inflight;
        }
        _NotifyDrained(state);
    });
    return state->http(std::move(ctx), std::move(request));
}

result<void> InfraHttpHost::_AdmitRpc(const std::shared_ptr<HandlerState>& state,
                                      bbt::infra::rpc::AdmissionItem&& item) {
    // 同步回调只转移拥有权，不运行业务，也不等待回复写终态。
    // 登记协程栈可能挂起（异步栈分配），因此绝不在 state->mtx 内做登记：
    // 锁内只做接纳校验、分配票据与计数，锁外登记；失败按票据取消回滚轮次。
    auto slot =
        std::make_shared<bbt::infra::rpc::AdmissionItem>(std::move(item));
    std::uint64_t ticket = 0;
    {
        std::lock_guard<std::mutex> lock(state->mtx);
        if (!state->open)
            return result<void>::err(state->close_reason.value_or(
                MakeError(ErrorCode::Closed, "inbound rpc: host sealed")));
        if (state->inflight >= state->max_inflight)
            return result<void>::err(MakeError(ErrorCode::Overloaded,
                                               "inbound rpc: queue full"));
        ticket = state->next_ticket++;
        ++state->inflight;
    }
    bool registered = false;
    try {
        g_scheduler->RegistCoroutineTask(
            256 * 1024,
            [state, ticket, slot] { _RunRpc(state, ticket, slot); },
            registered);
    } catch (...) {
        registered = false;
    }
    if (!registered) {
        // 登记失败：本票据不进入派发；取消并推进轮次（跳过连续取消票据），
        // 不阻塞后续已接纳请求。
        std::shared_ptr<co::sync::CoWaiter> turn;
        {
            std::lock_guard<std::mutex> lock(state->mtx);
            --state->inflight;
            state->cancelled.insert(ticket);
            _AdvanceDispatchLocked(*state, ticket);
            turn = state->turn;
        }
        if (turn)
            turn->Notify();
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
                                           "inbound rpc: scheduler unavailable"));
    }
    return result<void>::ok();
}

void InfraHttpHost::_AdvanceDispatchLocked(HandlerState& state,
                                           std::uint64_t ticket) {
    if (state.dispatch_ticket != ticket)
        return;
    ++state.dispatch_ticket;
    while (state.cancelled.erase(state.dispatch_ticket) != 0)
        ++state.dispatch_ticket;
}

void InfraHttpHost::_RunRpc(
    const std::shared_ptr<HandlerState>& state, std::uint64_t ticket,
    std::shared_ptr<bbt::infra::rpc::AdmissionItem> item) {
    namespace rpc = bbt::infra::rpc;
    // 派发前按票据轮次排队：只有一个 ticket 处于「可同步派发」位置，
    // 由 CoWaiter 分片挂起等待（不持 OS 锁跨挂起）。轮次在 handler 首次
    // 挂起（on_dispatched）或返回后推进，后续请求可并发进入 handler。
    for (;;) {
        std::shared_ptr<co::sync::CoWaiter> turn;
        {
            std::lock_guard<std::mutex> lock(state->mtx);
            if (state->dispatch_ticket == ticket || state->close_reason)
                break;
            if (!state->turn)
                state->turn = co::sync::CoWaiter::Create();
            turn = state->turn;
        }
        if (!turn) {
            // 等待位创建失败：仍按当前轮次尝试一次，避免饿死。
            bbtco_sleep(1);
            continue;
        }
        auto woken = WaitSlice(turn, NextSlice(co::Deadline::max()));
        if (!woken)
            break;
    }

    auto dispatched = [state, ticket] {
        std::shared_ptr<co::sync::CoWaiter> turn;
        {
            std::lock_guard<std::mutex> lock(state->mtx);
            _AdvanceDispatchLocked(*state, ticket);
            turn = state->turn;
        }
        if (turn)
            turn->Notify();
    };

    std::optional<Error> closed;
    {
        std::lock_guard<std::mutex> lock(state->mtx);
        closed = state->close_reason;
    }
    const bool call = item->request.Reply() == rpc::ReplyMode::Required;
    // 业务和回复实际返回后才减计数；Close 不提前宣布排空。
    auto done = boost::scope::make_scope_exit(
        [state, lease = item->lease, call] {
            if (call && lease)
                (void)lease->Release();
            {
                std::lock_guard<std::mutex> lock(state->mtx);
                --state->inflight;
            }
            _NotifyDrained(state);
        });
    auto reply = result<rpc::Response>::err(MakeError(ErrorCode::InternalError,
                                                      "inbound rpc: handler failed"));
    try {
        if (closed)
            reply = result<rpc::Response>::err(*closed);
        else if (item->request.Context().deadline <= std::chrono::steady_clock::now())
            reply = result<rpc::Response>::err(MakeError(ErrorCode::TimedOut,
                                                         "inbound rpc: expired while queued"));
        else
            reply = state->rpc(item->request, dispatched);
    } catch (const std::exception& e) {
        reply = result<rpc::Response>::err(MakeError(ErrorCode::InternalError, e.what()));
    } catch (...) {
        reply = result<rpc::Response>::err(MakeError(ErrorCode::InternalError,
                                                     "inbound rpc: non-standard handler exception"));
    }
    // handler 未调用 on_dispatched 也必须推进轮次。
    dispatched();
    // SendAccepted/Send lease 属 infra；业务完成不是第二次接纳终态。
    if (!call || !item->reply)
        return;
    if (!reply) {
        auto error = std::move(reply.error());
        if (auto valid = ValidateErrorAtBoundary(error); !valid)
            error = std::move(valid.error());
        auto written = bbt_co_await item->reply->Fail(std::move(error));
        (void)written; // 断链/超时已由 infra 记录；无论写结果如何均须归还 lease。
    } else {
        auto written = bbt_co_await item->reply->Complete(std::move(reply).value());
        (void)written;
    }
}

void InfraHttpHost::_NotifyDrained(
    const std::shared_ptr<HandlerState>& state) noexcept {
    std::shared_ptr<co::sync::CoWaiter> signal;
    {
        std::lock_guard<std::mutex> lock(state->mtx);
        signal = state->drained;
    }
    if (signal)
        signal->Notify();
}

bbt_co_async(result<void>) InfraHttpHost::Start(
    const bbt::coroutine::WaitOptions& wait) {
    if (auto r = _CheckCoContext("Start"); !r)
        return r;
    std::shared_ptr<bbt::infra::http::Server> server;
    std::shared_ptr<bbt::infra::rpc::CoRpcServer> rpc_server;
    std::shared_ptr<net::Engine> engine;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        if (!m_created || !m_engine)
            return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
                                               "network host not created"));
        if (m_engine_started)
            return result<void>::err(MakeError(ErrorCode::Overloaded,
                                               "network host already started"));
        engine = m_engine;
        server = m_server;
        rpc_server = m_rpc_server;
        m_engine_started = true;
    }

    // Engine 的 native 执行由应用入口 host hook 直接调用一次 Run；本宿主只
    // 观察真实 Ready，不创建 runner/线程。Run 未执行或启动失败时，
    // CoWaitRunning 如实返回 TimedOut / 映射后的失败，不伪报 Ready。
    auto ready = engine->CoWaitRunning(wait);
    if (!ready)
        return result<void>::err(std::move(ready.error()));

    std::vector<bbt::infra::net::Address> addresses;
    if (auto error = engine->LocalAddresses(addresses);
        error || ((server || rpc_server) && addresses.empty())) {
        return result<void>::err(EngineError(error, "LocalAddresses failed"));
    }
    if (server) {
        if (auto r = server->Start(); !r)
            return r;
    }
    if (rpc_server) {
        // CoRpcServer::Start 接受受管 Co 域调用：infra 已把可挂起的队列/accept
        // 任务登记移出 server 状态锁，并在注册窗口内按封口态裁决 Start/Close。
        // framework 直接调用、不新开 native 线程，错误如实上抛。
        if (auto r = rpc_server->Start(); !r)
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
    // RPC 会话层没有独立的 StopAccepting 阶段：业务门关闭后新入站由 _AdmitRpc
    // 同步拒绝；会话/监听封口只在 Close 发生（提前封 Engine 接纳会让会话提前
    // 退役，迟到请求拿不到业务门的 Closed 终态）。HTTP 栈保留其停止接纳步骤。
    if (auto server = http_server())
        server->StopAccepting();
}

bbt_co_async(result<void>) InfraHttpHost::WaitHandlersDone(
    const bbt::coroutine::WaitOptions& wait) {
    if (auto r = _CheckCoContext("WaitHandlersDone"); !r)
        return r;
    for (;;) {
        std::shared_ptr<co::sync::CoWaiter> signal;
        {
            std::lock_guard<std::mutex> lock(m_state->mtx);
            // Close 先到：立即把关闭原因交给等待者，不进入排空。
            if (m_state->close_reason)
                return result<void>::err(*m_state->close_reason);
            if (m_state->inflight == 0) {
                // 排空已证实：同一把业务门内拒绝后续接纳，避免「检查归零 →
                // 封口」之间又进入一个 handler。
                m_state->open = false;
                return result<void>::ok();
            }
            if (!m_state->drained)
                m_state->drained = co::sync::CoWaiter::Create();
            signal = m_state->drained;
        }
        auto woken = WaitSlice(signal, NextSlice(wait.deadline));
        if (!woken)
            return result<void>::err(std::move(woken.error()));
        if (wait.deadline != co::Deadline::max() &&
            std::chrono::steady_clock::now() >= wait.deadline) {
            std::lock_guard<std::mutex> lock(m_state->mtx);
            if (m_state->close_reason)
                return result<void>::err(*m_state->close_reason);
            if (m_state->inflight == 0) {
                m_state->open = false;
                return result<void>::ok();
            }
            return result<void>::err(MakeError(ErrorCode::TimedOut,
                                               "inbound handlers still running"));
        }
    }
}

bbt_co_async(result<void>) InfraHttpHost::Close(
    const bbt::infra::CloseOptions& options) {
    if (auto r = _CheckCoContext("Close"); !r)
        return r;
    // 参数校验先于任何副作用：非法组合不污染关闭位/首因。
    if (auto valid = bbt::framework::internal::ValidateCloseValue(options); !valid)
        return result<void>::err(std::move(valid.error()));
    _Seal(options.reason.value_or(MakeError(ErrorCode::Closed,
                                            "framework host: closed by owner")));

    std::shared_ptr<bbt::infra::http::Server> server;
    std::shared_ptr<bbt::infra::rpc::CoRpcServer> rpc_server;
    std::shared_ptr<bbt::infra::http::Client> client;
    std::shared_ptr<bbt::infra::rpc::CoRpcClient> rpc_client;
    std::shared_ptr<net::Engine> engine;
    bool engine_started = false;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        server = m_server;
        rpc_server = m_rpc_server;
        client = m_client;
        rpc_client = m_rpc_client;
        engine = m_engine;
        engine_started = m_engine_started;
    }

    result<void> first_error = result<void>::ok();
    const auto remember = [&first_error](result<void>&& r) {
        if (!r && first_error)
            first_error = result<void>::err(std::move(r.error()));
    };

    StopAccepting();
    // 模块按各自公共 CloseOptions 面封口：HTTP/RPC server 只封口并结算自身
    // 用户可见责任，物理回收由各自的 WaitStopped / Engine 物理凭据观察。
    if (server)
        remember(server->Close(options));
    if (rpc_server)
        remember(bbt_co_await rpc_server->Close(options));
    if (client)
        remember(client->Close(options));
    if (rpc_client)
        remember(bbt_co_await rpc_client->Close(options));
    // Engine 逻辑封口：只对本宿主自建的 Engine 发起。借用形态下 Engine 归应用
    // 根，由入口宿主闸门在主任务结束后 RequestClose；宿主/组件不得关闭它。
    if (engine && engine_started && !m_engine_borrowed) {
        engine->RequestStopAccepting();
        engine->RequestClose();
    }
    return first_error;
}

bbt_co_async(result<void>) InfraHttpHost::WaitPhysicalReleased(
    const bbt::coroutine::WaitOptions& wait) {
    if (auto r = _CheckCoContext("WaitPhysicalReleased"); !r)
        return r;
    std::shared_ptr<net::Engine> engine;
    bool started = false;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        engine = m_engine;
        started = m_engine_started;
    }
    if (!engine || !started) {
        // 未 Start 即关：无 runner/网络代责任，物理释放由「未创建」直接成立。
        std::lock_guard<std::mutex> lock(m_mtx);
        m_server.reset();
        m_rpc_server.reset();
        m_rpc_client.reset();
        m_client.reset();
        m_engine.reset();
        m_bound_ready = false;
        m_bound = {};
        m_created = false;
        return result<void>::ok();
    }
    if (m_engine_borrowed) {
        // 借用形态：Engine 的物理凭据是「入口 host hook 已收口且 Run 返回」，
        // 只在主任务结束后才成立；CoStop 不等待它（否则会与主任务自等待）。
        // 本宿主只释放自己的模块 owner，Engine 的引用仍归应用根。
        std::lock_guard<std::mutex> lock(m_mtx);
        m_server.reset();
        m_rpc_server.reset();
        m_rpc_client.reset();
        m_client.reset();
        m_engine.reset();
        m_bound_ready = false;
        m_bound = {};
        m_created = false;
        m_engine_started = false;
        return result<void>::ok();
    }
    // 自有形态的物理凭据：Run 已返回且 Phase()==Stopped（infra 契约）。本函数
    // 只观察，不 join、不驱动 Engine；50ms 分片让出 worker，超时如实返回。
    for (;;) {
        if (engine->Phase() == net::EnginePhase::Stopped)
            break;
        const auto now = std::chrono::steady_clock::now();
        if (wait.deadline != co::Deadline::max() && now >= wait.deadline)
            return result<void>::err(MakeError(
                ErrorCode::TimedOut,
                "host: engine physical release not observed before deadline"));
        bbtco_sleep(static_cast<int>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                NextSlice(wait.deadline) - now)
                .count()));
    }
    // 物理凭据到手：本对象的模块 owner 与 Engine 引用可以释放。
    std::lock_guard<std::mutex> lock(m_mtx);
    m_server.reset();
    m_rpc_server.reset();
    m_rpc_client.reset();
    m_client.reset();
    m_engine.reset();
    m_bound_ready = false;
    m_bound = {};
    m_created = false;
    m_engine_started = false;
    return result<void>::ok();
}

void InfraHttpHost::_Seal(bbt::infra::Error reason) noexcept {
    try {
        {
            std::lock_guard<std::mutex> lock(m_state->mtx);
            m_state->open = false;
            if (!m_state->close_reason)
                m_state->close_reason = reason;
        }
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (!m_close_reason)
                m_close_reason = reason;
        }
        _NotifyDrained(m_state);
    } catch (...) {
        // noexcept 契约：封口失败不逃逸。
    }
}

InfraHttpHost::~InfraHttpHost() noexcept {
    // 非阻塞：只发布关闭原因、封业务门、对模块与自有 Engine 发非阻塞关闭
    // 请求，并释放本对象的 owner 引用；物理回收由 Run 返回 + Phase Stopped
    // 观察，本析构不 join。
    _Seal(MakeError(ErrorCode::Closed, "framework host: destroyed"));
    std::shared_ptr<bbt::infra::http::Server> server;
    std::shared_ptr<bbt::infra::rpc::CoRpcServer> rpc_server;
    std::shared_ptr<bbt::infra::http::Client> client;
    std::shared_ptr<bbt::infra::rpc::CoRpcClient> rpc_client;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        server = std::move(m_server);
        rpc_server = std::move(m_rpc_server);
        client = std::move(m_client);
        rpc_client = std::move(m_rpc_client);
        m_engine.reset();
        m_created = false;
        m_engine_started = false;
    }
    try {
        if (server)
            server->RequestClose(MakeError(ErrorCode::Closed,
                                           "framework host: destroyed"));
        if (rpc_server)
            rpc_server->RequestClose(MakeError(ErrorCode::Closed,
                                               "framework host: destroyed"));
        if (client)
            client->Close(bbt::infra::CloseOptions::Immediate(
                MakeError(ErrorCode::Closed, "framework host: destroyed")));
        if (rpc_client)
            rpc_client->RequestClose(MakeError(ErrorCode::Closed,
                                               "framework host: destroyed"));
    } catch (...) {
        // 析构不逃逸。
    }
}

std::optional<bbt::infra::Error> InfraHttpHost::CloseReason() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_close_reason;
}

std::optional<bbt::infra::ListenAddress> InfraHttpHost::bound_address() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_bound_ready ? std::optional<bbt::infra::ListenAddress>{m_bound}
                         : std::nullopt;
}

std::string InfraHttpHost::bound_endpoint() const {
    const auto address = bound_address();
    return address ? address->host + ":" + std::to_string(address->port)
                   : std::string{};
}

std::shared_ptr<bbt::infra::http::Server> InfraHttpHost::http_server() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_server;
}

std::shared_ptr<bbt::infra::net::Engine> InfraHttpHost::egress_engine() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_engine;
}

bbt::infra::net::EngineConfig InfraHttpHost::engine_config() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_engine_config;
}

std::shared_ptr<bbt::infra::http::Client> InfraHttpHost::http_client() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_client;
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
