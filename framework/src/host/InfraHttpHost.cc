#include <bbt/framework/internal/InfraHttpHost.hpp>

#include <boost/scope/scope_exit.hpp>

#include <utility>

namespace bbt::framework {

InfraHttpHost::InfraHttpHost(bbt::infra::NetworkLimits limits, bbt::infra::ListenAddress listen)
    : m_limits(std::move(limits)), m_listen(std::move(listen)), m_state(std::make_shared<HandlerState>())
{
}

void InfraHttpHost::InstallHandler(bbt::infra::HttpHandler handler) {
    m_state->http = std::move(handler);
}

void InfraHttpHost::InstallRpcHandler(bbt::infra::RpcHandler handler)
{
    m_state->rpc = std::move(handler);
}

result<void> InfraHttpHost::Create() {
    auto rt = bbt::infra::NetworkRuntime::Create(m_limits);
    if (!rt)
        return result<void>::err(std::move(rt.error()));
    // m_runtime 经 m_mtx 发布：只读口从控制线程并发读，不能与
    // Create/Close 的写并发。
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_runtime = std::move(rt.value());
    }
    return result<void>::ok();
}

result<bbt::infra::HttpResponse> InfraHttpHost::_InvokeHttp(const std::shared_ptr<HandlerState>& st,
                                                            bbt::infra::IncomingCallContext ctx,
                                                            bbt::infra::HttpRequest request)
{
    // 「门检查 + 计数」同一临界区：Close 置关前后进入者二选一，不半进入。
    {
        std::lock_guard<std::mutex> lk(st->mtx);
        if (!st->open)
            return result<bbt::infra::HttpResponse>::err(
                MakeError(ErrorCode::RuntimeUnavailable, "inbound handler: host already closed"));
        ++st->inflight;
    }
    // 减计数由作用域守卫承担（构造不持锁，无锁环）：正常返回与异常栈展开
    // 都走同一条「锁内 --inflight + notify_all」。若像以前那样把减一写在
    // st->http(...) 之后，handler（或其间任何分配/业务）抛出的异常会越过它，
    // inflight 永久 +1，HostLifecycle::_WaitHandlersDone 耗尽预算后以
    // Deadline::max() 无界续等，宿主保活不释放（fail-closed，非 UAF）。
    auto inflight_guard = boost::scope::make_scope_exit([st]() noexcept {
        {
            std::lock_guard<std::mutex> lk(st->mtx);
            --st->inflight;
        }
        // 归零与减一同一锁序，通知不丢给已进入等待的 WaitHandlersDone。
        st->cv.notify_all();
    });
    return st->http(std::move(ctx), std::move(request));
}

result<bbt::infra::RpcEnvelope> InfraHttpHost::_InvokeRpc(const std::shared_ptr<HandlerState>& st,
                                                          bbt::infra::IncomingCallContext ctx,
                                                          bbt::infra::RpcEnvelope request)
{
    {
        std::lock_guard<std::mutex> lk(st->mtx);
        if (!st->open)
            return result<bbt::infra::RpcEnvelope>::err(
                MakeError(ErrorCode::RuntimeUnavailable, "inbound rpc handler: host already closed"));
        ++st->inflight;
    }
    // 同 _InvokeHttp：异常栈展开也必须减在途计数（见上）。
    auto inflight_guard = boost::scope::make_scope_exit([st]() noexcept {
        {
            std::lock_guard<std::mutex> lk(st->mtx);
            --st->inflight;
        }
        st->cv.notify_all();
    });
    return st->rpc(std::move(ctx), std::move(request));
}

result<void> InfraHttpHost::Start() {
    std::shared_ptr<bbt::infra::NetworkRuntime> rt;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        rt = m_runtime;
    }
    if (!rt)
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "InfraHttpHost::Start: runtime not created"));
    if (auto r = rt->Start(); !r)
        return r;

    // 闭包按 shared_ptr 捕获 handler 状态，绝不捕获宿主 this——见文件头寿命边界。
    const auto state = m_state;
    if (state->rpc) {
        auto srv = rt->ListenRpc(m_listen, [state](bbt::infra::IncomingCallContext ctx, bbt::infra::RpcEnvelope req) {
            return _InvokeRpc(state, std::move(ctx), std::move(req));
        });
        if (!srv)
            return result<void>::err(std::move(srv.error()));
        // m_rpc_server 与 m_bound 在同一临界区发布：bound_endpoint()/
        // bound_address() 从其他线程轮询时就绪判据是原子的——读不到
        // 「server 已置位但 bound 仍是默认值」的撕裂状态。
        std::lock_guard<std::mutex> lk(m_mtx);
        m_rpc_server = std::move(srv.value());
        m_bound = m_rpc_server->LocalAddress();
    } else if (state->http) {
        auto srv = rt->ListenHttp(m_listen, [state](bbt::infra::IncomingCallContext ctx, bbt::infra::HttpRequest req) {
            return _InvokeHttp(state, std::move(ctx), std::move(req));
        });
        if (!srv)
            return result<void>::err(std::move(srv.error()));
        std::lock_guard<std::mutex> lk(m_mtx);
        m_server = std::move(srv.value());
        m_bound  = m_server->LocalAddress();
    }
    return result<void>::ok();
}

void InfraHttpHost::StopAccepting() noexcept
{
    // 拒绝门先在 m_state->mtx 内置关，且先于 infra StopAccepting 生效：这样
    // 「infra 在 StopAccepting 之前已接纳、但协程尚未进入 _Invoke*」的请求，其
    // 一旦进入必在门口被拒（RuntimeUnavailable），不再触达业务；WaitHandlersDone
    // 只需等待「已过门」者。顺序不可颠倒——先闭门再停 infra 接纳，否则闭门前的
    // 一瞬仍有请求可过门（C1 残余窗口）。
    {
        std::lock_guard<std::mutex> lk(m_state->mtx);
        m_state->open = false;
    }
    std::shared_ptr<bbt::infra::RpcServer> rsrv;
    std::shared_ptr<bbt::infra::HttpServer> srv;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        rsrv = m_rpc_server;
        srv = m_server;
    }
    if (rsrv)
        rsrv->StopAccepting();
    else if (srv)
        srv->StopAccepting();
}

result<void> InfraHttpHost::WaitHandlersDone(
    bbt::coroutine::Deadline deadline) {
    // 记账与 handler 同在 m_state：宿主析构不影响在途协程访问计数。
    const auto state = m_state;
    std::unique_lock<std::mutex> lk(state->mtx);
    // 谓词等待 + 到期判定：_Invoke* 每次减一都在同一锁序下 notify_all，
    // 无遗漏唤醒；deadline 到点仍有在途 handler → TimedOut。
    if (state->cv.wait_until(lk, deadline, [&state] { return state->inflight == 0; }))
        return result<void>::ok();
    return result<void>::err(MakeError(ErrorCode::TimedOut,
        "inbound handlers still running at deadline"));
}

void InfraHttpHost::Close() noexcept {
    // 拒绝门先置关：Close 之后新进入的 handler 被拒，不再向下调用业务。
    // （不追溯已在途者——其排空由 WaitHandlersDone 与 CoApp 生命周期负责。）
    {
        std::lock_guard<std::mutex> lk(m_state->mtx);
        m_state->open = false;
    }
    std::shared_ptr<bbt::infra::NetworkRuntime> rt;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        rt = m_runtime;
    }
    // 同步收口：NetworkRuntime::Close 逐个收口子对象（含 HttpServer/RpcServer
    // 底层 HttpServer）、transport 与 io 域，返回即物理资源已释放；不重开、
    // 无协程等待。
    if (rt)
        rt->Close();
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_server.reset();
        m_rpc_server.reset();
        m_runtime.reset();
    }
}

std::optional<bbt::infra::ListenAddress> InfraHttpHost::bound_address() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    if (!m_server && !m_rpc_server)
        return std::nullopt;
    return m_bound;
}

std::string InfraHttpHost::bound_endpoint() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    if (!m_server && !m_rpc_server)
        return {};
    return m_bound.host + ":" + std::to_string(m_bound.port);
}

std::shared_ptr<bbt::infra::HttpServer> InfraHttpHost::http_server() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_server;
}

std::shared_ptr<bbt::infra::RpcServer> InfraHttpHost::rpc_server() const
{
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_rpc_server;
}

std::shared_ptr<bbt::infra::NetworkRuntime>
InfraHttpHost::network_runtime() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_runtime;
}

} // namespace bbt::framework
