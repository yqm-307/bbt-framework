#include <bbt/framework/internal/InfraHttpHost.hpp>

#include <utility>

namespace bbt::framework {

InfraHttpHost::InfraHttpHost(bbt::infra::NetworkLimits limits,
                             bbt::infra::ListenAddress listen)
    : m_limits(std::move(limits)), m_listen(std::move(listen)) {}

void InfraHttpHost::InstallHandler(bbt::infra::HttpHandler handler) {
    m_handler = std::move(handler);
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
    if (m_handler) {
        auto srv = rt->ListenHttp(
            m_listen,
            [this](bbt::infra::IncomingCallContext ctx,
                   bbt::infra::HttpRequest req) {
                return _Handle(std::move(ctx), std::move(req));
            });
        if (!srv)
            return result<void>::err(std::move(srv.error()));
        // m_server 与 m_bound 在同一临界区发布：bound_endpoint()/
        // bound_address() 从其他线程轮询时就绪判据是原子的——读不到
        // 「server 已置位但 bound 仍是默认值」的撕裂状态。
        std::lock_guard<std::mutex> lk(m_mtx);
        m_server = std::move(srv.value());
        m_bound  = m_server->LocalAddress();
    }
    return result<void>::ok();
}

result<bbt::infra::HttpResponse> InfraHttpHost::_Handle(
    bbt::infra::IncomingCallContext ctx,
    bbt::infra::HttpRequest request) {
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        ++m_inflight;
    }
    auto r = m_handler(std::move(ctx), std::move(request));
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        --m_inflight;
    }
    // 归零与减一同一锁序，通知不丢给已进入等待的 WaitHandlersDone。
    m_cv.notify_all();
    return r;
}

void InfraHttpHost::StopAccepting() noexcept {
    std::shared_ptr<bbt::infra::HttpServer> srv;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        srv = m_server;
    }
    if (srv)
        srv->StopAccepting();
}

result<void> InfraHttpHost::WaitHandlersDone(
    bbt::coroutine::Deadline deadline) {
    std::unique_lock<std::mutex> lk(m_mtx);
    // 谓词等待 + 到期判定：_Handle 每次减一都在同一锁序下 notify_all，
    // 无遗漏唤醒；deadline 到点仍有在途 handler → TimedOut。
    if (m_cv.wait_until(lk, deadline, [this] { return m_inflight == 0; }))
        return result<void>::ok();
    return result<void>::err(MakeError(ErrorCode::TimedOut,
        "inbound handlers still running at deadline"));
}

void InfraHttpHost::Close() noexcept {
    std::shared_ptr<bbt::infra::NetworkRuntime> rt;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        rt = m_runtime;
    }
    // 同步收口：NetworkRuntime::Close 逐个收口子对象（含 HttpServer）、
    // transport 与 io 域，返回即物理资源已释放；不重开、无协程等待。
    if (rt)
        rt->Close();
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_server.reset();
        m_runtime.reset();
    }
}

std::optional<bbt::infra::ListenAddress> InfraHttpHost::bound_address() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    if (!m_server)
        return std::nullopt;
    return m_bound;
}

std::string InfraHttpHost::bound_endpoint() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    if (!m_server)
        return {};
    return m_bound.host + ":" + std::to_string(m_bound.port);
}

std::shared_ptr<bbt::infra::HttpServer> InfraHttpHost::http_server() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_server;
}

std::shared_ptr<bbt::infra::NetworkRuntime>
InfraHttpHost::network_runtime() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_runtime;
}

} // namespace bbt::framework
