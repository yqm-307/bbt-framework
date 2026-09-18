#include <bbt/framework/internal/InfraHttpHost.hpp>

#include <chrono>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>

namespace bbt::framework {

namespace {

// 控制线程等待的上限粒度：谓词之外的活性复查（外部 cancel、Scheduler
// 意外停止）按此间隔重估，不是时序凑数。
constexpr std::chrono::milliseconds kRecheck{50};

} // namespace

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
    // m_runtime 经 m_mtx 发布：RequestClose/WaitClosed 允许从控制线程
    // 并发读，不能与 Create/ReleaseClosed 的写并发。
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
    bbt::coroutine::Deadline deadline,
    bbt::coroutine::CancellationToken cancel) {
    std::unique_lock<std::mutex> lk(m_mtx);
    for (;;) {
        if (m_inflight == 0)
            return result<void>::ok();
        if (cancel.IsCancellationRequested())
            return result<void>::err(MakeError(ErrorCode::Cancelled,
                "WaitHandlersDone cancelled"));
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline)
            return result<void>::err(MakeError(ErrorCode::TimedOut,
                "inbound handlers still running at deadline"));
        const auto remain =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - now);
        // 谓词之外的活性复查点：token 没有通知通道，按有界间隔重估。
        m_cv.wait_for(lk, remain < kRecheck ? remain : kRecheck);
    }
}

void InfraHttpHost::RequestClose() noexcept {
    std::shared_ptr<bbt::infra::NetworkRuntime> rt;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        rt = m_runtime;
    }
    if (rt)
        rt->RequestClose();
}

bbt::infra::CloseStatus InfraHttpHost::WaitClosed(
    bbt::coroutine::Deadline deadline,
    bbt::coroutine::CancellationToken cancel) {
    std::shared_ptr<bbt::infra::NetworkRuntime> rt;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        rt = m_runtime;
    }
    if (!rt || rt->IsClosed())
        return bbt::infra::CloseStatus::Closed;

    // 桥接：ICoCloseable::WaitClosed 只在协程内合法，INetworkHost 语义
    // 是控制线程等待。注册一次性协程执行真实 WaitClosed（deadline/cancel
    // 原样传递，不放宽），控制线程经条件变量收结果。
    struct Slot {
        std::mutex                                   m;
        std::condition_variable                      cv;
        std::optional<bbt::infra::CloseStatus>       st;
    };
    auto slot = std::make_shared<Slot>();
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [rt, deadline, cancel, slot]() mutable {
            auto st = rt->WaitClosed(deadline, std::move(cancel));
            {
                std::lock_guard<std::mutex> lk(slot->m);
                slot->st = st;
            }
            slot->cv.notify_all();
        },
        succ);
    if (!succ)
        return bbt::infra::CloseStatus::RuntimeUnavailable;

    std::unique_lock<std::mutex> lk(slot->m);
    while (!slot->st.has_value()) {
        // 协程在 Scheduler Stop 排空时可能被整队丢弃而永不运行；
        // 以活性复查退出，不把控制线程永久挂住。
        if (!g_scheduler->IsRunning())
            return bbt::infra::CloseStatus::RuntimeUnavailable;
        slot->cv.wait_for(lk, kRecheck);
    }
    return *slot->st;
}

void InfraHttpHost::ReleaseClosed() noexcept {
    std::lock_guard<std::mutex> lk(m_mtx);
    m_server.reset();
    m_runtime.reset();
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
