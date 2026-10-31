#include <bbt/framework/internal/InfraHttpHost.hpp>

#include <boost/scope/scope_exit.hpp>

#include <algorithm>
#include <chrono>
#include <future>
#include <utility>
#include <vector>

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
#include <bbt/infra/http/Server.hpp>
#include <bbt/infra/rpc/HttpTransport.hpp>
#endif
#if defined(BBT_FRAMEWORK_HAS_RPC_TCP)
#include <bbt/infra/rpc/TcpTransport.hpp>
#endif

namespace bbt::framework {

namespace {

// Engine::Run 相位等待上限：只用于「Run 线程已拉起但尚未发布 Running」的
// 启动窗口，不是网络/收口预算（后者由 WaitHandlersDone deadline 与
// rpc::Server 自身语义表达）。
constexpr std::chrono::milliseconds kEngineRunWait{5000};

// 用 std::error_code 承载 engine Run/相位等待结果时，转成 framework Error。
bbt::infra::Error EngineError(std::error_code ec, const char* what) {
    if (!ec)
        ec = std::make_error_code(std::errc::io_error);
    return MakeError(ErrorCode::RuntimeUnavailable,
                     std::string(what) + ": " + ec.message());
}

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
// Run 结果的跨线程发布协议：promise/future 是唯一同步口（set_value 与
// wait_for 就绪构成 happens-before），不在线程间传普通 error_code 副本。
// 拉起 Engine::Run 专用 native 线程；Run 返回即发布结果。
std::pair<std::thread, std::future<std::error_code>> StartEngineThread(
    const std::shared_ptr<bbt::infra::net::Engine>& engine,
    bbt::infra::net::EngineConfig config) {
    std::promise<std::error_code> done;
    auto result = done.get_future();
    std::thread th([engine, config = std::move(config),
                    done = std::move(done)]() mutable {
        std::error_code ec;
        try {
            ec = engine->Run(std::move(config));
        } catch (...) {
            ec = std::make_error_code(std::errc::io_error);
        }
        done.set_value(ec);
    });
    return {std::move(th), std::move(result)};
}

// 等待 Engine 发布 Running（有界）。返回值区分三路：
//   Running   相位已 Running；
//   Finished  Run 已返回（结果经 run_result 发布，调用方 get 一次）；
//   TimedOut  启动窗口耗尽（Run 仍在跑——调用方 RequestClose 并 join 后再读）。
// 失败经 future 即刻可见：Run 提前返回（端口占用/驱动槽不足等）时不再
// 等满整个窗口。
enum class EngineWait { Running, Finished, TimedOut };

EngineWait WaitEngineRunning(
    const std::shared_ptr<bbt::infra::net::Engine>& engine,
    std::future<std::error_code>& run_result) {
    const auto until = std::chrono::steady_clock::now() + kEngineRunWait;
    while (engine->Phase() != bbt::infra::net::EnginePhase::Running) {
        if (run_result.wait_for(std::chrono::milliseconds{0}) ==
            std::future_status::ready)
            return EngineWait::Finished;
        if (std::chrono::steady_clock::now() >= until)
            return EngineWait::TimedOut;
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    return EngineWait::Running;
}
#endif

} // namespace

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

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
void InfraHttpHost::SetRpcInboundTransport(
    bbt::framework::RpcInboundBridge bridge) {
    // Start 前装配期调用；Start 后不再变更。调用方（CoApp 构造）保证不越过
    // Create/Start 窗口——与 InstallRpcHandler 同一时序约束。
    m_inbound_bridge = bridge;
}

std::shared_ptr<bbt::infra::net::Engine> InfraHttpHost::egress_engine() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    // 入站宿主：共享 server Engine（Running 时即可 CoDial，零额外驱动槽）；
    // 纯出站宿主：Start 期创建的 client-only Engine。
    if (m_engine)
        return m_engine;
    return m_egress_engine;
}
#endif

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
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    // ProtoWireV1/TcpV1：inbound transport + rpc::Server::Create 必须发生在
    // Engine Idle 期（只配置 handoff，不 Run/监听/建线程），故 server 装配
    // 放这里而不是 Start。Engine 的 Run/Start 归 Start 相位；监听器归
    // EngineConfig，不由 rpc::Server 暴露。transport 选择：
    //   ProtoWireV1 → HttpRpcInboundTransport（HTTP/1.1 + protobuf body）
    //   TcpV1       → TcpRpcInboundTransport（u32be 长度前缀帧）
    if (m_state->rpc) {
        auto engine = std::make_shared<bbt::infra::net::Engine>(
            std::shared_ptr<bbt::infra::net::RawHandler>{});
        std::shared_ptr<bbt::infra::rpc::IRpcInboundTransport> inbound;
        if (m_inbound_bridge == RpcInboundBridge::TcpV1) {
#if defined(BBT_FRAMEWORK_HAS_RPC_TCP)
            bbt::infra::rpc::TcpRpcServerOptions options;
            options.limits = m_limits;
            auto made = bbt::infra::rpc::TcpRpcInboundTransport::Create(
                engine, options);
            if (!made)
                return result<void>::err(std::move(made.error()));
            inbound = std::move(made).value();
#else
            return result<void>::err(MakeError(
                ErrorCode::RuntimeUnavailable,
                "InfraHttpHost::Create: TcpV1 requires bbt::infra_rpc_tcp "
                "(BBT_INFRA_RPC_TCP not configured)"));
#endif
        } else {
            bbt::infra::http::ServerOptions options;
            options.limits = m_limits;
            // 上游要求 max_target_bytes <= limits.max_header_bytes：target
            // 上限不能超过已配置 header 上限。从既有 header 限额导出（不放大
            // 用户限额），保持较小的合法 max_header_bytes（如 4096）可用。
            options.max_target_bytes = std::min<std::size_t>(
                options.max_target_bytes, m_limits.max_header_bytes);
            auto made = bbt::infra::rpc::HttpRpcInboundTransport::Create(
                engine, options);
            if (!made)
                return result<void>::err(std::move(made.error()));
            inbound = std::move(made).value();
        }
        auto server = bbt::infra::rpc::Server::Create(
            std::move(inbound),
            [state = m_state](bbt::infra::IncomingCallContext ctx,
                              bbt::infra::RpcEnvelope req) {
                return _InvokeRpc(state, std::move(ctx), std::move(req));
            });
        if (!server)
            return result<void>::err(std::move(server.error()));
        std::lock_guard<std::mutex> lk(m_mtx);
        m_engine     = std::move(engine);
        m_rpc_server = std::move(server.value());
    }
#endif
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

    const auto state = m_state;
    if (state->rpc) {
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
        std::shared_ptr<bbt::infra::net::Engine> engine;
        std::shared_ptr<bbt::infra::rpc::Server> server;
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            engine = m_engine;
            server = m_rpc_server;
        }
        if (!engine || !server)
            return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
                "InfraHttpHost::Start: rpc engine/server not created"));

        // Engine::Run 占宿主专用 native 线程：server 布局（listen 非空）
        // 要求 handoff（rpc::Server::Create 已在 Idle 期配置），
        // runtime_progress=Independent 声明由独立执行流（本仓 S 线程 /
        // Processer driver）持续推进 runtime。
        bbt::infra::net::EngineConfig config;
        config.runtime_progress = bbt::infra::net::RuntimeProgress::Independent;
        config.options.num_event_loop = 1;   // server：1 main + 1 sub
        std::error_code addr_error;
        auto listen = bbt::infra::net::Address::Ip(
            m_listen.host, static_cast<int>(m_listen.port), addr_error);
        if (addr_error)
            return result<void>::err(EngineError(addr_error,
                "InfraHttpHost::Start: invalid listen address"));
        config.listen_addresses = {listen};

        // Engine::Run 占宿主专用 native 线程；Run 结果经 promise/future 跨线程
        // 发布（唯一同步口），Run 提前返回经 wait_for(0) 即刻可见——不复制
        // error_code、不等满窗口（F4）。
        auto [th, run_result] = StartEngineThread(engine, config);
        m_engine_thread = std::move(th);
        m_engine_result = std::move(run_result);
        const auto wait = WaitEngineRunning(engine, m_engine_result);
        if (wait != EngineWait::Running) {
            // Finished：Run 已返回，join 收线程后直接读已发布结果；
            // TimedOut：仍在启动窗口——先 RequestClose 再 join，Run 返回后
            // 结果保证已发布（join 完成即线程函数已结束）。
            if (wait == EngineWait::TimedOut)
                engine->RequestClose();
            if (m_engine_thread.joinable())
                m_engine_thread.join();
            return result<void>::err(EngineError(m_engine_result.get(),
                "InfraHttpHost::Start: rpc engine did not reach Running"));
        }

        std::vector<bbt::infra::net::Address> addresses;
        if (auto ec = engine->LocalAddresses(addresses); ec || addresses.empty())
            return result<void>::err(EngineError(ec,
                "InfraHttpHost::Start: engine LocalAddresses failed"));
        // rpc::Server::Start 一次性：Engine Running 且有监听后登记接纳。
        if (auto r = server->Start(); !r)
            return r;

        // ProtoWireV1/TcpV1 出站不另建 server-side 资源：rpc::Client 经
        // HttpRpcTransport/TcpRpcTransport 借用本 Engine（Running 即可
        // CoDial）；egress_engine() 对本路径返回 m_engine——零额外驱动槽，
        // 纯入站既有 worker 配置保持可启动（F2/F3）。
        //
        // bound_address() 的就绪判据原子（server 已 Start + bound 已就位）。
        // net::Address::ToString() 已含端口，故主机部分沿用配置
        // m_listen.host，端口取内核分配结果。rpc::Server 本体在 Create 期已
        // 就位，不能用它当监听就绪判据——须由 ready 标志单独表达。
        const auto bound = addresses.front();
        std::lock_guard<std::mutex> lk(m_mtx);
        m_bound.host  = m_listen.host;
        m_bound.port  = bound.Port();
        m_bound_ready = true;
        return result<void>::ok();
#else
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "InfraHttpHost::Start: ProtoWireV1 requires bbt::infra_rpc_net"));
#endif
    } else if (state->http) {
        auto srv = rt->ListenHttp(m_listen, [state](bbt::infra::IncomingCallContext ctx, bbt::infra::HttpRequest req) {
            return _InvokeHttp(state, std::move(ctx), std::move(req));
        });
        if (!srv)
            return result<void>::err(std::move(srv.error()));
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_server = std::move(srv.value());
            m_bound  = m_server->LocalAddress();
            m_bound_ready = true;
        }
    }
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    // 非 wire 入站（无 server Engine 可借）时创建 client-only 出站 Engine：
    // HttpEgress 的 rpc::Client（HttpRpcTransport/TcpRpcTransport）借用底座。
    // 与 server Engine 同一装配/收口模型：Start 拉起 Run 线程，Close
    // RequestClose+join+Stopped 核验。LegacyHeaders 出站不经此 Engine
    // （旧栈 NetworkRuntime::CreateHttpClient），但本宿主无法预知出站
    // profile——底座常备，代价固定为 1 个驱动槽 + 1 条 Run 线程。
    // 已装 server Engine（state->rpc）时跳过：egress_engine() 直接借用它。
    if (!state->rpc) {
        auto engine = std::make_shared<bbt::infra::net::Engine>(
            std::shared_ptr<bbt::infra::net::RawHandler>{});
        bbt::infra::net::EngineConfig config;
        config.runtime_progress = bbt::infra::net::RuntimeProgress::Independent;
        config.options.num_event_loop = 1;
        auto [th, run_result] = StartEngineThread(engine, config);
        m_egress_thread = std::move(th);
        m_egress_result = std::move(run_result);
        const auto wait = WaitEngineRunning(engine, m_egress_result);
        if (wait != EngineWait::Running) {
            if (wait == EngineWait::TimedOut)
                engine->RequestClose();
            if (m_egress_thread.joinable())
                m_egress_thread.join();
            return result<void>::err(EngineError(m_egress_result.get(),
                "InfraHttpHost::Start: egress engine did not reach Running"));
        }
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_egress_engine = std::move(engine);
        }
    }
#endif
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
    std::shared_ptr<bbt::infra::HttpServer> srv;
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    std::shared_ptr<bbt::infra::rpc::Server> rsrv;
#endif
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        srv = m_server;
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
        rsrv = m_rpc_server;
#endif
    }
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    if (rsrv)
        rsrv->StopAccepting();
    else
#endif
    if (srv)
        srv->StopAccepting();
}

result<void> InfraHttpHost::WaitHandlersDone(
    bbt::coroutine::Deadline deadline) {
    // 记账与 handler 同在 m_state：宿主析构不影响在途协程访问计数。
    const auto state = m_state;
    {
        std::unique_lock<std::mutex> lk(state->mtx);
        // 谓词等待 + 到期判定：_Invoke* 每次减一都在同一锁序下 notify_all，
        // 无遗漏唤醒；deadline 到点仍有在途 handler → TimedOut。
        if (!state->cv.wait_until(lk, deadline, [&state] { return state->inflight == 0; }))
            return result<void>::err(MakeError(ErrorCode::TimedOut,
                "inbound handlers still running at deadline"));
    }
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    // ProtoWireV1：framework 在途归零后的收口责任分两段，全部在本入口承担
    // （可返回 result，HostLifecycle 据此进入 incomplete/续等/失败路径）：
    //
    //   1) rsrv->Drain(deadline)：等已授权 handler 真实返回且回复写终态。
    //      未成功 Start（m_bound_ready 未置位）的宿主没有已接纳 handler，
    //      不调用 Drain——上游契约「StopAccepting before Drain」在未监听宿主
    //      上不成立，只跳过。
    //   2) rsrv->Close() + rsrv->WaitStopped(deadline)：强制逻辑封口自身
    //      连接，再等 listener 停、全部 callback 销毁、连接 Released。
    //      Drain==0 只证明 inflight==0，不证明 tasks==0——TaskLease 在
    //      callback 捕获最后销毁时才递减，WaitStopped 是唯一能证明
    //      callback 排空/会话清账/AcceptingStopped 的入口（上游
    //      http/Server.hpp:58-60）。失败（含超时）如实返回，由
    //      HostLifecycle 续等/判失败；绝不把 WaitStopped 失败当作「可随后
    //      续等」而直接去停 Engine/释放 owner（Engine 的 Run/join 不持有
    //      Scheduler 上的 TaskLease，join 不是 callback 销毁的凭据）。
    //      Deadline::max() 续等时同一路径：续等成功才置 m_stopped 并允许
    //      Close 释放；续等仍失败 → HostLifecycle fail-closed 保活 owner。
    std::shared_ptr<bbt::infra::rpc::Server> rsrv;
    bool bound_ready;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        rsrv = m_rpc_server;
        bound_ready = m_bound_ready;
    }
    if (rsrv && bound_ready) {
        if (auto r = rsrv->Drain(deadline); !r)
            return r;
        rsrv->Close();
        if (auto stopped = rsrv->WaitStopped(deadline); !stopped)
            return result<void>::err(std::move(stopped.error()));
        std::lock_guard<std::mutex> lk(m_mtx);
        m_stopped = true;
    }
#endif
    return result<void>::ok();
}

void InfraHttpHost::Close() noexcept {
    // 拒绝门先置关：Close 之后新进入的 handler 被拒，不再向下调用业务。
    // （不追溯已在途者——其排空由 WaitHandlersDone 与 CoApp 生命周期负责。）
    {
        std::lock_guard<std::mutex> lk(m_state->mtx);
        m_state->open = false;
    }

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    // ProtoWireV1 同步收口序列（上游 rpc::Server 契约，每项幂等）：
    //   WaitHandlersDone 已承担 server->Close()+WaitStopped(deadline)（含
    //   HostLifecycle Deadline::max 续等）——m_stopped 置位是唯一「
    //   callback 已全部销毁、连接 Released」的物理凭据；Close 只在取得该
    //   凭据后停 Engine 并释放 owner。未取得凭据（未经 WaitHandlersDone
    //   的裸 Close，或续等仍失败）→ 保留 server/engine/runtime  owner 与
    //   Run 线程，由外部终止进程或后续 HostLifecycle 续等收口；不伪报
    //   「已物理关闭」（review-fixes §29-42 缺口修复）。Engine Run 的
    //   future 结果与 Stopped 相位必须实际检查：Run 返回非零或相位未
    //   Stopped（同代解绑失败保留 Closing）都不是「网络资源/回调/driver
    //   已收口」的证据，此时同样保留 owner 不释放（fail-closed）。
    std::shared_ptr<bbt::infra::net::Engine> engine;
    bool stopped;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        engine  = m_engine;
        stopped = m_stopped;
    }
    if (engine && !m_engine_closed) {
        if (!stopped) {
            // 兜底：直接 Close（不经 StopAccepting/WaitHandlersDone）且
            // server 从未 Start（m_bound_ready 未置位）——此时没有已接纳
            // handler/callback，按原同步关闭契约仍可安全收口（server
            // Stop 幂等，WaitStopped 只需「stopping 置位」即可判定）。先
            // 置关再取证，与已 Start 路径同一判据。
            std::shared_ptr<bbt::infra::rpc::Server> rsrv;
            {
                std::lock_guard<std::mutex> lk(m_mtx);
                rsrv = m_rpc_server;
            }
            if (rsrv) {
                rsrv->Close();
                if (auto ok = rsrv->WaitStopped(
                        std::chrono::steady_clock::now() + kEngineRunWait); !ok) {
                    std::lock_guard<std::mutex> lk(m_mtx);
                    m_close_error = std::move(ok.error());
                } else {
                    std::lock_guard<std::mutex> lk(m_mtx);
                    m_stopped = true;
                    stopped = true;
                }
            } else {
                stopped = true;   // 无 RPC server：无 callback 责任
            }
        }
        if (stopped) {
            if (m_engine_thread.joinable()) {
                engine->RequestClose();
                m_engine_thread.join();
            }
            // Run 线程从未拉起（Start 未成功/未调用）：无 Run 结果可查，
            // Engine 也未进入 Running——Idle Engine 无已安装 listener/
            // driver，相位停留在 Idle 即已收口，直接按已证实处理。
            if (!m_engine_result.valid()) {
                stopped = true;
            } else {
            // 收口须同时满足：Run 已发布结果（join 完成即线程函数结束，
            // future 已 ready）且返回码为零、相位已 Stopped——同代解绑
            // 失败保留 Closing 时不得放行 owner（Engine.cc:2779-2817）。
            // future::get 只能取一次，先取后判。
            std::error_code run_ec = std::make_error_code(std::errc::io_error);
            const bool run_ready =
                m_engine_result.wait_for(std::chrono::milliseconds{0}) ==
                    std::future_status::ready;
            if (run_ready)
                run_ec = m_engine_result.get();
            const bool stopped_ok =
                run_ready && !run_ec &&
                engine->Phase() == bbt::infra::net::EnginePhase::Stopped;
            if (!stopped_ok) {
                std::lock_guard<std::mutex> lk(m_mtx);
                if (!m_close_error)
                    m_close_error = EngineError(run_ec,
                        "InfraHttpHost::Close: engine did not reach Stopped");
            }
            // Engine 未达 Stopped → 保留 owner（fail-closed）：Run 线程已
            // join，后续不会重入；owner 保留等待外部终止或后续续等。
            stopped = stopped_ok;
            }
            // Engine 收口只尝试一次（join/get 不可重复）：无论成败都标记，
            // 幂等重入不再重复 join/get；未证实时 owner 保留由下方释放门
            // （stopped || !m_engine）负责。
            m_engine_closed = true;
        }
    }

    // client-only 出站 Engine 收口（与 server Engine 同一判据：Run 线程 join +
    // 结果零 + Stopped 相位实际检查）。无 rpc::Server/WaitStopped 凭据责任
    // （client-only 无已接纳 handler/callback）；仅当 Run 线程曾拉起时收口。
    // 未证实 Stopped → 保留 owner（fail-closed），不伪报已释放。
    std::shared_ptr<bbt::infra::net::Engine> egress;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        egress = m_egress_engine;
    }
    if (egress && !m_egress_engine_closed) {
        bool egress_stopped = false;
        if (m_egress_thread.joinable()) {
            egress->RequestClose();
            m_egress_thread.join();
        }
        if (!m_egress_result.valid()) {
            egress_stopped = true;   // Run 线程从未拉起：Idle 即已收口
        } else {
            std::error_code run_ec =
                std::make_error_code(std::errc::io_error);
            const bool run_ready =
                m_egress_result.wait_for(std::chrono::milliseconds{0}) ==
                    std::future_status::ready;
            if (run_ready)
                run_ec = m_egress_result.get();
            egress_stopped =
                run_ready && !run_ec &&
                egress->Phase() == bbt::infra::net::EnginePhase::Stopped;
            if (!egress_stopped) {
                std::lock_guard<std::mutex> lk(m_mtx);
                if (!m_close_error)
                    m_close_error = EngineError(run_ec,
                        "InfraHttpHost::Close: egress engine did not reach Stopped");
            }
        }
        // 收口只尝试一次：无论成败都标记，幂等重入不再重复 join/get。
        m_egress_engine_closed = true;
        if (egress_stopped) {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_egress_engine.reset();
        }
    }
#endif

    std::shared_ptr<bbt::infra::NetworkRuntime> rt;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        rt = m_runtime;
    }
    // 同步收口：NetworkRuntime::Close 逐个收口子对象（含 HttpServer 底层）、
    // transport 与 io 域，返回即物理资源已释放；不重开、无协程等待。
    if (rt)
        rt->Close();
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_server.reset();
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
        // 仅当 RPC 收口凭据已证实（WaitStopped ok + Engine Stopped）才释放
        // server/engine owner；未证实则保留（fail-closed），不伪报已释放。
        if (stopped || !m_engine) {
            m_rpc_server.reset();
            m_engine.reset();
            m_stopped = false;
        }
#endif
        m_runtime.reset();
        m_bound_ready = false;
        m_bound = {};
    }
}

std::optional<bbt::infra::ListenAddress> InfraHttpHost::bound_address() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    if (!m_bound_ready)
        return std::nullopt;
    return m_bound;
}

std::string InfraHttpHost::bound_endpoint() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    if (!m_bound_ready)
        return {};
    return m_bound.host + ":" + std::to_string(m_bound.port);
}

std::shared_ptr<bbt::infra::HttpServer> InfraHttpHost::http_server() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_server;
}

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
std::shared_ptr<bbt::infra::rpc::Server> InfraHttpHost::rpc_server() const
{
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_rpc_server;
}

std::optional<bbt::infra::Error> InfraHttpHost::close_error() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_close_error;
}
#endif

std::shared_ptr<bbt::infra::NetworkRuntime>
InfraHttpHost::network_runtime() const {
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_runtime;
}

} // namespace bbt::framework
