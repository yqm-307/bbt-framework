// bbt-framework Issue #4 RPC-T4（修偏轮）：并发隔离 / 容量 / 迟到寿命 / 关闭矩阵。
//
// 修正要点（相对旧候选 4207772d…）：
//   旧候选经 MakeCoAppForTest + DispatchInboundForTest + StubNetHost（INetworkHost
//   桩）走 LegacyHeaders/positional，未走正式 wire 路径——那不是本票核心验收。
//   本文件改为：
//     * 服务端：公开 `fw::CoApp app(opts)`（opts.inbound_bridge=ProtoWireV1），
//       真实 InfraHttpHost 监听 127.0.0.1:0，业务服务经公开 ProtoMethod 注册
//       测试专用 Hold/Fast（只读 GetValue 语义，无写副作用）；
//     * 客户端：infra 公开 `NetworkRuntime::Create → Start → CreateHttpClient`
//       得真实 HttpClient，经 rpc::ToWireEnvelope + MakeRpcWireHttpRequest 组真实
//       POST /rpc protobuf body，走真实 socket loopback（非注入 rpc_send、非直接
//       dispatch、非私有身份）。
//   未认证 principal 来自真实 transport（loopback 无认证 → 空串），测试不手造
//   IncomingCallContext。
//
// 用例：
//   T1 r6_concurrent_context_not_crossed        并发挂起/逆序恢复；上下文不串。
//   T2 r6_client_timeout_inflight_holds_capacity 客户端先超时（OutcomeUnknown+
//                                                 committed），handler 物理在途
//                                                 仍占容量 → 新请求精确 Overloaded。
//   T3 r7_capacity_overloaded_then_recovers_net  Service max_inflight 有界 + 峰值
//                                                 oracle + 释放恢复。
//   T4 r8_normal_shutdown_after_drain            正常排空 → Close → Closed（配对）。
//   T5 r8_shutdown_incomplete_late_completion    在途关闭 → ShutdownIncomplete →
//                                                 迟到完成 → 资源 Close 在 handler
//                                                 完成后。
//
// 诚实缺口：R4/S4（跨 hop 预算）、R8/S8（独立 supervisor 硬停）未覆盖；
// NetworkLimits 的 max_inflight/字节维度未分别压测（R7 partial）。
//
// 与仓内既有单测一致：Boost.Test 经 included/unit_test.hpp 静态内嵌。

#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/rpc/RpcWire.hpp>

#include <bbt/example/v1/get_value.pb.h>

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/ShutdownState.hpp>
#include <bbt/framework/internal/HostLifecycle.hpp>

// ProtoCodec<GetValueRequest/Response> 特化（schema 真源 = .proto descriptor）。
#include "getvalue_service.hpp"

namespace fw  = bbt::framework;
namespace co  = bbt::coroutine;
namespace inf = bbt::infra;
namespace rpcw = bbt::infra::rpc;
using Clock = std::chrono::steady_clock;

namespace {

constexpr std::chrono::milliseconds kWait{15000};
// P0-A 结构化结果标记：未认证 loopback，不表示身份已认证。
constexpr const char* kAuthState = "unauthenticated_loopback";

// ── 屏障原语（std CV / 协程闩；无固定 sleep 凑时序）────────────────────

class TestLatch {
public:
    explicit TestLatch(int n) : m_count(n) {}
    void CountDown() {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (--m_count > 0) return;
        }
        m_cv.notify_all();
    }
    bool WaitFor(std::chrono::milliseconds ms) {
        std::unique_lock<std::mutex> lk(m_mtx);
        return m_cv.wait_for(lk, ms, [this] { return m_count == 0; });
    }
private:
    std::mutex              m_mtx;
    std::condition_variable m_cv;
    int                     m_count;
};

// 单次放行协程闩：Open 前 Wait 真实挂起当前 handler 协程；Open 早到不丢唤醒。
class CoGate {
public:
    void Open() {
        std::vector<co::sync::CoWaiter::SPtr> notify;
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_open = true;
            notify.swap(m_waiters);
        }
        for (auto& w : notify) w->Notify();
    }
    co::WaitStatus Wait(co::Deadline deadline) {
        auto waiter = co::sync::CoWaiter::Create();
        co::WaitOptions wo;
        wo.deadline = deadline;
        return waiter->WaitWithCallback(wo, [&]() -> bool {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (m_open) waiter->Notify();
            else        m_waiters.push_back(waiter);
            return true;
        });
    }
private:
    std::mutex                            m_mtx;
    bool                                  m_open = false;
    std::vector<co::sync::CoWaiter::SPtr> m_waiters;
};

// ── 观测态 ─────────────────────────────────────────────────────────────

struct CtxSnapshot {
    bool          valid = false;
    std::string   request_id;
    std::string   peer_principal;
    co::Deadline  deadline{};
    std::uint64_t co_id = 0;
};

struct SequenceLog {
    std::mutex               mtx;
    std::vector<std::string> events;
    void Push(const std::string& e) {
        std::lock_guard<std::mutex> lk(mtx);
        events.push_back(e);
    }
    int IndexOf(const std::string& e) {
        std::lock_guard<std::mutex> lk(mtx);
        for (std::size_t i = 0; i < events.size(); ++i)
            if (events[i] == e) return static_cast<int>(i);
        return -1;
    }
};

std::shared_ptr<SequenceLog> g_seq;

struct ProbeState {
    explicit ProbeState(int arrive_n) : arrived(arrive_n) {}

    TestLatch arrived;          // 到达 handler 的请求数
    TestLatch handler_done{1};  // 单跟踪 handler 的完成（仅 T2/T5 用）

    std::mutex                                        mtx;
    std::map<std::string, std::shared_ptr<CoGate>>    gates;
    std::map<std::string, CtxSnapshot>                before;
    std::map<std::string, CtxSnapshot>                after;
    std::map<std::string, bool>                       active;
    std::vector<std::string>                          completed;
    int                                               cur_active  = 0;
    int                                               peak_active = 0;

    std::shared_ptr<CoGate> GateFor(const std::string& rid) {
        std::lock_guard<std::mutex> lk(mtx);
        auto& g = gates[rid];
        if (!g) g = std::make_shared<CoGate>();
        return g;
    }
    bool Active(const std::string& rid) {
        std::lock_guard<std::mutex> lk(mtx);
        const auto it = active.find(rid);
        return it != active.end() && it->second;
    }
    CtxSnapshot At(const std::map<std::string, CtxSnapshot>& src,
                   const std::string& rid) {
        std::lock_guard<std::mutex> lk(mtx);
        const auto it = src.find(rid);
        return it == src.end() ? CtxSnapshot{} : it->second;
    }
    int Peak() {
        std::lock_guard<std::mutex> lk(mtx);
        return peak_active;
    }
    std::vector<std::string> Completed() {
        std::lock_guard<std::mutex> lk(mtx);
        return completed;
    }
    void CompleteAll() {
        std::vector<std::shared_ptr<CoGate>> all;
        {
            std::lock_guard<std::mutex> lk(mtx);
            for (auto& kv : gates) all.push_back(kv.second);
        }
        for (auto& g : all) g->Open();
    }
};

std::shared_ptr<ProbeState> g_probe;

CtxSnapshot SnapCtx(const fw::RequestContext& c) {
    CtxSnapshot s;
    s.valid          = true;
    s.request_id     = c.request_id;
    s.peer_principal = c.peer_principal;
    s.deadline       = c.deadline;
    s.co_id          = co::GetLocalCoroutineId();
    return s;
}

// ── 资源 owner（自有 ICoCloseable；记录 Close 时序，不伪造计数）─────────

class OwnerRes final : public inf::ICoCloseable {
public:
    void Close() noexcept override {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (m_closed) return;
        m_closed = true;
        if (g_seq) g_seq->Push("resource_close");
    }
    bool IsClosed() const noexcept override {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_closed;
    }
private:
    mutable std::mutex m_mtx;
    bool               m_closed = false;
};

// ── 测试服务：公开 ProtoMethod，只读 GetValue 语义 ─────────────────────

class NetMatrixSvc final : public fw::CoService<NetMatrixSvc> {
public:
    static constexpr std::string_view kServiceName = "test.matrix.GetValueService";

    // Hold：快照上下文 → 到 arrived → 在 per-rid 独立 gate 上以无界期限挂起
    // （不依 ctx.deadline 自动返回，保持物理在途）→ 恢复后再快照当前上下文。
    fw::CoRpcResp Hold(fw::CoRpcReq req) {
        auto parsed = req.ParseProto<bbt::example::v1::GetValueRequest>();
        if (!parsed) return fw::CoRpcResp::Error(parsed.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        auto p = g_probe;
        if (!p) return fw::CoRpcResp::Error(fw::MakeError(
            fw::ErrorCode::InternalError, "no probe"));
        const std::string rid = ctx.value()->request_id;
        {
            std::lock_guard<std::mutex> lk(p->mtx);
            p->before[rid] = SnapCtx(*ctx.value());
            p->active[rid] = true;
            ++p->cur_active;
            if (p->cur_active > p->peak_active) p->peak_active = p->cur_active;
        }
        auto gate = p->GateFor(rid);
        p->arrived.CountDown();
        const auto st = gate->Wait(co::Deadline::max());
        const bool completed = (st == co::WaitStatus::Completed);
        {
            std::lock_guard<std::mutex> lk(p->mtx);
            if (completed) {
                auto ctx2 = fw::CurrentRequestContext();   // 恢复后重新取受管上下文
                if (ctx2) p->after[rid] = SnapCtx(*ctx2.value());
                p->completed.push_back(rid);
                if (g_seq) g_seq->Push("handler_done:" + rid);
            }
            p->active[rid] = false;
            --p->cur_active;
        }
        p->handler_done.CountDown();
        if (!completed)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::TimedOut, "hold: gate not opened"));
        bbt::example::v1::GetValueResponse out;
        out.set_found(true);
        out.set_value("held:" + rid);
        return fw::CoRpcResp::FromProto(out);
    }

    fw::CoRpcResp Fast(fw::CoRpcReq req) {
        auto parsed = req.ParseProto<bbt::example::v1::GetValueRequest>();
        if (!parsed) return fw::CoRpcResp::Error(parsed.error());
        bbt::example::v1::GetValueResponse out;
        out.set_found(true);
        out.set_value("fast:" + parsed.value().key());
        return fw::CoRpcResp::FromProto(out);
    }

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::ProtoMethod<&NetMatrixSvc::Hold,
                        bbt::example::v1::GetValueRequest,
                        bbt::example::v1::GetValueResponse>("Hold"),
        fw::ProtoMethod<&NetMatrixSvc::Fast,
                        bbt::example::v1::GetValueRequest,
                        bbt::example::v1::GetValueResponse>("Fast"));
};

// ── 装配 ───────────────────────────────────────────────────────────────

fw::ServiceOptions ConcurrentOpts(std::size_t max_inflight) {
    return fw::ServiceOptions{
        fw::ExecutionPolicy::Concurrent,
        max_inflight, /*mailbox_capacity*/ 0, /*max_actors*/ 0,
        /*ordered_ingress*/ false, /*max_ordered_streams*/ 0,
        /*max_cached_results*/ 0, /*max_cached_result_bytes*/ 0};
}

// 正式装配：inbound_bridge=ProtoWireV1（proto body wire profile）。
fw::CoAppOptions AppOpts(std::chrono::milliseconds shutdown_step_budget) {
    fw::CoAppOptions o{};
    o.network_limits = inf::NetworkLimits{
        /*max_connections*/ 64, /*max_inflight*/ 64,
        /*max_header_bytes*/ 16 * 1024, /*max_body_bytes*/ 64 * 1024,
        /*incoming_timeout*/ std::chrono::milliseconds{30000}};
    o.listen = inf::ListenAddress{"127.0.0.1", 0};
    o.static_routes = {};
    o.shutdown_step_budget = shutdown_step_budget;
    o.inbound_bridge = fw::RpcInboundBridge::ProtoWireV1;
    return o;
}

struct RunHandle {
    std::thread       th;
    std::atomic<bool> done{false};
    int               rc{-1};
};
struct RunJoinOnExit {
    fw::CoApp*                  app = nullptr;
    std::unique_ptr<RunHandle>* h   = nullptr;
    void JoinNow() { if (h && *h && (*h)->th.joinable()) (*h)->th.join(); }
    // 失败/异常退出也必须安全收尾：先 request_shutdown 再释放全部挂起 gate，
    // 避免存在在途 handler 时 join 无界阻塞（不依赖 ctest 超时兜底）。
    ~RunJoinOnExit() {
        if (app) app->request_shutdown();
        if (g_probe) g_probe->CompleteAll();
        JoinNow();
    }
};

std::unique_ptr<RunHandle> RunApp(fw::CoApp& app) {
    // 配置非线程安全：仅在调度器尚未初始化（无工作线程）时写入进程全局配置。
    // HostLifecycle 只在未初始化时 Start，运行时为进程寿命单例（无 Stop），
    // 故 T2+ 调用时调度器已初始化，跳过写入以规避运行期数据竞争。
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
        // handler 在协程内做 proto wire 编解码 + 分发栈，默认 12KB 栈触底；
        // 按示例口径放大到 512KB（仅覆盖本测试负载规模）。
        g_bbt_coroutine_config->m_cfg_stack_size = 512 * 1024;
    }
    auto h = std::make_unique<RunHandle>();
    RunHandle* p = h.get();
    h->th = std::thread([p, &app] {
        p->rc = app.run();
        p->done.store(true, std::memory_order_release);
    });
    return h;
}

// 有界 yield 轮询观测（非固定 sleep 凑时序）。
std::string WaitEndpoint(fw::CoApp& app) {
    const auto end = Clock::now() + kWait;
    while (Clock::now() < end) {
        auto ep = app.bound_endpoint();
        if (!ep.empty()) return ep;
        std::this_thread::yield();
    }
    return {};
}

bool WaitShutdownState(fw::CoApp& app, fw::ShutdownState want,
                       std::chrono::milliseconds ms = kWait) {
    const auto end = Clock::now() + ms;
    while (Clock::now() < end) {
        if (app.shutdown_state() == want) return true;
        std::this_thread::yield();
    }
    return app.shutdown_state() == want;
}

// ── 客户端：infra 公开 NetworkRuntime + HttpClient（真实 socket）───────

struct ClientEnv {
    std::shared_ptr<inf::NetworkRuntime> rt;
    std::shared_ptr<inf::HttpClient>     client;

    bool Start() {
        inf::NetworkLimits lim{};
        lim.max_connections  = 64;
        lim.max_inflight     = 64;
        lim.max_header_bytes = 16 * 1024;
        lim.max_body_bytes   = 64 * 1024;
        lim.incoming_timeout = std::chrono::milliseconds{30000};
        auto r = inf::NetworkRuntime::Create(lim);
        if (!r) return false;
        rt = r.value();
        if (auto s = rt->Start(); !s) return false;
        auto c = rt->CreateHttpClient();
        if (!c) return false;
        client = c.value();
        return true;
    }
    void Stop() { if (rt) rt->Close(); }
};
struct ClientStopOnExit {
    ClientEnv* c;
    ~ClientStopOnExit() { if (c) c->Stop(); }
};

struct ClientCallOut {
    std::optional<inf::result<inf::HttpResponse>> r;
    TestLatch                                     done{1};
};

std::shared_ptr<ClientCallOut> CallAsync(
    const std::shared_ptr<inf::HttpClient>& client,
    const std::string& endpoint,
    const std::string& method,
    const std::string& rid,
    const std::string& key,
    std::uint32_t      budget_ms,
    co::Deadline       deadline) {
    auto out = std::make_shared<ClientCallOut>();
    inf::RpcEnvelope env;
    env.service         = std::string(NetMatrixSvc::kServiceName);
    env.method          = method;
    env.request_id      = rid;
    env.request_schema  =
        bbt::example::v1::GetValueRequest::descriptor()->full_name();
    env.response_schema =
        bbt::example::v1::GetValueResponse::descriptor()->full_name();
    // 合法元数据（framework 入站接受的 route.* 前缀）：证明 metadata 不升级
    // peer_principal。（注：framework 入站仅接受 route.* 前缀，trace.* 被拒。）
    env.metadata = {{"route.rid", rid}};
    bbt::example::v1::GetValueRequest q;
    q.set_key(key);
    std::string bytes;
    q.SerializeToString(&bytes);
    env.payload.assign(bytes.begin(), bytes.end());

    auto wire = rpcw::ToWireEnvelope(env, budget_ms);
    if (!wire) {
        out->r.emplace(inf::result<inf::HttpResponse>::err(wire.error()));
        out->done.CountDown();
        return out;
    }
    auto req = rpcw::MakeRpcWireHttpRequest(wire.value(), "http://" + endpoint);
    if (!req) {
        out->r.emplace(inf::result<inf::HttpResponse>::err(req.error()));
        out->done.CountDown();
        return out;
    }
    inf::HttpRequest r = std::move(req.value());
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [out, client, r = std::move(r), deadline]() mutable {
            inf::CallOptions opt;
            opt.deadline = deadline;
            out->r.emplace(client->Request(std::move(r), opt));
            out->done.CountDown();
        },
        succ);
    return succ ? out : nullptr;
}

bool WaitCall(const std::shared_ptr<ClientCallOut>& out,
              std::chrono::milliseconds ms = kWait) {
    return out && out->done.WaitFor(ms);
}

bool GetWireReply(const std::shared_ptr<ClientCallOut>& out,
                  rpcw::RpcWireEnvelope& w) {
    if (!out || !out->r.has_value()) return false;
    if (!out->r.value()) return false;                      // transport 失败
    auto parsed = rpcw::ParseRpcWireHttpResponse(out->r.value().value());
    if (!parsed) return false;
    w = std::move(parsed.value());
    return true;
}

int CodeOf(fw::ErrorCode c) { return static_cast<int>(c); }

} // namespace

BOOST_AUTO_TEST_SUITE(framework_matrix_network)

// T1（A）：3 条真实 network 请求在各 per-rid gate 挂起，逐个逆序放行；上下文
//         随逻辑请求绑定、不串；合法 metadata 不升级未认证 principal。
BOOST_AUTO_TEST_CASE(r6_concurrent_context_not_crossed) {
    g_probe = std::make_shared<ProbeState>(3);
    fw::CoApp app(AppOpts(std::chrono::milliseconds{2000}));
    BOOST_REQUIRE(app.add_service<NetMatrixSvc>(ConcurrentOpts(8)));

    auto rh = RunApp(app);
    RunJoinOnExit join_guard{&app, &rh};
    const std::string ep = WaitEndpoint(app);
    BOOST_REQUIRE(!ep.empty());
    ClientEnv cl;
    BOOST_REQUIRE(cl.Start());
    ClientStopOnExit cl_guard{&cl};

    const std::vector<std::string> rids{"t1-a", "t1-b", "t1-c"};
    // 三个不同预算（15/20/25s，均 < 传输硬看门 incoming_timeout=30s）：生效期限
    // 由各自 remaining_budget_ms 主导、间隔 5s，故严格递增且与调度到达顺序无关。
    std::vector<std::shared_ptr<ClientCallOut>> outs;
    for (int i = 0; i < 3; ++i) {
        outs.push_back(CallAsync(
            cl.client, ep, "Hold", rids[i], "k" + std::to_string(i),
            static_cast<std::uint32_t>(15000 + i * 5000),
            Clock::now() + std::chrono::seconds{60}));
        BOOST_REQUIRE(outs.back() != nullptr);
    }
    if (!g_probe->arrived.WaitFor(std::chrono::milliseconds{6000})) {
        for (int i = 0; i < 3; ++i) {
            const bool d = WaitCall(outs[i], std::chrono::milliseconds{2000});
            std::string msg =
                "t1-diag rid=" + rids[i] + " call_done=" + (d ? "1" : "0");
            if (d && outs[i]->r.has_value()) {
                if (!outs[i]->r.value()) {
                    msg += " transport_err_code=" + std::to_string(
                        static_cast<int>(outs[i]->r.value().error().code));
                    msg += " msg=" + outs[i]->r.value().error().message;
                } else {
                    auto pr = rpcw::ParseRpcWireHttpResponse(
                        outs[i]->r.value().value());
                    if (pr) {
                        msg += " wire_success=" + std::string(
                            pr.value().success ? "true" : "false");
                        if (!pr.value().success) {
                            msg += " wire_code=" + std::to_string(
                                static_cast<int>(pr.value().error.code));
                            msg += " wire_msg=" + pr.value().error.message;
                        }
                    } else {
                        msg += " wire_parse_failed";
                    }
                }
            }
            BOOST_TEST_MESSAGE(msg);
        }
        BOOST_REQUIRE(g_probe->arrived.WaitFor(std::chrono::milliseconds{1000}));
    }

    // 逐个逆序放行 + 等待对应 client done：完成顺序确定（非连续 Open）。
    for (int i = 2; i >= 0; --i) {
        g_probe->GateFor(rids[i])->Open();
        BOOST_REQUIRE(WaitCall(outs[i]));
        // 成功响应须解出正式业务 wire 信封（非仅 transport 层 200）：
        // success 与 request_id 精确匹配，且 payload 为 held:<rid> 业务值。
        rpcw::RpcWireEnvelope w;
        BOOST_REQUIRE(GetWireReply(outs[i], w));
        BOOST_REQUIRE(w.success);
        BOOST_TEST(w.request_id == rids[i]);
        bbt::example::v1::GetValueResponse resp;
        BOOST_REQUIRE(resp.ParseFromArray(w.payload.data(),
                                          static_cast<int>(w.payload.size())));
        BOOST_TEST(resp.value() == std::string("held:") + rids[i]);
    }

    auto completed = g_probe->Completed();
    BOOST_REQUIRE(completed.size() == 3);
    BOOST_CHECK(completed[0] == "t1-c");
    BOOST_CHECK(completed[1] == "t1-b");
    BOOST_CHECK(completed[2] == "t1-a");

    co::Deadline d[3];
    for (int i = 0; i < 3; ++i) {
        const auto b = g_probe->At(g_probe->before, rids[i]);
        const auto a = g_probe->At(g_probe->after, rids[i]);
        BOOST_CHECK(b.valid);
        BOOST_CHECK(a.valid);
        BOOST_CHECK(b.request_id == rids[i]);
        BOOST_CHECK(a.request_id == rids[i]);
        BOOST_CHECK(b.co_id != 0);
        BOOST_CHECK(a.co_id == b.co_id);       // 上下文随协程绑定
        BOOST_CHECK(b.peer_principal.empty()); // 合法 metadata 不升级 principal
        BOOST_CHECK(a.peer_principal.empty());
        BOOST_CHECK(a.deadline == b.deadline); // 跨挂起不漂
        d[i] = b.deadline;
    }
    BOOST_CHECK(d[0] < d[1]);
    BOOST_CHECK(d[1] < d[2]);                  // 各请求预算独立、未串
    BOOST_TEST_MESSAGE("auth_state=" << kAuthState);

    app.request_shutdown();
    join_guard.JoinNow();
    BOOST_CHECK(app.shutdown_state() == fw::ShutdownState::Closed);
}

// T2（B）：客户端有限 deadline 先落 OutcomeUnknown+RequestCommitted，handler 经
//         独立 gate 物理在途仍占容量（max_inflight=1）→ 新请求精确 Overloaded；
//         释放后容量恢复（探测请求非原调用 retry）；owner 在迟到完成前未被 Close
//         （IsClosed 仅观测 Close() 是否被调用，不表示所有权/寿命）。
BOOST_AUTO_TEST_CASE(r6_client_timeout_inflight_holds_capacity) {
    g_probe = std::make_shared<ProbeState>(1);
    g_seq   = std::make_shared<SequenceLog>();
    fw::CoApp app(AppOpts(std::chrono::milliseconds{2000}));
    BOOST_REQUIRE(app.add_service<NetMatrixSvc>(ConcurrentOpts(1)));
    auto owner = std::make_shared<OwnerRes>();
    BOOST_REQUIRE(app.add_resource<OwnerRes>(owner));

    auto rh = RunApp(app);
    RunJoinOnExit join_guard{&app, &rh};
    const std::string ep = WaitEndpoint(app);
    BOOST_REQUIRE(!ep.empty());
    ClientEnv cl;
    BOOST_REQUIRE(cl.Start());
    ClientStopOnExit cl_guard{&cl};

    // 取服务强引用后立即释放；释放不影响框架持有的实例（CoApp 生命周期期间
    // 持有）。此块不证明「因在途而强持有 service」。
    {
        auto svc = app.find_service(NetMatrixSvc::kServiceName);
        BOOST_REQUIRE(svc);
    }
    BOOST_CHECK(owner->IsClosed() == false);

    // A：client deadline 1.5s、预算 30s → handler 在 gate 上物理在途（不依 ctx.deadline）。
    auto a = CallAsync(cl.client, ep, "Hold", "t2-a", "ka", 30000u,
                       Clock::now() + std::chrono::milliseconds{1500});
    BOOST_REQUIRE(a != nullptr);
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));   // handler 已挂起
    BOOST_REQUIRE(WaitCall(a));
    BOOST_REQUIRE(a->r.has_value());
    BOOST_REQUIRE(!a->r.value());                     // transport 层失败
    const inf::Error& ae = a->r.value().error();
    BOOST_TEST(static_cast<int>(ae.code) == CodeOf(fw::ErrorCode::OutcomeUnknown));
    BOOST_REQUIRE(ae.request_phase.has_value());
    BOOST_TEST(static_cast<int>(*ae.request_phase) ==
               static_cast<int>(inf::RequestPhase::RequestCommitted));

    // 此刻：handler completion marker 仍无、owner 尚未被 Close
    // （IsClosed 只观测 Close() 调用，不代表所有权/寿命）。
    BOOST_CHECK(g_probe->Active("t2-a"));
    BOOST_CHECK(owner->IsClosed() == false);

    // 容量仍被占：新请求 Fast → 精确 Overloaded（只尝试一次）。
    auto b = CallAsync(cl.client, ep, "Fast", "t2-b", "kb", 30000u,
                       Clock::now() + std::chrono::seconds{30});
    BOOST_REQUIRE(b != nullptr);
    BOOST_REQUIRE(WaitCall(b));
    rpcw::RpcWireEnvelope wb;
    BOOST_REQUIRE(GetWireReply(b, wb));
    BOOST_CHECK(wb.success == false);
    BOOST_TEST(static_cast<int>(wb.error.code) == CodeOf(fw::ErrorCode::Overloaded));
    BOOST_TEST(wb.request_id == std::string("t2-b"));

    // release 原 handler → 迟到完成。
    g_probe->GateFor("t2-a")->Open();
    BOOST_REQUIRE(g_probe->handler_done.WaitFor(kWait));
    BOOST_CHECK(!g_probe->Active("t2-a"));

    // 容量恢复：新探测请求（非原调用 retry）成功。
    auto c = CallAsync(cl.client, ep, "Fast", "t2-c", "kc", 30000u,
                       Clock::now() + std::chrono::seconds{30});
    BOOST_REQUIRE(c != nullptr);
    BOOST_REQUIRE(WaitCall(c));
    rpcw::RpcWireEnvelope wc;
    BOOST_REQUIRE(GetWireReply(c, wc));
    BOOST_CHECK(wc.success);
    BOOST_TEST_MESSAGE("auth_state=" << kAuthState);

    app.request_shutdown();
    join_guard.JoinNow();
    BOOST_CHECK(app.shutdown_state() == fw::ShutdownState::Closed);
    BOOST_CHECK(owner->IsClosed());   // 关闭序列最终 Close owner
}

// T3（D）：Service max_inflight=2 有界 → 第三个明确 Overloaded（不排队）；峰值恰为
//         声明 limit；释放后恢复接纳。NetworkLimits 维度未分别压测（R7 partial）。
BOOST_AUTO_TEST_CASE(r7_capacity_overloaded_then_recovers_net) {
    g_probe = std::make_shared<ProbeState>(2);
    fw::CoApp app(AppOpts(std::chrono::milliseconds{2000}));
    BOOST_REQUIRE(app.add_service<NetMatrixSvc>(ConcurrentOpts(2)));

    auto rh = RunApp(app);
    RunJoinOnExit join_guard{&app, &rh};
    const std::string ep = WaitEndpoint(app);
    BOOST_REQUIRE(!ep.empty());
    ClientEnv cl;
    BOOST_REQUIRE(cl.Start());
    ClientStopOnExit cl_guard{&cl};

    std::vector<std::shared_ptr<ClientCallOut>> held;
    for (int i = 0; i < 2; ++i) {
        held.push_back(CallAsync(
            cl.client, ep, "Hold", "t3-h" + std::to_string(i),
            "kh" + std::to_string(i), 30000u,
            Clock::now() + std::chrono::seconds{60}));
        BOOST_REQUIRE(held.back() != nullptr);
    }
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));   // 两条都挂起 → 容量满
    BOOST_CHECK(g_probe->Peak() == 2);                // 真实峰值 = 声明 limit

    auto over = CallAsync(cl.client, ep, "Fast", "t3-over", "ko", 30000u,
                          Clock::now() + std::chrono::seconds{30});
    BOOST_REQUIRE(over != nullptr);
    BOOST_REQUIRE(WaitCall(over));
    rpcw::RpcWireEnvelope wo;
    BOOST_REQUIRE(GetWireReply(over, wo));
    BOOST_CHECK(wo.success == false);
    BOOST_TEST(static_cast<int>(wo.error.code) == CodeOf(fw::ErrorCode::Overloaded));
    BOOST_CHECK(g_probe->Peak() == 2);                // 拒绝路径不进 handler，峰值不越界

    g_probe->CompleteAll();
    for (std::size_t i = 0; i < held.size(); ++i) {
        auto& o = held[i];
        BOOST_REQUIRE(WaitCall(o));
        // held 回复须为正式业务成功信封（success + request_id 对应）。
        rpcw::RpcWireEnvelope wh;
        BOOST_REQUIRE(GetWireReply(o, wh));
        BOOST_REQUIRE(wh.success);
        BOOST_TEST(wh.request_id == std::string("t3-h") + std::to_string(i));
    }

    auto again = CallAsync(cl.client, ep, "Fast", "t3-again", "ka", 30000u,
                           Clock::now() + std::chrono::seconds{30});
    BOOST_REQUIRE(again != nullptr);
    BOOST_REQUIRE(WaitCall(again));
    rpcw::RpcWireEnvelope wa;
    BOOST_REQUIRE(GetWireReply(again, wa));
    BOOST_CHECK(wa.success);
    BOOST_TEST_MESSAGE("auth_state=" << kAuthState);

    app.request_shutdown();
    join_guard.JoinNow();
    BOOST_CHECK(app.shutdown_state() == fw::ShutdownState::Closed);
}

// T4（C-配对正常）：一次正常请求后 request_shutdown → 排空 → 同步 Close → Closed；
//                     无未完成清理项；资源 owner 被 Close。
BOOST_AUTO_TEST_CASE(r8_normal_shutdown_after_drain) {
    g_probe = std::make_shared<ProbeState>(0);
    g_seq   = std::make_shared<SequenceLog>();
    fw::CoApp app(AppOpts(std::chrono::milliseconds{2000}));
    BOOST_REQUIRE(app.add_service<NetMatrixSvc>(ConcurrentOpts(4)));
    auto owner = std::make_shared<OwnerRes>();
    BOOST_REQUIRE(app.add_resource<OwnerRes>(owner));

    auto rh = RunApp(app);
    RunJoinOnExit join_guard{&app, &rh};
    const std::string ep = WaitEndpoint(app);
    BOOST_REQUIRE(!ep.empty());
    ClientEnv cl;
    BOOST_REQUIRE(cl.Start());
    ClientStopOnExit cl_guard{&cl};

    BOOST_CHECK(app.shutdown_state() == fw::ShutdownState::Running);
    auto ok = CallAsync(cl.client, ep, "Fast", "t4-ok", "k", 30000u,
                        Clock::now() + std::chrono::seconds{30});
    BOOST_REQUIRE(ok != nullptr);
    BOOST_REQUIRE(WaitCall(ok));
    rpcw::RpcWireEnvelope wk;
    BOOST_REQUIRE(GetWireReply(ok, wk));
    BOOST_REQUIRE(wk.success);

    app.request_shutdown();
    join_guard.JoinNow();
    BOOST_CHECK(rh->rc == fw::HostLifecycle::kExitOk);
    BOOST_CHECK(app.shutdown_state() == fw::ShutdownState::Closed);
    BOOST_CHECK(app.pending_cleanup().empty());
    BOOST_CHECK(owner->IsClosed());
    BOOST_TEST_MESSAGE("auth_state=" << kAuthState);
}

// T5（C-超预算配对）：一条真实在途 handler 被 gate 扣住时 request_shutdown →
//      ShutdownIncomplete、run 未返回；释放 → 迟到完成 → run 收束；资源 Close
//      调用序列发生在 handler completion 之后（handler_done:t5-a <
//      resource_close）。不宣称析构/所有权观测。
BOOST_AUTO_TEST_CASE(r8_shutdown_incomplete_late_completion) {
    g_probe = std::make_shared<ProbeState>(1);
    g_seq   = std::make_shared<SequenceLog>();
    // 小步预算：在途 handler 让排空步超预算 → ShutdownIncomplete。
    fw::CoApp app(AppOpts(std::chrono::milliseconds{300}));
    BOOST_REQUIRE(app.add_service<NetMatrixSvc>(ConcurrentOpts(4)));
    auto owner = std::make_shared<OwnerRes>();
    BOOST_REQUIRE(app.add_resource<OwnerRes>(owner));

    auto rh = RunApp(app);
    RunJoinOnExit join_guard{&app, &rh};
    const std::string ep = WaitEndpoint(app);
    BOOST_REQUIRE(!ep.empty());
    ClientEnv cl;
    BOOST_REQUIRE(cl.Start());
    ClientStopOnExit cl_guard{&cl};

    auto a = CallAsync(cl.client, ep, "Hold", "t5-a", "ka", 30000u,
                       Clock::now() + std::chrono::seconds{60});
    BOOST_REQUIRE(a != nullptr);
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));   // handler 在途
    BOOST_CHECK(app.shutdown_state() == fw::ShutdownState::Running);

    app.request_shutdown();
    BOOST_REQUIRE(WaitShutdownState(app, fw::ShutdownState::ShutdownIncomplete));
    BOOST_CHECK(!rh->done.load());                    // run 未返回
    BOOST_CHECK(!owner->IsClosed());                  // owner 未被 Close 调用（时序，非所有权）
    BOOST_CHECK(g_probe->Active("t5-a"));             // handler 仍物理在途
    BOOST_CHECK(!app.pending_cleanup().empty());      // 有未完成清理项

    // release 原 handler → 迟到完成。
    g_probe->GateFor("t5-a")->Open();
    BOOST_REQUIRE(g_probe->handler_done.WaitFor(kWait));

    join_guard.JoinNow();
    BOOST_CHECK(rh->rc == fw::HostLifecycle::kExitShutdownLate);
    BOOST_CHECK(app.shutdown_state() == fw::ShutdownState::Closed);
    BOOST_CHECK(app.pending_cleanup().empty());
    BOOST_CHECK(owner->IsClosed());

    const int hi = g_seq->IndexOf("handler_done:t5-a");
    const int ci = g_seq->IndexOf("resource_close");
    BOOST_REQUIRE(hi >= 0);
    BOOST_REQUIRE(ci >= 0);
    BOOST_CHECK(hi < ci);                             // Close 在 handler 完成之后
    BOOST_TEST_MESSAGE("auth_state=" << kAuthState);
}

BOOST_AUTO_TEST_SUITE_END()
