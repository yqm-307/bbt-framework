/**
 * 正式会话路径上的并发隔离 / 容量 / 迟到寿命 / 关闭矩阵。
 *
 * 装配固定为两个互异身份的会话 owner：
 * * 服务端：公开 `fw::CoApp::Create`（AppOptions.local_identity 必填）经
 *   `CoInstallService`/`CoInstallComponent` 装配，真实 InfraHttpHost 监听
 *   127.0.0.1:0，业务服务经公开 ProtoMethod 注册测试专用 Hold/Fast
 *   （只读 GetValue 语义，无写副作用）；
 * * 客户端：独立 `fw::InfraHttpHost`（互异 local_identity）的 `rpc_client()`
 *   owner，经公开 `RpcEgress::Call("tcp://<ep>", Request, CallOptions)` 走
 *   真实 TCP loopback 的 SessionFrame + Hello/HelloAck（非注入 rpc_send、
 *   非直接 dispatch、非私有身份）。
 * 未认证 principal 来自真实 transport（loopback 无认证 → 空串），测试不手造
 * 入站上下文；deadline 由 infra 按 remaining_budget_ms 与 incoming_timeout
 * 的 local-min 填入 Request::Context()。
 *
 * 用例：
 * T1 r6_concurrent_context_not_crossed        并发挂起/逆序恢复；上下文不串。
 * T2 r6_client_timeout_inflight_holds_capacity 客户端先超时（OutcomeUnknown+
 * committed），handler 物理在途
 * 仍占容量 → 新请求精确 Overloaded。
 * T3 r7_capacity_overloaded_then_recovers_net  Service max_inflight 有界 + 峰值
 * oracle + 释放恢复。
 * T4 r8_normal_shutdown_after_drain            正常排空 → CoStop → Stopped（配对）。
 * T5 r8_shutdown_incomplete_late_completion    在途关闭 → StopIncomplete →
 * 迟到完成 → 再次 CoStop 推进收口，
 * 资源 Close 在 handler 完成后。
 *
 * 诚实缺口：跨 hop 预算、独立 supervisor 硬停未覆盖；
 * NetworkLimits 的 max_inflight/字节维度未分别压测。
 *
 * 与仓内既有单测一致：Boost.Test 经 included/unit_test.hpp 静态内嵌。
 */

#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <condition_variable>
#include <functional>
#include <future>
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

#include <bbt/framework/internal/InfraHttpHost.hpp>
#include <bbt/framework/internal/RpcBridge.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>

#include <bbt/example/v1/get_value.pb.h>

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/Component.hpp>
#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/Object.hpp>
#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/Result.hpp>

/**
 * ProtoCodec<GetValueRequest/Response> 特化（schema 真源 = .proto descriptor）。
 */
#include "getvalue_service.hpp"

namespace fw  = bbt::framework;
namespace co  = bbt::coroutine;
namespace inf = bbt::infra;
using Clock = std::chrono::steady_clock;

namespace {

constexpr std::chrono::milliseconds kWait{15000};
/**
 * 结构化结果标记：未认证 loopback，不表示身份已认证。
 */
constexpr const char* kAuthState = "unauthenticated_loopback";

/**
 * 旧 run() 退出码在本测试内的对应映射：
 * kExitOk = CoStart/CoWaitShutdownRequest/CoStop 全部 ok 且终态 Stopped；
 * kExitStartFailed = 装配/CoStart 失败；
 * kExitStopIncomplete = 停止路径未达 Stopped（收口未竟，含再次推进后）。
 */
constexpr int kExitOk = 0;
constexpr int kExitStartFailed = 2;
constexpr int kExitStopIncomplete = 4;

/**
 * ── 屏障原语（std CV / 协程闩；无固定 sleep 凑时序）────────────────────
 */

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

/**
 * 单次放行协程闩：Open 前 Wait 真实挂起当前 handler 协程；Open 早到不丢唤醒。
 */
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

/**
 * ── 观测态 ─────────────────────────────────────────────────────────────
 */

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

    /**
     * 到达 handler 的请求数
     */
    TestLatch arrived;
    /**
     * 单跟踪 handler 的完成（仅 T2/T5 用）
     */
    TestLatch handler_done{1};

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

/**
 * ── 资源组件（旧 add_resource 的对应物）：OnStop 内同步 Close，记录时序，
 * 不伪造计数。外部观测经 Options 注入的共享状态（受管对象本体不可直取）。──
 */
struct ResShared {
    std::atomic<int>  close_calls{0};
    std::atomic<bool> closed{false};
};

class OwnerResComponent final : public fw::CoComponent {
public:
    struct Options { std::shared_ptr<ResShared> s; };

    explicit OwnerResComponent(fw::detail::ConstructKey k, Options o)
        : fw::CoComponent(k), m_s(std::move(o.s)) {}

protected:
    ~OwnerResComponent() noexcept override = default;

    /**
     * infra 关闭是同步契约：Close() 返回即物理释放、幂等。
     */
    fw::result<void> OnStop() override {
        m_s->close_calls.fetch_add(1);
        m_s->closed.store(true, std::memory_order_release);
        if (g_seq) g_seq->Push("resource_close");
        return fw::result<void>::ok();
    }

private:
    std::shared_ptr<ResShared> m_s;
    friend class fw::detail::Registry;
};

/**
 * ── 测试服务：公开 ProtoMethod，只读 GetValue 语义 ─────────────────────
 */

class NetMatrixSvc final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "test.matrix.GetValueService";

    explicit NetMatrixSvc(fw::detail::ConstructKey k) : fw::CoService<>(k) {}

    /**
     * Hold：快照上下文 → 到 arrived → 在 per-rid 独立 gate 上以无界期限挂起
     * （不依 ctx.deadline 自动返回，保持物理在途）→ 恢复后再快照当前上下文。
     */
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
                /**
                 * 恢复后重新取受管上下文
                 */
                auto ctx2 = fw::CurrentRequestContext();
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

protected:
    ~NetMatrixSvc() noexcept override = default;
    friend class fw::detail::Registry;
};

/**
 * ── 装配 ───────────────────────────────────────────────────────────────
 */

fw::ServiceOptions ConcurrentOpts(std::size_t max_inflight) {
    return fw::ServiceOptions{}
        .SetExecution(fw::ExecutionPolicy::Concurrent)
        .SetMaxInflight(max_inflight);
}

/**
 * 正式装配：会话（local_identity 必填；入站走 admission，无 bridge 选择）。
 * lifecycle_budget 覆盖首次 CoStop 的排空期限（T5 用小预算制造未竟）。
 */
fw::AppOptions AppOpts(std::chrono::milliseconds lifecycle_budget) {
    return fw::AppOptions{}
        .SetLimits(64, 64, 64, 64)
        .SetShutdownBudget(lifecycle_budget)
        .SetLifecycleBudget(lifecycle_budget)
        .SetNetworkLimits(inf::NetworkLimits{
            /*max_connections*/ 64, /*max_inflight*/ 64,
            /*max_header_bytes*/ 16 * 1024, /*max_body_bytes*/ 64 * 1024,
            /*incoming_timeout*/ std::chrono::milliseconds{30000}})
        .Listen("127.0.0.1:0")
        .SetLocalIdentity("matrix-server");
}

struct ServerHandle {
    std::thread                              th;
    std::promise<std::shared_ptr<fw::CoApp>> app_ready;
    std::promise<bool>                       started;
    std::atomic<bool>                        done{false};
    /**
     * 首次 CoStop 的返回（T5 记录 drain 未竟的诚实结果）。
     */
    std::optional<fw::result<void>>          first_stop;
    int                                      rc{-1};
    /**
     * T5 用：首次 CoStop 后任务挂在本闩上等待测试放行再次推进；
     * 设置时任务在 StopIncomplete 与二次 CoStop 之间保持「未返回」。
     */
    std::shared_ptr<TestLatch>               after_first_stop;
};

struct RunJoinOnExit {
    std::shared_ptr<fw::CoApp>* app = nullptr;
    ServerHandle*               h   = nullptr;
    void JoinNow() { if (h && h->th.joinable()) h->th.join(); }
    /**
     * 失败/异常退出也必须安全收尾：先 RequestShutdown 再释放全部挂起 gate，
     * 避免存在在途 handler 时 join 无界阻塞（不依赖 ctest 超时兜底）。
     */
    ~RunJoinOnExit() {
        if (app && *app) (*app)->RequestShutdown();
        if (h && h->after_first_stop) h->after_first_stop->CountDown();
        if (g_probe) g_probe->CompleteAll();
        JoinNow();
    }
};

/**
 * 服务端受管任务体：Create → 组件/服务装配 → CoStart → 等关闭请求 →
 * 首次 CoStop →（T5 门控）→ 再次 CoStop 推进收口。
 * 全部生命周期入口必须在受管协程内执行。
 */
void ServerTaskBody(ServerHandle* h, fw::AppOptions opts,
                    std::size_t max_inflight,
                    std::vector<std::pair<std::string, std::shared_ptr<ResShared>>>
                        components) {
    auto set_rc = [&](int rc) {
        h->rc = rc;
        h->done.store(true, std::memory_order_release);
    };
    try {
        auto app = fw::CoApp::Create(std::move(opts), "matrix-server");
        h->app_ready.set_value(app);
        for (auto& [name, s] : components) {
            auto installed = bbt_co_await
                app->CoInstallComponent<OwnerResComponent>(
                    name, OwnerResComponent::Options{std::move(s)});
            if (!installed) { set_rc(kExitStartFailed); return; }
        }
        auto installed = bbt_co_await app->CoInstallService<NetMatrixSvc>(
            ConcurrentOpts(max_inflight));
        if (!installed) { set_rc(kExitStartFailed); return; }
        auto started = bbt_co_await app->CoStart();
        if (!started) { set_rc(kExitStartFailed); return; }
        h->started.set_value(true);

        auto waited = bbt_co_await app->CoWaitShutdownRequest();
        if (!waited) { set_rc(kExitStopIncomplete); return; }

        auto first = bbt_co_await app->CoStop();
        h->first_stop.emplace(first);
        if (h->after_first_stop) {
            /**
             * 首次收口未竟：挂起等测试放行（保持「未返回」供断言），
             * 放行后再次以更晚期限推进收口（StopIncomplete 契约）。
             */
            h->after_first_stop->WaitFor(std::chrono::minutes{2});
            auto second = bbt_co_await app->CoStop();
            (void)second;
        }
        set_rc(app->State() == fw::AppState::Stopped
                   ? kExitOk : kExitStopIncomplete);
    } catch (...) {
        set_rc(kExitStartFailed);
    }
}

std::unique_ptr<ServerHandle> RunServer(
    fw::AppOptions opts, std::size_t max_inflight,
    std::vector<std::pair<std::string, std::shared_ptr<ResShared>>> components = {},
    std::shared_ptr<TestLatch> after_first_stop = {}) {
    /**
     * 配置非线程安全：仅在调度器尚未初始化（无工作线程）时写入进程全局配置。
     * 运行时为进程寿命单例（无 Stop），故后续用例调用时调度器已初始化，
     * 跳过写入以规避运行期数据竞争。
     */
    if (!g_scheduler->IsInitialized()) {
        /**
         * Registered driver capacity: server 2 + host egress 1 + test client 1 + runnable worker 1.
         */
        g_bbt_coroutine_config->m_cfg_static_thread_num = 5;
        /**
         * handler 在协程内做 proto wire 编解码 + 分发栈，默认 12KB 栈触底；
         * 按示例口径放大到 512KB（仅覆盖本测试负载规模）。
         */
        g_bbt_coroutine_config->m_cfg_stack_size = 512 * 1024;
        g_scheduler->Start();
    }
    auto h = std::make_unique<ServerHandle>();
    h->after_first_stop = std::move(after_first_stop);
    ServerHandle* p = h.get();
    h->th = std::thread([p, opts = std::move(opts), max_inflight,
                         components = std::move(components)]() mutable {
        bool submitted = false;
        g_scheduler->RegistCoroutineTask(
            [p, opts = std::move(opts), max_inflight,
             components = std::move(components)]() mutable {
                ServerTaskBody(p, std::move(opts), max_inflight,
                               std::move(components));
            }, submitted);
        if (!submitted) {
            p->rc = kExitStartFailed;
            p->done.store(true, std::memory_order_release);
        }
        /**
         * 宿主线程等到受管任务结束（不违反 worker 阻塞禁令）。
         */
        while (!p->done.load(std::memory_order_acquire))
            std::this_thread::yield();
    });
    return h;
}

/**
 * 有界 yield 轮询观测（非固定 sleep 凑时序）。
 */
std::string WaitEndpoint(fw::CoApp& app) {
    const auto end = Clock::now() + kWait;
    while (Clock::now() < end) {
        auto ep = app.BoundEndpoint();
        if (!ep.empty()) return ep;
        std::this_thread::yield();
    }
    return {};
}

bool WaitAppState(fw::CoApp& app, fw::AppState want,
                  std::chrono::milliseconds ms = kWait) {
    const auto end = Clock::now() + ms;
    while (Clock::now() < end) {
        if (app.State() == want) return true;
        std::this_thread::yield();
    }
    return app.State() == want;
}

/**
 * ── 客户端：独立会话 owner（互异 identity，真实 TCP loopback）──────
 */

struct ClientEnv {
    std::shared_ptr<fw::InfraHttpHost> rt;
    std::shared_ptr<fw::RpcEgress> egress;

    bool Start() {
        inf::NetworkLimits lim{};
        lim.max_connections  = 64;
        lim.max_inflight     = 64;
        lim.max_header_bytes = 16 * 1024;
        lim.max_body_bytes   = 64 * 1024;
        lim.incoming_timeout = std::chrono::milliseconds{30000};
        auto r = std::make_shared<fw::InfraHttpHost>(
            lim, inf::ListenAddress{"127.0.0.1", 0}, "matrix-client");
        if (!r) return false;
        rt = r;
        if (auto s = rt->Create(); !s) return false;
        if (auto s = rt->Start(); !s) return false;
        if (!rt->rpc_client())
            return false;
        egress = std::make_shared<fw::RpcEgress>(rt);
        return true;
    }
    void Stop() { if (rt) rt->Close(); }
};
struct ClientStopOnExit {
    ClientEnv* c;
    ~ClientStopOnExit() { if (c) c->Stop(); }
};

struct ClientCallOut {
    std::optional<fw::result<inf::rpc::Response>> r;
    TestLatch                                     done{1};
};

/**
 * 一次真实 Call：预算由 CallOptions.deadline 承载（session 换算成 wire
 * remaining_budget_ms），服务端 Context().deadline 取它与 incoming_timeout
 * 的 local-min——因此各请求的 handler 期限相互独立。
 */
std::shared_ptr<ClientCallOut> CallAsync(
    const std::shared_ptr<fw::RpcEgress>& egress,
    const std::string& endpoint,
    const std::string& method,
    const std::string& rid,
    const std::string& key,
    std::chrono::milliseconds budget) {
    auto out = std::make_shared<ClientCallOut>();
    inf::rpc::Request env;
    env.SetService(std::string(NetMatrixSvc::kServiceName))
        .SetMethod(method)
        .SetRequestId(rid)
        .SetRequestSchema(
            bbt::example::v1::GetValueRequest::descriptor()->full_name())
        .SetResponseSchema(
            bbt::example::v1::GetValueResponse::descriptor()->full_name())
        .SetReply(inf::rpc::ReplyMode::Required)
        .SetTraffic(inf::rpc::TrafficClass::Service);
    /**
     * 合法元数据（framework 入站接受的 route.* 前缀）：证明 metadata 不升级
     * peer_principal。
     */
    env.SetMetadata({{"route.rid", rid}});
    bbt::example::v1::GetValueRequest q;
    q.set_key(key);
    std::string bytes;
    q.SerializeToString(&bytes);
    env.SetPayload(std::vector<std::uint8_t>(bytes.begin(), bytes.end()));

    const std::string target = "tcp://" + endpoint;
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [out, egress, target, env = std::move(env), budget]() mutable {
            inf::CallOptions opt;
            opt.deadline = Clock::now() + budget;
            out->r.emplace(bbt_co_await egress->Call(target, env, opt));
            out->done.CountDown();
        },
        succ);
    return succ ? out : nullptr;
}

bool WaitCall(const std::shared_ptr<ClientCallOut>& out,
              std::chrono::milliseconds ms = kWait) {
    return out && out->done.WaitFor(ms);
}

/**
 * 成功回复：ok + 五关联字段回显由 infra 会话校验，这里只取业务 payload。
 */
bool GetReply(const std::shared_ptr<ClientCallOut>& out,
              inf::rpc::Response& reply) {
    if (!out || !out->r.has_value()) return false;
    if (!out->r.value())
        /**
         * 业务/协议错误分支
         */
        return false;
    reply = out->r.value().value();
    return true;
}

int CodeOf(fw::ErrorCode c) { return static_cast<int>(c); }

} // namespace

BOOST_AUTO_TEST_SUITE(framework_matrix_network)

/**
 * T1（A）：3 条真实 network 请求在各 per-rid gate 挂起，逐个逆序放行；上下文
 * 随逻辑请求绑定、不串；合法 metadata 不升级未认证 principal。
 */
BOOST_AUTO_TEST_CASE(r6_concurrent_context_not_crossed) {
    g_probe = std::make_shared<ProbeState>(3);
    auto rh = RunServer(AppOpts(std::chrono::milliseconds{2000}), 8);
    auto app = rh->app_ready.get_future().get();
    BOOST_REQUIRE(rh->started.get_future().get());
    RunJoinOnExit join_guard{&app, rh.get()};
    const std::string ep = WaitEndpoint(*app);
    BOOST_REQUIRE(!ep.empty());
    ClientEnv cl;
    BOOST_REQUIRE(cl.Start());
    ClientStopOnExit cl_guard{&cl};

    const std::vector<std::string> rids{"t1-a", "t1-b", "t1-c"};
    /**
     * 三个不同预算（15/20/25s，均 < 传输硬看门 incoming_timeout=30s）：生效期限
     * 由各自 remaining_budget_ms 主导、间隔 5s，故严格递增且与调度到达顺序无关。
     */
    std::vector<std::shared_ptr<ClientCallOut>> outs;
    for (int i = 0; i < 3; ++i) {
        outs.push_back(CallAsync(
            cl.egress, ep, "Hold", rids[i], "k" + std::to_string(i),
            std::chrono::milliseconds{15000 + i * 5000}));
        BOOST_REQUIRE(outs.back() != nullptr);
    }
    if (!g_probe->arrived.WaitFor(std::chrono::milliseconds{6000})) {
        for (int i = 0; i < 3; ++i) {
            const bool d = WaitCall(outs[i], std::chrono::milliseconds{2000});
            std::string msg =
                "t1-diag rid=" + rids[i] + " call_done=" + (d ? "1" : "0");
            if (d && outs[i]->r.has_value()) {
                if (!outs[i]->r.value()) {
                    msg += " err_code=" + std::to_string(
                                              static_cast<int>(outs[i]->r.value().error().code));
                    msg += " msg=" + outs[i]->r.value().error().message;
                } else {
                    msg += " reply_payload_bytes=" + std::to_string(
                                                         outs[i]->r.value().value().Payload().size());
                }
            }
            BOOST_TEST_MESSAGE(msg);
        }
        BOOST_REQUIRE(g_probe->arrived.WaitFor(std::chrono::milliseconds{1000}));
    }

    /**
     * 逐个逆序放行 + 等待对应 client done：完成顺序确定（非连续 Open）。
     */
    for (int i = 2; i >= 0; --i) {
        g_probe->GateFor(rids[i])->Open();
        BOOST_REQUIRE(WaitCall(outs[i]));
        /**
         * 成功响应须解出正式业务 wire 信封（非仅 transport 层 200）：
         * success 与 request_id 精确匹配，且 payload 为 held:<rid> 业务值。
         */
        inf::rpc::Response reply;
        BOOST_REQUIRE(GetReply(outs[i], reply));
        BOOST_TEST(reply.RequestId() == rids[i]);
        bbt::example::v1::GetValueResponse resp;
        BOOST_REQUIRE(resp.ParseFromArray(
            reply.Payload().data(), static_cast<int>(reply.Payload().size())));
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
        /**
         * 上下文随协程绑定
         */
        BOOST_CHECK(a.co_id == b.co_id);
        /**
         * 合法 metadata 不升级 principal
         */
        BOOST_CHECK(b.peer_principal.empty());
        BOOST_CHECK(a.peer_principal.empty());
        /**
         * 跨挂起不漂
         */
        BOOST_CHECK(a.deadline == b.deadline);
        d[i] = b.deadline;
    }
    BOOST_CHECK(d[0] < d[1]);
    /**
     * 各请求预算独立、未串
     */
    BOOST_CHECK(d[1] < d[2]);
    BOOST_TEST_MESSAGE("auth_state=" << kAuthState);

    app->RequestShutdown();
    join_guard.JoinNow();
    BOOST_CHECK(rh->rc == kExitOk);
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
}

/**
 * T2（B）：客户端有限 deadline 先落 OutcomeUnknown+RequestCommitted，handler 经
 * 独立 gate 物理在途仍占容量（max_inflight=1）→ 新请求精确 Overloaded；
 * 释放后容量恢复（探测请求非原调用 retry）；组件 owner 在迟到完成前未被
 * 收口（closed 仅观测 OnStop 是否被调用，不表示所有权/寿命）。
 */
BOOST_AUTO_TEST_CASE(r6_client_timeout_inflight_holds_capacity) {
    g_probe = std::make_shared<ProbeState>(1);
    g_seq   = std::make_shared<SequenceLog>();
    auto res = std::make_shared<ResShared>();
    auto rh = RunServer(AppOpts(std::chrono::milliseconds{2000}), 1,
                        {{"owner-res", res}});
    auto app = rh->app_ready.get_future().get();
    BOOST_REQUIRE(rh->started.get_future().get());
    RunJoinOnExit join_guard{&app, rh.get()};
    const std::string ep = WaitEndpoint(*app);
    BOOST_REQUIRE(!ep.empty());
    ClientEnv cl;
    BOOST_REQUIRE(cl.Start());
    ClientStopOnExit cl_guard{&cl};

    /**
     * 取服务句柄：FindService 非拥有句柄只证明受管对象登记可见
     * （受管模型不提供 shared_ptr 强引用）。
     */
    {
        auto svc = app->FindService(NetMatrixSvc::kServiceName);
        BOOST_REQUIRE(svc);
    }
    BOOST_CHECK(res->closed.load() == false);

    /**
     * A：client deadline 1.5s、预算 30s → handler 在 gate 上物理在途（不依 ctx.deadline）。
     */
    auto a = CallAsync(cl.egress, ep, "Hold", "t2-a", "ka",
                       std::chrono::milliseconds{1500});
    BOOST_REQUIRE(a != nullptr);
    /**
     * handler 已挂起
     */
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));
    BOOST_REQUIRE(WaitCall(a));
    BOOST_REQUIRE(a->r.has_value());
    /**
     * transport 层失败
     */
    BOOST_REQUIRE(!a->r.value());
    const inf::Error& ae = a->r.value().error();
    BOOST_TEST(static_cast<int>(ae.code) == CodeOf(fw::ErrorCode::OutcomeUnknown));
    BOOST_REQUIRE(ae.request_phase.has_value());
    BOOST_TEST(static_cast<int>(*ae.request_phase) ==
               static_cast<int>(inf::RequestPhase::RequestCommitted));

    /**
     * 此刻：handler completion marker 仍无、owner 尚未被收口
     * （closed 只观测 OnStop 调用，不代表所有权/寿命）。
     */
    BOOST_CHECK(g_probe->Active("t2-a"));
    BOOST_CHECK(res->closed.load() == false);

    /**
     * 容量仍被占：新请求 Fast → 精确 Overloaded（只尝试一次）。
     */
    auto b = CallAsync(cl.egress, ep, "Fast", "t2-b", "kb",
                       std::chrono::seconds{30});
    BOOST_REQUIRE(b != nullptr);
    BOOST_REQUIRE(WaitCall(b));
    BOOST_REQUIRE(b->r.has_value());
    /**
     * 服务级容量拒绝走错误分支
     */
    BOOST_REQUIRE(!b->r.value());
    BOOST_TEST(static_cast<int>(b->r.value().error().code) ==
               CodeOf(fw::ErrorCode::Overloaded));

    /**
     * release 原 handler → 迟到完成。
     */
    g_probe->GateFor("t2-a")->Open();
    BOOST_REQUIRE(g_probe->handler_done.WaitFor(kWait));
    BOOST_CHECK(!g_probe->Active("t2-a"));

    /**
     * 容量恢复：新探测请求（非原调用 retry）成功。迟到回复已过写窗口，
     * infra 只发布本条终态、不写入共享 link，因此会话仍可复用。
     */
    auto c = CallAsync(cl.egress, ep, "Fast", "t2-c", "kc",
                       std::chrono::seconds{30});
    BOOST_REQUIRE(c != nullptr);
    BOOST_REQUIRE(WaitCall(c));
    inf::rpc::Response rc;
    BOOST_REQUIRE(GetReply(c, rc));
    BOOST_TEST_MESSAGE("auth_state=" << kAuthState);

    app->RequestShutdown();
    join_guard.JoinNow();
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    /**
     * 关闭序列最终收口组件 owner
     */
    BOOST_CHECK(res->closed.load());
}

/**
 * T3（D）：Service max_inflight=2 有界 → 第三个明确 Overloaded（不排队）；峰值恰为
 * 声明 limit；释放后恢复接纳。NetworkLimits 维度未分别压测。
 */
BOOST_AUTO_TEST_CASE(r7_capacity_overloaded_then_recovers_net) {
    g_probe = std::make_shared<ProbeState>(2);
    auto rh = RunServer(AppOpts(std::chrono::milliseconds{2000}), 2);
    auto app = rh->app_ready.get_future().get();
    BOOST_REQUIRE(rh->started.get_future().get());
    RunJoinOnExit join_guard{&app, rh.get()};
    const std::string ep = WaitEndpoint(*app);
    BOOST_REQUIRE(!ep.empty());
    ClientEnv cl;
    BOOST_REQUIRE(cl.Start());
    ClientStopOnExit cl_guard{&cl};

    std::vector<std::shared_ptr<ClientCallOut>> held;
    for (int i = 0; i < 2; ++i) {
        held.push_back(CallAsync(
            cl.egress, ep, "Hold", "t3-h" + std::to_string(i),
            "kh" + std::to_string(i), std::chrono::seconds{60}));
        BOOST_REQUIRE(held.back() != nullptr);
    }
    /**
     * 两条都挂起 → 容量满
     */
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));
    /**
     * 真实峰值 = 声明 limit
     */
    BOOST_CHECK(g_probe->Peak() == 2);

    auto over = CallAsync(cl.egress, ep, "Fast", "t3-over", "ko",
                          std::chrono::seconds{30});
    BOOST_REQUIRE(over != nullptr);
    BOOST_REQUIRE(WaitCall(over));
    BOOST_REQUIRE(over->r.has_value());
    BOOST_REQUIRE(!over->r.value());
    BOOST_TEST(static_cast<int>(over->r.value().error().code) ==
               CodeOf(fw::ErrorCode::Overloaded));
    /**
     * 拒绝路径不进 handler，峰值不越界
     */
    BOOST_CHECK(g_probe->Peak() == 2);

    g_probe->CompleteAll();
    for (std::size_t i = 0; i < held.size(); ++i) {
        auto& o = held[i];
        BOOST_REQUIRE(WaitCall(o));
        /**
         * held 回复须为正式业务成功信封（success + request_id 对应）。
         */
        inf::rpc::Response rh;
        BOOST_REQUIRE(GetReply(o, rh));
        BOOST_TEST(rh.RequestId() == std::string("t3-h") + std::to_string(i));
    }

    auto again = CallAsync(cl.egress, ep, "Fast", "t3-again", "ka",
                           std::chrono::seconds{30});
    BOOST_REQUIRE(again != nullptr);
    BOOST_REQUIRE(WaitCall(again));
    inf::rpc::Response ra;
    BOOST_REQUIRE(GetReply(again, ra));
    BOOST_TEST_MESSAGE("auth_state=" << kAuthState);

    app->RequestShutdown();
    join_guard.JoinNow();
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
}

/**
 * T4（C-配对正常）：一次正常请求后 RequestShutdown → 排空 → CoStop →
 * Stopped；无未完成清理项；组件 owner 被收口。
 */
BOOST_AUTO_TEST_CASE(r8_normal_shutdown_after_drain) {
    g_probe = std::make_shared<ProbeState>(0);
    g_seq   = std::make_shared<SequenceLog>();
    auto res = std::make_shared<ResShared>();
    auto rh = RunServer(AppOpts(std::chrono::milliseconds{2000}), 4,
                        {{"owner-res", res}});
    auto app = rh->app_ready.get_future().get();
    BOOST_REQUIRE(rh->started.get_future().get());
    RunJoinOnExit join_guard{&app, rh.get()};
    const std::string ep = WaitEndpoint(*app);
    BOOST_REQUIRE(!ep.empty());
    ClientEnv cl;
    BOOST_REQUIRE(cl.Start());
    ClientStopOnExit cl_guard{&cl};

    BOOST_CHECK(app->State() == fw::AppState::Running);
    auto ok = CallAsync(cl.egress, ep, "Fast", "t4-ok", "k",
                        std::chrono::seconds{30});
    BOOST_REQUIRE(ok != nullptr);
    BOOST_REQUIRE(WaitCall(ok));
    inf::rpc::Response rk;
    BOOST_REQUIRE(GetReply(ok, rk));

    app->RequestShutdown();
    join_guard.JoinNow();
    BOOST_CHECK(rh->rc == kExitOk);
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_CHECK(app->PendingCleanup().empty());
    BOOST_CHECK(res->closed.load());
    BOOST_TEST_MESSAGE("auth_state=" << kAuthState);
}

/**
 * T5（C-超预算配对）：一条真实在途 handler 被 gate 扣住时 RequestShutdown →
 * 首次 CoStop 排空未竟（StopIncomplete、任务未返回）；释放 → 迟到完成 →
 * 再次 CoStop 推进收口至 Stopped；资源收口发生在 handler completion 之后
 * （handler_done:t5-a < resource_close）。不宣称析构/所有权观测。
 */
BOOST_AUTO_TEST_CASE(r8_shutdown_incomplete_late_completion) {
    g_probe = std::make_shared<ProbeState>(1);
    g_seq   = std::make_shared<SequenceLog>();
    auto res = std::make_shared<ResShared>();
    /**
     * 小步预算：在途 handler 让首次 CoStop 的排空步超预算 → StopIncomplete。
     */
    auto after_first = std::make_shared<TestLatch>(1);
    auto rh = RunServer(AppOpts(std::chrono::milliseconds{300}), 4,
                        {{"owner-res", res}}, after_first);
    auto app = rh->app_ready.get_future().get();
    BOOST_REQUIRE(rh->started.get_future().get());
    RunJoinOnExit join_guard{&app, rh.get()};
    const std::string ep = WaitEndpoint(*app);
    BOOST_REQUIRE(!ep.empty());
    ClientEnv cl;
    BOOST_REQUIRE(cl.Start());
    ClientStopOnExit cl_guard{&cl};

    auto a = CallAsync(cl.egress, ep, "Hold", "t5-a", "ka",
                       std::chrono::seconds{60});
    BOOST_REQUIRE(a != nullptr);
    /**
     * handler 在途
     */
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));
    BOOST_CHECK(app->State() == fw::AppState::Running);

    app->RequestShutdown();
    BOOST_REQUIRE(WaitAppState(*app, fw::AppState::StopIncomplete));
    /**
     * 收口任务未返回（首次 CoStop 已过、二次推进待放行）
     */
    BOOST_CHECK(!rh->done.load());
    /**
     * 组件收口尚未执行（时序，非所有权）
     */
    BOOST_CHECK(!res->closed.load());
    /**
     * handler 仍物理在途
     */
    BOOST_CHECK(g_probe->Active("t5-a"));
    /**
     * 有未完成清理项（在途 handler 的执行租约未归还）
     */
    BOOST_CHECK(!app->PendingCleanup().empty());

    /**
     * release 原 handler → 迟到完成。
     */
    g_probe->GateFor("t5-a")->Open();
    BOOST_REQUIRE(g_probe->handler_done.WaitFor(kWait));
    /**
     * 放行收口任务再次推进 CoStop。
     */
    after_first->CountDown();
    join_guard.JoinNow();

    BOOST_CHECK(rh->rc == kExitOk);
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_CHECK(app->PendingCleanup().empty());
    BOOST_CHECK(res->closed.load());

    const int hi = g_seq->IndexOf("handler_done:t5-a");
    const int ci = g_seq->IndexOf("resource_close");
    BOOST_REQUIRE(hi >= 0);
    BOOST_REQUIRE(ci >= 0);
    /**
     * 组件收口在 handler 完成之后
     */
    BOOST_CHECK(hi < ci);
    BOOST_TEST_MESSAGE("auth_state=" << kAuthState);
}

BOOST_AUTO_TEST_SUITE_END()
