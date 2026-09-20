// service-actor/v2 F1-b2 切片验证：真实 infra HTTP loopback 闭环。
//
// 覆盖（全部真实断言、无 mock/stub、无 sleep 凑时序）：
//  T1 inbound_context_fields_land     入站字段落地：request_id/deadline/
//                                    cancel/peer_principal/trace_id 取自
//                                    IncomingCallContext+RpcEnvelope；
//                                    route.* 之外 metadata 与未授权 fw.*
//                                    被拒；custom 不能伪造系统字段。
//  T2 concurrent_context_isolated     8 并发请求各自 RequestScope 随协程
//      _cross_worker                  绑定互不串扰；占用对端 worker 的
//                                    spinner 协程制造确定性跨 worker 恢复。
//  T3 unmanaged_coroutine_invalid     非受管协程经已托管服务实例调用
//      _context_end_to_end            this->call → 端到端真实 InvalidContext；
//                                    受管入站路径作正路径对照（真实回路）。
//  T4 outbound_deadline_min_and       出站期限缺省继承父预算、显式期限取
//      _expired_before_io             min；父预算过期 → 发起 I/O 前 TimedOut。
//  T5 waitclosed_bridging             infra WaitClosed 只许协程内：控制线程
//                                    直接调用 InvalidContext；协程内调用收
//                                    Closed；INetworkHost 桥接后控制线程
//                                    同步收 Closed。
//  T6 handler_drain_shutdown_late     真实 in-flight handler 未排空 →
//                                    WaitHandlersDone 预算耗尽 →
//                                    ShutdownIncomplete → 放行后完整收束
//                                    → kExitShutdownLate。
//  T7 actor_serial_remote_wait        同一 actor 在远端等待期间不重入
//      _no_reenter                    （mailbox 持执行资格），不同 actor
//                                    并发推进；邮箱容量上限 → Overloaded。
//
// 线桥用框架默认 RpcHttpBridge（internal/）：x-bbt-* header ↔ envelope，
// 业务负载使用 CoRpcReq/CoRpcResp 位置参数 codec。静态路由的 endpoint 值
// 对 loopback 用 "self" 逻辑标记，测试缝发送闭包替换为宿主真实绑定
// 地址——路由门（服务名白名单）仍完整生效。
//
// 与上游单测一致：Boost.Test 经 included/unit_test.hpp 静态内嵌。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/coroutine/sync/CompletionSignal.hpp>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>

// 机器面（宿主/线桥/生命周期/测试缝构造）显式走 internal 头。
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/HostLifecycle.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>
#include <bbt/framework/internal/RpcHttpBridge.hpp>

namespace fw  = bbt::framework;
namespace co  = bbt::coroutine;
namespace inf = bbt::infra;
using Clock = std::chrono::steady_clock;

namespace {

// ── 测试屏障原语（与 F1-b1 同形态：std CV，无 sleep 凑时序）──

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

constexpr std::chrono::milliseconds kWait{15000};

// ── CoRpc 位置参数测试辅助 ──

fw::result<std::int32_t> IntArg(const fw::CoRpcReq& req) {
    auto args = req.Parse<std::int32_t>();
    if (!args) return fw::result<std::int32_t>::err(args.error());
    return fw::result<std::int32_t>::ok(args.value());
}

fw::result<std::tuple<std::string, std::int32_t>> AcctArgs(
    const fw::CoRpcReq& req) {
    return req.Parse<std::string, std::int32_t>();
}

fw::CoRpcReq RequestInt(std::int32_t value) {
    auto req = fw::CoRpcReq::From(value);
    if (!req) throw std::logic_error("request encode failed");
    return std::move(req).value();
}

fw::CoRpcReq RequestAcct(std::string account, std::int32_t value) {
    auto req = fw::CoRpcReq::From(
        std::tuple{std::move(account), value});
    if (!req) throw std::logic_error("request encode failed");
    return std::move(req).value();
}

fw::CoRpcResp ReplyInt(const fw::result<std::int32_t>& value) {
    if (!value) return fw::CoRpcResp::Error(value.error());
    return fw::CoRpcResp::From(value.value());
}

// 线桥别名：框架默认 bridge（internal/RpcHttpBridge.hpp），测试侧不再
// 手撸 header 映射。
namespace Wire = fw::http_bridge;

// ── 观测态：业务实例默认构造，经全局指针挂接当前用例的探针 ──

struct CtxSnapshot {
    std::string     request_id;
    std::string     peer_principal;
    std::string     actor_key;    // 无 → ""
    std::string     trace_id;     // 无 → ""
    co::Deadline    deadline{};
    bool            cancel_req = false;
    std::uint64_t   co_id = 0;
    std::thread::id tid{};
};

struct ProbeState {
    explicit ProbeState(int arrive_n) : arrived(arrive_n) {}

    TestLatch arrived;

    mutable std::mutex      mtx;
    std::condition_variable ev_cv;
    std::map<std::string, std::shared_ptr<co::CompletionSignal>> gates;
    std::map<std::string, CtxSnapshot> before;
    std::map<std::string, CtxSnapshot> after;
    std::vector<std::string>         events;
    co::Deadline                     last_egress_deadline{};
    std::atomic<int>                 egress_calls{0};
    std::atomic<int>                 expired_sends_before{-1};

    std::shared_ptr<co::CompletionSignal> GateFor(const std::string& rid) {
        std::lock_guard<std::mutex> lk(mtx);
        auto& g = gates[rid];
        if (!g)
            g = std::make_shared<co::CompletionSignal>();
        return g;
    }
    void CompleteAll() {
        std::lock_guard<std::mutex> lk(mtx);
        for (auto& [rid, g] : gates)
            g->Complete();
    }
    void Event(std::string e) {
        {
            std::lock_guard<std::mutex> lk(mtx);
            events.push_back(std::move(e));
        }
        ev_cv.notify_all();
    }
    bool WaitEvent(const std::string& e, std::chrono::milliseconds ms) {
        std::unique_lock<std::mutex> lk(mtx);
        return ev_cv.wait_for(lk, ms, [&] {
            return std::find(events.begin(), events.end(), e)
                   != events.end();
        });
    }
    std::size_t EventIndex(const std::string& e) const {
        std::lock_guard<std::mutex> lk(mtx);
        for (std::size_t i = 0; i < events.size(); ++i)
            if (events[i] == e) return i;
        return events.size();
    }
    void Snapshot(std::map<std::string, CtxSnapshot>& dst,
                  const std::string& rid, const CtxSnapshot& s) {
        std::lock_guard<std::mutex> lk(mtx);
        dst[rid] = s;
    }
    CtxSnapshot At(const std::map<std::string, CtxSnapshot>& src,
                   const std::string& rid) const {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = src.find(rid);
        return it != src.end() ? it->second : CtxSnapshot{};
    }
    void SetEgressDeadline(co::Deadline d) {
        std::lock_guard<std::mutex> lk(mtx);
        last_egress_deadline = d;
    }
    co::Deadline EgressDeadline() const {
        std::lock_guard<std::mutex> lk(mtx);
        return last_egress_deadline;
    }
};

std::shared_ptr<ProbeState> g_probe;

CtxSnapshot SnapCtx(const fw::RequestContext& c) {
    CtxSnapshot s;
    s.request_id     = c.request_id;
    s.peer_principal = c.peer_principal;
    s.actor_key      = c.actor_key.value_or("");
    s.trace_id       = c.trace_id.value_or("");
    s.deadline       = c.deadline;
    s.cancel_req     = c.cancel.IsCancellationRequested();
    s.co_id          = co::GetLocalCoroutineId();
    s.tid            = std::this_thread::get_id();
    return s;
}

fw::Error WaitErr(co::WaitStatus st, const char* what) {
    switch (st) {
    case co::WaitStatus::TimedOut:
        return fw::MakeError(fw::ErrorCode::TimedOut, what);
    case co::WaitStatus::Cancelled:
        return fw::MakeError(fw::ErrorCode::Cancelled, what);
    default:
        return fw::MakeError(fw::ErrorCode::InternalError, what);
    }
}

// ── 测试服务 ──

// "probe"：Concurrent；inspect 立即回显，wait_echo 在 per-rid 信号上
// 真实挂起（用 ctx 的 deadline/cancel 作等待参数），恢复后再次快照上下文。
class ProbeSvc final : public fw::CoService<ProbeSvc> {
public:
    static constexpr std::string_view kServiceName = "probe";
    fw::CoRpcResp Inspect(fw::CoRpcReq req) {
        auto value = IntArg(req);
        if (!value) return fw::CoRpcResp::Error(value.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        auto p = g_probe;
        if (!p)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::InternalError, "no probe"));
        p->Snapshot(p->before, ctx.value()->request_id,
                    SnapCtx(*ctx.value()));
        return fw::CoRpcResp::From(value.value());
    }
    fw::CoRpcResp WaitEcho(fw::CoRpcReq req) {
        auto value = IntArg(req);
        if (!value) return fw::CoRpcResp::Error(value.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        auto p = g_probe;
        if (!p)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::InternalError, "no probe"));
        const std::string rid = ctx.value()->request_id;
        p->Snapshot(p->before, rid, SnapCtx(*ctx.value()));
        auto gate = p->GateFor(rid);
        p->arrived.CountDown();
        const auto st = gate->Wait(
            {ctx.value()->deadline, ctx.value()->cancel});
        if (st != co::WaitStatus::Completed)
            return fw::CoRpcResp::Error(WaitErr(st, "wait_echo gate"));
        p->Snapshot(p->after, rid, SnapCtx(*ctx.value()));
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&ProbeSvc::Inspect>("inspect"),
        fw::Method<&ProbeSvc::WaitEcho>("wait_echo"));
};

// "echo"：Concurrent 远端被调方；slow 在全局 slow 门上挂起。
class EchoSvc final : public fw::CoService<EchoSvc> {
public:
    static constexpr std::string_view kServiceName = "echo";
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        return ReplyInt(IntArg(req));
    }
    fw::CoRpcResp Slow(fw::CoRpcReq req) {
        auto value = IntArg(req);
        if (!value) return fw::CoRpcResp::Error(value.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        auto p = g_probe;
        const auto st = p->GateFor("slow")->Wait(
            {ctx.value()->deadline, ctx.value()->cancel});
        if (st != co::WaitStatus::Completed)
            return fw::CoRpcResp::Error(WaitErr(st, "echo slow"));
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EchoSvc::Ping>("ping"),
        fw::Method<&EchoSvc::Slow>("slow"));
};

// "caller"：Concurrent；在受管 handler 内发起到 echo 的真实远端调用。
class CallerSvc final : public fw::CoService<CallerSvc> {
public:
    static constexpr std::string_view kServiceName = "caller";
    fw::CoRpcResp Dial(fw::CoRpcReq req) {
        auto response = this->call("echo", "ping", req);
        if (!response) return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    fw::CoRpcResp SpawnDial(fw::CoRpcReq req) {
        auto ctx = fw::CurrentRequestContext();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        auto p = g_probe;
        const auto parent_co = co::GetLocalCoroutineId();
        struct Slot {
            fw::ErrorCode code = fw::ErrorCode::InternalError;
            std::uint64_t co_id = 0;
        };
        auto slot = std::make_shared<Slot>();
        auto sig  = std::make_shared<co::CompletionSignal>();
        bbtco [this, req, slot, sig]() {
            slot->co_id = co::GetLocalCoroutineId();
            auto response = this->call("echo", "ping", req);
            if (response)
                slot->code = fw::ErrorCode::InternalError;
            else
                slot->code = response.error().code;
            sig->Complete();
        };
        const auto st = sig->Wait(
            {ctx.value()->deadline, ctx.value()->cancel});
        if (st != co::WaitStatus::Completed)
            return fw::CoRpcResp::Error(WaitErr(st, "spawn_dial wait"));
        p->Event("parent_co:" + std::to_string(parent_co));
        p->Event("child_co:" + std::to_string(slot->co_id));
        return fw::CoRpcResp::From(
            static_cast<std::int32_t>(slot->code));
    }
    fw::result<fw::CoRpcResp> DirectDial(fw::CoRpcReq req) {
        return this->call("echo", "ping", req);
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&CallerSvc::Dial>("dial"),
        fw::Method<&CallerSvc::SpawnDial>("spawn_dial"));
};

// "relay"：Concurrent；三种出站期限形态，真实经 loopback 到 echo。
class RelaySvc final : public fw::CoService<RelaySvc> {
public:
    static constexpr std::string_view kServiceName = "relay";
    fw::CoRpcResp CallDefault(fw::CoRpcReq req) {
        auto ctx = fw::CurrentRequestContext();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        g_probe->Snapshot(g_probe->before, ctx.value()->request_id,
                          SnapCtx(*ctx.value()));
        auto response = this->call("echo", "ping", req);
        if (!response) return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    fw::CoRpcResp CallLonger(fw::CoRpcReq req) {
        auto ctx = fw::CurrentRequestContext();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        g_probe->Snapshot(g_probe->before, ctx.value()->request_id,
                          SnapCtx(*ctx.value()));
        fw::CallOptions opt;
        opt.deadline = Clock::now() + std::chrono::minutes{10};
        auto response = this->call("echo", "ping", req, {}, opt);
        if (!response) return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    fw::CoRpcResp CallExpired(fw::CoRpcReq req) {
        auto ctx = fw::CurrentRequestContext();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        auto p = g_probe;
        auto sig = std::make_shared<co::CompletionSignal>();
        (void)sig->Wait({ctx.value()->deadline, ctx.value()->cancel});
        p->expired_sends_before.store(
            p->egress_calls.load(std::memory_order_acquire),
            std::memory_order_release);
        auto response = this->call("echo", "ping", req);
        if (!response) return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&RelaySvc::CallDefault>("call_default"),
        fw::Method<&RelaySvc::CallLonger>("call_longer"),
        fw::Method<&RelaySvc::CallExpired>("call_expired"));
};

// "acct"：ActorSerial；actor key 取自第一个位置参数
// （客户端填 "acct-<v>"）。Xfer 在远端（echo.slow）等待，XferFast 走
// 即时远端；事件日志证明同 key 不重入、异 key 并发。
class AcctSvc final : public fw::CoService<AcctSvc> {
public:
    static constexpr std::string_view kServiceName = "acct";
    fw::CoRpcResp Xfer(fw::CoRpcReq req) {
        auto args = AcctArgs(req);
        if (!args) return fw::CoRpcResp::Error(args.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        auto p = g_probe;
        const std::string rid = ctx.value()->request_id;
        p->Snapshot(p->before, rid, SnapCtx(*ctx.value()));
        p->Event("start:" + rid);
        auto response = this->call(
            "echo", "slow", RequestInt(std::get<1>(args.value())));
        p->Event("end:" + rid);
        if (!response) return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    fw::CoRpcResp XferFast(fw::CoRpcReq req) {
        auto args = AcctArgs(req);
        if (!args) return fw::CoRpcResp::Error(args.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        auto p = g_probe;
        const std::string rid = ctx.value()->request_id;
        p->Snapshot(p->before, rid, SnapCtx(*ctx.value()));
        p->Event("start:" + rid);
        auto response = this->call(
            "echo", "ping", RequestInt(std::get<1>(args.value())));
        p->Event("end:" + rid);
        if (!response) return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::ActorMethodAt<&AcctSvc::Xfer, std::string>("xfer"),
        fw::ActorMethodAt<&AcctSvc::XferFast, std::string>("xfer_fast"));
};

// ── 装配选项与宿主夹具 ──

fw::ServiceOptions ConcurrentOpts() {
    return fw::ServiceOptions{
        fw::ExecutionPolicy::Concurrent,
        /*max_inflight*/ 64,
        /*mailbox_capacity*/ 0,
        /*max_actors*/ 0,
        /*ordered_ingress*/ false,
        /*max_ordered_streams*/ 0,
        /*max_cached_results*/ 0,
        /*max_cached_result_bytes*/ 0};
}

fw::ServiceOptions ActorOpts(std::size_t mailbox_capacity = 16) {
    return fw::ServiceOptions{
        fw::ExecutionPolicy::ActorSerial,
        /*max_inflight*/ 64,
        mailbox_capacity,
        /*max_actors*/ 8,
        /*ordered_ingress*/ false,
        /*max_ordered_streams*/ 0,
        /*max_cached_results*/ 0,
        /*max_cached_result_bytes*/ 0};
}

inf::NetworkLimits Limits(std::chrono::milliseconds incoming_timeout) {
    return inf::NetworkLimits{
        /*max_connections*/ 64,
        /*max_inflight*/ 64,
        /*max_header_bytes*/ 16384,
        /*max_body_bytes*/ 65536,
        incoming_timeout};
}

struct RunHandle {
    // 析构兜底：REQUIRE 早退时线程仍 joinable 会 terminate；
    // 正常路径由 AppBox::Stop 先 request_shutdown 再 join。
    ~RunHandle() { if (th.joinable()) th.detach(); }
    std::thread       th;
    std::atomic<bool> done{false};
    int               rc{-1};
};

std::unique_ptr<RunHandle> RunApp(fw::CoApp& app) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    // F1-b2 的受管 handler 会在协程内做嵌套出站调用（入站派发 →
    // this->call → 出站适配 → 发送闭包 → client->Request → 信号等待），
    // 链上各帧携带 Error/result/envelope 等值类型；12KB 默认栈在首个
    // 真实嵌套调用即触底（guard 页 SIGSEGV）。按真实工作负载放大栈。
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
    auto h = std::make_unique<RunHandle>();
    RunHandle* p = h.get();
    h->th = std::thread([p, &app] {
        p->rc = app.run();
        p->done.store(true, std::memory_order_release);
    });
    return h;
}

void JoinRun(const std::unique_ptr<RunHandle>& h) {
    if (h->th.joinable()) h->th.join();
}

// 占用某个 worker 的 spinner 协程：协程内不让出（协作式无抢占），
// 其所在 worker 因此无法拉取被唤醒的协程——跨 worker 证据的确定性来源。
struct Spinner {
    std::atomic<bool>            release{false};
    std::atomic<bool>            spinning{false};
    std::atomic<std::thread::id> tid{};
};

std::shared_ptr<Spinner> LaunchSpinner() {
    auto s = std::make_shared<Spinner>();
    bbtco [s]() {
        s->tid.store(std::this_thread::get_id(),
                     std::memory_order_release);
        s->spinning.store(true, std::memory_order_release);
        while (!s->release.load(std::memory_order_acquire))
            std::this_thread::yield();
    };
    return s;
}

// 测试期持有的 spinner；AppBox::Stop 先放行再关停，避免挂死。
std::vector<std::shared_ptr<Spinner>> g_spinners;

void WaitClosedOnThread(const std::shared_ptr<inf::NetworkRuntime>& rt,
                        std::chrono::milliseconds ms);

// 测试夹具：真实 InfraHttpHost + CoApp + 测试侧 client runtime。
// 成员声明顺序即析构逆序：run 先于 app/host 收尾。
struct AppBox {
    std::shared_ptr<fw::InfraHttpHost>          host;
    // 出站闭包使用的 HttpClient 插槽：Start 后由 app 自身 runtime 建立。
    std::shared_ptr<std::shared_ptr<inf::HttpClient>> egress_slot;
    std::shared_ptr<inf::NetworkRuntime>        client_rt;
    std::shared_ptr<inf::HttpClient>            client;
    std::string                                 endpoint;
    std::unique_ptr<fw::CoApp>                  app;
    std::unique_ptr<RunHandle>                  run;

    AppBox()                         = default;
    ~AppBox() { Stop(); }
    AppBox(AppBox&&)                 = default;
    AppBox& operator=(AppBox&&)      = default;
    AppBox(const AppBox&)            = delete;
    AppBox& operator=(const AppBox&) = delete;

    // 幂等收尾：放行测试门/spinner → 请求关闭 → join run 线程。
    // REQUIRE 早退路径同样走这里，保证不 terminate、不留运行中调度器。
    void Stop() {
        for (auto& s : g_spinners)
            s->release.store(true, std::memory_order_release);
        if (g_probe) g_probe->CompleteAll();
        if (client)    client->RequestClose();
        if (client_rt) client_rt->RequestClose();
        if (app)       app->request_shutdown();
        if (run && run->th.joinable()) run->th.join();
        if (client_rt) WaitClosedOnThread(client_rt, kWait);
    }
};

// incoming_timeout 决定入站 ctx 的预算（accept 时刻 + timeout）；
// routed 列出允许出站的服务名（路由门白名单）；register_fn 注册服务。
AppBox StartApp(std::chrono::milliseconds incoming_timeout,
                std::chrono::milliseconds step_budget,
                const std::vector<std::string>& routed,
                const std::function<void(fw::CoApp&)>& register_fn) {
    AppBox box;
    const auto limits = Limits(incoming_timeout);
    box.host = std::make_shared<fw::InfraHttpHost>(
        limits, inf::ListenAddress{"127.0.0.1", 0});
    box.egress_slot =
        std::make_shared<std::shared_ptr<inf::HttpClient>>();

    fw::CoAppOptions opts;
    opts.network_limits = limits;
    opts.listen         = inf::ListenAddress{"127.0.0.1", 0};
    for (const auto& s : routed)
        opts.static_routes.push_back(
            fw::StaticRoute{s, inf::RpcAddress{"http", "self"}});
    opts.shutdown_step_budget = step_budget;
    auto host = box.host;
    auto slot = box.egress_slot;
    // 测试缝出站：路由地址是逻辑标记 "self"，发送闭包在发送时刻
    // 解析为宿主真实绑定地址——路由门（服务名白名单）仍完整生效。
    fw::CoAppSeam seam;
    seam.host     = box.host;
    seam.rpc_send =
        [host, slot](const inf::RpcAddress& /*addr*/,
                     const inf::RpcEnvelope& env,
                     const inf::CallOptions& o)
            -> fw::result<inf::RpcEnvelope> {
        if (g_probe) {
            g_probe->egress_calls.fetch_add(1, std::memory_order_acq_rel);
            g_probe->SetEgressDeadline(o.deadline);
        }
        auto client = *slot;
        if (!client)
            return fw::result<inf::RpcEnvelope>::err(fw::MakeError(
                fw::ErrorCode::RuntimeUnavailable,
                "egress client not installed"));
        auto res = client->Request(
            Wire::ToHttpRequest(
                env, "http://" + host->bound_endpoint() + "/rpc"),
            o);
        if (!res)
            return fw::result<inf::RpcEnvelope>::err(
                std::move(res.error()));
        return Wire::EnvelopeFromResponse(res.value());
    };

    box.app = fw::MakeCoAppForTest(opts, std::move(seam));
    register_fn(*box.app);

    // 默认入站线桥（internal）：envelope↔HTTP 映射与业务路径同源。
    fw::InstallRpcHttpBridge(*box.host, *box.app);

    box.run = RunApp(*box.app);
    // 就绪判据：HTTP server 完成 bind 后 bound_endpoint 非空（此时
    // 生命周期已过网络 Create/Start，处于服务中）。
    const auto until = Clock::now() + kWait;
    while (box.endpoint.empty()) {
        BOOST_REQUIRE_MESSAGE(!box.run->done.load(),
            "app.run exited before listen, rc=" << box.run->rc);
        BOOST_REQUIRE(Clock::now() < until);
        box.endpoint = box.host->bound_endpoint();
        std::this_thread::yield();
    }
    BOOST_REQUIRE(box.app->shutdown_state() ==
                  fw::ShutdownState::Running);

    auto egress = box.host->network_runtime()->CreateHttpClient();
    BOOST_REQUIRE(egress);
    *box.egress_slot = egress.value();

    auto crt = inf::NetworkRuntime::Create(limits);
    BOOST_REQUIRE(crt);
    box.client_rt = crt.value();
    BOOST_REQUIRE(box.client_rt->Start());
    auto cl = box.client_rt->CreateHttpClient();
    BOOST_REQUIRE(cl);
    box.client = cl.value();
    return box;
}

void WaitClosedOnThread(const std::shared_ptr<inf::NetworkRuntime>& rt,
                        std::chrono::milliseconds ms) {
    const auto until = Clock::now() + ms;
    while (!rt->IsClosed()) {
        if (Clock::now() > until) return;
        std::this_thread::yield();
    }
}

void StopApp(AppBox& box) { box.Stop(); }

// 客户端发送一条 envelope 并等回包；在协程内调用。
fw::result<inf::RpcEnvelope> SendRpc(
    const std::shared_ptr<inf::HttpClient>& client,
    const std::string& endpoint,
    const inf::RpcEnvelope& env,
    std::chrono::milliseconds budget) {
    inf::CallOptions o;
    o.deadline = Clock::now() + budget;
    o.cancel   = {};
    auto res = client->Request(
        Wire::ToHttpRequest(env, "http://" + endpoint + "/rpc"), o);
    if (!res)
        return fw::result<inf::RpcEnvelope>::err(std::move(res.error()));
    return Wire::EnvelopeFromResponse(res.value());
}

// 控制线程发起一次 RPC 并限时等待结果（失败返回 err(InternalError)）。
fw::result<inf::RpcEnvelope> CallRpc(
    const AppBox& box, const inf::RpcEnvelope& env,
    std::chrono::milliseconds budget = kWait) {
    std::optional<fw::result<inf::RpcEnvelope>> out;
    TestLatch done{1};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            out.emplace(SendRpc(box.client, box.endpoint, env, budget));
            done.CountDown();
        },
        succ);
    if (!succ || !done.WaitFor(budget))
        return fw::result<inf::RpcEnvelope>::err(fw::MakeError(
            fw::ErrorCode::InternalError, "CallRpc did not complete"));
    return std::move(*out);
}

inf::RpcEnvelope MakeEnv(std::string service, std::string method,
                         std::string rid, std::int32_t v) {
    inf::RpcEnvelope env;
    env.service         = std::move(service);
    env.method          = std::move(method);
    env.request_id      = std::move(rid);
    env.request_schema  = std::string(fw::kCoRpcPositionalSchema);
    env.response_schema = std::string(fw::kCoRpcPositionalSchema);
    env.payload         = RequestInt(v).payload();
    return env;
}

// ActorSerial 服务的入站：第一个位置参数为 actor key，第二个为 v。
inf::RpcEnvelope MakeAcctEnv(std::string method, std::string rid,
                             std::int32_t v) {
    inf::RpcEnvelope env;
    env.service         = "acct";
    env.method          = std::move(method);
    env.request_id      = std::move(rid);
    env.request_schema  = std::string(fw::kCoRpcPositionalSchema);
    env.response_schema = std::string(fw::kCoRpcPositionalSchema);
    env.payload         = RequestAcct("acct-" + std::to_string(v), v).payload();
    return env;
}

fw::result<std::int32_t> DecodeReply(
    const fw::result<inf::RpcEnvelope>& r) {
    if (!r) return fw::result<std::int32_t>::err(r.error());
    auto d = fw::CoRpcReq(r.value().payload).Parse<std::int32_t>();
    if (!d) return fw::result<std::int32_t>::err(d.error());
    return fw::result<std::int32_t>::ok(d.value());
}

BOOST_AUTO_TEST_SUITE(framework_f1b2)

// T1：入站字段落地与 metadata 边界。
BOOST_AUTO_TEST_CASE(inbound_context_fields_land) {
    g_probe = std::make_shared<ProbeState>(0);
    auto box = StartApp(std::chrono::milliseconds{30000},
                        std::chrono::milliseconds{2000},
                        /*routed*/{},
                        [](fw::CoApp& app) {
        BOOST_REQUIRE(app.add_service<ProbeSvc>(ConcurrentOpts()));
    });

    // 正路径：合法 route.* + 白名单 fw.trace_id；伪造 route.request_id
    // 不得进入上下文字段。
    const auto send_at = Clock::now();
    auto env = MakeEnv("probe", "inspect", "t1-a", 7);
    env.metadata.emplace_back("route.shard", "s7");
    env.metadata.emplace_back("route.request_id", "forged");
    env.metadata.emplace_back("fw.trace_id", "trace-9");
    auto r = CallRpc(box, env);
    BOOST_REQUIRE(r);
    BOOST_CHECK(r.value().request_id == "t1-a");
    auto rep = DecodeReply(r);
    BOOST_REQUIRE(rep);
    BOOST_CHECK(rep.value() == 7);

    const auto snap = g_probe->At(g_probe->before, "t1-a");
    BOOST_CHECK(snap.request_id == "t1-a");          // 非 forged
    BOOST_CHECK(snap.trace_id == "trace-9");
    BOOST_CHECK(snap.peer_principal == "");          // loopback 未认证
    BOOST_CHECK(!snap.cancel_req);
    BOOST_CHECK(snap.co_id != 0);
    // deadline = accept + incoming_timeout：落在发送时刻后的 30s 窗口内。
    BOOST_CHECK(snap.deadline > send_at);
    BOOST_CHECK(snap.deadline <= send_at +
                std::chrono::milliseconds{30000} + kWait);

    // route.* 之外的 metadata 键 → InvalidArgument。
    auto bad1 = MakeEnv("probe", "inspect", "t1-b", 1);
    bad1.metadata.emplace_back("evil", "x");
    auto r1 = CallRpc(box, bad1);
    BOOST_REQUIRE(!r1);
    BOOST_CHECK(r1.error().code == fw::ErrorCode::InvalidArgument);

    // 非白名单 fw.* → InvalidArgument（用户不能伪造系统字段）。
    auto bad2 = MakeEnv("probe", "inspect", "t1-c", 1);
    bad2.metadata.emplace_back("fw.unknown_key", "x");
    auto r2 = CallRpc(box, bad2);
    BOOST_REQUIRE(!r2);
    BOOST_CHECK(r2.error().code == fw::ErrorCode::InvalidArgument);

    // 未注册服务/未知方法/schema 不符 → 显式错误而非 handler 乱败。
    auto r3 = CallRpc(box, MakeEnv("no.such", "inspect", "t1-d", 1));
    BOOST_REQUIRE(!r3);
    BOOST_CHECK(r3.error().code == fw::ErrorCode::NotFound);
    auto r4 = CallRpc(box, MakeEnv("probe", "no.such", "t1-e", 1));
    BOOST_REQUIRE(!r4);
    BOOST_CHECK(r4.error().code == fw::ErrorCode::NotFound);
    auto bad_schema = MakeEnv("probe", "inspect", "t1-f", 1);
    bad_schema.request_schema = "test.Other/v9";
    auto r5 = CallRpc(box, bad_schema);
    BOOST_REQUIRE(!r5);
    BOOST_CHECK(r5.error().code == fw::ErrorCode::TypeMismatch);

    StopApp(box);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
}

// T2：并发请求上下文互不串扰 + 确定性跨 worker 恢复。
BOOST_AUTO_TEST_CASE(concurrent_context_isolated_cross_worker) {
    constexpr int kN = 8;
    g_probe = std::make_shared<ProbeState>(kN);
    auto box = StartApp(std::chrono::milliseconds{30000},
                        std::chrono::milliseconds{2000},
                        /*routed*/{},
                        [](fw::CoApp& app) {
        BOOST_REQUIRE(app.add_service<ProbeSvc>(ConcurrentOpts()));
    });

    // N 个客户端协程并发发 wait_echo；各自挂起在 per-rid 门上。
    TestLatch done{kN};
    std::vector<std::optional<fw::result<inf::RpcEnvelope>>> results(kN);
    for (int i = 0; i < kN; ++i) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [i, &box, &results, &done] {
                results[i].emplace(SendRpc(
                    box.client, box.endpoint,
                    MakeEnv("probe", "wait_echo",
                            "t2-" + std::to_string(i), i),
                    kWait));
                done.CountDown();
            },
            succ);
        BOOST_REQUIRE(succ);
    }
    // 全部 handler 已挂起（各记了 before 快照）。
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));

    // 选第一个落盘的 rid 作跨 worker 目标：占用它挂起前所在 worker。
    const std::string target = g_probe->before.begin()->first;
    const auto target_tid = g_probe->before.at(target).tid;
    std::shared_ptr<Spinner> held;
    for (int i = 0; i < 10 && !held; ++i) {
        auto s = LaunchSpinner();
        // 有界等待其开始；被派到已占 worker 时永不启动 → 放行后重试。
        const auto until = Clock::now() + std::chrono::seconds{2};
        while (!s->spinning.load(std::memory_order_acquire)) {
            if (Clock::now() > until) break;
            std::this_thread::yield();
        }
        if (!s->spinning.load(std::memory_order_acquire)) {
            s->release.store(true, std::memory_order_release);
            continue;
        }
        if (s->tid.load(std::memory_order_acquire) == target_tid)
            held = s;
        else
            s->release.store(true, std::memory_order_release);
    }
    BOOST_REQUIRE(held);
    g_spinners.push_back(held);

    // 放行所有请求：恢复在被占 worker 上无法发生，目标请求必然
    // 由另一 worker 拉起重入——这是确定性的跨 worker 恢复。
    g_probe->CompleteAll();
    BOOST_REQUIRE(done.WaitFor(kWait));

    int switched = 0;
    for (int i = 0; i < kN; ++i) {
        const std::string rid = "t2-" + std::to_string(i);
        BOOST_REQUIRE_MESSAGE(results[i].has_value() && results[i].value(),
                              "reply failed: " << rid);
        BOOST_CHECK(results[i].value().value().request_id == rid);
        auto rep = DecodeReply(results[i].value());
        BOOST_REQUIRE(rep);
        BOOST_CHECK(rep.value() == i);

        const auto b = g_probe->At(g_probe->before, rid);
        const auto a = g_probe->At(g_probe->after, rid);
        // 上下文随逻辑协程走：同一协程 id，恢复后字段不串扰。
        BOOST_CHECK(b.request_id == rid);
        BOOST_CHECK(a.request_id == rid);
        BOOST_CHECK(b.co_id != 0);
        BOOST_CHECK(a.co_id == b.co_id);
        BOOST_CHECK(b.peer_principal == "");
        BOOST_CHECK(a.peer_principal == "");
        BOOST_CHECK(!b.cancel_req);
        BOOST_CHECK(!a.cancel_req);
        BOOST_CHECK(a.deadline == b.deadline);
        if (a.tid != b.tid) ++switched;
    }
    const auto bt = g_probe->At(g_probe->before, target);
    const auto at = g_probe->At(g_probe->after, target);
    BOOST_CHECK(at.tid != bt.tid);   // 目标请求的确定性跨 worker
    BOOST_CHECK(switched >= 1);
    BOOST_TEST_MESSAGE("cross-worker switched=" << switched << "/" << kN);

    StopApp(box);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
}

// T3：非受管协程 → InvalidContext 端到端；受管路径作正对照。
BOOST_AUTO_TEST_CASE(unmanaged_coroutine_invalid_context_end_to_end) {
    g_probe = std::make_shared<ProbeState>(0);
    auto box = StartApp(std::chrono::milliseconds{30000},
                        std::chrono::milliseconds{2000},
                        /*routed*/{"echo"},
                        [](fw::CoApp& app) {
        BOOST_REQUIRE(app.add_service<EchoSvc>(ConcurrentOpts()));
        BOOST_REQUIRE(app.add_service<CallerSvc>(ConcurrentOpts()));
    });

    // (a) 受管 handler 内 spawn 的不受管子协程：已托管服务实例 +
    //     非受管协程 → this->call 真实返回 InvalidContext（负载回传）。
    auto r = CallRpc(box, MakeEnv("caller", "spawn_dial", "t3-a", 1));
    BOOST_REQUIRE(r);
    auto rep = DecodeReply(r);
    BOOST_REQUIRE(rep);
    BOOST_CHECK(rep.value() ==
                static_cast<std::int32_t>(fw::ErrorCode::InvalidContext));
    // 确属另一个协程（不是同一协程丢失上下文）。
    std::uint64_t parent_co = 0, child_co = 0;
    {
        std::lock_guard<std::mutex> lk(g_probe->mtx);
        for (const auto& e : g_probe->events) {
            if (e.rfind("parent_co:", 0) == 0)
                parent_co = std::stoull(e.substr(10));
            if (e.rfind("child_co:", 0) == 0)
                child_co = std::stoull(e.substr(9));
        }
    }
    BOOST_CHECK(parent_co != 0);
    BOOST_CHECK(child_co != 0);
    BOOST_CHECK(child_co != parent_co);

    // (b) 测试协程（不受管）直接调用已托管实例 → InvalidContext。
    auto svc = box.app->find_service("caller");
    BOOST_REQUIRE(svc);
    auto caller = std::static_pointer_cast<CallerSvc>(svc.value());
    TestLatch l{1};
    std::optional<fw::result<fw::CoRpcResp>> direct;
    bbtco [caller, &direct, &l]() {
        direct.emplace(caller->DirectDial(RequestInt(5)));
        l.CountDown();
    };
    BOOST_REQUIRE(l.WaitFor(kWait));
    BOOST_REQUIRE(direct.has_value());
    BOOST_REQUIRE(!direct.value());
    BOOST_CHECK(direct.value().error().code ==
                fw::ErrorCode::InvalidContext);

    // (c) 受管路径正对照：handler 内 this->call 经 loopback 真实回路，
    //     envelope → dispatch → 回复 → 解码全通。
    auto ok = CallRpc(box, MakeEnv("caller", "dial", "t3-c", 42));
    BOOST_REQUIRE(ok);
    auto rep2 = DecodeReply(ok);
    BOOST_REQUIRE(rep2);
    BOOST_CHECK(rep2.value() == 42);
    BOOST_CHECK(ok.value().request_id == "t3-c");

    StopApp(box);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
}

// T4：出站期限缺省继承、显式取 min、过期在 I/O 前 TimedOut。
BOOST_AUTO_TEST_CASE(outbound_deadline_min_and_expired_before_io) {
    g_probe = std::make_shared<ProbeState>(0);
    auto box = StartApp(std::chrono::milliseconds{300},   // 紧预算
                        std::chrono::milliseconds{2000},
                        /*routed*/{"echo"},
                        [](fw::CoApp& app) {
        BOOST_REQUIRE(app.add_service<EchoSvc>(ConcurrentOpts()));
        BOOST_REQUIRE(app.add_service<RelaySvc>(ConcurrentOpts()));
    });

    // 缺省期限 → 出站获得的就是父 ctx deadline（继承）。
    auto r1 = CallRpc(box, MakeEnv("relay", "call_default", "t4-d", 1));
    BOOST_REQUIRE(r1);
    BOOST_CHECK(DecodeReply(r1).value() == 1);
    const auto snap_d = g_probe->At(g_probe->before, "t4-d");
    BOOST_CHECK(g_probe->EgressDeadline() == snap_d.deadline);

    // 显式期限（10min）远超父预算（300ms）→ min 规则取父预算。
    auto r2 = CallRpc(box, MakeEnv("relay", "call_longer", "t4-l", 2));
    BOOST_REQUIRE(r2);
    BOOST_CHECK(DecodeReply(r2).value() == 2);
    const auto snap_l = g_probe->At(g_probe->before, "t4-l");
    BOOST_CHECK(g_probe->EgressDeadline() == snap_l.deadline);

    // 父预算到期后发起出站 → 适配器在发起 I/O 之前 TimedOut，
    // 出站发送计数不增（无 I/O 发生）。
    const int sends_before = g_probe->egress_calls.load();
    auto r3 = CallRpc(box, MakeEnv("relay", "call_expired", "t4-x", 3),
                      std::chrono::milliseconds{5000});
    BOOST_REQUIRE(!r3);
    BOOST_CHECK(r3.error().code == fw::ErrorCode::TimedOut);
    BOOST_CHECK(g_probe->expired_sends_before.load() == sends_before);
    BOOST_CHECK(g_probe->egress_calls.load() == sends_before);

    StopApp(box);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
}

// T5：WaitClosed 控制线程/协程两种口径与桥接。
BOOST_AUTO_TEST_CASE(waitclosed_bridging) {
    g_probe = std::make_shared<ProbeState>(0);
    auto box = StartApp(std::chrono::milliseconds{30000},
                        std::chrono::milliseconds{2000},
                        /*routed*/{},
                        [](fw::CoApp& app) {
        BOOST_REQUIRE(app.add_service<ProbeSvc>(ConcurrentOpts()));
    });
    auto server  = box.host->http_server();
    auto runtime = box.host->network_runtime();
    BOOST_REQUIRE(server);
    BOOST_REQUIRE(runtime);

    // (a) infra 语义：控制线程直接 WaitClosed → InvalidContext。
    BOOST_CHECK(server->WaitClosed(Clock::now() + std::chrono::seconds{1},
                                   {}) ==
                inf::CloseStatus::InvalidContext);

    // (b) 协程内 WaitClosed 为真实语义：RequestClose 触发关闭后
    //     waiter 在协程内收 Closed（应用仍 Running、调度器活着）。
    TestLatch waiter_done{1};
    std::atomic<inf::CloseStatus> waiter_st{
        inf::CloseStatus::InvalidContext};
    bbtco [runtime, &waiter_done, &waiter_st]() {
        waiter_st.store(runtime->WaitClosed(
            Clock::now() + std::chrono::seconds{30}, {}),
            std::memory_order_release);
        waiter_done.CountDown();
    };
    box.host->RequestClose();
    BOOST_REQUIRE(waiter_done.WaitFor(kWait));
    BOOST_CHECK(waiter_st.load() == inf::CloseStatus::Closed);

    // (c) INetworkHost::WaitClosed 控制线程桥接：第二个宿主在同一
    //     运行时代际内 Create/Start/RequestClose，控制线程调用
    //     WaitClosed 内部经协程桥接同步收 Closed——上层不感知
    //     「只能在协程内」的 infra 约束差异。
    auto host2 = std::make_shared<fw::InfraHttpHost>(
        Limits(std::chrono::milliseconds{30000}),
        inf::ListenAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(host2->Create());
    BOOST_REQUIRE(host2->Start());
    host2->RequestClose();
    const auto bridged = host2->WaitClosed(
        Clock::now() + std::chrono::seconds{10}, {});
    BOOST_CHECK(bridged == inf::CloseStatus::Closed);
    host2->ReleaseClosed();

    StopApp(box);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
}

// T6：真实在途 handler 未排空 → ShutdownIncomplete → 迟到收尾。
BOOST_AUTO_TEST_CASE(handler_drain_shutdown_late) {
    g_probe = std::make_shared<ProbeState>(1);
    auto box = StartApp(std::chrono::milliseconds{30000},
                        std::chrono::milliseconds{300},   // 紧关闭预算
                        /*routed*/{},
                        [](fw::CoApp& app) {
        BOOST_REQUIRE(app.add_service<ProbeSvc>(ConcurrentOpts()));
    });

    // 让 handler 停在 per-rid 门上：真实 in-flight 请求。
    auto inflight = MakeEnv("probe", "wait_echo", "t6-h", 9);
    TestLatch inflight_done{1};
    std::optional<fw::result<inf::RpcEnvelope>> inflight_res;
    bbtco [&box, &inflight, &inflight_res, &inflight_done]() {
        inflight_res.emplace(
            SendRpc(box.client, box.endpoint, inflight, kWait));
        inflight_done.CountDown();
    };
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));   // handler 已挂起

    box.app->request_shutdown();
    // 等状态机进入 ShutdownIncomplete（在途 handler 未排空）。
    const auto until = Clock::now() + kWait;
    while (box.app->shutdown_state() !=
           fw::ShutdownState::ShutdownIncomplete) {
        BOOST_REQUIRE(Clock::now() < until);
        std::this_thread::yield();
    }
    const auto pending = box.app->pending_cleanup();
    BOOST_REQUIRE(pending.size() == 1);
    BOOST_CHECK(pending[0] == "WaitHandlersDone");
    BOOST_CHECK(!box.run->done.load());

    // 迟到收尾：放行 in-flight handler → 排空 → 完整收束 → 非零返回。
    g_probe->CompleteAll();
    BOOST_REQUIRE(inflight_done.WaitFor(kWait));
    BOOST_REQUIRE(inflight_res.has_value());
    BOOST_CHECK(inflight_res.value());   // handler 真实回了包
    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitShutdownLate);
    BOOST_CHECK(box.app->shutdown_state() == fw::ShutdownState::Closed);
    BOOST_CHECK(box.app->pending_cleanup().empty());

    if (box.client)    box.client->RequestClose();
    if (box.client_rt) box.client_rt->RequestClose();
}

// T7：同一 actor 远端等待期间不重入；不同 actor 并发推进；
//     邮箱容量上限 → Overloaded。
BOOST_AUTO_TEST_CASE(actor_serial_remote_wait_no_reenter) {
    g_probe = std::make_shared<ProbeState>(0);
    auto box = StartApp(std::chrono::milliseconds{30000},
                        std::chrono::milliseconds{2000},
                        /*routed*/{"echo"},
                        [](fw::CoApp& app) {
        BOOST_REQUIRE(app.add_service<EchoSvc>(ConcurrentOpts()));
        BOOST_REQUIRE(
            app.add_service<AcctSvc>(ActorOpts(/*mailbox_capacity*/1)));
    });

    TestLatch replies{4};
    std::map<std::string, fw::result<inf::RpcEnvelope>> res;
    std::mutex res_mtx;
    auto send = [&](const char* method, std::int32_t v,
                    const std::string& rid) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [&box, &res, &res_mtx, &replies, method, v, rid] {
                auto r = SendRpc(box.client, box.endpoint,
                                 MakeAcctEnv(method, rid, v), kWait);
                std::lock_guard<std::mutex> lk(res_mtx);
                res.emplace(rid, std::move(r));
                replies.CountDown();
            },
            succ);
        BOOST_REQUIRE(succ);
    };

    // A1 进入执行并在远端（echo.slow）挂起：actor acct-1 执行资格
    // 仍由其 drain 协程持有。
    send("xfer", 1, "a1");
    BOOST_REQUIRE(g_probe->WaitEvent("start:a1", kWait));

    // B1 属 acct-2：在 A1 挂起期间完整跑完——不同 actor 并发推进。
    send("xfer_fast", 2, "b1");
    BOOST_REQUIRE(g_probe->WaitEvent("end:b1", kWait));

    // A2/A3 同属 acct-1：mailbox 等待位容量 1，一者入队另一者
    // Overloaded——证明 A1 挂起期间同 key 请求被排队而非重入。
    send("xfer_fast", 1, "a2");
    send("xfer_fast", 1, "a3");

    // 等一者先出局（Overloaded 立刻回）；另一者仍排队等 A1。
    {
        const auto until = Clock::now() + kWait;
        std::string loser;
        for (;;) {
            {
                std::lock_guard<std::mutex> lk(res_mtx);
                for (const char* rid : {"a2", "a3"})
                    if (auto it = res.find(rid); it != res.end() &&
                        !it->second &&
                        it->second.error().code ==
                            fw::ErrorCode::Overloaded)
                        loser = rid;
            }
            if (!loser.empty()) break;
            BOOST_REQUIRE(Clock::now() < until);
            std::this_thread::yield();
        }
        // 此刻 A1 仍在远端等待：同 key 的重入确实未发生。
        BOOST_CHECK(g_probe->EventIndex("end:a1") ==
                    g_probe->events.size());
        g_probe->Event("loser:" + loser);
    }

    // 放行远端 → A1 完成 → 排队者按序执行（不重入：先 end:a1 后 start）。
    g_probe->GateFor("slow")->Complete();
    BOOST_REQUIRE(replies.WaitFor(kWait));

    // 四个回复全部落位：a1/b1/排队者成功，loser 为 Overloaded。
    std::string queued;
    {
        std::lock_guard<std::mutex> lk(res_mtx);
        BOOST_CHECK(res.at("a1"));
        BOOST_CHECK(DecodeReply(res.at("a1")).value() == 1);
        BOOST_CHECK(res.at("b1"));
        BOOST_CHECK(DecodeReply(res.at("b1")).value() == 2);
        int overloaded = 0;
        for (const char* rid : {"a2", "a3"}) {
            if (res.at(rid)) {
                queued = rid;
            } else {
                BOOST_CHECK(res.at(rid).error().code ==
                            fw::ErrorCode::Overloaded);
                ++overloaded;
            }
        }
        BOOST_REQUIRE(overloaded == 1);
        BOOST_REQUIRE(!queued.empty());
        BOOST_CHECK(DecodeReply(res.at(queued)).value() == 1);
    }

    // 事件序：start:a1 < end:b1 < end:a1 < start:<queued> < end:<queued>
    // ——同 actor 在远端等待期间无任何重入，不同 actor 穿插完成。
    const auto i_start_a1 = g_probe->EventIndex("start:a1");
    const auto i_end_b1   = g_probe->EventIndex("end:b1");
    const auto i_end_a1   = g_probe->EventIndex("end:a1");
    const auto i_start_q  = g_probe->EventIndex("start:" + queued);
    const auto i_end_q    = g_probe->EventIndex("end:" + queued);
    BOOST_CHECK(i_start_a1 < i_end_b1);
    BOOST_CHECK(i_end_b1   < i_end_a1);
    BOOST_CHECK(i_end_a1   < i_start_q);
    BOOST_CHECK(i_start_q  < i_end_q);

    // actor_key 按请求落进上下文。
    BOOST_CHECK(g_probe->At(g_probe->before, "a1").actor_key == "acct-1");
    BOOST_CHECK(g_probe->At(g_probe->before, "b1").actor_key == "acct-2");
    BOOST_CHECK(g_probe->At(g_probe->before, queued).actor_key == "acct-1");

    StopApp(box);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace
