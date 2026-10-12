// service-actor/v2 F1-b2 切片验证：真实 infra HTTP loopback 闭环。
//
// 覆盖（全部真实断言、无 mock/stub、无 sleep 凑时序）：
//  T1 inbound_context_fields_land     入站字段落地：request_id/deadline/
//                                    peer_principal/trace_id 取自
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
//  T5 sync_close_on_control_thread    关闭是同步契约：控制线程直接 Close 即
//                                    物理释放（无协程等待桥接）；重复 Close
//                                    幂等。
//  T6 handler_drain_shutdown_late     真实 in-flight handler 未排空 →
//                                    WaitHandlersDone 预算耗尽 →
//                                    ShutdownIncomplete → 放行后完整收束
//                                    → kExitShutdownLate。
//  T7 actor_serial_remote_wait        同一 actor 在远端等待期间不重入
//      _no_reenter                    （mailbox 持执行资格），不同 actor
//                                    并发推进；邮箱容量上限 → Overloaded。
//  T8 inbound_deadline_reply          入站回包送达预留（kReplyDeliveryReserve=
//      _reserve_exact                 20ms）的精确扣减：已知绝对
//                                     incoming.deadline 经真实 Dispatcher →
//                                     方法表 → RequestScope 落到 handler 可见
//                                     ctx.deadline 恰减一次（绝对相等，不用
//                                     ±差值）；<=20ms 紧预算在进 handler 前
//                                     TimedOut 且目标 handler 计数不增。
//                                     驱动经 DispatchInboundForTest（CoApp::
//                                     dispatch_inbound 的测试转发口）——HTTP
//                                     loopback 的 incoming.deadline 由 infra
//                                     在 accept 时刻换算，测试侧取不到已知
//                                     绝对值，故本用例不复用 loopback 路径。
//  T9 dispatch_direct_timeout_late    DispatchDirect 超预算失败路径的 CoApp
//      _task_keeps_app_alive          所有权：注册任务停在调用 CoApp 之前，
//                                     外部超时返回 InternalError；停 run 并
//                                     放弃调用方强引用后 weak_ptr 证在途任务
//                                     仍强持有 app；放行后任务真实执行，对已
//                                     停止的 app 安全返回 RuntimeUnavailable
//                                     并最终释放对 app 的强引用。
//  T10 lifecycle_readers_survive_run_teardown
//                                    CoApp 生命周期对象共享寿命：run 收束期间
//                                    并发读取关闭状态/待收尾/失败清单与请求关闭
//                                    不得触到已析构的 HostLifecycle。
//  T11 callrpc_late_completion_after_timeout
//                                    CallRpc 超预算返回后任务迟到完成：结果与
//                                    完成信号在调用者/任务共享的堆状态上，不
//                                    借用调用者栈对象。
//  T12 lifecycle_publish_revoked_on_run_exception
//                                    run 发布 m_lifecycle 后异常退出（宿主
//                                    WaitHandlersDone 抛出，越过 Run）也必须
//                                    撤销发布：读口不得触到已析构的 run 局部
//                                    HostLifecycle。
//
// 线桥用框架默认 RpcHttpBridge（internal/）：x-bbt-* header ↔ envelope，
// 业务负载使用 CoRpcReq/CoRpcResp 位置参数 codec。静态路由的 endpoint 值
// 对 loopback 用 "self" 逻辑标记，测试缝发送闭包替换为宿主真实绑定
// 地址——路由门（服务名白名单）仍完整生效。
//
// 与上游单测一致：Boost.Test 经 included/unit_test.hpp 静态内嵌。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>
#include <boost/scope/scope_exit.hpp>

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
#include <bbt/coroutine/sync/CoWaiter.hpp>

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
        std::lock_guard<std::mutex> lk(m_mtx);
        if (--m_count > 0)
            return;
        // 锁内通知：等待者只能在通知返回、锁被释放后才可能因计数归零返回，
        // 于是「等待者返回后析构 latch」不可能仍与通知并发（通知在锁内的
        // 最后一次 CV 使用先于任何返回者退出）。超时返回仍由调用方按值持有
        // 的堆状态保活 latch（见 CallState），不靠本排序兜底。
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

// ── 协程闩（CoWaiter 上的单次放行语义）──
// Open 前 Wait 真实挂起；Open 后（含 Open 早于登记）全部放行，不丢唤醒；
// 使用 CoWaiter 的单次等待语义，无 sleep。支持多等待者（每等待者独立
// CoWaiter，登记成功后再发布/自唤醒）。
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
        const auto st = waiter->WaitWithCallback(wo, [&]() -> bool {
            // on_registered：登记成功后才发布；Open 早到则自唤醒（PENDING
            // 早到路径），入队失败/早到都不丢通知。
            std::lock_guard<std::mutex> lk(m_mtx);
            if (m_open) waiter->Notify();
            else        m_waiters.push_back(waiter);
            return true;
        });
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_waiters.erase(
                std::remove(m_waiters.begin(), m_waiters.end(), waiter),
                m_waiters.end());
        }
        return st;
    }

private:
    std::mutex                            m_mtx;
    bool                                  m_open = false;
    std::vector<co::sync::CoWaiter::SPtr> m_waiters;
};

// ── 预派发门（仅 T9 超时寿命用例使用）──
// 让经 DispatchDirect 注册的派发任务在调用 CoApp 之前停住：Park() 先发布
// 「已停在调用前」信号，再协作挂起（有界期限）到 Open()——不永久占用
// worker、不阻塞操作系统线程。全部状态经 shared_ptr 共享在堆上，不捕获
// 栈对象，也不接触裸 app。
struct PreDispatchGate {
    bool Park(co::Deadline deadline = Clock::now() + kWait)
    {
        arrived.CountDown();
        return m_gate.Wait(deadline) == co::WaitStatus::Completed;
    }
    bool Arrived(std::chrono::milliseconds ms)
    {
        return arrived.WaitFor(ms);
    }
    void Open()
    {
        m_gate.Open();
    }

    TestLatch arrived{1};
    CoGate m_gate;
};

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
    std::uint64_t   co_id = 0;
    std::thread::id tid{};
};

struct ProbeState {
    explicit ProbeState(int arrive_n) : arrived(arrive_n) {}

    TestLatch arrived;

    mutable std::mutex      mtx;
    std::condition_variable ev_cv;
    std::map<std::string, std::shared_ptr<CoGate>> gates;
    std::map<std::string, CtxSnapshot> before;
    std::map<std::string, CtxSnapshot> after;
    std::vector<std::string>         events;
    co::Deadline                     last_egress_deadline{};
    std::atomic<int>                 egress_calls{0};
    std::atomic<int>                 expired_sends_before{-1};
    // 目标 handler 真实进入次数（T8 用：拒绝路径必须不增）。
    std::atomic<int> inspect_calls{0};

    std::shared_ptr<CoGate> GateFor(const std::string& rid) {
        std::lock_guard<std::mutex> lk(mtx);
        auto& g = gates[rid];
        if (!g)
            g = std::make_shared<CoGate>();
        return g;
    }
    void CompleteAll() {
        std::vector<std::shared_ptr<CoGate>> all;
        {
            std::lock_guard<std::mutex> lk(mtx);
            for (auto& [rid, g] : gates) all.push_back(g);
        }
        for (auto& g : all) g->Open();
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
//    真实挂起（用 ctx 的 deadline 作等待参数），恢复后再次快照上下文。
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
        p->inspect_calls.fetch_add(1, std::memory_order_acq_rel);
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
        const auto st = gate->Wait(ctx.value()->deadline);
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
        const auto st = p->GateFor("slow")->Wait(ctx.value()->deadline);
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
        auto gate = std::make_shared<CoGate>();
        bbtco [this, req, slot, gate]() {
            slot->co_id = co::GetLocalCoroutineId();
            auto response = this->call("echo", "ping", req);
            if (response)
                slot->code = fw::ErrorCode::InternalError;
            else
                slot->code = response.error().code;
            gate->Open();
        };
        const auto st = gate->Wait(ctx.value()->deadline);
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
        // 等到父预算真实到期（deadline 超时，无 sleep）：随后出站必然在
        // 发起 I/O 之前被适配器判 TimedOut（egress 计数不增）。
        auto waiter = co::sync::CoWaiter::Create();
        co::WaitOptions wo;
        wo.deadline = ctx.value()->deadline;
        (void)waiter->Wait(wo);
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

// 测试夹具：真实 InfraHttpHost + CoApp + 测试侧 client runtime。
// 成员声明顺序即析构逆序：run 先于 app/host 收尾。
struct AppBox {
    std::shared_ptr<fw::InfraHttpHost>          host;
    // 出站闭包使用的 HttpClient 插槽：Start 后由 app 自身 runtime 建立。
    std::shared_ptr<std::shared_ptr<inf::HttpClient>> egress_slot;
    std::shared_ptr<inf::NetworkRuntime>        client_rt;
    std::shared_ptr<inf::HttpClient>            client;
    std::string                                 endpoint;
    // CoApp 由夹具与在途派发任务共享强持有：DispatchDirect 超预算返回后
    // 任务仍可能迟到，app 由任务的 shared_ptr 保活，窗口内不悬垂。
    std::shared_ptr<fw::CoApp> app;
    std::unique_ptr<RunHandle> run;

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
        // infra 关闭是同步契约：Close() 返回即物理释放，无需等待收束。
        if (client)    client->Close();
        if (client_rt) client_rt->Close();
        if (app)       app->request_shutdown();
        if (run && run->th.joinable()) run->th.join();
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

void StopApp(AppBox& box) { box.Stop(); }

// 客户端发送一条 envelope 并等回包；在协程内调用。
fw::result<inf::RpcEnvelope> SendRpc(
    const std::shared_ptr<inf::HttpClient>& client,
    const std::string& endpoint,
    const inf::RpcEnvelope& env,
    std::chrono::milliseconds budget) {
    inf::CallOptions o;
    o.deadline = Clock::now() + budget;
    auto res = client->Request(
        Wire::ToHttpRequest(env, "http://" + endpoint + "/rpc"), o);
    if (!res)
        return fw::result<inf::RpcEnvelope>::err(std::move(res.error()));
    return Wire::EnvelopeFromResponse(res.value());
}

// 一次 RPC 调用/派发的共享完成状态：结果与完成信号按值放在堆上，由调用方
// 与在途协程共同强持有。调用方超预算返回（或放弃自己的引用）后，迟到任务
// 仍写入同一块状态并通知同一个 latch——不存在指向调用者栈对象的引用，
// latch 的寿命也与任务同生共死（配合 TestLatch 锁内通知，成功等待路径同样
// 不与通知交叠）。
struct CallState {
    std::optional<fw::result<inf::RpcEnvelope>> out;
    TestLatch done{1};
};

// 控制线程发起一次 RPC 并限时等待结果（失败返回 err(InternalError)）。
//  - budget：请求自身的发送期限（与生产出站路径同形）；
//  - state：调用方持有的完成状态（可选；给出后可在超预算返回后观察迟到结果）；
//  - wait_budget：调用者等待上限，默认与 budget 相同。两者分开才能真实构造
//    「调用者已超时返回、任务仍在途」的寿命窗口。
// client/endpoint/请求/期限全部按值捕获，结果写在共享堆状态上：等待超限或
// 注册失败都只返回错误，不会让在途任务引用调用者栈对象。
fw::result<inf::RpcEnvelope> CallRpc(const AppBox& box, const inf::RpcEnvelope& env,
                                     std::chrono::milliseconds budget = kWait,
                                     std::shared_ptr<CallState> state = nullptr,
                                     std::optional<std::chrono::milliseconds> wait_budget = std::nullopt)
{
    if (!state)
        state = std::make_shared<CallState>();
    const auto wait = wait_budget.value_or(budget);
    auto client = box.client;
    auto endpoint = box.endpoint;
    auto request = env;
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [state, client, endpoint, request, budget] {
            state->out.emplace(SendRpc(client, endpoint, request, budget));
            state->done.CountDown();
        },
        succ);
    // 注册失败必须显式失败：不静默空过（否则调用方会把它误读成任务结果）。
    if (!succ)
        return fw::result<inf::RpcEnvelope>::err(
            fw::MakeError(fw::ErrorCode::InternalError, "CallRpc registration failed"));
    if (!state->done.WaitFor(wait))
        return fw::result<inf::RpcEnvelope>::err(fw::MakeError(
            fw::ErrorCode::InternalError, "CallRpc did not complete"));
    return std::move(*state->out);
}

// 经 DispatchInboundForTest（internal/CoAppSeam.hpp）在受管协程内直接分发：
// envelope → InboundDispatcher → 方法表 → RequestScope → handler 全链真实，
// 且 IncomingCallContext 由测试显式给出（可用已知绝对 deadline）。结果与完成
// 信号按值放进共享的 CallState：派发协程超预算仍在途时不留下指向栈对象的
// 悬垂引用。

// 可选测试控制块（默认空 → 与生产同形：无前置门、内部自建状态）：
//  - state：调用方持有的完成状态；超预算返回后仍可观察迟到任务的结果；
//  - before_dispatch：任务在调用 CoApp 之前的门（仅测试用，默认空）；
//  - deadline_at_dispatch：在派发协程内、紧邻 DispatchInboundForTest 之前取
//    「入站可见绝对期限」（覆盖 incoming.deadline）。近 20ms 边界的接受侧只与
//    派发 prologue（纯 CPU）竞速，注册/调度耗时不再进入判别式——判别靠同钟
//    绝对期限等值，不靠 wall-clock 容差。
struct DispatchControl {
    std::shared_ptr<CallState> state;
    std::function<bool()> before_dispatch;
    std::function<co::Deadline()> deadline_at_dispatch;
};

// app 以共享强持有交给派发任务：DispatchDirect 超预算返回（或调用方放弃
// 自己的引用）后，在途任务仍使 CoApp 本体存活，故迟到任务对 app 的调用
// 不会触到已析构对象（dispatcher 已释放则如实 RuntimeUnavailable）。
fw::result<inf::RpcEnvelope> DispatchDirect(const std::shared_ptr<fw::CoApp>& app,
                                            const inf::IncomingCallContext& incoming, const inf::RpcEnvelope& env,
                                            std::chrono::milliseconds budget = kWait, DispatchControl control = {})
{
    if (!control.state)
        control.state = std::make_shared<CallState>();
    auto state = control.state;
    auto request = env; // 副本随协程存活
    auto ctx = incoming;
    auto hook = std::move(control.before_dispatch);
    auto deadline_hook = std::move(control.deadline_at_dispatch);
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [state, app, request, ctx, hook, deadline_hook]() mutable {
            // 期限先取（同钟绝对），门其后；两者顺序固定，受控耗尽才落在期限之后。
            if (deadline_hook)
                ctx.deadline = deadline_hook();
            if (hook && !hook()) {
                state->out.emplace(fw::result<inf::RpcEnvelope>::err(
                    fw::MakeError(fw::ErrorCode::InternalError, "DispatchDirect gate did not open")));
            } else {
                state->out.emplace(fw::DispatchInboundForTest(*app, ctx, request));
            }
            state->done.CountDown();
        },
        succ);
    // 注册失败必须显式失败：不静默空过（否则会与「门未开」路径混为一谈）。
    if (!succ)
        return fw::result<inf::RpcEnvelope>::err(
            fw::MakeError(fw::ErrorCode::InternalError, "DispatchDirect registration failed"));
    if (!state->done.WaitFor(budget))
        return fw::result<inf::RpcEnvelope>::err(
            fw::MakeError(fw::ErrorCode::InternalError, "DispatchDirect did not complete"));
    return std::move(*state->out);
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

// T5：关闭是同步契约——控制线程直接 Close 即物理释放；重复 Close 幂等。
BOOST_AUTO_TEST_CASE(sync_close_on_control_thread) {
    g_probe = std::make_shared<ProbeState>(0);
    auto box = StartApp(std::chrono::milliseconds{30000},
                        std::chrono::milliseconds{2000},
                        /*routed*/{},
                        [](fw::CoApp& app) {
        BOOST_REQUIRE(app.add_service<ProbeSvc>(ConcurrentOpts()));
    });
    // 应用运行中的默认宿主已有真实 NetworkRuntime。
    auto runtime = box.host->network_runtime();
    BOOST_REQUIRE(runtime);
    BOOST_CHECK(!runtime->IsClosed());

    // 第二个独立宿主：控制线程 Create/Start/Close 同步收口——Close 返回即
    // 物理释放（无协程等待桥接、无 InvalidContext 约束），重复 Close 幂等。
    auto host2 = std::make_shared<fw::InfraHttpHost>(
        Limits(std::chrono::milliseconds{30000}),
        inf::ListenAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(host2->Create());
    BOOST_REQUIRE(host2->Start());
    auto rt2 = host2->network_runtime();
    BOOST_REQUIRE(rt2);
    BOOST_CHECK(!rt2->IsClosed());

    host2->Close();                                    // 同步
    BOOST_CHECK(rt2->IsClosed());                      // 返回即已释放
    BOOST_CHECK(host2->network_runtime() == nullptr);  // 句柄已释放
    BOOST_CHECK(host2->http_server() == nullptr);
    host2->Close();                                    // 幂等

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
    // 在途 handler 被放行后真实跑完请求全程：handler 侧 after 快照为证。
    // 注意 infra 同步 Close 契约——owner Close 经 Abort 丢弃未发送响应、
    // 不做 flush，故关闭期间完成的回复不保证送达原客户端（客户端结果可能
    // 是传输层错误），不作为断言对象。
    BOOST_CHECK(g_probe->At(g_probe->after, "t6-h").co_id != 0);
    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitShutdownLate);
    BOOST_CHECK(box.app->shutdown_state() == fw::ShutdownState::Closed);
    BOOST_CHECK(box.app->pending_cleanup().empty());

    if (box.client)    box.client->Close();
    if (box.client_rt) box.client_rt->Close();
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
    g_probe->GateFor("slow")->Open();
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

// T8：入站回包送达预留（20ms）的 framework 局部精确扣减。
//
// 只覆盖 framework 内这段语义：IncomingCallContext.deadline 经真实
// InboundDispatcher / 方法表 / RequestScope 落到 handler 可见的
// ctx.deadline 时恰好减去 kReplyDeliveryReserve(20ms) 一次；预算不足以
// 覆盖该预留（<=20ms）的请求在进入 handler 之前一律 TimedOut，目标
// handler 计数不增。不涉及跨层（infra local-min → framework）顺序声明。
//
// 断言口径：对已知绝对 incoming.deadline 做「减 20ms 后精确相等」——
// 不用 send_at ± 容差窗口（易 flaky），也不用「B 与 B+20 相差 20ms」
// 的相对差（该差值对「完全未扣」与「重复扣两次」同样成立，无法识别）。
BOOST_AUTO_TEST_CASE(inbound_deadline_reply_reserve_exact)
{
    using std::chrono::milliseconds;
    g_probe = std::make_shared<ProbeState>(0);
    auto box =
        StartApp(milliseconds{30000}, milliseconds{2000},
                 /*routed*/ {}, [](fw::CoApp& app) { BOOST_REQUIRE(app.add_service<ProbeSvc>(ConcurrentOpts())); });

    // (a) 充裕预算：handler 真实进入，ctx.deadline == 输入绝对值 - 20ms（恰一次）。
    const auto budget_a = Clock::now() + milliseconds{5000};
    inf::IncomingCallContext in_a;
    in_a.deadline = budget_a;
    const int calls_a = g_probe->inspect_calls.load();
    auto r_a = DispatchDirect(box.app, in_a, MakeEnv("probe", "inspect", "t8-a", 11));
    BOOST_REQUIRE(r_a);
    BOOST_CHECK(r_a.value().request_id == "t8-a");
    // I2 回源头：直接经 DispatchInboundForTest→InboundDispatcher 的回复必须
    // 五字段关联自洽——入站 request_schema 也由 dispatcher 回填，不依赖入站
    // 桥回显（缺它 infra HandleRpcRequest 会判 changed association fields）。
    BOOST_CHECK(r_a.value().service == "probe");
    BOOST_CHECK(r_a.value().method == "inspect");
    BOOST_CHECK(r_a.value().request_schema == std::string(fw::kCoRpcPositionalSchema));
    BOOST_CHECK(r_a.value().response_schema == std::string(fw::kCoRpcPositionalSchema));
    auto rep_a = DecodeReply(r_a);
    BOOST_REQUIRE(rep_a);
    BOOST_CHECK(rep_a.value() == 11);
    BOOST_CHECK(g_probe->inspect_calls.load() == calls_a + 1);

    const auto snap_a = g_probe->At(g_probe->before, "t8-a");
    BOOST_CHECK(snap_a.request_id == "t8-a"); // 确为本请求的 handler 内快照
    BOOST_CHECK(snap_a.co_id != 0);
    BOOST_CHECK(snap_a.deadline == budget_a - milliseconds{20});

    // (b) 紧预算 10ms（< 20ms 预留）：扣除预留后 handler 可见期限已过期，
    //     在进 handler 之前 TimedOut；handler 计数不增、无上下文快照。
    inf::IncomingCallContext in_b;
    in_b.deadline = Clock::now() + milliseconds{10};
    const int calls_b = g_probe->inspect_calls.load();
    auto r_b = DispatchDirect(box.app, in_b, MakeEnv("probe", "inspect", "t8-b", 12));
    BOOST_REQUIRE(!r_b);
    BOOST_CHECK(r_b.error().code == fw::ErrorCode::TimedOut);
    BOOST_CHECK(g_probe->inspect_calls.load() == calls_b);
    const auto snap_b = g_probe->At(g_probe->before, "t8-b");
    BOOST_CHECK(snap_b.co_id == 0);
    BOOST_CHECK(snap_b.request_id.empty());

    StopApp(box);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
}

// T8b：20ms 预留的**边界**（internal 确定性 seam，同 T8/T9 的 DispatchDirect 入口）。
//
// 代码契约（src/host/InboundDispatcher.cc）：ctx.deadline = incoming.deadline -
// kReplyDeliveryReserve(20ms)；其后 ctx.deadline <= now() → 进 handler 之前 TimedOut。
// 判别式因此是「扣除预留后的绝对期限是否已到」，**不是**名义预算大不大于 20ms：
//  - 恰 ==20ms：扣除后期限即「取期限那一刻」→ 恒拒绝，handler 不执行；
//  - 名义 +25ms 但派发被推到该期限之后（受控调度）→ 同样拒绝；
//  - 名义 +25ms 且取期限后仍有余量（剩余只有派发 prologue，纯 CPU）→ 执行，
//    且期限恰减一次 20ms（同钟精确等值）。
// 口径：期限在派发协程内、紧邻 DispatchInboundForTest 之前取（deadline_at_dispatch），
// 注册/调度耗时不再进入判别式；两侧都不使用 wall-clock 容差。本用例不涉及跨层
// （infra local-min → framework）顺序声明；跨层等值见 f3 T7 与 T7c。
BOOST_AUTO_TEST_CASE(inbound_deadline_reply_reserve_boundary)
{
    using std::chrono::milliseconds;
    g_probe = std::make_shared<ProbeState>(0);
    auto box =
        StartApp(milliseconds{30000}, milliseconds{2000},
                 /*routed*/ {}, [](fw::CoApp& app) { BOOST_REQUIRE(app.add_service<ProbeSvc>(ConcurrentOpts())); });

    // (a) 恰 ==20ms（边界值本身）：ctx.deadline == 取期限那一刻 → TimedOut，不进 handler。
    inf::IncomingCallContext in_eq;
    in_eq.deadline = Clock::now() + milliseconds{20};
    const int calls_eq = g_probe->inspect_calls.load();
    auto r_eq = DispatchDirect(box.app, in_eq, MakeEnv("probe", "inspect", "t8b-eq", 11));
    BOOST_REQUIRE(!r_eq);
    BOOST_CHECK(r_eq.error().code == fw::ErrorCode::TimedOut);
    BOOST_CHECK(g_probe->inspect_calls.load() == calls_eq);
    const auto snap_eq = g_probe->At(g_probe->before, "t8b-eq");
    BOOST_CHECK(snap_eq.co_id == 0);
    BOOST_CHECK(snap_eq.request_id.empty());

    // (b) 名义 +25ms（> 20ms 预留），但派发被受控推到「扣除预留后的期限已过」之后：
    //     仍然拒绝——名义预算大于预留不是进入 handler 的充分条件。
    auto armed_b = std::make_shared<std::atomic<std::int64_t>>(0);
    DispatchControl ctl_b;
    ctl_b.deadline_at_dispatch = [armed_b] {
        const auto d = Clock::now() + milliseconds{25};
        armed_b->store(std::chrono::duration_cast<std::chrono::nanoseconds>(d.time_since_epoch()).count(),
                       std::memory_order_release);
        return d;
    };
    ctl_b.before_dispatch = [armed_b] {
        // 受控耗尽：等到该期限本身已过（睡过头只会更晚，不影响判定）。
        const auto seen = co::Deadline{std::chrono::nanoseconds{armed_b->load(std::memory_order_acquire)}};
        std::this_thread::sleep_until(seen + milliseconds{1});
        return true;
    };
    const int calls_above = g_probe->inspect_calls.load();
    auto r_above = DispatchDirect(box.app, inf::IncomingCallContext{},
                                  MakeEnv("probe", "inspect", "t8b-above-rejected", 12), kWait, ctl_b);
    BOOST_REQUIRE(!r_above);
    BOOST_CHECK(r_above.error().code == fw::ErrorCode::TimedOut);
    BOOST_CHECK(g_probe->inspect_calls.load() == calls_above);
    const auto snap_above = g_probe->At(g_probe->before, "t8b-above-rejected");
    BOOST_CHECK(snap_above.co_id == 0);

    // (c) 同一名义 +25ms、无受控耗尽：只剩派发 prologue 与它竞速 → handler 执行，
    //     且期限恰减一次 20ms（同钟精确等值：未扣得 +20ms、双扣得 -20ms，均被排除）。
    auto armed_c = std::make_shared<std::atomic<std::int64_t>>(0);
    DispatchControl ctl_c;
    ctl_c.deadline_at_dispatch = [armed_c] {
        const auto d = Clock::now() + milliseconds{25};
        armed_c->store(std::chrono::duration_cast<std::chrono::nanoseconds>(d.time_since_epoch()).count(),
                       std::memory_order_release);
        return d;
    };
    const int calls_ok = g_probe->inspect_calls.load();
    auto r_ok = DispatchDirect(box.app, inf::IncomingCallContext{},
                               MakeEnv("probe", "inspect", "t8b-above-accepted", 13), kWait, ctl_c);
    if (!r_ok)
        BOOST_TEST_MESSAGE("t8b-above-accepted error code=" << static_cast<int>(r_ok.error().code)
                                                            << " msg=" << r_ok.error().message);
    BOOST_REQUIRE(r_ok);
    BOOST_CHECK(r_ok.value().request_id == "t8b-above-accepted");
    BOOST_CHECK(g_probe->inspect_calls.load() == calls_ok + 1);
    const auto snap_ok = g_probe->At(g_probe->before, "t8b-above-accepted");
    BOOST_CHECK(snap_ok.request_id == "t8b-above-accepted");
    BOOST_CHECK(snap_ok.deadline
                == co::Deadline{std::chrono::nanoseconds{armed_c->load(std::memory_order_acquire)}} - milliseconds{20});

    StopApp(box);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
}

// T9：DispatchDirect 超预算失败路径的 CoApp 所有权（免 UAF）。
//
// 注册到进程寿命 scheduler 的派发任务不属于宿主 drain（InfraHttpHost 的
// m_inflight 只记 HTTP handler），故 helper 必须自己在超预算返回后仍保活
// CoApp。本用例把这条失败路径真实走一遍：
//   (a) 预派发门令任务停在调用 CoApp 之前；外部紧预算等待超时 →
//       InternalError（任务仍在途，未被等待掩盖）；
//   (b) 停 run（on_release 释放 dispatcher）后放弃调用方强引用，weak_ptr
//       证明在途任务仍强持有 app；
//   (c) 放行门：任务真实执行，对已停止的 app 安全返回 RuntimeUnavailable、
//       不进 handler，且在结束时释放对 app 的强引用（weak 过期）。
BOOST_AUTO_TEST_CASE(dispatch_direct_timeout_late_task_keeps_app_alive)
{
    using std::chrono::milliseconds;
    auto probe = std::make_shared<ProbeState>(0);
    g_probe = probe;

    // 门与早退放行器先于 box 声明：断言早退时也由后析构的 Releaser 在 app
    // 完全停止后放行任务，迟到任务落到空 dispatcher 上，路径仍安全。
    auto gate = std::make_shared<PreDispatchGate>();
    struct Releaser {
        std::shared_ptr<PreDispatchGate> g;
        ~Releaser()
        {
            if (g)
                g->Open();
        }
    } releaser{gate};

    auto box =
        StartApp(milliseconds{30000}, milliseconds{2000},
                 /*routed*/ {}, [](fw::CoApp& app) { BOOST_REQUIRE(app.add_service<ProbeSvc>(ConcurrentOpts())); });
    std::weak_ptr<fw::CoApp> app_weak = box.app;

    DispatchControl control;
    control.state = std::make_shared<CallState>();
    control.before_dispatch = [gate] {
        return gate->Park();
    };

    inf::IncomingCallContext in;
    in.deadline = Clock::now() + milliseconds{5000};

    // 门到期而非显式放行：失败必须在调用 CoApp 前终止。
    auto expired_gate = std::make_shared<PreDispatchGate>();
    DispatchControl expired_control;
    const auto expired_at = Clock::now();
    expired_control.before_dispatch = [expired_gate, expired_at] {
        return expired_gate->Park(expired_at);
    };
    auto rejected = DispatchDirect(box.app, in, MakeEnv("probe", "inspect", "t9-expired", 5), kWait, expired_control);
    BOOST_REQUIRE(!rejected);
    BOOST_CHECK(rejected.error().code == fw::ErrorCode::InternalError);
    // 任务确已到门：本路径的失败来自「门未开」，不是注册失败空过
    // （注册失败时不会有任务执行，arrived 永远不会被置位）。
    BOOST_REQUIRE(expired_gate->Arrived(kWait));
    BOOST_CHECK(probe->inspect_calls.load() == 0);

    // (a) 任务停在门处、预算内不完结 → 外部超时返回 InternalError。
    auto r = DispatchDirect(box.app, in, MakeEnv("probe", "inspect", "t9-a", 5), milliseconds{300}, control);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == fw::ErrorCode::InternalError);
    BOOST_REQUIRE(gate->Arrived(kWait)); // 确已停在调用 CoApp 之前
    BOOST_CHECK(!app_weak.expired());    // 在途任务已强持有 app

    // (b) 停止 run（收束路径释放 dispatcher）；放弃调用方与全局探针的引用。
    StopApp(box);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
    box.app.reset();
    g_probe.reset();
    BOOST_REQUIRE(!app_weak.expired()); // 仍由在途任务强持有

    // (c) 放行：任务真实执行到已停止的 app → RuntimeUnavailable，不进 handler。
    gate->Open();
    // 结果读写经 mutex/CV 完成信号同步；weak_ptr 只观测对象寿命。
    BOOST_REQUIRE(control.state->done.WaitFor(kWait));
    const auto until = Clock::now() + kWait;
    while (!app_weak.expired()) {
        BOOST_REQUIRE(Clock::now() < until);
        std::this_thread::yield();
    }
    BOOST_REQUIRE(control.state->out.has_value());
    BOOST_REQUIRE(!control.state->out.value());
    BOOST_CHECK(control.state->out.value().error().code == fw::ErrorCode::RuntimeUnavailable);
    BOOST_CHECK(probe->inspect_calls.load() == 0); // 未进入目标 handler
}

// T10：CoApp 生命周期对象的共享寿命（run 收束期间并发读取）。
//
// m_lifecycle 发布的是 run 局部 HostLifecycle 的地址：run 收束时清空指针并
// 析构局部对象。若读取者取到指针后与「清空 + 析构」并发，就会进入已析构对象
// （CondVar destroy vs broadcast：TSan 在 T9 报告过同一对操作）。本用例让
// 读线程在收束全程持续走完四个读取入口（状态/待收尾/失败清单/请求关闭），
// 断言收束正常完成、run 结束后各读口仍安全。修复前该窗口是真实 UAF。
BOOST_AUTO_TEST_CASE(lifecycle_readers_survive_run_teardown)
{
    g_probe = std::make_shared<ProbeState>(0);
    auto box =
        StartApp(std::chrono::milliseconds{30000}, std::chrono::milliseconds{2000},
                 /*routed*/ {}, [](fw::CoApp& app) { BOOST_REQUIRE(app.add_service<ProbeSvc>(ConcurrentOpts())); });

    std::atomic<bool> stop{false};
    std::atomic<long> reads{0};
    TestLatch reader_ran{1};
    std::thread reader([&] {
        bool first = true;
        while (!stop.load(std::memory_order_acquire)) {
            // 四个读取入口全部走一遍：它们都经 m_lifecycle 取用 run 的局部
            // 生命周期对象，收束窗口内不得触到已析构对象。
            (void)box.app->shutdown_state();
            (void)box.app->pending_cleanup();
            (void)box.app->lifecycle_failures();
            box.app->request_shutdown();
            if (first) {
                reader_ran.CountDown();
                first = false;
            }
            reads.fetch_add(1, std::memory_order_relaxed);
        }
    });
    // 受控屏障：读线程至少完整走完一轮四个入口，收束才被启动。
    BOOST_REQUIRE(reader_ran.WaitFor(kWait));

    StopApp(box); // 与读线程并发：请求关闭 → join run（清空发布指针并析构
                  // 局部生命周期对象），读线程仍在窗口内活动。
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
    BOOST_CHECK(reads.load(std::memory_order_relaxed) > 0);

    stop.store(true, std::memory_order_release);
    reader.join();

    // run 结束后各读口仍安全：报停驻状态，不再触碰已析构的生命周期对象。
    BOOST_CHECK(box.app->shutdown_state() == fw::ShutdownState::Closed);
    BOOST_CHECK(box.app->pending_cleanup().empty());
    BOOST_CHECK(box.app->lifecycle_failures().empty());
}

// T11：CallRpc 超预算返回后任务迟到完成（无栈借用）。
//
// 构造：请求的发送期限（5s）远长于调用者等待上限（150ms），handler 停在
// per-rid 门上（ctx 预算 30s）。调用者必然先超限返回（任务不可能在门放行前
// 完成），随后放行 handler：迟到任务在 CallRpc 返回之后把结果写进共享堆状态
// 并通知 latch——修复前该写入落在调用者已析构的栈 optional/latch 上。
BOOST_AUTO_TEST_CASE(callrpc_late_completion_after_timeout)
{
    using std::chrono::milliseconds;
    g_probe = std::make_shared<ProbeState>(1);
    auto box =
        StartApp(milliseconds{30000}, milliseconds{2000},
                 /*routed*/ {}, [](fw::CoApp& app) { BOOST_REQUIRE(app.add_service<ProbeSvc>(ConcurrentOpts())); });

    auto state = std::make_shared<CallState>();
    auto r = CallRpc(box, MakeEnv("probe", "wait_echo", "t11-late", 21),
                     /*budget*/ milliseconds{5000}, state,
                     /*wait_budget*/ milliseconds{150});
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == fw::ErrorCode::InternalError);
    // 请求已真实进 handler 并停在门上：任务确在途（不是尚未被调度）。
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));
    BOOST_CHECK(!box.run->done.load());

    // 放行 handler：迟到任务完成并写入共享状态。
    g_probe->CompleteAll();
    BOOST_REQUIRE(state->done.WaitFor(kWait));
    BOOST_REQUIRE(state->out.has_value());
    BOOST_REQUIRE(state->out.value());
    BOOST_CHECK(state->out.value().value().request_id == "t11-late");
    auto rep = DecodeReply(state->out.value());
    BOOST_REQUIRE(rep);
    BOOST_CHECK(rep.value() == 21);

    // 迟到任务收束后应用仍正常服务（未因栈借用/悬垂写入受损）。
    auto again = CallRpc(box, MakeEnv("probe", "inspect", "t11-after", 3));
    BOOST_REQUIRE(again);
    auto rep2 = DecodeReply(again);
    BOOST_REQUIRE(rep2);
    BOOST_CHECK(rep2.value() == 3);

    StopApp(box);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
}

// T12：run 的异常退出（发布点之后、清空点之前抛出）也必须撤销发布。
//
// 窗口：run() 发布 m_lifecycle 后，其后的任何抛出（Hooks 构造、lifecycle.Run、
// early_failures 分配等 bad_alloc）都会栈回卷析构 run 的局部 HostLifecycle，而
// 旧代码的 m_lifecycle 仍指向该已析构栈对象。修复 = 发布前装无分配 scope guard，
// 任何退出（return/throw）都在局部 lifecycle 析构前、于 m_lifecycle_mtx 内清空。
//
// 触发经既有测试缝（MakeCoAppForTest 注入 INetworkHost）：桩的 WaitHandlersDone
// 主动抛出；该点在 HostLifecycle::_ShutDown 未被 try/catch 包裹，异常越过 Run 与
// run()。注意 HostLifecycle 的三个 Hook（on_scheduler_started/on_handlers_drained/
// on_release）的异常在 Run 内被 catch 收口为失败项、**不**越过 Run——它们不能覆盖
// 本窗口，故本用例不以 Hook 冒充覆盖。
//
// 关闭请求在 Start() 回调之后才发出：此时 run 已发布 m_lifecycle，request_shutdown
// 走「指针非空」分支、不写停驻状态，故 m_parked_state 保持初始 Running——这正是
// 能判别修复的读数（无 guard 的旧码此处进入悬垂对象，shutdown_state() 读到 Closing）。
class ThrowingDrainHost final : public fw::INetworkHost {
public:
    TestLatch started{1};
    fw::result<void> Create() override
    {
        return fw::result<void>::ok();
    }
    fw::result<void> Start() override
    {
        started.CountDown();
        return fw::result<void>::ok();
    }
    void StopAccepting() noexcept override
    {
    }
    fw::result<void> WaitHandlersDone(co::Deadline) override
    {
        throw std::runtime_error("injected WaitHandlersDone throw");
    }
    void Close() noexcept override
    {
    }
};

BOOST_AUTO_TEST_CASE(lifecycle_publish_revoked_on_run_exception)
{
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;

    fw::CoAppOptions opts{inf::NetworkLimits{/*max_connections*/ 64, /*max_inflight*/ 64,
                                             /*max_header_bytes*/ 8192, /*max_body_bytes*/ 65536,
                                             /*incoming_timeout*/ std::chrono::milliseconds{30000}},
                          inf::ListenAddress{"127.0.0.1", 0},
                          /*static_routes*/ {},
                          /*shutdown_step_budget*/ std::chrono::milliseconds{2000}};
    auto host = std::make_shared<ThrowingDrainHost>();
    auto app = fw::MakeCoAppForTest(opts, fw::CoAppSeam{host, {}});
    BOOST_REQUIRE(app);

    std::atomic<bool> threw{false};
    std::thread runner([&] {
        try {
            (void)app->run();
        } catch (const std::runtime_error&) {
            threw.store(true, std::memory_order_release);
        }
    });

    auto finish_runner = boost::scope::make_scope_exit([&]() noexcept {
        app->request_shutdown();
        if (runner.joinable()) {
            runner.join();
        }
    });

    // run 已发布 m_lifecycle 并进入服务（Start 回调）后才请求关闭。
    BOOST_REQUIRE(host->started.WaitFor(kWait));
    app->request_shutdown();
    runner.join();
    BOOST_REQUIRE_MESSAGE(threw.load(std::memory_order_acquire), "host WaitHandlersDone throw must reach out of run()");

    // run 已返回（栈回卷，局部 HostLifecycle 已析构）。修复后 m_lifecycle 已被
    // guard 清空，读口走「未发布」分支、返回明确的停驻值，绝不解引用已析构对象
    // （修复前此处进入悬垂对象：shutdown_state() 会读到 Closing，本断言即失败）。
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Running);
    BOOST_CHECK(app->pending_cleanup().empty());
    BOOST_CHECK(app->lifecycle_failures().empty());
    app->request_shutdown(); // 指针已空 → 停驻状态 CAS Running→Closing
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Closing);
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace
