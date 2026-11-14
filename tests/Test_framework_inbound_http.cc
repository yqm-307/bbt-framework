// 真实 infra V2 会话闭环（v2::CoApp）。
//
// 覆盖（全部真实断言、无 mock/stub、无 sleep 凑时序）：
//  T1 inbound_context_fields_land     入站字段落地：request_id/deadline/
//                                    peer_principal/trace_id 取自
//                                    infra 填充的 V2 Request Context 与
//                                    metadata；route.* 之外 metadata 与
//                                    未授权 fw.* 被拒；custom 不能伪造系统字段。
//  T2 concurrent_context_isolated     8 并发请求各自 RequestScope 随协程
//      _cross_worker                  绑定互不串扰；占用对端 worker 的
//                                    spinner 协程制造确定性跨 worker 恢复。
//  T3 unmanaged_coroutine_invalid     非受管协程经已托管服务实例调用
//      _context_end_to_end            this->call → 端到端真实 InvalidContext；
//                                    受管入站路径作正路径对照（真实回路）。
//  T4 outbound_deadline_min_and       出站期限缺省继承父预算、显式期限取
//      _expired_before_io             min；父预算过期 → 发起 I/O 前 TimedOut。
//  T5 sync_close_on_control_thread    宿主 Close 收口：逻辑封口 + 物理观察；
//                                    重复 Close 幂等。
//  T6 handler_drain_shutdown_late     真实 in-flight handler 未排空 →
//                                    StopIncomplete → 放行后完整收束 →
//                                    再次显式 CoStop 到 Stopped。
//  T9 dispatch_direct_timeout_late    DispatchDirect 超预算失败路径的 CoApp
//      _task_keeps_app_alive          所有权：注册任务停在调用 CoApp 之前，
//                                    外部超时返回 InternalError；停 run 并
//                                    放弃调用方强引用后 weak_ptr 证在途任务
//                                    仍强持有 app；放行后任务真实执行，对已
//                                    停止的 app 安全返回 RuntimeUnavailable
//                                    并最终释放对 app 的强引用。
//  T10 lifecycle_readers_survive_run_teardown
//                                    CoApp 生命周期对象共享寿命：run 收束期间
//                                    并发读取状态/待收尾/失败清单与请求关闭
//                                    不得触到已析构的对象。
//  T11 callrpc_late_completion_after_timeout
//                                    CallRpc 超预算返回后任务迟到完成：结果与
//                                    完成信号在调用者/任务共享的堆状态上，不
//                                    借用调用者栈对象。
//  T12 lifecycle_stop_exception_contained
//                                    宿主收口等待抛异常：被收口为失败项，
//                                    不逃出 CoStop，状态 StopIncomplete、
//                                    owner/资源保留、失败项可观察。
//  T13 rpc_admission_nested_co_rpc_close
//                                    V2 RPC admission 入站（InstallRpcAdmission，
//                                    不是 HTTP）→ handler 内嵌套 Co RPC →
//                                    自有 Co 资源 → CoStop 收口。
//  T14 http_wire_nested_co_rpc_close
//                                    真实 HTTP wire：infra http::Client POST →
//                                    loopback → infra HTTP parser → 宿主
//                                    InstallHandler → framework 业务分发 →
//                                    嵌套 Co RPC（真实 RPC 对端宿主）→ 200
//                                    应答 → CoStop 收口资源与宿主 owner。
//  T15 http_host_start_failure_rolls_back
//                                    同一 HTTP 配置下 listener 绑定失败（端口
//                                    被占用）的启动回滚：宿主封口、对照新宿主
//                                    真实起停。
//  T16 http_ingress_service_component
//                                    真实入口 HTTP→Service→Component：同一
//                                    v2::CoApp 上，infra HTTP wire → 应用侧
//                                    handler → DispatchInboundForTest →
//                                    InboundDispatcher → Service → 真实
//                                    ComponentHandle::Borrow（组件实例）。
//  T17 host_gate_single_run_child     子进程真实入口：bbtco_main 装配 →
//                                    bbtco_main_host_hook 对同一共享 Engine
//                                    恰执行一次 Run（进程退出码与 port 文件
//                                    证据）。
//
// 入站两条协议互不替代：
//  - RPC 入站（T1–T13、T16 嵌套段）走真实 V2 会话（SessionFrame + u32be +
//    Hello/HelloAck）：v2::CoApp 经 AppNetworkOptions 内部装配
//    InstallRpcAdmission，客户端是互异 identity 的第二个会话 owner +
//    RpcEgress::Call。业务负载使用 CoRpcReq/CoRpcResp 位置参数 codec。
//    RPC 成功不能当作 HTTP 证据。
//  - HTTP 入站（T14/T16）走 bbt::infra::http::Client/Server 的真实 HTTP/1.1
//    wire；宿主 Create 拒绝 HTTP 与 RPC 共用一个 listener，故 HTTP 用例的应用
//    宿主只装 HTTP handler，RPC 能力由互异 identity 的对端宿主承担。
//  - T17 以独立子进程验收 runtime 的生成式入口 + HostGate（Run 只在入口
//    native host hook 恰执行一次）。
//
// 装配形态：生产 v2::CoApp 的静态路由在 Create 时写入 AppRuntimeState；
// 测试用例需要「handler 内 this->call 打到对端」时，静态路由在 Create 前由
// ReserveLoopbackPort 预占一个空闲 loopback 端口，同时作为监听地址与
// 路由 endpoint——与生产自环（同 App 出站回本进程监听）同一条公开路径，
// 不经发送注入。
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
#include <cstdlib>
#include <fstream>
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

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/http/Client.hpp>
#include <bbt/infra/http/Server.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/V2.hpp>

// 机器面（宿主/线桥/生命周期/测试缝构造）显式走 internal 头。
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/HostLifecycle.hpp>
#include <bbt/framework/internal/RpcBridge.hpp>

#include "framework_co_harness.hpp"

namespace fw = bbt::framework;
namespace v2 = bbt::framework::v2;
namespace co = bbt::coroutine;
namespace inf = bbt::infra;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

namespace {

// ── 测试屏障原语（std CV，无 sleep 凑时序）──

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
    std::mutex m_mtx;
    std::condition_variable m_cv;
    int m_count;
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
        for (auto& w : notify)
            w->Notify();
    }
    co::WaitStatus Wait(co::Deadline deadline) {
        auto waiter = co::sync::CoWaiter::Create();
        co::WaitOptions wo;
        wo.deadline = deadline;
        const auto st = waiter->WaitWithCallback(wo, [&]() -> bool {
            // on_registered：登记成功后才发布；Open 早到则自唤醒（PENDING
            // 早到路径），入队失败/早到都不丢通知。
            std::lock_guard<std::mutex> lk(m_mtx);
            if (m_open)
                waiter->Notify();
            else
                m_waiters.push_back(waiter);
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
    std::mutex m_mtx;
    bool m_open = false;
    std::vector<co::sync::CoWaiter::SPtr> m_waiters;
};

// ── 预派发门（仅 T9 超时寿命用例使用）──
// 让经 DispatchDirect 注册的派发任务在调用 CoApp 之前停住：Park() 先发布
// 「已停在调用前」信号，再协作挂起（有界期限）到 Open()——不永久占用
// worker、不阻塞操作系统线程。全部状态经 shared_ptr 共享在堆上，不捕获
// 栈对象，也不接触裸 app。
struct PreDispatchGate {
    bool Park(co::Deadline deadline = Clock::now() + kWait) {
        arrived.CountDown();
        return m_gate.Wait(deadline) == co::WaitStatus::Completed;
    }
    bool Arrived(std::chrono::milliseconds ms) {
        return arrived.WaitFor(ms);
    }
    void Open() {
        m_gate.Open();
    }

    TestLatch arrived{1};
    CoGate m_gate;
};

// ── CoRpc 位置参数测试辅助 ──

fw::result<std::int32_t> IntArg(const fw::CoRpcReq& req) {
    auto args = req.Parse<std::int32_t>();
    if (!args)
        return fw::result<std::int32_t>::err(args.error());
    return fw::result<std::int32_t>::ok(args.value());
}

fw::result<std::tuple<std::string, std::int32_t>> AcctArgs(
    const fw::CoRpcReq& req) {
    return req.Parse<std::string, std::int32_t>();
}

fw::CoRpcReq RequestInt(std::int32_t value) {
    auto req = fw::CoRpcReq::From(value);
    if (!req)
        throw std::logic_error("request encode failed");
    return std::move(req).value();
}

fw::CoRpcReq RequestAcct(std::string account, std::int32_t value) {
    auto req = fw::CoRpcReq::From(
        std::tuple{std::move(account), value});
    if (!req)
        throw std::logic_error("request encode failed");
    return std::move(req).value();
}

fw::CoRpcResp ReplyInt(const fw::result<std::int32_t>& value) {
    if (!value)
        return fw::CoRpcResp::Error(value.error());
    return fw::CoRpcResp::From(value.value());
}

// ── 观测态：业务实例默认构造，经全局指针挂接当前用例的探针 ──

struct CtxSnapshot {
    std::string request_id;
    std::string peer_principal;
    std::string actor_key; // 无 → ""
    std::string trace_id;  // 无 → ""
    co::Deadline deadline{};
    std::uint64_t co_id = 0;
    std::thread::id tid{};
};

struct ProbeState {
    explicit ProbeState(int arrive_n) : arrived(arrive_n) {}

    TestLatch arrived;

    mutable std::mutex mtx;
    std::condition_variable ev_cv;
    std::map<std::string, std::shared_ptr<CoGate>> gates;
    std::map<std::string, CtxSnapshot> before;
    std::map<std::string, CtxSnapshot> after;
    std::vector<std::string> events;
    co::Deadline last_egress_deadline{};
    std::atomic<int> egress_calls{0};
    std::atomic<int> expired_sends_before{-1};
    // 目标 handler 真实进入次数（拒绝路径必须不增）。
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
            for (auto& [rid, g] : gates)
                all.push_back(g);
        }
        for (auto& g : all)
            g->Open();
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
            return std::find(events.begin(), events.end(), e) != events.end();
        });
    }
    std::size_t EventIndex(const std::string& e) const {
        std::lock_guard<std::mutex> lk(mtx);
        for (std::size_t i = 0; i < events.size(); ++i)
            if (events[i] == e)
                return i;
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
    s.request_id = c.request_id;
    s.peer_principal = c.peer_principal;
    s.actor_key = c.actor_key.value_or("");
    s.trace_id = c.trace_id.value_or("");
    s.deadline = c.deadline;
    s.co_id = co::GetLocalCoroutineId();
    s.tid = std::this_thread::get_id();
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

// ── 测试服务（v2：ConstructKey 受控构造、protected 析构、friend Registry）──

// "probe"：Concurrent；inspect 立即回显，wait_echo 在 per-rid 信号上
//    真实挂起（用 ctx 的 deadline 作等待参数），恢复后再次快照上下文。
class ProbeSvc final : public v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "probe";
    explicit ProbeSvc(fw::detail::ConstructKey k) : v2::CoService<>(k) {}
    fw::CoRpcResp Inspect(fw::CoRpcReq req) {
        auto value = IntArg(req);
        if (!value)
            return fw::CoRpcResp::Error(value.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
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
        if (!value)
            return fw::CoRpcResp::Error(value.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
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
        p->Event("resumed:" + rid);
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&ProbeSvc::Inspect>("inspect"),
        fw::Method<&ProbeSvc::WaitEcho>("wait_echo"));
protected:
    ~ProbeSvc() noexcept override = default;
    friend class fw::detail::Registry;
};

// "echo"：Concurrent 远端被调方；slow 在全局 slow 门上挂起。
class EchoSvc final : public v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "echo";
    explicit EchoSvc(fw::detail::ConstructKey k) : v2::CoService<>(k) {}
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        return ReplyInt(IntArg(req));
    }
    fw::CoRpcResp Slow(fw::CoRpcReq req) {
        auto value = IntArg(req);
        if (!value)
            return fw::CoRpcResp::Error(value.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        auto p = g_probe;
        const auto st = p->GateFor("slow")->Wait(ctx.value()->deadline);
        if (st != co::WaitStatus::Completed)
            return fw::CoRpcResp::Error(WaitErr(st, "echo slow"));
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EchoSvc::Ping>("ping"),
        fw::Method<&EchoSvc::Slow>("slow"));
protected:
    ~EchoSvc() noexcept override = default;
    friend class fw::detail::Registry;
};

// "caller"：Concurrent；在受管 handler 内发起到 echo 的真实远端调用。
class CallerSvc final : public v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "caller";
    explicit CallerSvc(fw::detail::ConstructKey k) : v2::CoService<>(k) {}
    fw::CoRpcResp Dial(fw::CoRpcReq req) {
        auto response = this->call("echo", "ping", req);
        if (!response)
            return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    fw::CoRpcResp SpawnDial(fw::CoRpcReq req) {
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        auto p = g_probe;
        const auto parent_co = co::GetLocalCoroutineId();
        struct Slot {
            fw::ErrorCode code = fw::ErrorCode::InternalError;
            std::uint64_t co_id = 0;
        };
        auto slot = std::make_shared<Slot>();
        auto gate = std::make_shared<CoGate>();
        bbtco[this, req, slot, gate]() {
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
protected:
    ~CallerSvc() noexcept override = default;
    friend class fw::detail::Registry;
};

// "relay"：Concurrent；三种出站期限形态，真实经 loopback 到 echo。
class RelaySvc final : public v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "relay";
    explicit RelaySvc(fw::detail::ConstructKey k) : v2::CoService<>(k) {}
    fw::CoRpcResp CallDefault(fw::CoRpcReq req) {
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        g_probe->Snapshot(g_probe->before, ctx.value()->request_id,
                          SnapCtx(*ctx.value()));
        auto response = this->call("echo", "ping", req);
        if (!response)
            return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    fw::CoRpcResp CallLonger(fw::CoRpcReq req) {
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        g_probe->Snapshot(g_probe->before, ctx.value()->request_id,
                          SnapCtx(*ctx.value()));
        fw::CallOptions opt;
        opt.deadline = Clock::now() + std::chrono::minutes{10};
        auto response = this->call("echo", "ping", req, {}, opt);
        if (!response)
            return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    fw::CoRpcResp CallExpired(fw::CoRpcReq req) {
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        auto p = g_probe;
        // 等到父预算真实到期（deadline 超时，无 sleep）：随后出站必然在
        // 发起 I/O 之前被适配器判 TimedOut。
        auto waiter = co::sync::CoWaiter::Create();
        co::WaitOptions wo;
        wo.deadline = ctx.value()->deadline;
        (void)waiter->Wait(wo);
        p->expired_sends_before.store(
            p->egress_calls.load(std::memory_order_acquire),
            std::memory_order_release);
        auto response = this->call("echo", "ping", req);
        if (!response)
            return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&RelaySvc::CallDefault>("call_default"),
        fw::Method<&RelaySvc::CallLonger>("call_longer"),
        fw::Method<&RelaySvc::CallExpired>("call_expired"));
protected:
    ~RelaySvc() noexcept override = default;
    friend class fw::detail::Registry;
};

// "acct"：ActorSerial；actor key 取自第一个位置参数
// （客户端填 "acct-<v>"）。Xfer 在远端（echo.slow）等待，XferFast 走
// 即时远端；事件日志证明同 key 不重入、异 key 并发。
class AcctSvc final : public v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "acct";
    explicit AcctSvc(fw::detail::ConstructKey k) : v2::CoService<>(k) {}
    fw::CoRpcResp Xfer(fw::CoRpcReq req) {
        auto args = AcctArgs(req);
        if (!args)
            return fw::CoRpcResp::Error(args.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        auto p = g_probe;
        const std::string rid = ctx.value()->request_id;
        p->Snapshot(p->before, rid, SnapCtx(*ctx.value()));
        p->Event("start:" + rid);
        auto response = this->call(
            "echo", "slow", RequestInt(std::get<1>(args.value())));
        p->Event("end:" + rid);
        if (!response)
            return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    fw::CoRpcResp XferFast(fw::CoRpcReq req) {
        auto args = AcctArgs(req);
        if (!args)
            return fw::CoRpcResp::Error(args.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        auto p = g_probe;
        const std::string rid = ctx.value()->request_id;
        p->Snapshot(p->before, rid, SnapCtx(*ctx.value()));
        p->Event("start:" + rid);
        auto response = this->call(
            "echo", "ping", RequestInt(std::get<1>(args.value())));
        p->Event("end:" + rid);
        if (!response)
            return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::ActorMethodAt<&AcctSvc::Xfer, std::string>("xfer"),
        fw::ActorMethodAt<&AcctSvc::XferFast, std::string>("xfer_fast"));
protected:
    ~AcctSvc() noexcept override = default;
    friend class fw::detail::Registry;
};

// ── 装配选项 ──

fw::ServiceOptions ConcurrentOpts() {
    return fw::ServiceOptions{}.SetMaxInflight(64);
}

fw::ServiceOptions ActorOpts(std::size_t mailbox_capacity = 16,
                              std::size_t max_actors = 8) {
    return fw::ServiceOptions{}
        .SetMaxInflight(64)
        .SetActorSerial()
        .SetSerialQueueCapacity(mailbox_capacity)
        .SetMaxSerialKeys(max_actors);
}

inf::NetworkLimits Limits(std::chrono::milliseconds incoming_timeout) {
    return inf::NetworkLimits{
        /*max_connections*/ 64,
        /*max_inflight*/ 64,
        /*max_header_bytes*/ 16384,
        /*max_body_bytes*/ 65536,
        incoming_timeout};
}

// 预占一个空闲 loopback 端口：bind :0 取内核分配端口后关闭。v2 静态路由在
// Create 时写入，同进程自环场景用它同时作为监听地址与路由 endpoint；
// 竞态窗口（bind→Run）极小，失败时 Run/CoStart 如实报错，不假通过。
struct ReserveLoopbackPort {
    std::uint16_t port{0};
    ReserveLoopbackPort() {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0)
            return;
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            ::close(fd);
            return;
        }
        socklen_t len = sizeof(addr);
        if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
            ::close(fd);
            return;
        }
        port = ntohs(addr.sin_port);
        ::close(fd);
    }
};

struct RunHandle {
    // 析构兜底：REQUIRE 早退时线程仍 joinable 会 terminate；
    // 正常路径由 AppBox::Stop 先 RequestShutdown 再 join。
    ~RunHandle() {
        if (th.joinable())
            th.detach();
    }
    std::thread th;
    std::atomic<bool> done{false};
    fw::result<void> rc = fw::result<void>::err(
        fw::MakeError(fw::ErrorCode::RuntimeUnavailable, "not run"));
};

std::unique_ptr<RunHandle> RunApp(v2::CoApp& app) {
    auto h = std::make_unique<RunHandle>();
    RunHandle* p = h.get();
    h->th = std::thread([p, &app] {
        p->rc = framework_test::RunAppUntilStopped(app);
        p->done.store(true, std::memory_order_release);
    });
    return h;
}

void JoinRun(const std::unique_ptr<RunHandle>& h) {
    if (h->th.joinable())
        h->th.join();
}

// 占用某个 worker 的 spinner 协程：协程内不让出（协作式无抢占），
// 其所在 worker 因此无法拉取被唤醒的协程——跨 worker 证据的确定性来源。
struct Spinner {
    std::atomic<bool> release{false};
    std::atomic<bool> spinning{false};
    std::atomic<std::thread::id> tid{};
};

std::shared_ptr<Spinner> LaunchSpinner() {
    auto s = std::make_shared<Spinner>();
    bbtco[s]() {
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

// 测试夹具：真实 v2::CoApp（AppNetworkOptions → 内部 InstallRpcAdmission +
// 共享 Engine）+ 互异 identity 的会话 owner（测试客户端）。
struct AppBox {
    std::shared_ptr<v2::CoApp> app;
    std::shared_ptr<inf::net::Engine> engine;
    std::shared_ptr<fw::InfraHttpHost> client_rt;
    std::shared_ptr<fw::InfraHttpHost> egress_rt;
    std::shared_ptr<fw::RpcEgress> client;
    std::string endpoint;
    std::unique_ptr<RunHandle> run;

    AppBox() = default;
    ~AppBox() { Stop(); }
    AppBox(AppBox&&) = default;
    AppBox& operator=(AppBox&&) = default;
    AppBox(const AppBox&) = delete;
    AppBox& operator=(const AppBox&) = delete;

    void Stop() {
        for (auto& s : g_spinners)
            s->release.store(true, std::memory_order_release);
        if (g_probe)
            g_probe->CompleteAll();
        if (client_rt)
            (void)framework_test::HostClose(*client_rt);
        if (egress_rt)
            (void)framework_test::HostClose(*egress_rt);
        if (app)
            app->RequestShutdown();
        if (run && run->th.joinable())
            run->th.join();
        if (engine)
            framework_test::CloseSharedEngine(engine);
    }
};

std::shared_ptr<fw::InfraHttpHost> MakeHost(
    const inf::NetworkLimits& limits, const std::string& identity) {
    return std::make_shared<fw::InfraHttpHost>(
        limits, inf::ListenAddress{"127.0.0.1", 0}, identity);
}

// incoming_timeout 决定入站 ctx 的预算（accept 时刻 + timeout）；
// routed 列出允许出站的服务名（路由门白名单，endpoint 指向预占端口）；
// register_fn 在受管协程内经 CoInstallService 装配服务。
AppBox StartApp(std::chrono::milliseconds incoming_timeout,
                std::chrono::milliseconds step_budget,
                const std::vector<std::string>& routed,
                const std::function<void(v2::CoApp&)>& register_fn) {
    framework_test::EnsureRuntime();
    AppBox box;
    const auto limits = Limits(incoming_timeout);

    ReserveLoopbackPort reserved;
    BOOST_REQUIRE(reserved.port != 0);
    fw::AppNetworkOptions network;
    network.SetLimits(limits)
        .SetListenAddress(inf::ListenAddress{"127.0.0.1", reserved.port})
        .SetLocalAppId("inbound-server");

    fw::AppOptions options;
    options.SetLimits(64, 64, 8, 64)
        .SetShutdownBudget(step_budget)
        .SetLifecycleBudget(std::chrono::milliseconds{3000})
        .SetNetworkOptions(std::move(network));
    for (const auto& s : routed)
        options.AddRoute(
            s, "127.0.0.1:" + std::to_string(reserved.port));

    // 出站注入独立 identity 的宿主：InfraHttpHost 的 client/server 共用同一
    // identity，同宿主自连接会被 infra 握手拒绝；路由门与静态路由仍完整生效。
    auto egress_rt = MakeHost(limits, "inbound-egress");
    auto egress_slot = std::make_shared<std::shared_ptr<fw::RpcEgress>>();
    fw::CoAppSeam seam;
    seam.rpc_send = [egress_slot](std::string_view target,
                                  const inf::rpc::Request& env,
                                  const inf::CallOptions& o)
        -> fw::result<inf::rpc::Response> {
        auto egress = *egress_slot;
        if (!egress)
            return fw::result<inf::rpc::Response>::err(fw::MakeError(
                fw::ErrorCode::RuntimeUnavailable,
                "egress host not started"));
        return egress->Call(target, env, o);
    };

    box.app = framework_test::CreateV2App([&] {
        return std::shared_ptr<v2::CoApp>(
            fw::MakeCoAppForTest(std::move(options), std::move(seam))
                .release());
    });
    BOOST_REQUIRE(box.app);
    box.engine = box.app->engine();
    BOOST_REQUIRE(box.engine);

    register_fn(*box.app);

    box.run = RunApp(*box.app);
    // 应用根共享 Engine 的等价入口 host hook：宿主 Create 装配完成后经
    // HostGate::Pending() 为标记发起唯一一次 Run（生产由
    // bbtco_main_host_hook 承担）。
    std::thread([&] {
        (void)framework_test::RunAppEngineWhenPublished(
            box.engine, inf::ListenAddress{"127.0.0.1", reserved.port});
    }).detach();
    const auto until = Clock::now() + kWait;
    while (box.endpoint.empty()) {
        BOOST_REQUIRE_MESSAGE(!box.run->done.load(),
                              "co lifetime exited before listen, ok="
                                  << framework_test::CoOk(box.run->rc) << " msg="
                                  << box.run->rc.error().message);
        BOOST_REQUIRE(Clock::now() < until);
        box.endpoint = box.app->BoundEndpoint();
        std::this_thread::yield();
    }
    BOOST_REQUIRE(box.app->State() == fw::AppState::Running);

    box.egress_rt = egress_rt;
    BOOST_REQUIRE(framework_test::HostCreateStart(*box.egress_rt));
    BOOST_REQUIRE(box.egress_rt->rpc_client() != nullptr);
    *egress_slot = std::make_shared<fw::RpcEgress>(box.egress_rt);

    auto crt = MakeHost(limits, "inbound-client");
    BOOST_REQUIRE(crt);
    box.client_rt = crt;
    BOOST_REQUIRE(framework_test::HostCreateStart(*box.client_rt));
    BOOST_REQUIRE(box.client_rt->rpc_client() != nullptr);
    box.client = std::make_shared<fw::RpcEgress>(box.client_rt);
    return box;
}

void StopApp(AppBox& box) {
    box.Stop();
}

// 客户端发送一条 V2 Call 并等回包；在协程内调用。
fw::result<inf::rpc::Response> SendRpc(
    const std::shared_ptr<fw::RpcEgress>& egress,
    const std::string& endpoint,
    const inf::rpc::Request& env,
    std::chrono::milliseconds budget) {
    inf::CallOptions o;
    o.deadline = Clock::now() + budget;
    return egress->Call("tcp://" + endpoint, env, o);
}

struct CallState {
    std::optional<fw::result<inf::rpc::Response>> out;
    TestLatch done{1};
};

fw::result<inf::rpc::Response> CallRpc(const AppBox& box, const inf::rpc::Request& env,
                                       std::chrono::milliseconds budget = kWait,
                                       std::shared_ptr<CallState> state = nullptr,
                                       std::optional<std::chrono::milliseconds> wait_budget = std::nullopt) {
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
    if (!succ)
        return fw::result<inf::rpc::Response>::err(
            fw::MakeError(fw::ErrorCode::InternalError, "CallRpc registration failed"));
    if (!state->done.WaitFor(wait))
        return fw::result<inf::rpc::Response>::err(fw::MakeError(
            fw::ErrorCode::InternalError, "CallRpc did not complete"));
    return std::move(*state->out);
}

struct DispatchControl {
    std::shared_ptr<CallState> state;
    std::function<bool()> before_dispatch;
};

fw::result<inf::rpc::Response> DispatchDirect(const std::shared_ptr<v2::CoApp>& app,
                                              const inf::rpc::Request& env,
                                              std::chrono::milliseconds budget = kWait, DispatchControl control = {}) {
    if (!control.state)
        control.state = std::make_shared<CallState>();
    auto state = control.state;
    auto request = env;
    auto hook = std::move(control.before_dispatch);
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [state, app, request, hook]() mutable {
            if (hook && !hook()) {
                state->out.emplace(fw::result<inf::rpc::Response>::err(
                    fw::MakeError(fw::ErrorCode::InternalError, "DispatchDirect gate did not open")));
            } else {
                state->out.emplace(fw::v2::DispatchInboundForTest(*app, request));
            }
            state->done.CountDown();
        },
        succ);
    if (!succ)
        return fw::result<inf::rpc::Response>::err(
            fw::MakeError(fw::ErrorCode::InternalError, "DispatchDirect registration failed"));
    if (!state->done.WaitFor(budget))
        return fw::result<inf::rpc::Response>::err(
            fw::MakeError(fw::ErrorCode::InternalError, "DispatchDirect did not complete"));
    return std::move(*state->out);
}

inf::rpc::Request MakeEnv(std::string service, std::string method,
                          std::string rid, std::int32_t v) {
    inf::rpc::Request env;
    env.SetService(std::move(service))
        .SetMethod(std::move(method))
        .SetRequestId(std::move(rid))
        .SetRequestSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetResponseSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetReply(inf::rpc::ReplyMode::Required)
        .SetTraffic(inf::rpc::TrafficClass::Service)
        .SetPayload(RequestInt(v).payload());
    return env;
}

inf::rpc::Request MakeAcctEnv(std::string method, std::string rid,
                              std::int32_t v) {
    inf::rpc::Request env;
    env.SetService("acct")
        .SetMethod(std::move(method))
        .SetRequestId(std::move(rid))
        .SetRequestSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetResponseSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetReply(inf::rpc::ReplyMode::Required)
        .SetTraffic(inf::rpc::TrafficClass::Actor)
        .SetPayload(RequestAcct("acct-" + std::to_string(v), v).payload());
    return env;
}

fw::result<std::int32_t> DecodeReply(
    const fw::result<inf::rpc::Response>& r) {
    if (!r)
        return fw::result<std::int32_t>::err(r.error());
    auto d = fw::CoRpcReq(r.value().Payload()).Parse<std::int32_t>();
    if (!d)
        return fw::result<std::int32_t>::err(d.error());
    return fw::result<std::int32_t>::ok(d.value());
}

BOOST_AUTO_TEST_SUITE(framework_f1b2)

// T1：入站字段落地与 metadata 边界。
BOOST_AUTO_TEST_CASE(inbound_context_fields_land) {
    g_probe = std::make_shared<ProbeState>(0);
    auto box = StartApp(std::chrono::milliseconds{30000},
                        std::chrono::milliseconds{2000},
                        /*routed*/ {},
                        [](v2::CoApp& app) {
                            BOOST_REQUIRE(
                                framework_test::CoOk(
                                    framework_test::CoEvalVoid([&] {
                                        return bbt_co_await
                                            app.CoInstallService<ProbeSvc>(
                                                ConcurrentOpts());
                                    })));
                        });

    const auto send_at = Clock::now();
    auto env = MakeEnv("probe", "inspect", "t1-a", 7);
    env.SetMetadata({
        {"route.shard", "s7"},
        {"route.request_id", "forged"},
        {"fw.trace_id", "trace-9"},
    });
    auto r = CallRpc(box, env);
    BOOST_REQUIRE(r);
    BOOST_CHECK(r.value().RequestId() == "t1-a");
    auto rep = DecodeReply(r);
    BOOST_REQUIRE(rep);
    BOOST_CHECK(rep.value() == 7);

    const auto snap = g_probe->At(g_probe->before, "t1-a");
    BOOST_CHECK(snap.request_id == "t1-a"); // 非 forged
    BOOST_CHECK(snap.trace_id == "trace-9");
    BOOST_CHECK(snap.peer_principal == ""); // loopback 未认证
    BOOST_CHECK(snap.co_id != 0);
    BOOST_CHECK(snap.deadline > send_at);
    BOOST_CHECK(snap.deadline <= send_at + kWait);

    auto bad1 = MakeEnv("probe", "inspect", "t1-b", 1);
    bad1.SetMetadata({{"evil", "x"}});
    auto r1 = CallRpc(box, bad1);
    BOOST_REQUIRE(!r1);
    BOOST_CHECK(r1.error().code == fw::ErrorCode::ProtocolError);

    auto bad2 = MakeEnv("probe", "inspect", "t1-c", 1);
    bad2.SetMetadata({{"fw.unknown_key", "x"}});
    auto r2 = CallRpc(box, bad2);
    BOOST_REQUIRE(!r2);
    BOOST_CHECK(r2.error().code == fw::ErrorCode::InvalidArgument);

    auto r3 = CallRpc(box, MakeEnv("no.such", "inspect", "t1-d", 1));
    BOOST_REQUIRE(!r3);
    BOOST_CHECK(r3.error().code == fw::ErrorCode::NotFound);
    auto r4 = CallRpc(box, MakeEnv("probe", "no.such", "t1-e", 1));
    BOOST_REQUIRE(!r4);
    BOOST_CHECK(r4.error().code == fw::ErrorCode::NotFound);
    auto bad_schema = MakeEnv("probe", "inspect", "t1-f", 1);
    bad_schema.SetRequestSchema("test.Other/v9");
    auto r5 = CallRpc(box, bad_schema);
    BOOST_REQUIRE(!r5);
    BOOST_CHECK(r5.error().code == fw::ErrorCode::TypeMismatch);

    StopApp(box);
    BOOST_CHECK(framework_test::CoOk(box.run->rc));
}

// T2：并发请求上下文互不串扰 + 确定性跨 worker 恢复。
BOOST_AUTO_TEST_CASE(concurrent_context_isolated_cross_worker) {
    constexpr int kN = 8;
    g_probe = std::make_shared<ProbeState>(kN);
    auto box = StartApp(std::chrono::milliseconds{30000},
                        std::chrono::milliseconds{2000},
                        /*routed*/ {},
                        [](v2::CoApp& app) {
                            BOOST_REQUIRE(
                                framework_test::CoOk(
                                    framework_test::CoEvalVoid([&] {
                                        return bbt_co_await
                                            app.CoInstallService<ProbeSvc>(
                                                ConcurrentOpts());
                                    })));
                        });

    TestLatch done{kN};
    std::vector<std::optional<fw::result<inf::rpc::Response>>> results(kN);
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
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));

    const std::string target = g_probe->before.begin()->first;
    const auto target_tid = g_probe->before.at(target).tid;
    std::shared_ptr<Spinner> held;
    const auto spinner_deadline = Clock::now() + kWait;
    for (int i = 0; i < 64 && !held && Clock::now() < spinner_deadline;
         ++i) {
        auto s = LaunchSpinner();
        while (!s->spinning.load(std::memory_order_acquire) &&
               Clock::now() < spinner_deadline)
            std::this_thread::yield();
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

    g_probe->CompleteAll();
    const bool resumed_elsewhere = g_probe->WaitEvent("resumed:" + target, kWait);
    held->release.store(true, std::memory_order_release);
    BOOST_REQUIRE(resumed_elsewhere);
    BOOST_REQUIRE(done.WaitFor(kWait));

    int switched = 0;
    for (int i = 0; i < kN; ++i) {
        const std::string rid = "t2-" + std::to_string(i);
        BOOST_REQUIRE_MESSAGE(results[i].has_value() && results[i].value(),
                              "reply failed: " << rid);
        BOOST_CHECK(results[i].value().value().RequestId() == rid);
        auto rep = DecodeReply(results[i].value());
        BOOST_REQUIRE(rep);
        BOOST_CHECK(rep.value() == i);

        const auto b = g_probe->At(g_probe->before, rid);
        const auto a = g_probe->At(g_probe->after, rid);
        BOOST_CHECK(b.request_id == rid);
        BOOST_CHECK(a.request_id == rid);
        BOOST_CHECK(b.co_id != 0);
        BOOST_CHECK(a.co_id == b.co_id);
        BOOST_CHECK(b.peer_principal == "");
        BOOST_CHECK(a.peer_principal == "");
        BOOST_CHECK(a.deadline == b.deadline);
        if (a.tid != b.tid)
            ++switched;
    }
    const auto bt = g_probe->At(g_probe->before, target);
    const auto at = g_probe->At(g_probe->after, target);
    BOOST_CHECK(at.tid != bt.tid);
    BOOST_CHECK(switched >= 1);
    BOOST_TEST_MESSAGE("cross-worker switched=" << switched << "/" << kN);

    StopApp(box);
    BOOST_CHECK(framework_test::CoOk(box.run->rc));
}

// T3：非受管协程 → InvalidContext 端到端；受管路径作正对照。
BOOST_AUTO_TEST_CASE(unmanaged_coroutine_invalid_context_end_to_end) {
    g_probe = std::make_shared<ProbeState>(0);
    auto box = StartApp(std::chrono::milliseconds{30000},
                        std::chrono::milliseconds{2000},
                        /*routed*/ {"echo"},
                        [](v2::CoApp& app) {
                            BOOST_REQUIRE(
                                framework_test::CoOk(
                                    framework_test::CoEvalVoid([&] {
                                        return bbt_co_await
                                            app.CoInstallService<EchoSvc>(
                                                ConcurrentOpts());
                                    })));
                            BOOST_REQUIRE(
                                framework_test::CoOk(
                                    framework_test::CoEvalVoid([&] {
                                        return bbt_co_await
                                            app.CoInstallService<CallerSvc>(
                                                ConcurrentOpts());
                                    })));
                        });

    auto r = CallRpc(box, MakeEnv("caller", "spawn_dial", "t3-a", 1));
    BOOST_REQUIRE(r);
    auto rep = DecodeReply(r);
    BOOST_REQUIRE(rep);
    BOOST_CHECK(rep.value() ==
                static_cast<std::int32_t>(fw::ErrorCode::InvalidContext));
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

    auto svc = framework_test::ServiceInstance(*box.app, "caller");
    BOOST_REQUIRE(svc);
    auto caller = std::static_pointer_cast<CallerSvc>(svc);
    TestLatch l{1};
    std::optional<fw::result<fw::CoRpcResp>> direct;
    bbtco[caller, &direct, &l]() {
        direct.emplace(caller->DirectDial(RequestInt(5)));
        l.CountDown();
    };
    BOOST_REQUIRE(l.WaitFor(kWait));
    BOOST_REQUIRE(direct.has_value());
    BOOST_REQUIRE(!direct.value());
    BOOST_CHECK(direct.value().error().code ==
                fw::ErrorCode::InvalidContext);

    auto ok = CallRpc(box, MakeEnv("caller", "dial", "t3-c", 42));
    BOOST_REQUIRE(ok);
    auto rep2 = DecodeReply(ok);
    BOOST_REQUIRE(rep2);
    BOOST_CHECK(rep2.value() == 42);
    BOOST_CHECK(ok.value().RequestId() == "t3-c");

    StopApp(box);
    BOOST_CHECK(framework_test::CoOk(box.run->rc));
}

// T4：出站期限缺省继承、显式取 min、过期在 I/O 前 TimedOut。
BOOST_AUTO_TEST_CASE(outbound_deadline_min_and_expired_before_io) {
    g_probe = std::make_shared<ProbeState>(0);
    auto box = StartApp(std::chrono::milliseconds{300},
                        std::chrono::milliseconds{2000},
                        /*routed*/ {"echo"},
                        [](v2::CoApp& app) {
                            BOOST_REQUIRE(
                                framework_test::CoOk(
                                    framework_test::CoEvalVoid([&] {
                                        return bbt_co_await
                                            app.CoInstallService<EchoSvc>(
                                                ConcurrentOpts());
                                    })));
                            BOOST_REQUIRE(
                                framework_test::CoOk(
                                    framework_test::CoEvalVoid([&] {
                                        return bbt_co_await
                                            app.CoInstallService<RelaySvc>(
                                                ConcurrentOpts());
                                    })));
                        });

    auto r1 = CallRpc(box, MakeEnv("relay", "call_default", "t4-d", 1));
    BOOST_REQUIRE(r1);
    BOOST_CHECK(DecodeReply(r1).value() == 1);

    auto r2 = CallRpc(box, MakeEnv("relay", "call_longer", "t4-l", 2));
    BOOST_REQUIRE(r2);
    BOOST_CHECK(DecodeReply(r2).value() == 2);

    auto r3 = CallRpc(box, MakeEnv("relay", "call_expired", "t4-x", 3),
                      std::chrono::milliseconds{5000});
    BOOST_REQUIRE(!r3);
    BOOST_CHECK(r3.error().code == fw::ErrorCode::TimedOut);

    StopApp(box);
    BOOST_CHECK(framework_test::CoOk(box.run->rc));
}

// T5：宿主 Close 收口——逻辑封口 + 物理观察；重复 Close 幂等。
BOOST_AUTO_TEST_CASE(sync_close_on_control_thread) {
    g_probe = std::make_shared<ProbeState>(0);
    auto box = StartApp(std::chrono::milliseconds{30000},
                        std::chrono::milliseconds{2000},
                        /*routed*/ {},
                        [](v2::CoApp& app) {
                            BOOST_REQUIRE(
                                framework_test::CoOk(
                                    framework_test::CoEvalVoid([&] {
                                        return bbt_co_await
                                            app.CoInstallService<ProbeSvc>(
                                                ConcurrentOpts());
                                    })));
                        });
    BOOST_REQUIRE(box.engine);

    auto host2 = std::make_shared<fw::InfraHttpHost>(
        Limits(std::chrono::milliseconds{30000}),
        inf::ListenAddress{"127.0.0.1", 0}, "inbound-close-probe");
    BOOST_REQUIRE(framework_test::HostCreateStart(*host2));
    auto rt2 = host2->egress_engine();
    BOOST_REQUIRE(rt2);
    BOOST_CHECK(rt2->Phase() == inf::net::EnginePhase::Running);

    BOOST_CHECK(framework_test::CoOk(framework_test::HostClose(*host2)));
    BOOST_CHECK(rt2->Phase() == inf::net::EnginePhase::Stopped);
    BOOST_CHECK(host2->egress_engine() == nullptr);
    BOOST_CHECK(host2->http_server() == nullptr);
    BOOST_CHECK(framework_test::CoOk(framework_test::HostClose(*host2)));

    StopApp(box);
    BOOST_CHECK(framework_test::CoOk(box.run->rc));
}

// T6：真实在途 handler 未排空 → StopIncomplete → 迟到收尾。
BOOST_AUTO_TEST_CASE(handler_drain_shutdown_late) {
    g_probe = std::make_shared<ProbeState>(1);
    auto box = StartApp(std::chrono::milliseconds{30000},
                        std::chrono::milliseconds{300},
                        /*routed*/ {},
                        [](v2::CoApp& app) {
                            BOOST_REQUIRE(
                                framework_test::CoOk(
                                    framework_test::CoEvalVoid([&] {
                                        return bbt_co_await
                                            app.CoInstallService<ProbeSvc>(
                                                ConcurrentOpts());
                                    })));
                        });

    auto inflight = MakeEnv("probe", "wait_echo", "t6-h", 9);
    TestLatch inflight_done{1};
    std::optional<fw::result<inf::rpc::Response>> inflight_res;
    bbtco[&box, &inflight, &inflight_res, &inflight_done]() {
        inflight_res.emplace(
            SendRpc(box.client, box.endpoint, inflight, kWait));
        inflight_done.CountDown();
    };
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));

    box.app->RequestShutdown();
    const auto until = Clock::now() + kWait;
    while (box.app->State() != fw::AppState::StopIncomplete) {
        BOOST_REQUIRE(Clock::now() < until);
        std::this_thread::yield();
    }
    {
        const auto done_until = Clock::now() + kWait;
        while (!box.run->done.load(std::memory_order_acquire)) {
            BOOST_REQUIRE(Clock::now() < done_until);
            std::this_thread::yield();
        }
    }
    BOOST_CHECK(!framework_test::CoOk(box.run->rc));

    g_probe->CompleteAll();
    BOOST_REQUIRE(inflight_done.WaitFor(kWait));
    BOOST_CHECK(g_probe->At(g_probe->after, "t6-h").co_id != 0);
    JoinRun(box.run);
    BOOST_CHECK(framework_test::CoOk(framework_test::CoStop(*box.app)));
    BOOST_CHECK(box.app->State() == fw::AppState::Stopped);
    BOOST_CHECK(box.app->PendingCleanup().empty());

    if (box.client_rt)
        (void)framework_test::HostClose(*box.client_rt);
}

// T9：DispatchDirect 超预算失败路径的 CoApp 所有权（免 UAF）。
BOOST_AUTO_TEST_CASE(dispatch_direct_timeout_late_task_keeps_app_alive) {
    using std::chrono::milliseconds;
    auto probe = std::make_shared<ProbeState>(0);
    g_probe = probe;

    auto gate = std::make_shared<PreDispatchGate>();
    struct Releaser {
        std::shared_ptr<PreDispatchGate> g;
        ~Releaser() {
            if (g)
                g->Open();
        }
    } releaser{gate};

    auto box =
        StartApp(milliseconds{30000}, milliseconds{2000},
                 /*routed*/ {}, [](v2::CoApp& app) {
                     BOOST_REQUIRE(framework_test::CoOk(
                         framework_test::CoEvalVoid([&] {
                             return bbt_co_await
                                 app.CoInstallService<ProbeSvc>(
                                     ConcurrentOpts());
                         })));
                 });
    std::weak_ptr<v2::CoApp> app_weak = box.app;

    DispatchControl control;
    control.state = std::make_shared<CallState>();
    control.before_dispatch = [gate] {
        return gate->Park();
    };

    auto expired_gate = std::make_shared<PreDispatchGate>();
    DispatchControl expired_control;
    const auto expired_at = Clock::now();
    expired_control.before_dispatch = [expired_gate, expired_at] {
        return expired_gate->Park(expired_at);
    };
    auto rejected = DispatchDirect(box.app, MakeEnv("probe", "inspect", "t9-expired", 5), kWait, expired_control);
    BOOST_REQUIRE(!rejected);
    BOOST_CHECK(rejected.error().code == fw::ErrorCode::InternalError);
    BOOST_REQUIRE(expired_gate->Arrived(kWait));
    BOOST_CHECK(probe->inspect_calls.load() == 0);

    auto r = DispatchDirect(box.app, MakeEnv("probe", "inspect", "t9-a", 5), milliseconds{300}, control);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == fw::ErrorCode::InternalError);
    BOOST_REQUIRE(gate->Arrived(kWait));
    BOOST_CHECK(!app_weak.expired());

    StopApp(box);
    BOOST_CHECK(framework_test::CoOk(box.run->rc));
    box.app.reset();
    g_probe.reset();
    BOOST_REQUIRE(!app_weak.expired());

    gate->Open();
    BOOST_REQUIRE(control.state->done.WaitFor(kWait));
    const auto until = Clock::now() + kWait;
    while (!app_weak.expired()) {
        BOOST_REQUIRE(Clock::now() < until);
        std::this_thread::yield();
    }
    BOOST_REQUIRE(control.state->out.has_value());
    BOOST_REQUIRE(!control.state->out.value());
    BOOST_CHECK(control.state->out.value().error().code == fw::ErrorCode::RuntimeUnavailable);
    BOOST_CHECK(probe->inspect_calls.load() == 0);
}

// T10：CoApp 生命周期对象的共享寿命（run 收束期间并发读取）。
BOOST_AUTO_TEST_CASE(lifecycle_readers_survive_run_teardown) {
    g_probe = std::make_shared<ProbeState>(0);
    auto box =
        StartApp(std::chrono::milliseconds{30000}, std::chrono::milliseconds{2000},
                 /*routed*/ {}, [](v2::CoApp& app) {
                     BOOST_REQUIRE(framework_test::CoOk(
                         framework_test::CoEvalVoid([&] {
                             return bbt_co_await
                                 app.CoInstallService<ProbeSvc>(
                                     ConcurrentOpts());
                         })));
                 });

    std::atomic<bool> stop{false};
    std::atomic<long> reads{0};
    TestLatch reader_ran{1};
    std::thread reader([&] {
        bool first = true;
        while (!stop.load(std::memory_order_acquire)) {
            (void)box.app->State();
            (void)box.app->PendingCleanup();
            (void)box.app->LifecycleFailures();
            box.app->RequestShutdown();
            if (first) {
                reader_ran.CountDown();
                first = false;
            }
            reads.fetch_add(1, std::memory_order_relaxed);
        }
    });
    BOOST_REQUIRE(reader_ran.WaitFor(kWait));

    StopApp(box);
    BOOST_CHECK(framework_test::CoOk(box.run->rc));
    BOOST_CHECK(reads.load(std::memory_order_relaxed) > 0);

    stop.store(true, std::memory_order_release);
    reader.join();

    BOOST_CHECK(box.app->State() == fw::AppState::Stopped);
    BOOST_CHECK(box.app->PendingCleanup().empty());
}

// T11：CallRpc 超预算返回后任务迟到完成（无栈借用）。
BOOST_AUTO_TEST_CASE(callrpc_late_completion_after_timeout) {
    using std::chrono::milliseconds;
    g_probe = std::make_shared<ProbeState>(1);
    auto box =
        StartApp(milliseconds{30000}, milliseconds{2000},
                 /*routed*/ {}, [](v2::CoApp& app) {
                     BOOST_REQUIRE(framework_test::CoOk(
                         framework_test::CoEvalVoid([&] {
                             return bbt_co_await
                                 app.CoInstallService<ProbeSvc>(
                                     ConcurrentOpts());
                         })));
                 });

    auto state = std::make_shared<CallState>();
    auto r = CallRpc(box, MakeEnv("probe", "wait_echo", "t11-late", 21),
                     /*budget*/ milliseconds{5000}, state,
                     /*wait_budget*/ milliseconds{150});
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == fw::ErrorCode::InternalError);
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));
    BOOST_CHECK(!box.run->done.load());

    g_probe->CompleteAll();
    BOOST_REQUIRE(state->done.WaitFor(kWait));
    BOOST_REQUIRE(state->out.has_value());
    BOOST_REQUIRE(state->out.value());
    BOOST_CHECK(state->out.value().value().RequestId() == "t11-late");
    auto rep = DecodeReply(state->out.value());
    BOOST_REQUIRE(rep);
    BOOST_CHECK(rep.value() == 21);

    auto again = CallRpc(box, MakeEnv("probe", "inspect", "t11-after", 3));
    BOOST_REQUIRE(again);
    auto rep2 = DecodeReply(again);
    BOOST_REQUIRE(rep2);
    BOOST_CHECK(rep2.value() == 3);

    StopApp(box);
    BOOST_CHECK(framework_test::CoOk(box.run->rc));
}

// 宿主在收口等待里抛异常：异常不得越过生命周期入口，收口未完成 →
// StopIncomplete、owner/资源保留、失败项可观察。
class ThrowingDrainHost final : public fw::INetworkHost {
public:
    TestLatch started{1};
    fw::result<void> Create() override { return fw::result<void>::ok(); }
    bbt_co_async(fw::result<void>) Start(const co::WaitOptions&) override {
        started.CountDown();
        return fw::result<void>::ok();
    }
    void StopAccepting() noexcept override {}
    bbt_co_async(fw::result<void>) WaitHandlersDone(
        const co::WaitOptions&) override {
        throw std::runtime_error("injected WaitHandlersDone throw");
    }
    bbt_co_async(fw::result<void>) Close(
        const bbt::infra::CloseOptions&) override {
        return fw::result<void>::ok();
    }
    bbt_co_async(fw::result<void>) WaitPhysicalReleased(
        const co::WaitOptions&) override {
        return fw::result<void>::ok();
    }
    std::optional<bbt::infra::Error> CloseReason() const override {
        return std::nullopt;
    }
};

BOOST_AUTO_TEST_CASE(lifecycle_stop_exception_contained) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 6;
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;

    auto options = fw::AppOptions{}
        .SetLimits(64, 64, 8, 64)
        .SetShutdownBudget(std::chrono::milliseconds{2000})
        .SetLifecycleBudget(std::chrono::milliseconds{2000});
    auto host = std::make_shared<ThrowingDrainHost>();
    std::unique_ptr<v2::CoApp> app;
    bool ran = framework_test::RunInCoTask([&] {
        app = fw::MakeCoAppForTest(options, fw::CoAppSeam{host, {}});
    });
    BOOST_REQUIRE(ran);
    BOOST_REQUIRE(app);

    auto started = framework_test::CoStart(*app);
    BOOST_REQUIRE(framework_test::CoOk(started));
    BOOST_REQUIRE(host->started.WaitFor(kWait));

    app->RequestShutdown();
    const auto stopped = framework_test::CoStop(*app);
    BOOST_REQUIRE(!framework_test::CoOk(stopped));
    BOOST_CHECK(stopped.error().code == fw::ErrorCode::InternalError);
    BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);
}

// T13：V2 RPC admission 入站链路闭环 → handler 内嵌套 Co RPC → CoStop 收口。
BOOST_AUTO_TEST_CASE(rpc_admission_nested_co_rpc_close) {
    g_probe = std::make_shared<ProbeState>(0);
    auto box = StartApp(std::chrono::milliseconds{30000},
                        std::chrono::milliseconds{2000},
                        /*routed*/ {"echo", "relay"},
                        [](v2::CoApp& app) {
                            BOOST_REQUIRE(
                                framework_test::CoOk(
                                    framework_test::CoEvalVoid([&] {
                                        return bbt_co_await
                                            app.CoInstallService<EchoSvc>(
                                                ConcurrentOpts());
                                    })));
                            BOOST_REQUIRE(
                                framework_test::CoOk(
                                    framework_test::CoEvalVoid([&] {
                                        return bbt_co_await
                                            app.CoInstallService<RelaySvc>(
                                                ConcurrentOpts());
                                    })));
                        });

    auto r = CallRpc(box, MakeEnv("relay", "call_default", "t13-a", 21));
    BOOST_REQUIRE(r);
    auto rep = DecodeReply(r);
    BOOST_REQUIRE(rep);
    BOOST_CHECK_EQUAL(rep.value(), 21);
    auto svc = box.app->FindService("relay");
    BOOST_REQUIRE(svc);

    StopApp(box);
    BOOST_CHECK(framework_test::CoOk(box.run->rc));
    BOOST_CHECK(box.app->State() == fw::AppState::Stopped);
    BOOST_CHECK(box.app->PendingCleanup().empty());
}

// "relay"（HTTP 链路专用）：Concurrent；业务在 handler 内发起嵌套 Co RPC。
class HttpRelaySvc final : public v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "relay";
    explicit HttpRelaySvc(fw::detail::ConstructKey k) : v2::CoService<>(k) {}
    fw::CoRpcResp CallDefault(fw::CoRpcReq req) {
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        if (g_probe)
            g_probe->Snapshot(g_probe->before, ctx.value()->request_id,
                              SnapCtx(*ctx.value()));
        auto response = this->call("echo", "ping", req);
        if (!response)
            return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&HttpRelaySvc::CallDefault>("call_default"));
protected:
    ~HttpRelaySvc() noexcept override = default;
    friend class fw::detail::Registry;
};

// ── 真实 HTTP 链路夹具 ──
//
// 公开 HTTP 路由经 AppOptions::AddHttpRoute 装配，由 framework 的
// InstallHttpAdmission 校验协议并构造 V2 请求，进入真实 InboundDispatcher
// 与 Service；本用例使用真实 HTTP/1.1 wire，不使用测试派发 seam。
//
// 宿主 Create 拒绝 HTTP 与 RPC 共用一个 listener，故应用宿主只装 HTTP
// handler；嵌套 Co RPC 的对端由互异 identity 的第二个真实宿主承担。

struct HttpProbe {
    void Record(std::string m, std::string t, std::string b) {
        std::lock_guard<std::mutex> lk(mtx);
        ++calls;
        method = std::move(m);
        target = std::move(t);
        body = std::move(b);
    }
    int Calls() {
        std::lock_guard<std::mutex> lk(mtx);
        return calls;
    }
    std::string Method() {
        std::lock_guard<std::mutex> lk(mtx);
        return method;
    }
    std::string Target() {
        std::lock_guard<std::mutex> lk(mtx);
        return target;
    }
    std::string Body() {
        std::lock_guard<std::mutex> lk(mtx);
        return body;
    }

    std::mutex mtx;
    int calls{0};
    std::string method;
    std::string target;
    std::string body;
};

struct PeerProbe {
    void Record(std::string s, std::string m, std::string rid, std::int32_t v) {
        std::lock_guard<std::mutex> lk(mtx);
        ++calls;
        service = std::move(s);
        method = std::move(m);
        request_id = std::move(rid);
        value = v;
    }
    int Calls() {
        std::lock_guard<std::mutex> lk(mtx);
        return calls;
    }
    std::string Service() {
        std::lock_guard<std::mutex> lk(mtx);
        return service;
    }
    std::string Method() {
        std::lock_guard<std::mutex> lk(mtx);
        return method;
    }
    std::string RequestId() {
        std::lock_guard<std::mutex> lk(mtx);
        return request_id;
    }
    std::int32_t Value() {
        std::lock_guard<std::mutex> lk(mtx);
        return value;
    }

    std::mutex mtx;
    int calls{0};
    std::string service;
    std::string method;
    std::string request_id;
    std::int32_t value{0};
};

struct HttpAppBox {
    std::shared_ptr<fw::InfraHttpHost> peer;
    std::shared_ptr<fw::InfraHttpHost> egress_rt;
    std::shared_ptr<fw::RpcEgress> egress;
    std::shared_ptr<fw::InfraHttpHost> host;
    std::shared_ptr<v2::CoApp> app;
    std::shared_ptr<fw::InfraHttpHost> client_rt;
    std::string endpoint;
    std::unique_ptr<RunHandle> run;

    HttpAppBox() = default;
    ~HttpAppBox() { Stop(); }
    HttpAppBox(HttpAppBox&&) = default;
    HttpAppBox& operator=(HttpAppBox&&) = default;
    HttpAppBox(const HttpAppBox&) = delete;
    HttpAppBox& operator=(const HttpAppBox&) = delete;

    void Stop() {
        if (client_rt)
            (void)framework_test::HostClose(*client_rt);
        if (app)
            app->RequestShutdown();
        if (run && run->th.joinable())
            run->th.join();
        if (egress_rt)
            (void)framework_test::HostClose(*egress_rt);
        if (peer)
            (void)framework_test::HostClose(*peer);
    }
};

HttpAppBox StartHttpApp(std::chrono::milliseconds incoming_timeout,
                        std::chrono::milliseconds step_budget,
                        const std::shared_ptr<HttpProbe>& http_probe,
                        const std::shared_ptr<PeerProbe>& peer_probe) {
    HttpAppBox box;
    const auto limits = Limits(incoming_timeout);

    g_bbt_coroutine_config->m_cfg_static_thread_num = 9;
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
    framework_test::EnsureRuntime();

    box.peer = MakeHost(limits, "http-peer");
    box.peer->InstallRpcHandler(
        [peer_probe](const inf::rpc::Request& request,
                     const std::function<void()>& dispatched)
            -> fw::result<inf::rpc::Response> {
            dispatched();
            auto value = fw::CoRpcReq(request.Payload()).Parse<std::int32_t>();
            if (!value)
                return fw::result<inf::rpc::Response>::err(value.error());
            peer_probe->Record(request.Service(), request.Method(),
                               request.RequestId(), value.value());
            auto reply = inf::rpc::Response::FromRequest(request);
            reply.SetPayload(
                fw::CoRpcResp::From(value.value() + 1000).payload());
            return fw::result<inf::rpc::Response>::ok(std::move(reply));
        });
    BOOST_REQUIRE(framework_test::HostCreateStart(*box.peer));
    BOOST_REQUIRE(!box.peer->bound_endpoint().empty());

    box.host = MakeHost(limits, "http-app");
    fw::AppNetworkOptions network;
    network.SetLimits(limits)
        .SetListenAddress(inf::ListenAddress{"127.0.0.1", 0})
        .SetLocalAppId("http-app");
    fw::AppOptions opts;
    opts.SetLimits(64, 64, 8, 64)
        .SetShutdownBudget(step_budget)
        .SetLifecycleBudget(std::chrono::milliseconds{3000})
        .SetNetworkOptions(std::move(network))
        .AddHttpRoute(fw::HttpServiceRoute{
            "/relay/call_default", "relay", "call_default",
            std::string(fw::kCoRpcPositionalSchema),
            std::string(fw::kCoRpcPositionalSchema)})
        .AddRoute("echo", box.peer->bound_endpoint());

    auto host = box.host;
    box.egress_rt = MakeHost(limits, "http-egress");
    box.egress = std::make_shared<fw::RpcEgress>(box.egress_rt);

    fw::CoAppSeam seam;
    seam.host = box.host;
    seam.rpc_send =
        [host, egress = box.egress](std::string_view target,
                                    const inf::rpc::Request& env,
                                    const inf::CallOptions& o)
        -> fw::result<inf::rpc::Response> {
        if (g_probe)
            g_probe->egress_calls.fetch_add(1, std::memory_order_acq_rel);
        std::string ep(target);
        constexpr std::string_view kScheme = "tcp://";
        if (ep.rfind(kScheme, 0) == 0)
            ep.erase(0, kScheme.size());
        if (ep.empty())
            return fw::result<inf::rpc::Response>::err(fw::MakeError(
                fw::ErrorCode::RuntimeUnavailable,
                "egress endpoint not installed"));
        return egress->Call("tcp://" + ep, env, o);
    };

    bool created = framework_test::RunInCoTask([&] {
        box.app = std::shared_ptr<v2::CoApp>(
            fw::MakeCoAppForTest(opts, std::move(seam)).release());
        auto r = bbt_co_await box.app->CoInstallService<HttpRelaySvc>(
            ConcurrentOpts());
        if (!r)
            throw std::runtime_error("install failed: " + r.error().message);
    });
    BOOST_REQUIRE(created);
    BOOST_REQUIRE(box.app);
    box.run = RunApp(*box.app);
    // seam 宿主的等价入口 host hook：Create 在 CoStart 内执行后自有 Engine
    // 就位，native 测试线程在有界轮询里发起唯一一次 Run（生产由
    // bbtco_main_host_hook 承担；CoAppSeam 的 start_host_engine 字段当前
    // 无消费者，本辅助即测试侧同一角色）。
    std::thread([&] {
        (void)framework_test::RunSeamHostEngineWhenReady(*host);
    }).detach();
    const auto until = Clock::now() + kWait;
    while (box.endpoint.empty()) {
        BOOST_REQUIRE_MESSAGE(!box.run->done.load(),
                              "co lifetime exited before listen, ok="
                                  << framework_test::CoOk(box.run->rc) << " msg="
                                  << box.run->rc.error().message);
        BOOST_REQUIRE(Clock::now() < until);
        box.endpoint = box.host->bound_endpoint();
        std::this_thread::yield();
    }
    BOOST_REQUIRE(box.app->State() == fw::AppState::Running);

    box.client_rt = MakeHost(limits, "http-client");
    BOOST_REQUIRE(framework_test::HostCreateStart(*box.client_rt));
    BOOST_REQUIRE(box.client_rt->http_client() != nullptr);
    BOOST_REQUIRE(framework_test::HostCreateStart(*box.egress_rt));
    BOOST_REQUIRE(box.egress_rt->rpc_client() != nullptr);
    return box;
}

// T14：真实 HTTP wire 链路闭环。
BOOST_AUTO_TEST_CASE(http_wire_nested_co_rpc_close) {
    using std::chrono::milliseconds;
    g_probe = std::make_shared<ProbeState>(0);
    auto http_probe = std::make_shared<HttpProbe>();
    auto peer_probe = std::make_shared<PeerProbe>();
    auto box = StartHttpApp(milliseconds{30000}, milliseconds{2000},
                            http_probe, peer_probe);

    auto server = box.host->http_server();
    BOOST_REQUIRE_MESSAGE(server != nullptr,
                          "HTTP 装配必须建立 infra http::Server");
    BOOST_CHECK_EQUAL(server->Stats().requests, 0);

    const auto payload = RequestInt(21).payload();
    const std::string body(payload.begin(), payload.end());
    struct HttpResult {
        std::optional<fw::result<inf::http::Response>> out;
        TestLatch done{1};
    };
    auto result = std::make_shared<HttpResult>();
    auto client = box.client_rt->http_client();
    BOOST_REQUIRE(client != nullptr);
    const auto endpoint = box.endpoint;
    bool registered = false;
    g_scheduler->RegistCoroutineTask(
        [result, client, endpoint, body] {
            inf::CallOptions call;
            call.deadline = Clock::now() + std::chrono::seconds{10};
            result->out.emplace(bbt_co_await client->Post(
                "http://" + endpoint + "/relay/call_default",
                inf::http::RequestBody{body, "application/x-protobuf"},
                inf::http::RequestOptions{
                    {{"X-Request-Id", "http-wire-1"}}, {}, std::nullopt},
                call));
            result->done.CountDown();
        },
        registered);
    BOOST_REQUIRE(registered);
    BOOST_REQUIRE(result->done.WaitFor(kWait));
    BOOST_REQUIRE(result->out.has_value());
    BOOST_REQUIRE_MESSAGE(bool(result->out.value()),
                          "HTTP 请求必须在真实 wire 上完成，不得以传输错误冒充命中");
    auto response = result->out.value().value();

    BOOST_CHECK_EQUAL(response.status_code, 200);
    BOOST_CHECK_EQUAL(server->Stats().requests, 1);

    BOOST_CHECK_EQUAL(g_probe->At(g_probe->before, "http-wire-1").request_id,
                      "http-wire-1");
    BOOST_CHECK_EQUAL(g_probe->egress_calls.load(), 1);
    BOOST_CHECK_EQUAL(peer_probe->Calls(), 1);
    BOOST_CHECK_EQUAL(peer_probe->Service(), "echo");
    BOOST_CHECK_EQUAL(peer_probe->Method(), "ping");
    BOOST_CHECK(!peer_probe->RequestId().empty());
    BOOST_CHECK(peer_probe->RequestId() != "http-wire-1");
    BOOST_CHECK_EQUAL(peer_probe->Value(), 21);

    auto decoded = fw::CoRpcReq(
                       std::vector<std::uint8_t>(response.body.begin(), response.body.end()))
                       .Parse<std::int32_t>();
    BOOST_REQUIRE(decoded);
    BOOST_CHECK_EQUAL(decoded.value(), 1021);

    auto send_wire = [client, endpoint, body](std::string request_id,
                                               std::string content_type) {
        std::optional<fw::result<inf::http::Response>> wire;
        const bool ran = framework_test::RunInCoTask([&] {
            inf::http::RequestOptions request_options;
            request_options.headers.emplace_back("X-Request-Id",
                                                 std::move(request_id));
            inf::CallOptions call;
            call.deadline = Clock::now() + std::chrono::seconds{10};
            wire.emplace(bbt_co_await client->Post(
                "http://" + endpoint + "/relay/call_default",
                inf::http::RequestBody{body, std::move(content_type)},
                request_options, call));
        });
        BOOST_REQUIRE(ran);
        BOOST_REQUIRE(wire.has_value());
        return std::move(*wire);
    };
    const auto wrong_type = send_wire("http-wire-wrong-type", "text/plain");
    BOOST_REQUIRE(wrong_type);
    BOOST_CHECK_EQUAL(wrong_type.value().status_code, 415);
    const auto missing_type = send_wire("http-wire-missing-type", "");
    BOOST_REQUIRE(missing_type);
    BOOST_CHECK_EQUAL(missing_type.value().status_code, 415);
    BOOST_CHECK_EQUAL(g_probe->egress_calls.load(), 1);
    BOOST_CHECK_EQUAL(peer_probe->Calls(), 1);
    BOOST_CHECK_EQUAL(server->Stats().requests, 3);

    BOOST_TEST_MESSAGE("http wire: "
                       << http_probe->Method() << " " << http_probe->Target()
                       << " -> " << response.status_code << " body="
                       << decoded.value() << " peer="
                       << peer_probe->Service() << "."
                       << peer_probe->Method() << " peer_calls="
                       << peer_probe->Calls() << " server_requests="
                       << server->Stats().requests);

    box.Stop();
    BOOST_CHECK(framework_test::CoOk(box.run->rc));
    BOOST_CHECK(box.app->State() == fw::AppState::Stopped);
    BOOST_CHECK(box.app->PendingCleanup().empty());
    BOOST_CHECK(box.host->http_server() == nullptr);
    BOOST_CHECK(box.host->egress_engine() == nullptr);
}

// T15：同一 HTTP 配置下的启动失败回滚。
BOOST_AUTO_TEST_CASE(http_host_start_failure_rolls_back) {
    using std::chrono::milliseconds;
    g_probe = std::make_shared<ProbeState>(0);

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(fd >= 0);
    struct FdGuard {
        int fd;
        ~FdGuard() {
            if (fd >= 0)
                ::close(fd);
        }
    } guard{fd};
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    BOOST_REQUIRE(::bind(fd, reinterpret_cast<sockaddr*>(&addr),
                         sizeof(addr)) == 0);
    BOOST_REQUIRE(::listen(fd, 1) == 0);
    socklen_t len = sizeof(addr);
    BOOST_REQUIRE(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr),
                                &len) == 0);
    const std::uint16_t busy_port = ntohs(addr.sin_port);
    BOOST_REQUIRE(busy_port != 0);

    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
    framework_test::EnsureRuntime();

    const auto limits = Limits(milliseconds{30000});
    auto handler = [](inf::IncomingCallContext, inf::http::IncomingRequest)
        -> fw::result<inf::http::Response> {
        return fw::result<inf::http::Response>::ok(
            inf::http::Response{200, {}, "ok"});
    };

    auto opts = fw::AppOptions{}
        .SetLimits(64, 64, 8, 64)
        .SetShutdownBudget(milliseconds{2000})
        .SetLifecycleBudget(milliseconds{2000});

    auto host = std::make_shared<fw::InfraHttpHost>(
        limits, inf::ListenAddress{"127.0.0.1", busy_port},
        "http-fail");
    host->InstallHandler(handler);
    fw::CoAppSeam seam;
    seam.host = host;
    std::unique_ptr<v2::CoApp> app;
    bool ran = framework_test::RunInCoTask([&] {
        app = fw::MakeCoAppForTest(opts, std::move(seam));
    });
    BOOST_REQUIRE(ran);
    BOOST_REQUIRE(app);
    // 等价入口 host hook：seam 宿主自有 Engine 的 Run 在 native 线程执行一次。
    std::thread([&] {
        (void)framework_test::RunSeamHostEngineWhenReady(*host);
    }).detach();

    const auto started = framework_test::CoStart(*app);
    BOOST_REQUIRE_MESSAGE(!framework_test::CoOk(started),
                          "被占用端口必须让 HTTP 宿主启动失败");
    const auto reason = host->CloseReason();
    BOOST_REQUIRE(reason.has_value());
    BOOST_CHECK(!host->bound_address().has_value());
    BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);
    auto cleanup_options = bbt::infra::CloseOptions::Immediate(
        fw::MakeError(fw::ErrorCode::Closed, "test cleanup retry"));
    cleanup_options.deadline = Clock::now() + std::chrono::seconds{5};
    cleanup_options.wait_release = true;
    const auto cleanup = framework_test::CoStop(*app, cleanup_options);
    BOOST_CHECK_MESSAGE(!framework_test::CoOk(cleanup),
                        "物理 Run 未结束时 CoStop 必须如实报告未完成收口");
    BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);

    auto control_host = MakeHost(limits, "http-fail-control");
    control_host->InstallHandler(handler);
    fw::CoAppSeam control_seam;
    control_seam.host = control_host;
    std::unique_ptr<v2::CoApp> control;
    ran = framework_test::RunInCoTask([&] {
        control = fw::MakeCoAppForTest(opts, std::move(control_seam));
    });
    BOOST_REQUIRE(ran);
    std::thread([&] {
        (void)framework_test::RunSeamHostEngineWhenReady(*control_host);
    }).detach();
    BOOST_REQUIRE(framework_test::CoOk(framework_test::CoStart(*control)));
    BOOST_CHECK(control->State() == fw::AppState::Running);
    BOOST_REQUIRE(framework_test::CoOk(framework_test::CoStop(*control)));
    BOOST_CHECK(control->State() == fw::AppState::Stopped);
    BOOST_CHECK(control_host->http_server() == nullptr);
}

// ── T16：真实入口 HTTP→Service→Component ──
//
// 同一 v2::CoApp 上的完整入口链：infra HTTP/1.1 wire（真实 socket，宿主
// InstallHandler）→ 应用侧 HTTP→V2 适配 → v2::DispatchInboundForTest →
// InboundDispatcher → v2::CoService（借 v2::CoComponent 处理请求）。
// 组件是 App scope 的真实受管对象：handler 内经 GetComponent + Borrow 取得
// 命令面（本用例的 EchoComponent 计数借用次数并回显值），证明
// 「借用边界只在 borrow 寿命内」而非裸指针长持。

class EchoComponent final : public v2::CoComponent {
public:
    struct Options {};
    static std::atomic<int> borrows;

    explicit EchoComponent(fw::detail::ConstructKey k, Options)
        : v2::CoComponent(k) {}

    std::int32_t Echo(std::int32_t v) const { return v + 7; }

protected:
    ~EchoComponent() noexcept override = default;
    friend class fw::detail::Registry;
};
std::atomic<int> EchoComponent::borrows{0};

class HttpEntrySvc final
    : public v2::CoService<EchoComponent> {
public:
    static constexpr std::string_view kServiceName = "entry";
    explicit HttpEntrySvc(fw::detail::ConstructKey k)
        : v2::CoService<EchoComponent>(k) {}

    fw::CoRpcResp Handle(fw::CoRpcReq req) {
        auto value = IntArg(req);
        if (!value)
            return fw::CoRpcResp::Error(value.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        if (g_probe)
            g_probe->Snapshot(g_probe->before, ctx.value()->request_id,
                              SnapCtx(*ctx.value()));
        auto h = GetComponent<EchoComponent>("echo-comp");
        if (!h)
            return fw::CoRpcResp::Error(h.error());
        auto b = h.value().Borrow();
        if (!b)
            return fw::CoRpcResp::Error(b.error());
        EchoComponent::borrows.fetch_add(1);
        return fw::CoRpcResp::From(b.value().Get().Echo(value.value()));
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&HttpEntrySvc::Handle>("handle"));
protected:
    ~HttpEntrySvc() noexcept override = default;
    friend class fw::detail::Registry;
};

BOOST_AUTO_TEST_CASE(http_ingress_service_component) {
    using std::chrono::milliseconds;
    g_probe = std::make_shared<ProbeState>(0);
    EchoComponent::borrows.store(0);

    const auto limits = Limits(milliseconds{30000});
    g_bbt_coroutine_config->m_cfg_static_thread_num = 6;
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
    framework_test::EnsureRuntime();

    auto http_probe = std::make_shared<HttpProbe>();
    auto host = MakeHost(limits, "http-entry");
    auto app_slot = std::make_shared<std::weak_ptr<v2::CoApp>>();
    host->InstallHandler(
        [app_slot, http_probe](inf::IncomingCallContext,
                               inf::http::IncomingRequest request)
            -> fw::result<inf::http::Response> {
            http_probe->Record(request.method, request.target, request.body);
            if (request.method != "POST" || request.target != "/entry/handle")
                return fw::result<inf::http::Response>::ok(
                    inf::http::Response{404, {}, "not found"});
            auto app = app_slot->lock();
            if (!app)
                return fw::result<inf::http::Response>::err(fw::MakeError(
                    fw::ErrorCode::RuntimeUnavailable,
                    "http handler: app is gone"));
            inf::rpc::Request env;
            env.SetService("entry")
                .SetMethod("handle")
                .SetRequestId("http-entry-1")
                .SetRequestSchema(std::string(fw::kCoRpcPositionalSchema))
                .SetResponseSchema(std::string(fw::kCoRpcPositionalSchema))
                .SetReply(inf::rpc::ReplyMode::Required)
                .SetTraffic(inf::rpc::TrafficClass::Service)
                .SetPayload(std::vector<std::uint8_t>(request.body.begin(),
                                                      request.body.end()));
            auto reply = fw::v2::DispatchInboundForTest(*app, env);
            if (!reply)
                return fw::result<inf::http::Response>::err(reply.error());
            const auto& payload = reply.value().Payload();
            return fw::result<inf::http::Response>::ok(inf::http::Response{
                200, {}, std::string(payload.begin(), payload.end())});
        });

    fw::AppOptions opts;
    opts.SetLimits(64, 64, 8, 64)
        .SetShutdownBudget(milliseconds{2000})
        .SetLifecycleBudget(milliseconds{3000});

    std::shared_ptr<v2::CoApp> app;
    fw::result<void> r1 = fw::result<void>::ok();
    fw::result<void> r2 = fw::result<void>::ok();
    bool ran = framework_test::RunInCoTask([&] {
        fw::CoAppSeam seam;
        seam.host = host;
        app = std::shared_ptr<v2::CoApp>(
            fw::MakeCoAppForTest(opts, std::move(seam)).release());
        r1 = bbt_co_await
            app->CoInstallComponent<EchoComponent>("echo-comp",
                                                   EchoComponent::Options{});
        r2 = bbt_co_await app->CoInstallService<HttpEntrySvc>(
            ConcurrentOpts().BindComponent<EchoComponent>("echo-comp"));
    });
    BOOST_REQUIRE(ran);
    BOOST_REQUIRE(static_cast<bool>(r1));
    BOOST_REQUIRE(static_cast<bool>(r2));
    BOOST_REQUIRE(app);
    *app_slot = app;

    auto run = RunApp(*app);
    std::thread([&] {
        (void)framework_test::RunSeamHostEngineWhenReady(*host);
    }).detach();
    const auto until = Clock::now() + kWait;
    std::string endpoint;
    while (endpoint.empty()) {
        BOOST_REQUIRE(Clock::now() < until);
        endpoint = host->bound_endpoint();
        std::this_thread::yield();
    }

    auto client_rt = MakeHost(limits, "http-entry-client");
    BOOST_REQUIRE(framework_test::HostCreateStart(*client_rt));
    auto client = client_rt->http_client();
    BOOST_REQUIRE(client);

    const auto payload = RequestInt(21).payload();
    const std::string body(payload.begin(), payload.end());
    struct HttpResult {
        std::optional<fw::result<inf::http::Response>> out;
        TestLatch done{1};
    };
    auto result = std::make_shared<HttpResult>();
    bool registered = false;
    g_scheduler->RegistCoroutineTask(
        [result, client, endpoint, body] {
            inf::CallOptions call;
            call.deadline = Clock::now() + std::chrono::seconds{10};
            result->out.emplace(bbt_co_await client->Post(
                "http://" + endpoint + "/entry/handle",
                inf::http::RequestBody{body, "application/octet-stream"},
                inf::http::RequestOptions{}, call));
            result->done.CountDown();
        },
        registered);
    BOOST_REQUIRE(registered);
    BOOST_REQUIRE(result->done.WaitFor(kWait));
    BOOST_REQUIRE(result->out.has_value());
    BOOST_REQUIRE(result->out.value());
    auto response = result->out.value().value();

    BOOST_CHECK_EQUAL(response.status_code, 200);
    BOOST_CHECK_EQUAL(http_probe->Calls(), 1);
    auto decoded = fw::CoRpcReq(
                       std::vector<std::uint8_t>(response.body.begin(), response.body.end()))
                       .Parse<std::int32_t>();
    BOOST_REQUIRE(decoded);
    BOOST_CHECK_EQUAL(decoded.value(), 28); // 21 + 7：组件真实回显
    BOOST_CHECK_EQUAL(EchoComponent::borrows.load(), 1);
    BOOST_CHECK_EQUAL(g_probe->At(g_probe->before, "http-entry-1").request_id,
                      "http-entry-1");

    app->RequestShutdown();
    JoinRun(run);
    BOOST_CHECK(framework_test::CoOk(run->rc));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    (void)framework_test::HostClose(*client_rt);
}

// ── T17：入口 host hook 单次 Run（独立子进程验收）──
//
// 子进程经 runtime 生成式入口 bbtco_main + bbtco_main_host_hook：装配
// v2::CoApp 后入口 hook 经 HostGate::RunHostGate 对同一共享 Engine 恰执行
// 一次 Engine::Run（真实端口绑定 + ready 写 port 文件 + stdin EOF 收口）。
// 父进程以真实退出码与 port 文件为凭据。
BOOST_AUTO_TEST_CASE(host_gate_single_run_child) {
    using std::chrono::milliseconds;
    const std::string bin = BBT_HOSTGATE_CHILD_BIN;
    std::ifstream probe(bin);
    if (!probe.good()) {
        BOOST_TEST_MESSAGE("hostgate_child binary not found at " << bin
                           << " — skipping host-gate entry acceptance");
        return;
    }

    const std::string port_file =
        std::string("/tmp/hostgate-child-") + std::to_string(::getpid()) +
        ".port";
    ::unlink(port_file.c_str());

    ::setenv("BBT_EXAMPLE_MODE", "hostgate", 1);
    ::setenv("BBT_EXAMPLE_PORT_FILE", port_file.c_str(), 1);
    const std::string cmd = bin;
    auto* pipe = ::popen(cmd.c_str(), "r");
    BOOST_REQUIRE(pipe != nullptr);

    std::string port;
    {
        const auto until = Clock::now() + kWait;
        bool ready = false;
        while (Clock::now() < until) {
            std::ifstream f(port_file);
            if (f >> port && !port.empty()) {
                ready = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{20});
        }
        BOOST_REQUIRE_MESSAGE(ready, "child did not publish bound port");
    }

    const int status = ::pclose(pipe);
    BOOST_CHECK(WIFEXITED(status));
    if (WIFEXITED(status))
        BOOST_CHECK_EQUAL(WEXITSTATUS(status), 0);
    ::unlink(port_file.c_str());
}

// ── T18：挂起期间组件关闭/解绑后实时借用返回失败 ──
//
// 借用边界（只验证 borrow 门，不引入 Service 执行租约）：
//   - handler 先取得 ComponentHandle 再停在放行门上（挂起期间）；
//   - 并行地对同一组件执行 CoUnbindComponent（关闭绑定边）+
//     CoUninstallComponent（移除登记）；
//   - 放行后旧 binding 代句柄的 Borrow 返回 Closed，下一次实时
//     GetComponent 返回 NotFound——借用边界成立。
class GatedBorrowSvc final : public v2::CoService<EchoComponent> {
public:
    static constexpr std::string_view kServiceName = "gated-borrow";
    static std::shared_ptr<CoGate> gate;
    static std::atomic<int> entered;
    static std::atomic<int> notfound_seen;
    static std::atomic<int> closed_seen;

    explicit GatedBorrowSvc(fw::detail::ConstructKey k)
        : v2::CoService<EchoComponent>(k) {}

    fw::CoRpcResp HoldThenBorrow(fw::CoRpcReq req) {
        auto value = IntArg(req);
        if (!value)
            return fw::CoRpcResp::Error(value.error());
        // 先取真实绑定句柄（带 binding 代），再挂起在放行门上：测试侧
        // 在此期间完成解绑 + 卸载。
        auto h = GetComponent<EchoComponent>("echo-comp");
        if (!h)
            return fw::CoRpcResp::Error(h.error());
        entered.fetch_add(1);
        const auto st = gate->Wait(Clock::now() + 30s);
        if (st != co::WaitStatus::Completed)
            return fw::CoRpcResp::Error(WaitErr(st, "gated borrow wait"));
        // 恢复后第一次实时借用：binding 代已封 → Closed。
        auto b = h.value().Borrow();
        if (!b && b.error().code == fw::ErrorCode::Closed)
            closed_seen.fetch_add(1);
        // 同时验证下一次实时 GetComponent：组件已卸载 → NotFound。
        auto again = GetComponent<EchoComponent>("echo-comp");
        if (!again) {
            if (again.error().code == fw::ErrorCode::NotFound)
                notfound_seen.fetch_add(1);
            return fw::CoRpcResp::Error(again.error());
        }
        if (!b)
            return fw::CoRpcResp::Error(b.error());
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&GatedBorrowSvc::HoldThenBorrow>("hold_then_borrow"));
protected:
    ~GatedBorrowSvc() noexcept override = default;
    friend class fw::detail::Registry;
};
std::shared_ptr<CoGate> GatedBorrowSvc::gate = std::make_shared<CoGate>();
std::atomic<int> GatedBorrowSvc::entered{0};
std::atomic<int> GatedBorrowSvc::notfound_seen{0};
std::atomic<int> GatedBorrowSvc::closed_seen{0};

BOOST_AUTO_TEST_CASE(suspended_component_close_then_borrow_fails) {
    using std::chrono::milliseconds;
    EchoComponent::borrows.store(0);
    GatedBorrowSvc::gate = std::make_shared<CoGate>();
    GatedBorrowSvc::entered.store(0);
    GatedBorrowSvc::notfound_seen.store(0);
    GatedBorrowSvc::closed_seen.store(0);
    g_probe = std::make_shared<ProbeState>(0);

    auto app = framework_test::CreateV2App([] {
        return v2::CoApp::Create(
            fw::AppOptions{}
                .SetLimits(64, 64, 8, 64)
                .SetShutdownBudget(milliseconds{2000})
                .SetLifecycleBudget(milliseconds{3000}),
            "suspend-borrow");
    });
    BOOST_REQUIRE(app);

    fw::result<void> rc = fw::result<void>::ok();
    bool ran = framework_test::RunInCoTask([&] {
        auto a = bbt_co_await app->CoInstallComponent<EchoComponent>(
            "echo-comp", EchoComponent::Options{});
        auto b = bbt_co_await app->CoInstallService<GatedBorrowSvc>(
            ConcurrentOpts().BindComponent<EchoComponent>("echo-comp"));
        auto c = bbt_co_await app->CoStart();
        if (!a) rc = std::move(a);
        else if (!b) rc = std::move(b);
        else rc = std::move(c);
    });
    BOOST_REQUIRE(ran);
    BOOST_REQUIRE(static_cast<bool>(rc));

    // 先入一条真实请求并让 handler 停在放行门上（挂起期间组件被解绑/卸载）。
    auto env = MakeEnv("gated-borrow", "hold_then_borrow", "t18-h", 5);
    TestLatch handler_done{1};
    std::optional<fw::result<inf::rpc::Response>> out;
    bbtco[app, env, &out, &handler_done]() mutable {
        out.emplace(fw::v2::DispatchInboundForTest(*app, env));
        handler_done.CountDown();
    };
    {
        const auto until = Clock::now() + kWait;
        while (GatedBorrowSvc::entered.load() == 0) {
            BOOST_REQUIRE(Clock::now() < until);
            std::this_thread::yield();
        }
    }

    auto svc_h = app->FindService("gated-borrow");
    BOOST_REQUIRE(svc_h);

    // 挂起期间解绑（关闭该绑定边的新 borrow 并等旧 borrow 排空）。
    fw::result<void> unbind = fw::result<void>::err(
        fw::MakeError(fw::ErrorCode::RuntimeUnavailable, "not run"));
    ran = framework_test::RunInCoTask([&] {
        unbind = bbt_co_await app->CoUnbindComponent<EchoComponent>(
            svc_h.value(), "echo-comp");
    });
    BOOST_REQUIRE(ran);
    BOOST_REQUIRE(static_cast<bool>(unbind));

    // 解绑后卸载组件（移除登记——借用边界：下一次实时借用/调用失败）。
    fw::result<void> uninstall = fw::result<void>::err(
        fw::MakeError(fw::ErrorCode::RuntimeUnavailable, "not run"));
    ran = framework_test::RunInCoTask([&] {
        uninstall = bbt_co_await app->CoUninstallComponent<EchoComponent>(
            "echo-comp");
    });
    BOOST_REQUIRE(ran);
    BOOST_REQUIRE(static_cast<bool>(uninstall));

    // 放行 handler：旧 binding 代句柄的 Borrow 必返 Closed；下一次
    // GetComponent 必返 NotFound。
    GatedBorrowSvc::gate->Open();
    BOOST_REQUIRE(handler_done.WaitFor(kWait));
    BOOST_CHECK(GatedBorrowSvc::closed_seen.load() == 1);
    BOOST_CHECK(GatedBorrowSvc::notfound_seen.load() == 1);
    BOOST_CHECK(!app->FindComponent<EchoComponent>("echo-comp"));

    auto stopped = framework_test::CoStop(*app);
    BOOST_CHECK(framework_test::CoOk(stopped));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace
