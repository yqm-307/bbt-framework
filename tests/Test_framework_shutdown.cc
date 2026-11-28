/**
 * 资源管理与关闭（唯一公开 API：CoInstall / CoStart / CoWaitShutdownRequest /
 * CoStop / RequestShutdown / FindService / LifecycleFailures / PendingCleanup）。
 *
 * 覆盖（全部真实断言、真实 V2 会话 loopback、无 mock 传输、无 sleep 凑时序）：
 * T1 request_shutdown_external_thread   外部控制线程 RequestShutdown 生效：
 * _clean_rc0                        专用 std::thread 发起关闭 → 正常收束
 *                                   → run 返回 kExitOk(0) → Stopped。
 * T2 inflight_egress_during_closing   关闭期间在途 handler 仍可出站：handler
 *                                   挂起 → RequestShutdown → Stopping →
 *                                   放行 → handler 真实 call 经
 *                                   loopback 到独立 echo peer → 回复送达
 *                                   → 排空 → kExitOk。
 * T3 request_shutdown_before_start    早于启动的关闭请求：启动后立即进入
 *                                   关闭序列 → kExitOk。
 * T4 shutdown_incomplete_actor_late   预算耗尽 → StopIncomplete（LifecycleFailure
 * _egress                           与失败相位可观察、run 不返回、调度器与
 *                                   出站 runtime 保留、Service 强持有）
 *                                   → 放行后 actor handler 在 StopIncomplete
 *                                   期间完成真实出站 → 迟到收口 → 第二次
 *                                   CoStop 推进 → kExitStopIncomplete。
 * T5 late_callback_past_logical_end   诊断区分逻辑请求结束与物理清理：
 *                                   handler 物理挂起超过 incoming 看门
 *                                   （客户端见 OutcomeUnknown 即逻辑结束）
 *                                   → 排空仍等待物理在途 → StopIncomplete
 *                                   → 迟到回调安全执行（资源仍存活）→
 *                                   kExitStopIncomplete。
 * T6 wait_handlers_budget_exhaustion  WaitHandlersDone 步预算耗尽路径
 *                                   （INetworkHost 桩，无 socket）：首次
 *                                   CoStop 记失败+宿主同步 Close→
 *                                   StopIncomplete；再次推进不重排空。
 * T7/T7c reply_reserve                V2 正式入站的跨层预算：infra local-min
 *                                   后 framework 恰扣一次 20ms 预留；紧预算在
 *                                   handler 前 TimedOut（服务端观测判定）。
 * T9 v2_multihop                      两跳预算不放大：ResolveCallDeadline 取
 *                                   min、显式子期限不生效于放大方向。
 * T10 v2_host_small_limits_start      合法小限额 + 2 workers 的纯入站宿主。
 * T11 v2_engine_start_occupied        Run 提前返回（端口占用）Start 及时失败。
 * _port_fails_fast
 * T12 v2_egress_body_limit            出站请求字节超限在发起 I/O 前拒绝。
 *
 * 迁移语义映射：
 * - 排空未被证实（预算耗尽/宿主 Close 打断）→ StopIncomplete + LifecycleFailure
 *   + 宿主同步 Close + 撤销接纳（不再无界续等/park）；再次 CoStop 以更晚
 *   期限推进收口（StopIncomplete 契约）——旧 ShutdownIncomplete 语义如实落地。
 * - run 返回码映射为受管任务 rc 约定：kExitOk/kExitStartFailed/kExitStopIncomplete。
 * - 入站接线：生产 InstallRpcAdmission 需 app.m_runtime（候选公开面无访问口），
 *   测试侧 admission 复刻器转发 DispatchInboundForTest（同纪律：同步派发门
 *   在可挂起动作前释放 + ValidateErrorAtBoundary 边界校验）。
 *
 * 与仓内既有单测一致：Boost.Test 经 included/unit_test.hpp 静态内嵌。
 */
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
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

#include <bbt/infra/http/Client.hpp>
#include <bbt/infra/http/Server.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>

/**
 * 机器面（真实宿主/测试缝构造/有序类型/边界校验）显式走 internal 头。
 */
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/ErrorDomainRule.hpp>
#include <bbt/framework/internal/NetworkHost.hpp>
#include <bbt/framework/internal/RpcBridge.hpp>

namespace fw = bbt::framework;
namespace co = bbt::coroutine;
namespace inf = bbt::infra;
using Clock = std::chrono::steady_clock;

namespace {

/**
 * ── 测试屏障原语（std CV，无 sleep 凑时序）──
 */

class TestLatch {
public:
    explicit TestLatch(int n) : m_count(n) {}
    void CountDown() {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (m_count <= 0)
                return;
            if (--m_count > 0)
                return;
        }
        /**
         * 锁内通知：等待者只能在通知返回、锁被释放后才可能因计数归零返回，
         * 于是「等待者返回后析构 latch」不可能仍与通知并发（通知在锁内的
         * 最后一次 CV 使用先于任何返回者退出）。超时返回仍由调用方按值持有
         * 的堆状态保活 latch（见 CallState），不靠本排序兜底。
         */
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

class TestGate {
public:
    void Open() {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_open = true;
        }
        m_cv.notify_all();
    }
    bool WaitFor(std::chrono::milliseconds ms) {
        std::unique_lock<std::mutex> lk(m_mtx);
        return m_cv.wait_for(lk, ms, [this] { return m_open; });
    }
    void Wait() {
        std::unique_lock<std::mutex> lk(m_mtx);
        m_cv.wait(lk, [this] { return m_open; });
    }

private:
    std::mutex m_mtx;
    std::condition_variable m_cv;
    bool m_open = false;
};

constexpr std::chrono::milliseconds kWait{15000};

/**
 * 进程内唯一 runtime：native 测试宿主负责初始化；协程配置只在尚未
 * 初始化时写入（非线程安全）。宿主/Engine/受管对象创建都依赖已
 * Start 的调度器。
 */
void EnsureScheduler() {
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 8;
        /**
         * 受管 handler 的嵌套出站链（入站派发 → call → 出站适配 →
         * client->Call → 信号等待）在 12KB 默认栈触底；沿用 64KB。
         */
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
        g_scheduler->Start();
    }
}

/**
 * ── 协程闩（CoWaiter 上的单次放行语义）──
 * Open 前 Wait 真实挂起；Open 后（含 Open 早于登记）全部放行，不丢唤醒；
 * 使用 CoWaiter 的单次等待语义，无 sleep。支持多等待者。
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
        for (auto& w : notify)
            w->Notify();
    }
    co::WaitStatus Wait(co::Deadline deadline) {
        auto waiter = co::sync::CoWaiter::Create();
        co::WaitOptions wo;
        wo.deadline = deadline;
        const auto st = waiter->WaitWithCallback(wo, [&]() -> bool {
            std::lock_guard<std::mutex> lk(m_mtx);
            /**
             * 早到：登记后自唤醒，不丢
             */
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

/**
 * ── CoRpc 位置参数测试辅助 ──
 */

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

/**
 * ── 观测态 ──
 */

struct Probe {
    explicit Probe(int arrive_n) : arrived(arrive_n) {}

    TestLatch arrived;

    /**
     * 业务 handler 挂起点：CoWaiter 闩，由首个挂起的 handler 惰性建立；
     * 测试经 CompleteGate 放行（非 sleep 时序）。
     */
    std::shared_ptr<CoGate> Gate() {
        std::lock_guard<std::mutex> lk(mtx);
        if (!gate)
            gate = std::make_shared<CoGate>();
        return gate;
    }
    void CompleteGate() {
        std::shared_ptr<CoGate> g;
        {
            std::lock_guard<std::mutex> lk(mtx);
            g = gate;
        }
        if (g)
            g->Open();
    }

    mutable std::mutex mtx;
    std::shared_ptr<CoGate> gate;
    std::vector<std::string> events;

    void Event(std::string e) {
        std::lock_guard<std::mutex> lk(mtx);
        events.push_back(std::move(e));
    }
    bool HasEvent(const std::string& e) const {
        std::lock_guard<std::mutex> lk(mtx);
        return std::find(events.begin(), events.end(), e) != events.end();
    }
    /**
     * 有界等待任意带前缀的事件（handler 出站完成可能晚于客户端终态）。
     */
    bool WaitEventPrefix(const std::string& prefix,
                         std::chrono::milliseconds ms) {
        const auto until = Clock::now() + ms;
        for (;;) {
            {
                std::lock_guard<std::mutex> lk(mtx);
                for (const auto& e : events)
                    if (e.rfind(prefix, 0) == 0)
                        return true;
            }
            if (Clock::now() >= until)
                return false;
            std::this_thread::yield();
        }
    }
};

std::shared_ptr<Probe> g_probe;

/**
 * V2 跨层预算探针（T7）：handler 内可见的绝对期限与执行计数。
 */
std::atomic<std::int64_t> g_budget_deadline_ns{0};
std::atomic<int> g_budget_calls{0};

/**
 * T7 服务端同钟观测（真实 RpcServer handler 路径）：
 * - g_inbound_entries：被安装的 RpcHandler 真实进入次数——证明请求经
 * infra HandleRpcRequest/local-min 后抵达 framework 入站入口（loopback
 * 上请求先发出再等待，客户端先超时也不会让请求不到服务端）。
 * - g_inbound_pre_deadline_ns：进入 InboundDispatcher **之前**的
 * request.Context().deadline（infra 入站解码的 local-min 结果），与服务端
 * handler 内可见的 ctx.deadline（扣减后）同钟相减即 framework 预留的精确值。
 * - g_inbound_error_code：服务端 Dispatcher 返回的错误码（错误路径），
 * 使「服务端拒绝」不依赖 client 侧错误语义（同域 TimedOut）。
 */
std::atomic<int> g_inbound_entries{0};
std::atomic<std::int64_t> g_inbound_pre_deadline_ns{0};
std::atomic<int> g_inbound_error_code{0};
/**
 * T7c 补充观测：进入 handler 的真实时刻（同钟）——handler 可见期限相对它的余量
 * 是「名义预算够不够」的实测证据，仅记录，不进入契约断言。
 */
std::atomic<std::int64_t> g_inbound_entry_ns{0};
/**
 * T7c 受控耗尽：置位时入站 handler 在交付 framework **之前**等到接收端 local-min
 * 期限确实过去——用受控调度构造「名义预算 > 预留 仍被拒」，不靠 wall-clock 容差。
 */
std::atomic<bool> g_inbound_hold_until_deadline{false};

/**
 * 观测型入站（T7/T7c/T12）：真实 CoRpcServer 入站路径进入本闭包时记录
 * request.Context().deadline（framework 扣减前、服务端同钟口径）与入口计数，
 * 再经既有测试缝 DispatchInboundForTest 走真实 InboundDispatcher/RequestScope
 * 分发。只用于跨层预算观测；与生产 InstallRpcAdmission 同纪律（同步派发门
 * 在可挂起动作前释放；DispatchInboundForTest 不透传 on_dispatched 是候选缝
 * 签名缺口——测试侧先放行，见交接「公共缺口」）。
 */
void InstallBudgetProbeHandler(const std::shared_ptr<fw::InfraHttpHost>& host,
                               const std::shared_ptr<std::shared_ptr<fw::CoApp>>& app_slot) {
    host->InstallRpcHandler(
        [app_slot](const inf::rpc::Request& request,
                   const std::function<void()>& dispatched)
            -> fw::result<inf::rpc::Response> {
            g_inbound_entry_ns.store(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    Clock::now().time_since_epoch())
                    .count(),
                std::memory_order_release);
            g_inbound_pre_deadline_ns.store(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    request.Context().deadline.time_since_epoch())
                    .count(),
                std::memory_order_release);
            g_inbound_entries.fetch_add(1, std::memory_order_acq_rel);
            if (g_inbound_hold_until_deadline.load(std::memory_order_acquire))
                std::this_thread::sleep_until(
                    request.Context().deadline + std::chrono::milliseconds{1});
            /**
             * 契约：所有可挂起动作之前释放同步派发门。
             */
            dispatched();
            auto reply = fw::DispatchInboundForTest(**app_slot, request);
            if (!reply) {
                if (auto valid = fw::ValidateErrorAtBoundary(reply.error()); !valid)
                    reply = fw::result<inf::rpc::Response>::err(std::move(valid.error()));
                g_inbound_error_code.store(
                    static_cast<int>(reply.error().code),
                    std::memory_order_release);
                return reply;
            }
            return reply;
        });
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

/**
 * ── 测试服务 ──
 */

/**
 * "relay"：Concurrent。ping 即时回显；wait_dial 在 gate 上挂起（ctx 预算内），
 * 放行后执行真实 call 到 echo peer；wait_dial_long 以无界期限挂起
 * （物理在途可超过 incoming 看门），放行后仍尝试出站并记录结果码。
 */
class RelaySvc final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "relay";
    explicit RelaySvc(fw::detail::ConstructKey k) : fw::CoService<>(k) {}
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        return ReplyInt(IntArg(req));
    }
    fw::CoRpcResp WaitDial(fw::CoRpcReq req) {
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        auto p = g_probe;
        if (!p)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::InternalError, "no probe"));
        auto gate = p->Gate();
        p->arrived.CountDown();
        const auto st = gate->Wait(ctx.value()->deadline);
        if (st != co::WaitStatus::Completed)
            return fw::CoRpcResp::Error(WaitErr(st, "wait_dial gate"));
        auto response = bbt_co_await call("echo", "ping", req);
        p->Event(std::string("outbound:") +
                 (response ? "ok" : "code" + std::to_string(static_cast<int>(response.error().code))));
        if (!response)
            return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    fw::CoRpcResp WaitDialLong(fw::CoRpcReq req) {
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
        auto gate = p->Gate();
        p->arrived.CountDown();
        const auto st = gate->Wait(co::Deadline::max());
        if (st != co::WaitStatus::Completed)
            return fw::CoRpcResp::Error(WaitErr(st, "wait_dial_long"));
        p->Event("late_handler_resumed");
        auto response = bbt_co_await call("echo", "ping", req);
        p->Event(std::string("late_outbound:") +
                 (response ? "ok" : "code" + std::to_string(static_cast<int>(response.error().code))));
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&RelaySvc::Ping>("ping"),
        fw::Method<&RelaySvc::WaitDial>("wait_dial"),
        fw::Method<&RelaySvc::WaitDialLong>("wait_dial_long"));

protected:
    ~RelaySvc() noexcept override = default;

private:
    friend class fw::detail::Registry;
};

/**
 * "acct"：ActorSerial。park_dial 挂起后出站；第一个位置参数为 key。
 */
class AcctRelaySvc final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "acct";
    explicit AcctRelaySvc(fw::detail::ConstructKey k) : fw::CoService<>(k) {}
    fw::CoRpcResp ParkDial(fw::CoRpcReq req) {
        auto args = AcctArgs(req);
        if (!args)
            return fw::CoRpcResp::Error(args.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        auto p = g_probe;
        if (!p)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::InternalError, "no probe"));
        auto gate = p->Gate();
        p->arrived.CountDown();
        const auto st = gate->Wait(ctx.value()->deadline);
        if (st != co::WaitStatus::Completed)
            return fw::CoRpcResp::Error(WaitErr(st, "park_dial gate"));
        auto response = bbt_co_await call(
            "echo", "ping", RequestInt(std::get<1>(args.value())));
        p->Event(std::string("actor_outbound:") +
                 (response ? "ok" : "code" + std::to_string(static_cast<int>(response.error().code))));
        if (!response)
            return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::ActorMethodAt<&AcctRelaySvc::ParkDial, std::string>(
            "park_dial"));

protected:
    ~AcctRelaySvc() noexcept override = default;

private:
    friend class fw::detail::Registry;
};

/**
 * "budget"：Concurrent。probe 记录 handler 内可见的绝对期限（ctx.deadline）
 * 与执行计数——用于 T7 的 V2 跨层预算断言。
 */
class BudgetSvc final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "budget";
    explicit BudgetSvc(fw::detail::ConstructKey k) : fw::CoService<>(k) {}
    fw::CoRpcResp Probe(fw::CoRpcReq req) {
        auto v = IntArg(req);
        if (!v)
            return fw::CoRpcResp::Error(v.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        g_budget_deadline_ns.store(
            std::chrono::duration_cast<std::chrono::nanoseconds>(ctx.value()->deadline.time_since_epoch()).count(),
            std::memory_order_release);
        g_budget_calls.fetch_add(1, std::memory_order_acq_rel);
        return ReplyInt(v);
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(fw::Method<&BudgetSvc::Probe>("probe"));

protected:
    ~BudgetSvc() noexcept override = default;

private:
    friend class fw::detail::Registry;
};

/**
 * ── 装配选项 ──
 */

fw::ServiceOptions ConcurrentOpts() {
    return fw::ServiceOptions{}
        .SetExecution(fw::ExecutionPolicy::Concurrent)
        .SetMaxInflight(64);
}

fw::ServiceOptions ActorOpts() {
    return fw::ServiceOptions{}
        .SetExecution(fw::ExecutionPolicy::ActorSerial)
        .SetMaxInflight(64)
        .SetMaxSerialKeys(8)
        .SetSerialQueueCapacity(16);
}

inf::NetworkLimits Limits(std::chrono::milliseconds incoming_timeout) {
    return inf::NetworkLimits{
        /*max_connections*/ 64,
        /*max_inflight*/ 64,
        /*max_header_bytes*/ 16384,
        /*max_body_bytes*/ 65536,
        incoming_timeout};
}

fw::AppOptions AppOpts(inf::NetworkLimits limits,
                       std::chrono::milliseconds lifecycle_budget,
                       std::vector<fw::StaticRoute> routes,
                       std::string identity) {
    auto opts = fw::AppOptions{}
                    .SetLimits(64, 64, 64, 64)
                    .SetShutdownBudget(lifecycle_budget)
                    .SetLifecycleBudget(lifecycle_budget)
                    .SetNetworkLimits(std::move(limits))
                    .SetLocalIdentity(std::move(identity));
    for (auto& r : routes)
        opts.AddRoute(r.service_name, r.address);
    return opts;
}

/**
 * 测试侧入站接线复刻器：与生产 InstallRpcAdmission 同一纪律——同步派发门
 * 在任何可挂起动作之前释放，回复经 ValidateErrorAtBoundary 边界校验。
 * 生产入口需要 app.m_runtime（候选公开面无访问口，见交接「公共缺口」），
 * 测试侧经 app 槽位转发 DispatchInboundForTest。
 */
void InstallAdmissionForTest(
    const std::shared_ptr<fw::InfraHttpHost>& host,
    const std::shared_ptr<std::shared_ptr<fw::CoApp>>& app_slot) {
    host->InstallRpcHandler(
        [app_slot](const inf::rpc::Request& request,
                   const std::function<void()>& dispatched)
            -> fw::result<inf::rpc::Response> {
            dispatched();
            auto reply = fw::DispatchInboundForTest(**app_slot, request);
            if (!reply) {
                if (auto valid = fw::ValidateErrorAtBoundary(reply.error()); !valid)
                    return fw::result<inf::rpc::Response>::err(
                        std::move(valid.error()));
            }
            return reply;
        });
}

/**
 * ── 宿主夹具：真实 InfraHttpHost + CoApp ──
 */

struct RunHandle {
    std::atomic<bool> done{false};
    int rc{-1};
    std::promise<bool> started;
};

void JoinRun(const std::unique_ptr<RunHandle>& h) {
    const auto until = Clock::now() + std::chrono::minutes{3};
    while (!h->done.load(std::memory_order_acquire) && Clock::now() < until)
        std::this_thread::yield();
}

/**
 * 旧 run() 退出码在本测试内的对应映射：
 * kExitOk = 全部收口至 Stopped 且无 LifecycleFailure；
 * kExitStartFailed = 装配或 CoStart 失败；
 * kExitStopIncomplete = 收口未竟（曾记失败或 StopIncomplete 推进后才完成）。
 */
constexpr int kExitOk = 0;
constexpr int kExitStartFailed = 2;
constexpr int kExitStopIncomplete = 4;

/**
 * 路由 endpoint 逻辑标记 → 对端真实地址的解析槽：InfraHttpHost::Create
 * 依赖已 Start 的调度器，echo peer 只能在 app 起来之后创建，其真实
 * endpoint 无法写进启动前固定的 static_routes——路由白名单仍由
 * FindRoute 把关，发送闭包在发送时刻经此槽解析标记（同入站用例的
 * "self" 处理形态）。空标记值发送 → RuntimeUnavailable 如实返回。
 */
struct EndpointSlot {
    void Set(std::string s) {
        std::lock_guard<std::mutex> lk(mtx);
        ep = std::move(s);
    }
    std::string Get() const {
        std::lock_guard<std::mutex> lk(mtx);
        return ep;
    }
    mutable std::mutex mtx;
    std::string ep;
};

struct AppBox {
    std::shared_ptr<fw::InfraHttpHost> host;
    /**
     * app 测试缝出站的会话 owner（identity 与 app 互异）：Create/Start 在
     * 调度器起来后完成——InfraHttpHost::Create 依赖已 Start 的调度器。
     */
    std::shared_ptr<fw::InfraHttpHost> egress_rt;
    std::shared_ptr<fw::RpcEgress> egress;
    std::shared_ptr<EndpointSlot> peer_ep;
    /**
     * app 由夹具与受管任务共享强持有：Register/Start/Stop 全部在受管
     * 协程内完成。
     */
    std::shared_ptr<std::shared_ptr<fw::CoApp>> app_slot =
        std::make_shared<std::shared_ptr<fw::CoApp>>();
    std::unique_ptr<RunHandle> run;
    std::string endpoint;

    /* 首次收口未竟才等测试放行；未安装闩时直接推进。 */
    std::shared_ptr<CoGate> after_first_stop;

    std::shared_ptr<fw::CoApp> App() const { return *app_slot; }

    AppBox() = default;
    ~AppBox() { Stop(); }
    AppBox(AppBox&&) = default;
    AppBox& operator=(AppBox&&) = default;
    AppBox(const AppBox&) = delete;
    AppBox& operator=(const AppBox&) = delete;

    /**
     * 幂等收尾：放行测试门/推进闩 → 请求关闭 → join 受管任务线程。
     * REQUIRE 早退路径同样走这里，保证不 terminate。
     */
    void Stop() {
        if (g_probe)
            g_probe->CompleteGate();
        if (after_first_stop)
            after_first_stop->Open();
        if (egress_rt)
            egress_rt->Close();
        if (App())
            App()->RequestShutdown();
        if (run)
            JoinRun(run);
        if (host)
            host->Close();
    }
};

/**
 * 受管服务端任务体：register_fn（装配）→ CoStart → 等关闭请求 →
 * 首次 CoStop →（after_first_stop 放行）→ 未竟时再次 CoStop 推进收口。
 * 全部生命周期入口必须在受管协程内执行。
 */
void ServerTaskBody(
    const std::shared_ptr<std::shared_ptr<fw::CoApp>>& app_slot,
    const std::shared_ptr<CoGate>& after_first_stop,
    RunHandle* h,
    const std::function<fw::result<void>(fw::CoApp&)>& register_fn) {
    auto set_rc = [&](int rc) {
        h->rc = rc;
        h->done.store(true, std::memory_order_release);
    };
    try {
        auto app = *app_slot;
        if (register_fn) {
            auto r = register_fn(*app);
            if (!r) {
                set_rc(kExitStartFailed);
                return;
            }
        }
        auto started = bbt_co_await app->CoStart();
        if (!started) {
            set_rc(kExitStartFailed);
            return;
        }
        h->started.set_value(true);

        auto waited = bbt_co_await app->CoWaitShutdownRequest();
        if (!waited) {
            set_rc(kExitStopIncomplete);
            return;
        }

        {
            auto _r = bbt_co_await app->CoStop();
            (void)_r;
        }

        /**
         * 收口未竟（Stopping/StopIncomplete）才等测试放行推进。等待必须让出
         * 协程 worker：该线程也可能负责推进迟到出站的网络驱动。
         */
        if (app->State() == fw::AppState::StopIncomplete ||
            app->State() == fw::AppState::Stopping) {
            if (after_first_stop)
                (void)after_first_stop->Wait(Clock::now() + std::chrono::minutes{2});
            {
                auto _r2 = bbt_co_await app->CoStop();
                (void)_r2;
            }
        }
        set_rc(app->State() == fw::AppState::Stopped &&
                       app->LifecycleFailures().empty()
                   ? kExitOk
                   : kExitStopIncomplete);
    } catch (...) {
        try {
            h->started.set_value(false);
        } catch (...) {}
        set_rc(kExitStartFailed);
    }
}

/**
 * 组装 app：options/装配经 register_fn 提供给受管任务；安装入站
 * admission；不启动完成等待（供「早于启动的 RequestShutdown」用例与
 * 正常启动路径共用）。routes 为完整静态路由（endpoint 是 host:port 或
 * 逻辑标记——出站闭包在发送时刻解析标记）。
 */
AppBox PrepareApp(std::chrono::milliseconds incoming_timeout,
                  std::chrono::milliseconds step_budget,
                  std::vector<fw::StaticRoute> routes,
                  std::function<fw::result<void>(fw::CoApp&)> register_fn,
                  const std::function<void(AppBox&)>& configure_fn = {}) {
    AppBox box;
    const auto limits = Limits(incoming_timeout);
    box.host = std::make_shared<fw::InfraHttpHost>(
        limits, inf::ListenAddress{"127.0.0.1", 0}, "shutdown-server");
    box.peer_ep = std::make_shared<EndpointSlot>();
    box.egress_rt = std::make_shared<fw::InfraHttpHost>(
        limits, inf::ListenAddress{"127.0.0.1", 0}, "shutdown-egress");
    box.egress = std::make_shared<fw::RpcEgress>(box.egress_rt);
    InstallAdmissionForTest(box.host, box.app_slot);
    if (configure_fn)
        configure_fn(box);

    auto eps = box.peer_ep;
    auto egress = box.egress;
    box.run = std::make_unique<RunHandle>();
    RunHandle* h = box.run.get();
    auto app_slot = box.app_slot;
    auto host = box.host;
    auto after_first_stop = box.after_first_stop;
    EnsureScheduler();
    {
        bool submitted = false;
        g_scheduler->RegistCoroutineTask(
            [app_slot, host, after_first_stop, h,
             register_fn = std::move(register_fn),
             limits, step_budget, routes = std::move(routes),
             eps, egress]() mutable {
                /**
                 * 测试缝出站：endpoint 逻辑标记在发送时刻经解析槽换成
                 * echo peer / 自身监听的真实地址（同入站用例的 "self" 处理
                 * 形态）；路由门（服务名白名单）仍完整生效。出站是真实 V2 Call。
                 */
                fw::RpcSendAppFn send =
                    [eps, egress](std::string_view target,
                                  const inf::rpc::Request& env,
                                  const inf::CallOptions& o)
                    -> bbt_co_async(fw::result<inf::rpc::Response>) {
                    std::string ep(target);
                    constexpr std::string_view kScheme = "tcp://";
                    if (ep.rfind(kScheme, 0) == 0)
                        ep.erase(0, kScheme.size());
                    if (ep == "peer" || ep == "self")
                        ep = eps->Get();
                    if (ep.empty())
                        bbt_co_return fw::result<inf::rpc::Response>::err(fw::MakeError(
                            fw::ErrorCode::RuntimeUnavailable,
                            "egress peer endpoint not installed"));
                    bbt_co_return bbt_co_await egress->Call("tcp://" + ep, env, o);
                };
                try {
                    *app_slot = fw::MakeCoAppForTest(
                        AppOpts(limits, step_budget, std::move(routes),
                                "shutdown-server"),
                        fw::CoAppSeam{host, std::move(send)});
                } catch (...) {
                    h->rc = kExitStartFailed;
                    h->done.store(true, std::memory_order_release);
                    try {
                        h->started.set_value(false);
                    } catch (...) {}
                    return;
                }
                ServerTaskBody(app_slot, after_first_stop, h, register_fn);
            },
            submitted);
        if (!submitted) {
            h->rc = kExitStartFailed;
            h->done.store(true, std::memory_order_release);
            try {
                h->started.set_value(false);
            } catch (...) {}
        }
    }
    return box;
}

/**
 * 启动并等到监听就绪（bound_endpoint 非空），再装出站会话 owner。
 */
void LaunchApp(AppBox& box) {
    auto started = box.run->started.get_future();
    BOOST_REQUIRE_MESSAGE(
        started.wait_for(std::chrono::seconds{15}) ==
                std::future_status::ready &&
            started.get(),
        "server task did not start");
    const auto until = Clock::now() + kWait;
    while (box.endpoint.empty()) {
        BOOST_REQUIRE_MESSAGE(!box.run->done.load(),
                              "server task exited before listen, rc=" << box.run->rc);
        BOOST_REQUIRE(Clock::now() < until);
        box.endpoint = box.host->bound_endpoint();
        std::this_thread::yield();
    }
    BOOST_REQUIRE(box.App()->State() == fw::AppState::Running);
    BOOST_REQUIRE(box.egress_rt->Create());
    BOOST_REQUIRE(box.egress_rt->Start());
    BOOST_REQUIRE_MESSAGE(box.egress_rt->rpc_client() != nullptr,
                          "host rpc client unavailable");
}

AppBox StartApp(std::chrono::milliseconds incoming_timeout,
                std::chrono::milliseconds step_budget,
                std::vector<fw::StaticRoute> routes,
                std::function<fw::result<void>(fw::CoApp&)> register_fn,
                const std::function<void(AppBox&)>& configure_fn = {}) {
    auto box = PrepareApp(incoming_timeout, step_budget, std::move(routes),
                          std::move(register_fn), configure_fn);
    LaunchApp(box);
    return box;
}

/**
 * ── 测试客户端：独立 runtime + RpcEgress（发往被测 app）──
 */

struct TestClient {
    std::shared_ptr<fw::InfraHttpHost> rt;
    std::shared_ptr<fw::RpcEgress> egress;

    void Stop() {
        /**
         * infra 关闭是同步契约：Close() 返回即物理释放，无需等待收束。
         */
        if (rt)
            rt->Close();
    }
};

TestClient StartClient(inf::NetworkLimits limits) {
    EnsureScheduler();
    TestClient c;
    auto rt = std::make_shared<fw::InfraHttpHost>(
        limits, inf::ListenAddress{"127.0.0.1", 0}, "shutdown-client");
    BOOST_REQUIRE(rt);
    c.rt = rt;
    BOOST_REQUIRE(c.rt->Create());
    BOOST_REQUIRE(c.rt->Start());
    BOOST_REQUIRE(c.rt->rpc_client() != nullptr);
    c.egress = std::make_shared<fw::RpcEgress>(c.rt);
    return c;
}

/**
 * ── 独立 echo peer：裸 infra runtime 上的 V2 端点，回位置参数 v+100
 * （+100 证明回复确由对端处理产生，非本地伪造）。──
 */

struct EchoPeer {
    std::shared_ptr<fw::InfraHttpHost> rt;
    std::string endpoint;
    std::atomic<int> hits{0};

    void Stop() {
        if (rt)
            /**
             * 同步收口：返回即物理释放
             */
            rt->Close();
    }
};

std::unique_ptr<EchoPeer> StartEchoPeer(inf::NetworkLimits limits) {
    EnsureScheduler();
    auto p = std::make_unique<EchoPeer>();
    auto rt = std::make_shared<fw::InfraHttpHost>(
        limits, inf::ListenAddress{"127.0.0.1", 0}, "echo-peer");
    BOOST_REQUIRE(rt);
    p->rt = rt;
    EchoPeer* pp = p.get();
    p->rt->InstallRpcHandler(
        [pp](const inf::rpc::Request& request,
             const std::function<void()>& dispatched)
            -> fw::result<inf::rpc::Response> {
            /**
             * 无挂起动作：先释放同步派发门，再同步处理。
             */
            dispatched();
            pp->hits.fetch_add(1, std::memory_order_acq_rel);
            auto decoded =
                fw::CoRpcReq(request.Payload()).Parse<std::int32_t>();
            if (!decoded)
                return fw::result<inf::rpc::Response>::err(
                    std::move(decoded.error()));
            auto encoded = fw::CoRpcResp::From(decoded.value() + 100);
            if (!encoded.ok())
                return fw::result<inf::rpc::Response>::err(encoded.error());
            return fw::result<inf::rpc::Response>::ok(
                inf::rpc::Response::FromRequest(request)
                    .SetPayload(encoded.payload()));
        });
    BOOST_REQUIRE(p->rt->Create());
    BOOST_REQUIRE(p->rt->Start());
    const auto bound = *p->rt->bound_address();
    p->endpoint = bound.host + ":" + std::to_string(bound.port);
    return p;
}

/**
 * ── 请求构造/收发 ──
 */

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

/**
 * ActorSerial 服务的入站：第一个位置参数为 actor key，第二个为 v。
 */
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

/**
 * 在协程内向被测 app 发一条 V2 Call（Client::Call 是协程阻塞调用）。
 */
fw::result<inf::rpc::Response> SendRpc(
    const std::shared_ptr<fw::RpcEgress>& egress,
    const std::string& endpoint,
    const inf::rpc::Request& env,
    std::chrono::milliseconds budget) {
    inf::CallOptions o;
    o.deadline = Clock::now() + budget;
    return bbt_co_await egress->Call("tcp://" + endpoint, env, o);
}

/**
 * 一次 RPC 调用/派发的共享完成状态：结果与完成信号按值放在堆上，由调用方
 * 与在途协程共同强持有。调用方超等待上限返回（或放弃自己的引用）后，迟到
 * 任务仍写入同一块状态并通知同一个 latch——不存在指向调用者栈对象的引用；
 * latch 的寿命也与任务同生共死（配合 TestLatch 锁内通知，成功等待路径同样
 * 不与通知交叠）。
 */
struct CallState {
    std::optional<fw::result<inf::rpc::Response>> out;
    TestLatch done{1};
};

/**
 * 控制线程发起一次 RPC 并限时等待结果（失败返回 err(InternalError)）。
 * - budget：请求自身的发送期限（与生产出站路径同形）；
 * - state：调用方持有的完成状态（可选；给出后可在超预算返回后观察迟到结果）；
 * - wait_budget：调用者等待上限，默认与 budget 相同。两者分开才能真实构造
 * 「调用者已超时返回、任务仍在途」的寿命窗口。
 * tc.egress / endpoint / 请求 / 期限全部按值或共享捕获，结果写在共享堆状态
 * 上：等待超限或注册失败都只返回错误，不会让在途任务引用调用者栈对象。
 */
fw::result<inf::rpc::Response> CallRpc(const TestClient& tc, const std::string& endpoint, const inf::rpc::Request& env,
                                       std::chrono::milliseconds budget = kWait,
                                       std::shared_ptr<CallState> state = nullptr,
                                       std::optional<std::chrono::milliseconds> wait_budget = std::nullopt) {
    if (!state)
        state = std::make_shared<CallState>();
    const auto wait = wait_budget.value_or(budget);
    auto client = tc.egress;
    auto endpoint_v = endpoint;
    auto request = env;
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [state, client, endpoint_v, request, budget] {
            state->out.emplace(SendRpc(client, endpoint_v, request, budget));
            state->done.CountDown();
        },
        succ);
    /**
     * 注册失败必须显式失败：不静默空过（否则调用方会把它误读成任务结果）。
     */
    if (!succ)
        return fw::result<inf::rpc::Response>::err(
            fw::MakeError(fw::ErrorCode::InternalError, "CallRpc registration failed"));
    if (!state->done.WaitFor(wait))
        return fw::result<inf::rpc::Response>::err(fw::MakeError(
            fw::ErrorCode::InternalError, "CallRpc did not complete"));
    return std::move(*state->out);
}

/**
 * 有界状态轮询（显式状态判据，非 sleep）：直到谓词为真或预算耗尽。
 */
template <class Pred> bool WaitUntil(Pred pred, std::chrono::milliseconds ms) {
    const auto until = Clock::now() + ms;
    while (!pred()) {
        if (Clock::now() > until)
            return false;
        std::this_thread::yield();
    }
    return true;
}

bool WaitState(const fw::CoApp& app, fw::AppState want, std::chrono::milliseconds ms) {
    return WaitUntil([&app, want] { return app.State() == want; }, ms);
}

/**
 * ── INetworkHost 桩：记录调用序列；T6 用于驱动 WaitHandlersDone 预算耗尽路径 ──
 */

class StubNetHost final : public fw::INetworkHost {
public:
    std::function<fw::result<void>(co::Deadline)> on_wait_handlers;
    TestLatch start_seen{1};

    fw::result<void> Create() override {
        _Record("net.create");
        return fw::result<void>::ok();
    }
    fw::result<void> Start() override {
        _Record("net.start");
        start_seen.CountDown();
        return fw::result<void>::ok();
    }
    void StopAccepting() noexcept override { _Record("net.stop_accepting"); }
    fw::result<void> WaitHandlersDone(co::Deadline d) override {
        _Record("net.wait_handlers");
        if (on_wait_handlers)
            return on_wait_handlers(d);
        return fw::result<void>::ok();
    }
    void Close() noexcept override {
        _Record("net.close");
        closed.store(true, std::memory_order_release);
    }
    std::optional<bbt::infra::Error> CloseReason() const noexcept override {
        if (!closed.load(std::memory_order_acquire))
            return std::nullopt;
        return fw::MakeError(fw::ErrorCode::Closed, "host closed");
    }

    std::vector<std::string> Names() const {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_calls;
    }

private:
    void _Record(const char* name) {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_calls.push_back(name);
    }
    mutable std::mutex m_mtx;
    std::vector<std::string> m_calls;
    std::atomic<bool> closed{false};
};

BOOST_AUTO_TEST_SUITE(framework_f3)

/**
 * T1：外部控制线程 RequestShutdown 生效；正常关闭完成 run 返回 0。
 */
BOOST_AUTO_TEST_CASE(request_shutdown_external_thread_clean_rc0) {
    g_probe = std::make_shared<Probe>(0);
    auto box = StartApp(std::chrono::milliseconds{30000},
                        std::chrono::milliseconds{5000},
                        /*routes*/ {},
                        [](fw::CoApp& app) {
                            return bbt_co_await app.CoInstallService<RelaySvc>(ConcurrentOpts());
                        });
    auto tc = StartClient(Limits(std::chrono::milliseconds{30000}));

    /**
     * 正路径对照：运行中真实请求可达。
     */
    auto r = CallRpc(tc, box.endpoint, MakeEnv("relay", "ping", "t1-a", 7));
    BOOST_REQUIRE(r);
    BOOST_CHECK(DecodeReply(r).value() == 7);
    BOOST_CHECK(box.App()->State() == fw::AppState::Running);

    /**
     * 关闭请求来自独立于受管任务与测试主线程的外部线程。
     */
    std::thread supervisor([&box] { box.App()->RequestShutdown(); });
    supervisor.join();

    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == kExitOk);
    BOOST_CHECK(box.App()->State() == fw::AppState::Stopped);
    BOOST_CHECK(box.App()->PendingCleanup().empty());
    BOOST_CHECK(box.App()->LifecycleFailures().empty());
    tc.Stop();
}

/**
 * T2：关闭期间在途 handler 仍可完成真实出站。
 */
BOOST_AUTO_TEST_CASE(inflight_egress_during_closing) {
    g_probe = std::make_shared<Probe>(1);
    auto box = StartApp(
        std::chrono::milliseconds{30000},
        std::chrono::milliseconds{5000},
        /*routes*/ {fw::StaticRoute{"echo", "peer"}},
        [](fw::CoApp& app) {
            return bbt_co_await app.CoInstallService<RelaySvc>(ConcurrentOpts());
        });
    /**
     * echo peer 只能在调度器起来后创建；真实 endpoint 回填解析槽。
     */
    auto peer = StartEchoPeer(Limits(std::chrono::milliseconds{30000}));
    box.peer_ep->Set(peer->endpoint);
    auto tc = StartClient(Limits(std::chrono::milliseconds{30000}));

    /**
     * 在途请求：handler 挂起在 gate 上（真 in-flight）。
     */
    auto inflight_done = std::make_shared<TestLatch>(1);
    auto inflight_res = std::make_shared<std::optional<fw::result<inf::rpc::Response>>>();
    bbtco [tc_egress = tc.egress, endpoint = box.endpoint, inflight_done, inflight_res]() {
        inflight_res->emplace(
            SendRpc(tc_egress, endpoint,
                    MakeEnv("relay", "wait_dial", "t2-h", 9), kWait));
        inflight_done->CountDown();
    };
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));

    /**
     * 外部线程请求关闭；等状态机确实进入 Stopping（在途 handler 未排空，
     * 生命周期停在 WaitHandlersDone，不会滑到 Stopped）。
     */
    std::thread supervisor([&box] { box.App()->RequestShutdown(); });
    supervisor.join();
    BOOST_REQUIRE(WaitState(*box.App(), fw::AppState::Stopping, kWait));
    /**
     * 受管任务仍在收口内
     */
    BOOST_CHECK(!box.run->done.load());

    /**
     * 放行：handler 在关闭序列内发起真实出站——调度器未被强停、出站
     * client 所在 runtime 未 Close、服务实例仍存活。
     */
    g_probe->CompleteGate();
    BOOST_REQUIRE(inflight_done->WaitFor(kWait));
    BOOST_REQUIRE(inflight_res->has_value());
    /**
     * 关闭期间在途 handler 被放行后仍完成真实出站（应答码非假装）——
     * 以 handler 侧 outbound:ok 事件与 peer 命中为证。infra 同步 Close 契约
     * （owner Close 丢弃未发送响应、不做 flush）下，回复不保证送达原客户端，
     * 故不以其为断言对象。
     */
    BOOST_CHECK(g_probe->HasEvent("outbound:ok"));
    BOOST_CHECK(peer->hits.load() == 1);

    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == kExitOk);
    BOOST_CHECK(box.App()->State() == fw::AppState::Stopped);
    tc.Stop();
    peer->Stop();
}

/**
 * T3：早于启动的 RequestShutdown → 启动完成后立即进入关闭序列。
 */
BOOST_AUTO_TEST_CASE(request_shutdown_before_start) {
    g_probe = std::make_shared<Probe>(0);
    auto box = PrepareApp(std::chrono::milliseconds{30000},
                          std::chrono::milliseconds{5000},
                          /*routes*/ {},
                          [](fw::CoApp& app) {
                              return bbt_co_await app.CoInstallService<RelaySvc>(ConcurrentOpts());
                          });
    /**
     * 请求在启动完成前置位：CoWaitShutdownRequest 立即返回，随即进入
     * 关闭序列。
     */
    auto started = box.run->started.get_future();
    BOOST_REQUIRE(started.wait_for(kWait) == std::future_status::ready);
    BOOST_REQUIRE(started.get());
    box.App()->RequestShutdown();

    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == kExitOk);
    BOOST_CHECK(box.App()->State() == fw::AppState::Stopped);
    BOOST_CHECK(box.App()->PendingCleanup().empty());
}

/**
 * T4：预算耗尽 → StopIncomplete → actor 在途 handler 迟到完成真实出站
 * → 资源强持有证据 → 再次推进收口（曾超预算如实记录）。
 */
BOOST_AUTO_TEST_CASE(shutdown_incomplete_actor_late_egress) {
    g_probe = std::make_shared<Probe>(1);
    /**
     * 首次 CoStop 后等测试放行再推进第二次收口（保持「未返回」断言窗口）。
     */
    auto box = StartApp(
        std::chrono::milliseconds{30000},
        /**
         * 紧关闭预算：必然耗尽
         */
        std::chrono::milliseconds{300},
        /*routes*/ {fw::StaticRoute{"echo", "peer"}},
        [](fw::CoApp& app) {
            return bbt_co_await app.CoInstallService<AcctRelaySvc>(ActorOpts());
        },
        [](AppBox& b) { b.after_first_stop = std::make_shared<CoGate>(); });
    auto peer = StartEchoPeer(Limits(std::chrono::milliseconds{30000}));
    box.peer_ep->Set(peer->endpoint);
    auto tc = StartClient(Limits(std::chrono::milliseconds{30000}));

    /**
     * actor 在途请求挂起：mailbox drain 协程持有执行资格。
     */
    auto inflight_done = std::make_shared<TestLatch>(1);
    auto inflight_res = std::make_shared<std::optional<fw::result<inf::rpc::Response>>>();
    bbtco [tc_egress = tc.egress, endpoint = box.endpoint, inflight_done, inflight_res]() {
        inflight_res->emplace(
            SendRpc(tc_egress, endpoint,
                    MakeAcctEnv("park_dial", "t4-h", 3), kWait));
        inflight_done->CountDown();
    };
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));

    box.App()->RequestShutdown();
    /**
     * 预算耗尽 → StopIncomplete：可观察状态 + 失败清单（排空未证实）。
     */
    BOOST_REQUIRE(
        WaitState(*box.App(), fw::AppState::StopIncomplete, kWait));
    const auto failures = box.App()->LifecycleFailures();
    BOOST_REQUIRE(!failures.empty());
    bool drain_failed = false;
    for (const auto& f : failures)
        if (f.phase == "wait_handlers_done")
            drain_failed = true;
    BOOST_CHECK(drain_failed);

    /**
     * 契约要求：受管任务仍在收口窗口；App 持续强持有未收束
     * Service/net::Engine；出站 runtime 与 Scheduler 保留（未被强停）。
     */
    BOOST_CHECK(!box.run->done.load());
    BOOST_CHECK(g_scheduler->IsInitialized());
    auto rt = box.egress_rt->egress_engine();
    BOOST_REQUIRE(rt);
    /**
     * 出站 I/O 驱动仍在（测试缝出站会话 owner 独立，不受服务端宿主关闭影响）
     */
    BOOST_CHECK(rt->Phase() == inf::net::EnginePhase::Running);
    /**
     * 服务实例仍强持有（StopIncomplete 语义：对象不销毁，留待再次收口）。
     */
    auto svc = box.App()->FindService("acct");
    BOOST_REQUIRE(svc);
    BOOST_CHECK(svc.value().IsValid());

    /**
     * 迟到收尾放行：actor handler 在 StopIncomplete 期间完成真实
     * 出站——晚到操作访问的一切资源（协程、ctx、实例、egress、对端）
     * 必须仍有效。
     */
    g_probe->CompleteGate();
    BOOST_REQUIRE(inflight_done->WaitFor(kWait));
    BOOST_REQUIRE(inflight_res->has_value());
    /**
     * actor 在途 handler 迟到完成真实出站，应答码来自 peer，不是
     * 假装——以 handler 侧 actor_outbound:ok 事件与 peer 命中为证。infra
     * 同步 Close 丢弃未发送响应，回复不保证送达原客户端。
     */
    for (const auto& e : g_probe->events)
        BOOST_TEST_MESSAGE("event=" << e);
    /**
     * actor 出站完成可能晚于客户端终态（客户端已先见传输层终态）：
     * 有界等事件而不是即时读，避免断言窗口竞态。
     */
    BOOST_REQUIRE(g_probe->WaitEventPrefix("actor_outbound:", kWait));
    for (const auto& e : g_probe->events)
        BOOST_TEST_MESSAGE("event=" << e);
    BOOST_CHECK(g_probe->HasEvent("actor_outbound:ok"));
    BOOST_CHECK(peer->hits.load() == 1);

    /**
     * 迟到操作全部结束后放行第二次推进：完整收束，rc 非零
     * （记录曾超预算），不是「优雅关闭成功」。
     */
    box.after_first_stop->Open();
    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == kExitStopIncomplete);
    BOOST_CHECK(box.App()->State() == fw::AppState::Stopped);
    BOOST_CHECK(box.App()->PendingCleanup().empty());
    tc.Stop();
    peer->Stop();
}

/**
 * T5：逻辑请求结束 ≠ 物理清理完成——handler 物理在途超过 incoming 看门，
 * 客户端已见 OutcomeUnknown；排空仍等物理结束 → StopIncomplete →
 * 迟到回调资源安全 → 再次推进收口。
 */
BOOST_AUTO_TEST_CASE(late_callback_past_logical_end) {
    g_probe = std::make_shared<Probe>(1);
    auto box = StartApp(
        /**
         * 紧 incoming 看门
         */
        std::chrono::milliseconds{400},
        /**
         * 紧关闭预算
         */
        std::chrono::milliseconds{300},
        /*routes*/ {fw::StaticRoute{"echo", "peer"}},
        [](fw::CoApp& app) {
            return bbt_co_await app.CoInstallService<RelaySvc>(ConcurrentOpts());
        },
        [](AppBox& b) { b.after_first_stop = std::make_shared<CoGate>(); });
    auto peer = StartEchoPeer(Limits(std::chrono::milliseconds{30000}));
    box.peer_ep->Set(peer->endpoint);
    auto tc = StartClient(Limits(std::chrono::milliseconds{400}));

    /**
     * handler 以无界期限物理挂起：incoming 看门（400ms）先于 handler
     * 结束到期 → infra 切断回复路径；请求已完整写出但无可信回复，客户端
     * 见 OutcomeUnknown（逻辑结束），但物理 handler 协程仍在途。
     */
    auto inflight_done = std::make_shared<TestLatch>(1);
    auto inflight_res = std::make_shared<std::optional<fw::result<inf::rpc::Response>>>();
    bbtco [tc_egress = tc.egress, endpoint = box.endpoint, inflight_done, inflight_res]() {
        inflight_res->emplace(
            SendRpc(tc_egress, endpoint,
                    MakeEnv("relay", "wait_dial_long", "t5-h", 5),
                    std::chrono::milliseconds{10000}));
        inflight_done->CountDown();
    };
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));
    /**
     * 逻辑结束：回复路径在看门时刻被切断。
     */
    BOOST_REQUIRE(inflight_done->WaitFor(kWait));
    BOOST_REQUIRE(inflight_res->has_value());
    BOOST_REQUIRE(!inflight_res->value());
    const auto& error = inflight_res->value().error();
    BOOST_CHECK_EQUAL(static_cast<int>(error.code),
                      static_cast<int>(fw::ErrorCode::OutcomeUnknown));
    BOOST_REQUIRE(error.request_phase.has_value());
    BOOST_CHECK_EQUAL(static_cast<int>(*error.request_phase),
                      static_cast<int>(inf::RequestPhase::RequestCommitted));
    /**
     * 物理仍在途
     */
    BOOST_CHECK(!g_probe->HasEvent("late_handler_resumed"));

    box.App()->RequestShutdown();
    /**
     * 物理在途未排空 → 预算耗尽 → StopIncomplete。
     */
    BOOST_REQUIRE(
        WaitState(*box.App(), fw::AppState::StopIncomplete, kWait));
    BOOST_CHECK(!box.run->done.load());
    BOOST_CHECK(g_scheduler->IsInitialized());
    BOOST_CHECK(box.egress_rt->egress_engine() &&
                box.egress_rt->egress_engine()->Phase() == inf::net::EnginePhase::Running);
    /**
     * 服务实例仍强持有（迟到回调稍后还要访问它）。
     */
    auto svc = box.App()->FindService("relay");
    BOOST_REQUIRE(svc);
    BOOST_CHECK(svc.value().IsValid());

    /**
     * 放行迟到 handler：协程恢复、上下文/实例/出站注入点必须仍有效
     * （实现若提前释放即 UAF/崩溃或 RuntimeUnavailable）。ctx 已过期
     * → 适配器在发起 I/O 前 TimedOut，peer 不收任何请求。
     */
    g_probe->CompleteGate();
    const auto until = Clock::now() + kWait;
    while (!g_probe->HasEvent("late_outbound:code" +
                              std::to_string(static_cast<int>(fw::ErrorCode::TimedOut)))) {
        if (g_probe->HasEvent("late_outbound:ok"))
            BOOST_FAIL("late outbound unexpectedly succeeded");
        BOOST_REQUIRE(Clock::now() < until);
        std::this_thread::yield();
    }
    BOOST_CHECK(g_probe->HasEvent("late_handler_resumed"));
    BOOST_CHECK(peer->hits.load() == 0);

    box.after_first_stop->Open();
    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == kExitStopIncomplete);
    BOOST_CHECK(box.App()->State() == fw::AppState::Stopped);
    BOOST_CHECK(box.App()->PendingCleanup().empty());
    tc.Stop();
    peer->Stop();
}

/**
 * T6：WaitHandlersDone 步预算耗尽（桩，无 socket）——首次等待获有限步预算
 * 超时 → 记失败 + 宿主同步 Close → StopIncomplete；再次推进（宿主已
 * Close，不重排空）收口至 Stopped。
 */
BOOST_AUTO_TEST_CASE(wait_handlers_budget_exhaustion) {
    EnsureScheduler();
    auto host = std::make_shared<StubNetHost>();
    auto wait_calls = std::make_shared<std::atomic<int>>(0);
    auto first_deadline_finite = std::make_shared<std::atomic<bool>>(false);
    host->on_wait_handlers =
        [wait_calls, first_deadline_finite](co::Deadline d) -> fw::result<void> {
        const auto call = wait_calls->fetch_add(1);
        if (call == 0) {
            /**
                 * 预算内等待：期限必须是有限步预算（约 step_budget 量级）。
                 */
            first_deadline_finite->store(
                d < Clock::now() + std::chrono::hours{1},
                std::memory_order_release);
            return fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::TimedOut, "handlers still running"));
        }
        return fw::result<void>::ok();
    };

    /**
     * 桩宿主无出站需求：测试缝只换 INetworkHost，rpc_send 留空。
     * 生命周期入口在受管协程执行（与 ServerTaskBody 同纪律）。
     */
    auto done = std::make_shared<TestLatch>(1);
    auto rc_out = std::make_shared<std::atomic<int>>(-1);
    auto final_state = std::make_shared<std::atomic<fw::AppState>>(fw::AppState::Created);
    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [host, done, rc_out, final_state] {
            auto set_rc = [&](int rc) {
                rc_out->store(rc);
                done->CountDown();
            };
            try {
                auto opts = AppOpts(
                    Limits(std::chrono::milliseconds{30000}),
                    std::chrono::milliseconds{300}, {}, "shutdown-stub");
                auto app = fw::MakeCoAppForTest(opts, fw::CoAppSeam{host, {}});
                auto r = bbt_co_await app->CoInstallService<RelaySvc>(
                    ConcurrentOpts());
                if (!r) {
                    set_rc(kExitStartFailed);
                    return;
                }
                r = bbt_co_await app->CoStart();
                if (!r) {
                    set_rc(kExitStartFailed);
                    return;
                }
                app->RequestShutdown();
                {
                    auto _r = bbt_co_await app->CoStop();
                    (void)_r;
                }
                if (app->State() != fw::AppState::StopIncomplete) {
                    set_rc(kExitStartFailed);
                    return;
                }
                /**
                 * 再次推进：宿主已 Close → 不重排空直接收口。
                 */
                {
                    auto _r = bbt_co_await app->CoStop();
                    (void)_r;
                }
                final_state->store(app->State(), std::memory_order_release);
                set_rc(app->State() == fw::AppState::Stopped
                           ? kExitStopIncomplete
                           : kExitStartFailed);
            } catch (...) {
                set_rc(kExitStartFailed);
            }
        },
        submitted);
    BOOST_REQUIRE(submitted);

    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));
    BOOST_REQUIRE(done->WaitFor(kWait));
    BOOST_CHECK(rc_out->load() == kExitStopIncomplete);
    BOOST_CHECK(final_state->load() == fw::AppState::Stopped);
    BOOST_CHECK(first_deadline_finite->load());
    /**
     * 一次预算内 wait + close；第二次推进不重排空（wait_calls 恒 1）。
     */
    BOOST_CHECK(wait_calls->load() == 1);
    const auto names = host->Names();
    auto pos = [&names](const char* n) {
        return std::find(names.begin(), names.end(), n) - names.begin();
    };
    BOOST_CHECK(pos("net.stop_accepting") < pos("net.wait_handlers"));
    BOOST_CHECK(pos("net.wait_handlers") < pos("net.close"));
}

/**
 * T8：CallRpc 超等待上限返回后任务迟到完成（无栈借用，f3 侧同根判别回归）。
 *
 * 构造（与入站用例的迟到完成用例同形）：请求的发送期限（5s）远长于调用者等待上限
 * （150ms），relay/wait_dial handler 停在 probe 门上（ctx 预算 30s）。调用者
 * 必然先超限返回——任务在门放行前不可能完成——随后放行 handler：迟到任务在
 * CallRpc 已返回之后把结果写进共享堆状态并通知 latch。全部等待均为有界谓词
 * 等待，不引入固定等待，也不做无界 join。
 */
BOOST_AUTO_TEST_CASE(callrpc_late_completion_after_wait_timeout) {
    using std::chrono::milliseconds;
    g_probe = std::make_shared<Probe>(1);
    auto box = StartApp(milliseconds{30000}, milliseconds{2000},
                        /*routes*/ {fw::StaticRoute{"echo", "peer"}},
                        [](fw::CoApp& app) { return bbt_co_await app.CoInstallService<RelaySvc>(ConcurrentOpts()); });
    auto peer = StartEchoPeer(Limits(milliseconds{30000}));
    box.peer_ep->Set(peer->endpoint);
    auto tc = StartClient(Limits(milliseconds{30000}));

    auto state = std::make_shared<CallState>();
    auto r = CallRpc(tc, box.endpoint, MakeEnv("relay", "wait_dial", "t8-late", 21),
                     /*budget*/ milliseconds{5000}, state,
                     /*wait_budget*/ milliseconds{150});
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == fw::ErrorCode::InternalError);
    /**
     * 请求已真实进 handler 并停在门上：任务确在途（不是尚未被调度）；此刻
     * 调用者栈帧早已退出。
     */
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));
    BOOST_CHECK(!box.run->done.load());

    /**
     * 放行 handler：迟到任务完成真实出站（到 echo peer）并写入共享状态。
     */
    g_probe->CompleteGate();
    BOOST_REQUIRE(state->done.WaitFor(kWait));
    BOOST_REQUIRE(state->out.has_value());
    BOOST_REQUIRE(state->out.value());
    BOOST_CHECK(state->out.value().value().RequestId() == "t8-late");
    /**
     * echo peer 对每个位置参数 +100：回复值 121 证明结果确由迟到任务收妥。
     */
    auto rep = DecodeReply(state->out.value());
    BOOST_REQUIRE(rep);
    BOOST_CHECK(rep.value() == 121);
    BOOST_CHECK(peer->hits.load() == 1);

    /**
     * 迟到任务收束后应用仍正常服务（未因栈借用/悬垂写入受损）。
     */
    auto again = CallRpc(tc, box.endpoint, MakeEnv("relay", "ping", "t8-after", 3));
    BOOST_REQUIRE(again);
    auto rep2 = DecodeReply(again);
    BOOST_REQUIRE(rep2);
    BOOST_CHECK(rep2.value() == 3);

    box.App()->RequestShutdown();
    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == kExitOk);
    tc.Stop();
    peer->Stop();
}

/**
 * T7：V2 正式入站的跨层预算——真实 infra CoRpcServer 入站 + 真实 V2 Call
 * 出站，以**服务端同钟口径**钉死 framework 在 infra local-min 之后恰好扣一次
 * kReplyDeliveryReserve(20ms)。
 */
BOOST_AUTO_TEST_CASE(v2_inbound_reply_reserve_exact) {
    g_probe = std::make_shared<Probe>(0);
    g_budget_calls.store(0);
    g_budget_deadline_ns.store(0);
    g_inbound_entries.store(0);
    g_inbound_pre_deadline_ns.store(0);
    g_inbound_error_code.store(0);
    g_inbound_hold_until_deadline.store(false);

    auto box =
        PrepareApp(std::chrono::milliseconds{30000}, std::chrono::milliseconds{5000},
                   /*routes*/ {}, [](fw::CoApp& app) { return bbt_co_await app.CoInstallService<BudgetSvc>(ConcurrentOpts()); }, [](AppBox& b) { InstallBudgetProbeHandler(b.host, b.app_slot); });
    LaunchApp(box);
    BOOST_REQUIRE(box.host->rpc_server() != nullptr);
    BOOST_CHECK(box.host->bound_address().has_value());
    auto tc = StartClient(Limits(std::chrono::milliseconds{30000}));

    /**
     * (a) 充裕预算 B=3000ms：handler 执行；pre（local-min 后、扣减前）与
     * post（扣减后）同钟相减恰为 20ms —— 恰一次、扣在 local-min 之后。
     */
    const auto before = Clock::now();
    auto r_a = CallRpc(tc, box.endpoint,
                       MakeEnv("budget", "probe", "t7-a", 11),
                       std::chrono::milliseconds{3000});
    if (!r_a)
        BOOST_TEST_MESSAGE("t7-a error code=" << static_cast<int>(r_a.error().code)
                                              << " domain=" << r_a.error().domain
                                              << " msg=" << r_a.error().message);
    BOOST_REQUIRE(r_a);
    BOOST_CHECK_EQUAL(r_a.value().RequestId(), "t7-a");
    auto rep_a = DecodeReply(r_a);
    BOOST_REQUIRE(rep_a);
    BOOST_CHECK(rep_a.value() == 11);
    BOOST_CHECK(g_inbound_entries.load() == 1);
    BOOST_CHECK(g_budget_calls.load() == 1);

    const auto pre = std::chrono::steady_clock::time_point{
        std::chrono::nanoseconds{g_inbound_pre_deadline_ns.load()}};
    const auto post = std::chrono::steady_clock::time_point{
        std::chrono::nanoseconds{g_budget_deadline_ns.load()}};
    /**
     * 核心判别：同钟精确 20ms（0=不扣、40=双扣均被排除）。
     */
    BOOST_CHECK(pre - post == std::chrono::milliseconds{20});
    /**
     * 服务端 local-min 采用客户端预算（≈B），非 B+reserve——证明预留施加在
     * local-min **之后**而非之前；±500ms 仅吸收 loopback/调度抖动。
     */
    const auto delta = pre - before;
    BOOST_CHECK(delta >= std::chrono::milliseconds{3000 - 500});
    BOOST_CHECK(delta <= std::chrono::milliseconds{3000 + 500});

    /**
     * (b) 紧远端预算：wire remaining_budget_ms≈10 → 服务端 local-min 取 ~10ms，
     * framework 入站扣减 20ms 后过期 → 服务端 TimedOut、业务 handler 不执行。
     * 客户端期限同源，可能先超时；只用服务端观测判定。
     */
    auto r_b = CallRpc(tc, box.endpoint,
                       MakeEnv("budget", "probe", "t7-b", 12),
                       std::chrono::milliseconds{10}, nullptr, kWait);
    /**
     * 客户端结果不作断言对象（预算同源，先超时是契约内行为）
     */
    (void)r_b;
    const auto until_b = Clock::now() + kWait;
    while (g_inbound_entries.load() < 2 && Clock::now() < until_b)
        std::this_thread::yield();
    BOOST_CHECK(g_inbound_entries.load() == 2);
    BOOST_CHECK(g_inbound_error_code.load() == static_cast<int>(fw::ErrorCode::TimedOut));
    BOOST_CHECK(g_budget_calls.load() == 1);

    box.App()->RequestShutdown();
    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == kExitOk);
    tc.Stop();
}

/**
 * T7c：20ms 预留的**真实链**边界（V2 入站 + 真实 CoRpcServer）。
 */
BOOST_AUTO_TEST_CASE(v2_inbound_reply_reserve_boundary) {
    using std::chrono::milliseconds;
    g_probe = std::make_shared<Probe>(0);
    g_budget_calls.store(0);
    g_budget_deadline_ns.store(0);
    g_inbound_entries.store(0);
    g_inbound_pre_deadline_ns.store(0);
    g_inbound_entry_ns.store(0);
    g_inbound_error_code.store(0);
    g_inbound_hold_until_deadline.store(false);

    auto box =
        PrepareApp(milliseconds{30000}, milliseconds{5000},
                   /*routes*/ {}, [](fw::CoApp& app) { return bbt_co_await app.CoInstallService<BudgetSvc>(ConcurrentOpts()); }, [](AppBox& b) { InstallBudgetProbeHandler(b.host, b.app_slot); });
    LaunchApp(box);
    BOOST_REQUIRE(box.host->rpc_server() != nullptr);
    auto tc = StartClient(Limits(milliseconds{30000}));

    /**
     * (a) wire 预算恰 ==20ms：接收端 local-min 取 20ms → 扣预留后期限即请求
     * 到达时刻 → 拒绝。客户端预算同源，可能先超时；判定用服务端观测。
     */
    const auto before_a = Clock::now();
    auto r_a = CallRpc(tc, box.endpoint,
                       MakeEnv("budget", "probe", "t7c-eq", 11),
                       milliseconds{20}, nullptr, kWait);
    (void)r_a;
    const auto until_a = Clock::now() + kWait;
    while (g_inbound_entries.load() < 1 && Clock::now() < until_a)
        std::this_thread::yield();
    BOOST_CHECK(g_inbound_entries.load() == 1);
    BOOST_CHECK(g_inbound_error_code.load() == static_cast<int>(fw::ErrorCode::TimedOut));
    BOOST_CHECK(g_budget_calls.load() == 0);
    /**
     * 接收端 local-min 确实采用 wire 预算（20ms 量级），不是服务端 30s 本地期限。
     */
    const auto pre_a = std::chrono::nanoseconds{g_inbound_pre_deadline_ns.load()};
    const auto pre_a_after = pre_a - std::chrono::duration_cast<std::chrono::nanoseconds>(
                                         before_a.time_since_epoch());
    BOOST_CHECK(pre_a_after > milliseconds{0});
    BOOST_CHECK(pre_a_after < milliseconds{500});

    /**
     * (b) wire 预算 200ms，但派发被受控推到「扣除预留后的期限已过」之后：
     * 仍然拒绝——名义预算大于预留不是进入 handler 的充分条件。
     */
    g_inbound_error_code.store(0);
    g_inbound_hold_until_deadline.store(true);
    auto r_b = CallRpc(tc, box.endpoint,
                       MakeEnv("budget", "probe", "t7c-above-rejected", 13),
                       milliseconds{200}, nullptr, kWait);
    (void)r_b;
    const auto until_b = Clock::now() + kWait;
    while (g_inbound_entries.load() < 2 && Clock::now() < until_b)
        std::this_thread::yield();
    g_inbound_hold_until_deadline.store(false);
    /**
     * 等受控耗尽的派发真正落定（错误码发布在 Dispatch 返回之后）。
     */
    while (g_inbound_error_code.load() !=
               static_cast<int>(fw::ErrorCode::TimedOut) &&
           Clock::now() < until_b)
        std::this_thread::yield();
    BOOST_CHECK(g_inbound_entries.load() == 2);
    BOOST_CHECK(g_inbound_error_code.load() ==
                static_cast<int>(fw::ErrorCode::TimedOut));
    BOOST_CHECK(g_budget_calls.load() == 0);

    /**
     * (c) 同一 200ms 预算、无受控耗尽：只剩派发 prologue 与它竞速 → handler 执行，
     * 且期限恰减一次 20ms。
     */
    auto r_c = CallRpc(tc, box.endpoint,
                       MakeEnv("budget", "probe", "t7c-above-ok", 12),
                       milliseconds{200});
    if (!r_c)
        BOOST_TEST_MESSAGE("t7c-above-ok error code="
                           << static_cast<int>(r_c.error().code)
                           << " msg=" << r_c.error().message);
    BOOST_REQUIRE(r_c);
    auto rep_c = DecodeReply(r_c);
    BOOST_REQUIRE(rep_c);
    BOOST_CHECK(rep_c.value() == 12);
    BOOST_CHECK(g_inbound_entries.load() == 3);
    BOOST_CHECK(g_budget_calls.load() == 1);
    const auto pre_c = std::chrono::nanoseconds{g_inbound_pre_deadline_ns.load()};
    const auto post_c = std::chrono::nanoseconds{g_budget_deadline_ns.load()};
    BOOST_CHECK(pre_c - post_c == milliseconds{20});
    BOOST_TEST_MESSAGE("t7c-above-ok handler headroom="
                       << (post_c - std::chrono::nanoseconds{g_inbound_entry_ns.load()}).count() << "ns");

    box.App()->RequestShutdown();
    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == kExitOk);
    tc.Stop();
}

/**
 * ── 多 hop 预算：同一进程同一 CoApp 的两个方法经 loopback socket 相扣 ──
 */

struct HopProbe {
    struct Ingress {
        std::string service;
        std::string method;
        /**
         * request.Context().deadline（infra local-min 结果）
         */
        std::int64_t pre_ns{0};
        /**
         * 记录时刻（同钟）
         */
        std::int64_t at_ns{0};
    };
    static std::int64_t Ns(Clock::time_point t) {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch()).count();
    }
    void Reset() {
        std::lock_guard<std::mutex> lk(mtx);
        ingress.clear();
        handler_deadline_ns.clear();
        adapted_ns = 0;
        widen_requested_ns = 0;
        egress_start_ns = 0;
        egress_ok = false;
        handler_calls = 0;
    }
    void RecordIngress(std::string service, std::string method, co::Deadline pre, Clock::time_point at) {
        std::lock_guard<std::mutex> lk(mtx);
        ingress.push_back(Ingress{std::move(service), std::move(method), Ns(pre), Ns(at)});
    }
    Ingress At(const std::string& method) const {
        std::lock_guard<std::mutex> lk(mtx);
        for (const auto& i : ingress)
            if (i.method == method)
                return i;
        return Ingress{};
    }
    void SetHandlerDeadline(const std::string& role, co::Deadline d) {
        std::lock_guard<std::mutex> lk(mtx);
        handler_deadline_ns[role] = Ns(d);
        handler_calls += 1;
    }
    std::int64_t HandlerDeadline(const std::string& role) const {
        std::lock_guard<std::mutex> lk(mtx);
        const auto it = handler_deadline_ns.find(role);
        return it != handler_deadline_ns.end() ? it->second : 0;
    }
    int HandlerCalls() const {
        std::lock_guard<std::mutex> lk(mtx);
        return handler_calls;
    }
    void SetAdapted(co::Deadline d) {
        std::lock_guard<std::mutex> lk(mtx);
        adapted_ns = Ns(d);
    }
    std::int64_t Adapted() const {
        std::lock_guard<std::mutex> lk(mtx);
        return adapted_ns;
    }
    void SetWidenRequested(co::Deadline d) {
        std::lock_guard<std::mutex> lk(mtx);
        widen_requested_ns = Ns(d);
    }
    std::int64_t WidenRequested() const {
        std::lock_guard<std::mutex> lk(mtx);
        return widen_requested_ns;
    }
    void MarkEgressStart(Clock::time_point t) {
        std::lock_guard<std::mutex> lk(mtx);
        egress_start_ns = Ns(t);
    }
    std::int64_t EgressStart() const {
        std::lock_guard<std::mutex> lk(mtx);
        return egress_start_ns;
    }
    void MarkEgressEnd(bool ok) {
        std::lock_guard<std::mutex> lk(mtx);
        egress_ok = ok;
    }
    bool EgressOk() const {
        std::lock_guard<std::mutex> lk(mtx);
        return egress_ok;
    }

    mutable std::mutex mtx;
    std::vector<Ingress> ingress;
    std::map<std::string, std::int64_t> handler_deadline_ns;
    std::int64_t adapted_ns{0};
    std::int64_t widen_requested_ns{0};
    std::int64_t egress_start_ns{0};
    bool egress_ok{false};
    int handler_calls{0};
};

std::shared_ptr<HopProbe> g_hop;

std::int64_t MsNs(std::chrono::milliseconds d) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(d).count();
}

/**
 * 两跳服务：leaf 为叶子（记录 handler 内可见期限后回值）；forward 记录父 ctx
 * 期限后做一次真实受管出站到 leaf。forward_widen 变体显式给出远大于父预算的
 * 子期限——适配器必须取 min，不得放大父预算（「去掉 min」变异的判别点）。
 */
class HopSvc final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "budget_hop";
    explicit HopSvc(fw::detail::ConstructKey k) : fw::CoService<>(k) {}

    fw::CoRpcResp Leaf(fw::CoRpcReq req) {
        auto v = IntArg(req);
        if (!v)
            return fw::CoRpcResp::Error(v.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        if (g_hop)
            g_hop->SetHandlerDeadline("leaf", ctx.value()->deadline);
        return ReplyInt(v);
    }

    fw::CoRpcResp Forward(fw::CoRpcReq req) {
        return _Forward(req, false);
    }
    fw::CoRpcResp ForwardWiden(fw::CoRpcReq req) {
        return _Forward(req, true);
    }

    static constexpr auto kRpcMethods =
        fw::RpcMethods(fw::Method<&HopSvc::Leaf>("leaf"), fw::Method<&HopSvc::Forward>("forward"),
                       fw::Method<&HopSvc::ForwardWiden>("forward_widen"));

protected:
    ~HopSvc() noexcept override = default;

private:
    fw::CoRpcResp _Forward(fw::CoRpcReq req, bool widen) {
        auto v = IntArg(req);
        if (!v)
            return fw::CoRpcResp::Error(v.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        const std::string role = widen ? "forward_widen" : "forward";
        if (g_hop)
            g_hop->SetHandlerDeadline(role, ctx.value()->deadline);
        fw::CallOptions opt;
        if (widen) {
            opt.deadline = Clock::now() + std::chrono::seconds{30};
            if (g_hop)
                g_hop->SetWidenRequested(*opt.deadline);
        }
        if (g_hop)
            g_hop->MarkEgressStart(Clock::now());
        auto reply = bbt_co_await call(kServiceName, "leaf", req, {}, opt);
        if (g_hop)
            g_hop->MarkEgressEnd(static_cast<bool>(reply));
        if (!reply)
            return fw::CoRpcResp::Error(reply.error());
        return reply.value();
    }
    friend class fw::detail::Registry;
};

/**
 * 两跳观测入站：进入 framework 入站入口之前记录 request.Context().deadline 与
 * 该时刻（同钟），再走真实 DispatchInboundForTest→InboundDispatcher 分发。
 */
void InstallHopProbeHandler(const std::shared_ptr<fw::InfraHttpHost>& host,
                            const std::shared_ptr<std::shared_ptr<fw::CoApp>>& app_slot) {
    host->InstallRpcHandler(
        [app_slot](const inf::rpc::Request& request,
                   const std::function<void()>& dispatched)
            -> fw::result<inf::rpc::Response> {
            if (g_hop)
                g_hop->RecordIngress(request.Service(), request.Method(),
                                     request.Context().deadline, Clock::now());
            dispatched();
            return fw::DispatchInboundForTest(**app_slot, request);
        });
}

/**
 * 多 hop 的 seam 发送实现：与 PrepareApp 默认 rpc_send 同形，但在发起 I/O
 * 之前记录生产 ResolveCallDeadline 已解析的出站期限（观测点），并记录发起时刻。
 */
fw::RpcSendAppFn HopSeamSend(std::shared_ptr<EndpointSlot> eps,
                             std::shared_ptr<fw::RpcEgress> egress) {
    return [eps, egress](std::string_view target,
                         const inf::rpc::Request& env,
                         const inf::CallOptions& o)
               -> bbt_co_async(fw::result<inf::rpc::Response>) {
        if (g_hop)
            g_hop->SetAdapted(o.deadline);
        std::string ep(target);
        constexpr std::string_view kScheme = "tcp://";
        if (ep.rfind(kScheme, 0) == 0)
            ep.erase(0, kScheme.size());
        if (ep == "self" || ep == "peer")
            ep = eps->Get();
        if (ep.empty())
            bbt_co_return fw::result<inf::rpc::Response>::err(
                fw::MakeError(fw::ErrorCode::RuntimeUnavailable, "hop egress self endpoint not installed"));
        bbt_co_return bbt_co_await egress->Call("tcp://" + ep, env, o);
    };
}

/**
 * 两跳夹具：与 PrepareApp 同形（复用 AppBox/Limits/EndpointSlot），差异
 * 只有「出站实现」与「endpoint 解析」两点，见上方口径声明。
 */
AppBox PrepareHopApp(std::chrono::milliseconds incoming_timeout, std::chrono::milliseconds step_budget,
                     std::vector<fw::StaticRoute> routes, std::function<fw::result<void>(fw::CoApp&)> register_fn) {
    AppBox box;
    const auto limits = Limits(incoming_timeout);
    box.host = std::make_shared<fw::InfraHttpHost>(
        limits, inf::ListenAddress{"127.0.0.1", 0}, "hop-server");
    box.egress_rt = std::make_shared<fw::InfraHttpHost>(
        limits, inf::ListenAddress{"127.0.0.1", 0}, "hop-egress");
    box.egress = std::make_shared<fw::RpcEgress>(box.egress_rt);
    /**
     * 本夹具承载「自身 endpoint」
     */
    box.peer_ep = std::make_shared<EndpointSlot>();
    InstallHopProbeHandler(box.host, box.app_slot);

    auto eps = box.peer_ep;
    auto egress = box.egress;
    box.run = std::make_unique<RunHandle>();
    RunHandle* h = box.run.get();
    auto app_slot = box.app_slot;
    auto host = box.host;
    auto after_first_stop = box.after_first_stop;
    EnsureScheduler();
    {
        bool submitted = false;
        g_scheduler->RegistCoroutineTask(
            [app_slot, host, after_first_stop, h,
             register_fn = std::move(register_fn),
             limits, step_budget, routes = std::move(routes),
             eps, egress]() mutable {
                try {
                    *app_slot = fw::MakeCoAppForTest(
                        AppOpts(limits, step_budget, std::move(routes),
                                "hop-server"),
                        fw::CoAppSeam{host, HopSeamSend(eps, egress)});
                } catch (...) {
                    h->rc = kExitStartFailed;
                    h->done.store(true, std::memory_order_release);
                    try {
                        h->started.set_value(false);
                    } catch (...) {}
                    return;
                }
                ServerTaskBody(app_slot, after_first_stop, h, register_fn);
            },
            submitted);
        if (!submitted) {
            h->rc = kExitStartFailed;
            h->done.store(true, std::memory_order_release);
            try {
                h->started.set_value(false);
            } catch (...) {}
        }
    }
    return box;
}

/**
 * T9：真实两跳的预算不放大——A（测试侧真实 V2 出站）→
 * B（本进程 CoApp hop1 入站）→ B 的出站 → B（同一 CoApp hop2 入站）。
 * 断言全部落在同一 steady_clock 的绝对期限上：
 * - 每跳 local-min 之后 framework 恰扣一次 20ms（同钟精确等值）；
 * - 生产 ResolveCallDeadline 后的出站期限 == 父 ctx 期限（无显式期限时继承，
 *   不延长、不放大）；
 * - 到达 hop2 的预算相对父 ctx 期限只多出**实测**单程时延——出站放大（例如
 *   改用新预算、或去掉 min 让显式子期限生效）会远超该实测上界；
 * - 逐跳只缩：d_hop2 < d_hop1；d_hop1 - d_hop2 的窗口是 20ms ± 实测单程，
 *   上界再含 wire 整数毫秒的 <1ms 向下取整余量（0/40ms 仍被排除）。
 * widen 变体显式把子期限设到 30s（远大于父预算 ~3s）：适配器取 min，仍不放大。
 */
BOOST_AUTO_TEST_CASE(v2_multihop_budget_never_amplified) {
    using std::chrono::milliseconds;
    g_hop = std::make_shared<HopProbe>();
    auto box =
        PrepareHopApp(milliseconds{30000}, milliseconds{5000},
                      /*routes*/ {fw::StaticRoute{std::string(HopSvc::kServiceName), "self"}},
                      [](fw::CoApp& app) { return bbt_co_await app.CoInstallService<HopSvc>(ConcurrentOpts()); });
    LaunchApp(box);
    /**
     * 正式 V2 入站
     */
    BOOST_REQUIRE(box.host->rpc_server() != nullptr);
    BOOST_REQUIRE(!box.endpoint.empty());
    /**
     * "self" → 本进程自身监听地址
     */
    box.peer_ep->Set(box.endpoint);
    auto tc = StartClient(Limits(milliseconds{30000}));

    /**
     * (a) 无显式子期限：子预算继承父预算。
     */
    auto r_a = CallRpc(tc, box.endpoint,
                       MakeEnv(std::string(HopSvc::kServiceName), "forward", "mh-a", 11),
                       milliseconds{3000});
    if (!r_a)
        BOOST_TEST_MESSAGE("mh-a error code=" << static_cast<int>(r_a.error().code)
                                              << " msg=" << r_a.error().message);
    BOOST_REQUIRE(r_a);
    auto rep_a = DecodeReply(r_a);
    BOOST_REQUIRE(rep_a);
    BOOST_CHECK(rep_a.value() == 11);

    const auto ing_f = g_hop->At("forward");
    const auto ing_l = g_hop->At("leaf");
    const auto d_f = g_hop->HandlerDeadline("forward");
    const auto d_l = g_hop->HandlerDeadline("leaf");
    /**
     * 第 1 跳：真实 infra local-min → framework 恰扣一次 20ms。
     */
    BOOST_CHECK_NE(d_f, 0);
    BOOST_CHECK(ing_f.pre_ns - d_f == MsNs(milliseconds{20}));
    /**
     * 生产 ResolveCallDeadline 后的出站期限 == 父 ctx 期限（继承，不延长）。
     */
    BOOST_CHECK(g_hop->Adapted() == d_f);
    BOOST_CHECK(g_hop->HandlerCalls() == 2);
    BOOST_CHECK(g_hop->EgressOk());
    /**
     * 第 2 跳：同一 20ms 精确等值。
     */
    BOOST_CHECK_NE(d_l, 0);
    BOOST_CHECK(ing_l.pre_ns - d_l == MsNs(milliseconds{20}));
    /**
     * 实测单程（hop1 发起出站 → hop2 抵达入站包装）：本用例唯一的时延量。
     */
    const auto hop_latency = ing_l.at_ns - g_hop->EgressStart();
    BOOST_CHECK_MESSAGE(hop_latency > 0, "hop latency not observed");
    BOOST_CHECK_MESSAGE(hop_latency < MsNs(milliseconds{15}),
                        "hop latency too large for discriminating power: " << hop_latency << "ns");
    /**
     * 到达第 2 跳的预算相对父 ctx 期限只多出实测单程 → 出站未放大预算。
     */
    BOOST_TEST_MESSAGE("mh-a hop_latency=" << hop_latency << "ns arrival_excess_over_parent=" << (ing_l.pre_ns - d_f)
                                           << "ns drop_d1_minus_d2=" << (d_f - d_l) << "ns");
    BOOST_CHECK(ing_l.pre_ns - d_f <= hop_latency);
    /**
     * wire 向下取整为整数毫秒：量化余量 <1ms 可使扣减量大于 20ms。
     * 逐跳只缩；窗口包含实测单程及该量化项，0/40ms 仍在窗口外。
     */
    const auto wire_quantization = MsNs(milliseconds{1});
    BOOST_CHECK(d_l < d_f);
    BOOST_CHECK(d_f - d_l >= MsNs(milliseconds{20}) - hop_latency);
    BOOST_CHECK(d_f - d_l <= MsNs(milliseconds{20}) + hop_latency + wire_quantization);

    /**
     * (b) 显式子期限 = now + 30s（远大于父预算 ~3s）：父预算仍不可放大。
     */
    g_hop->Reset();
    auto r_b = CallRpc(tc, box.endpoint,
                       MakeEnv(std::string(HopSvc::kServiceName), "forward_widen", "mh-b", 12),
                       milliseconds{3000});
    if (!r_b)
        BOOST_TEST_MESSAGE("mh-b error code=" << static_cast<int>(r_b.error().code)
                                              << " msg=" << r_b.error().message);
    BOOST_REQUIRE(r_b);
    auto rep_b = DecodeReply(r_b);
    BOOST_REQUIRE(rep_b);
    BOOST_CHECK(rep_b.value() == 12);

    const auto ing_fb = g_hop->At("forward_widen");
    const auto ing_lb = g_hop->At("leaf");
    const auto d_fb = g_hop->HandlerDeadline("forward_widen");
    const auto d_lb = g_hop->HandlerDeadline("leaf");
    /**
     * 显式请求确实远大于父预算（否则本变体不具判别力）。
     */
    BOOST_CHECK(g_hop->WidenRequested() - d_fb > MsNs(milliseconds{5000}));
    /**
     * 适配器取 min：出站期限仍是父预算，不是显式 30s。
     */
    BOOST_CHECK(g_hop->Adapted() == d_fb);
    BOOST_CHECK(ing_fb.pre_ns - d_fb == MsNs(milliseconds{20}));
    BOOST_CHECK(ing_lb.pre_ns - d_lb == MsNs(milliseconds{20}));
    const auto hop_latency_b = ing_lb.at_ns - g_hop->EgressStart();
    BOOST_CHECK_MESSAGE(hop_latency_b < MsNs(milliseconds{15}),
                        "hop latency too large for discriminating power: " << hop_latency_b << "ns");
    BOOST_TEST_MESSAGE("mh-b hop_latency=" << hop_latency_b
                                           << "ns arrival_excess_over_parent=" << (ing_lb.pre_ns - d_fb)
                                           << "ns drop_d1_minus_d2=" << (d_fb - d_lb) << "ns");
    BOOST_CHECK(ing_lb.pre_ns - d_fb <= hop_latency_b);
    BOOST_CHECK(d_lb < d_fb);

    box.App()->RequestShutdown();
    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == kExitOk);
    tc.Stop();
}

/**
 * ── 复审修复点的定向回归（入站保序/出站/关闭/封口结果处理）──
 */

/**
 * T10（F1+F2）：合法的较小限额 + 仅 2 workers（无出站 Engine 槽）的
 * 纯入站 V2 宿主必须 Create/Start 成功并保持既有配置可启动。
 */
BOOST_AUTO_TEST_CASE(v2_host_small_limits_start) {
    EnsureScheduler();
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    inf::NetworkLimits limits{
        /*max_connections*/ 16,
        /*max_inflight*/ 16,
        /**
         * 合法小限额
         */
        /*max_header_bytes*/ 4096,
        /*max_body_bytes*/ 8192,
        std::chrono::milliseconds{30000}};
    auto host = std::make_shared<fw::InfraHttpHost>(
        limits, inf::ListenAddress{"127.0.0.1", 0}, "shutdown-small");
    host->InstallRpcHandler(
        [](const inf::rpc::Request& request,
           const std::function<void()>& dispatched)
            -> fw::result<inf::rpc::Response> {
            dispatched();
            return fw::result<inf::rpc::Response>::ok(
                inf::rpc::Response::FromRequest(request));
        });
    auto created = host->Create();
    if (!created)
        BOOST_TEST_MESSAGE("Create failed: " << created.error().message);
    BOOST_REQUIRE(created);
    auto started = host->Start();
    if (!started)
        BOOST_TEST_MESSAGE("Start failed: " << started.error().message);
    BOOST_REQUIRE(started);
    /**
     * 真实监听就绪（内核分配端口）。
     */
    BOOST_REQUIRE(host->bound_address().has_value());
    BOOST_CHECK(host->bound_address().value().port != 0);
    BOOST_CHECK(host->rpc_server() != nullptr);

    /**
     * 完整关闭序列；正常路径封口无失败事实。
     */
    host->StopAccepting();
    auto drained = host->WaitHandlersDone(
        Clock::now() + std::chrono::milliseconds{5000});
    BOOST_CHECK(drained);
    host->Close();
    BOOST_CHECK(!host->close_error().has_value());
}

/**
 * T11（F4）：Run 提前返回（端口已被占用）时 Start 必须及时返回错误，
 * 而不是等满相位窗口——Run 结果经 promise/future 跨线程发布，wait_for(0)
 * 即刻可见。
 */
BOOST_AUTO_TEST_CASE(v2_engine_start_occupied_port_fails_fast) {
    EnsureScheduler();
    BOOST_REQUIRE(g_scheduler->IsInitialized());
    auto limits = Limits(std::chrono::milliseconds{30000});

    auto handler = [](const inf::rpc::Request& request,
                      const std::function<void()>& dispatched)
        -> fw::result<inf::rpc::Response> {
        dispatched();
        return fw::result<inf::rpc::Response>::ok(
            inf::rpc::Response::FromRequest(request));
    };

    auto ok_host = std::make_shared<fw::InfraHttpHost>(
        limits, inf::ListenAddress{"127.0.0.1", 0}, "shutdown-port-a");
    ok_host->InstallRpcHandler(handler);
    BOOST_REQUIRE(ok_host->Create());
    BOOST_REQUIRE(ok_host->Start());
    const auto bound = ok_host->bound_address();
    BOOST_REQUIRE(bound.has_value());

    /**
     * 第二宿主同地址监听：Engine::Run 绑定必失败且提前返回。
     */
    auto dup_host = std::make_shared<fw::InfraHttpHost>(
        limits, inf::ListenAddress{"127.0.0.1", bound.value().port},
        "shutdown-port-b");
    dup_host->InstallRpcHandler(handler);
    BOOST_REQUIRE(dup_host->Create());
    const auto t0 = Clock::now();
    auto r = dup_host->Start();
    const auto elapsed = Clock::now() - t0;
    BOOST_REQUIRE(!r);
    /**
     * 判别窗口：修复前等满 kEngineRunWait(5000ms)；修复后 Run 返回即刻可见。
     * 取 3000ms 保留充足调度余量，仍与 5000ms 超时路径分离。
     */
    BOOST_CHECK(elapsed < std::chrono::milliseconds{3000});
    dup_host->Close();
    ok_host->StopAccepting();
    BOOST_CHECK(ok_host->WaitHandlersDone(
        Clock::now() + std::chrono::milliseconds{5000}));
    ok_host->Close();
}

/**
 * T12：V2 出站恢复 runtime 限额 owner 的请求字节校验——超限请求在发送侧
 * 被拒，远端 handler 零触达。
 */
BOOST_AUTO_TEST_CASE(v2_egress_body_limit_rejected_remote_untouched) {
    EnsureScheduler();
    BOOST_REQUIRE(g_scheduler->IsInitialized());
    g_probe = std::make_shared<Probe>(0);
    g_budget_calls.store(0);
    g_inbound_entries.store(0);

    /**
     * 被测 app：V2 入站 + 观测 handler（入口计数 g_inbound_entries）。
     */
    auto box =
        PrepareApp(std::chrono::milliseconds{30000}, std::chrono::milliseconds{5000},
                   /*routes*/ {}, [](fw::CoApp& app) { return bbt_co_await app.CoInstallService<BudgetSvc>(ConcurrentOpts()); }, [](AppBox& b) { InstallBudgetProbeHandler(b.host, b.app_slot); });
    LaunchApp(box);
    BOOST_REQUIRE(box.host->rpc_server() != nullptr);

    /**
     * 出站宿主：把 max_body_bytes 压到 V2 帧长下限（1024）。payload 本身超过
     * 1024B，必须在发起任何 I/O 前被 RpcEgress 拒绝。
     */
    inf::NetworkLimits caller_limits{
        /*max_connections*/ 16,
        /*max_inflight*/ 16,
        /*max_header_bytes*/ 16384,
        /*max_body_bytes*/ 1024,
        std::chrono::milliseconds{30000}};
    auto caller_rt = std::make_shared<fw::InfraHttpHost>(
        caller_limits, inf::ListenAddress{"127.0.0.1", 0}, "shutdown-big");
    BOOST_REQUIRE(caller_rt->Create());
    /**
     * 纯出站宿主：不监听
     */
    BOOST_REQUIRE(caller_rt->Start());
    TestClient caller;
    caller.rt = caller_rt;
    caller.egress = std::make_shared<fw::RpcEgress>(caller_rt);

    auto env = MakeEnv("budget", "probe", "t12-big", 7);
    env.SetPayload(std::vector<std::uint8_t>(2048, 0x42));
    auto r = CallRpc(caller, box.endpoint, env, std::chrono::milliseconds{5000});
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == fw::ErrorCode::InvalidArgument);
    /**
     * 远端 handler 零触达：拒绝发生在发起 I/O 之前（不止探针 rc，看远端计数）。
     */
    BOOST_CHECK(g_inbound_entries.load() == 0);
    BOOST_CHECK(g_budget_calls.load() == 0);

    caller.Stop();
    box.App()->RequestShutdown();
    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == kExitOk);
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace
