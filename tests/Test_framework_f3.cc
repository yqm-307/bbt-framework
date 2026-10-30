// co-service-actor/v1 F3 切片验证：资源管理与关闭（F-06 三条）。
//
// 覆盖（全部真实断言、真实 HTTP loopback、无 mock 传输、无 sleep 凑时序）：
//  T1 request_shutdown_external_thread   外部控制线程 request_shutdown 生效：
//      _clean_rc0                        专用 std::thread 发起关闭 → 正常收束
//                                      → run 返回 kExitOk(0) → Closed。
//  T2 inflight_egress_during_closing     关闭期间在途 handler 仍可出站：handler
//                                      挂起 → request_shutdown → Closing →
//                                      放行 → handler 真实 this->call 经
//                                      loopback 到独立 echo peer → 回复送达
//                                      → 排空 → kExitOk。
//  T3 request_shutdown_before_run        早于 run 的关闭请求：run 完成启动后
//                                      立即进入关闭序列 → kExitOk。
//  T4 shutdown_incomplete_actor_late     预算耗尽 → ShutdownIncomplete（可观
//      _egress                           察未完成项、run 不返回、调度器与 I/O
//                                      驱动保留、Service/Actor/Runtime 强持有）
//                                      → 放行后 actor handler 在
//                                      ShutdownIncomplete 期间完成真实出站
//                                      → 迟到收尾 → kExitShutdownLate。
//  T5 late_callback_past_logical_end     诊断区分逻辑请求结束与物理清理：
//                                      handler 物理挂起超过 incoming 看门
//                                      （客户端见 OutcomeUnknown 即逻辑结束）
//                                      → 排空仍等待物理在途 → ShutdownIncomplete
//                                      → 迟到回调安全执行（资源仍存活）→
//                                      kExitShutdownLate。
//  T6 wait_handlers_budget_exhaustion   WaitHandlersDone 步预算耗尽路径
//                                      （INetworkHost 桩，无 socket）：第二次
//                                      等待获得无界期限、未完成项可观察、run
//                                      不返回、迟到收尾 → kExitShutdownLate。
//
// 进程内单例 Scheduler 约束：同一时刻至多一个 CoApp 在 run 内（F1-b1 已冻结
// 口径），故「在途 handler 出站」的对端不是第二个 CoApp，而是独立
// NetworkRuntime 上的裸 HTTP echo peer——出站能力本身是证据对象，对端实现
// 形态无关。测试客户端同样使用独立 runtime。
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
#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
// T7(b)/T7c 的 raw-wire 回复解析边界值类型（ParseRpcWireHttpResponse 只接
// 新栈 http::Response；发送仍走共享 HttpClient，仅做值转换）。
#include <bbt/infra/http/Client.hpp>
#endif

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>

// 机器面（注册表/生命周期/真实宿主/线桥/测试缝构造）显式走 internal 头。
#include <bbt/framework/internal/ActorRegistry.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/HostLifecycle.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>
#include <bbt/framework/internal/RpcHttpBridge.hpp>
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
// T7(b) 紧远端预算负例经 infra 公开 wire 面直发（见 SendRawWire）。
#include <bbt/infra/rpc/RpcWire.hpp>
#include <bbt/framework/internal/RpcWireBridge.hpp>
#endif

namespace fw  = bbt::framework;
namespace co  = bbt::coroutine;
namespace inf = bbt::infra;
using Clock = std::chrono::steady_clock;

namespace {

// ── 测试屏障原语（与 F1-b1/F1-b2 同形态：std CV，无 sleep 凑时序）──

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
    std::mutex              m_mtx;
    std::condition_variable m_cv;
    bool                    m_open = false;
};

constexpr std::chrono::milliseconds kWait{15000};

// ── 协程闩（CoWaiter 上的单次放行语义）──
// Open 前 Wait 真实挂起；Open 后（含 Open 早于登记）全部放行，不丢唤醒；
// 使用 CoWaiter 的单次等待语义，无 sleep。支持多等待者。
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
            std::lock_guard<std::mutex> lk(m_mtx);
            if (m_open) waiter->Notify();   // 早到：登记后自唤醒，不丢
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
// 手撸 header 映射（与 F1-b2 同一形态）。
namespace Wire = fw::http_bridge;

// ── 观测态 ──

struct Probe {
    explicit Probe(int arrive_n) : arrived(arrive_n) {}

    TestLatch arrived;

    // 业务 handler 挂起点：CoWaiter 闩，由首个挂起的 handler 惰性建立；
    // 测试经 CompleteGate 放行（非 sleep 时序）。
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
        if (g) g->Open();
    }

    mutable std::mutex      mtx;
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
};

std::shared_ptr<Probe> g_probe;

// ProtoWireV1 跨层预算探针（T7）：handler 内可见的绝对期限与执行计数。
std::atomic<std::int64_t> g_budget_deadline_ns{0};
std::atomic<int> g_budget_calls{0};

// T7 服务端同钟观测（真实 RpcServer handler 路径）：
//  - g_inbound_entries：被安装的 RpcHandler 真实进入次数——证明请求经
//    infra HandleRpcRequest/local-min 后抵达 framework 入站入口（loopback
//    上请求先发出再等待，客户端先超时也不会让请求不到服务端）。
//  - g_inbound_pre_deadline_ns：进入 InboundDispatcher **之前**的
//    IncomingCallContext.deadline（infra local-min 结果），与服务端 handler
//    内可见的 ctx.deadline（扣减后）同钟相减即 framework 预留的精确值。
//  - g_inbound_error_code：服务端 Dispatcher 返回的错误码（错误路径），
//    使「服务端拒绝」不依赖 client 侧错误语义（同域 TimedOut）。
std::atomic<int> g_inbound_entries{0};
std::atomic<std::int64_t> g_inbound_pre_deadline_ns{0};
std::atomic<int> g_inbound_error_code{0};
// T7c 补充观测：进入 handler 的真实时刻（同钟）——handler 可见期限相对它的余量
// 是「名义预算够不够」的实测证据，仅记录，不进入契约断言。
std::atomic<std::int64_t> g_inbound_entry_ns{0};
// T7c 受控耗尽：置位时入站 handler 在交付 framework **之前**等到接收端 local-min
// 期限确实过去——用受控调度构造「名义预算 > 预留 仍被拒」，不靠 wall-clock 容差。
std::atomic<bool> g_inbound_hold_until_deadline{false};

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

// "relay"：Concurrent。ping 即时回显；wait_dial 在 gate 上挂起（ctx 预算内），
// 放行后执行真实 this->call 到 echo peer；wait_dial_long 以无界期限挂起
// （物理在途可超过 incoming 看门），放行后仍尝试出站并记录结果码。
class RelaySvc final : public fw::CoService<RelaySvc> {
public:
    static constexpr std::string_view kServiceName = "relay";
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        return ReplyInt(IntArg(req));
    }
    fw::CoRpcResp WaitDial(fw::CoRpcReq req) {
        auto ctx = fw::CurrentRequestContext();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        auto p = g_probe;
        if (!p)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::InternalError, "no probe"));
        auto gate = p->Gate();
        p->arrived.CountDown();
        const auto st = gate->Wait(ctx.value()->deadline);
        if (st != co::WaitStatus::Completed)
            return fw::CoRpcResp::Error(WaitErr(st, "wait_dial gate"));
        auto response = this->call("echo", "ping", req);
        p->Event(std::string("outbound:") +
                 (response ? "ok" : "code" +
                  std::to_string(static_cast<int>(response.error().code))));
        if (!response) return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    fw::CoRpcResp WaitDialLong(fw::CoRpcReq req) {
        auto value = IntArg(req);
        if (!value) return fw::CoRpcResp::Error(value.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
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
        auto response = this->call("echo", "ping", req);
        p->Event(std::string("late_outbound:") +
                 (response ? "ok" : "code" +
                  std::to_string(static_cast<int>(response.error().code))));
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&RelaySvc::Ping>("ping"),
        fw::Method<&RelaySvc::WaitDial>("wait_dial"),
        fw::Method<&RelaySvc::WaitDialLong>("wait_dial_long"));
};

// "acct"：ActorSerial。park_dial 挂起后出站；第一个位置参数为 key。
class AcctRelaySvc final : public fw::CoService<AcctRelaySvc> {
public:
    static constexpr std::string_view kServiceName = "acct";
    fw::CoRpcResp ParkDial(fw::CoRpcReq req) {
        auto args = AcctArgs(req);
        if (!args) return fw::CoRpcResp::Error(args.error());
        auto ctx = fw::CurrentRequestContext();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        auto p = g_probe;
        if (!p)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::InternalError, "no probe"));
        auto gate = p->Gate();
        p->arrived.CountDown();
        const auto st = gate->Wait(ctx.value()->deadline);
        if (st != co::WaitStatus::Completed)
            return fw::CoRpcResp::Error(WaitErr(st, "park_dial gate"));
        auto response = this->call(
            "echo", "ping", RequestInt(std::get<1>(args.value())));
        p->Event(std::string("actor_outbound:") +
                 (response ? "ok" : "code" +
                  std::to_string(static_cast<int>(response.error().code))));
        if (!response) return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::ActorMethodAt<&AcctRelaySvc::ParkDial, std::string>(
            "park_dial"));
};

// "budget"：Concurrent。probe 记录 handler 内可见的绝对期限（ctx.deadline）
// 与执行计数——用于 T7 的 ProtoWireV1 跨层预算断言。
class BudgetSvc final : public fw::CoService<BudgetSvc> {
public:
    static constexpr std::string_view kServiceName = "budget";
    fw::CoRpcResp Probe(fw::CoRpcReq req)
    {
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
};

// ── 装配选项 ──

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

fw::ServiceOptions ActorOpts() {
    return fw::ServiceOptions{
        fw::ExecutionPolicy::ActorSerial,
        /*max_inflight*/ 64,
        /*mailbox_capacity*/ 16,
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

// ── 宿主夹具：真实 InfraHttpHost + CoApp（单 app，单例调度器约束）──

struct RunHandle {
    // 析构兜底：REQUIRE 早退时线程仍 joinable 会 terminate。
    ~RunHandle() { if (th.joinable()) th.detach(); }
    std::thread       th;
    std::atomic<bool> done{false};
    int               rc{-1};
};

// 路由 endpoint 逻辑标记 → 对端真实地址的解析槽：NetworkRuntime::Create
// 依赖已 Start 的调度器，echo peer 只能在 app 起来之后创建，其真实
// endpoint 无法写进 run 前固定的 static_routes——路由白名单仍由
// find_route 把关，发送闭包在发送时刻经此槽解析标记（同 F1-b2 的
// "self" 处理形态）。空标记值发送 → RuntimeUnavailable 如实返回。
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
    std::string        ep;
};

struct AppBox {
    std::shared_ptr<fw::InfraHttpHost>          host;
    std::shared_ptr<std::shared_ptr<inf::HttpClient>> egress_slot;
    std::shared_ptr<EndpointSlot>               peer_ep;
    std::unique_ptr<fw::CoApp>                  app;
    std::unique_ptr<RunHandle>                  run;
    std::string                                 endpoint;

    AppBox()                         = default;
    ~AppBox() { Stop(); }
    AppBox(AppBox&&)                 = default;
    AppBox& operator=(AppBox&&)      = default;
    AppBox(const AppBox&)            = delete;
    AppBox& operator=(const AppBox&) = delete;

    void Stop() {
        if (g_probe) g_probe->CompleteGate();
        if (app)     app->request_shutdown();
        if (run && run->th.joinable()) run->th.join();
    }
};

// 组装 app 并安装入站 handler，但不启动 run（供「早于 run 的
// request_shutdown」用例与正常启动路径共用）。routes 为完整静态路由
// （endpoint 即真实 host:port——F3 的出站对端是已就位的 echo peer）。
AppBox PrepareApp(std::chrono::milliseconds incoming_timeout,
                  std::chrono::milliseconds step_budget,
                  std::vector<fw::StaticRoute> routes,
                  const std::function<void(fw::CoApp&)>& register_fn) {
    AppBox box;
    const auto limits = Limits(incoming_timeout);
    box.host = std::make_shared<fw::InfraHttpHost>(
        limits, inf::ListenAddress{"127.0.0.1", 0});
    box.egress_slot =
        std::make_shared<std::shared_ptr<inf::HttpClient>>();
    box.peer_ep = std::make_shared<EndpointSlot>();

    fw::CoAppOptions opts;
    opts.network_limits       = limits;
    opts.listen               = inf::ListenAddress{"127.0.0.1", 0};
    opts.static_routes        = std::move(routes);
    opts.shutdown_step_budget = step_budget;
    auto slot = box.egress_slot;
    auto eps  = box.peer_ep;
    // 测试缝出站：endpoint 逻辑标记 "peer" 在发送时刻经解析槽换成
    // echo peer 的真实地址（同 F1-b2 的 "self" 处理形态）；路由门
    // （服务名白名单）仍完整生效。
    fw::CoAppSeam seam;
    seam.host     = box.host;
    seam.rpc_send =
        [slot, eps](const inf::RpcAddress& addr,
                    const inf::RpcEnvelope& env,
                    const inf::CallOptions& o)
            -> fw::result<inf::RpcEnvelope> {
        auto client = *slot;
        if (!client)
            return fw::result<inf::RpcEnvelope>::err(fw::MakeError(
                fw::ErrorCode::RuntimeUnavailable,
                "egress client not installed"));
        const std::string ep =
            addr.endpoint == "peer" ? eps->Get() : addr.endpoint;
        if (ep.empty())
            return fw::result<inf::RpcEnvelope>::err(fw::MakeError(
                fw::ErrorCode::RuntimeUnavailable,
                "egress peer endpoint not installed"));
        auto res = client->Request(
            Wire::ToHttpRequest(env, "http://" + ep + "/rpc"), o);
        if (!res)
            return fw::result<inf::RpcEnvelope>::err(
                std::move(res.error()));
        return Wire::EnvelopeFromResponse(res.value());
    };

    box.app = fw::MakeCoAppForTest(opts, std::move(seam));
    register_fn(*box.app);

    // 默认入站线桥（internal）：envelope↔HTTP 映射与业务路径同源。
    fw::InstallRpcHttpBridge(*box.host, *box.app);
    return box;
}

std::unique_ptr<RunHandle> RunApp(fw::CoApp& app) {
    // ProtoWireV1 宿主需要 2 个已登记 worker：server Engine 绑 1 main+1 sub
    // 驱动槽（上游 DriverBinder 槽不足时拒绝整个绑定）。出站不另占槽——
    // 正式 wire 由同一 NetworkRuntime::CreateHttpClient 通道承载（F2：
    // 纯入站 2 workers 保持既有可启动配置）。本设置必须在 Scheduler 首次
    // Start 前生效（runtime 进程寿命单例，后续调用只复用）。
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    // 受管 handler 的嵌套出站链（入站派发 → this->call → 出站适配 →
    // client->Request → 信号等待）在 12KB 默认栈触底（F1-b2 实测）；
    // 本文件同样含嵌套出站，沿用 64KB。
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

// 启动 run 并等到监听就绪（bound_endpoint 非空），再装出站 client。
void LaunchApp(AppBox& box) {
    box.run = RunApp(*box.app);
    const auto until = Clock::now() + kWait;
    while (box.endpoint.empty()) {
        BOOST_REQUIRE_MESSAGE(!box.run->done.load(),
            "app.run exited before listen, rc=" << box.run->rc);
        BOOST_REQUIRE(Clock::now() < until);
        box.endpoint = box.host->bound_endpoint();
        std::this_thread::yield();
    }
    BOOST_REQUIRE(box.app->shutdown_state() == fw::ShutdownState::Running);
    auto egress = box.host->network_runtime()->CreateHttpClient();
    if (!egress)
        BOOST_TEST_MESSAGE("CreateHttpClient failed code="
            << static_cast<int>(egress.error().code)
            << " msg=" << egress.error().message);
    BOOST_REQUIRE(egress);
    *box.egress_slot = egress.value();
}

AppBox StartApp(std::chrono::milliseconds incoming_timeout,
                std::chrono::milliseconds step_budget,
                std::vector<fw::StaticRoute> routes,
                const std::function<void(fw::CoApp&)>& register_fn) {
    auto box = PrepareApp(incoming_timeout, step_budget, std::move(routes),
                          register_fn);
    LaunchApp(box);
    return box;
}

// ── 测试客户端：独立 runtime + HttpClient（发往被测 app）──

struct TestClient {
    std::shared_ptr<inf::NetworkRuntime> rt;
    std::shared_ptr<inf::HttpClient>     client;

    void Stop() {
        // infra 关闭是同步契约：Close() 返回即物理释放，无需等待收束。
        if (client) client->Close();
        if (rt)     rt->Close();
    }
};

TestClient StartClient(inf::NetworkLimits limits) {
    TestClient c;
    auto rt = inf::NetworkRuntime::Create(limits);
    BOOST_REQUIRE(rt);
    c.rt = rt.value();
    BOOST_REQUIRE(c.rt->Start());
    auto cl = c.rt->CreateHttpClient();
    BOOST_REQUIRE(cl);
    c.client = cl.value();
    return c;
}

// ── 独立 echo peer：裸 infra runtime 上的 HTTP 端点，回位置参数 v+100
//    （+100 证明回复确由对端处理产生，非本地伪造）。──

struct EchoPeer {
    std::shared_ptr<inf::NetworkRuntime> rt;
    std::shared_ptr<inf::HttpServer>     server;
    std::string                          endpoint;
    std::atomic<int>                     hits{0};

    void Stop() {
        if (server) server->StopAccepting();
        if (rt)     rt->Close();   // 同步收口：返回即物理释放
    }
};

std::unique_ptr<EchoPeer> StartEchoPeer(inf::NetworkLimits limits) {
    auto p = std::make_unique<EchoPeer>();
    auto rt = inf::NetworkRuntime::Create(limits);
    BOOST_REQUIRE(rt);
    p->rt = rt.value();
    BOOST_REQUIRE(p->rt->Start());
    EchoPeer* pp = p.get();
    auto srv = p->rt->ListenHttp(
        inf::ListenAddress{"127.0.0.1", 0},
        [pp](inf::IncomingCallContext, inf::HttpRequest req)
            -> fw::result<inf::HttpResponse> {
            pp->hits.fetch_add(1, std::memory_order_acq_rel);
            auto env = Wire::EnvelopeFromRequest(req);
            if (!env)
                return fw::result<inf::HttpResponse>::ok(
                    Wire::ToHttpResponse(
                        fw::result<inf::RpcEnvelope>::err(
                            std::move(env.error()))));
            auto decoded =
                fw::CoRpcReq(env.value().payload).Parse<std::int32_t>();
            if (!decoded)
                return fw::result<inf::HttpResponse>::ok(
                    Wire::ToHttpResponse(
                        fw::result<inf::RpcEnvelope>::err(
                            std::move(decoded.error()))));
            auto encoded = fw::CoRpcResp::From(
                decoded.value() + 100);
            if (!encoded.ok())
                return fw::result<inf::HttpResponse>::ok(
                    Wire::ToHttpResponse(
                        fw::result<inf::RpcEnvelope>::err(encoded.error())));
            inf::RpcEnvelope reply;
            reply.service         = env.value().service;
            reply.method          = env.value().method;
            reply.request_id      = env.value().request_id;
            reply.response_schema = env.value().response_schema;
            reply.payload         = encoded.payload();
            return fw::result<inf::HttpResponse>::ok(
                Wire::ToHttpResponse(
                    fw::result<inf::RpcEnvelope>::ok(std::move(reply))));
        });
    BOOST_REQUIRE(srv);
    p->server = srv.value();
    const auto bound = p->server->LocalAddress();
    p->endpoint = bound.host + ":" + std::to_string(bound.port);
    return p;
}

// ── 请求构造/收发 ──

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

// 在协程内向被测 app 发一条 envelope（client->Request 是协程阻塞调用）。
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
// 与在途协程共同强持有。调用方超等待上限返回（或放弃自己的引用）后，迟到
// 任务仍写入同一块状态并通知同一个 latch——不存在指向调用者栈对象的引用；
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
// tc.client / endpoint / 请求 / 期限全部按值或共享捕获，结果写在共享堆状态
// 上：等待超限或注册失败都只返回错误，不会让在途任务引用调用者栈对象。
fw::result<inf::RpcEnvelope> CallRpc(const TestClient& tc, const std::string& endpoint, const inf::RpcEnvelope& env,
                                     std::chrono::milliseconds budget = kWait,
                                     std::shared_ptr<CallState> state = nullptr,
                                     std::optional<std::chrono::milliseconds> wait_budget = std::nullopt)
{
    if (!state)
        state = std::make_shared<CallState>();
    const auto wait = wait_budget.value_or(budget);
    auto client = tc.client;
    auto endpoint_v = endpoint;
    auto request = env;
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [state, client, endpoint_v, request, budget] {
            state->out.emplace(SendRpc(client, endpoint_v, request, budget));
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

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
// 经框架默认出站（ProtoWireV1 → HttpEgress::Send 共享 HttpClient + 公开
// RpcWire 编解码）真实 POST /rpc 到被测
// app 的 RpcServer；在协程内调用（Send 是协程阻塞调用）。状态经 shared_ptr
// 自持，超时不产生栈悬垂。
struct WireCall {
    std::shared_ptr<fw::InfraHttpHost> host; // 迟到返回前宿主不释放
    std::shared_ptr<fw::HttpEgress> egress;
    inf::RpcAddress addr;
    inf::RpcEnvelope env;
    inf::CallOptions opt;
    std::optional<fw::result<inf::RpcEnvelope>> out;
    TestLatch done{1};
};

fw::result<inf::RpcEnvelope> SendWire(const std::shared_ptr<fw::InfraHttpHost>& host, const std::string& endpoint,
                                      const inf::RpcEnvelope& env, std::chrono::milliseconds budget)
{
    auto st = std::make_shared<WireCall>();
    st->host = host;
    st->egress = std::make_shared<fw::HttpEgress>(host, fw::RpcEgressProfile::ProtoWireV1);
    st->addr = inf::RpcAddress{"http", endpoint};
    st->env = env;
    st->opt.deadline = Clock::now() + budget;
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [st] {
            st->out.emplace(st->egress->Send(st->addr, st->env, st->opt));
            st->done.CountDown();
        },
        succ);
    if (!succ || !st->done.WaitFor(kWait))
        return fw::result<inf::RpcEnvelope>::err(
            fw::MakeError(fw::ErrorCode::InternalError, "SendWire did not complete"));
    return std::move(*st->out);
}

// T7(b) 紧**远端**预算负例：把「客户端整段等待期限」与「wire
// remaining_budget_ms」解耦。经 infra 公开 wire 面（RpcWireEnvelope +
// EncodeRpcWireEnvelope）手工构造 body，用真实 HttpClient::Request 直发被测
// app 的 RpcServer——仍是真实 socket + 真实 RPC 入站（HandleRpcRequest
// 的接收端 local-min），但**明确不是框架出站**（框架出站正例见 T7(a) 的
// SendWire → HttpEgress::Send ProtoWireV1）。client 期限取长（等得起、
// 请求必达入口），wire 预算取短（服务端 local-min 取短，framework 入站
// 扣减后即过期）。上游新栈（bbt::infra::http）没有
// MakeRpcWireHttpRequest 工厂：手工 raw 发送只须编码 body + 显式 POST
// /rpc + 固定 Content-Type——与 wire profile 同一线路，不伪造第二条
// 通道；发送复用宿主的 NetworkRuntime::CreateHttpClient（共享限额 owner）。
struct RawWireCall {
    std::shared_ptr<inf::HttpClient> client;
    std::optional<fw::result<inf::HttpResponse>> out;
    TestLatch done{1};
};

// 旧栈 HttpResponse → 新栈 http::Response 的边界值转换（status→status_code；
// headers/body 同名透传）。wire 解析面只接新类型。
inf::http::Response ToWireResponse(const inf::HttpResponse& res) {
    inf::http::Response out;
    out.status_code = static_cast<int>(res.status);
    out.headers     = res.headers;
    out.body        = res.body;
    return out;
}

fw::result<inf::HttpResponse> SendRawWire(const std::shared_ptr<inf::HttpClient>& client,
                                        const std::string& endpoint,
                                        const inf::rpc::RpcWireEnvelope& wire,
                                        std::chrono::milliseconds client_budget)
{
    auto body = inf::rpc::EncodeRpcWireEnvelope(wire, /*is_response=*/false);
    if (!body)
        return fw::result<inf::HttpResponse>::err(body.error());
    inf::HttpRequest req;
    req.method  = "POST";
    req.url     = "http://" + endpoint + inf::rpc::kRpcWireHttpPath;
    req.headers = {{std::string(inf::rpc::kRpcWireContentTypeKey),
                    std::string(inf::rpc::kRpcWireContentType)}};
    req.body.assign(body.value().begin(), body.value().end());
    auto st = std::make_shared<RawWireCall>();
    st->client = client;
    inf::CallOptions opt;
    opt.deadline = Clock::now() + client_budget; // 客户端整段等待期限（长）
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [st, r = std::move(req), opt]() mutable {
            st->out.emplace(st->client->Request(std::move(r), opt));
            st->done.CountDown();
        },
        succ);
    if (!succ || !st->done.WaitFor(kWait))
        return fw::result<inf::HttpResponse>::err(
            fw::MakeError(fw::ErrorCode::InternalError, "SendRawWire did not complete"));
    return std::move(*st->out);
}

// 在宿主上安装观测型 RpcHandler（T7/T7c）：真实 infra RpcServer 入站路径
// （HandleRpcRequest 的接收端 local-min、信封编码与五字段关联照常生效）
// 进入本闭包时记录 IncomingCallContext.deadline（framework 扣减前、服务端
// 同钟口径）与入口计数，再经既有测试缝 DispatchInboundForTest 走真实
// InboundDispatcher/RequestScope 分发。只用于跨层预算观测，不改生产桥
// InstallRpcWireBridge 的装配；不新增业务可见面（DispatchInboundForTest
// 与 InstallRpcHandler 均为既有入口）。
// T7c 受控耗尽：g_inbound_hold_until_deadline 置位时先等到接收端 local-min
// 期限确实过去再交付 framework（睡过头只会更晚），用于在同一名义预算下确定地
// 构造「调度已耗尽 → 拒绝」。
void InstallBudgetProbeHandler(const std::shared_ptr<fw::InfraHttpHost>& host, fw::CoApp& app)
{
    fw::CoApp* app_ptr = &app;
    host->InstallRpcHandler(
        [app_ptr](inf::IncomingCallContext ctx, inf::RpcEnvelope env) -> fw::result<inf::RpcEnvelope> {
            g_inbound_entry_ns.store(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count(),
                std::memory_order_release);
            g_inbound_pre_deadline_ns.store(
                std::chrono::duration_cast<std::chrono::nanoseconds>(ctx.deadline.time_since_epoch()).count(),
                std::memory_order_release);
            g_inbound_entries.fetch_add(1, std::memory_order_acq_rel);
            if (g_inbound_hold_until_deadline.load(std::memory_order_acquire))
                std::this_thread::sleep_until(ctx.deadline + std::chrono::milliseconds{1});
            auto reply = fw::DispatchInboundForTest(*app_ptr, ctx, std::move(env));
            if (!reply) {
                g_inbound_error_code.store(static_cast<int>(reply.error().code), std::memory_order_release);
                return fw::result<inf::RpcEnvelope>::err(std::move(reply.error()));
            }
            return reply;
        });
}
#endif

// 有界状态轮询（显式状态判据，非 sleep）：直到谓词为真或预算耗尽。
template <class Pred> bool WaitUntil(Pred pred, std::chrono::milliseconds ms)
{
    const auto until = Clock::now() + ms;
    while (!pred()) {
        if (Clock::now() > until) return false;
        std::this_thread::yield();
    }
    return true;
}

bool WaitState(const fw::CoApp& app, fw::ShutdownState want, std::chrono::milliseconds ms)
{
    return WaitUntil([&app, want] { return app.shutdown_state() == want; }, ms);
}

// ── INetworkHost 桩：记录调用序列；T6 用于驱动 WaitHandlersDone 预算耗尽路径 ──

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
        if (on_wait_handlers) return on_wait_handlers(d);
        return fw::result<void>::ok();
    }
    void Close() noexcept override { _Record("net.close"); }

    std::vector<std::string> Names() const {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_calls;
    }
private:
    void _Record(const char* name) {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_calls.push_back(name);
    }
    mutable std::mutex       m_mtx;
    std::vector<std::string> m_calls;
};

BOOST_AUTO_TEST_SUITE(framework_f3)

// T1：外部控制线程 request_shutdown 生效；正常关闭完成 run 返回 0。
BOOST_AUTO_TEST_CASE(request_shutdown_external_thread_clean_rc0) {
    g_probe = std::make_shared<Probe>(0);
    auto box = StartApp(std::chrono::milliseconds{30000},
                        std::chrono::milliseconds{5000},
                        /*routes*/{},
                        [](fw::CoApp& app) {
        BOOST_REQUIRE(app.add_service<RelaySvc>(ConcurrentOpts()));
    });
    auto tc = StartClient(Limits(std::chrono::milliseconds{30000}));

    // 正路径对照：运行中真实请求可达。
    auto r = CallRpc(tc, box.endpoint, MakeEnv("relay", "ping", "t1-a", 7));
    BOOST_REQUIRE(r);
    BOOST_CHECK(DecodeReply(r).value() == 7);
    BOOST_CHECK(box.app->shutdown_state() == fw::ShutdownState::Running);

    // 关闭请求来自独立于 run 控制线程与测试主线程的外部线程。
    std::thread supervisor([&box] { box.app->request_shutdown(); });
    supervisor.join();

    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
    BOOST_CHECK(box.app->shutdown_state() == fw::ShutdownState::Closed);
    BOOST_CHECK(box.app->pending_cleanup().empty());
    BOOST_CHECK(box.app->lifecycle_failures().empty());
    tc.Stop();
}

// T2：关闭期间在途 handler 仍可完成真实出站（F-06①）。
BOOST_AUTO_TEST_CASE(inflight_egress_during_closing) {
    g_probe = std::make_shared<Probe>(1);
    auto box = StartApp(
        std::chrono::milliseconds{30000},
        std::chrono::milliseconds{5000},
        /*routes*/{fw::StaticRoute{"echo",
                                   inf::RpcAddress{"http", "peer"}}},
        [](fw::CoApp& app) {
            BOOST_REQUIRE(app.add_service<RelaySvc>(ConcurrentOpts()));
        });
    // echo peer 只能在调度器起来后创建；真实 endpoint 回填解析槽。
    auto peer = StartEchoPeer(Limits(std::chrono::milliseconds{30000}));
    box.peer_ep->Set(peer->endpoint);
    auto tc = StartClient(Limits(std::chrono::milliseconds{30000}));

    // 在途请求：handler 挂起在 gate 上（真 in-flight）。
    TestLatch inflight_done{1};
    std::optional<fw::result<inf::RpcEnvelope>> inflight_res;
    bbtco [&]() {
        inflight_res.emplace(
            SendRpc(tc.client, box.endpoint,
                    MakeEnv("relay", "wait_dial", "t2-h", 9), kWait));
        inflight_done.CountDown();
    };
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));

    // 外部线程请求关闭；等状态机确实进入 Closing（在途 handler 未排空，
    // 生命周期停在 WaitHandlersDone，不会滑到 Closed）。
    std::thread supervisor([&box] { box.app->request_shutdown(); });
    supervisor.join();
    BOOST_REQUIRE(WaitState(*box.app, fw::ShutdownState::Closing, kWait));
    BOOST_CHECK(!box.run->done.load());   // 控制线程仍在 run 内

    // 放行：handler 在关闭序列内发起真实出站——调度器未被强停、出站
    // client 所在 runtime 未 Close、服务实例仍存活。
    g_probe->CompleteGate();
    BOOST_REQUIRE(inflight_done.WaitFor(kWait));
    BOOST_REQUIRE(inflight_res.has_value());
    // F-06①：关闭期间在途 handler 被放行后仍完成真实出站（应答码非假装）——
    // 以 handler 侧 outbound:ok 事件与 peer 命中为证。infra 同步 Close 契约
    // （owner Close 丢弃未发送响应、不做 flush）下，回复不保证送达原客户端，
    // 故不以其为断言对象。
    BOOST_CHECK(g_probe->HasEvent("outbound:ok"));
    BOOST_CHECK(peer->hits.load() == 1);

    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
    BOOST_CHECK(box.app->shutdown_state() == fw::ShutdownState::Closed);
    tc.Stop();
    peer->Stop();
}

// T3：早于 run 的 request_shutdown → 完成启动后立即进入关闭序列。
BOOST_AUTO_TEST_CASE(request_shutdown_before_run) {
    g_probe = std::make_shared<Probe>(0);
    auto box = PrepareApp(std::chrono::milliseconds{30000},
                          std::chrono::milliseconds{5000},
                          /*routes*/{},
                          [](fw::CoApp& app) {
        BOOST_REQUIRE(app.add_service<RelaySvc>(ConcurrentOpts()));
    });
    box.app->request_shutdown();
    BOOST_CHECK(box.app->shutdown_state() == fw::ShutdownState::Closing);

    box.run = RunApp(*box.app);
    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
    BOOST_CHECK(box.app->shutdown_state() == fw::ShutdownState::Closed);
    BOOST_CHECK(box.app->pending_cleanup().empty());
}

// T4：预算耗尽 → ShutdownIncomplete → actor 在途 handler 迟到完成真实出站
//     → 资源强持有证据 → kExitShutdownLate（F-06②③ 主用例）。
BOOST_AUTO_TEST_CASE(shutdown_incomplete_actor_late_egress) {
    g_probe = std::make_shared<Probe>(1);
    auto box = StartApp(
        std::chrono::milliseconds{30000},
        std::chrono::milliseconds{300},    // 紧关闭预算：必然耗尽
        /*routes*/{fw::StaticRoute{"echo",
                                   inf::RpcAddress{"http", "peer"}}},
        [](fw::CoApp& app) {
            BOOST_REQUIRE(app.add_service<AcctRelaySvc>(ActorOpts()));
        });
    auto peer = StartEchoPeer(Limits(std::chrono::milliseconds{30000}));
    box.peer_ep->Set(peer->endpoint);
    auto tc = StartClient(Limits(std::chrono::milliseconds{30000}));

    // actor 在途请求挂起：mailbox drain 协程持有执行资格。
    TestLatch inflight_done{1};
    std::optional<fw::result<inf::RpcEnvelope>> inflight_res;
    bbtco [&]() {
        inflight_res.emplace(
            SendRpc(tc.client, box.endpoint,
                    MakeAcctEnv("park_dial", "t4-h", 3), kWait));
        inflight_done.CountDown();
    };
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));

    box.app->request_shutdown();
    // 预算耗尽 → ShutdownIncomplete：可观察状态 + 未完成项。
    BOOST_REQUIRE(
        WaitState(*box.app, fw::ShutdownState::ShutdownIncomplete, kWait));
    const auto pending = box.app->pending_cleanup();
    BOOST_REQUIRE(pending.size() == 1);
    BOOST_CHECK(pending[0] == "WaitHandlersDone");

    // 契约要求：控制线程仍在 run 内；App 持续强持有未收束
    // Service/Actor/NetworkRuntime；I/O 驱动与 Scheduler 保留（未被强停）。
    BOOST_CHECK(!box.run->done.load());
    BOOST_CHECK(g_scheduler->IsInitialized());
    auto rt = box.host->network_runtime();
    BOOST_REQUIRE(rt);
    BOOST_CHECK(!rt->IsClosed());                     // I/O 驱动仍在
    BOOST_REQUIRE(box.host->http_server());           // 网络对象未释放
    auto* reg = box.app->actor_registry();
    BOOST_REQUIRE(reg != nullptr);                    // 注册表仍强持有
    auto actor = reg->GetOrCreate("acct", "acct-3");
    BOOST_REQUIRE(actor);                             // actor 实例仍存活

    // 迟到收尾放行：actor handler 在 ShutdownIncomplete 期间完成真实
    // 出站——晚到操作访问的一切资源（协程、ctx、实例、egress、对端）
    // 必须仍有效。
    g_probe->CompleteGate();
    BOOST_REQUIRE(inflight_done.WaitFor(kWait));
    BOOST_REQUIRE(inflight_res.has_value());
    // F-06②③：actor 在途 handler 迟到完成真实出站，应答码来自 peer，不是
    // 假装——以 handler 侧 actor_outbound:ok 事件与 peer 命中为证。infra
    // 同步 Close 丢弃未发送响应，回复不保证送达原客户端。
    BOOST_CHECK(g_probe->HasEvent("actor_outbound:ok"));
    BOOST_CHECK(peer->hits.load() == 1);

    JoinRun(box.run);
    // 迟到操作全部结束后：完整收束，run 返回非零（记录曾超预算），不是
    // 「优雅关闭成功」。
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitShutdownLate);
    BOOST_CHECK(box.app->shutdown_state() == fw::ShutdownState::Closed);
    BOOST_CHECK(box.app->pending_cleanup().empty());
    BOOST_CHECK(box.app->lifecycle_failures().empty());
    tc.Stop();
    peer->Stop();
}

// T5：逻辑请求结束 ≠ 物理清理完成——handler 物理在途超过 incoming 看门，
//     客户端已见 OutcomeUnknown；排空仍等物理结束 → ShutdownIncomplete →
//     迟到回调资源安全 → kExitShutdownLate。
BOOST_AUTO_TEST_CASE(late_callback_past_logical_end) {
    g_probe = std::make_shared<Probe>(1);
    auto box = StartApp(
        std::chrono::milliseconds{400},    // 紧 incoming 看门
        std::chrono::milliseconds{300},    // 紧关闭预算
        /*routes*/{fw::StaticRoute{"echo",
                                   inf::RpcAddress{"http", "peer"}}},
        [](fw::CoApp& app) {
            BOOST_REQUIRE(app.add_service<RelaySvc>(ConcurrentOpts()));
        });
    auto peer = StartEchoPeer(Limits(std::chrono::milliseconds{30000}));
    box.peer_ep->Set(peer->endpoint);
    auto tc = StartClient(Limits(std::chrono::milliseconds{400}));

    // handler 以无界期限物理挂起：incoming 看门（400ms）先于 handler
    // 结束到期 → infra 切断回复路径；请求已完整写出但无可信回复，客户端
    // 见 OutcomeUnknown（逻辑结束），但物理 handler 协程仍在途。
    TestLatch inflight_done{1};
    std::optional<fw::result<inf::RpcEnvelope>> inflight_res;
    bbtco [&]() {
        inflight_res.emplace(
            SendRpc(tc.client, box.endpoint,
                    MakeEnv("relay", "wait_dial_long", "t5-h", 5),
                    std::chrono::milliseconds{10000}));
        inflight_done.CountDown();
    };
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));
    // 逻辑结束：回复路径在看门时刻被切断。
    BOOST_REQUIRE(inflight_done.WaitFor(kWait));
    BOOST_REQUIRE(inflight_res.has_value());
    BOOST_REQUIRE(!inflight_res.value());
    const auto& error = inflight_res.value().error();
    BOOST_CHECK_EQUAL(static_cast<int>(error.code),
                      static_cast<int>(fw::ErrorCode::OutcomeUnknown));
    BOOST_REQUIRE(error.request_phase.has_value());
    BOOST_CHECK_EQUAL(static_cast<int>(*error.request_phase),
                      static_cast<int>(inf::RequestPhase::RequestCommitted));
    BOOST_CHECK(!g_probe->HasEvent("late_handler_resumed"));  // 物理仍在途

    box.app->request_shutdown();
    // 物理在途未排空 → 预算耗尽 → ShutdownIncomplete。
    BOOST_REQUIRE(
        WaitState(*box.app, fw::ShutdownState::ShutdownIncomplete, kWait));
    BOOST_CHECK(!box.run->done.load());
    BOOST_CHECK(g_scheduler->IsInitialized());
    BOOST_CHECK(box.host->network_runtime() &&
                !box.host->network_runtime()->IsClosed());
    // 服务实例仍强持有（迟到回调稍后还要访问它）。
    auto svc = box.app->find_service("relay");
    BOOST_REQUIRE(svc);

    // 放行迟到 handler：协程恢复、上下文/实例/出站注入点必须仍有效
    // （实现若提前释放即 UAF/崩溃或 RuntimeUnavailable）。ctx 已过期
    // → 适配器在发起 I/O 前 TimedOut，peer 不收任何请求。
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

    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitShutdownLate);
    BOOST_CHECK(box.app->shutdown_state() == fw::ShutdownState::Closed);
    BOOST_CHECK(box.app->pending_cleanup().empty());
    tc.Stop();
    peer->Stop();
}

// T6：WaitHandlersDone 步预算耗尽（桩，无 socket）——第一次等待获有限步预算
//     超时、第二次获无界期限；未完成项可观察、run 不返回、迟到收尾 →
//     kExitShutdownLate。
BOOST_AUTO_TEST_CASE(wait_handlers_budget_exhaustion) {
    auto host = std::make_shared<StubNetHost>();
    TestLatch unbounded_wait{1};   // 第二次（无界）WaitHandlersDone 已进入
    TestGate  allow_drain;         // 迟到收尾放行
    std::atomic<int> wait_calls{0};
    std::atomic<bool> first_deadline_finite{false};
    std::atomic<bool> second_deadline_unbounded{false};

    host->on_wait_handlers =
        [&](co::Deadline d) -> fw::result<void> {
            const auto call = wait_calls.fetch_add(1);
            if (call == 0) {
                // 预算内等待：期限必须是有限步预算（约 step_budget 量级）。
                first_deadline_finite.store(
                    d < Clock::now() + std::chrono::hours{1},
                    std::memory_order_release);
                return fw::result<void>::err(fw::MakeError(
                    fw::ErrorCode::TimedOut, "handlers still running"));
            }
            // 预算耗尽后的续等：宿主以 Deadline::max() 等迟到收尾——
            // 期限约束的是优雅关闭是否成功，不是进程退出硬时限。
            second_deadline_unbounded.store(
                d > Clock::now() + std::chrono::hours{1},
                std::memory_order_release);
            unbounded_wait.CountDown();
            allow_drain.Wait();
            return fw::result<void>::ok();
        };

    fw::CoAppOptions opts;
    opts.network_limits       = Limits(std::chrono::milliseconds{30000});
    opts.listen               = inf::ListenAddress{"127.0.0.1", 0};
    opts.shutdown_step_budget = std::chrono::milliseconds{300};
    // 桩宿主无出站需求：测试缝只换 INetworkHost，rpc_send 留空
    // （受管出站如实 RuntimeUnavailable）。
    auto app = fw::MakeCoAppForTest(
        std::move(opts), fw::CoAppSeam{host, {}});
    BOOST_REQUIRE(app->add_service<RelaySvc>(ConcurrentOpts()));

    auto h = RunApp(*app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));
    app->request_shutdown();

    // WaitHandlersDone 超时 → ShutdownIncomplete；run 不返回、不提前关闭。
    BOOST_REQUIRE(unbounded_wait.WaitFor(kWait));
    BOOST_CHECK(app->shutdown_state() ==
                fw::ShutdownState::ShutdownIncomplete);
    const auto pending = app->pending_cleanup();
    BOOST_REQUIRE(pending.size() == 1);
    BOOST_CHECK(pending[0] == "WaitHandlersDone");
    BOOST_CHECK(!h->done.load());
    BOOST_CHECK(g_scheduler->IsInitialized());
    {
        const auto names = host->Names();
        BOOST_CHECK(std::find(names.begin(), names.end(),
                              "net.stop_accepting") != names.end());
        BOOST_CHECK(std::find(names.begin(), names.end(),
                              "net.close") == names.end());   // 未提前关闭
    }

    // 迟到收尾完成：走完关闭，run 返回非零。
    allow_drain.Open();
    JoinRun(h);
    BOOST_CHECK(h->rc == fw::HostLifecycle::kExitShutdownLate);
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Closed);
    BOOST_CHECK(app->pending_cleanup().empty());
    BOOST_CHECK(first_deadline_finite.load());
    BOOST_CHECK(second_deadline_unbounded.load());
    {
        const auto names = host->Names();
        auto pos = [&names](const char* n) {
            return std::find(names.begin(), names.end(), n) - names.begin();
        };
        BOOST_CHECK(pos("net.stop_accepting") < pos("net.wait_handlers"));
        // 两次 wait_handlers（预算内 + 无界续等）都在 close 之前。
        BOOST_CHECK(pos("net.wait_handlers") < pos("net.close"));
    }
}

// T8：CallRpc 超等待上限返回后任务迟到完成（无栈借用，f3 侧同根判别回归）。
//
// 构造（与 F1-b2 T11 同形）：请求的发送期限（5s）远长于调用者等待上限
// （150ms），relay/wait_dial handler 停在 probe 门上（ctx 预算 30s）。调用者
// 必然先超限返回——任务在门放行前不可能完成——随后放行 handler：迟到任务在
// CallRpc 已返回之后把结果写进共享堆状态并通知 latch（修复前该写入落在调用者
// 已析构的栈 optional/latch 上）。全部等待均为有界谓词等待（谓词满足即返回），
// 不引入固定 15s 等待，也不做无界 join。
// 本用例判别的是「结果与完成信号由共享堆状态承载」这一属性，不对原 102B
// double-free 作任何因果宣称。
BOOST_AUTO_TEST_CASE(callrpc_late_completion_after_wait_timeout)
{
    using std::chrono::milliseconds;
    g_probe = std::make_shared<Probe>(1);
    auto box = StartApp(milliseconds{30000}, milliseconds{2000},
                        /*routes*/ {fw::StaticRoute{"echo", inf::RpcAddress{"http", "peer"}}},
                        [](fw::CoApp& app) { BOOST_REQUIRE(app.add_service<RelaySvc>(ConcurrentOpts())); });
    auto peer = StartEchoPeer(Limits(milliseconds{30000}));
    box.peer_ep->Set(peer->endpoint);
    auto tc = StartClient(Limits(milliseconds{30000}));

    auto state = std::make_shared<CallState>();
    auto r = CallRpc(tc, box.endpoint, MakeEnv("relay", "wait_dial", "t8-late", 21),
                     /*budget*/ milliseconds{5000}, state,
                     /*wait_budget*/ milliseconds{150});
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == fw::ErrorCode::InternalError);
    // 请求已真实进 handler 并停在门上：任务确在途（不是尚未被调度）；此刻
    // 调用者栈帧早已退出。
    BOOST_REQUIRE(g_probe->arrived.WaitFor(kWait));
    BOOST_CHECK(!box.run->done.load());

    // 放行 handler：迟到任务完成真实出站（到 echo peer）并写入共享状态。
    g_probe->CompleteGate();
    BOOST_REQUIRE(state->done.WaitFor(kWait));
    BOOST_REQUIRE(state->out.has_value());
    BOOST_REQUIRE(state->out.value());
    BOOST_CHECK(state->out.value().value().request_id == "t8-late");
    // echo peer 对每个位置参数 +100：回复值 121 证明结果确由迟到任务收妥。
    auto rep = DecodeReply(state->out.value());
    BOOST_REQUIRE(rep);
    BOOST_CHECK(rep.value() == 121);
    BOOST_CHECK(peer->hits.load() == 1);

    // 迟到任务收束后应用仍正常服务（未因栈借用/悬垂写入受损）。
    auto again = CallRpc(tc, box.endpoint, MakeEnv("relay", "ping", "t8-after", 3));
    BOOST_REQUIRE(again);
    auto rep2 = DecodeReply(again);
    BOOST_REQUIRE(rep2);
    BOOST_CHECK(rep2.value() == 3);

    box.app->request_shutdown();
    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
    tc.Stop();
    peer->Stop();
}

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
// T7：ProtoWireV1 正式入站的跨层预算（3-4）——真实 infra RpcServer 入站 +
// 真实 infra RpcClient 出站，以**服务端同钟口径**钉死 framework 在 infra
// local-min 之后恰好扣一次 kReplyDeliveryReserve(20ms)。
//
// 判别性设计（不靠 wall-clock 容差）：
//  - 被安装的 RpcHandler 在进入 InboundDispatcher **之前**记录
//    IncomingCallContext.deadline（= infra HandleRpcRequest 的 local-min
//    结果，走真实 _InvokeRpc 门检查）；handler 内记录扣减后的 ctx.deadline。
//    两者同钟相减恰为 20ms——「不扣」得 0、「双扣」得 40，与 20 三值分离，
//    单值等值断言即可识别扣减次数（既有 f1b2 T8 是 dispatcher 直连的
//    局部证据，不能替代本跨层证据）。
//  - 入口计数 g_inbound_entries 证明请求真实抵达服务端 dispatcher 入口。
//  - 紧**远端**预算（wire remaining_budget_ms=10，client 期限长 5s）：以服务端
//    记录的 dispatcher 错误码（TimedOut）+ 入口计数 + 业务 handler 未执行
//    判定；两条预算解耦，在无故障前提下入口抵达不再依赖 loopback 传输快过
//    预算，从而相对旧 10ms 双预算版显著降低竞速窗口（条件性措辞：本设计不改
//    变「传输/调度仍可能抖动」这一事实，不作无条件无 flake 声明）。仍以服务端
//    错误码为准，不依赖 client 侧同域 TimedOut 区分。
BOOST_AUTO_TEST_CASE(wire_inbound_reply_reserve_exact)
{
    g_probe = std::make_shared<Probe>(0);
    g_budget_calls.store(0);
    g_budget_deadline_ns.store(0);
    g_inbound_entries.store(0);
    g_inbound_pre_deadline_ns.store(0);
    g_inbound_error_code.store(0);

    auto box =
        PrepareApp(std::chrono::milliseconds{30000}, std::chrono::milliseconds{5000},
                   /*routes*/ {}, [](fw::CoApp& app) { BOOST_REQUIRE(app.add_service<BudgetSvc>(ConcurrentOpts())); });
    // 入站走正式 ProtoWireV1：以观测型 RpcHandler 装机（真实 RpcServer
    // 入站路径 + 真实 DispatchInboundForTest→InboundDispatcher）。
    InstallBudgetProbeHandler(box.host, *box.app);
    LaunchApp(box);
    // 真实 infra RpcServer 路径已建立（对照 LegacyHeaders 的 http_server()）。
    BOOST_REQUIRE(box.host->rpc_server() != nullptr);
    BOOST_CHECK(box.host->bound_address().has_value());

    // (a) 充裕预算 B=3000ms：handler 执行；pre（local-min 后、扣减前）与
    //     post（扣减后）同钟相减恰为 20ms —— 恰一次、扣在 local-min 之后。
    const auto before = Clock::now();
    auto r_a =
        SendWire(box.host, box.endpoint, MakeEnv("budget", "probe", "t7-a", 11), std::chrono::milliseconds{3000});
    if (!r_a)
        BOOST_TEST_MESSAGE("t7-a error code=" << static_cast<int>(r_a.error().code) << " domain=" << r_a.error().domain
                                              << " msg=" << r_a.error().message);
    BOOST_REQUIRE(r_a);
    auto rep_a = DecodeReply(r_a);
    BOOST_REQUIRE(rep_a);
    BOOST_CHECK(rep_a.value() == 11);
    BOOST_CHECK(g_inbound_entries.load() == 1);
    BOOST_CHECK(g_budget_calls.load() == 1);

    const auto pre = std::chrono::steady_clock::time_point{std::chrono::nanoseconds{g_inbound_pre_deadline_ns.load()}};
    const auto post = std::chrono::steady_clock::time_point{std::chrono::nanoseconds{g_budget_deadline_ns.load()}};
    // 核心判别：同钟精确 20ms（0=不扣、40=双扣均被排除）。
    BOOST_CHECK(pre - post == std::chrono::milliseconds{20});
    // 服务端 local-min 采用客户端预算（≈B），非 B+reserve——证明预留施加在
    // local-min **之后**而非之前；±500ms 仅吸收 loopback/调度抖动。
    const auto delta = pre - before;
    BOOST_CHECK(delta >= std::chrono::milliseconds{3000 - 500});
    BOOST_CHECK(delta <= std::chrono::milliseconds{3000 + 500});

    // (b) 紧**远端**预算：客户端期限长（5s）→ 在无故障前提下请求应达服务端
    //     dispatcher 入口（与旧 10ms 双预算版相比不再依赖 loopback 传输快过
    //     预算）；wire remaining_budget_ms=10
    //     → 服务端 local-min 取 10ms，framework 入站扣减 20ms 后过期 →
    //     服务端 TimedOut、业务 handler 不执行。经 infra 公开 wire 面手工
    //     构造 + 真实 HttpClient::Request 直发：真实 socket + 真实 RpcServer
    //     入站，但明确不是框架出站（框架出站正例见 (a) 的 SendWire）。
    auto wire_b = inf::rpc::ToWireEnvelope(MakeEnv("budget", "probe", "t7-b", 12), /*remaining_budget_ms*/ 10);
    BOOST_REQUIRE(wire_b);
    auto r_b = SendRawWire(*box.egress_slot, box.endpoint, wire_b.value(), std::chrono::milliseconds{5000});
    // 客户端合同：错误经 wire envelope 承载（HTTP 200 + success=false +
    // error=TimedOut），不是传输/HTTP 层错误——infra 公开 wire 响应契约。
    if (!r_b)
        BOOST_TEST_MESSAGE("t7-b transport error code=" << static_cast<int>(r_b.error().code)
                                                        << " msg=" << r_b.error().message);
    BOOST_REQUIRE(r_b);
    BOOST_CHECK(r_b.value().status == 200);
    auto reply_b = inf::rpc::ParseRpcWireHttpResponse(ToWireResponse(r_b.value()));
    BOOST_REQUIRE(reply_b);
    BOOST_CHECK(!reply_b.value().success);
    BOOST_CHECK(static_cast<int>(reply_b.value().error.code) == static_cast<int>(fw::ErrorCode::TimedOut));
    // 服务端断言：在「回复已抵达」这一先决条件下，入口已进入、分发已完成
    // ——由此免去旧 10ms 双预算版对 WaitUntil 轮询的依赖（条件性措辞：断言
    // 随该先决条件成立，不作无条件无 flake 声明）。
    BOOST_CHECK(g_inbound_entries.load() == 2);                                            // 请求已达服务端入口
    BOOST_CHECK(g_inbound_error_code.load() == static_cast<int>(fw::ErrorCode::TimedOut)); // 服务端拒绝码
    BOOST_CHECK(g_budget_calls.load() == 1);                                               // 业务 handler 未执行

    box.app->request_shutdown();
    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
}

// T7b：真实 InstallRpcWireBridge 成功回复五字段对齐（I2 回源头修复后，
// InboundDispatcher 自身回填 request_schema，入站桥不再需要回显补丁）。
// 若源头未回填，infra HandleRpcRequest 会对回复返回 ProtocolError
// 「rpc handler changed association fields」，本用例失败。
BOOST_AUTO_TEST_CASE(wire_inbound_bridge_success_five_fields)
{
    g_probe = std::make_shared<Probe>(0);
    g_budget_calls.store(0);
    g_budget_deadline_ns.store(0);

    auto box =
        PrepareApp(std::chrono::milliseconds{30000}, std::chrono::milliseconds{5000},
                   /*routes*/ {}, [](fw::CoApp& app) { BOOST_REQUIRE(app.add_service<BudgetSvc>(ConcurrentOpts())); });
    fw::InstallRpcWireBridge(*box.host, *box.app);
    LaunchApp(box);
    BOOST_REQUIRE(box.host->rpc_server() != nullptr);

    auto r = SendWire(box.host, box.endpoint, MakeEnv("budget", "probe", "t7b-a", 21), std::chrono::milliseconds{3000});
    if (!r)
        BOOST_TEST_MESSAGE("t7b error code=" << static_cast<int>(r.error().code) << " domain=" << r.error().domain
                                             << " msg=" << r.error().message);
    BOOST_REQUIRE(r);
    auto rep = DecodeReply(r);
    BOOST_REQUIRE(rep);
    BOOST_CHECK(rep.value() == 21);
    BOOST_CHECK(g_budget_calls.load() == 1);

    box.app->request_shutdown();
    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
}

// ── 多 hop 预算（R4/S4）：同一进程同一 CoApp 的两个方法经 loopback socket 相扣 ──
//
// 口径（如实声明）：不是跨机器、不是跨进程、不涉及认证/授权。进程内单例
// Scheduler 只允许一个 CoApp 处于 run 内（CoApp.cc 的活跃宿主登记），故「两跳」
// 由同一 CoApp 的两个方法相扣，而不是三个 CoApp。两跳都真实经 infra
// RpcServer/HandleRpcRequest（接收端 local-min）+ 真实 framework
// InboundDispatcher（每跳恰扣一次 kReplyDeliveryReserve）；hop1 的出站用生产
// HttpEgress(ProtoWireV1) → 共享 HttpClient + 公开 RpcWire 出站，只把逻辑 endpoint "self" 在
// 发送时刻解析到本进程自身的监听地址（端口 0 只能 run 后得知；路由白名单仍由
// find_route 按 service 名把关，解析只替换地址文本）。

// 两跳链观测态：各跳串行完成，全部读写走锁；记录按角色（方法名）取用。
struct HopProbe {
    struct Ingress {
        std::string service;
        std::string method;
        std::int64_t pre_ns{0}; // IncomingCallContext.deadline（infra local-min 结果）
        std::int64_t at_ns{0};  // 记录时刻（同钟）
    };

    static std::int64_t Ns(Clock::time_point t)
    {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch()).count();
    }
    void Reset()
    {
        std::lock_guard<std::mutex> lk(mtx);
        ingress.clear();
        handler_deadline_ns.clear();
        adapted_ns = 0;
        widen_requested_ns = 0;
        egress_start_ns = 0;
        egress_ok = false;
        handler_calls = 0;
    }
    void RecordIngress(std::string service, std::string method, co::Deadline pre, Clock::time_point at)
    {
        std::lock_guard<std::mutex> lk(mtx);
        ingress.push_back(Ingress{std::move(service), std::move(method), Ns(pre), Ns(at)});
    }
    Ingress At(const std::string& method) const
    {
        std::lock_guard<std::mutex> lk(mtx);
        for (const auto& i : ingress)
            if (i.method == method)
                return i;
        return Ingress{};
    }
    void SetHandlerDeadline(const std::string& role, co::Deadline d)
    {
        std::lock_guard<std::mutex> lk(mtx);
        handler_deadline_ns[role] = Ns(d);
        handler_calls += 1;
    }
    std::int64_t HandlerDeadline(const std::string& role) const
    {
        std::lock_guard<std::mutex> lk(mtx);
        const auto it = handler_deadline_ns.find(role);
        return it != handler_deadline_ns.end() ? it->second : 0;
    }
    int HandlerCalls() const
    {
        std::lock_guard<std::mutex> lk(mtx);
        return handler_calls;
    }
    void SetAdapted(co::Deadline d)
    {
        std::lock_guard<std::mutex> lk(mtx);
        adapted_ns = Ns(d);
    }
    std::int64_t Adapted() const
    {
        std::lock_guard<std::mutex> lk(mtx);
        return adapted_ns;
    }
    void SetWidenRequested(co::Deadline d)
    {
        std::lock_guard<std::mutex> lk(mtx);
        widen_requested_ns = Ns(d);
    }
    std::int64_t WidenRequested() const
    {
        std::lock_guard<std::mutex> lk(mtx);
        return widen_requested_ns;
    }
    void MarkEgressStart(Clock::time_point t)
    {
        std::lock_guard<std::mutex> lk(mtx);
        egress_start_ns = Ns(t);
    }
    std::int64_t EgressStart() const
    {
        std::lock_guard<std::mutex> lk(mtx);
        return egress_start_ns;
    }
    void MarkEgressEnd(bool ok)
    {
        std::lock_guard<std::mutex> lk(mtx);
        egress_ok = ok;
    }
    bool EgressOk() const
    {
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

std::int64_t MsNs(std::chrono::milliseconds d)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(d).count();
}

// 两跳服务：leaf 为叶子（记录 handler 内可见期限后回值）；forward 记录父 ctx
// 期限后做一次真实受管出站到 leaf。forward_widen 变体显式给出远大于父预算的
// 子期限——适配器必须取 min，不得放大父预算（「去掉 min」变异的判别点）。
class HopSvc final : public fw::CoService<HopSvc> {
public:
    static constexpr std::string_view kServiceName = "budget_hop";

    fw::CoRpcResp Leaf(fw::CoRpcReq req)
    {
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

    fw::CoRpcResp Forward(fw::CoRpcReq req)
    {
        return _Forward(req, false);
    }
    fw::CoRpcResp ForwardWiden(fw::CoRpcReq req)
    {
        return _Forward(req, true);
    }

    static constexpr auto kRpcMethods =
        fw::RpcMethods(fw::Method<&HopSvc::Leaf>("leaf"), fw::Method<&HopSvc::Forward>("forward"),
                       fw::Method<&HopSvc::ForwardWiden>("forward_widen"));

private:
    fw::CoRpcResp _Forward(fw::CoRpcReq req, bool widen)
    {
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
        auto reply = this->call(kServiceName, "leaf", req, {}, opt);
        if (g_hop)
            g_hop->MarkEgressEnd(static_cast<bool>(reply));
        if (!reply)
            return fw::CoRpcResp::Error(reply.error());
        return reply.value();
    }
};

// 两跳观测入站：进入 framework 入站入口之前记录 IncomingCallContext.deadline 与
// 该时刻（同钟），再走真实 DispatchInboundForTest→InboundDispatcher 分发。
void InstallHopProbeHandler(const std::shared_ptr<fw::InfraHttpHost>& host, fw::CoApp& app)
{
    fw::CoApp* app_ptr = &app;
    host->InstallRpcHandler(
        [app_ptr](inf::IncomingCallContext ctx, inf::RpcEnvelope env) -> fw::result<inf::RpcEnvelope> {
            if (g_hop)
                g_hop->RecordIngress(env.service, env.method, ctx.deadline, Clock::now());
            return fw::DispatchInboundForTest(*app_ptr, ctx, std::move(env));
        });
}

// 两跳夹具：与 PrepareApp 同形（复用 AppBox/RunApp/Limits/EndpointSlot），差异
// 只有「出站实现」与「endpoint 解析」两点，见上方口径声明。
AppBox PrepareHopApp(std::chrono::milliseconds incoming_timeout, std::chrono::milliseconds step_budget,
                     std::vector<fw::StaticRoute> routes, const std::function<void(fw::CoApp&)>& register_fn)
{
    AppBox box;
    const auto limits = Limits(incoming_timeout);
    box.host = std::make_shared<fw::InfraHttpHost>(limits, inf::ListenAddress{"127.0.0.1", 0});
    box.egress_slot = std::make_shared<std::shared_ptr<inf::HttpClient>>();
    box.peer_ep = std::make_shared<EndpointSlot>(); // 本夹具承载「自身 endpoint」

    fw::CoAppOptions opts;
    opts.network_limits = limits;
    opts.listen = inf::ListenAddress{"127.0.0.1", 0};
    opts.static_routes = std::move(routes);
    opts.shutdown_step_budget = step_budget;

    auto host = box.host;
    auto eps = box.peer_ep;
    auto egress = std::make_shared<fw::HttpEgress>(host, fw::RpcEgressProfile::ProtoWireV1);
    fw::CoAppSeam seam;
    seam.host = host;
    seam.rpc_send = [egress, eps](const inf::RpcAddress& addr, const inf::RpcEnvelope& env,
                                  const inf::CallOptions& o) -> fw::result<inf::RpcEnvelope> {
        // 生产 AdaptCallOptions 之后、真正发起 I/O 之前收到的出站期限。
        if (g_hop)
            g_hop->SetAdapted(o.deadline);
        const std::string ep = addr.endpoint == "self" ? eps->Get() : addr.endpoint;
        if (ep.empty())
            return fw::result<inf::RpcEnvelope>::err(
                fw::MakeError(fw::ErrorCode::RuntimeUnavailable, "hop egress self endpoint not installed"));
        return egress->Send(inf::RpcAddress{addr.transport, ep}, env, o);
    };

    box.app = fw::MakeCoAppForTest(opts, std::move(seam));
    register_fn(*box.app);
    InstallHopProbeHandler(box.host, *box.app);
    return box;
}

// T9：真实两跳的预算不放大（R4/S4「跨 hop」）——A（测试侧真实框架出站）→
// B（本进程 CoApp hop1 入站）→ B 的出站 → B（同一 CoApp hop2 入站）。
// 断言全部落在同一 steady_clock 的绝对期限上：
//  - 每跳 local-min 之后 framework 恰扣一次 20ms（同钟精确等值）；
//  - 生产 AdaptCallOptions 后的出站期限 == 父 ctx 期限（无显式期限时继承，
//    不延长、不放大）；
//  - 到达 hop2 的预算相对父 ctx 期限只多出**实测**单程时延——出站放大（例如
//    改用新预算、或去掉 min 让显式子期限生效）会远超该实测上界；
//  - 逐跳只缩：d_hop2 < d_hop1；d_hop1 - d_hop2 的窗口是 20ms ± 实测单程，
//    上界再含 wire 整数毫秒的 <1ms 向下取整余量（0/40ms 仍被排除）。
// widen 变体显式把子期限设到 30s（远大于父预算 ~3s）：适配器取 min，仍不放大。
BOOST_AUTO_TEST_CASE(wire_multihop_budget_never_amplified)
{
    using std::chrono::milliseconds;
    g_hop = std::make_shared<HopProbe>();
    auto box =
        PrepareHopApp(milliseconds{30000}, milliseconds{5000},
                      /*routes*/ {fw::StaticRoute{std::string(HopSvc::kServiceName), inf::RpcAddress{"http", "self"}}},
                      [](fw::CoApp& app) { BOOST_REQUIRE(app.add_service<HopSvc>(ConcurrentOpts())); });
    LaunchApp(box);
    BOOST_REQUIRE(box.host->rpc_server() != nullptr); // 正式 ProtoWireV1 入站
    BOOST_REQUIRE(!box.endpoint.empty());
    box.peer_ep->Set(box.endpoint); // "self" → 本进程自身监听地址

    // (a) 无显式子期限：子预算继承父预算。
    auto r_a = SendWire(box.host, box.endpoint, MakeEnv(std::string(HopSvc::kServiceName), "forward", "mh-a", 11),
                        milliseconds{3000});
    if (!r_a)
        BOOST_TEST_MESSAGE("mh-a error code=" << static_cast<int>(r_a.error().code) << " msg=" << r_a.error().message);
    BOOST_REQUIRE(r_a);
    auto rep_a = DecodeReply(r_a);
    BOOST_REQUIRE(rep_a);
    BOOST_CHECK(rep_a.value() == 11);

    const auto ing_f = g_hop->At("forward");
    const auto ing_l = g_hop->At("leaf");
    const auto d_f = g_hop->HandlerDeadline("forward");
    const auto d_l = g_hop->HandlerDeadline("leaf");
    // 第 1 跳：真实 infra local-min → framework 恰扣一次 20ms。
    BOOST_CHECK_NE(d_f, 0);
    BOOST_CHECK(ing_f.pre_ns - d_f == MsNs(milliseconds{20}));
    // 生产 AdaptCallOptions 后的出站期限 == 父 ctx 期限（继承，不延长）。
    BOOST_CHECK(g_hop->Adapted() == d_f);
    BOOST_CHECK(g_hop->HandlerCalls() == 2);
    BOOST_CHECK(g_hop->EgressOk());
    // 第 2 跳：同一 20ms 精确等值。
    BOOST_CHECK_NE(d_l, 0);
    BOOST_CHECK(ing_l.pre_ns - d_l == MsNs(milliseconds{20}));
    // 实测单程（hop1 发起出站 → hop2 抵达入站包装）：本用例唯一的时延量。
    const auto hop_latency = ing_l.at_ns - g_hop->EgressStart();
    BOOST_CHECK_MESSAGE(hop_latency > 0, "hop latency not observed");
    BOOST_CHECK_MESSAGE(hop_latency < MsNs(milliseconds{15}),
                        "hop latency too large for discriminating power: " << hop_latency << "ns");
    // 到达第 2 跳的预算相对父 ctx 期限只多出实测单程 → 出站未放大预算。
    BOOST_TEST_MESSAGE("mh-a hop_latency=" << hop_latency << "ns arrival_excess_over_parent=" << (ing_l.pre_ns - d_f)
                                           << "ns drop_d1_minus_d2=" << (d_f - d_l) << "ns");
    BOOST_CHECK(ing_l.pre_ns - d_f <= hop_latency);
    // wire 向下取整为整数毫秒：量化余量 <1ms 可使扣减量大于 20ms。
    // 逐跳只缩；窗口包含实测单程及该量化项，0/40ms 仍在窗口外。
    const auto wire_quantization = MsNs(milliseconds{1});
    BOOST_CHECK(d_l < d_f);
    BOOST_CHECK(d_f - d_l >= MsNs(milliseconds{20}) - hop_latency);
    BOOST_CHECK(d_f - d_l <= MsNs(milliseconds{20}) + hop_latency + wire_quantization);

    // (b) 显式子期限 = now + 30s（远大于父预算 ~3s）：父预算仍不可放大。
    g_hop->Reset();
    auto r_b = SendWire(box.host, box.endpoint, MakeEnv(std::string(HopSvc::kServiceName), "forward_widen", "mh-b", 12),
                        milliseconds{3000});
    if (!r_b)
        BOOST_TEST_MESSAGE("mh-b error code=" << static_cast<int>(r_b.error().code) << " msg=" << r_b.error().message);
    BOOST_REQUIRE(r_b);
    auto rep_b = DecodeReply(r_b);
    BOOST_REQUIRE(rep_b);
    BOOST_CHECK(rep_b.value() == 12);

    const auto ing_fb = g_hop->At("forward_widen");
    const auto ing_lb = g_hop->At("leaf");
    const auto d_fb = g_hop->HandlerDeadline("forward_widen");
    const auto d_lb = g_hop->HandlerDeadline("leaf");
    // 显式请求确实远大于父预算（否则本变体不具判别力）。
    BOOST_CHECK(g_hop->WidenRequested() - d_fb > MsNs(milliseconds{5000}));
    // 适配器取 min：出站期限仍是父预算，不是显式 30s。
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

    box.app->request_shutdown();
    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
}

// T7c：20ms 预留的**真实链**边界（正式 ProtoWireV1 入站 + 真实 RpcServer）。
// 契约（infra HandleRpcRequest + framework InboundDispatcher）：
//   pre = min(服务端本地期限, now_s + wire.remaining_budget_ms)；
//   ctx.deadline = pre - kReplyDeliveryReserve(20ms)；ctx.deadline <= now() → 拒绝。
// 故 wire 预算恰 ==20ms 时 ctx.deadline == now_s，一律在 handler 之前拒绝（「==20ms
// 应执行」的推断不成立）；名义 >20ms 也不充分——(b)/(c) 用同一个 45ms 预算、只改
// 受控调度，证明判定用的是检查时刻的剩余时间。
BOOST_AUTO_TEST_CASE(wire_inbound_reply_reserve_boundary_20ms)
{
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
                   /*routes*/ {}, [](fw::CoApp& app) { BOOST_REQUIRE(app.add_service<BudgetSvc>(ConcurrentOpts())); });
    InstallBudgetProbeHandler(box.host, *box.app);
    LaunchApp(box);
    BOOST_REQUIRE(box.host->rpc_server() != nullptr);

    // (a) wire 预算恰 ==20ms（客户端整段等待取 5s，确保请求必达服务端入口）：
    //     接收端 local-min 取 20ms → 扣预留后期限即请求到达时刻 → 拒绝。
    const auto before_a = Clock::now();
    auto wire_a = inf::rpc::ToWireEnvelope(MakeEnv("budget", "probe", "t7c-eq", 11), 20);
    BOOST_REQUIRE(wire_a);
    auto r_a = SendRawWire(*box.egress_slot, box.endpoint, wire_a.value(), milliseconds{5000});
    if (!r_a)
        BOOST_TEST_MESSAGE("t7c-eq transport error code=" << static_cast<int>(r_a.error().code));
    BOOST_REQUIRE(r_a);
    BOOST_CHECK(r_a.value().status == 200);
    auto reply_a = inf::rpc::ParseRpcWireHttpResponse(ToWireResponse(r_a.value()));
    BOOST_REQUIRE(reply_a);
    BOOST_CHECK(!reply_a.value().success);
    BOOST_CHECK(static_cast<int>(reply_a.value().error.code) == static_cast<int>(fw::ErrorCode::TimedOut));
    BOOST_CHECK(g_inbound_entries.load() == 1); // 已抵真实入站入口（拒绝在预留门）
    BOOST_CHECK(g_inbound_error_code.load() == static_cast<int>(fw::ErrorCode::TimedOut));
    BOOST_CHECK(g_budget_calls.load() == 0); // 业务 handler 未执行
    // 接收端 local-min 确实采用 wire 预算（20ms 量级），不是服务端 30s 本地期限。
    const auto pre_a = std::chrono::nanoseconds{g_inbound_pre_deadline_ns.load()};
    const auto pre_a_after = pre_a - std::chrono::duration_cast<std::chrono::nanoseconds>(before_a.time_since_epoch());
    BOOST_CHECK(pre_a_after > milliseconds{0});
    BOOST_CHECK(pre_a_after < milliseconds{500});

    // (b) wire 预算 45ms（= 20ms 预留 + 25ms），无受控耗尽：取期限与检查点之间只剩
    //     入站 prologue（纯 CPU）→ handler 执行，且恰扣一次 20ms。
    auto wire_b = inf::rpc::ToWireEnvelope(MakeEnv("budget", "probe", "t7c-above-ok", 12), 45);
    BOOST_REQUIRE(wire_b);
    auto r_b = SendRawWire(*box.egress_slot, box.endpoint, wire_b.value(), milliseconds{5000});
    BOOST_REQUIRE(r_b);
    BOOST_CHECK(r_b.value().status == 200);
    auto reply_b = inf::rpc::ParseRpcWireHttpResponse(ToWireResponse(r_b.value()));
    BOOST_REQUIRE(reply_b);
    BOOST_CHECK(reply_b.value().success);
    auto rep_b = fw::CoRpcReq(reply_b.value().payload).Parse<std::int32_t>();
    BOOST_REQUIRE(rep_b);
    BOOST_CHECK(rep_b.value() == 12);
    BOOST_CHECK(g_inbound_entries.load() == 2);
    BOOST_CHECK(g_budget_calls.load() == 1);
    const auto pre_b = std::chrono::nanoseconds{g_inbound_pre_deadline_ns.load()};
    const auto post_b = std::chrono::nanoseconds{g_budget_deadline_ns.load()};
    BOOST_CHECK(pre_b - post_b == milliseconds{20});
    BOOST_TEST_MESSAGE("t7c-above-ok handler headroom="
                       << (post_b - std::chrono::nanoseconds{g_inbound_entry_ns.load()}).count() << "ns");

    // (c) 同一 45ms 预算 + 受控耗尽（入站 handler 等到接收端 local-min 期限确实过去
    //     再交付 framework）：同一名义预算下结果相反——判定用的是剩余时间。
    g_inbound_hold_until_deadline.store(true);
    auto wire_c = inf::rpc::ToWireEnvelope(MakeEnv("budget", "probe", "t7c-above-rejected", 13), 45);
    BOOST_REQUIRE(wire_c);
    auto r_c = SendRawWire(*box.egress_slot, box.endpoint, wire_c.value(), milliseconds{5000});
    g_inbound_hold_until_deadline.store(false);
    BOOST_REQUIRE(r_c);
    BOOST_CHECK(r_c.value().status == 200);
    auto reply_c = inf::rpc::ParseRpcWireHttpResponse(ToWireResponse(r_c.value()));
    BOOST_REQUIRE(reply_c);
    BOOST_CHECK(!reply_c.value().success);
    BOOST_CHECK(static_cast<int>(reply_c.value().error.code) == static_cast<int>(fw::ErrorCode::TimedOut));
    BOOST_CHECK(g_inbound_entries.load() == 3);
    BOOST_CHECK(g_budget_calls.load() == 1); // 仍未执行

    box.app->request_shutdown();
    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
}

// ── 本轮 review 修复的定向回归（F1/F2/F3/F4 + WaitStopped 结果处理）──

// T10（F1+F2）：合法的较小 header 限额 + 仅 2 workers（无出站 Engine 槽）的
// 纯入站 ProtoWireV1 宿主必须 Create/Start 成功并保持既有配置可启动。
// F1 回归：上游要求 max_target_bytes <= limits.max_header_bytes，本用例的
// 4096 在修复前直接报 "invalid HTTP Server options"。F2 回归：修复前 Start
// 无条件补建 client-only Engine（再占 1 驱动槽），2 workers 必败。
BOOST_AUTO_TEST_CASE(wire_host_small_header_two_workers_start)
{
    // 2 workers 前提必须显式成立（单测单独运行时同样成立）——不通过提高
    // 线程数隐藏回退；进程内单例已在场时该配置只复用。
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    if (!g_scheduler->IsInitialized())
        g_scheduler->Start();
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    inf::NetworkLimits limits{
        /*max_connections*/ 16,
        /*max_inflight*/ 16,
        /*max_header_bytes*/ 4096,   // 修复前 Create 即失败的合法小限额
        /*max_body_bytes*/ 8192,
        std::chrono::milliseconds{30000}};
    auto host = std::make_shared<fw::InfraHttpHost>(
        limits, inf::ListenAddress{"127.0.0.1", 0});
    host->InstallRpcHandler(
        [](inf::IncomingCallContext, inf::RpcEnvelope env)
            -> fw::result<inf::RpcEnvelope> {
            return fw::result<inf::RpcEnvelope>::ok(std::move(env));
        });
    auto created = host->Create();
    if (!created)
        BOOST_TEST_MESSAGE("Create failed: " << created.error().message);
    BOOST_REQUIRE(created);
    auto started = host->Start();
    if (!started)
        BOOST_TEST_MESSAGE("Start failed: " << started.error().message);
    BOOST_REQUIRE(started);
    // 真实监听就绪（内核分配端口）。
    BOOST_REQUIRE(host->bound_address().has_value());
    BOOST_CHECK(host->bound_address().value().port != 0);
    BOOST_CHECK(host->rpc_server() != nullptr);

    // 完整关闭序列；正常路径 WaitStopped 无失败事实。
    host->StopAccepting();
    auto drained = host->WaitHandlersDone(
        Clock::now() + std::chrono::milliseconds{5000});
    BOOST_CHECK(drained);
    host->Close();
    BOOST_CHECK(!host->close_error().has_value());
}

// T11（F4）：Run 提前返回（端口已被占用）时 Start 必须及时返回错误，
// 而不是等满相位窗口——Run 结果经 promise/future 跨线程发布，wait_for(0)
// 即刻可见。修复前读按值复制的过期快照，必然等满 5s。
BOOST_AUTO_TEST_CASE(wire_engine_start_occupied_port_fails_fast)
{
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    if (!g_scheduler->IsInitialized())
        g_scheduler->Start();
    BOOST_REQUIRE(g_scheduler->IsInitialized());
    auto limits = Limits(std::chrono::milliseconds{30000});

    auto ok_host = std::make_shared<fw::InfraHttpHost>(
        limits, inf::ListenAddress{"127.0.0.1", 0});
    ok_host->InstallRpcHandler(
        [](inf::IncomingCallContext, inf::RpcEnvelope env)
            -> fw::result<inf::RpcEnvelope> {
            return fw::result<inf::RpcEnvelope>::ok(std::move(env));
        });
    BOOST_REQUIRE(ok_host->Create());
    BOOST_REQUIRE(ok_host->Start());
    const auto bound = ok_host->bound_address();
    BOOST_REQUIRE(bound.has_value());

    // 第二宿主同地址监听：Engine::Run 绑定必失败且提前返回。
    auto dup_host = std::make_shared<fw::InfraHttpHost>(
        limits, inf::ListenAddress{"127.0.0.1", bound.value().port});
    dup_host->InstallRpcHandler(
        [](inf::IncomingCallContext, inf::RpcEnvelope env)
            -> fw::result<inf::RpcEnvelope> {
            return fw::result<inf::RpcEnvelope>::ok(std::move(env));
        });
    BOOST_REQUIRE(dup_host->Create());
    const auto t0 = Clock::now();
    auto r = dup_host->Start();
    const auto elapsed = Clock::now() - t0;
    BOOST_REQUIRE(!r);
    // 判别窗口：修复前等满 kEngineRunWait(5000ms)；修复后 Run 返回即刻可见。
    // 取 3000ms 保留充足调度余量，仍与 5000ms 超时路径分离。
    BOOST_CHECK(elapsed < std::chrono::milliseconds{3000});
    dup_host->Close();
    ok_host->StopAccepting();
    BOOST_CHECK(ok_host->WaitHandlersDone(
        Clock::now() + std::chrono::milliseconds{5000}));
    ok_host->Close();
}

// T12（F3）：ProtoWireV1 出站恢复 runtime 限额 owner 的请求/响应字节校验
// ——超限请求在发送侧被拒，远端 handler 零触达。修复前走默认
// http::ClientOptions（body=8MiB），超限请求实际进入远端 handler。
BOOST_AUTO_TEST_CASE(wire_egress_body_limit_rejected_remote_untouched)
{
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    if (!g_scheduler->IsInitialized())
        g_scheduler->Start();
    BOOST_REQUIRE(g_scheduler->IsInitialized());
    g_probe = std::make_shared<Probe>(0);
    g_budget_calls.store(0);
    g_inbound_entries.store(0);

    // 被测 app：ProtoWireV1 入站 + 观测 handler（入口计数 g_inbound_entries）。
    auto box =
        PrepareApp(std::chrono::milliseconds{30000}, std::chrono::milliseconds{5000},
                   /*routes*/ {}, [](fw::CoApp& app) { BOOST_REQUIRE(app.add_service<BudgetSvc>(ConcurrentOpts())); });
    InstallBudgetProbeHandler(box.host, *box.app);
    LaunchApp(box);
    BOOST_REQUIRE(box.host->rpc_server() != nullptr);

    // 出站宿主：把 max_body_bytes 压到 128——network_limits 的请求字节校验
    // 必须在发起任何 I/O 前拒绝 2048B 的 wire body（旧链同一语义）。
    inf::NetworkLimits caller_limits{
        /*max_connections*/ 16,
        /*max_inflight*/ 16,
        /*max_header_bytes*/ 16384,
        /*max_body_bytes*/ 128,
        std::chrono::milliseconds{30000}};
    auto caller = std::make_shared<fw::InfraHttpHost>(
        caller_limits, inf::ListenAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(caller->Create());
    BOOST_REQUIRE(caller->Start());   // 纯出站宿主：不监听

    auto env = MakeEnv("budget", "probe", "t12-big", 7);
    env.payload.assign(2048, 0x42);   // wire body 远超 128B 上限
    auto r = SendWire(caller, box.endpoint, env, std::chrono::milliseconds{5000});
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == fw::ErrorCode::InvalidArgument);
    // 远端 handler 零触达：拒绝发生在发起 I/O 之前（不止探针 rc，看远端计数）。
    BOOST_CHECK(g_inbound_entries.load() == 0);
    BOOST_CHECK(g_budget_calls.load() == 0);

    caller->Close();
    box.app->request_shutdown();
    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
}
#endif

BOOST_AUTO_TEST_SUITE_END()

} // namespace
