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
//                                      （客户端见 TransportError 即逻辑结束）
//                                      → 排空仍等待物理在途 → ShutdownIncomplete
//                                      → 迟到回调安全执行（资源仍存活）→
//                                      kExitShutdownLate。
//  T6 waitclosed_budget_exhaustion       WaitClosed 步预算耗尽路径（INetworkHost
//                                      桩，无 socket）：第二次等待获得无界期
//                                      限、未完成项可观察、run 不返回、迟到收
//                                      尾 → kExitShutdownLate。
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
#include <bbt/coroutine/sync/CompletionSignal.hpp>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>

// 机器面（注册表/生命周期/真实宿主/线桥/测试缝构造）显式走 internal 头。
#include <bbt/framework/internal/ActorRegistry.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/HostLifecycle.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>
#include <bbt/framework/internal/RpcHttpBridge.hpp>

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

    // 业务 handler 挂起点：CompletionSignal 依赖运行时 generation，只能
    // 在调度器已 Start 后创建——由首个挂起的 handler 惰性建立；测试经
    // CompleteGate 放行（非 sleep 时序）。
    std::shared_ptr<co::CompletionSignal> Gate() {
        std::lock_guard<std::mutex> lk(mtx);
        if (!gate)
            gate = std::make_shared<co::CompletionSignal>();
        return gate;
    }
    void CompleteGate() {
        std::lock_guard<std::mutex> lk(mtx);
        if (gate) gate->Complete();
    }

    mutable std::mutex      mtx;
    std::shared_ptr<co::CompletionSignal> gate;
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
        const auto st = gate->Wait(
            {ctx.value()->deadline, ctx.value()->cancel});
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
        const auto st = gate->Wait(
            {co::Deadline::max(), co::CancellationToken{}});
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
        const auto st = gate->Wait(
            {ctx.value()->deadline, ctx.value()->cancel});
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

void WaitClosedOnThread(const std::shared_ptr<inf::NetworkRuntime>& rt,
                        std::chrono::milliseconds ms) {
    const auto until = Clock::now() + ms;
    while (!rt->IsClosed()) {
        if (Clock::now() > until) return;
        std::this_thread::yield();
    }
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
        if (client) client->RequestClose();
        if (rt)     rt->RequestClose();
        if (rt)     WaitClosedOnThread(rt, kWait);
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
        if (rt)     rt->RequestClose();
        if (rt)     WaitClosedOnThread(rt, kWait);
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
    o.cancel   = {};
    auto res = client->Request(
        Wire::ToHttpRequest(env, "http://" + endpoint + "/rpc"), o);
    if (!res)
        return fw::result<inf::RpcEnvelope>::err(std::move(res.error()));
    return Wire::EnvelopeFromResponse(res.value());
}

// 控制线程发起一次 RPC 并限时等待结果。
fw::result<inf::RpcEnvelope> CallRpc(
    const TestClient& tc, const std::string& endpoint,
    const inf::RpcEnvelope& env,
    std::chrono::milliseconds budget = kWait) {
    std::optional<fw::result<inf::RpcEnvelope>> out;
    TestLatch done{1};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            out.emplace(SendRpc(tc.client, endpoint, env, budget));
            done.CountDown();
        },
        succ);
    if (!succ || !done.WaitFor(budget))
        return fw::result<inf::RpcEnvelope>::err(fw::MakeError(
            fw::ErrorCode::InternalError, "CallRpc did not complete"));
    return std::move(*out);
}

// 有界状态轮询（显式状态判据，非 sleep）：直到谓词为真或预算耗尽。
bool WaitState(const fw::CoApp& app, fw::ShutdownState want,
               std::chrono::milliseconds ms) {
    const auto until = Clock::now() + ms;
    while (app.shutdown_state() != want) {
        if (Clock::now() > until) return false;
        std::this_thread::yield();
    }
    return true;
}

// ── INetworkHost 桩：记录调用序列；T6 用于驱动 WaitClosed 预算耗尽路径 ──

class StubNetHost final : public fw::INetworkHost {
public:
    std::function<fw::result<void>(co::Deadline, co::CancellationToken)>
        on_wait_handlers;
    std::function<bbt::infra::CloseStatus(co::Deadline, co::CancellationToken)>
        on_wait_closed;
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
    fw::result<void> WaitHandlersDone(co::Deadline d,
                                      co::CancellationToken t) override {
        _Record("net.wait_handlers");
        if (on_wait_handlers) return on_wait_handlers(d, t);
        return fw::result<void>::ok();
    }
    void RequestClose() noexcept override { _Record("net.request_close"); }
    bbt::infra::CloseStatus WaitClosed(co::Deadline d,
                                     co::CancellationToken t) override {
        _Record("net.wait_closed");
        if (on_wait_closed) return on_wait_closed(d, t);
        return bbt::infra::CloseStatus::Closed;
    }
    void ReleaseClosed() noexcept override { _Record("net.release"); }

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
    // 关闭完整走完 Scheduler::Stop：调度器代际归零。
    BOOST_CHECK(g_scheduler->GetRunGeneration() == 0);
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
    // client 所在 runtime 未 RequestClose、服务实例仍存活。
    g_probe->CompleteGate();
    BOOST_REQUIRE(inflight_done.WaitFor(kWait));
    BOOST_REQUIRE(inflight_res.has_value());
    BOOST_REQUIRE(inflight_res.value());
    // +100 为 peer 处理产生：出站确实经 loopback 到达对端并回来。
    BOOST_CHECK(DecodeReply(inflight_res.value()).value() == 109);
    BOOST_CHECK(g_probe->HasEvent("outbound:ok"));
    BOOST_CHECK(peer->hits.load() == 1);

    JoinRun(box.run);
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitOk);
    BOOST_CHECK(box.app->shutdown_state() == fw::ShutdownState::Closed);
    BOOST_CHECK(g_scheduler->GetRunGeneration() == 0);
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
    BOOST_CHECK(g_scheduler->GetRunGeneration() == 0);
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
    BOOST_CHECK(g_scheduler->IsRunning());
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
    BOOST_REQUIRE(inflight_res.value());
    BOOST_CHECK(DecodeReply(inflight_res.value()).value() == 103);
    BOOST_CHECK(g_probe->HasEvent("actor_outbound:ok"));
    BOOST_CHECK(peer->hits.load() == 1);

    JoinRun(box.run);
    // 迟到操作全部结束后：完整收束 + Scheduler::Stop，run 返回非零
    // （记录曾超预算），不是「优雅关闭成功」。
    BOOST_CHECK(box.run->rc == fw::HostLifecycle::kExitShutdownLate);
    BOOST_CHECK(box.app->shutdown_state() == fw::ShutdownState::Closed);
    BOOST_CHECK(box.app->pending_cleanup().empty());
    BOOST_CHECK(box.app->lifecycle_failures().empty());
    BOOST_CHECK(g_scheduler->GetRunGeneration() == 0);
    tc.Stop();
    peer->Stop();
}

// T5：逻辑请求结束 ≠ 物理清理完成——handler 物理在途超过 incoming 看门，
//     客户端已见 TransportError；排空仍等物理结束 → ShutdownIncomplete →
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
    // 结束到期 → infra 切断回复路径 → 客户端见到的是传输错误（逻辑结
    // 束），但物理 handler 协程仍在途。
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
    BOOST_CHECK(inflight_res.value().error().code ==
                fw::ErrorCode::TransportError);
    BOOST_CHECK(!g_probe->HasEvent("late_handler_resumed"));  // 物理仍在途

    box.app->request_shutdown();
    // 物理在途未排空 → 预算耗尽 → ShutdownIncomplete。
    BOOST_REQUIRE(
        WaitState(*box.app, fw::ShutdownState::ShutdownIncomplete, kWait));
    BOOST_CHECK(!box.run->done.load());
    BOOST_CHECK(g_scheduler->IsRunning());
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
    BOOST_CHECK(g_scheduler->GetRunGeneration() == 0);
    tc.Stop();
    peer->Stop();
}

// T6：WaitClosed 步预算耗尽（桩，无 socket）——第二次等待获无界期限、
//     未完成项可观察、run 不返回、迟到收尾 → kExitShutdownLate。
BOOST_AUTO_TEST_CASE(waitclosed_budget_exhaustion) {
    auto host = std::make_shared<StubNetHost>();
    TestLatch unbounded_wait{1};   // 第二次（无界）WaitClosed 已进入
    TestGate  allow_close;         // 迟到收尾放行
    std::atomic<int> closed_calls{0};
    std::atomic<bool> first_deadline_finite{false};
    std::atomic<bool> second_deadline_unbounded{false};

    host->on_wait_closed =
        [&](co::Deadline d, co::CancellationToken)
            -> bbt::infra::CloseStatus {
            const auto call = closed_calls.fetch_add(1);
            if (call == 0) {
                // 预算内等待：期限必须是有限步预算（约 step_budget 量级）。
                first_deadline_finite.store(
                    d < Clock::now() + std::chrono::hours{1},
                    std::memory_order_release);
                return bbt::infra::CloseStatus::TimedOut;
            }
            // 预算耗尽后的续等：宿主以 Deadline::max() 等迟到收尾——
            // 期限约束的是优雅关闭是否成功，不是进程退出硬时限。
            second_deadline_unbounded.store(
                d > Clock::now() + std::chrono::hours{1},
                std::memory_order_release);
            unbounded_wait.CountDown();
            allow_close.Wait();
            return bbt::infra::CloseStatus::Closed;
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

    // WaitClosed 超时 → ShutdownIncomplete；run 不返回、不提前释放。
    BOOST_REQUIRE(unbounded_wait.WaitFor(kWait));
    BOOST_CHECK(app->shutdown_state() ==
                fw::ShutdownState::ShutdownIncomplete);
    const auto pending = app->pending_cleanup();
    BOOST_REQUIRE(pending.size() == 1);
    BOOST_CHECK(pending[0] == "WaitClosed");
    BOOST_CHECK(!h->done.load());
    BOOST_CHECK(g_scheduler->IsRunning());
    {
        const auto names = host->Names();
        BOOST_CHECK(std::find(names.begin(), names.end(),
                              "net.stop_accepting") != names.end());
        BOOST_CHECK(std::find(names.begin(), names.end(),
                              "net.request_close") != names.end());
        BOOST_CHECK(std::find(names.begin(), names.end(),
                              "net.release") == names.end());
    }

    // 迟到收尾完成：走完释放与 Scheduler::Stop，run 返回非零。
    allow_close.Open();
    JoinRun(h);
    BOOST_CHECK(h->rc == fw::HostLifecycle::kExitShutdownLate);
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Closed);
    BOOST_CHECK(app->pending_cleanup().empty());
    BOOST_CHECK(g_scheduler->GetRunGeneration() == 0);
    BOOST_CHECK(first_deadline_finite.load());
    BOOST_CHECK(second_deadline_unbounded.load());
    {
        const auto names = host->Names();
        auto pos = [&names](const char* n) {
            return std::find(names.begin(), names.end(), n) - names.begin();
        };
        BOOST_CHECK(pos("net.stop_accepting") < pos("net.wait_handlers"));
        BOOST_CHECK(pos("net.wait_handlers") < pos("net.request_close"));
        BOOST_CHECK(pos("net.request_close") < pos("net.wait_closed"));
        // 两次 wait_closed（预算内 + 无界续等）都在 release 之前。
        BOOST_CHECK(pos("net.wait_closed") < pos("net.release"));
    }
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace
