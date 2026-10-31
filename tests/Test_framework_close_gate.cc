// 关闭门判别回归：StopAccepting 闭门的最小根因修复。
//
// 背景：旧实现只在 Close() 置关拒绝门（InfraHttpHost::HandlerState::
// open）。关闭顺序是 StopAccepting → WaitHandlersDone → Close，故
// [StopAccepting, Close) 窗口内 open 仍为 true：一个 infra 在 StopAccepting 之前
// 已用 TryAcquireHandler 接纳、但协程尚未进入本类入站包装（_InvokeRpc/_InvokeHttp）
// 的请求，若在 WaitHandlersDone 返回之后、Close 置关之前进入，会过门并触达业务。
// 根因修：StopAccepting 内先于 infra StopAccepting，在同一 state mutex 下闭门。
//
// 本文件的判别性：
//  - 修法后：StopAccepting 之后进入的通行者被拒（RuntimeUnavailable），不触达
//    handler；已过门者不被抢占，WaitHandlersDone 不漏记。
//  - 修法前（负例判别力）：上面的断言会真实失败——迟到进入者会过门、触达 handler
//    （计数 +1，late 为 ok）。故本文件对本次根因修复有真实判别力，而非恒真回归。
//
// 测试缝：本回归的判别点是「infra 已接纳、协程尚未进入入站包装」的到达时刻。infra
// pin（@3136d1de）没有 http Dispatch→RunHandler 的可控注入点（无 gate/tick seam，
// 且公开面不暴露已注册的 handler），故无法在不改 infra 的前提下从网络侧决定性地
// 命中该时刻。本文件经 InfraHttpHost 自身新增的 internal 测试缝 **InvokeRpcForTest**
// 在受控时刻重放该进入——走与 infra 完全相同的生产包装（同一 _InvokeRpc 与同一
// 拒绝门），只把调度交给测试。该缝只有 RPC 一路：受审候选（StopAccepting 窗口）只存在于 RPC
// 包装路径的关闭序列中；HTTP 侧未新增缝（现头无 InvokeHttpForTest），本文件也不调用。
//
// T1 仅用测试缝（确定性、无 socket）：StopAccepting 闭门、不触达业务、WaitHandlersDone
//    不漏、放行后收束、Close 幂等。
// T2 走真实 infra loopback（HTTP）：已过门 handler 不被 StopAccepting 抢占，排空后
//    WaitHandlersDone 返回 ok、回复路径不被关断。
// T3 独立宿主：直接 Close（不经 StopAccepting）也拒绝之后进入者——兼容性小断言。
//    修前/修后均应通过，**不是**本回归判别点，只固定已有语义不被后续改动去掉。
//
// 第二组判别性（异常路径：handler 抛异常时框架在途计数不能漏减）：
//  - 事实：infra 侧 `HttpSession::RunHandler` 有 try/catch 边界（异常转 InternalError、
//    并 `OnHandlerExited()` 无条件回收 infra 自己的记账），但框架 `_InvokeHttp`/
//    `_InvokeRpc` 的在途记账是自己的（HandlerState::inflight）。旧实现把 `--inflight`
//    写在 `st->http(...)`/`st->rpc(...)` 之后：handler（或其间任何分配/业务）抛出即
//    越过减一 → inflight 永久 +1 → WaitHandlersDone 耗尽预算后永久 TimedOut，
//    HostLifecycle 以 Deadline::max() 无界续等 → 宿主保活不释放。
//  - 修后：减一由构造于 ++ 之后的 noexcept 作用域守卫承担，正常返回与异常栈展开
//    都走同一条「锁内 --inflight + notify_all」。
//  - T4 走测试缝（确定性、无 socket）：handler 抛 std::runtime_error，异常按原语义
//    外传到调用方（该缝无 infra 边界），此后 WaitHandlersDone 必须 ok（修前 TimedOut）。
//  - T5 走真实 infra loopback（HTTP）：handler 抛 std::runtime_error，infra 边界按契约
//    转 InternalError（客户端收到 500 终态回复、进程不抛异常），WaitHandlersDone
//    必须 ok（修前 TimedOut）。
//
// 失败路径安全（本文件同时是关闭门回归的**安全**夹具）：
//  - 跨线程/跨帧共享的状态（计数、门、闩、结果槽）一律放 shared_ptr 自我持有，并按
//    值捕获进 handler 闭包与协程闭包：测试帧提前退出（BOOST_REQUIRE 失败、超时）也
//    不会给仍在途的 handler/协程留悬垂栈引用。
//  - 收尾放 RAII 夹具析构（先放行被测门 → 有界等待 → 幂等关闭）：失败路径同样执行，
//    既不会析构 joinable 的 std::thread（否则 std::terminate），也不会无界等待掩盖错误。
//
// 与上游单测一致：Boost.Test 经 included/unit_test.hpp 静态内嵌。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
// T6/T7 的真实 ProtoWireV1 loopback：有效 wire body 才能过 infra 解析抵达
// framework handler（否则在 HandleRpcRequest 编码边界即被拒，不形成
// in-flight/TaskLease）。
#include <bbt/infra/rpc/RpcWire.hpp>
#endif

#include <bbt/framework/internal/InfraHttpHost.hpp>

namespace fw = bbt::framework;
namespace inf = bbt::infra;
namespace co = bbt::coroutine;

namespace {

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

// 一次性放行门（cv 语义，非 sleep 凑时序）。
struct Gate {
    std::mutex m;
    std::condition_variable cv;
    bool open = false;
    void Open()
    {
        std::lock_guard<std::mutex> lk(m);
        open = true;
        cv.notify_all();
    }
    void Wait()
    {
        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [this] { return open; });
    }
};

// 倒计数闩（有界等待）。
struct Latch {
    std::mutex m;
    std::condition_variable cv;
    int count;
    explicit Latch(int n) : count(n)
    {
    }
    void Down()
    {
        std::lock_guard<std::mutex> lk(m);
        if (--count == 0)
            cv.notify_all();
    }
    bool WaitFor(std::chrono::milliseconds ms)
    {
        std::unique_lock<std::mutex> lk(m);
        return cv.wait_for(lk, ms, [this] { return count == 0; });
    }
};

inf::NetworkLimits Limits()
{
    return inf::NetworkLimits{/*max_connections*/ 64,
                              /*max_inflight   */ 64,
                              /*max_header_bytes*/ 64 * 1024,
                              /*max_body_bytes */ 1024 * 1024,
                              /*incoming_timeout*/ 30s};
}

int Code(const fw::Error& e)
{
    return static_cast<int>(e.code);
}

// T1 的跨线程状态：计数、门、闩、结果槽都由 shared_ptr 自我持有，测试线程与
// handler 闭包均按值捕获——任一方先退出都不产生悬垂访问。
struct T1State {
    std::atomic<int> calls{0};
    Gate first_gate;     // 首个（已过门）handler 挂此门
    Latch first_done{1}; // 调用线程已离开包装
    // 由调用线程写、测试线程在读前经 first_done（或 join）同步后读。
    std::optional<fw::result<inf::RpcEnvelope>> entered;
};

// T1 夹具（RAII）：析构即安全收尾，BOOST_REQUIRE 早退同样执行。
// 顺序固定：先放行被测门（handler 与调用线程随即解除阻塞）→ join（此时必然有界）→
// 幂等 Close。绝不 detach、绝不无界等待。
struct T1Harness {
    std::shared_ptr<T1State> st;
    std::shared_ptr<fw::InfraHttpHost> host;
    std::thread th;

    T1Harness() = default;
    T1Harness(const T1Harness&) = delete;
    T1Harness& operator=(const T1Harness&) = delete;
    ~T1Harness()
    {
        if (st)
            st->first_gate.Open();
        if (th.joinable())
            th.join();
        if (host)
            host->Close();
    }
};

// T3 的最小探针：只验证 handler 未被触达。
struct CloseProbe {
    std::atomic<int> calls{0};
};

} // namespace

BOOST_AUTO_TEST_SUITE(framework_rpc_close_gate)

// T1（R1）：StopAccepting 在同一 state mutex 内先于 infra StopAccepting 闭门——
// 「infra 已接纳、尚未进入包装」的迟到进入者被拒、不触达业务；已过门者不抢占、
// WaitHandlersDone 不漏记；Close 幂等。
BOOST_AUTO_TEST_CASE(stop_accepting_closes_gate_rejects_late_entrant)
{
    auto st = std::make_shared<T1State>();
    T1Harness h;
    h.st = st;
    h.host = std::make_shared<fw::InfraHttpHost>(Limits(), inf::ListenAddress{"127.0.0.1", 0});
    const auto host = h.host;

    host->InstallRpcHandler([st](inf::IncomingCallContext, inf::RpcEnvelope req) -> fw::result<inf::RpcEnvelope> {
        const int n = st->calls.fetch_add(1);
        // 只有「已过门」的首个请求挂起；迟到进入者若过门则立即返回——
        // 使负例（修法前）得到干净失败而非死等，判别力可观测。
        if (n == 0)
            st->first_gate.Wait();
        return fw::result<inf::RpcEnvelope>::ok(std::move(req));
    });

    // 调用线程只触碰 shared_ptr 状态与宿主 shared_ptr；ctx/envelope 在线程内自建。
    h.th = std::thread([st, host] {
        inf::IncomingCallContext ctx;
        ctx.deadline = Clock::now() + 30s;
        st->entered.emplace(host->InvokeRpcForTest(ctx, inf::RpcEnvelope{"svc", "m", "rid-1", "", "", {}, {}}));
        st->first_done.Down();
    });

    // 等首个进入包装并挂起（inflight==1）。失败早退由 T1Harness 收尾。
    {
        const auto until = Clock::now() + 5s;
        while (st->calls.load() == 0) {
            BOOST_REQUIRE_MESSAGE(Clock::now() < until, "entered handler never reached wrapper");
            std::this_thread::yield();
        }
    }

    // StopAccepting：修复后必在此闭门（先于 infra StopAccepting）。
    host->StopAccepting();

    // 迟到进入者（infra 已接纳、尚未进入包装）必被拒，且不触达业务。
    inf::IncomingCallContext ctx2;
    ctx2.deadline = Clock::now() + 30s;
    auto late = host->InvokeRpcForTest(ctx2, inf::RpcEnvelope{"svc", "m", "rid-2", "", "", {}, {}});
    BOOST_CHECK_MESSAGE(!late, "late entrant must be refused after StopAccepting");
    if (!late)
        BOOST_CHECK_EQUAL(Code(late.error()), Code(fw::MakeError(fw::ErrorCode::RuntimeUnavailable, "x")));
    BOOST_CHECK_EQUAL(st->calls.load(), 1); // 拒绝发生在 handler 之前

    // WaitHandlersDone 不漏：已过门者仍在途 → 预算内不返回 ok。
    auto r1 = host->WaitHandlersDone(Clock::now() + 150ms);
    BOOST_CHECK_MESSAGE(!r1, "drain must not report success with inflight>0");
    if (!r1)
        BOOST_CHECK(r1.error().code == fw::ErrorCode::TimedOut);

    // 放行已过门者：不被抢占（等到它自己完成）。
    st->first_gate.Open();
    BOOST_REQUIRE(st->first_done.WaitFor(5s));
    BOOST_CHECK(st->entered.has_value());
    if (st->entered) {
        BOOST_CHECK(bool(*st->entered));
        if (*st->entered)
            BOOST_CHECK_EQUAL(st->entered->value().request_id, "rid-1");
    }
    BOOST_CHECK_EQUAL(st->calls.load(), 1);

    // 排空后 WaitHandlersDone 返回 ok。
    BOOST_CHECK(host->WaitHandlersDone(Clock::now() + 2s));
    if (h.th.joinable())
        h.th.join();

    // Close 幂等：此处 StopAccepting 已闭门，故下面两条只证明「Close 之后仍拒绝」
    // 与幂等，不构成「不经 StopAccepting 的直接 Close」证据（那由 T3 独立宿主覆盖）。
    host->Close();
    host->Close();
    auto late2 = host->InvokeRpcForTest(ctx2, inf::RpcEnvelope{"svc", "m", "rid-3", "", "", {}, {}});
    BOOST_CHECK_MESSAGE(!late2, "entrant after Close must be refused");
    if (!late2)
        BOOST_CHECK(late2.error().code == fw::ErrorCode::RuntimeUnavailable);
    BOOST_CHECK_EQUAL(st->calls.load(), 1);
}

// T3：独立宿主的「直接 Close」兼容小断言——不经 StopAccepting，Close 也必须置关
// （拒绝之后进入者、不触达 handler），且幂等。宿主未 Create/Start（无 runtime），
// 断言只针对拒绝门，语义与已启动宿主一致（同一 m_state 门）。修前代码同样在 Close
// 置关，故此用例不是本回归判别点。
BOOST_AUTO_TEST_CASE(direct_close_without_stop_accepting_refuses_entrant)
{
    auto probe = std::make_shared<CloseProbe>();
    fw::InfraHttpHost host(Limits(), inf::ListenAddress{"127.0.0.1", 0});
    host.InstallRpcHandler([probe](inf::IncomingCallContext, inf::RpcEnvelope req) -> fw::result<inf::RpcEnvelope> {
        probe->calls.fetch_add(1);
        return fw::result<inf::RpcEnvelope>::ok(std::move(req));
    });

    host.Close();
    host.Close(); // 幂等

    inf::IncomingCallContext ctx;
    ctx.deadline = Clock::now() + 30s;
    auto late = host.InvokeRpcForTest(ctx, inf::RpcEnvelope{"svc", "m", "rid-direct", "", "", {}, {}});
    BOOST_CHECK_MESSAGE(!late, "direct Close must refuse entrant without StopAccepting");
    if (!late)
        BOOST_CHECK(late.error().code == fw::ErrorCode::RuntimeUnavailable);
    BOOST_CHECK_EQUAL(probe->calls.load(), 0);
}

// T2/T5 共用的跨帧共享状态：handler 闭包与协程闭包都按值捕获 shared_ptr，测试帧先退
// （REQUIRE 失败/超时）也不给在途 handler/协程留悬垂栈引用。
struct LoopbackState {
    std::atomic<int> calls{0};
    Gate handler_gate;                                // 已过门 handler 挂此门（T5 不用：handler 立即抛）
    Latch entered{1};                                 // handler 已进入包装
    std::atomic<bool> returned{false};                // handler 已返回（未被抢占的观测，T5 不用）
    std::optional<fw::result<inf::HttpResponse>> out; // 客户端协程结果槽
    Latch done{1};                                    // 客户端协程已收尾
};

// T2/T5 共用夹具：成员自我持有，析构即收尾。REQUIRE 早退同样走到这里。
struct LoopbackHarness {
    std::shared_ptr<LoopbackState> st = std::make_shared<LoopbackState>();
    std::shared_ptr<fw::InfraHttpHost> host;
    std::shared_ptr<inf::NetworkRuntime> rt;
    std::shared_ptr<inf::HttpClient> client;
    std::string endpoint;

    LoopbackHarness() : host(std::make_shared<fw::InfraHttpHost>(Limits(), inf::ListenAddress{"127.0.0.1", 0}))
    {
    }
    LoopbackHarness(const LoopbackHarness&) = delete;
    LoopbackHarness& operator=(const LoopbackHarness&) = delete;
    ~LoopbackHarness()
    {
        Stop();
    }

    // 幂等收尾，全部有界（不用无界等待掩盖错误）：先放行被测门，让在途 handler 与
    // 客户端请求按真实路径收束，再有界等待排空，最后按宿主 → client → runtime 关闭。
    // 即便某一步超预算，跨帧状态仍由 shared_ptr 保活，不产生悬垂访问。
    void Stop()
    {
        st->handler_gate.Open();
        (void)host->WaitHandlersDone(Clock::now() + 2s);
        (void)st->done.WaitFor(2s);
        if (client)
            client->Close();
        if (rt)
            rt->Close();
        if (host)
            host->Close();
    }
};

// T2（R2）：真实 infra loopback（HTTP）——已过门 handler 不被 StopAccepting 抢占，
// 关闭序列等到它完成后 WaitHandlersDone 返回 ok，回复路径不被 StopAccepting 关断。
BOOST_AUTO_TEST_CASE(entered_handler_not_preempted_and_drained)
{
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
        g_scheduler->Start();
    }

    LoopbackHarness h;
    const auto st = h.st;
    h.host->InstallHandler([st](inf::IncomingCallContext, inf::HttpRequest) -> fw::result<inf::HttpResponse> {
        st->calls.fetch_add(1);
        st->entered.Down();
        st->handler_gate.Wait(); // 已过门：挂起，等测试放行
        st->returned.store(true);
        return fw::result<inf::HttpResponse>::ok(inf::HttpResponse{200, {}, "ok"});
    });

    BOOST_REQUIRE(h.host->Create());
    BOOST_REQUIRE(h.host->Start());
    h.endpoint = h.host->bound_endpoint();
    BOOST_REQUIRE(!h.endpoint.empty());

    auto rtr = inf::NetworkRuntime::Create(Limits());
    BOOST_REQUIRE(rtr);
    h.rt = rtr.value();
    BOOST_REQUIRE(h.rt->Start());
    auto cl = h.rt->CreateHttpClient();
    BOOST_REQUIRE(cl);
    h.client = cl.value();

    // 协程闭包按值捕获 shared_ptr 状态、client 与 endpoint：测试帧退出后
    // 仍在途的客户端请求不会解引用已析构的栈对象。
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [st, client = h.client, endpoint = h.endpoint] {
            inf::HttpRequest req;
            req.method = "GET";
            req.url = "http://" + endpoint + "/probe";
            inf::CallOptions opt;
            opt.deadline = Clock::now() + 5s;
            st->out.emplace(client->Request(std::move(req), opt));
            st->done.Down();
        },
        succ);
    BOOST_REQUIRE(succ);

    // handler 已过门并挂起（inflight==1）。
    BOOST_REQUIRE(st->entered.WaitFor(5s));
    BOOST_CHECK_EQUAL(st->calls.load(), 1);

    // 修复后 StopAccepting 在此闭门；已过门者不被抢占。
    h.host->StopAccepting();
    BOOST_CHECK(!st->returned.load());
    auto r = h.host->WaitHandlersDone(Clock::now() + 150ms);
    BOOST_CHECK_MESSAGE(!r, "entered handler must keep drain pending");
    if (!r)
        BOOST_CHECK(r.error().code == fw::ErrorCode::TimedOut);
    BOOST_CHECK(!st->returned.load()); // StopAccepting 未取消在途 handler

    // 放行 → handler 完成 → 排空。
    st->handler_gate.Open();
    BOOST_REQUIRE(st->done.WaitFor(5s));
    BOOST_REQUIRE(st->out.has_value());
    BOOST_CHECK_MESSAGE(bool(st->out.value()), "reply path must stay open across StopAccepting");
    BOOST_CHECK(st->returned.load());
    BOOST_CHECK(h.host->WaitHandlersDone(Clock::now() + 2s));

    // 正常路径同样走幂等收尾（宿主 → client → runtime），析构兜底不再重复动作。
    h.Stop();
}

// T4：handler 抛异常的框架侧判别（RPC 测试缝，确定性、无 socket）。
// 该缝无 infra 的 RunHandler 边界，故异常按原语义外传到调用方；判别点是「异常栈
// 展开也必须减在途计数」——修前 inflight 永久 +1，下面的排空断言会真实失败
// （TimedOut），而非恒真。
BOOST_AUTO_TEST_CASE(rpc_handler_exception_propagates_and_releases_inflight)
{
    auto probe = std::make_shared<CloseProbe>(); // T3/T4 共用的最小计数探针
    fw::InfraHttpHost host(Limits(), inf::ListenAddress{"127.0.0.1", 0});
    host.InstallRpcHandler([probe](inf::IncomingCallContext, inf::RpcEnvelope) -> fw::result<inf::RpcEnvelope> {
        probe->calls.fetch_add(1);
        throw std::runtime_error("rpc handler boom");
    });

    inf::IncomingCallContext ctx;
    ctx.deadline = Clock::now() + 30s;
    bool threw = false;
    try {
        auto r = host.InvokeRpcForTest(ctx, inf::RpcEnvelope{"svc", "m", "rid-exc", "", "", {}, {}});
        (void)r;
    } catch (const std::runtime_error&) {
        threw = true;
    }
    BOOST_CHECK_MESSAGE(threw, "seam must propagate the handler exception to the caller unchanged");
    BOOST_CHECK_EQUAL(probe->calls.load(), 1); // 异常发生在 handler 内

    // 判别点：异常栈展开已减一 → 排空成立。修前 inflight 停在 1 → TimedOut。
    auto drained = host.WaitHandlersDone(Clock::now() + 2s);
    BOOST_CHECK_MESSAGE(drained, "in-flight count must be released when the handler unwinds by exception");
    if (!drained)
        BOOST_CHECK(drained.error().code == fw::ErrorCode::TimedOut);

    // 记账未被破坏：换一个正常 handler 再进一次仍能记账并排空——双重减一会把
    // size_t 减成极大值，下面的排空断言随即失败。
    host.InstallRpcHandler([](inf::IncomingCallContext, inf::RpcEnvelope req) -> fw::result<inf::RpcEnvelope> {
        return fw::result<inf::RpcEnvelope>::ok(std::move(req));
    });
    auto ok_call = host.InvokeRpcForTest(ctx, inf::RpcEnvelope{"svc", "m", "rid-ok", "", "", {}, {}});
    BOOST_CHECK(bool(ok_call));
    if (ok_call)
        BOOST_CHECK_EQUAL(ok_call.value().request_id, "rid-ok");
    BOOST_CHECK(host.WaitHandlersDone(Clock::now() + 2s));
}

// T5：handler 抛异常的框架侧判别（真实 infra loopback，HTTP）。
// infra 契约（`HttpServer.hpp`：异常由边界捕获转 InternalError；实现
// `HttpServerImpl::RunHandler`）把 handler 异常在协程边界内吞掉 → 客户端收到 500
// 终态回复、进程不抛异常。但框架自己的在途记账仍必须归零——修前 inflight 永久 +1，
// 下面的排空断言会真实失败（TimedOut）。
BOOST_AUTO_TEST_CASE(loopback_http_handler_exception_drains_inflight)
{
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
        g_scheduler->Start();
    }

    LoopbackHarness h;
    const auto st = h.st;
    h.host->InstallHandler([st](inf::IncomingCallContext, inf::HttpRequest) -> fw::result<inf::HttpResponse> {
        st->calls.fetch_add(1);
        st->entered.Down();
        throw std::runtime_error("http handler boom"); // 越过 _InvokeHttp 调用点
    });

    BOOST_REQUIRE(h.host->Create());
    BOOST_REQUIRE(h.host->Start());
    h.endpoint = h.host->bound_endpoint();
    BOOST_REQUIRE(!h.endpoint.empty());

    auto rtr = inf::NetworkRuntime::Create(Limits());
    BOOST_REQUIRE(rtr);
    h.rt = rtr.value();
    BOOST_REQUIRE(h.rt->Start());
    auto cl = h.rt->CreateHttpClient();
    BOOST_REQUIRE(cl);
    h.client = cl.value();

    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [st, client = h.client, endpoint = h.endpoint] {
            inf::HttpRequest req;
            req.method = "GET";
            req.url = "http://" + endpoint + "/probe";
            inf::CallOptions opt;
            opt.deadline = Clock::now() + 5s;
            st->out.emplace(client->Request(std::move(req), opt));
            st->done.Down();
        },
        succ);
    BOOST_REQUIRE(succ);

    BOOST_REQUIRE(st->entered.WaitFor(5s)); // handler 已进入（抛异常前）
    BOOST_CHECK_EQUAL(st->calls.load(), 1);
    // 客户端拿到终态回复：infra 边界把异常转 InternalError → 500。
    BOOST_REQUIRE(st->done.WaitFor(5s));
    BOOST_REQUIRE(st->out.has_value());
    BOOST_REQUIRE_MESSAGE(bool(st->out.value()),
                          "handler exception must yield a terminal reply, not an aborted connection");
    BOOST_CHECK_EQUAL(static_cast<int>(st->out.value().value().status), 500);

    // 判别点：异常栈展开已减一 → 排空成立。修前 inflight 停在 1 → TimedOut。
    auto drained = h.host->WaitHandlersDone(Clock::now() + 2s);
    BOOST_CHECK_MESSAGE(drained, "framework in-flight count must be released on handler exception");

    h.Stop();
}

// T6（关闭收口责任移交回归，review-fixes §29-42）：迟到 handler/callback 排空
// 收口责任已由 WaitHandlersDone 承担（Drain → Close → WaitStopped），不再由
// Close 的 5s 兜底掩盖。
// 真实判别链（全部经同一真实 loopback InfraHttpHost，无 mock）：
//   1) 已 Start 宿主上发起真实 RPC 请求，handler 挂起（inflight>0）；
//   2) StopAccepting → WaitHandlersDone(有界期限) 必须返回 TimedOut——此时
//      Drain/WaitStopped 尚未执行，rpc::Server 不得已被 Close/Stopped，
//      Engine 仍 Running（owner 未释放），区分「Drain==0 当 tasks==0」；
//   3) 放行 handler → WaitHandlersDone(Deadline::max) 续等必须成功完成
//      全链（inflight 归零 → Drain → server->Close() → WaitStopped）；
//   4) IsClosed() 为 true 证明 Close 已被调用；随后宿主 Close() 释放
//      owner（rpc_server() 置空）且 close_error() 为空——证明 Engine
//      Run 结果与 Stopped 相位被实际检查（非只 join 后放行）。
// 修前：步骤 2 的 WaitStopped 在 Close 内以独立 5s 期限兜底，失败后仍
// RequestClose/join/reset owner——本断言（步骤 4 的 close_error 为空 +
// IsClosed 为真）无法区分；修法后才把失败如实传播给 WaitHandlersDone。
BOOST_AUTO_TEST_CASE(late_handler_waitstopped_failure_then_recover)
{
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
        g_scheduler->Start();
    }
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    struct Shared {
        std::atomic<int> calls{0};
        Gate handler_gate;
        Latch entered{1};
        std::optional<inf::result<inf::HttpResponse>> out;
        Latch done{1};
    };
    auto st = std::make_shared<Shared>();

    auto host = std::make_shared<fw::InfraHttpHost>(
        Limits(), inf::ListenAddress{"127.0.0.1", 0});
    host->InstallRpcHandler(
        [st](inf::IncomingCallContext, inf::RpcEnvelope req)
            -> fw::result<inf::RpcEnvelope> {
            st->calls.fetch_add(1);
            st->entered.Down();
            st->handler_gate.Wait();
            return fw::result<inf::RpcEnvelope>::ok(std::move(req));
        });
    BOOST_REQUIRE(host->Create());
    BOOST_REQUIRE(host->Start());
    const auto bound = host->bound_address();
    BOOST_REQUIRE(bound.has_value());
    BOOST_REQUIRE(host->rpc_server() != nullptr);

    // 真实 loopback 请求：独立 runtime + HttpClient → 被测 server。
    auto rt = inf::NetworkRuntime::Create(Limits());
    BOOST_REQUIRE(rt);
    BOOST_REQUIRE(rt.value()->Start());
    auto client = rt.value()->CreateHttpClient();
    BOOST_REQUIRE(client);

    bool succ = false;
    const auto endpoint = host->bound_endpoint();
    // 有效 ProtoWireV1 wire body：ToWireEnvelope + EncodeRpcWireEnvelope——
    // 过 infra HandleRpcRequest 解析后才抵达 framework handler（形成真实
    // in-flight 与 TaskLease）；裸 POST 空 body 会在编码边界被拒。
    inf::rpc::RpcWireEnvelope wire;
    auto wire_r = inf::rpc::ToWireEnvelope(
        inf::RpcEnvelope{"probe", "hold", "t6-1", "", "", {}, {}},
        /*remaining_budget_ms*/ 30000);
    BOOST_REQUIRE(wire_r);
    wire = wire_r.value();
    auto body = inf::rpc::EncodeRpcWireEnvelope(wire, /*is_response=*/false);
    BOOST_REQUIRE(body);
    g_scheduler->RegistCoroutineTask(
        [st, client = client.value(), endpoint, body = std::move(body.value())] {
            inf::HttpRequest req;
            req.method = "POST";
            req.url = "http://" + endpoint + "/rpc";
            req.headers = {{std::string(inf::rpc::kRpcWireContentTypeKey),
                            std::string(inf::rpc::kRpcWireContentType)}};
            req.body.assign(body.begin(), body.end());
            inf::CallOptions opt;
            opt.deadline = Clock::now() + 5s;
            st->out.emplace(client->Request(std::move(req), opt));
            st->done.Down();
        },
        succ);
    BOOST_REQUIRE(succ);

    // handler 已过门并挂起（framework inflight==1 + infra inflight==1）。
    BOOST_REQUIRE(st->entered.WaitFor(5s));
    BOOST_CHECK_EQUAL(st->calls.load(), 1);

    host->StopAccepting();

    // 有界期限：inflight>0 → TimedOut；此时 server 不得已被 Close/Stopped，
    // Engine 不得已被 RequestClose——owner 未释放（fail-closed 前段）。
    auto r1 = host->WaitHandlersDone(Clock::now() + 150ms);
    BOOST_CHECK_MESSAGE(!r1, "drain must fail while handler in-flight");
    if (!r1)
        BOOST_CHECK(r1.error().code == fw::ErrorCode::TimedOut);
    BOOST_REQUIRE(host->rpc_server() != nullptr);
    BOOST_CHECK(!host->rpc_server()->IsClosed());   // 未提前逻辑封口
    // 底层 Engine 仍在驱动（AcceptingStopped 未置位前 Run 不返回），未停。

    // 放行 → Deadline::max 续等：inflight 归零 → Drain → Close →
    // WaitStopped 全链成功。
    st->handler_gate.Open();
    BOOST_REQUIRE(st->done.WaitFor(5s));
    auto r2 = host->WaitHandlersDone(co::Deadline::max());
    BOOST_CHECK_MESSAGE(r2,
        "Deadline::max retry must complete full server drain+stop chain");

    // 收口凭据：rpc::Server 已逻辑关闭（IsClosed），WaitStopped 已成功。
    BOOST_REQUIRE(host->rpc_server() != nullptr);
    BOOST_CHECK(host->rpc_server()->IsClosed());

    // Close：取得收口凭据后停 Engine（Run 返回 + Phase==Stopped 实际检查）
    // 并释放 owner——无 close_error 证明 Engine 未走「未达 Stopped 保留」路径。
    host->Close();
    BOOST_CHECK(!host->close_error().has_value());
    BOOST_CHECK(host->rpc_server() == nullptr);   // owner 已释放

    client.value()->Close();
    rt.value()->Close();
}

// T7（直接 Close 兜底路径）：不经 StopAccepting/WaitHandlersDone 的裸
// Close 也必须安全收口——兜底 WaitStopped 成功后正常释放（原同步关闭契约），
// 不是把「未走 WaitHandlersDone」当永久失败保留 owner。
BOOST_AUTO_TEST_CASE(direct_close_on_started_host_releases_owners)
{
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
        g_scheduler->Start();
    }
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    auto probe = std::make_shared<CloseProbe>();
    auto host = std::make_shared<fw::InfraHttpHost>(
        Limits(), inf::ListenAddress{"127.0.0.1", 0});
    host->InstallRpcHandler(
        [probe](inf::IncomingCallContext, inf::RpcEnvelope req)
            -> fw::result<inf::RpcEnvelope> {
            probe->calls.fetch_add(1);
            return fw::result<inf::RpcEnvelope>::ok(std::move(req));
        });
    BOOST_REQUIRE(host->Create());
    BOOST_REQUIRE(host->Start());
    BOOST_REQUIRE(host->bound_address().has_value());

    // 直接 Close：fallback WaitStopped 必成功（无在途 handler/callback），
    // owner 正常释放，close_error 为空。
    host->Close();
    BOOST_CHECK(!host->close_error().has_value());
    BOOST_CHECK(host->rpc_server() == nullptr);
    // 幂等重入安全（不重复 join/get）。
    host->Close();
}

BOOST_AUTO_TEST_SUITE_END()
