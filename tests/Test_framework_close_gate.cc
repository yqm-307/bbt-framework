// 关闭门判别回归：StopAccepting 闭门的最小根因修复。
//
// 背景：旧实现只在 Close() 置关拒绝门（InfraHttpHost::HandlerState::
// open）。关闭顺序是 StopAccepting → WaitHandlersDone → Close，故
// [StopAccepting, Close) 窗口内 open 仍为 true：一个 infra 在 StopAccepting 之前
// 已接纳、但协程尚未进入本类入站包装（_InvokeRpc/_InvokeHttp）的请求，若在
// WaitHandlersDone 返回之后、Close 置关之前进入，会过门并触达业务。
// 根因修：StopAccepting 内先于 infra StopAccepting，在同一 state mutex 下闭门。
//
// 本文件的判别性（V2 面）：
//  - T1 用真实 V2 会话：首个请求已过门并挂起；StopAccepting 之后在同一
//    已建立会话上再发一个请求——该请求 infra 仍会接纳并交给框架同步派发门，
//    修法后必被拒（Closed「host sealed」），不触达 handler。旧实现该迟到
//    进入者会过门（calls 变 2、late 为 ok），故本用例对根因修复有真实
//    判别力，而非恒真回归。
//  - 已过门者不被抢占，WaitHandlersDone 不漏记。
//
// 与旧版「infra 已接纳、协程尚未进入包装」的确定性构造的差别：旧版经
// InfraHttpHost 的 internal 同步派发测试缝在受控时刻重放该进入；该缝随
// V1 一起删除且没有 V2 等价物。本版改用公共 V2 面可证实的终态
// （同一会话上的迟到 CALL 被同步派发门拒绝），不再声称命中了包装前的
// 精确时刻——判别对象从「时刻」改为「StopAccepting 之后不得触达业务」。
//
// 第二组判别性（异常路径：handler 抛异常时框架在途计数不能漏减）：
//  - 事实：infra 侧 `_RunRpc` 有 try/catch 边界（异常转 InternalError、并
//    无条件回收 infra 自己的记账），但框架 `_InvokeHttp`/`_RunRpc` 的在途
//    记账是自己的（HandlerState::inflight）。旧实现把 `--inflight` 写在
//    `st->http(...)`/`st->rpc(...)` 之后：handler（或其间任何分配/业务）抛出即
//    越过减一 → inflight 永久 +1 → WaitHandlersDone 耗尽预算后永久 TimedOut，
//    HostLifecycle 以 Deadline::max() 无界续等 → 宿主保活不释放。
//  - 修后：减一由构造于 ++ 之后的 noexcept 作用域守卫承担，正常返回与异常栈展开
//    都走同一条「锁内 --inflight + notify_all」。
//  - T4 走真实 V2 loopback：handler 抛 std::runtime_error，infra 边界按契约
//    转 InternalError（客户端收到错误终态回复、进程不抛异常），WaitHandlersDone
//    必须 ok（修前 TimedOut）。
//  - T5 走真实 infra loopback（HTTP）：同一契约的 HTTP 侧对照。
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

#include <algorithm>
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
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/http/Client.hpp>
#include <bbt/infra/http/Server.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/Framework.hpp>
#include <bbt/framework/internal/RpcBridge.hpp>

namespace fw = bbt::framework;
namespace inf = bbt::infra;
namespace co = bbt::coroutine;

namespace {

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

// 一次性放行门（std cv 语义，非 sleep 凑时序）。
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

// 协程侧一次性放行门：handler 在受管协程内挂起，不阻塞调度 worker。
struct CoGate {
    void Open() {
        std::vector<co::sync::CoWaiter::SPtr> notify;
        {
            std::lock_guard<std::mutex> lk(m);
            open = true;
            notify.swap(waiters);
        }
        for (auto& w : notify)
            w->Notify();
    }
    co::WaitStatus Wait(co::Deadline deadline) {
        auto waiter = co::sync::CoWaiter::Create();
        co::WaitOptions wo;
        wo.deadline = deadline;
        const auto st = waiter->WaitWithCallback(wo, [&]() -> bool {
            std::lock_guard<std::mutex> lk(m);
            if (open)
                waiter->Notify();
            else
                waiters.push_back(waiter);
            return true;
        });
        {
            std::lock_guard<std::mutex> lk(m);
            waiters.erase(std::remove(waiters.begin(), waiters.end(), waiter),
                          waiters.end());
        }
        return st;
    }

private:
    std::mutex m;
    bool open = false;
    std::vector<co::sync::CoWaiter::SPtr> waiters;
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

// 一条 V2 Call 请求：五关联字段 + Required/Service。
inf::rpc::Request MakeRequest(std::string rid,
                              std::string service = "probe",
                              std::string method = "hold") {
    inf::rpc::Request request;
    request.SetService(std::move(service))
        .SetMethod(std::move(method))
        .SetRequestId(std::move(rid))
        .SetRequestSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetResponseSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetReply(inf::rpc::ReplyMode::Required)
        .SetTraffic(inf::rpc::TrafficClass::Service);
    return request;
}

// 测试侧第二个会话 owner（identity 与目标服务互异，避免 self-connection 被拒）。
struct CallerRuntime {
    std::shared_ptr<fw::InfraHttpHost> rt;
    std::shared_ptr<fw::RpcEgress> egress;
    std::string endpoint;

    void Start(std::string identity) {
        rt = std::make_shared<fw::InfraHttpHost>(
            Limits(), inf::ListenAddress{"127.0.0.1", 0}, std::move(identity));
        BOOST_REQUIRE(rt->Create());
        BOOST_REQUIRE(rt->Start());
        BOOST_REQUIRE(rt->rpc_client() != nullptr);
        egress = std::make_shared<fw::RpcEgress>(rt);
    }
    void Stop() {
        if (rt)
            rt->Close();
    }
};

// 控制线程发起一次真实 V2 Call 并限时等待结果。
fw::result<inf::rpc::Response> Call(const std::shared_ptr<fw::RpcEgress>& egress,
                                    const std::string& endpoint,
                                    const inf::rpc::Request& request,
                                    std::chrono::milliseconds budget = 5s) {
    struct State {
        std::optional<fw::result<inf::rpc::Response>> out;
        std::exception_ptr eptr;
        Latch done{1};
    };
    auto st = std::make_shared<State>();
    const std::string target = "tcp://" + endpoint;
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [st, egress, target, request, budget] {
            try {
                inf::CallOptions options;
                options.deadline = Clock::now() + budget;
                st->out.emplace(egress->Call(target, request, options));
            } catch (...) {
                st->eptr = std::current_exception();
            }
            st->done.Down();
        },
        succ);
    if (!succ || !st->done.WaitFor(budget + 5s))
        return fw::result<inf::rpc::Response>::err(fw::MakeError(
            fw::ErrorCode::InternalError, "Call did not complete"));
    if (st->eptr)
        std::rethrow_exception(st->eptr);
    return std::move(*st->out);
}

// T1 的跨线程状态：计数、门、闩、结果槽都由 shared_ptr 自我持有，测试线程与
// handler 闭包均按值捕获——任一方先退出都不产生悬垂访问。
struct T1State {
    std::atomic<int> calls{0};
    Latch arrived{1};    // handler 已进入包装
    CoGate first_gate;   // 首个（已过门）handler 挂此门
    Latch first_done{1}; // 调用线程已离开包装
    // 由调用线程写、测试线程在读前经 first_done（或 join）同步后读。
    std::optional<fw::result<inf::rpc::Response>> entered;
};

// T3 的最小探针：只验证 handler 未被触达。
struct CloseProbe {
    std::atomic<int> calls{0};
};

// Close 打断排空用的宿主桩：第一次/第二次 WaitHandlersDone 的分支由
// fail_first_with_timeout 选定；owner_closed 置位后 CloseReason 可观察。
struct InterruptStubHost final : fw::INetworkHost {
    std::atomic<int> wait_calls{0};
    std::atomic<int> close_calls{0};
    mutable std::atomic<int> close_reason_calls{0};
    std::atomic<bool> owner_closed{false};
    std::atomic<bool> fail_first_with_timeout{false};

    fw::result<void> Create() override { return fw::result<void>::ok(); }
    fw::result<void> Start() override { return fw::result<void>::ok(); }
    void StopAccepting() noexcept override {}
    fw::result<void> WaitHandlersDone(co::Deadline) override
    {
        ++wait_calls;
        if (fail_first_with_timeout.load() && wait_calls.load() == 1)
            return fw::result<void>::err(
                fw::MakeError(fw::ErrorCode::TimedOut, "budget"));
        owner_closed.store(true);
        return fw::result<void>::err(
            fw::MakeError(fw::ErrorCode::Unavailable, "owner abort"));
    }
    void Close() noexcept override { ++close_calls; }
    std::optional<bbt::infra::Error> CloseReason() const noexcept override
    {
        ++close_reason_calls;
        if (!owner_closed.load()) return std::nullopt;
        return fw::MakeError(fw::ErrorCode::Unavailable, "owner abort");
    }
};

// 真实 loopback 暂停 handler 的共享状态：handler 进入即报闩，放行后报完成。
struct PauseState {
    Latch entered{1};
    Latch done{1};
    CoGate gate;
    std::atomic<int> calls{0};
    std::atomic<bool> returned{false};
};

} // namespace

BOOST_AUTO_TEST_SUITE(framework_rpc_close_gate)

// T1（R1）：StopAccepting 在同一 state mutex 内先于 infra StopAccepting 闭门——
// StopAccepting 之后在同一已建立会话上到达的迟到请求被拒、不触达业务；已过门者
// 不抢占、WaitHandlersDone 不漏记；Close 幂等。
BOOST_AUTO_TEST_CASE(stop_accepting_closes_gate_rejects_late_entrant) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 6;
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
    if (!g_scheduler->IsInitialized())
        g_scheduler->Start();
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    auto st = std::make_shared<T1State>();
    auto host = std::make_shared<fw::InfraHttpHost>(
        Limits(), inf::ListenAddress{"127.0.0.1", 0}, "close-gate-server");

    host->InstallRpcHandler(
        [st](const inf::rpc::Request& request, const std::function<void()>& dispatched)
            -> fw::result<inf::rpc::Response> {
            // 契约：所有可挂起动作之前释放同步派发门。
            dispatched();
            const int n = st->calls.fetch_add(1);
            st->arrived.Down();
            if (n == 0) {
                // 只有「已过门」的首个请求挂起；迟到进入者若过门则立即返回——
                // 使负例（修法前）得到干净失败而非死等，判别力可观测。
                st->first_gate.Wait(Clock::now() + 30s);
            }
            return fw::result<inf::rpc::Response>::ok(
                inf::rpc::Response::FromRequest(request));
        });
    BOOST_REQUIRE(host->Create());
    BOOST_REQUIRE(host->Start());
    const auto endpoint = host->bound_endpoint();
    BOOST_REQUIRE(!endpoint.empty());
    BOOST_REQUIRE(host->rpc_server() != nullptr);

    CallerRuntime caller;
    caller.Start("close-gate-caller");

    bool first_registered = false;
    g_scheduler->RegistCoroutineTask(
        [st, egress = caller.egress, endpoint] {
            inf::CallOptions options;
            options.deadline = Clock::now() + 30s;
            st->entered.emplace(
                egress->Call("tcp://" + endpoint, MakeRequest("rid-1"), options));
            st->first_done.Down();
        },
        first_registered);
    BOOST_REQUIRE(first_registered);

    // 等首个进入包装并挂起（inflight==1）。失败早退由收尾段放行门兜底。
    BOOST_REQUIRE_MESSAGE(st->arrived.WaitFor(5s),
                          "entered handler never reached wrapper");

    // StopAccepting：修复后必在此闭门（先于 infra StopAccepting）。
    host->StopAccepting();

    // 迟到进入者：同一已建立会话上再发一条 CALL——infra 仍会接纳并交给框架
    // 同步派发门；修法后必被拒（host sealed），且不触达业务。
    auto late = Call(caller.egress, endpoint, MakeRequest("rid-2"));
    BOOST_CHECK_MESSAGE(!late, "late entrant must be refused after StopAccepting");
    if (!late)
        BOOST_CHECK_EQUAL(Code(late.error()),
                          Code(fw::MakeError(fw::ErrorCode::Closed, "x")));
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
            BOOST_CHECK_EQUAL(st->entered->value().RequestId(), "rid-1");
    }
    BOOST_CHECK_EQUAL(st->calls.load(), 1);

    // 排空后 WaitHandlersDone 返回 ok。
    BOOST_CHECK(host->WaitHandlersDone(Clock::now() + 2s));

    // Close 幂等：此处 StopAccepting 已闭门，故下面只证明「Close 之后仍拒绝」
    // 与幂等，不构成「不经 StopAccepting 的直接 Close」证据（由 T3 覆盖）。
    host->Close();
    host->Close();
    auto late2 = Call(caller.egress, endpoint, MakeRequest("rid-3"));
    BOOST_CHECK_MESSAGE(!late2, "entrant after Close must be refused");
    BOOST_CHECK_EQUAL(st->calls.load(), 1);

    caller.Stop();
}

// T3：独立宿主的「直接 Close」终态——不经 StopAccepting/WaitHandlersDone，
// Close 必须发布关闭原因、释放 server/client owner，且幂等。旧版用
// internal 同步派发测试缝重放「Close 后进入者」；该缝随 V1 删除且无 V2 等价物，
// 本版改为断言公共面可证实的终态：排空等待把同一原因交回（不是硬编码文案）。
BOOST_AUTO_TEST_CASE(direct_close_publishes_terminal_state) {
    auto probe = std::make_shared<CloseProbe>();
    fw::InfraHttpHost host(Limits(), inf::ListenAddress{"127.0.0.1", 0},
                           "close-gate-direct");
    host.InstallRpcHandler(
        [probe](const inf::rpc::Request& request,
                const std::function<void()>& dispatched)
            -> fw::result<inf::rpc::Response> {
            dispatched();
            probe->calls.fetch_add(1);
            return fw::result<inf::rpc::Response>::ok(
                inf::rpc::Response::FromRequest(request));
        });

    host.Close(bbt::infra::MakeError(fw::ErrorCode::Unavailable, "owner abort"));
    host.Close(); // 幂等

    auto reason = host.CloseReason();
    BOOST_REQUIRE(reason.has_value());
    BOOST_CHECK(reason->code == fw::ErrorCode::Unavailable);
    BOOST_CHECK_EQUAL(reason->message, "owner abort");

    // 关闭后进入排空路径：立即把同一关闭原因交给等待者，不进入收口。
    auto drained = host.WaitHandlersDone(Clock::now() + 1s);
    BOOST_CHECK_MESSAGE(!drained, "closed host must not report a clean drain");
    if (!drained) {
        BOOST_CHECK(drained.error().code == fw::ErrorCode::Unavailable);
        BOOST_CHECK_EQUAL(drained.error().message, "owner abort");
    }
    BOOST_CHECK(host.rpc_server() == nullptr); // owner 已释放
    BOOST_CHECK(host.rpc_client() == nullptr);
    BOOST_CHECK_EQUAL(probe->calls.load(), 0); // handler 从未被触达
}

// T2（R2）：真实 infra loopback（HTTP）——已过门 handler 不被 StopAccepting 抢占，
// 关闭序列等到它完成后 WaitHandlersDone 返回 ok，回复路径不被 StopAccepting 关断。
BOOST_AUTO_TEST_CASE(entered_handler_not_preempted_and_drained)
{
    if (!g_scheduler->IsInitialized()) {
        // Registered driver capacity: server 2 + host egress 1 + test client 1 + runnable worker 1.
        g_bbt_coroutine_config->m_cfg_static_thread_num = 5;
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
        g_scheduler->Start();
    }

    auto st = std::make_shared<PauseState>();
    auto host = std::make_shared<fw::InfraHttpHost>(
        Limits(), inf::ListenAddress{"127.0.0.1", 0}, "close-gate-http");
    host->InstallHandler(
        [st](inf::IncomingCallContext, inf::http::IncomingRequest)
            -> fw::result<inf::http::Response> {
            st->calls.fetch_add(1);
            st->entered.Down();
            st->gate.Wait(Clock::now() + 30s); // 已过门：挂起，等测试放行
            st->returned.store(true);
            return fw::result<inf::http::Response>::ok(
                inf::http::Response{200, {}, "ok"});
        });
    BOOST_REQUIRE(host->Create());
    BOOST_REQUIRE(host->Start());
    const auto endpoint = host->bound_endpoint();
    BOOST_REQUIRE(!endpoint.empty());

    auto rt = std::make_shared<fw::InfraHttpHost>(
        Limits(), inf::ListenAddress{"127.0.0.1", 0}, "close-gate-http-client");
    BOOST_REQUIRE(rt->Create());
    BOOST_REQUIRE(rt->Start());
    auto client = rt->http_client();
    BOOST_REQUIRE(client);

    struct HttpState {
        std::optional<fw::result<inf::http::Response>> out;
        Latch done{1};
    };
    auto http_state = std::make_shared<HttpState>();
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [http_state, client, endpoint] {
            inf::CallOptions opt;
            opt.deadline = Clock::now() + 5s;
            http_state->out.emplace(bbt_co_await client->Get(
                "http://" + endpoint + "/probe", {{}, {}, std::nullopt}, opt));
            http_state->done.Down();
        },
        succ);
    BOOST_REQUIRE(succ);

    BOOST_REQUIRE(st->entered.WaitFor(5s));
    BOOST_CHECK_EQUAL(st->calls.load(), 1);

    host->StopAccepting();
    BOOST_CHECK(!st->returned.load());
    auto r = host->WaitHandlersDone(Clock::now() + 150ms);
    BOOST_CHECK_MESSAGE(!r, "entered handler must keep drain pending");
    if (!r)
        BOOST_CHECK(r.error().code == fw::ErrorCode::TimedOut);
    BOOST_CHECK(!st->returned.load()); // StopAccepting 未取消在途 handler

    st->gate.Open();
    BOOST_REQUIRE(http_state->done.WaitFor(5s));
    BOOST_REQUIRE(http_state->out.has_value());
    BOOST_CHECK_MESSAGE(bool(http_state->out.value()),
                        "reply path must stay open across StopAccepting");
    BOOST_CHECK(st->returned.load());
    BOOST_CHECK(host->WaitHandlersDone(Clock::now() + 2s));

    client->Close();
    rt->Close();
    host->Close();
}

// T4：handler 抛异常的框架侧判别（真实 V2 loopback）。infra 的 _RunRpc 边界
// 把异常转 InternalError 回复；判别点是「异常栈展开也必须减在途计数」——
// 修前 inflight 永久 +1，下面的排空断言会真实失败（TimedOut），而非恒真。
BOOST_AUTO_TEST_CASE(rpc_handler_exception_releases_inflight) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 6;
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
    if (!g_scheduler->IsInitialized())
        g_scheduler->Start();
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    auto probe = std::make_shared<CloseProbe>();
    auto host = std::make_shared<fw::InfraHttpHost>(
        Limits(), inf::ListenAddress{"127.0.0.1", 0}, "close-gate-exc");
    host->InstallRpcHandler(
        [probe](const inf::rpc::Request&, const std::function<void()>& dispatched)
            -> fw::result<inf::rpc::Response> {
            dispatched();
            probe->calls.fetch_add(1);
            throw std::runtime_error("rpc handler boom");
        });
    BOOST_REQUIRE(host->Create());
    BOOST_REQUIRE(host->Start());
    const auto endpoint = host->bound_endpoint();
    BOOST_REQUIRE(!endpoint.empty());

    CallerRuntime caller;
    caller.Start("close-gate-exc-caller");

    auto r = Call(caller.egress, endpoint, MakeRequest("rid-exc"));
    BOOST_REQUIRE_MESSAGE(!r, "handler exception must surface as an error reply");
    BOOST_CHECK(r.error().code == fw::ErrorCode::InternalError);
    BOOST_CHECK_EQUAL(probe->calls.load(), 1); // 异常发生在 handler 内

    // 记账未被破坏：换一个正常 handler 再进一次仍能记账（双重减一会把
    // size_t 减成极大值，后续排空断言随即失败）。
    host->InstallRpcHandler(
        [](const inf::rpc::Request& request, const std::function<void()>& dispatched)
            -> fw::result<inf::rpc::Response> {
            dispatched();
            return fw::result<inf::rpc::Response>::ok(
                inf::rpc::Response::FromRequest(request));
        });
    auto ok_call = Call(caller.egress, endpoint, MakeRequest("rid-ok"));
    BOOST_REQUIRE(ok_call);
    BOOST_CHECK_EQUAL(ok_call.value().RequestId(), "rid-ok");

    // 判别点：异常栈展开已减一 → 排空成立。修前 inflight 停在 1 → TimedOut。
    auto drained = host->WaitHandlersDone(Clock::now() + 2s);
    BOOST_CHECK_MESSAGE(
        drained,
        "in-flight count must be released when the handler unwinds by exception");
    if (!drained)
        BOOST_CHECK(drained.error().code == fw::ErrorCode::TimedOut);

    caller.Stop();
    host->Close();
}

// T5：handler 抛异常的框架侧判别（真实 infra loopback，HTTP）。
// infra 契约（`http/Server.hpp`：异常由边界捕获转 InternalError）把 handler
// 异常在协程边界内吞掉 → 客户端收到 500 终态回复、进程不抛异常。但框架自己的
// 在途记账仍必须归零——修前 inflight 永久 +1，下面的排空断言会真实失败（TimedOut）。
BOOST_AUTO_TEST_CASE(loopback_http_handler_exception_drains_inflight) {
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 5;
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
        g_scheduler->Start();
    }

    auto st = std::make_shared<PauseState>();
    auto host = std::make_shared<fw::InfraHttpHost>(
        Limits(), inf::ListenAddress{"127.0.0.1", 0}, "close-gate-http-exc");
    host->InstallHandler(
        [st](inf::IncomingCallContext, inf::http::IncomingRequest)
            -> fw::result<inf::http::Response> {
            st->calls.fetch_add(1);
            st->entered.Down();
            throw std::runtime_error("http handler boom"); // 越过 _InvokeHttp 调用点
        });
    BOOST_REQUIRE(host->Create());
    BOOST_REQUIRE(host->Start());
    const auto endpoint = host->bound_endpoint();
    BOOST_REQUIRE(!endpoint.empty());

    auto rt = std::make_shared<fw::InfraHttpHost>(
        Limits(), inf::ListenAddress{"127.0.0.1", 0}, "close-gate-http-exc-client");
    BOOST_REQUIRE(rt->Create());
    BOOST_REQUIRE(rt->Start());
    auto client = rt->http_client();
    BOOST_REQUIRE(client);

    struct HttpState {
        std::optional<fw::result<inf::http::Response>> out;
        Latch done{1};
    };
    auto http_state = std::make_shared<HttpState>();
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [http_state, client, endpoint] {
            inf::CallOptions opt;
            opt.deadline = Clock::now() + 5s;
            http_state->out.emplace(bbt_co_await client->Get(
                "http://" + endpoint + "/probe", {{}, {}, std::nullopt}, opt));
            http_state->done.Down();
        },
        succ);
    BOOST_REQUIRE(succ);

    BOOST_REQUIRE(st->entered.WaitFor(5s)); // handler 已进入（抛异常前）
    BOOST_CHECK_EQUAL(st->calls.load(), 1);
    // 客户端拿到终态回复：infra 边界把异常转 InternalError → 500。
    BOOST_REQUIRE(http_state->done.WaitFor(5s));
    BOOST_REQUIRE(http_state->out.has_value());
    BOOST_REQUIRE_MESSAGE(
        bool(http_state->out.value()),
        "handler exception must yield a terminal reply, not an aborted connection");
    BOOST_CHECK_EQUAL(
        static_cast<int>(http_state->out.value().value().status_code), 500);

    // 判别点：异常栈展开已减一 → 排空成立。修前 inflight 停在 1 → TimedOut。
    auto drained = host->WaitHandlersDone(Clock::now() + 2s);
    BOOST_CHECK_MESSAGE(
        drained,
        "framework in-flight count must be released on handler exception");

    client->Close();
    rt->Close();
    host->Close();
}

// T6（关闭收口责任移交回归）：迟到 handler/callback 排空收口责任由
// WaitHandlersDone 承担（业务 inflight 归零 → CoRpcServer::Shutdown 同步封口），
// 不再由 Close 的 5s 兜底掩盖。真实判别链（全部经同一真实 V2 loopback，无 mock）：
//   1) 已 Start 宿主上发起真实 CALL，handler 挂起（inflight>0）；
//   2) StopAccepting → WaitHandlersDone(有界期限) 必须返回 TimedOut——此时
//      尚未封口，CoRpcServer 不得已 IsClosed；
//   3) 放行 handler → WaitHandlersDone(Deadline::max) 续等必须成功完成
//      （inflight 归零 → CoRpcServer::Shutdown 同步封口）；
//   4) IsClosed() 为 true 证明封口已执行；随后宿主 Close() 释放
//      owner（rpc_server() 置空）且 close_error() 为空。
BOOST_AUTO_TEST_CASE(late_handler_waitstopped_failure_then_recover)
{
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 6;
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
        g_scheduler->Start();
    }
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    struct Shared {
        std::atomic<int> calls{0};
        CoGate handler_gate;
        Latch entered{1};
        std::optional<fw::result<inf::rpc::Response>> out;
        Latch done{1};
    };
    auto st = std::make_shared<Shared>();

    auto host = std::make_shared<fw::InfraHttpHost>(
        Limits(), inf::ListenAddress{"127.0.0.1", 0}, "close-gate-late");
    host->InstallRpcHandler(
        [st](const inf::rpc::Request& request,
             const std::function<void()>& dispatched)
            -> fw::result<inf::rpc::Response> {
            dispatched();
            st->calls.fetch_add(1);
            st->entered.Down();
            st->handler_gate.Wait(Clock::now() + 30s);
            return fw::result<inf::rpc::Response>::ok(
                inf::rpc::Response::FromRequest(request));
        });
    BOOST_REQUIRE(host->Create());
    BOOST_REQUIRE(host->Start());
    BOOST_REQUIRE(host->bound_address().has_value());
    BOOST_REQUIRE(host->rpc_server() != nullptr);

    CallerRuntime caller;
    caller.Start("close-gate-late-caller");
    const auto endpoint = host->bound_endpoint();

    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [st, egress = caller.egress, endpoint] {
            inf::CallOptions options;
            options.deadline = Clock::now() + 30s;
            st->out.emplace(
                egress->Call("tcp://" + endpoint, MakeRequest("t6-1"), options));
            st->done.Down();
        },
        succ);
    BOOST_REQUIRE(succ);

    // handler 已过门并挂起（framework inflight==1 + infra inflight==1）。
    BOOST_REQUIRE(st->entered.WaitFor(5s));
    BOOST_CHECK_EQUAL(st->calls.load(), 1);

    host->StopAccepting();

    // 有界期限：inflight>0 → TimedOut；此时 server 不得已被 Close/Stopped。
    auto r1 = host->WaitHandlersDone(Clock::now() + 150ms);
    BOOST_CHECK_MESSAGE(!r1, "drain must fail while handler in-flight");
    if (!r1)
        BOOST_CHECK(r1.error().code == fw::ErrorCode::TimedOut);
    BOOST_REQUIRE(host->rpc_server() != nullptr);
    BOOST_CHECK(!host->rpc_server()->IsClosed()); // 未提前逻辑封口

    // 放行 → Deadline::max 续等：inflight 归零 → Shutdown 封口。
    st->handler_gate.Open();
    BOOST_REQUIRE(st->done.WaitFor(5s));
    auto r2 = host->WaitHandlersDone(co::Deadline::max());
    BOOST_CHECK_MESSAGE(
        r2, "Deadline::max retry must complete full server drain+stop chain");

    // 收口凭据：CoRpcServer 已逻辑关闭（IsClosed）。
    BOOST_REQUIRE(host->rpc_server() != nullptr);
    BOOST_CHECK(host->rpc_server()->IsClosed());

    // Close：取得收口凭据后停 Engine（Run 返回 + Phase==Stopped 实际检查）
    // 并释放 owner——无 close_error 证明 Engine 未走「未达 Stopped 保留」路径。
    host->Close();
    BOOST_CHECK(!host->close_error().has_value());
    BOOST_CHECK(host->rpc_server() == nullptr); // owner 已释放

    caller.Stop();
}

// T7（直接 Close 兜底路径）：不经 StopAccepting/WaitHandlersDone 的裸
// Close 也必须安全收口——同步 Shutdown 后正常释放（原同步关闭契约），
// 不是把「未走 WaitHandlersDone」当永久失败保留 owner。
BOOST_AUTO_TEST_CASE(direct_close_on_started_host_releases_owners)
{
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 5;
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
        g_scheduler->Start();
    }
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    auto probe = std::make_shared<CloseProbe>();
    auto host = std::make_shared<fw::InfraHttpHost>(
        Limits(), inf::ListenAddress{"127.0.0.1", 0}, "close-gate-direct-started");
    host->InstallRpcHandler(
        [probe](const inf::rpc::Request& request,
                const std::function<void()>& dispatched)
            -> fw::result<inf::rpc::Response> {
            dispatched();
            probe->calls.fetch_add(1);
            return fw::result<inf::rpc::Response>::ok(
                inf::rpc::Response::FromRequest(request));
        });
    BOOST_REQUIRE(host->Create());
    BOOST_REQUIRE(host->Start());
    BOOST_REQUIRE(host->bound_address().has_value());

    // 直接 Close：同步 Shutdown 必成功（无在途 handler/callback），
    // owner 正常释放，close_error 为空。
    host->Close();
    BOOST_CHECK(!host->close_error().has_value());
    BOOST_CHECK(host->rpc_server() == nullptr);
    // 幂等重入安全（不重复 join/get）。
    host->Close();
}

// T8：Close 立即返回（不等暂停的 handler），析构不因 joinable 线程 terminate；
// 关闭原因经公共读口发布，handler 放行后安全返回（HandlerState 自持）。
BOOST_AUTO_TEST_CASE(close_with_paused_handler_returns_without_waiting)
{
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 6;
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
        g_scheduler->Start();
    }
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    auto st = std::make_shared<PauseState>();
    auto host = std::make_shared<fw::InfraHttpHost>(
        Limits(), inf::ListenAddress{"127.0.0.1", 0}, "close-gate-paused");
    host->InstallRpcHandler(
        [st](const inf::rpc::Request& request,
             const std::function<void()>& dispatched)
            -> fw::result<inf::rpc::Response> {
            dispatched();
            st->calls.fetch_add(1);
            st->entered.Down();
            st->gate.Wait(Clock::now() + 30s);
            st->returned.store(true);
            st->done.Down();
            return fw::result<inf::rpc::Response>::ok(
                inf::rpc::Response::FromRequest(request));
        });
    BOOST_REQUIRE(host->Create());
    BOOST_REQUIRE(host->Start());
    BOOST_REQUIRE(host->bound_address().has_value());

    CallerRuntime caller;
    caller.Start("close-gate-paused-caller");
    const auto endpoint = host->bound_endpoint();

    bool registered = false;
    g_scheduler->RegistCoroutineTask(
        [egress = caller.egress, endpoint] {
            inf::CallOptions options;
            options.deadline = Clock::now() + 5s;
            auto ignored =
                egress->Call("tcp://" + endpoint, MakeRequest("t8-1"), options);
            (void)ignored;
        },
        registered);
    BOOST_REQUIRE(registered);
    BOOST_REQUIRE(st->entered.WaitFor(5s));
    BOOST_CHECK_EQUAL(st->calls.load(), 1);

    const auto begin = Clock::now();
    host->Close(fw::MakeError(fw::ErrorCode::Unavailable, "owner abort"));
    const auto close_cost = Clock::now() - begin;
    // handler 仍暂停：Close 的返回不得依赖它完成。
    BOOST_CHECK(!st->returned.load());
    BOOST_CHECK_MESSAGE(close_cost < 2s,
                        "Close must not wait for paused handler");
    auto reason = host->CloseReason();
    BOOST_REQUIRE(reason.has_value());
    BOOST_CHECK(reason->code == fw::ErrorCode::Unavailable);
    BOOST_CHECK_EQUAL(reason->message, "owner abort");

    // 迟到进入者路径已被 Close 封口：排空等待立即拿到同一原因（不是硬编码文案）。
    auto late = host->WaitHandlersDone(Clock::now() + 1s);
    BOOST_CHECK(!late);
    if (!late) {
        BOOST_CHECK(late.error().code == fw::ErrorCode::Unavailable);
        BOOST_CHECK_EQUAL(late.error().message, "owner abort");
    }

    // 析构安全：业务未完成时释放宿主，析构必须 join 而不是 terminate/detach；
    // handler 由 HandlerState 保活，放行后安全返回（不访问已析构宿主）。
    std::atomic<bool> destroy_started{false};
    std::thread destroy([&host, &destroy_started] {
        destroy_started.store(true);
        host.reset();
    });
    while (!destroy_started.load()) {
        std::this_thread::yield();
    }
    st->gate.Open();
    destroy.join();
    BOOST_CHECK(st->done.WaitFor(5s));
    BOOST_CHECK(st->returned.load());

    caller.Stop();
}

// T9：HostLifecycle 用明确状态（CloseReason）识别「排空被 Close 打断」，
// 不再 park：跳过 on_handlers_drained（handler 可能仍借业务对象），走
// on_release，返回 kExitShutdownFailed。
BOOST_AUTO_TEST_CASE(lifecycle_close_interrupts_drain_without_parking)
{
    auto host = std::make_shared<InterruptStubHost>();
    fw::HostLifecycle lifecycle(host, 100ms);
    lifecycle.RequestShutdown();
    int drained = 0;
    int released = 0;
    fw::HostLifecycle::Hooks hooks;
    hooks.on_handlers_drained = [&drained] { ++drained; };
    hooks.on_release = [&released] { ++released; };

    int rc = -1;
    std::thread runner([&] { rc = lifecycle.Run(hooks); });
    runner.join();

    BOOST_CHECK_EQUAL(rc, fw::HostLifecycle::kExitShutdownFailed);
    BOOST_CHECK_EQUAL(drained, 0);
    BOOST_CHECK_EQUAL(released, 1);
    BOOST_CHECK_EQUAL(host->close_calls.load(), 1);
    const auto failures = lifecycle.Failures();
    bool interrupted = false;
    for (const auto& failure : failures)
        if (failure.find("interrupted by host close") != std::string::npos)
            interrupted = true;
    BOOST_CHECK_MESSAGE(interrupted,
                        "failure list must record the close interrupt");
}

// T10：第一次排空超时、无界续等期间被 Close 打断——迟到收尾按 kExitShutdownLate
// 报告，而不是因失败项改报 Failed，也不是 park。
BOOST_AUTO_TEST_CASE(lifecycle_close_during_unbounded_retry_reports_late)
{
    auto host = std::make_shared<InterruptStubHost>();
    host->fail_first_with_timeout.store(true);
    fw::HostLifecycle lifecycle(host, 50ms);
    lifecycle.RequestShutdown();
    int drained = 0;
    int released = 0;
    fw::HostLifecycle::Hooks hooks;
    hooks.on_handlers_drained = [&drained] { ++drained; };
    hooks.on_release = [&released] { ++released; };

    int rc = -1;
    std::thread runner([&] { rc = lifecycle.Run(hooks); });
    runner.join();

    BOOST_CHECK_EQUAL(rc, fw::HostLifecycle::kExitShutdownLate);
    BOOST_CHECK_EQUAL(drained, 0);
    BOOST_CHECK_EQUAL(released, 1);
    BOOST_CHECK(host->wait_calls.load() >= 2);
    BOOST_CHECK(lifecycle.IncompleteSteps().empty());
}

BOOST_AUTO_TEST_SUITE_END()
