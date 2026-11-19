/**
 * RequestContext/RequestScope 受管上下文与出站期限语义测试。
 * 期限算术（缺省继承、显式取最小、无有限预算拒绝、已过期拒绝）经纯函数
 * ResolveCallDeadline 以注入时钟值确定性断言；序号绑定、I/O 发起与错误
 * 先后经真实 Service 出站入口（CoServiceBase::call → _CallSend）在 CoApp
 * 测试缝上断言：缝发送计数与票据观测点（IsTicketSent / 下一次有效绑定
 * 可否成功）证明绑定水位。缺发送入口按层区分：宿主传输空（缝 rpc_send
 * 为空）失败在票据绑定之后；服务级发送入口未接线（Running App 热安装的
 * OnStart 窗口，早于 _BindEgressToService）失败在票据绑定之前。业务取消
 * 由流程按 CoXxx 返回值决定，框架不提供
 * 单操作业务取消；协程等待结果按 coroutine 契约处理。
 * 全部确定性可测：不用 sleep 凑时序、无空断言。
 */
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/OrderedSession.hpp>
#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/internal/CallOptionsAdapter.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/NetworkHost.hpp>
#include <bbt/framework/internal/RequestScope.hpp>

namespace fw = bbt::framework;
namespace co = bbt::coroutine;
using Clock = std::chrono::steady_clock;

/**
 * 用例 9 的编译期部分：framework::CallOptions 与 infra::CallOptions
 * 是分层类型，不可直接互换（分层类型契约）。
 */
static_assert(!std::is_same<fw::CallOptions, bbt::infra::CallOptions>::value,
              "framework::CallOptions and infra::CallOptions must be distinct types");
static_assert(
    !std::is_convertible<fw::CallOptions, bbt::infra::CallOptions>::value &&
        !std::is_convertible<bbt::infra::CallOptions, fw::CallOptions>::value,
    "framework::CallOptions and infra::CallOptions must not be convertible");

/**
 * 取消令牌已从 framework/infra 的 CallOptions 与 RequestContext 移除：
 * 编译期断言无 cancel 字段，防止回退引入单操作业务取消。
 */
template <class T, class = void>
struct HasCancelField : std::false_type {};
template <class T>
struct HasCancelField<T, std::void_t<decltype(std::declval<T&>().cancel)>>
    : std::true_type {};
static_assert(!HasCancelField<fw::CallOptions>::value,
              "framework::CallOptions must not carry a cancel token");
static_assert(!HasCancelField<bbt::infra::CallOptions>::value,
              "infra::CallOptions must not carry a cancel token");
static_assert(!HasCancelField<fw::RequestContext>::value,
              "RequestContext must not carry a cancel token");

namespace {

std::shared_ptr<const fw::RequestContext> MakeCtx(
    const std::string& request_id,
    co::Deadline deadline) {
    auto ctx = std::make_shared<fw::RequestContext>();
    ctx->request_id = request_id;
    ctx->deadline = deadline;
    return ctx;
}

/**
 * 不接纳请求也不启动的宿主桩：出站断言走缝发送，不需要真实 I/O。
 */
class StubNetHost final : public fw::INetworkHost {
public:
    fw::result<void> Create() override { return fw::result<void>::ok(); }
    fw::result<void> Start() override { return fw::result<void>::ok(); }
    void StopAccepting() noexcept override {}
    fw::result<void> WaitHandlersDone(co::Deadline) override {
        return fw::result<void>::ok();
    }
    void Close() noexcept override {}
};

/**
 * 缝发送的如实回信：回显 envelope 全部受校验字段，载荷为哑字节。
 * _CallSend 的回复封包校验通过即证明信封一致性。
 */
bbt::infra::rpc::Response ReplyFor(const bbt::infra::rpc::Request& env) {
    bbt::infra::rpc::Response reply;
    reply.SetService(env.Service())
        .SetMethod(env.Method())
        .SetRequestId(env.RequestId())
        .SetRequestSchema(env.RequestSchema())
        .SetResponseSchema(env.ResponseSchema())
        .SetPayload({'o', 'k'});
    return reply;
}

/**
 * 缝发送记录仪：发送次数、下行期限与 fw.sequence 元数据，是旧注入点
 * io/seq 断言在真实出站入口上的等价观察点。记录与读取都沿同一受管协程
 * 顺序发生（bbt_co_await 沿当前协程同步执行），无需加锁。
 */
struct SendProbe {
    int sends = 0;
    bool reply_ok = true;
    co::Deadline last_deadline{};
    std::string last_sequence;

    fw::RpcSendAppFn Fn() {
        return [this](std::string_view,
                      const bbt::infra::rpc::Request& env,
                      const bbt::infra::CallOptions& o)
                   -> bbt_co_async(fw::result<bbt::infra::rpc::Response>) {
            ++sends;
            last_deadline = o.deadline;
            for (const auto& [k, v] : env.Metadata())
                if (k == "fw.sequence")
                    last_sequence = v;
            if (!reply_ok)
                return fw::result<bbt::infra::rpc::Response>::err(fw::MakeError(
                    fw::ErrorCode::TransportError, "probe seam send failed"));
            return fw::result<bbt::infra::rpc::Response>::ok(ReplyFor(env));
        };
    }
};

constexpr std::string_view kEchoTarget = "svc.echo";

/**
 * OnStart 热安装观察记录：Running App 上 CoInstallService 在
 * _InstallServiceImpl 内先执行 OnStart，之后才绑服务级出站注入点。
 * armed 时 OnStart 经真实出站入口发起一次带有效上下文与有序票据的调用，
 * 结果记入本结构——预期失败如实记录、不上抛，不作为启动失败。
 * 记录只在 OnStart 协程写、安装返回后宿主侧读（安装等待 hook 完成，顺序
 * 经 await 成立），与 SendProbe 同口径不加锁。
 */
struct OnStartObservation {
    bool armed = false;
    SendProbe* seam = nullptr;
    fw::OrderedSession* session = nullptr;
    std::optional<fw::OrderedStamp> stamp;
    fw::CoRpcReq request;

    bool observed = false;
    bool call_ok = false;
    fw::ErrorCode error_code{};
    bool ticket_sent = false;
    int sends = 0;
};

/**
 * 出站探针服务：只承载受管出站入口，CallTo 把参数原样交给
 * CoServiceBase::call（受管校验/envelope/票据绑定/期限校验/发送/回复校验
 * 全部在框架机器面）。不声明 kRpcMethods（空表）——本服务无入站方法。
 */
class EgressProbeSvc final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "egress-probe";
    explicit EgressProbeSvc(fw::detail::ConstructKey k)
        : fw::CoService<>(k), obs_(s_pending_onstart) {}

    /**
     * 观察挂载：实例由 Registry 在安装内构造，构造参数只有 ConstructKey，
     * 用安装前挂载、安装后取下的槽位传入（同一时刻至多一个用例安装探针）。
     */
    static void ArmOnStartObservation(OnStartObservation* obs) {
        s_pending_onstart = obs;
    }

    fw::result<fw::CoRpcResp> CallTo(std::string_view service,
                                     const fw::CoRpcReq& request,
                                     const fw::CallOptions& options) {
        return bbt_co_await call(service, "ping", request,
                                 fw::RouteFields{}, options);
    }

protected:
    ~EgressProbeSvc() noexcept override = default;

    /**
     * armed 时执行一次真实出站观察：服务已构造、出站注入点尚未绑定
     * （_BindEgressToService 在 _InstallServiceImpl 返回后才执行），
     * 调用命中 _CallSend 首道缺入口检查。错误码、票据标记与缝发送计数
     * 如实记录后返回 ok；未 armed 时与基类同为 no-op。
     */
    fw::result<void> OnStart() override {
        if (!obs_ || !obs_->armed)
            return fw::result<void>::ok();
        fw::CallOptions opt;
        opt.ordered = obs_->stamp;
        auto out = [&] {
            fw::RequestScope scope(MakeCtx("prewire-onstart",
                                           Clock::now() + std::chrono::seconds{30}));
            return CallTo(kEchoTarget, obs_->request, opt);
        }();
        obs_->observed = true;
        obs_->call_ok = static_cast<bool>(out);
        if (!out)
            obs_->error_code = out.error().code;
        obs_->ticket_sent = obs_->session->IsTicketSent(*obs_->stamp);
        obs_->sends = obs_->seam->sends;
        return fw::result<void>::ok();
    }

private:
    friend class fw::detail::Registry;
    static inline OnStartObservation* s_pending_onstart = nullptr;
    OnStartObservation* obs_ = nullptr;
};

/**
 * 取已安装服务的受管实例（锁内取 owner+指针，保活纪律与宿主一致）。
 */
template <class S>
S* ManagedService(fw::CoApp& app, std::shared_ptr<void>& owner) {
    const auto reg = fw::RegistryForTest(app);
    std::lock_guard<std::mutex> lk(reg->Mutex());
    auto* e = reg->FindByTypeNameLocked(
        fw::ObjectKind::Service, S::kServiceName);
    if (!e)
        return nullptr;
    owner = e->object;
    return static_cast<S*>(e->object.get());
}

/**
 * 建装好探针服务的缝 App：路由在构造期落进运行时状态，出站链不依赖
 * CoStart；缝发送为空即「无发送入口」形态。
 */
struct ProbeApp {
    std::shared_ptr<fw::CoApp> app;
    EgressProbeSvc* svc = nullptr;
    std::shared_ptr<void> owner;
};

ProbeApp InstallProbeApp(fw::RpcSendAppFn send) {
    ProbeApp out;
    fw::AppOptions opts;
    opts.SetLimits(64, 64, 64, 64)
        .AddRoute(std::string{kEchoTarget}, "127.0.0.1:9");
    out.app = fw::MakeCoAppForTest(
        std::move(opts),
        fw::CoAppSeam{std::make_shared<StubNetHost>(), std::move(send)});
    if (!out.app)
        return out;
    auto installed = bbt_co_await out.app->CoInstallService<EgressProbeSvc>(
        fw::ServiceOptions{}.SetMaxInflight(4));
    if (!installed)
        return out;
    out.svc = ManagedService<EgressProbeSvc>(*out.app, out.owner);
    return out;
}

class TestLatch {
public:
    explicit TestLatch(int n) : m_count(n) {}
    void CountDown() {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (--m_count > 0)
                return;
        }
        m_cv.notify_all();
    }
    void Wait() {
        std::unique_lock<std::mutex> lk(m_mtx);
        m_cv.wait(lk, [this] { return m_count == 0; });
    }

private:
    std::mutex m_mtx;
    std::condition_variable m_cv;
    int m_count;
};

/**
 * 协程用例需要真实调度器；2 个静态 worker 足够（用例内不并发压测）。
 * runtime 是进程寿命单例：只 Start 一次、无 Stop；已初始化则复用。
 * CoApp 装配与出站机器面在协程内执行，按同仓 CoApp 用例口径在 Start()
 * 前放大栈到 64 KiB。
 */
struct SchedulerFixture {
    SchedulerFixture() {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
        if (!g_scheduler->IsInitialized())
            g_scheduler->Start();
    }
};

/**
 * 在受管协程内执行 fn（bbt_co_await 消费点要求协程上下文）：经
 * g_scheduler 投递协程任务，宿主线程用限时 std::cv 等其完成。
 */
template <class F>
bool RunInManagedCoroutine(F&& fn,
                           std::chrono::milliseconds budget = std::chrono::milliseconds{5000}) {
    if (bbt::coroutine::IsInCoroutineContext()) {
        fn();
        return true;
    }
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
        g_scheduler->Start();
    }
    struct Slot {
        std::mutex mtx;
        std::condition_variable cv;
        bool done = false;
        std::exception_ptr error;
    };
    auto slot = std::make_shared<Slot>();
    auto f = std::forward<F>(fn);
    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [slot, f = std::move(f)]() mutable {
            try {
                f();
            } catch (...) { slot->error = std::current_exception(); }
            std::lock_guard<std::mutex> lk(slot->mtx);
            slot->done = true;
            slot->cv.notify_all();
        },
        submitted);
    if (!submitted)
        throw std::runtime_error("test coroutine submission rejected");
    std::unique_lock<std::mutex> lk(slot->mtx);
    const bool done = slot->cv.wait_for(lk, budget, [&] { return slot->done; });
    if (!done)
        std::terminate();
    if (slot->error)
        std::rethrow_exception(slot->error);
    return done;
}

#define CO_TEST_CASE(name)                                 \
    static void name##_body();                             \
    BOOST_AUTO_TEST_CASE(name) {                           \
        BOOST_REQUIRE(RunInManagedCoroutine(name##_body)); \
    }                                                      \
    static void name##_body()

fw::OrderedGrant MakeGrant(const std::string& tag) {
    fw::OrderedGrant g;
    g.service = "svc." + tag;
    g.actor_key = "actor." + tag;
    g.producer_id = "producer." + tag;
    g.producer_epoch = "pepoch." + tag;
    g.receiver_epoch = "repoch." + tag;
    g.peer_principal = "peer." + tag;
    return g;
}

} // namespace

BOOST_TEST_GLOBAL_FIXTURE(SchedulerFixture);

BOOST_AUTO_TEST_SUITE(framework_f1a)

/**
 * 用例 1：缺省 deadline 继承父请求剩余预算；父预算变小后取到的值随之变小。
 * 纯函数侧断言继承算术，真实入口侧断言继承值随发送下行到缝发送点。
 */
CO_TEST_CASE(default_deadline_inherits_parent_remaining_budget) {
    const auto now = Clock::now();
    const auto d_far = now + std::chrono::seconds{60};
    const auto d_near = now + std::chrono::seconds{5};
    auto parent_far = MakeCtx("r-far", d_far);
    auto parent_near = MakeCtx("r-near", d_near);

    /**
     * deadline 缺省 → 继承父预算
     */
    auto r1 = fw::ResolveCallDeadline(parent_far.get(), std::nullopt, now);
    BOOST_REQUIRE(r1);
    BOOST_CHECK(r1.value() == d_far);

    auto r2 = fw::ResolveCallDeadline(parent_near.get(), std::nullopt, now);
    BOOST_REQUIRE(r2);
    BOOST_CHECK(r2.value() == d_near);

    /**
     * 真实入口：缺省期限调用成功，继承后的期限随发送下行。
     */
    auto probe = std::make_shared<SendProbe>();
    auto pa = InstallProbeApp(probe->Fn());
    BOOST_REQUIRE(pa.svc != nullptr);

    auto req_r = fw::CoRpcReq::From(std::string{"probe"});
    BOOST_REQUIRE(req_r);
    auto out = [&] {
        fw::RequestScope scope(MakeCtx("r-near", d_near));
        return pa.svc->CallTo(kEchoTarget, req_r.value(), fw::CallOptions{});
    }();
    BOOST_REQUIRE(out);
    BOOST_CHECK(probe->sends == 1);
    BOOST_CHECK(probe->last_deadline == d_near);
}

/**
 * 用例 2：显式期限不得延长父预算（取最小值）；短于父预算则取显式值。
 */
CO_TEST_CASE(explicit_deadline_cannot_extend_parent_budget) {
    const auto now = Clock::now();
    const auto parent_d = now + std::chrono::seconds{10};
    auto parent = MakeCtx("p", parent_d);

    auto r1 = fw::ResolveCallDeadline(parent.get(),
                                      parent_d + std::chrono::seconds{50}, now);
    BOOST_REQUIRE(r1);
    BOOST_CHECK(r1.value() == parent_d);

    auto r2 = fw::ResolveCallDeadline(parent.get(),
                                      parent_d - std::chrono::seconds{5}, now);
    BOOST_REQUIRE(r2);
    BOOST_CHECK(r2.value() == parent_d - std::chrono::seconds{5});
}

/**
 * 用例 3：无有限期限 → 配置失败（明确错误，不是默认无限），票据未绑定、
 * 缝发送零次（错误先于票据绑定与 I/O）。
 */
CO_TEST_CASE(top_level_requires_finite_deadline) {
    const auto now = Clock::now();

    /**
     * 顶层入口无期限
     */
    auto r1 = fw::ResolveCallDeadline(nullptr, std::nullopt, now);
    BOOST_REQUIRE(!r1);
    BOOST_CHECK(r1.error().code == fw::ErrorCode::InvalidArgument);

    /**
     * 顶层显式无限同样拒绝
     */
    auto r2 = fw::ResolveCallDeadline(nullptr, co::Deadline::max(), now);
    BOOST_REQUIRE(!r2);
    BOOST_CHECK(r2.error().code == fw::ErrorCode::InvalidArgument);

    /**
     * 真实入口：上下文无有限预算 → InvalidArgument；有序票据先于期限
     * 校验就位，错误后票据仍未绑定、发送未发生。
     */
    auto probe = std::make_shared<SendProbe>();
    auto pa = InstallProbeApp(probe->Fn());
    BOOST_REQUIRE(pa.svc != nullptr);
    auto session_r = fw::OrderedSession::Open(MakeGrant("f1a3"));
    BOOST_REQUIRE(session_r);
    auto session = session_r.value();
    auto stamp_r = session->prepare();
    BOOST_REQUIRE(stamp_r);

    auto req_r = fw::CoRpcReq::From(std::string{"probe"});
    BOOST_REQUIRE(req_r);
    fw::CallOptions opt;
    opt.ordered = stamp_r.value();
    auto out = [&] {
        fw::RequestScope scope(MakeCtx("r-unbudgeted", co::Deadline::max()));
        return pa.svc->CallTo(kEchoTarget, req_r.value(), opt);
    }();
    BOOST_REQUIRE(!out);
    BOOST_CHECK(out.error().code == fw::ErrorCode::InvalidArgument);
    BOOST_CHECK(probe->sends == 0);
    BOOST_CHECK(!session->IsTicketSent(stamp_r.value()));
}

/**
 * 用例 4：已过期 → TimedOut，先于票据绑定与任何 I/O；水位不动（同一
 * 序号的下一次有效绑定可成功）。
 */
CO_TEST_CASE(expired_returns_timedout_before_any_io) {
    const auto now = Clock::now();

    auto parent = MakeCtx("p", now - std::chrono::milliseconds{1});
    auto r1 = fw::ResolveCallDeadline(parent.get(), std::nullopt, now);
    BOOST_REQUIRE(!r1);
    BOOST_CHECK(r1.error().code == fw::ErrorCode::TimedOut);

    /**
     * 顶层显式期限已过期同样 TimedOut。
     */
    auto r2 = fw::ResolveCallDeadline(nullptr, now - std::chrono::milliseconds{1}, now);
    BOOST_REQUIRE(!r2);
    BOOST_CHECK(r2.error().code == fw::ErrorCode::TimedOut);

    /**
     * 真实入口：过期 → TimedOut，缝发送零次、票据未绑定；随后同一票据
     * 的有效调用成功——证明水位未因过期调用推进。
     */
    auto probe = std::make_shared<SendProbe>();
    auto pa = InstallProbeApp(probe->Fn());
    BOOST_REQUIRE(pa.svc != nullptr);
    auto session_r = fw::OrderedSession::Open(MakeGrant("f1a4"));
    BOOST_REQUIRE(session_r);
    auto session = session_r.value();
    auto stamp_r = session->prepare();
    BOOST_REQUIRE(stamp_r);

    auto req_r = fw::CoRpcReq::From(std::string{"probe"});
    BOOST_REQUIRE(req_r);
    fw::CallOptions opt;
    opt.ordered = stamp_r.value();

    auto expired_out = [&] {
        fw::RequestScope scope(
            MakeCtx("r-late", now - std::chrono::milliseconds{1}));
        return pa.svc->CallTo(kEchoTarget, req_r.value(), opt);
    }();
    BOOST_REQUIRE(!expired_out);
    BOOST_CHECK(expired_out.error().code == fw::ErrorCode::TimedOut);
    BOOST_CHECK(probe->sends == 0);
    BOOST_CHECK(!session->IsTicketSent(stamp_r.value()));

    /**
     * 下一次有效绑定：同一序号按序绑定并发送成功（水位未动）。
     */
    auto ok_out = [&] {
        fw::RequestScope scope(
            MakeCtx("r-fresh", Clock::now() + std::chrono::seconds{30}));
        return pa.svc->CallTo(kEchoTarget, req_r.value(), opt);
    }();
    BOOST_REQUIRE(ok_out);
    BOOST_CHECK(probe->sends == 1);
    BOOST_CHECK(session->IsTicketSent(stamp_r.value()));
}

/**
 * 取消相关的旧用例已删除：framework 不经出站装配表达业务级取消；等待中的
 * 协程级取消由 coroutine 自身契约处理，业务级取消由 CoXxx 返回值决定。
 */

/**
 * 用例 7：非受管执行 → InvalidContext；受管作用域登记后可取出，
 * 嵌套内层遮蔽外层、退出后恢复。
 */
BOOST_AUTO_TEST_CASE(unmanaged_context_returns_invalid_context) {
    auto r0 = fw::CurrentRequestContext();
    BOOST_REQUIRE(!r0);
    BOOST_CHECK(r0.error().code == fw::ErrorCode::InvalidContext);

    /**
     * 非受管协程同样 InvalidContext。
     */
    TestLatch latch(1);
    /**
     * 0=未运行 1=InvalidContext 2=其它
     */
    std::atomic<int> seen{0};
    bbtco_ref {
        auto r = fw::CurrentRequestContext();
        seen.store(!r && r.error().code == fw::ErrorCode::InvalidContext
                       ? 1
                       : 2);
        latch.CountDown();
    };
    latch.Wait();
    BOOST_CHECK(seen.load() == 1);

    /**
     * 受管作用域：登记后可取出且字段一致；嵌套遮蔽与退出恢复。
     */
    auto outer = MakeCtx("outer", Clock::now() + std::chrono::seconds{30});
    {
        fw::RequestScope scope_outer(outer);
        auto r1 = fw::CurrentRequestContext();
        BOOST_REQUIRE(r1);
        BOOST_CHECK(r1.value()->request_id == "outer");
        {
            auto inner = MakeCtx("inner",
                                 Clock::now() + std::chrono::seconds{30});
            fw::RequestScope scope_inner(inner);
            auto r2 = fw::CurrentRequestContext();
            BOOST_REQUIRE(r2);
            BOOST_CHECK(r2.value()->request_id == "inner");
        }
        auto r3 = fw::CurrentRequestContext();
        BOOST_REQUIRE(r3);
        BOOST_CHECK(r3.value()->request_id == "outer");
    }
    auto r4 = fw::CurrentRequestContext();
    BOOST_REQUIRE(!r4);
    BOOST_CHECK(r4.error().code == fw::ErrorCode::InvalidContext);

    /**
     * 协程内建立的受管上下文经协程 id 绑定，可取出。
     */
    TestLatch latch2(1);
    std::atomic<int> seen2{0};
    bbtco_ref {
        auto ctx = MakeCtx("co-scope",
                           Clock::now() + std::chrono::seconds{30});
        fw::RequestScope scope(ctx);
        auto r = fw::CurrentRequestContext();
        seen2.store(r && r.value()->request_id == "co-scope" ? 1 : 2);
        latch2.CountDown();
    };
    latch2.Wait();
    BOOST_CHECK(seen2.load() == 1);
}

/**
 * 用例 8（已移除）：原用例验证旧取消装配注入点的异常边界。取消装配已从
 * framework 公共面删除；出站发送、票据与期限行为由本文件真实入口用例与
 * egress_request_phase 承担。
 */

/**
 * 用例 9：分层类型不可互换（编译期 static_assert 见文件头，
 * 此处给出运行期可报告断言）。
 */
BOOST_AUTO_TEST_CASE(layered_types_not_interchangeable) {
    BOOST_CHECK(
        (!std::is_same<fw::CallOptions, bbt::infra::CallOptions>::value));
    BOOST_CHECK(
        (!std::is_convertible<fw::CallOptions,
                              bbt::infra::CallOptions>::value));
}

/**
 * 补充正路径：成功有序发送经真实出站入口完成——票据先绑定（fw.sequence
 * 元数据随 envelope 下行、票据标记已发送），期限继承值随发送下行；下一
 * 序号票据随后按水位依序绑定发送（正常有序发送，水位恰推进一格）。
 */
CO_TEST_CASE(success_path_consumes_sequence_and_initiates_io) {
    auto probe = std::make_shared<SendProbe>();
    auto pa = InstallProbeApp(probe->Fn());
    BOOST_REQUIRE(pa.svc != nullptr);

    auto session_r = fw::OrderedSession::Open(MakeGrant("f1a9"));
    BOOST_REQUIRE(session_r);
    auto session = session_r.value();
    auto stamp1_r = session->prepare();
    BOOST_REQUIRE(stamp1_r);
    auto stamp2_r = session->prepare();
    BOOST_REQUIRE(stamp2_r);

    const auto parent_d = Clock::now() + std::chrono::seconds{30};
    auto req_r = fw::CoRpcReq::From(std::string{"probe"});
    BOOST_REQUIRE(req_r);

    fw::CallOptions opt1;
    opt1.ordered = stamp1_r.value();
    auto out1 = [&] {
        fw::RequestScope scope(MakeCtx("p", parent_d));
        return pa.svc->CallTo(kEchoTarget, req_r.value(), opt1);
    }();
    BOOST_REQUIRE(out1);
    BOOST_CHECK(probe->sends == 1);
    BOOST_CHECK(session->IsTicketSent(stamp1_r.value()));
    BOOST_CHECK(probe->last_deadline == parent_d);
    BOOST_CHECK(probe->last_sequence == "1");

    /**
     * 下一序号：按水位依序绑定并发送成功。
     */
    fw::CallOptions opt2;
    opt2.ordered = stamp2_r.value();
    auto out2 = [&] {
        fw::RequestScope scope(MakeCtx("p", parent_d));
        return pa.svc->CallTo(kEchoTarget, req_r.value(), opt2);
    }();
    BOOST_REQUIRE(out2);
    BOOST_CHECK(probe->sends == 2);
    BOOST_CHECK(session->IsTicketSent(stamp2_r.value()));
    BOOST_CHECK(probe->last_sequence == "2");
}

/**
 * 无发送入口（缝 rpc_send 为空）→ RuntimeUnavailable；票据绑定先于发送
 * 入口完成：失败后票据已标记已发送，且下一序号的有效绑定仍成功——水位
 * 已因绑定推进，而非因发送成功。
 */
CO_TEST_CASE(missing_egress_entry_fails_after_ticket_binding) {
    auto pa = InstallProbeApp(fw::RpcSendAppFn{});
    BOOST_REQUIRE(pa.svc != nullptr);

    auto session_r = fw::OrderedSession::Open(MakeGrant("f1a-missing"));
    BOOST_REQUIRE(session_r);
    auto session = session_r.value();
    auto stamp1_r = session->prepare();
    BOOST_REQUIRE(stamp1_r);
    auto stamp2_r = session->prepare();
    BOOST_REQUIRE(stamp2_r);

    const auto parent_d = Clock::now() + std::chrono::seconds{30};
    auto req_r = fw::CoRpcReq::From(std::string{"probe"});
    BOOST_REQUIRE(req_r);

    fw::CallOptions opt1;
    opt1.ordered = stamp1_r.value();
    auto out1 = [&] {
        fw::RequestScope scope(MakeCtx("p", parent_d));
        return pa.svc->CallTo(kEchoTarget, req_r.value(), opt1);
    }();
    BOOST_REQUIRE(!out1);
    BOOST_CHECK(out1.error().code == fw::ErrorCode::RuntimeUnavailable);
    BOOST_CHECK(session->IsTicketSent(stamp1_r.value()));

    /**
     * 水位已推进的可再证观察：下一序号绑定成功（票据标记已发送）。
     */
    fw::CallOptions opt2;
    opt2.ordered = stamp2_r.value();
    auto out2 = [&] {
        fw::RequestScope scope(MakeCtx("p", parent_d));
        return pa.svc->CallTo(kEchoTarget, req_r.value(), opt2);
    }();
    BOOST_REQUIRE(!out2);
    BOOST_CHECK(out2.error().code == fw::ErrorCode::RuntimeUnavailable);
    BOOST_CHECK(session->IsTicketSent(stamp2_r.value()));
}

/**
 * 两层对照的「服务发送器未接线」层：宿主传输记录仪在位（SendProbe 在缝上、
 * 路由有效），缺的是服务级出站注入点——Running App 热安装窗口内 OnStart
 * 先于 _BindEgressToService 执行。OnStart 内带有效上下文与有序票据的真实
 * 调用命中 _CallSend 首道缺入口检查：RuntimeUnavailable、票据未绑定、
 * 发送计数 0——上下文与票据俱全仍不绑定，证明未接线检查先于绑定水位。
 * 安装返回后注入点已接：同一票据（同序号）按序绑定发送成功——序号 "1"
 * 说明接线前的失败没有推进水位。与
 * missing_egress_entry_fails_after_ticket_binding（宿主传输空、绑定后
 * 失败）构成相反对照。
 */
CO_TEST_CASE(unwired_service_sender_fails_before_ticket_binding) {
    auto probe = std::make_shared<SendProbe>();
    fw::AppOptions opts;
    opts.SetLimits(64, 64, 64, 64)
        .SetShutdownBudget(std::chrono::milliseconds{2000})
        .SetLifecycleBudget(std::chrono::milliseconds{2000})
        .SetNetworkLimits(bbt::infra::NetworkLimits{
            /*max_connections*/ 64,
            /*max_inflight*/ 64,
            /*max_header_bytes*/ 8192,
            /*max_body_bytes*/ 65536,
            /*incoming_timeout*/ std::chrono::milliseconds{30000}})
        .AddRoute(std::string{kEchoTarget}, "127.0.0.1:9")
        .SetLocalIdentity(std::string{"egress-prewire-onstart-test"});
    auto app = fw::MakeCoAppForTest(
        std::move(opts),
        fw::CoAppSeam{std::make_shared<StubNetHost>(), probe->Fn()});
    BOOST_REQUIRE(app);
    auto started = bbt_co_await app->CoStart();
    BOOST_REQUIRE(started);
    BOOST_REQUIRE(app->State() == fw::AppState::Running);

    auto session_r = fw::OrderedSession::Open(MakeGrant("f1a-prewire"));
    BOOST_REQUIRE(session_r);
    auto session = session_r.value();
    auto stamp_r = session->prepare();
    BOOST_REQUIRE(stamp_r);

    auto req_r = fw::CoRpcReq::From(std::string{"probe"});
    BOOST_REQUIRE(req_r);

    OnStartObservation obs;
    obs.armed = true;
    obs.seam = probe.get();
    obs.session = session.get();
    obs.stamp = stamp_r.value();
    obs.request = req_r.value();

    /**
     * 接线前（OnStart 窗口）：RuntimeUnavailable、票据未绑定、发送计数 0。
     */
    EgressProbeSvc::ArmOnStartObservation(&obs);
    auto installed = bbt_co_await app->CoInstallService<EgressProbeSvc>(
        fw::ServiceOptions{}.SetMaxInflight(4));
    EgressProbeSvc::ArmOnStartObservation(nullptr);
    BOOST_REQUIRE(installed);
    BOOST_REQUIRE(obs.observed);
    BOOST_CHECK(!obs.call_ok);
    BOOST_CHECK(obs.error_code == fw::ErrorCode::RuntimeUnavailable);
    BOOST_CHECK(!obs.ticket_sent);
    BOOST_CHECK(obs.sends == 0);

    /**
     * 接线后：同一票据（同序号）按序绑定并发送成功。
     */
    std::shared_ptr<void> owner;
    auto* svc = ManagedService<EgressProbeSvc>(*app, owner);
    BOOST_REQUIRE(svc != nullptr);
    fw::CallOptions opt;
    opt.ordered = stamp_r.value();
    auto retry = [&] {
        fw::RequestScope scope(
            MakeCtx("p", Clock::now() + std::chrono::seconds{30}));
        return svc->CallTo(kEchoTarget, req_r.value(), opt);
    }();
    BOOST_REQUIRE(retry);
    BOOST_CHECK(probe->sends == 1);
    BOOST_CHECK(session->IsTicketSent(stamp_r.value()));
    BOOST_CHECK(probe->last_sequence == "1");
}

BOOST_AUTO_TEST_SUITE_END()
