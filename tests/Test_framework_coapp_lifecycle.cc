/**
 * CoApp 装配校验与生命周期：CoStart 相位（组件/服务 hook → 网络宿主
 * Create/Start）、StopAccepting → WaitHandlersDone →（对象收口）→
 *（排空失败时）Close 的调用序列、启动中途失败回退、排空预算耗尽
 * StopIncomplete 与迟到收口、同进程多实例独立生命周期。
 * 网络组件全部走 INetworkHost 桩（经 internal 测试缝注入）：断言真实调用
 * 序列、每步发生时 coroutine runtime 已初始化、超时与错误路径；无真实
 * socket、无 sleep 凑时序、无空断言。生命周期入口全部在真实受管协程内
 * 执行（CoStart/CoStop 是协程调用；RequestShutdown 由外部线程驱动）。
 *
 * 唯一公开 API 迁移语义映射（如实落地，非断言削弱）：
 * - 旧 HostLifecycle 的「排空失败永久保活」在新模型不存在：CoStop 在排空
 *   未被证实（超时/宿主 Close 打断/其他错误）时记 LifecycleFailure、撤销
 *   接纳、同步 Close 宿主并置 StopIncomplete，允许再次以新期限推进
 *   （tests-core-model 交接 S3/S4 同口径）。子进程保活探针随该语义删除。
 * - 排空失败路径的宿主收口是真实 Close（net.close 在断言序列中合法出现，
 *   仅成功排空路径不出现）；再次 CoStop 经 CloseReason 跳过重排空直接推进。
 * - 「run 返回码」映射为受管任务内的 rc 约定（见 kExit* 常量注释）。
 */
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>

/**
 * 机器面（宿主桩/测试缝构造）显式走 internal 头。
 */
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/NetworkHost.hpp>

namespace fw = bbt::framework;
namespace co = bbt::coroutine;
using Clock = std::chrono::steady_clock;

namespace {

/**
 * ── 测试屏障原语（std CV，无 sleep 凑时序）──
 */

class TestGate {
public:
    void Open() {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_open = true;
        }
        m_cv.notify_all();
    }
    void Wait() {
        std::unique_lock<std::mutex> lk(m_mtx);
        m_cv.wait(lk, [this] { return m_open; });
    }
    bool WaitFor(std::chrono::milliseconds ms) {
        std::unique_lock<std::mutex> lk(m_mtx);
        return m_cv.wait_for(lk, ms, [this] { return m_open; });
    }

private:
    std::mutex m_mtx;
    std::condition_variable m_cv;
    bool m_open = false;
};

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
    bool WaitFor(std::chrono::milliseconds ms) {
        std::unique_lock<std::mutex> lk(m_mtx);
        return m_cv.wait_for(lk, ms, [this] { return m_count == 0; });
    }

private:
    std::mutex m_mtx;
    std::condition_variable m_cv;
    int m_count;
};

constexpr std::chrono::milliseconds kWait{5000};

/**
 * 旧 run() 退出码在本测试内的对应映射：
 * kExitOk = CoStart/CoWaitShutdownRequest/CoStop 全部 ok 且 App 终态 Stopped
 * 且无 LifecycleFailure；
 * kExitStartFailed = 装配或 CoStart 返回错误；
 * kExitStopIncomplete = 收口后仍存在失败记录或未达 Stopped
 * （覆盖旧 kExitShutdownLate/kExitShutdownFailed 两档，原错误见失败清单）。
 */
constexpr int kExitOk = 0;
constexpr int kExitStartFailed = 2;
constexpr int kExitStopIncomplete = 4;

/**
 * ── INetworkHost 桩：记录每次调用的名字与当时 runtime 是否已初始化（初始化
 * 在进程寿命内恒为 true）；借此把「调度器先于网络、使用期保持运行」纳入
 * 同一序列断言。──
 */

struct NetCall {
    std::string name;
    /**
     * 调用发生时 coroutine runtime 已初始化
     */
    bool sched_ready;
};

class StubNetHost final : public fw::INetworkHost {
public:
    fw::result<void> create_rc = fw::result<void>::ok();
    fw::result<void> start_rc = fw::result<void>::ok();
    std::function<fw::result<void>(co::Deadline)> on_wait_handlers;
    std::function<void()> on_started;
    std::function<void()> on_closed;
    /**
     * Start() 被调用（启动完成、开始接纳）时倒计时
     */
    TestLatch start_seen{1};

    fw::result<void> Create() override {
        _Record("net.create");
        return create_rc;
    }
    fw::result<void> Start() override {
        _Record("net.start");
        start_seen.CountDown();
        auto rc = start_rc;
        if (rc && on_started)
            on_started();
        return rc;
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
        const bool first = !closed.exchange(true, std::memory_order_acq_rel);
        if (first && on_closed)
            on_closed();
    }
    void Close(bbt::infra::Error reason) noexcept override {
        {
            std::lock_guard<std::mutex> lk(m_reason_mtx);
            m_reason = std::move(reason);
        }
        Close();
    }
    std::optional<bbt::infra::Error> CloseReason() const noexcept override {
        if (!closed.load(std::memory_order_acquire))
            return std::nullopt;
        std::lock_guard<std::mutex> lk(m_reason_mtx);
        if (m_reason.message.empty())
            return fw::MakeError(fw::ErrorCode::Closed, "host closed");
        return m_reason;
    }

    std::vector<NetCall> Snapshot() const {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_calls;
    }
    std::vector<std::string> Names() const {
        std::lock_guard<std::mutex> lk(m_mtx);
        std::vector<std::string> out;
        for (const auto& c : m_calls)
            out.push_back(c.name);
        return out;
    }
    std::size_t TotalCalls() const {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_calls.size();
    }

private:
    void _Record(const char* name) {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_calls.push_back(NetCall{name, g_scheduler->IsInitialized()});
    }
    mutable std::mutex m_mtx;
    std::vector<NetCall> m_calls;
    std::atomic<bool> closed{false};
    mutable std::mutex m_reason_mtx;
    bbt::infra::Error m_reason{};
};

/**
 * ── 业务桩类型：受管 CoService（ConstructKey 构造 + protected 析构 +
 * friend Registry），方法表与旧版一致。──
 */

class EchoSvc final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "echo";
    explicit EchoSvc(fw::detail::ConstructKey k) : fw::CoService<>(k) {}
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value)
            return fw::CoRpcResp::Error(value.error());
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EchoSvc::Ping>("ping"));

protected:
    ~EchoSvc() noexcept override = default;

private:
    friend class fw::detail::Registry;
};

/**
 * 与 EchoSvc 同名：重复注册拒绝用。
 */
class EchoDupSvc final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "echo";
    explicit EchoDupSvc(fw::detail::ConstructKey k) : fw::CoService<>(k) {}
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value)
            return fw::CoRpcResp::Error(value.error());
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EchoDupSvc::Ping>("ping"));

protected:
    ~EchoDupSvc() noexcept override = default;

private:
    friend class fw::detail::Registry;
};

class KeyedSvc final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "keyed";
    explicit KeyedSvc(fw::detail::ConstructKey k) : fw::CoService<>(k) {}
    fw::CoRpcResp Get(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value)
            return fw::CoRpcResp::Error(value.error());
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::ActorMethodAt<&KeyedSvc::Get, std::int32_t>("get"));

protected:
    ~KeyedSvc() noexcept override = default;

private:
    friend class fw::detail::Registry;
};

/**
 * ActorSerial 缺 key 提取器（声明为普通 Method）：装配期校验失败用。
 */
class UnkeyedSvc final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "unkeyed";
    explicit UnkeyedSvc(fw::detail::ConstructKey k) : fw::CoService<>(k) {}
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value)
            return fw::CoRpcResp::Error(value.error());
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&UnkeyedSvc::Ping>("ping"));

protected:
    ~UnkeyedSvc() noexcept override = default;

private:
    friend class fw::detail::Registry;
};

/**
 * 出站调用桩：Dial 在受管 handler 内经 call 走真实路由门
 * （AppRuntimeState::FindRoute + seam_send 发送）。
 */
class CallerSvc final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "caller";
    explicit CallerSvc(fw::detail::ConstructKey k) : fw::CoService<>(k) {}
    fw::CoRpcResp Dial(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args)
            return fw::CoRpcResp::Error(args.error());
        auto response = bbt_co_await call(args.value(), "ping", req);
        if (!response)
            return fw::CoRpcResp::Error(response.error());
        return response.value();
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&CallerSvc::Dial>("dial"));

protected:
    ~CallerSvc() noexcept override = default;

private:
    friend class fw::detail::Registry;
};

/**
 * 资源验证组件：新模型中「命名资源」= App scope CoComponent；共享观测
 * 指针经 Options 注入（受管对象本体不直取，身份对照靠 shared_state）。
 */
struct ResShared {
    explicit ResShared(int v) : n(v) {}
    int n;
};

class DummyComponent final : public fw::CoComponent {
public:
    struct Options {
        std::shared_ptr<ResShared> s;
    };
    explicit DummyComponent(fw::detail::ConstructKey k, Options o)
        : fw::CoComponent(k), m_s(std::move(o.s)) {}
    const std::shared_ptr<ResShared>& Shared() const { return m_s; }

protected:
    ~DummyComponent() noexcept override = default;

private:
    std::shared_ptr<ResShared> m_s;
    friend class fw::detail::Registry;
};

/**
 * 资源观察口：公开包装 protected GetComponent（测试出口）。
 */
class ResSvc final : public fw::CoService<DummyComponent> {
public:
    static constexpr std::string_view kServiceName = "res";
    explicit ResSvc(fw::detail::ConstructKey k)
        : fw::CoService<DummyComponent>(k) {}
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value)
            return fw::CoRpcResp::Error(value.error());
        return fw::CoRpcResp::From(value.value());
    }
    using fw::CoService<DummyComponent>::GetComponent;
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&ResSvc::Ping>("ping"));

protected:
    ~ResSvc() noexcept override = default;

private:
    friend class fw::detail::Registry;
};

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

fw::AppOptions AppOpts(std::vector<fw::StaticRoute> routes = {}) {
    auto opts = fw::AppOptions{}
                    .SetLimits(64, 64, 64, 64)
                    .SetShutdownBudget(std::chrono::milliseconds{2000})
                    .SetLifecycleBudget(std::chrono::milliseconds{2000})
                    .SetNetworkLimits(bbt::infra::NetworkLimits{
                        /*max_connections*/ 64,
                        /*max_inflight*/ 64,
                        /*max_header_bytes*/ 8192,
                        /*max_body_bytes*/ 65536,
                        /*incoming_timeout*/ std::chrono::milliseconds{30000}})
                    /**
         * V2 会话身份必填；每个 owner 使用明确且互异身份。
         */
                    .SetLocalIdentity(std::string{"coapp-lifecycle-test"});
    for (auto& r : routes)
        opts.AddRoute(r.service_name, r.address);
    return opts;
}

/**
 * 测试缝构造：宿主桩 + 可注入出站发送（seam_send 为空 → 已解析路由的
 * 受管出站如实 RuntimeUnavailable）。构造必须在受管协程内调用
 * （CoRuntimeOnly 强约束）。
 */
std::shared_ptr<fw::CoApp> MakeApp(
    const fw::AppOptions& opts, const std::shared_ptr<StubNetHost>& host,
    fw::RpcSendAppFn rpc_send = {}) {
    return fw::MakeCoAppForTest(
        opts, fw::CoAppSeam{host, std::move(rpc_send)});
}

/**
 * 在受管协程内执行 fn：经 g_scheduler 投递协程任务，宿主线程用
 * 限时 std::cv 等其完成（宿主线程的 CV 等待不违反 worker 阻塞禁令）。
 * 返回 false 表示任务未在预算内完成（防挂死测试进程）。
 */
template <class F>
bool RunInManagedCoroutine(F&& fn,
                           std::chrono::milliseconds budget = std::chrono::milliseconds{10000}) {
    if (bbt::coroutine::IsInCoroutineContext()) {
        fn();
        return true;
    }
    /**
     * native 测试宿主负责初始化唯一 runtime，产品 App 不启动 runtime。
     */
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
        /**
         * 嵌套出站链（入站派发 → call → 出站适配 → 发送闭包）在
         * 12KB 默认栈触底（guard 页 SIGSEGV，与入站用例同口径）；放大栈。
         */
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
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
    /**
     * 不让仍在途任务使用已退出的测试栈。
     */
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
 * 受管运行任务：在宿主线程派生协程执行装配→CoStart→CoWaitShutdownRequest→
 * CoStop，rc 由终态与失败记录决定（kExit* 约定）。register_fn 在受管协程内
 * 完成装配（返回错误即 kExitStartFailed）。
 */
struct RunHandle {
    std::atomic<bool> done{false};
    int rc{-1};
};

std::unique_ptr<RunHandle> RunApp(
    std::shared_ptr<fw::CoApp> app,
    const std::function<fw::result<void>()>& register_fn = {}) {
    auto h = std::make_unique<RunHandle>();
    RunHandle* p = h.get();
    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [app = std::move(app), p, register_fn] {
            auto set_rc = [&](int rc) {
                p->rc = rc;
                p->done.store(true, std::memory_order_release);
            };
            try {
                if (register_fn) {
                    auto r = register_fn();
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
                auto waited = bbt_co_await app->CoWaitShutdownRequest();
                if (!waited) {
                    set_rc(kExitStopIncomplete);
                    return;
                }
                auto _stop = bbt_co_await app->CoStop();
                (void)_stop;
                set_rc(app->State() == fw::AppState::Stopped &&
                               app->LifecycleFailures().empty()
                           ? kExitOk
                           : kExitStopIncomplete);
            } catch (...) {
                set_rc(kExitStopIncomplete);
            }
        },
        submitted);
    if (!submitted) {
        p->rc = kExitStartFailed;
        p->done.store(true, std::memory_order_release);
    }
    return h;
}

void JoinRun(const std::unique_ptr<RunHandle>& h) {
    const auto until = Clock::now() + std::chrono::minutes{3};
    while (!h->done.load(std::memory_order_acquire) && Clock::now() < until)
        std::this_thread::yield();
}

bool WaitState(const fw::CoApp& app, fw::AppState want,
               std::chrono::milliseconds ms = kWait) {
    const auto until = Clock::now() + ms;
    while (app.State() != want && Clock::now() < until)
        std::this_thread::yield();
    return app.State() == want;
}

const std::vector<std::string> kCleanOrder{
    "net.create", "net.start", "net.stop_accepting", "net.wait_handlers"};

BOOST_AUTO_TEST_SUITE(framework_f1b1)

/**
 * 用例 0：排空未被证实（超时或非超时错误）→ 记 LifecycleFailure、撤销接纳、
 * 同步 Close 宿主、置 StopIncomplete；owner 保留，再次 CoStop 以更晚期限
 * 推进收口至 Stopped（StopIncomplete 契约）；原始排空错误保留可观察。
 * 对应旧「永久 ShutdownIncomplete 保活」用例的新模型落地：失败不再 park
 * 进程，而是诚实返回不完整状态并允许推进。
 */
CO_TEST_CASE(drain_failure_reports_incomplete_and_allows_retry) {
    for (const bool timeout_mode : {false, true}) {
        auto host = std::make_shared<StubNetHost>();
        std::atomic<int> wait_calls{0};
        host->on_wait_handlers =
            [timeout_mode, &wait_calls](co::Deadline) -> fw::result<void> {
            ++wait_calls;
            if (timeout_mode)
                return fw::result<void>::err(fw::MakeError(
                    fw::ErrorCode::TimedOut, "handlers still running"));
            return fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::InternalError, "permanent drain failure"));
        };
        auto app = MakeApp(AppOpts(), host);
        BOOST_REQUIRE(app);
        auto r = bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
        BOOST_REQUIRE(r);
        r = bbt_co_await app->CoStart();
        BOOST_REQUIRE(r);
        BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

        app->RequestShutdown();
        r = bbt_co_await app->CoStop();
        /**
         * 收口未竟：排空未被证实 → StopIncomplete；原始错误记入失败清单。
         */
        BOOST_REQUIRE(r);
        BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);
        const auto failures = app->LifecycleFailures();
        BOOST_REQUIRE(!failures.empty());
        BOOST_CHECK(failures.back().phase == "wait_handlers_done");
        const char* expect = timeout_mode ? "handlers still running"
                                          : "permanent drain failure";
        BOOST_CHECK_MESSAGE(
            failures.back().message.find(expect) != std::string::npos,
            "排空错误未保留可观察: " << failures.back().message);
        /**
         * 排空未被证实仍执行了宿主同步 Close（拒新接纳已完成，迟到
         * handler 终态由再次推进的对象收口负责）。
         */
        {
            const auto names = host->Names();
            BOOST_CHECK(std::find(names.begin(), names.end(), "net.close") !=
                        names.end());
        }
        /**
         * 服务对象 owner 保留：未销毁（StopIncomplete 语义）。
         */
        auto svc = app->FindService("echo");
        BOOST_REQUIRE(svc);
        BOOST_CHECK(svc.value().IsValid());

        /**
         * 再次推进：宿主已 Close → CloseReason 打断重排空，直接对象收口至
         * Stopped；WaitHandlersDone 不再被调用（不重排空）。
         */
        const int waits_before = wait_calls.load();
        r = bbt_co_await app->CoStop();
        BOOST_REQUIRE(r);
        BOOST_CHECK(app->State() == fw::AppState::Stopped);
        BOOST_CHECK(wait_calls.load() == waits_before);
    }
}

/**
 * 用例 0b：排空在预算内挂起（真实在途）→ StopAccepting 已发生、排空等待
 * 进行中（Stopping）→ 放行后排空被证实 → 收口至 Stopped、无失败项。
 * 证明「正在排空的关闭」与「排空失败的关闭」可区分：前者无 net.close。
 */
CO_TEST_CASE(drain_pending_then_late_completion_closes_clean) {
    auto host = std::make_shared<StubNetHost>();
    auto drain_entered = std::make_shared<TestLatch>(1);
    auto allow_drain = std::make_shared<TestGate>();
    host->on_wait_handlers =
        [drain_entered, allow_drain](co::Deadline) -> fw::result<void> {
        drain_entered->CountDown();
        allow_drain->Wait();
        return fw::result<void>::ok();
    };
    auto app = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app);
    auto r = bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
    BOOST_REQUIRE(r);
    r = bbt_co_await app->CoStart();
    BOOST_REQUIRE(r);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

    /**
     * 第二个受管协程驱动 CoStop（排空等待期间主协程保持可断言）。
     */
    auto stop_done = std::make_shared<std::atomic<bool>>(false);
    auto stop_rc = std::make_shared<std::optional<fw::result<void>>>();
    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [app, stop_done, stop_rc] {
            stop_rc->emplace(bbt_co_await app->CoStop());
            stop_done->store(true, std::memory_order_release);
        },
        submitted);
    BOOST_REQUIRE(submitted);

    app->RequestShutdown();
    BOOST_REQUIRE(drain_entered->WaitFor(kWait));
    BOOST_CHECK(app->State() == fw::AppState::Stopping);
    {
        const auto names = host->Names();
        BOOST_CHECK(std::find(names.begin(), names.end(),
                              "net.stop_accepting") != names.end());
        BOOST_CHECK(std::find(names.begin(), names.end(),
                              "net.wait_handlers") != names.end());
        /**
         * 排空进行中不得提前网络 Close。
         */
        BOOST_CHECK(std::find(names.begin(), names.end(),
                              "net.close") == names.end());
    }

    /**
     * 放行排空 → 完整收束至 Stopped、无 LifecycleFailure、无未完成项。
     */
    allow_drain->Open();
    const auto until = Clock::now() + kWait;
    while (!stop_done->load(std::memory_order_acquire) && Clock::now() < until)
        bbtco_sleep(1);
    BOOST_REQUIRE(stop_done->load(std::memory_order_acquire));
    BOOST_REQUIRE(stop_rc->has_value());
    BOOST_REQUIRE(**stop_rc);
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_CHECK(app->LifecycleFailures().empty());
    BOOST_CHECK(app->PendingCleanup().empty());
    const auto names = host->Names();
    BOOST_TEST(names == kCleanOrder);
}

/**
 * 用例 1：服务安装登记；同名重复注册被明确拒绝；服务身份快照可观察；
 * 组件经声明绑定在受管实例上可取回同一对象（资源缝正路径）。
 */
CO_TEST_CASE(install_registers_and_rejects_duplicate) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);

    auto r = bbt_co_await app->CoInstallService<ResSvc>(ConcurrentOpts());
    BOOST_REQUIRE(r);
    r = bbt_co_await app->CoInstallService<KeyedSvc>(ActorOpts());
    BOOST_REQUIRE(r);

    /**
     * 三个同类型不同名组件（资源对应物）；显式绑定逐个建立。
     */
    auto res = std::make_shared<ResShared>(7);
    auto primary = std::make_shared<ResShared>(11);
    auto replica = std::make_shared<ResShared>(22);
    r = bbt_co_await app->CoInstallComponent<DummyComponent>(
        "res-default", DummyComponent::Options{res});
    BOOST_REQUIRE(r);
    r = bbt_co_await app->CoInstallComponent<DummyComponent>(
        "primary", DummyComponent::Options{primary});
    BOOST_REQUIRE(r);
    r = bbt_co_await app->CoInstallComponent<DummyComponent>(
        "replica", DummyComponent::Options{replica});
    BOOST_REQUIRE(r);

    auto svc_handle = app->FindService("res");
    BOOST_REQUIRE(svc_handle);
    auto bound = bbt_co_await app->CoBindComponent<DummyComponent>(
        svc_handle.value(), "res-default");
    BOOST_REQUIRE(bound);
    bound = bbt_co_await app->CoBindComponent<DummyComponent>(
        svc_handle.value(), "primary");
    BOOST_REQUIRE(bound);
    bound = bbt_co_await app->CoBindComponent<DummyComponent>(
        svc_handle.value(), "replica");
    BOOST_REQUIRE(bound);

    /**
     * 同名重复注册被明确拒绝（EchoSvc 与 EchoDupSvc 的 kServiceName 同为
     * "echo"，先装 EchoSvc 占位）。
     */
    r = bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
    BOOST_REQUIRE(r);
    auto dup = bbt_co_await app->CoInstallService<EchoDupSvc>(ConcurrentOpts());
    BOOST_REQUIRE(!dup);
    BOOST_CHECK(dup.error().code == fw::ErrorCode::InvalidArgument);
    BOOST_TEST(dup.error().message.find("duplicate") != std::string::npos);

    r = bbt_co_await app->CoStart();
    BOOST_REQUIRE(r);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

    /**
     * 服务身份快照：受管对象 Info() 值面（kind/name/state）。
     */
    auto h = app->FindService("res");
    BOOST_REQUIRE(h);
    auto info = h.value().Info();
    BOOST_REQUIRE(info);
    BOOST_CHECK(info.value().kind == fw::ObjectKind::Service);
    BOOST_CHECK(info.value().name == "res");
    BOOST_CHECK(info.value().state == fw::ObjectState::Running);

    /**
     * 资源缝正路径：装配进 App 的组件经服务绑定可取回同一对象
     * （shared_state 指针对照证明实例身份一致）。
     */
    std::shared_ptr<void> owner;
    auto* svc = ManagedService<ResSvc>(*app, owner);
    BOOST_REQUIRE(svc != nullptr);
    auto got = svc->GetComponent<DummyComponent>("res-default");
    BOOST_REQUIRE(got);
    {
        auto borrow = got.value().Borrow();
        BOOST_REQUIRE(borrow);
        BOOST_CHECK(borrow.value().Get().Shared() == res);
        BOOST_CHECK(borrow.value().Get().Shared()->n == 7);
    }
    auto got_primary = svc->GetComponent<DummyComponent>("primary");
    auto got_replica = svc->GetComponent<DummyComponent>("replica");
    BOOST_REQUIRE(got_primary);
    BOOST_REQUIRE(got_replica);
    {
        auto b1 = got_primary.value().Borrow();
        auto b2 = got_replica.value().Borrow();
        BOOST_REQUIRE(b1);
        BOOST_REQUIRE(b2);
        BOOST_CHECK(b1.value().Get().Shared() == primary);
        BOOST_CHECK(b2.value().Get().Shared() == replica);
        BOOST_CHECK(b1.value().Get().Shared()->n == 11);
        BOOST_CHECK(b2.value().Get().Shared()->n == 22);
    }
    /**
     * 未绑定名 → NotFound（新模型为显式绑定表，未绑名没有隐式槽位）。
     */
    auto missing = svc->GetComponent<DummyComponent>("missing");
    BOOST_REQUIRE(!missing);
    BOOST_CHECK(missing.error().code == fw::ErrorCode::NotFound);

    /**
     * ActorSerial 服务同样登记在服务表：FindService 可查到（新模型中
     * key 派发是邮箱执行资格，不是独立 actor 对象表）。
     */
    auto keyed = app->FindService("keyed");
    BOOST_REQUIRE(keyed);
    BOOST_CHECK(keyed.value().IsValid());
    /**
     * 未注册名 → NotFound。
     */
    BOOST_CHECK(!app->FindService("no-such"));

    app->RequestShutdown();
    r = bbt_co_await app->CoStop();
    BOOST_REQUIRE(r);
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
}

/**
 * 用例 2：ServiceOptions 违反执行策略校验 → CoInstallService 失败，任何
 * 组件未启动（桩计数 0）；网络配置非法 → CoStart 拒绝，配置失败留档，
 * 组件 OnStart 一律不执行。
 */
CO_TEST_CASE(invalid_options_fail_before_any_startup) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);

    /**
     * max_inflight == 0
     */
    {
        auto o = ConcurrentOpts().SetMaxInflight(0);
        auto r = bbt_co_await app->CoInstallService<EchoSvc>(o);
        BOOST_REQUIRE(!r);
        BOOST_CHECK(r.error().code == fw::ErrorCode::InvalidArgument);
    }
    /**
     * ActorSerial 缺 serial 额度（旧 mailbox_capacity=0 对应物：新矩阵
     * 要求 max_serial_keys/serial_queue_capacity 均为有限正值）。
     */
    {
        auto o = ActorOpts().SetSerialQueueCapacity(0);
        auto r = bbt_co_await app->CoInstallService<KeyedSvc>(o);
        BOOST_REQUIRE(!r);
        BOOST_CHECK(r.error().code == fw::ErrorCode::InvalidArgument);
    }
    /**
     * ActorSerial 服务存在无 key 提取器方法 → 装配期失败（方法名透出）。
     */
    {
        auto r = bbt_co_await app->CoInstallService<UnkeyedSvc>(ActorOpts());
        BOOST_REQUIRE(!r);
        BOOST_CHECK(r.error().code == fw::ErrorCode::InvalidArgument);
        BOOST_TEST(r.error().message.find("ping") != std::string::npos);
    }

    /**
     * 注册失败不登记：同服务仍可重新合法注册。
     */
    auto ok = bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
    BOOST_REQUIRE(ok);

    /**
     * 网络配置校验失败（network_limits 非法）→ 不启动任何组件；
     * 配置失败留档可观察。
     */
    auto host2 = std::make_shared<StubNetHost>();
    auto bad_opts = AppOpts();
    bad_opts.SetNetworkLimits(bbt::infra::NetworkLimits{
        /*max_connections*/ 64,
        /*max_inflight*/ 0,
        /*max_header_bytes*/ 8192,
        /*max_body_bytes*/ 65536,
        /*incoming_timeout*/ std::chrono::milliseconds{30000}});
    auto app2 = MakeApp(bad_opts, host2);
    BOOST_REQUIRE(app2);
    auto r2 = bbt_co_await app2->CoStart();
    BOOST_REQUIRE(!r2);
    BOOST_CHECK(r2.error().code == fw::ErrorCode::InvalidArgument);
    /**
     * 网络校验失败走空集回退 + 宿主 Close（INetworkHost 契约：Create 后必可
     * Close）；除 Close 外无其他网络动作。
     */
    BOOST_TEST(host2->Names() == std::vector<std::string>({"net.close"}));
    BOOST_TEST(app2->LifecycleFailures().size() == std::size_t{1});
    BOOST_CHECK(app2->LifecycleFailures().front().phase == "validate");
    /**
     * 配置拒绝进入收口路径（_RollbackStart 空集 + Stopped），组件未启动。
     */
    BOOST_CHECK(app2->State() == fw::AppState::Stopped);
}

/**
 * 用例 3：未显式配置的出站目标被拒绝——不存在「自动连任意服务」；
 * 拒绝发生在出站路由门（FindRoute → NotFound），不触达网络发送。
 */
CO_TEST_CASE(unconfigured_route_target_rejected) {
    auto host = std::make_shared<StubNetHost>();
    /**
     * 测试缝出站：记录目标；不真正发送（本用例只断言门内拒绝）。
     */
    auto sends = std::make_shared<std::atomic<int>>(0);
    auto send = [sends](std::string_view,
                        const bbt::infra::rpc::Request&,
                        const bbt::infra::CallOptions&)
        -> bbt_co_async(fw::result<bbt::infra::rpc::Response>) {
        sends->fetch_add(1, std::memory_order_acq_rel);
        bbt_co_return fw::result<bbt::infra::rpc::Response>::err(fw::MakeError(
            fw::ErrorCode::RuntimeUnavailable, "test seam send"));
    };
    auto app = MakeApp(
        AppOpts({fw::StaticRoute{"svc.allowed", "127.0.0.1:9001"}}),
        host, std::move(send));
    auto r = bbt_co_await app->CoInstallService<CallerSvc>(ConcurrentOpts());
    BOOST_REQUIRE(r);
    r = bbt_co_await app->CoStart();
    BOOST_REQUIRE(r);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

    /**
     * 真实分发链：Dial(svc.unknown) → 路由门拒绝（NotFound + 服务名）。
     */
    auto env = bbt::infra::rpc::Request();
    env.SetService("caller")
        .SetMethod("dial")
        .SetRequestId("route-1")
        .SetRequestSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetResponseSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetReply(bbt::infra::rpc::ReplyMode::Required)
        .SetTraffic(bbt::infra::rpc::TrafficClass::Service)
        .SetPayload(fw::CoRpcReq::From(
                        std::tuple{std::string("svc.unknown")})
                        .value()
                        .payload());
    auto bad = fw::DispatchInboundForTest(*app, env);
    BOOST_REQUIRE(!bad);
    BOOST_CHECK(bad.error().code == fw::ErrorCode::NotFound);
    BOOST_TEST(bad.error().message.find("svc.unknown") != std::string::npos);
    BOOST_CHECK(sends->load() == 0);

    /**
     * 已配置路由过门：到达发送注入点（本测试缝如实返回 RuntimeUnavailable，
     * 证明路由通过而未伪造发送结果）。
     */
    env.SetRequestId("route-2");
    env.SetPayload(fw::CoRpcReq::From(
                       std::tuple{std::string("svc.allowed")})
                       .value()
                       .payload());
    auto via = fw::DispatchInboundForTest(*app, env);
    BOOST_REQUIRE(!via);
    BOOST_CHECK(via.error().code == fw::ErrorCode::RuntimeUnavailable);
    BOOST_CHECK(sends->load() == 1);

    r = bbt_co_await app->CoStop();
    BOOST_REQUIRE(r);
}

/**
 * 用例 4：启动顺序 = runtime 初始化 → 网络 Create/Start（开始接纳）→ 运行；
 * 桩调用点记录「runtime 已初始化」证明调度器先于网络、且整个使用期保持运行。
 */
CO_TEST_CASE(startup_order_scheduler_then_network) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);

    auto h = RunApp(app, [] {
        return fw::result<void>::ok();
    });
    BOOST_REQUIRE(WaitState(*app, fw::AppState::Running));

    app->RequestShutdown();
    JoinRun(h);
    BOOST_TEST(h->rc == kExitOk);
    BOOST_CHECK(app->State() == fw::AppState::Stopped);

    const auto calls = host->Snapshot();
    std::vector<std::string> names;
    for (const auto& c : calls)
        names.push_back(c.name);
    BOOST_TEST(names == kCleanOrder);
    /**
     * 全部网络调用都发生在 runtime 已初始化之后（初始化先于 net.create，
     * 且进程寿命内不复位）。
     */
    for (const auto& c : calls)
        BOOST_TEST(c.sched_ready);
}

/**
 * 用例 5：关闭顺序 = StopAccepting → 等 handler 结束 →（对象收口）；
 * StopAccepting 严格先于 WaitHandlersDone；成功排空路径不做宿主 Close。
 */
CO_TEST_CASE(shutdown_order_stop_accepting_before_drain) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);

    auto h = RunApp(app, [app] {
        return bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
    });
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));
    app->RequestShutdown();
    JoinRun(h);
    BOOST_TEST(h->rc == kExitOk);

    const auto calls = host->Snapshot();
    auto pos = [&calls](const char* n) -> std::size_t {
        for (std::size_t i = 0; i < calls.size(); ++i)
            if (calls[i].name == n)
                return i;
        return calls.size();
    };
    BOOST_TEST(pos("net.stop_accepting") < pos("net.wait_handlers"));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
}

/**
 * 用例 6：启动中途失败——宿主 Create/Start 失败：启动回退按契约同步
 * Close 宿主（Create 后无论 Start 是否成功 Close 安全可调），不留半启动。
 */
CO_TEST_CASE(startup_failure_rolls_back_in_order) {
    /**
     * 失败在第 1 个网络步（Create）
     */
    {
        auto host = std::make_shared<StubNetHost>();
        host->create_rc = fw::result<void>::err(
            fw::MakeError(fw::ErrorCode::InternalError, "create boom"));
        auto app = MakeApp(AppOpts(), host);
        auto r = bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
        BOOST_REQUIRE(r);

        r = bbt_co_await app->CoStart();
        BOOST_REQUIRE(!r);
        BOOST_CHECK(r.error().code == fw::ErrorCode::InternalError);
        BOOST_TEST(host->Names() == std::vector<std::string>(
                                        {"net.create", "net.close"}));
        BOOST_CHECK(app->State() == fw::AppState::Stopped);
        /**
         * 网络相位失败的错误以 CoStart 返回值透出（对象级失败清单只记
         * hook/校验失败，见 _StartImpl 回退路径）——错误可见性由返回值承担。
         */
    }
    /**
     * 失败在第 2 个网络步（Start）：Create 已立起 → 回退 Close。
     */
    {
        auto host = std::make_shared<StubNetHost>();
        host->start_rc = fw::result<void>::err(
            fw::MakeError(fw::ErrorCode::Unavailable, "start boom"));
        auto app = MakeApp(AppOpts(), host);
        auto r = bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
        BOOST_REQUIRE(r);

        r = bbt_co_await app->CoStart();
        BOOST_REQUIRE(!r);
        BOOST_CHECK(r.error().code == fw::ErrorCode::Unavailable);
        const auto calls = host->Snapshot();
        std::vector<std::string> names;
        for (const auto& c : calls)
            names.push_back(c.name);
        BOOST_TEST(names == std::vector<std::string>(
                                {"net.create", "net.start", "net.close"}));
        for (const auto& c : calls)
            BOOST_TEST(c.sched_ready);
        BOOST_CHECK(app->State() == fw::AppState::Stopped);
    }
}

/**
 * 用例 7：排空超时——桩让 handler 在预算内不结束：StopIncomplete、
 * 原始 TimedOut 保留可观察、宿主被同步 Close；再次推进收口至 Stopped。
 */
CO_TEST_CASE(drain_timeout_reports_incomplete_then_completes) {
    auto host = std::make_shared<StubNetHost>();
    auto wait_calls = std::make_shared<std::atomic<int>>(0);
    auto first_deadline_finite = std::make_shared<std::atomic<bool>>(false);
    host->on_wait_handlers =
        [wait_calls, first_deadline_finite](co::Deadline d) -> fw::result<void> {
        const auto call = wait_calls->fetch_add(1);
        if (call == 0) {
            /**
                 * 预算内等待：期限必须是有限生命周期预算。
                 */
            first_deadline_finite->store(
                d < Clock::now() + std::chrono::hours{1},
                std::memory_order_release);
            return fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::TimedOut, "handlers still running"));
        }
        return fw::result<void>::ok();
    };
    auto app = MakeApp(AppOpts(), host);
    auto r = bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
    BOOST_REQUIRE(r);
    r = bbt_co_await app->CoStart();
    BOOST_REQUIRE(r);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

    app->RequestShutdown();
    r = bbt_co_await app->CoStop();
    BOOST_REQUIRE(r);

    BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);
    const auto failures = app->LifecycleFailures();
    BOOST_REQUIRE(!failures.empty());
    BOOST_CHECK(failures.back().phase == "wait_handlers_done");
    BOOST_CHECK(failures.back().code == fw::ErrorCode::TimedOut);
    /**
     * StopAccepting 已先于超时发生；排空未证实后宿主被同步 Close。
     */
    {
        const auto names = host->Names();
        auto pos = [&names](const char* n) {
            return std::find(names.begin(), names.end(), n) - names.begin();
        };
        BOOST_CHECK(pos("net.stop_accepting") < pos("net.wait_handlers"));
        BOOST_CHECK(pos("net.wait_handlers") < pos("net.close"));
    }
    BOOST_CHECK(first_deadline_finite->load());

    /**
     * 再次推进：宿主已 Close → 不重排空直接收口；WaitHandlersDone 不增。
     */
    const int waits_before = wait_calls->load();
    r = bbt_co_await app->CoStop();
    BOOST_REQUIRE(r);
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_CHECK(wait_calls->load() == waits_before);
    BOOST_CHECK(app->PendingCleanup().empty());
}

/**
 * 用例 8：同进程多实例——第二个 CoApp 有独立生命周期与宿主桩；两个实例
 * 各自完整启动/收口互不干扰（新模型无全局 run 单例，多实例并存合法）。
 */
CO_TEST_CASE(two_apps_have_independent_lifecycles) {
    auto host1 = std::make_shared<StubNetHost>();
    auto host2 = std::make_shared<StubNetHost>();
    auto app1 = MakeApp(AppOpts(), host1);
    auto app2 = MakeApp(AppOpts(), host2);
    auto r = bbt_co_await app1->CoInstallService<EchoSvc>(ConcurrentOpts());
    BOOST_REQUIRE(r);
    r = bbt_co_await app2->CoInstallService<EchoSvc>(ConcurrentOpts());
    BOOST_REQUIRE(r);

    auto h1 = RunApp(app1, [] { return fw::result<void>::ok(); });
    auto h2 = RunApp(app2, [] { return fw::result<void>::ok(); });
    BOOST_REQUIRE(WaitState(*app1, fw::AppState::Running));
    BOOST_REQUIRE(WaitState(*app2, fw::AppState::Running));
    BOOST_CHECK(app1->State() == fw::AppState::Running);
    BOOST_CHECK(app2->State() == fw::AppState::Running);
    BOOST_TEST(host1->TotalCalls() == std::size_t{2});
    BOOST_TEST(host2->TotalCalls() == std::size_t{2});

    /**
     * 独立收口：先收 app1 不影响 app2 运行。
     */
    app1->RequestShutdown();
    JoinRun(h1);
    BOOST_TEST(h1->rc == kExitOk);
    BOOST_CHECK(app1->State() == fw::AppState::Stopped);
    BOOST_CHECK(app2->State() == fw::AppState::Running);

    app2->RequestShutdown();
    JoinRun(h2);
    BOOST_TEST(h2->rc == kExitOk);
    BOOST_CHECK(app2->State() == fw::AppState::Stopped);
}

/**
 * 补充：早于启动的 RequestShutdown → 关闭请求已置位，CoWaitShutdownRequest
 * 立即返回，启动后随即进入关闭序列。
 */
CO_TEST_CASE(shutdown_requested_before_start_still_stops) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);
    auto r = bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
    BOOST_REQUIRE(r);

    /**
     * 先于启动置位：CoWaitShutdownRequest 立即完成。
     */
    app->RequestShutdown();

    r = bbt_co_await app->CoStart();
    BOOST_REQUIRE(r);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

    auto waited = bbt_co_await app->CoWaitShutdownRequest();
    BOOST_REQUIRE(waited);
    r = bbt_co_await app->CoStop();
    BOOST_REQUIRE(r);
    const auto names = host->Names();
    BOOST_TEST(names == kCleanOrder);
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
}

/**
 * ── 再次发布与真实卸载契约用例的夹具 ──
 * 服务方法查找、授权流顺序状态与卸载拒绝路径全部经真实 CoApp 入口
 * （CoStart/CoPause/CoResume/CoUninstallService）与真实入站分发链
 * （DispatchInboundForTest）断言；确定性排程只用真实信号（handler 内
 * CoWaiter 门、邮箱串行次序、未知方法探测的错误文案迁移），无 sleep。
 */

/**
 * 有序流回声服务：授权流与序号状态的跨发布存活断言被测服务。重放路径
 * 不进 handler，实例计数即「只装配一次入口」的证据。
 */
class OrderedEchoSvc final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "ordered-echo";
    explicit OrderedEchoSvc(fw::detail::ConstructKey k) : fw::CoService<>(k) {}
    fw::CoRpcResp Echo(fw::CoRpcReq req) {
        auto args = req.Parse<std::string, std::int32_t>();
        if (!args)
            return fw::CoRpcResp::Error(args.error());
        hits->fetch_add(1, std::memory_order_acq_rel);
        return fw::CoRpcResp::From(std::get<1>(args.value()));
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::ActorMethodAt<&OrderedEchoSvc::Echo, std::string>("echo"));

    std::shared_ptr<std::atomic<int>> hits{std::make_shared<std::atomic<int>>(0)};

protected:
    ~OrderedEchoSvc() noexcept override = default;

private:
    friend class fw::detail::Registry;
};

/**
 * 协程侧放行门：handler 在受管协程内挂起让出 worker，不阻塞调度线程。
 */
class CoReleaseGate {
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
    co::WaitStatus Wait(Clock::duration budget) {
        auto waiter = co::sync::CoWaiter::Create();
        co::WaitOptions wo;
        wo.deadline = Clock::now() + budget;
        const auto st = waiter->WaitWithCallback(wo, [&]() -> bool {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (m_open)
                waiter->Notify();
            else
                m_waiters.push_back(waiter);
            return true;
        });
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_waiters.erase(std::remove(m_waiters.begin(), m_waiters.end(), waiter),
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
 * 可阻塞键控服务：hold 挂起在放行门上构造真实在途，touch 立即返回；
 * hook 记录阶段信号作用的实例身份（卸载控制的是同一已安装服务）。
 */
class HoldSvc final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "hold";
    explicit HoldSvc(fw::detail::ConstructKey k) : fw::CoService<>(k) {}

    fw::CoRpcResp Hold(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value)
            return fw::CoRpcResp::Error(value.error());
        entered->CountDown();
        /**
         * 等待被真实放行才继续：超时不能当成「已收到放行」，否则用例会
         * 把未受控的返回当成通过。
         */
        if (release->Wait(kWait) != co::WaitStatus::Completed)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::TimedOut, "test hold gate not released"));
        return fw::CoRpcResp::From(value.value());
    }
    fw::CoRpcResp Touch(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value)
            return fw::CoRpcResp::Error(value.error());
        touches->fetch_add(1, std::memory_order_acq_rel);
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::ActorMethodAt<&HoldSvc::Hold, std::int32_t>("hold"),
        fw::ActorMethodAt<&HoldSvc::Touch, std::int32_t>("touch"));

    std::shared_ptr<TestLatch> entered{std::make_shared<TestLatch>(1)};
    std::shared_ptr<CoReleaseGate> release{std::make_shared<CoReleaseGate>()};
    std::shared_ptr<std::atomic<int>> touches{std::make_shared<std::atomic<int>>(0)};
    std::atomic<int> stop_runs{0};
    std::atomic<int> uninstall_runs{0};
    std::atomic<const void*> stop_instance{nullptr};

    fw::result<void> OnStop() override {
        stop_instance.store(this, std::memory_order_release);
        stop_runs.fetch_add(1, std::memory_order_acq_rel);
        return fw::result<void>::ok();
    }
    void OnUninstall() override {
        uninstall_runs.fetch_add(1, std::memory_order_acq_rel);
    }

protected:
    ~HoldSvc() noexcept override = default;

private:
    friend class fw::detail::Registry;
};

/**
 * 键控服务入站信封：int32 位置参数即 actor key 源。
 */
bbt::infra::rpc::Request KeyedEnvelope(const char* service, const char* method,
                                       const std::string& rid,
                                       std::int32_t value) {
    bbt::infra::rpc::Request env;
    env.SetService(service)
        .SetMethod(method)
        .SetRequestId(rid)
        .SetRequestSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetResponseSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetReply(bbt::infra::rpc::ReplyMode::Required)
        .SetTraffic(bbt::infra::rpc::TrafficClass::Actor);
    env.SetPayload(fw::CoRpcReq::From(std::tuple{value}).value().payload());
    return env;
}

bbt::infra::rpc::Request PingEnvelope(const std::string& rid, std::int32_t value) {
    auto env = KeyedEnvelope("echo", "ping", rid, value);
    env.SetTraffic(bbt::infra::rpc::TrafficClass::Service);
    return env;
}

/**
 * 有序流信封：fw.* 白名单元数据与 grant 四元组精确匹配。
 */
bbt::infra::rpc::Request OrderedEnvelope(const std::string& rid, const char* actor,
                                         std::int32_t value,
                                         std::uint64_t sequence) {
    bbt::infra::rpc::Request env;
    env.SetService("ordered-echo")
        .SetMethod("echo")
        .SetRequestId(rid)
        .SetRequestSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetResponseSchema(std::string(fw::kCoRpcPositionalSchema))
        .SetReply(bbt::infra::rpc::ReplyMode::Required)
        .SetTraffic(bbt::infra::rpc::TrafficClass::Actor);
    env.SetPayload(
        fw::CoRpcReq::From(std::tuple{std::string{actor}, value}).value().payload());
    env.SetMetadata({
        {"fw.producer_id", "producer.test"},
        {"fw.producer_epoch", "producer-epoch-1"},
        {"fw.receiver_epoch", "receiver-epoch-1"},
        {"fw.sequence", std::to_string(sequence)},
    });
    return env;
}

/**
 * 独立协程内驱动一次真实入站分发；结果经槽位回传，控制流用闩等待。
 */
struct InboundCall {
    std::optional<fw::result<bbt::infra::rpc::Response>> out;
    TestLatch done{1};
};

std::shared_ptr<InboundCall> SpawnInbound(
    const std::shared_ptr<fw::CoApp>& app, const bbt::infra::rpc::Request& env) {
    auto slot = std::make_shared<InboundCall>();
    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [app, slot, env] {
            slot->out = fw::DispatchInboundForTest(*app, env);
            slot->done.CountDown();
        },
        submitted);
    if (!submitted) {
        slot->out = fw::result<bbt::infra::rpc::Response>::err(
            fw::MakeError(fw::ErrorCode::InternalError, "test coroutine rejected"));
        slot->done.CountDown();
    }
    return slot;
}

/**
 * 摘表完成的真实信号：对已卸载服务的未知方法探测，错误文案从
 * 「unknown method」迁移为「unregistered service」（RemoveDispatch 的
 * erase + closed + 邮箱 Close 全部完成）。有界次真实派发，无 sleep。
 */
bool ProbeUnregistered(const std::shared_ptr<fw::CoApp>& app) {
    const auto probe = KeyedEnvelope("hold", "no-such-method", "probe-unreg", 0);
    for (int i = 0; i < 200; ++i) {
        auto slot = SpawnInbound(app, probe);
        if (!slot->done.WaitFor(kWait) || !slot->out.has_value())
            return false;
        if (!*slot->out && slot->out->error().message.find(
                               "unregistered service") != std::string::npos)
            return true;
    }
    return false;
}

/**
 * 用例：Paused 后再次 CoStart（基线支持路径）。分发表重新发布（桩宿主
 * 记到第二组 create/start），服务方法在新发布上真实服务；Paused 期间
 * 进入的旧发布在途请求持记录收尾；有序流授权与序号状态随共享描述存活
 * （同 envelope 重放命中缓存、seq 2 紧接 1 被授权）；最后 CoStop 真实
 * 排空。CoResume 的区分见 resume_after_pause_serves_without_republish。
 */
CO_TEST_CASE(paused_restart_republishes_and_keeps_ordered_state) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);

    auto r = bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
    BOOST_REQUIRE(r);
    const fw::ServiceOptions ordered_opts =
        fw::ServiceOptions{}
            .SetExecution(fw::ExecutionPolicy::ActorSerial)
            .SetMaxInflight(64)
            .SetMaxSerialKeys(8)
            .SetSerialQueueCapacity(16)
            .SetOrderedIngress(
                fw::OrderedIngressOptions{/*enabled*/ true, 8, 64, 65536});
    r = bbt_co_await app->CoInstallService<OrderedEchoSvc>(ordered_opts);
    BOOST_REQUIRE(r);
    r = bbt_co_await app->CoInstallService<HoldSvc>(ActorOpts());
    BOOST_REQUIRE(r);

    fw::OrderedGrant grant;
    grant.service = "ordered-echo";
    grant.actor_key = "acct-1";
    grant.producer_id = "producer.test";
    grant.producer_epoch = "producer-epoch-1";
    grant.receiver_epoch = "receiver-epoch-1";
    grant.peer_principal.clear();
    auto granted = app->grant_ordered_stream(std::move(grant));
    BOOST_REQUIRE(granted);

    r = bbt_co_await app->CoStart();
    BOOST_REQUIRE(r);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

    std::shared_ptr<void> ordered_owner;
    auto* ordered_svc = ManagedService<OrderedEchoSvc>(*app, ordered_owner);
    BOOST_REQUIRE(ordered_svc != nullptr);
    std::shared_ptr<void> hold_owner;
    auto* hold_svc = ManagedService<HoldSvc>(*app, hold_owner);
    BOOST_REQUIRE(hold_svc != nullptr);

    /**
     * 首次发布基线：并发、键控与有序流三条路径都真实服务。
     */
    auto ping1 = SpawnInbound(app, PingEnvelope("ping-1", 7));
    BOOST_REQUIRE(ping1->done.WaitFor(kWait));
    BOOST_REQUIRE(ping1->out.has_value() && static_cast<bool>(*ping1->out));
    auto touch1 = SpawnInbound(app, KeyedEnvelope("hold", "touch", "touch-1", 2));
    BOOST_REQUIRE(touch1->done.WaitFor(kWait));
    BOOST_REQUIRE(touch1->out.has_value() && static_cast<bool>(*touch1->out));
    BOOST_CHECK_EQUAL(hold_svc->touches->load(), 1);
    const auto env1 = OrderedEnvelope("ordered-1", "acct-1", 41, 1);
    auto seq1 = SpawnInbound(app, env1);
    BOOST_REQUIRE(seq1->done.WaitFor(kWait));
    BOOST_REQUIRE(seq1->out.has_value() && static_cast<bool>(*seq1->out));
    BOOST_CHECK_EQUAL(ordered_svc->hits->load(), 1);
    const auto first_payload = seq1->out->value().Payload();

    r = bbt_co_await app->CoPause();
    BOOST_REQUIRE(r);

    /**
     * Paused 期间进入的在途请求（分发表仍在服务）：跨二次发布持旧记录。
     */
    auto held = SpawnInbound(app, KeyedEnvelope("hold", "hold", "hold-a", 1));
    BOOST_REQUIRE(hold_svc->entered->WaitFor(kWait));

    r = bbt_co_await app->CoStart();
    BOOST_REQUIRE(r);
    /**
     * 桩宿主记到第二组 create/start：CoStart 真实重跑网络相位、重新发布
     * 分发表——不是 CoResume 的只跑 hook。
     */
    BOOST_TEST(host->Names() == std::vector<std::string>({"net.create", "net.start",
                                                          "net.create", "net.start"}));

    /**
     * 旧发布记录收尾：已进入请求真实返回，不被二次发布抢占。
     */
    hold_svc->release->Open();
    BOOST_REQUIRE(held->done.WaitFor(kWait));
    BOOST_REQUIRE(held->out.has_value() && static_cast<bool>(*held->out));

    /**
     * 新发布服务新请求：并发与键控路径都可查找、可执行。
     */
    auto ping2 = SpawnInbound(app, PingEnvelope("ping-2", 8));
    BOOST_REQUIRE(ping2->done.WaitFor(kWait));
    BOOST_REQUIRE(ping2->out.has_value() && static_cast<bool>(*ping2->out));
    auto touch2 = SpawnInbound(app, KeyedEnvelope("hold", "touch", "touch-2", 2));
    BOOST_REQUIRE(touch2->done.WaitFor(kWait));
    BOOST_REQUIRE(touch2->out.has_value() && static_cast<bool>(*touch2->out));
    BOOST_CHECK_EQUAL(hold_svc->touches->load(), 2);

    /**
     * 授权流顺序状态未丢失：同 envelope 重放命中缓存（不进 handler）、
     * 序号 2 紧接 1 被授权执行。若二次装配新建了入口，重放丢缓存、seq2
     * 变序号缺口——两条一起排除「再次装配丢状态」。
     */
    auto replay = SpawnInbound(app, env1);
    BOOST_REQUIRE(replay->done.WaitFor(kWait));
    BOOST_REQUIRE(replay->out.has_value() && static_cast<bool>(*replay->out));
    BOOST_CHECK(replay->out->value().Payload() == first_payload);
    BOOST_CHECK_EQUAL(ordered_svc->hits->load(), 1);
    auto seq2 = SpawnInbound(app, OrderedEnvelope("ordered-2", "acct-1", 42, 2));
    BOOST_REQUIRE(seq2->done.WaitFor(kWait));
    BOOST_REQUIRE(seq2->out.has_value() && static_cast<bool>(*seq2->out));
    BOOST_CHECK_EQUAL(ordered_svc->hits->load(), 2);

    /**
     * 最后真正排空：成功排空路径无 net.close。
     */
    app->RequestShutdown();
    r = bbt_co_await app->CoStop();
    BOOST_REQUIRE(r);
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_TEST(host->Names() ==
               std::vector<std::string>({"net.create", "net.start", "net.create",
                                         "net.start", "net.stop_accepting",
                                         "net.wait_handlers"}));
}

/**
 * 用例：CoPause 后 CoResume——恢复入口按调用结果断言（不以
 * state==Running 代替），方法仍真实服务，但桩宿主不追加 create/start：
 * 恢复不重新发布分发表（对照上一用例 CoStart 的第二组 create/start）。
 */
CO_TEST_CASE(resume_after_pause_serves_without_republish) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);

    auto r = bbt_co_await app->CoInstallService<EchoSvc>(ConcurrentOpts());
    BOOST_REQUIRE(r);
    r = bbt_co_await app->CoStart();
    BOOST_REQUIRE(r);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

    auto ping1 = SpawnInbound(app, PingEnvelope("ping-r1", 7));
    BOOST_REQUIRE(ping1->done.WaitFor(kWait));
    BOOST_REQUIRE(ping1->out.has_value() && static_cast<bool>(*ping1->out));

    r = bbt_co_await app->CoPause();
    BOOST_REQUIRE(r);

    /**
     * 暂停不摘表：Paused 期间分发仍真实服务（暂停 ≠ 卸载）。
     */
    auto ping_paused = SpawnInbound(app, PingEnvelope("ping-r-paused", 7));
    BOOST_REQUIRE(ping_paused->done.WaitFor(kWait));
    BOOST_REQUIRE(ping_paused->out.has_value() && static_cast<bool>(*ping_paused->out));

    r = bbt_co_await app->CoResume();
    BOOST_REQUIRE(r);
    auto ping2 = SpawnInbound(app, PingEnvelope("ping-r2", 8));
    BOOST_REQUIRE(ping2->done.WaitFor(kWait));
    BOOST_REQUIRE(ping2->out.has_value() && static_cast<bool>(*ping2->out));
    BOOST_TEST(host->Names() ==
               std::vector<std::string>({"net.create", "net.start"}));

    app->RequestShutdown();
    r = bbt_co_await app->CoStop();
    BOOST_REQUIRE(r);
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    BOOST_TEST(host->Names() ==
               std::vector<std::string>({"net.create", "net.start",
                                         "net.stop_accepting",
                                         "net.wait_handlers"}));
}

/**
 * 用例：真实 CoUninstallService 契约。摘分发表先行（新查找 NotFound，
 * 全新 key 不进 handler）；已进入请求真实返回；同邮箱等待项由 drain
 * 消费、执行租约被关闭门拒绝（Closed，而非 NotFound）；卸载 hook 落在
 * 同一实例；登记与安装描述同步摘除（重复卸载 NotFound、同名重装无
 * duplicate）。mailbox.Close 与 HTTP 停接单都不代表卸载——本用例全部
 * 经 CoUninstallService 驱动并以分发结果断言。
 */
CO_TEST_CASE(uninstall_contract_inflight_settles_and_rejects_fresh) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);

    auto r = bbt_co_await app->CoInstallService<HoldSvc>(ActorOpts());
    BOOST_REQUIRE(r);
    r = bbt_co_await app->CoStart();
    BOOST_REQUIRE(r);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

    std::shared_ptr<void> hold_owner;
    auto* hold_svc = ManagedService<HoldSvc>(*app, hold_owner);
    BOOST_REQUIRE(hold_svc != nullptr);

    auto base = SpawnInbound(app, KeyedEnvelope("hold", "touch", "touch-base", 2));
    BOOST_REQUIRE(base->done.WaitFor(kWait));
    BOOST_REQUIRE(base->out.has_value() && static_cast<bool>(*base->out));
    BOOST_CHECK_EQUAL(hold_svc->touches->load(), 1);

    /**
     * A 进入 handler 并挂起（key 1 邮箱建立、执行租约在途）；B/C 排入同
     * 一邮箱（串行次序由邮箱保证）；M 是 B/C 之后提交的完整管线空跑，
     * 证明调度器已处理完 B/C 的派发前缀（B/C 的查找→入队无挂起点，
     * 卸载尚未开始，不存在竞争方）。无 sleep。
     */
    auto held = SpawnInbound(app, KeyedEnvelope("hold", "hold", "hold-a", 1));
    BOOST_REQUIRE(hold_svc->entered->WaitFor(kWait));
    auto queued_b = SpawnInbound(app, KeyedEnvelope("hold", "touch", "touch-b", 1));
    auto queued_c = SpawnInbound(app, KeyedEnvelope("hold", "touch", "touch-c", 1));
    auto mark = SpawnInbound(app, KeyedEnvelope("hold", "touch", "touch-m", 2));
    BOOST_REQUIRE(mark->done.WaitFor(kWait));
    BOOST_REQUIRE(mark->out.has_value() && static_cast<bool>(*mark->out));
    BOOST_CHECK_EQUAL(hold_svc->touches->load(), 2);

    /**
     * 真实卸载入口：RemoveDispatch 摘分发表后，本调用停在「等在途执行
     * 租约归零」（OnStop 前序等待），期间旧记录由在途请求持有。
     */
    /**
     * 异步卸载闭包按值持有 App 与结果槽：用例若在卸载收口前失败退出，
     * 在途任务不写已销毁的测试栈，也不失去 App 所有权。
     */
    struct UninstallCall {
        std::optional<fw::result<void>> out;
        TestLatch done{1};
    };
    auto u = std::make_shared<UninstallCall>();
    bool u_submitted = false;
    g_scheduler->RegistCoroutineTask(
        [app, u] {
            u->out = bbt_co_await app->CoUninstallService<HoldSvc>();
            u->done.CountDown();
        },
        u_submitted);
    BOOST_REQUIRE(u_submitted);

    /**
     * 摘表完成的真实信号（不 sleep、不读内部状态）。
     */
    BOOST_REQUIRE_MESSAGE(ProbeUnregistered(app),
                          "dispatch removal was not observed");

    /**
     * 摘表后全新 key：新查找 NotFound（不是 Closed），不进 handler。
     */
    auto fresh = SpawnInbound(app, KeyedEnvelope("hold", "touch", "touch-fresh", 9));
    BOOST_REQUIRE(fresh->done.WaitFor(kWait));
    BOOST_REQUIRE(fresh->out.has_value() && !*fresh->out);
    BOOST_CHECK(fresh->out->error().code == fw::ErrorCode::NotFound);
    BOOST_TEST(fresh->out->error().message.find("unregistered service") !=
               std::string::npos);
    BOOST_CHECK_EQUAL(hold_svc->touches->load(), 2);

    /**
     * 放行 A：已进入的请求真实返回业务结果（阶段信号不抢占在途）。
     */
    hold_svc->release->Open();
    BOOST_REQUIRE(held->done.WaitFor(kWait));
    BOOST_REQUIRE(held->out.has_value() && static_cast<bool>(*held->out));

    /**
     * B/C：已持有记录的等待项由 drain 消费，执行租约被关闭门拒绝——
     * Closed（不是 NotFound），也不进业务。
     */
    BOOST_REQUIRE(queued_b->done.WaitFor(kWait));
    BOOST_REQUIRE(queued_b->out.has_value() && !*queued_b->out);
    BOOST_CHECK(queued_b->out->error().code == fw::ErrorCode::Closed);
    BOOST_TEST(queued_b->out->error().message.find("actor handler refused") !=
               std::string::npos);
    BOOST_REQUIRE(queued_c->done.WaitFor(kWait));
    BOOST_REQUIRE(queued_c->out.has_value() && !*queued_c->out);
    BOOST_CHECK(queued_c->out->error().code == fw::ErrorCode::Closed);
    BOOST_CHECK_EQUAL(hold_svc->touches->load(), 2);

    /**
     * 卸载完成：OnStop/OnUninstall 落在同一已安装实例上。
     */
    BOOST_REQUIRE(u->done.WaitFor(kWait));
    BOOST_REQUIRE(u->out.has_value() && static_cast<bool>(*u->out));
    BOOST_CHECK_EQUAL(hold_svc->stop_runs.load(), 1);
    BOOST_CHECK_EQUAL(hold_svc->uninstall_runs.load(), 1);
    BOOST_CHECK(hold_svc->stop_instance.load() == static_cast<const void*>(hold_svc));

    /**
     * 登记与安装描述同步摘除：重复卸载 NotFound；同名重装无 duplicate；
     * 重装的服务可再次干净卸载。
     */
    auto again = bbt_co_await app->CoUninstallService<HoldSvc>();
    BOOST_REQUIRE(!again);
    BOOST_CHECK(again.error().code == fw::ErrorCode::NotFound);
    r = bbt_co_await app->CoInstallService<HoldSvc>(ActorOpts());
    BOOST_REQUIRE(r);
    r = bbt_co_await app->CoUninstallService<HoldSvc>();
    BOOST_REQUIRE(r);

    app->RequestShutdown();
    r = bbt_co_await app->CoStop();
    BOOST_REQUIRE(r);
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace
