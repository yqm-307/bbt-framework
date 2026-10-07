// service-actor/v2 F1-b1 切片验证：CoApp 装配、run 前校验、生命周期
// 启动/关闭顺序、启动中途失败回退、关闭超时 ShutdownIncomplete、多宿主
// 拒绝。网络组件全部走 INetworkHost 桩（经 internal 测试缝注入）：断言
// 真实调用序列、每步发生时 coroutine runtime 已初始化、超时与异常路径；
// 无真实 socket、无 sleep 凑时序、无空断言。
// 自定义 main（BOOST_TEST_NO_MAIN）：若环境带 BBT_F1B1_DRAIN_CHILD 标志，则
// 本进程作为「干净 exec 的自有子进程」运行永久排空失败探针；否则走 Boost 套件。
#define BOOST_TEST_NO_MAIN
#include <boost/test/included/unit_test.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <string>
#include <string_view>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

extern char** environ;

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/object/CoObject.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>

// 机器面（宿主桩/生命周期/注册表/测试缝构造）显式走 internal 头。
#include <bbt/framework/internal/ActorRegistry.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/HostLifecycle.hpp>

namespace fw = bbt::framework;
namespace co = bbt::coroutine;
using Clock = std::chrono::steady_clock;

namespace {

// ── 测试屏障原语（与 F2-a 同一形态：std CV，无 sleep 凑时序）──

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
    std::mutex              m_mtx;
    std::condition_variable m_cv;
    bool                    m_open = false;
};

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
    void Wait() {
        std::unique_lock<std::mutex> lk(m_mtx);
        m_cv.wait(lk, [this] { return m_count == 0; });
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

constexpr std::chrono::milliseconds kWait{5000};

// ── INetworkHost 桩：记录每次调用的名字与当时 runtime 是否已初始化（初始化
//    在进程寿命内恒为 true）；借此把「调度器先于网络、使用期保持运行」纳入
//    同一序列断言。──

struct NetCall {
    std::string name;
    bool        sched_ready;   // 调用发生时 coroutine runtime 已初始化
};

class StubNetHost final : public fw::INetworkHost {
public:
    fw::result<void> create_rc = fw::result<void>::ok();
    fw::result<void> start_rc  = fw::result<void>::ok();
    std::function<fw::result<void>(co::Deadline)> on_wait_handlers;
    std::function<void()> on_started;   // Start 成功后回调（控制线程）
    std::function<void()> on_closed;    // Close() 调用时回调（控制线程）
    TestLatch start_seen{1};   // Start() 被调用（启动完成、开始接纳）时倒计时

    fw::result<void> Create() override {
        _Record("net.create");
        return create_rc;
    }
    fw::result<void> Start() override {
        _Record("net.start");
        start_seen.CountDown();
        auto rc = start_rc;
        if (rc && on_started) on_started();
        return rc;
    }
    void StopAccepting() noexcept override { _Record("net.stop_accepting"); }
    fw::result<void> WaitHandlersDone(co::Deadline d) override {
        _Record("net.wait_handlers");
        if (on_wait_handlers) return on_wait_handlers(d);
        return fw::result<void>::ok();
    }
    void Close() noexcept override {
        _Record("net.close");
        if (on_closed) on_closed();
    }

    std::vector<NetCall> Snapshot() const {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_calls;
    }
    std::vector<std::string> Names() const {
        std::lock_guard<std::mutex> lk(m_mtx);
        std::vector<std::string> out;
        for (const auto& c : m_calls) out.push_back(c.name);
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
    mutable std::mutex    m_mtx;
    std::vector<NetCall>  m_calls;
};

// ── 业务桩类型：统一 CoRpcReq/CoRpcResp 公共面。──

class EchoSvc final : public fw::CoService<EchoSvc> {
public:
    static constexpr std::string_view kServiceName = "echo";
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value) return fw::CoRpcResp::Error(value.error());
        return fw::CoRpcResp::From(value.value());
    }
    // 资源缝观察口（业务侧 protected context() 的测试出口）。
    template <class R>
    std::shared_ptr<R> Resource() { return context().resource<R>(); }
    template <class R>
    std::shared_ptr<R> Resource(std::string_view name) {
        return context().resource<R>(name);
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EchoSvc::Ping>("ping"));
};

// 与 EchoSvc 同名：重复注册拒绝用。
class EchoDupSvc final : public fw::CoService<EchoDupSvc> {
public:
    static constexpr std::string_view kServiceName = "echo";
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value) return fw::CoRpcResp::Error(value.error());
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EchoDupSvc::Ping>("ping"));
};

class KeyedSvc final : public fw::CoService<KeyedSvc> {
public:
    static constexpr std::string_view kServiceName = "keyed";
    fw::CoRpcResp Get(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value) return fw::CoRpcResp::Error(value.error());
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::ActorMethodAt<&KeyedSvc::Get, std::int32_t>("get"));
};

// ActorSerial 缺 key 提取器（声明为普通 Method）：启动期校验失败用。
class UnkeyedSvc final : public fw::CoService<UnkeyedSvc> {
public:
    static constexpr std::string_view kServiceName = "unkeyed";
    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto value = req.Parse<std::int32_t>();
        if (!value) return fw::CoRpcResp::Error(value.error());
        return fw::CoRpcResp::From(value.value());
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&UnkeyedSvc::Ping>("ping"));
};

// 资源缝验证类型（F0 已覆盖装配校验；此处验证运行期可取回）。
struct DummyResource { int n = 7; };

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

fw::CoAppOptions AppOpts(std::vector<fw::StaticRoute> routes = {}) {
    return fw::CoAppOptions{
        bbt::infra::NetworkLimits{
            /*max_connections*/ 64,
            /*max_inflight*/ 64,
            /*max_header_bytes*/ 8192,
            /*max_body_bytes*/ 65536,
            /*incoming_timeout*/ std::chrono::milliseconds{30000}},
        bbt::infra::ListenAddress{"127.0.0.1", 0},
        std::move(routes),
        /*shutdown_step_budget*/ std::chrono::milliseconds{2000}};
}

// 测试缝构造：宿主桩 + 无出站发送（受管出站如实 RuntimeUnavailable）。
std::unique_ptr<fw::CoApp> MakeApp(
    const fw::CoAppOptions& opts, const std::shared_ptr<StubNetHost>& host) {
    return fw::MakeCoAppForTest(opts, fw::CoAppSeam{host, {}});
}

// 在线程上跑 run()；返回的句柄不可移动（atomic 成员），以 unique_ptr 持有。
struct RunHandle {
    std::thread      th;
    std::atomic<bool> done{false};
    int              rc{-1};
};

std::unique_ptr<RunHandle> RunApp(fw::CoApp& app) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
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

// 早退安全：任何 BOOST_REQUIRE 失败都会展开栈，若 run 线程仍阻塞在闸门上
// 则闸门/latch 对象被销毁（悬垂）或 run 线程未 join（terminate）。按「先放行
// 闸门、再 join 线程」的析构顺序声明（后声明先析构）：先构造 join 守卫，再构造
// 放行守卫。正常路径末尾的显式放行/join 与守卫幂等。
struct RunJoinOnExit {
    std::unique_ptr<RunHandle>* h;
    ~RunJoinOnExit() { if (h != nullptr && *h) JoinRun(*h); }
};
struct GateOpenOnExit {
    TestGate* g;
    ~GateOpenOnExit() { if (g != nullptr) g->Open(); }
};

const std::vector<std::string> kCleanOrder{
    "net.create", "net.start", "net.stop_accepting", "net.wait_handlers",
    "net.close"};

// ── 永久排空失败的子进程验证设施 ──────────────────────────────────────
// fail-closed 语义下「永久排空失败」是设计上的永久 ShutdownIncomplete 保活：
// Run 永不返回、绝不 Close/release（见 HostLifecycle.hpp 头部说明）。该状态
// 无法在同进程内断言（run 不返回），故以「干净 exec」起本测试可执行文件的一个
// 自有子进程运行场景：posix_spawn（不起 fork 已有 worker 线程的进程），由环境
// 标志让子进程 main 直接走探针分支。证据只在「两次 WaitHandlersDone 均已返回
// 并落定」后才写出；父进程读证据后只终止自有子进程并 waitpid，以 WIFSIGNALED
// 证明 Run 确实未返回；若 Run 返回，子进程显式写 RETURNED 并以失败码退出，
// 绝不以无限 pause 掩盖。

enum class DrainFailure {
    NonTimeoutError,   // 初次与续等都返回非超时错误 → 永久排空失败
    TimeoutThenError,  // 初次 TimedOut、续等非超时错误 → 永久排空失败
};

std::string ModeName(DrainFailure m) {
    return m == DrainFailure::NonTimeoutError ? "nontimeout" : "timeoutthen";
}

void WriteFd(int fd, const std::string& s) {
    std::size_t off = 0;
    while (off < s.size()) {
        const ssize_t w = ::write(fd, s.data() + off, s.size() - off);
        if (w <= 0) return;
        off += static_cast<std::size_t>(w);
    }
}

// 子进程探针：直接驱动 HostLifecycle（internal 缝），同时观察 ShutdownIncomplete
// 状态、失败清单、未完成清理项，以及两个资源收束挂接点（on_handlers_drained =
// 资源 Close 挂接点、on_release = 对象回收）与网络 Close。
[[noreturn]] void RunDrainFailureChild(int fd, DrainFailure mode) {
    auto wait_calls = std::make_shared<std::atomic<int>>(0);
    auto host = std::make_shared<StubNetHost>();
    host->on_closed = [fd] { WriteFd(fd, "NET_CLOSE\n"); };
    host->on_wait_handlers =
        [mode, wait_calls](co::Deadline) -> fw::result<void> {
            const int n = wait_calls->fetch_add(1, std::memory_order_acq_rel) + 1;
            if (mode == DrainFailure::NonTimeoutError)
                return fw::result<void>::err(fw::MakeError(
                    fw::ErrorCode::InternalError, "permanent drain failure"));
            if (n == 1)
                return fw::result<void>::err(fw::MakeError(
                    fw::ErrorCode::TimedOut, "handlers still running"));
            return fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::InternalError, "drain failed after timeout"));
        };

    int nfds[2];   // 子进程内部：Run 返回时唤醒主线程（父进程不参与）
    if (::pipe(nfds) != 0) { WriteFd(fd, "PIPE_FAIL\n"); ::_exit(2); }

    fw::HostLifecycle::Hooks hooks;
    hooks.on_handlers_drained = [fd] { WriteFd(fd, "RESOURCE_CLOSE\n"); };
    hooks.on_release          = [fd] { WriteFd(fd, "RELEASE\n"); };

    auto lc = std::make_shared<fw::HostLifecycle>(
        host, std::chrono::milliseconds{2000}, nullptr);
    std::atomic<bool> run_done{false};
    std::thread run_th([lc, &hooks, &run_done, nfds] {
        (void)lc->Run(hooks);
        run_done.store(true, std::memory_order_release);
        const ssize_t w = ::write(nfds[1], "R", 1);   // 唤醒主线程：Run 已返回
        (void)w;
    });
    run_th.detach();

    if (!host->start_seen.WaitFor(kWait)) {
        WriteFd(fd, "START_TIMEOUT\n");
        ::_exit(2);
    }
    lc->RequestShutdown();

    const auto until = Clock::now() + kWait;
    for (;;) {
        const int waits = wait_calls->load(std::memory_order_acquire);
        const auto fails = lc->Failures();
        const auto pending = lc->IncompleteSteps();
        auto count = [&fails](const char* needle) {
            int c = 0;
            for (const auto& f : fails)
                if (f.find(needle) != std::string::npos) ++c;
            return c;
        };
        // 「两次 Wait 均已返回并落定」的可证条件：两次调用都发生过；且「续等
        // 返回之后」才写入的失败原文已出现（非超时错误在返回后记 failure；
        // TimeoutThenError 的第二次非超时错误亦在返回后才记）。
        const bool settled =
            waits >= 2 &&
            (mode == DrainFailure::NonTimeoutError
                 ? count("permanent drain failure") >= 2
                 : count("drain failed after timeout") >= 1);
        if (settled && !pending.empty() && pending[0] == "WaitHandlersDone" &&
            !run_done.load(std::memory_order_acquire)) {
            std::string line = "PARKED waits=" + std::to_string(waits) +
                               " pending=" + pending[0] + " failures=";
            for (const auto& f : fails) line += "[" + f + "]";
            WriteFd(fd, line + "\n");
            break;
        }
        if (run_done.load(std::memory_order_acquire)) {
            WriteFd(fd, "RETURNED\n");   // Run 返回 → 显式失败，绝不无限保活掩盖
            ::_exit(4);
        }
        if (Clock::now() >= until) { WriteFd(fd, "NOT_PARKED\n"); ::_exit(2); }
        std::this_thread::yield();
    }

    // 已落定于永久 ShutdownIncomplete：Run 若在落定后返回，会经 nfds 唤醒——
    // 事件等待、不忙等、不 sleep 刷绿；父进程在本事件上阻塞期间终止本子进程。
    pollfd p{nfds[0], POLLIN, 0};
    const int pr = ::poll(&p, 1, -1);
    WriteFd(fd, pr > 0 ? "RETURNED\n" : "POLL_ERR\n");
    ::_exit(4);
}

struct DrainChildEvidence {
    bool        parked{false};         // 子进程报告已落定于永久 ShutdownIncomplete
    bool        signal_killed{false};  // 子进程被信号终止（Run 未返回）
    std::string out;                   // 子进程写出的全部证据文本
};

// 从 environ 复制并追加/覆盖探针标志（不 setenv：不依赖全局环境可变性）。
struct SpawnEnv {
    std::vector<std::string> store;
    std::vector<char*>       ptrs;
    explicit SpawnEnv(const std::vector<std::string>& extra) {
        for (char** e = environ; e != nullptr && *e != nullptr; ++e) {
            const std::string_view s{*e};
            if (s.rfind("BBT_F1B1_DRAIN_", 0) == 0) continue;
            store.emplace_back(s);
        }
        for (const auto& x : extra) store.push_back(x);
        ptrs.reserve(store.size() + 1);
        for (auto& s : store) ptrs.push_back(s.data());
        ptrs.push_back(nullptr);
    }
    char* const* data() const { return ptrs.data(); }
};

// posix_spawn 本可执行文件 + 环境标志运行自有子进程；读到 PARKED 证据（含两次
// Wait 落定与失败原文）后只 SIGKILL 并 waitpid 该自有子进程，排空残留证据。
DrainChildEvidence ProbeDrainFailureChild(DrainFailure mode) {
    DrainChildEvidence ev;
    char exe[4096];
    const ssize_t n = ::readlink("/proc/self/exe", exe, sizeof(exe) - 1);
    if (n <= 0) return ev;
    exe[n] = '\0';

    int fds[2];
    if (::pipe(fds) != 0) return ev;
    const int ev_r = fds[0];
    const int ev_w = fds[1];

    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    // 顺序要紧：先关读端释放可能等于目标 3 的 fd，再把写端 dup 到固定 fd 3，
    // 最后关掉写端原 fd（避免 addclose 关掉刚建立的 fd 3）。
    posix_spawn_file_actions_addclose(&fa, ev_r);
    posix_spawn_file_actions_adddup2(&fa, ev_w, 3);
    if (ev_w != 3) posix_spawn_file_actions_addclose(&fa, ev_w);

    SpawnEnv env({std::string("BBT_F1B1_DRAIN_CHILD=") + ModeName(mode),
                  std::string("BBT_F1B1_DRAIN_EVFD=3")});
    char* argv[] = {exe, nullptr};

    pid_t pid = -1;
    const int sp = ::posix_spawn(&pid, exe, &fa, nullptr, argv, env.data());
    posix_spawn_file_actions_destroy(&fa);
    ::close(ev_w);
    if (sp != 0) { ::close(ev_r); return ev; }   // 未能起子进程 → parked=false

    char buf[512];
    const auto deadline = Clock::now() + kWait;
    bool stop = false;
    while (!stop) {
        const auto remain = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - Clock::now()).count();
        if (remain <= 0) break;
        pollfd pfd{ev_r, POLLIN, 0};
        if (::poll(&pfd, 1, static_cast<int>(remain)) <= 0) break;
        const ssize_t r = ::read(ev_r, buf, sizeof(buf));
        if (r <= 0) break;
        ev.out.append(buf, static_cast<std::size_t>(r));
        if (ev.out.find("PARKED") != std::string::npos) {
            const auto pos = ev.out.find("PARKED");
            if (ev.out.find('\n', pos) != std::string::npos) {
                ev.parked = true;   // 完整 PARKED 行（含 waits/pending/failures）已读全
                stop = true;
            }
        } else if (ev.out.find("NOT_PARKED") != std::string::npos ||
                   ev.out.find("RETURNED") != std::string::npos ||
                   ev.out.find("START_TIMEOUT") != std::string::npos ||
                   ev.out.find("PIPE_FAIL") != std::string::npos) {
            stop = true;
        }
    }
    (void)::kill(pid, SIGKILL);   // 只终止自有子进程；不触碰他人进程
    int status = 0;
    (void)::waitpid(pid, &status, 0);
    ev.signal_killed = WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
    for (;;) {   // 排空子进程可能残留的证据（含任何 NET_CLOSE/RESOURCE_CLOSE/RELEASE）
        const ssize_t r = ::read(ev_r, buf, sizeof(buf));
        if (r <= 0) break;
        ev.out.append(buf, static_cast<std::size_t>(r));
    }
    ::close(ev_r);
    return ev;
}

BOOST_AUTO_TEST_SUITE(framework_f1b1)

// 用例 0：永久排空失败 → fail-closed 保活。WaitHandlersDone 初次/续等返回错误
// （排空未被证实）时，状态机绝不进入资源 Close 挂接点、绝不网络 Close、绝不
// 释放 owner，Run 永不返回（保持 ShutdownIncomplete）；原始排空错误保留可观察。
// 场景在「干净 exec 的自有子进程」中运行（posix_spawn，不在已有多线程的父进程
// 内 fork），因此不依赖套件声明顺序；证据在两次 Wait 均已返回落定后才写出。
BOOST_AUTO_TEST_CASE(drain_failure_fails_closed_never_closes_child) {
    for (const DrainFailure mode :
         {DrainFailure::NonTimeoutError, DrainFailure::TimeoutThenError}) {
        const DrainChildEvidence ev = ProbeDrainFailureChild(mode);
        BOOST_TEST_MESSAGE("drain child evidence(mode="
            << static_cast<int>(mode) << "): [" << ev.out << "]");
        BOOST_CHECK_MESSAGE(ev.parked,
            "子进程未落定于永久 ShutdownIncomplete 保活态: [" << ev.out << "]");
        // Run 从未返回：子进程落定后阻塞在事件上，被父进程信号终止。
        BOOST_CHECK_MESSAGE(ev.signal_killed,
            "子进程不是被信号终止，Run 可能已返回（fail-closed 破坏）");
        // 两次 WaitHandlersDone 均已返回并落定（waits=2 + 返回后才写入的失败原文）。
        BOOST_CHECK_MESSAGE(ev.out.find("waits=2") != std::string::npos,
            "未证实两次 WaitHandlersDone 均已返回: [" << ev.out << "]");
        // 未完成清理项仍在（ShutdownIncomplete 未收束）。
        BOOST_CHECK_MESSAGE(ev.out.find("pending=WaitHandlersDone") !=
                            std::string::npos,
            "未完成清理项未保留可观察: [" << ev.out << "]");
        // 排空未证实 → 资源 Close 挂接点/网络 Close/对象回收三者均不得触发。
        for (const char* marker : {"NET_CLOSE", "RESOURCE_CLOSE", "RELEASE"}) {
            BOOST_CHECK_MESSAGE(ev.out.find(marker) == std::string::npos,
                "排空未证实却触发了收束挂接点 " << marker << ": ["
                << ev.out << "]");
        }
        BOOST_CHECK_MESSAGE(ev.out.find("RETURNED") == std::string::npos,
            "Run 返回（fail-closed 破坏）: [" << ev.out << "]");
        // 原始排空错误保留可观察（第二路径确为「先 TimedOut 后非超时错误」）。
        const char* expect = (mode == DrainFailure::NonTimeoutError)
            ? "permanent drain failure" : "drain failed after timeout";
        BOOST_CHECK_MESSAGE(ev.out.find(expect) != std::string::npos,
            "永久排空失败的原始错误未保留可观察: [" << ev.out << "]");
    }
}

// 用例 0b：非超时排空错误 + 迟到收尾恢复——初次返回非超时错误（未证实排空）
// 不得当作已排空：进入 ShutdownIncomplete、保留 owner、不 Close；续等迟到排空
// 完成后才收束，因存在硬性失败项返回 kExitShutdownFailed。原错误保留可观察。
BOOST_AUTO_TEST_CASE(drain_error_enters_incomplete_then_recovers) {
    auto host = std::make_shared<StubNetHost>();
    TestLatch persist_entered{1};   // 已进入无界续等
    TestGate  allow_drain;          // 迟到收尾放行
    std::atomic<int> calls{0};
    host->on_wait_handlers =
        [&](co::Deadline) -> fw::result<void> {
            if (calls.fetch_add(1, std::memory_order_acq_rel) == 0)
                return fw::result<void>::err(fw::MakeError(
                    fw::ErrorCode::InternalError, "drain probe failed"));
            persist_entered.CountDown();
            allow_drain.Wait();
            return fw::result<void>::ok();
        };
    auto app = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app->add_service<EchoSvc>(ConcurrentOpts()));

    auto h = RunApp(*app);
    // 早退安全：任何 BOOST_REQUIRE 失败都先放行闸门、再 join run 线程（具体顺序
    // 见 RunJoinOnExit/GateOpenOnExit 说明），避免断言展开使闸门栈悬垂或线程死锁。
    RunJoinOnExit  join_guard{&h};
    GateOpenOnExit release_guard{&allow_drain};
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));
    app->request_shutdown();
    BOOST_REQUIRE(persist_entered.WaitFor(kWait));

    // 未证实排空：ShutdownIncomplete、pending=WaitHandlersDone、run 不返回。
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::ShutdownIncomplete);
    const auto pending = app->pending_cleanup();
    BOOST_REQUIRE(pending.size() == 1);
    BOOST_TEST(pending[0] == "WaitHandlersDone");
    BOOST_TEST(!h->done.load());
    const auto failures = app->lifecycle_failures();
    BOOST_REQUIRE(!failures.empty());
    BOOST_CHECK_MESSAGE(
        failures[0].find("drain probe failed") != std::string::npos,
        "非超时排空错误未保留可观察: " << failures[0]);
    {   // 未提前网络 Close（资源 Close/on_release 亦未发生）
        const auto names = host->Names();
        BOOST_CHECK(std::find(names.begin(), names.end(), "net.close") ==
                    names.end());
    }

    // 迟到排空完成 → 走完收束；存在硬性失败项 → kExitShutdownFailed。
    allow_drain.Open();
    JoinRun(h);
    BOOST_TEST(h->rc == fw::HostLifecycle::kExitShutdownFailed);
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Closed);
    BOOST_TEST(app->pending_cleanup().empty());
    const auto after = host->Names();
    BOOST_CHECK(std::find(after.begin(), after.end(), "net.close") !=
                after.end());
}

// 用例 1：add_service 注册成功；同服务名重复注册被明确拒绝；服务身份在
// 启动期经 _bind_runtime 绑定（id/generation != 0）；资源缝在受管实例上
// 可取回已装配对象。
BOOST_AUTO_TEST_CASE(add_service_registers_and_rejects_duplicate) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);

    BOOST_REQUIRE(app->add_service<EchoSvc>(ConcurrentOpts()));
    BOOST_REQUIRE(app->add_service<KeyedSvc>(ActorOpts()));
    auto res = std::make_shared<DummyResource>();
    auto primary = std::make_shared<DummyResource>();
    primary->n = 11;
    auto replica = std::make_shared<DummyResource>();
    replica->n = 22;
    BOOST_REQUIRE(app->add_resource(res));
    BOOST_REQUIRE(app->add_resource<DummyResource>("primary", primary));
    BOOST_REQUIRE(app->add_resource<DummyResource>("replica", replica));

    auto dup = app->add_service<EchoDupSvc>(ConcurrentOpts());
    BOOST_REQUIRE(!dup);
    BOOST_CHECK(dup.error().code == fw::ErrorCode::InvalidArgument);
    BOOST_TEST(dup.error().message.find("duplicate") != std::string::npos);

    auto h = RunApp(*app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

    // Concurrent：启动期绑定的单实例可读回对象身份。
    auto svc = app->find_service("echo");
    BOOST_REQUIRE(svc);
    BOOST_TEST(svc.value()->GetObjectInfo().id != 0);
    BOOST_TEST(svc.value()->GetObjectInfo().kind == "service");
    BOOST_TEST(svc.value()->GetObjectInfo().name == "echo");

    // 资源缝正路径：装配进宿主的资源经服务上下文可取回同一对象。
    auto* echo = static_cast<EchoSvc*>(svc.value().get());
    auto got = echo->Resource<DummyResource>();
    BOOST_REQUIRE(got != nullptr);
    BOOST_TEST(got.get() == res.get());
    BOOST_TEST(got->n == 7);
    auto got_primary = echo->Resource<DummyResource>("primary");
    auto got_replica = echo->Resource<DummyResource>("replica");
    BOOST_REQUIRE(got_primary != nullptr);
    BOOST_REQUIRE(got_replica != nullptr);
    BOOST_TEST(got_primary.get() == primary.get());
    BOOST_TEST(got_replica.get() == replica.get());
    BOOST_TEST(got_primary->n == 11);
    BOOST_TEST(got_replica->n == 22);
    BOOST_CHECK(echo->Resource<DummyResource>("missing") == nullptr);

    // ActorSerial：注册表激活时绑定身份 + actor key。
    auto* reg = app->actor_registry();
    BOOST_REQUIRE(reg != nullptr);
    auto actor = reg->GetOrCreate("keyed", "k-1");
    BOOST_REQUIRE(actor);
    BOOST_TEST(actor.value()->GetObjectInfo().id != 0);
    BOOST_REQUIRE(actor.value()->co_actor_key().has_value());
    BOOST_TEST(actor.value()->co_actor_key().value() == "k-1");

    // 未注册名/按 key 服务走 find_service 的明确错误。
    BOOST_CHECK(!app->find_service("no-such"));
    BOOST_CHECK(!app->find_service("keyed"));

    app->request_shutdown();
    JoinRun(h);
    BOOST_TEST(h->rc == fw::HostLifecycle::kExitOk);
}

// 用例 2：ServiceOptions 违反 F2-a 校验 → add_service 失败，任何组件未
// 启动（桩计数 0）；run 自身配置非法同样 run 前失败。
BOOST_AUTO_TEST_CASE(invalid_options_fail_before_any_startup) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);

    {   // max_inflight == 0
        auto o = ConcurrentOpts(); o.max_inflight = 0;
        auto r = app->add_service<EchoSvc>(o);
        BOOST_REQUIRE(!r);
        BOOST_CHECK(r.error().code == fw::ErrorCode::InvalidArgument);
    }
    {   // ActorSerial 缺 mailbox_capacity
        auto o = ActorOpts(); o.mailbox_capacity = 0;
        auto r = app->add_service<KeyedSvc>(o);
        BOOST_REQUIRE(!r);
        BOOST_CHECK(r.error().code == fw::ErrorCode::InvalidArgument);
    }
    {   // ActorSerial 服务存在无 key 提取器方法 → 装配期失败
        auto r = app->add_service<UnkeyedSvc>(ActorOpts());
        BOOST_REQUIRE(!r);
        BOOST_CHECK(r.error().code == fw::ErrorCode::InvalidArgument);
        BOOST_TEST(r.error().message.find("ping") != std::string::npos);
    }

    // 注册失败不登记：同名/同服务仍可重新合法注册。
    BOOST_REQUIRE(app->add_service<EchoSvc>(ConcurrentOpts()));

    // run 自身校验失败（network_limits 非法）→ 不启动任何组件。
    auto host2 = std::make_shared<StubNetHost>();
    auto bad_opts = AppOpts();
    bad_opts.network_limits.max_inflight = 0;
    auto app2 = MakeApp(bad_opts, host2);
    BOOST_CHECK(app2->run() == fw::HostLifecycle::kExitRejected);
    BOOST_TEST(host2->TotalCalls() == std::size_t{0});
    BOOST_TEST(app2->lifecycle_failures().size() == std::size_t{1});
}

// 用例 3：未显式配置的出站目标被拒绝——不存在「自动连任意服务」；
// 拒绝发生在任何网络动作之前（桩计数 0）。
BOOST_AUTO_TEST_CASE(unconfigured_route_target_rejected) {
    auto host = std::make_shared<StubNetHost>();
    fw::CoAppOptions opts = AppOpts({
        fw::StaticRoute{"svc.allowed",
                        bbt::infra::RpcAddress{"tcp", "127.0.0.1:9001"}}});
    auto app = MakeApp(opts, host);

    auto bad = app->find_route("svc.unknown");
    BOOST_REQUIRE(!bad);
    BOOST_CHECK(bad.error().code == fw::ErrorCode::NotFound);
    BOOST_TEST(bad.error().message.find("svc.unknown") != std::string::npos);

    auto good = app->find_route("svc.allowed");
    BOOST_REQUIRE(good);
    BOOST_TEST(good.value().transport == "tcp");
    BOOST_TEST(good.value().endpoint == "127.0.0.1:9001");

    // 拒绝发生在网络组件启动之前：桩一次都未被调用。
    BOOST_TEST(host->TotalCalls() == std::size_t{0});
}

// 用例 4：启动顺序 = runtime 初始化 → 网络 Create/Start（开始接纳）→ 运行；
// 桩调用点记录「runtime 已初始化」证明调度器先于网络、且整个使用期保持运行。
BOOST_AUTO_TEST_CASE(startup_order_scheduler_then_network) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app->add_service<EchoSvc>(ConcurrentOpts()));

    auto h = RunApp(*app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Running);

    app->request_shutdown();
    JoinRun(h);
    BOOST_TEST(h->rc == fw::HostLifecycle::kExitOk);

    const auto calls = host->Snapshot();
    std::vector<std::string> names;
    for (const auto& c : calls) names.push_back(c.name);
    BOOST_TEST(names == kCleanOrder);
    // 全部网络调用都发生在 runtime 已初始化之后（初始化先于 net.create，
    // 且进程寿命内不复位）。
    for (const auto& c : calls)
        BOOST_TEST(c.sched_ready);
}

// 用例 5：关闭顺序 = StopAccepting → 等 handler 结束 → Close；StopAccepting
// 严格先于 Close。
BOOST_AUTO_TEST_CASE(shutdown_order_stop_accepting_before_close) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app->add_service<EchoSvc>(ConcurrentOpts()));

    auto h = RunApp(*app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));
    app->request_shutdown();
    JoinRun(h);
    BOOST_TEST(h->rc == fw::HostLifecycle::kExitOk);

    const auto calls = host->Snapshot();
    auto pos = [&calls](const char* n) -> std::size_t {
        for (std::size_t i = 0; i < calls.size(); ++i)
            if (calls[i].name == n) return i;
        return calls.size();
    };
    BOOST_TEST(pos("net.stop_accepting") < pos("net.wait_handlers"));
    BOOST_TEST(pos("net.wait_handlers") < pos("net.close"));
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Closed);
}

// 用例 6：启动中途失败——Create 失败只回退（网络关闭序列不适用）；Start
// 失败按同一固定关闭顺序回退已立起的网络组件；不留半启动状态（run 返回）。
BOOST_AUTO_TEST_CASE(startup_failure_rolls_back_in_order) {
    {   // 失败在第 1 个网络步（Create）
        auto host = std::make_shared<StubNetHost>();
        host->create_rc = fw::result<void>::err(
            fw::MakeError(fw::ErrorCode::InternalError, "create boom"));
        auto app = MakeApp(AppOpts(), host);
        BOOST_REQUIRE(app->add_service<EchoSvc>(ConcurrentOpts()));

        BOOST_TEST(app->run() == fw::HostLifecycle::kExitStartFailed);
        // Create 未成功 → 网络关闭序列不适用。
        BOOST_TEST(host->Names() == std::vector<std::string>{"net.create"});
        BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Closed);
        BOOST_TEST(!app->lifecycle_failures().empty());
    }
    {   // 失败在第 2 个网络步（Start）：Create 已立起 → 全序列回退
        auto host = std::make_shared<StubNetHost>();
        host->start_rc = fw::result<void>::err(
            fw::MakeError(fw::ErrorCode::Unavailable, "start boom"));
        auto app = MakeApp(AppOpts(), host);
        BOOST_REQUIRE(app->add_service<EchoSvc>(ConcurrentOpts()));

        BOOST_TEST(app->run() == fw::HostLifecycle::kExitStartFailed);
        const auto calls = host->Snapshot();
        std::vector<std::string> names;
        for (const auto& c : calls) names.push_back(c.name);
        // create/start 失败步 + 同一固定顺序的回退序列。
        BOOST_TEST(names == std::vector<std::string>({
            "net.create", "net.start", "net.stop_accepting",
            "net.wait_handlers", "net.close"}));
        for (const auto& c : calls)
            BOOST_TEST(c.sched_ready);   // 回退期间 runtime 仍在驱动
    }
}

// 用例 7：关闭等待超时——桩让 handler 在预算内不结束：进入
// ShutdownIncomplete、run 不返回、不伪装成功；迟到收尾完成后 run 返回
// 非零（kExitShutdownLate），进程不退出、无 _Exit/abort。
BOOST_AUTO_TEST_CASE(shutdown_timeout_reports_incomplete_not_success) {
    auto host = std::make_shared<StubNetHost>();
    TestLatch waiting_unbounded{1};   // 第二次（无界）等待已进入
    TestGate  allow_drain;            // 迟到收尾放行
    std::atomic<int> drain_calls{0};

    host->on_wait_handlers =
        [&](co::Deadline) -> fw::result<void> {
            if (drain_calls.fetch_add(1) == 0) {
                // 预算内不结束 → 明确超时（不用 sleep：直接报 TimedOut，
                // 由状态机决定后续）。
                return fw::result<void>::err(fw::MakeError(
                    fw::ErrorCode::TimedOut, "handlers still running"));
            }
            // 预算耗尽后的无界续等：挂到测试放行——这就是「未完成的清理」
            // 仍在被等待的实据。
            waiting_unbounded.CountDown();
            allow_drain.Wait();
            return fw::result<void>::ok();
        };

    auto app = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app->add_service<EchoSvc>(ConcurrentOpts()));

    auto h = RunApp(*app);
    // 早退安全：任何 BOOST_REQUIRE 失败都先放行闸门、再 join run 线程。
    RunJoinOnExit  join_guard{&h};
    GateOpenOnExit release_guard{&allow_drain};
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));
    app->request_shutdown();

    // 等状态机进入无界续等：此时可断言未完成清理的真实状态。
    BOOST_REQUIRE(waiting_unbounded.WaitFor(kWait));
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::ShutdownIncomplete);
    const auto pending = app->pending_cleanup();
    BOOST_REQUIRE(pending.size() == 1);
    BOOST_TEST(pending[0] == "WaitHandlersDone");
    // run 不返回（栈上 CoApp 不被析构）；StopAccepting 已在超时前发生。
    BOOST_TEST(!h->done.load());
    const auto names = host->Names();
    BOOST_CHECK(std::find(names.begin(), names.end(),
                          "net.stop_accepting") != names.end());
    BOOST_CHECK(std::find(names.begin(), names.end(),
                          "net.close") == names.end());   // 未提前关闭

    // 迟到收尾完成：完整走完后序步骤，run 返回非零（曾超预算）。
    allow_drain.Open();
    JoinRun(h);
    BOOST_TEST(h->rc == fw::HostLifecycle::kExitShutdownLate);
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Closed);
    BOOST_TEST(app->pending_cleanup().empty());
    const auto after = host->Names();
    BOOST_CHECK(std::find(after.begin(), after.end(),
                          "net.close") != after.end());
}

// 用例 8：多 CoApp——另一宿主活跃时第二个 run() 被明确拒绝；同一实例
// 重复 run 同样拒绝；完整收束后新实例可顺序复用单例调度器。
// （契约未冻结多实例口径 → 本实现口径，报告待澄清。）
BOOST_AUTO_TEST_CASE(second_active_app_run_rejected) {
    auto host1 = std::make_shared<StubNetHost>();
    auto app1 = MakeApp(AppOpts(), host1);
    BOOST_REQUIRE(app1->add_service<EchoSvc>(ConcurrentOpts()));

    auto h1 = RunApp(*app1);
    BOOST_REQUIRE(host1->start_seen.WaitFor(kWait));

    // 第二个活跃 run() 被拒；其网络组件一次未被调用。
    auto host2 = std::make_shared<StubNetHost>();
    auto app2 = MakeApp(AppOpts(), host2);
    BOOST_TEST(app2->run() == fw::HostLifecycle::kExitRejected);
    BOOST_TEST(host2->TotalCalls() == std::size_t{0});
    BOOST_CHECK(app2->shutdown_state() == fw::ShutdownState::Running);

    app1->request_shutdown();
    JoinRun(h1);
    BOOST_TEST(h1->rc == fw::HostLifecycle::kExitOk);

    // 同一实例不可重入 run()。
    BOOST_TEST(app1->run() == fw::HostLifecycle::kExitRejected);

    // 顺序复用单例调度器：收束后新实例可正常 run。
    auto host3 = std::make_shared<StubNetHost>();
    auto app3 = MakeApp(AppOpts(), host3);
    BOOST_REQUIRE(app3->add_service<EchoSvc>(ConcurrentOpts()));
    auto h3 = RunApp(*app3);
    BOOST_REQUIRE(host3->start_seen.WaitFor(kWait));
    app3->request_shutdown();
    JoinRun(h3);
    BOOST_TEST(h3->rc == fw::HostLifecycle::kExitOk);
}

// 补充：早于 run 的 request_shutdown → 完成启动后立即进入关闭序列。
BOOST_AUTO_TEST_CASE(shutdown_requested_before_run_closes_immediately) {
    auto host = std::make_shared<StubNetHost>();
    auto app = MakeApp(AppOpts(), host);
    BOOST_REQUIRE(app->add_service<EchoSvc>(ConcurrentOpts()));
    app->request_shutdown();
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Closing);

    BOOST_TEST(app->run() == fw::HostLifecycle::kExitOk);
    const auto names = host->Names();
    BOOST_TEST(names == kCleanOrder);
    BOOST_CHECK(app->shutdown_state() == fw::ShutdownState::Closed);
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace

// 自定义入口（BOOST_TEST_NO_MAIN）：探针标志存在时本进程作为排空失败子进程，
// 否则交 Boost 运行套件。探针在 exec 出的干净进程中运行，不与父进程共享线程状态。
boost::unit_test::test_suite* init_unit_test(int, char**) { return nullptr; }

int main(int argc, char* argv[]) {
    const char* mode = std::getenv("BBT_F1B1_DRAIN_CHILD");
    const char* evfd = std::getenv("BBT_F1B1_DRAIN_EVFD");
    if (mode != nullptr && evfd != nullptr) {
        RunDrainFailureChild(std::atoi(evfd),
            std::string(mode) == "nontimeout"
                ? DrainFailure::NonTimeoutError
                : DrainFailure::TimeoutThenError);
    }
    return ::boost::unit_test::unit_test_main(&init_unit_test, argc, argv);
}
