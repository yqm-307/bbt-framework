// bbt-framework 公开出站策略 fixture。
//
// V2 会话拒绝 self-connection，因此本 fixture 由 caller 进程派生一个互异
// identity 的 callee 进程：
//
//   caller 进程（local_identity=rpc-policy-caller）
//     v2::CoApp 宿主 PolicyCallerService；其 handler 内经受保护的
//     `this->call<Req,Resp>` 出站，出站目标解析成功后、真实 socket 发送之前
//     由框架 AppRuntimeState::Send 调用公开策略 Preflight
//   callee 进程（local_identity=rpc-policy-callee）
//     v2::CoApp 宿主 PolicyCalleeService；入站经真实 V2 会话分发
//
// 只用公开面：`bbt/framework/V2.hpp`、`bbt/framework/RpcPolicy.hpp`、
// `RequestContext.hpp` + getvalue 的 proto/codec。**不** include internal/、
// **不**用 CoAppSeam/MakeCoAppForTest/DispatchInboundForTest/INetworkHost 桩/
// 发送注入——编译期即证明「不依赖 internal 测试缝」。
//
// callee 在自己的 handler 内读公开 `RequestContext::deadline` 并 journal 可见
// 剩余预算，作为「期限只缩不放」的真实 oracle（不依赖 reply envelope 预算，
// 后者只反映 caller 自己的入站预算）。
//
// 参数（生成式入口 `bbtco_main` 不暴露 argv，参数经环境变量传入）：
//   BBT_EXAMPLE_MODE=<mode> BBT_EXAMPLE_PORT_FILE / BBT_EXAMPLE_META_FILE /
//   BBT_EXAMPLE_JOURNAL_FILE
//   mode ∈ allow | reject | tighten | widen | expired | decorator
//   - allow     ：策略允许（只计数）
//   - reject    ：策略拒绝（Cancelled）：AppRuntimeState::Send 在发送之前返回错误
//   - tighten   ：策略收紧出站期限到 now+300ms（应传播到 callee 可见预算）
//   - widen     ：策略返回「放大」期限 now+3600s（框架必须忽略）
//   - expired   ：策略收紧到 now-1ms（框架须在 I/O 前 TimedOut）
//   - decorator ：外层策略包内层策略，两个虚 Preflight 各调一次、顺序固定
//   内部模式 BBT_EXAMPLE_MODE=callee（由 caller 派生，不对外使用）。
//
// 主内容在业务主协程里：装配 → CoStart → 写就绪文件 → 等关闭触发 → CoStop →
// 关闭后探针。入口是 runtime 的生成式 `bbtco_main`，不写 native main、不启动/
// 停止 scheduler、不建 runner 线程、不 join；共享 Engine 的唯一一次
// Engine::Run 由 bbtco_main_host_hook 执行。
//
// journal 事件：preflight name=<n> seq=<k> / callee_handler_entered
//   deadline_ms=<n> / listener_probe phase=after_run_before_exit port=<n>
//   result=refused|connected|error errno=<n> / resource_close /
//   shutdown rc=.. state=.. owner_closed=.. / callee_shutdown rc=..
//
// 关闭后验收（B1）：`CoStop()` 返回、主协程仍存活、进程与 `v2::CoApp` 对象均仍
// 存在时，由本进程对自己的 bound 端口做一次**有界** nonblocking connect 并入
// journal。只有 ECONNREFUSED 记 result=refused（唯一判绿值）；超时或其它 errno
// 记 result=error（不算绿）。该探针只证明「存活期 listener 已关」——它不证明
// 关闭期零额外 I/O，也不替代在途 I/O 排空的判定。
//
// 策略由 AppOptions::SetRpcPolicy 在构造期装配；默认空值不改变出站行为。

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/infra/CloseOptions.hpp>

#include <bbt/framework/Component.hpp>
#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/HostGate.hpp>
#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/RpcPolicy.hpp>
#include <bbt/framework/V2.hpp>

#include "getvalue_service.hpp" // ProtoCodec<GetValueRequest/Response>（公开面）

namespace fw = bbt::framework;
namespace v2 = bbt::framework::v2;
namespace inf = bbt::infra;

namespace {

constexpr const char* kAuthState = "unauthenticated_loopback";
constexpr const char* kCallerIdentity = "rpc-policy-caller";
constexpr const char* kCalleeIdentity = "rpc-policy-callee";

std::mutex g_journal_mtx;
std::ofstream g_journal;

void Journal(const std::string& line) {
    std::lock_guard<std::mutex> lk(g_journal_mtx);
    // 单次 write（含换行）+ O_APPEND：两个进程的整行不会互相穿插/覆盖。
    const std::string buf = line + '\n';
    g_journal.write(buf.data(), static_cast<std::streamsize>(buf.size()));
    g_journal.flush();
}

class CloseJournalComponent final : public v2::CoComponent {
public:
    struct Options {};
    static std::atomic_bool stopped;

    explicit CloseJournalComponent(fw::detail::ConstructKey k, Options)
        : v2::CoComponent(k) {}

protected:
    ~CloseJournalComponent() noexcept override = default;

    fw::result<void> OnStop(const inf::CloseOptions&) override {
        if (!stopped.exchange(true))
            Journal(std::string("event=resource_close auth_state=") +
                    kAuthState);
        return fw::result<void>::ok();
    }
    friend class fw::detail::Registry;
};
std::atomic_bool CloseJournalComponent::stopped{false};

// —— 业务（公开面）：caller 出站 + callee 观测 ——
//
// callee：journal handler 进入与**公开可见**的 handler 期限剩余（RequestContext
// ::deadline - now），作为「期限只缩不放」的真实 oracle。
class PolicyCalleeService final : public v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "bbt.example.v1.PolicyCallee";

    explicit PolicyCalleeService(fw::detail::ConstructKey k)
        : v2::CoService<>(k) {}

    fw::CoRpcResp GetValue(fw::CoRpcReq req) {
        long long remaining_ms = -1;
        std::string request_id;
        if (auto ctx = fw::CurrentRequestContext()) {
            request_id = ctx.value()->request_id;
            remaining_ms = std::chrono::duration_cast<std::chrono::milliseconds>(ctx.value()->deadline - std::chrono::steady_clock::now())
                               .count();
        }
        Journal("event=callee_handler_entered req=" + request_id + " deadline_ms=" + std::to_string(remaining_ms) + " auth_state=" + kAuthState);

        auto parsed = req.ParseProto<bbt::example::v1::GetValueRequest>();
        if (!parsed)
            return fw::CoRpcResp::Error(parsed.error());
        bbt::example::v1::GetValueResponse out;
        out.set_found(true);
        out.set_value(parsed.value().key());
        return fw::CoRpcResp::FromProto(out);
    }

    static constexpr auto kRpcMethods =
        fw::RpcMethods(fw::ProtoMethod<&PolicyCalleeService::GetValue, bbt::example::v1::GetValueRequest,
                                       bbt::example::v1::GetValueResponse>("GetValue"));

protected:
    ~PolicyCalleeService() noexcept override = default;
    friend class fw::detail::Registry;
};

// caller：经公开 typed 出站 `this->call<Req,Resp>` 转发到 callee（静态路由）或
// 未配置路由目标（路由门拒绝，策略不被询问）。
class PolicyCallerService final : public v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "bbt.example.v1.PolicyCaller";
    static constexpr std::string_view kCalleeService = "bbt.example.v1.PolicyCallee";
    static constexpr std::string_view kNoRouteService = "bbt.example.v1.PolicyUnrouted";

    explicit PolicyCallerService(fw::detail::ConstructKey k)
        : v2::CoService<>(k) {}

    fw::CoRpcResp Forward(fw::CoRpcReq req) {
        return _Forward(std::move(req), kCalleeService);
    }

    fw::CoRpcResp ForwardNoRoute(fw::CoRpcReq req) {
        return _Forward(std::move(req), kNoRouteService);
    }

    static constexpr auto kRpcMethods =
        fw::RpcMethods(fw::ProtoMethod<&PolicyCallerService::Forward, bbt::example::v1::GetValueRequest,
                                       bbt::example::v1::GetValueResponse>("Forward"),
                       fw::ProtoMethod<&PolicyCallerService::ForwardNoRoute, bbt::example::v1::GetValueRequest,
                                       bbt::example::v1::GetValueResponse>("ForwardNoRoute"));

protected:
    ~PolicyCallerService() noexcept override = default;
    friend class fw::detail::Registry;

private:
    fw::CoRpcResp _Forward(fw::CoRpcReq req, std::string_view target) {
        auto parsed = req.ParseProto<bbt::example::v1::GetValueRequest>();
        if (!parsed)
            return fw::CoRpcResp::Error(parsed.error());
        auto reply = this->call<bbt::example::v1::GetValueRequest, bbt::example::v1::GetValueResponse>(
            target, "GetValue", parsed.value(), {}, fw::CallOptions{});
        if (!reply)
            return fw::CoRpcResp::Error(reply.error());
        return fw::CoRpcResp::FromProto(reply.value());
    }
};

// —— B1 关闭后存活期 listener 探针（本段与负例替身共用同一份源码文本；
//    scratch 负例按 BEGIN/END 标记抽取） BEGIN ——
enum class ListenerProbeResult {
    kRefused,
    kConnected,
    kError
};

// 有界 nonblocking connect：只有 ECONNREFUSED 判「listener 已关」；connect
// 超时（poll 超时）或任何其它 errno 一律 kError，绝不判绿。
ListenerProbeResult ProbeListenerBoundPort(std::uint16_t port, int* out_errno) {
    *out_errno = 0;
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        *out_errno = errno;
        return ListenerProbeResult::kError;
    }
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        *out_errno = errno;
        ::close(fd);
        return ListenerProbeResult::kError;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);

    ListenerProbeResult result = ListenerProbeResult::kError;
    const int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (rc == 0) {
        result = ListenerProbeResult::kConnected; // 仍在接受连接
    } else if (errno == EINPROGRESS || errno == EALREADY) {
        pollfd pfd{};
        pfd.fd = fd;
        pfd.events = POLLOUT;
        const int n = ::poll(&pfd, 1, 2000); // 有界：最多 2s
        if (n == 1) {
            int soerr = 0;
            socklen_t slen = sizeof(soerr);
            if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &slen) != 0) {
                *out_errno = errno;
            } else if (soerr == 0) {
                result = ListenerProbeResult::kConnected;
            } else {
                *out_errno = soerr;
                if (soerr == ECONNREFUSED)
                    result = ListenerProbeResult::kRefused;
            }
        } else if (n < 0) {
            *out_errno = errno;
        } // n == 0：超时 → kError
    } else if (errno == ECONNREFUSED) {
        *out_errno = errno;
        result = ListenerProbeResult::kRefused;
    } else {
        *out_errno = errno;
    }
    ::close(fd);
    return result;
}

const char* ProbeResultName(ListenerProbeResult r) {
    switch (r) {
    case ListenerProbeResult::kRefused:
        return "refused";
    case ListenerProbeResult::kConnected:
        return "connected";
    case ListenerProbeResult::kError:
        return "error";
    }
    return "error";
}
// —— B1 关闭后存活期 listener 探针 END ——

fw::ServiceOptions ConcurrentOpts() {
    return fw::ServiceOptions{}.SetMaxInflight(64);
}

fw::AppOptions BaseOptions(std::string identity) {
    fw::AppNetworkOptions network;
    network.SetLimits(inf::NetworkLimits{
                          /*max_connections*/ 64, /*max_inflight*/ 64,
                          /*max_header_bytes*/ 16 * 1024,
                          /*max_body_bytes*/ 64 * 1024,
                          /*incoming_timeout*/
                          std::chrono::milliseconds{30000}})
        .SetListenAddress(inf::ListenAddress{"127.0.0.1", 0})
        .SetLocalAppId(std::move(identity));
    return fw::AppOptions{}
        .SetLimits(64, 64, 8, 64)
        .SetShutdownBudget(std::chrono::milliseconds{3000})
        .SetLifecycleBudget(std::chrono::milliseconds{3000})
        .SetNetworkOptions(std::move(network));
}

// —— 公开策略实现（继承 RpcPolicy）；组合包装见 DecoratorPolicy ——
class CountingAllowPolicy final : public fw::RpcPolicy {
public:
    CountingAllowPolicy(std::string name, std::optional<std::chrono::milliseconds> tighten)
        : m_name(std::move(name)), m_tighten(tighten) {
    }

    fw::result<fw::RpcPolicyDecision> Preflight(const fw::RpcCallView&) override {
        const unsigned seq = ++m_calls;
        Journal("event=preflight name=" + m_name + " seq=" + std::to_string(seq) + " auth_state=" + kAuthState);
        fw::RpcPolicyDecision d;
        if (m_tighten.has_value())
            d.tightened_deadline = std::chrono::steady_clock::now() + *m_tighten;
        return fw::result<fw::RpcPolicyDecision>::ok(std::move(d));
    }

private:
    std::string m_name;
    std::optional<std::chrono::milliseconds> m_tighten;
    std::atomic<unsigned> m_calls{0};
};

class RejectPolicy final : public fw::RpcPolicy {
public:
    fw::result<fw::RpcPolicyDecision> Preflight(const fw::RpcCallView&) override {
        const unsigned seq = ++m_calls;
        Journal("event=preflight name=reject seq=" + std::to_string(seq) + " auth_state=" + kAuthState);
        fw::RpcPolicyDecision d;
        d.kind = fw::RpcPolicyDecision::Kind::Reject;
        d.reject_error = fw::MakeError(fw::ErrorCode::Cancelled, "rpc policy rejected call (fixture)");
        return fw::result<fw::RpcPolicyDecision>::ok(std::move(d));
    }

private:
    std::atomic<unsigned> m_calls{0};
};

// 装饰器策略（包装）：外层记录后委托内层，验证两个虚 Preflight 各调一次、
// 顺序固定（外层先、内层后）。
class DecoratorPolicy final : public fw::RpcPolicy {
public:
    DecoratorPolicy(std::shared_ptr<fw::RpcPolicy> inner, std::string name)
        : m_inner(std::move(inner)), m_name(std::move(name)) {
    }

    fw::result<fw::RpcPolicyDecision> Preflight(const fw::RpcCallView& call) override {
        const unsigned seq = ++m_calls;
        Journal("event=preflight name=" + m_name + " seq=" + std::to_string(seq) + " auth_state=" + kAuthState);
        return m_inner->Preflight(call);
    }

private:
    std::shared_ptr<fw::RpcPolicy> m_inner;
    std::string m_name;
    std::atomic<unsigned> m_calls{0};
};

std::shared_ptr<fw::RpcPolicy> MakePolicy(const std::string& mode) {
    using ms = std::chrono::milliseconds;
    if (mode == "allow")
        return std::make_shared<CountingAllowPolicy>("allow", std::nullopt);
    if (mode == "reject")
        return std::make_shared<RejectPolicy>();
    if (mode == "tighten") // 收紧到 now+300ms
        return std::make_shared<CountingAllowPolicy>("tighten", ms{300});
    if (mode == "widen") // 放大到 now+3600s（框架必须忽略）
        return std::make_shared<CountingAllowPolicy>("widen", std::chrono::duration_cast<ms>(std::chrono::hours{1}));
    if (mode == "expired") // 收紧到 now-1ms（I/O 前 TimedOut）
        return std::make_shared<CountingAllowPolicy>("expired", ms{-1});
    if (mode == "decorator")
        return std::make_shared<DecoratorPolicy>(std::make_shared<CountingAllowPolicy>("inner", std::nullopt), "outer");
    return nullptr;
}

std::string Env(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? std::string{} : std::string{value};
}

// 生命周期结果 → 进程退出码，沿用既有验收口径：
//   0 按期优雅关闭 / 1 配置校验拒绝（未启动任何组件）/ 2 启动中途失败回退 /
//   3 关闭曾超预算（ShutdownIncomplete）/ 4 关闭收尾存在失败项。
int CoRc(const fw::result<void>& r) {
    if (r)
        return 0;
    switch (r.error().code) {
    case fw::ErrorCode::InvalidArgument:
        return 1;
    case fw::ErrorCode::TimedOut:
        return 3;
    default:
        return 4;
    }
}

void WriteTextFile(const std::string& path, const std::string& text,
                   const char* what) {
    std::ofstream f(path, std::ios::out | std::ios::trunc);
    f << text;
    if (!f)
        std::fprintf(stderr, "rpc_policy: cannot write %s file %s\n", what,
                     path.c_str());
}

std::uint16_t PortOf(const std::string& endpoint) {
    const auto colon = endpoint.rfind(':');
    if (colon == std::string::npos)
        return 0;
    return static_cast<std::uint16_t>(std::stoi(endpoint.substr(colon + 1)));
}

// 在当前协程内非阻塞地观察 stdin EOF（驱动器关管道即关闭触发）；不建观察线程。
void WaitStdinEof() {
    for (;;) {
        pollfd pfd{STDIN_FILENO, POLLIN, 0};
        const int ready = ::poll(&pfd, 1, 0);
        if (ready > 0) {
            char buf[64];
            const ssize_t n = ::read(STDIN_FILENO, buf, sizeof(buf));
            if (n <= 0)
                return;
            continue;
        }
        if (ready < 0 && errno != EINTR)
            return;
        bbtco_sleep(20);
    }
}

// 关闭触发只有一个：驱动器关 stdin。请求槽是持久位，先置位再等待不丢通知。
void ServeUntilEof(v2::CoApp& app) {
    WaitStdinEof();
    app.RequestShutdown();
}

// —— callee 进程：由 caller 派生，独立 v2::CoApp（互异 identity） ——
int RunCallee(const std::string& port_file, const std::string& meta_file) {
    auto app = v2::CoApp::Create(BaseOptions(kCalleeIdentity),
                                 "rpc-policy-callee");
    if (auto r = bbt_co_await
            app->CoInstallService<PolicyCalleeService>(ConcurrentOpts());
        !r) {
        std::fprintf(stderr, "callee: CoInstallService failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }

    auto started = bbt_co_await app->CoStart();
    if (!started) {
        std::fprintf(stderr, "callee: start failed: %s\n",
                     started.error().message.c_str());
        return CoRc(started);
    }
    const std::string bound = app->BoundEndpoint();
    if (bound.empty()) {
        std::fprintf(stderr, "callee: bind did not become ready\n");
        auto closed = bbt_co_await app->CoStop();
        (void)closed;
        return 5;
    }
    WriteTextFile(port_file, std::to_string(PortOf(bound)), "callee port");
    WriteTextFile(meta_file,
                  "process=callee\ncallee_service=" +
                      std::string(PolicyCallerService::kCalleeService) +
                      "\nlocal_identity=" + kCalleeIdentity +
                      "\nauth_state=" + kAuthState + "\n",
                  "callee meta");
    std::fprintf(stderr, "rpc_policy callee up endpoint=%s\n", bound.c_str());

    ServeUntilEof(*app);
    auto stopped = bbt_co_await app->CoStop();
    const int rc = CoRc(stopped);
    Journal(std::string("event=callee_shutdown rc=") + std::to_string(rc) +
            " auth_state=" + kAuthState);
    return rc;
}

struct CalleeProcess {
    pid_t pid{-1};
    int stdin_fd{-1};
    std::string port_file;
    std::string meta_file;
};

bool SpawnCallee(const std::string& dir, const std::string& journal_path,
                 CalleeProcess& out) {
    out.port_file = dir + "/callee.port";
    out.meta_file = dir + "/callee.meta";
    // 子进程参数经环境变量传：生成式入口（bbtco_main）拿不到 argv。必须在
    // fork 之前写好——fork 之后子进程只能调用 async-signal-safe 接口。
    ::setenv("BBT_EXAMPLE_MODE", "callee", 1);
    ::setenv("BBT_EXAMPLE_PORT_FILE", out.port_file.c_str(), 1);
    ::setenv("BBT_EXAMPLE_META_FILE", out.meta_file.c_str(), 1);
    ::setenv("BBT_EXAMPLE_JOURNAL_FILE", journal_path.c_str(), 1);
    int in_pipe[2] = {-1, -1};
    if (::pipe(in_pipe) != 0)
        return false;
    const pid_t pid = ::fork();
    if (pid < 0) {
        ::close(in_pipe[0]);
        ::close(in_pipe[1]);
        return false;
    }
    if (pid == 0) {
        ::close(in_pipe[1]);
        // 父进程异常消失时子进程随之终止，避免验收失败后遗留 callee。
        ::prctl(PR_SET_PDEATHSIG, SIGTERM);
        ::dup2(in_pipe[0], STDIN_FILENO);
        ::close(in_pipe[0]);
        std::vector<char> exe(4096);
        const ssize_t n = ::readlink("/proc/self/exe", exe.data(), exe.size() - 1);
        if (n <= 0)
            ::_exit(127);
        exe[static_cast<std::size_t>(n)] = '\0';
        // 生成式入口只认环境变量；fork 之后只做 async-signal-safe 的 execl，
        // 环境在 fork 前已由父进程写好（见下）。
        ::execl(exe.data(), exe.data(), static_cast<char*>(nullptr));
        ::_exit(127);
    }
    ::close(in_pipe[0]);
    out.pid = pid;
    out.stdin_fd = in_pipe[1];
    return true;
}

// 有界等待 callee 就绪；子进程提前退出即失败（不静默继续）。
bool WaitCalleePort(CalleeProcess& callee, std::uint16_t& port, int timeout_ms) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds{timeout_ms};
    while (std::chrono::steady_clock::now() < deadline) {
        int status = 0;
        if (::waitpid(callee.pid, &status, WNOHANG) == callee.pid)
            return false;
        std::ifstream f(callee.port_file);
        int value = 0;
        if (f >> value && value > 0) {
            port = static_cast<std::uint16_t>(value);
            return true;
        }
        bbtco_sleep(25);
    }
    return false;
}

// 关 stdin 触发 callee 优雅关闭；有界等待后强杀（异常路径也要回收）。
void ReapCallee(CalleeProcess& callee) {
    if (callee.pid < 0)
        return;
    if (callee.stdin_fd >= 0) {
        ::close(callee.stdin_fd);
        callee.stdin_fd = -1;
    }
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds{15};
    int status = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        const pid_t done = ::waitpid(callee.pid, &status, WNOHANG);
        if (done == callee.pid) {
            callee.pid = -1;
            return;
        }
        bbtco_sleep(20);
    }
    ::kill(callee.pid, SIGKILL);
    ::waitpid(callee.pid, &status, 0);
    callee.pid = -1;
}

int RunCaller(const std::string& mode, const std::string& port_file,
              const std::string& meta_file, const std::string& journal_path) {
    const auto slash = port_file.rfind('/');
    const std::string dir = slash == std::string::npos ? "." : port_file.substr(0, slash);

    CalleeProcess callee;
    if (!SpawnCallee(dir, journal_path, callee)) {
        std::fprintf(stderr, "fixture: cannot spawn callee process\n");
        return 6;
    }
    std::uint16_t callee_port = 0;
    if (!WaitCalleePort(callee, callee_port, 10000)) {
        std::fprintf(stderr, "fixture: callee did not become ready\n");
        ReapCallee(callee);
        return 6;
    }
    const std::string callee_endpoint = "127.0.0.1:" + std::to_string(callee_port);

    auto options = BaseOptions(kCallerIdentity);
    options.AddRoute(std::string(PolicyCallerService::kCalleeService),
                     callee_endpoint);
    options.SetRpcPolicy(MakePolicy(mode));

    auto app = v2::CoApp::Create(std::move(options), "rpc-policy-caller");
    if (auto r = bbt_co_await
            app->CoInstallService<PolicyCallerService>(ConcurrentOpts());
        !r) {
        std::fprintf(stderr, "fixture: CoInstallService(caller) failed: %s\n",
                     r.error().message.c_str());
        ReapCallee(callee);
        return 4;
    }
    CloseJournalComponent::stopped.store(false);
    if (auto r = bbt_co_await
            app->CoInstallComponent<CloseJournalComponent>(
                "journal", CloseJournalComponent::Options{});
        !r) {
        std::fprintf(stderr, "fixture: CoInstallComponent failed: %s\n",
                     r.error().message.c_str());
        ReapCallee(callee);
        return 4;
    }
    Journal(std::string("event=policy_installed mode=") + mode +
            " auth_state=" + kAuthState);

    auto started = bbt_co_await app->CoStart();
    if (!started) {
        ReapCallee(callee);
        std::fprintf(stderr, "fixture: start failed: %s\n",
                     started.error().message.c_str());
        return CoRc(started);
    }

    const std::string bound = app->BoundEndpoint();
    if (bound.empty()) {
        auto closed = bbt_co_await app->CoStop();
        (void)closed;
        ReapCallee(callee);
        std::fprintf(stderr, "fixture: bind did not become ready\n");
        return 5;
    }
    const std::uint16_t port = PortOf(bound);

    WriteTextFile(port_file, std::to_string(port), "port");
    WriteTextFile(meta_file,
                  "process=caller\nmode=" + mode +
                      "\ncaller_service=" + std::string(PolicyCallerService::kServiceName) +
                      "\ncallee_service=" + std::string(PolicyCallerService::kCalleeService) +
                      "\nstatic_route=" + std::string(PolicyCallerService::kCalleeService) +
                      "->" + callee_endpoint +
                      "\nlocal_identity=" + kCallerIdentity +
                      "\nauth_state=" + kAuthState + "\n",
                  "meta");
    std::fprintf(stderr, "rpc_policy fixture up mode=%s endpoint=%s callee=%s\n",
                 mode.c_str(), bound.c_str(), callee_endpoint.c_str());

    ServeUntilEof(*app);
    // 存活期探针要求 listener 已物理关闭；默认 Immediate 只封口，不能作该凭据。
    // 等模块自己的回收，不等主任务返回后才结束的共享 Engine::Run。
    auto close_options = inf::CloseOptions::Immediate();
    close_options.wait_release = true;
    close_options.deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    auto stopped = bbt_co_await app->CoStop(close_options);
    const int rc = CoRc(stopped);

    const int state = static_cast<int>(app->State());
    Journal(std::string("event=shutdown rc=") + std::to_string(rc) + " state=" + std::to_string(state) + " owner_closed=" + (CloseJournalComponent::stopped.load() ? "1" : "0") + " auth_state=" + kAuthState);

    // B1：co_stop 已返回，而本进程与 app 对象仍存活——此刻对 bound
    // 端口做有界探针才可能区分「框架关掉了 listener」与「进程回收后被内核顺带
    // 释放」。端口取自已服务过真实 RPC 的同一个值（driver 核 port 字段一致）。
    int probe_errno = 0;
    const ListenerProbeResult probe = ProbeListenerBoundPort(port, &probe_errno);
    Journal(std::string("event=listener_probe phase=after_run_before_exit port=") + std::to_string(port) + " result=" + ProbeResultName(probe) + " errno=" + std::to_string(probe_errno) + " auth_state=" + kAuthState);

    // caller 已收束后再关 callee；异常路径也被有界回收。
    ReapCallee(callee);
    std::fprintf(stderr,
                 "rpc_policy fixture rc=%d state=%d owner_closed=%d "
                 "listener_probe=%s errno=%d\n",
                 rc, state, CloseJournalComponent::stopped.load() ? 1 : 0,
                 ProbeResultName(probe), probe_errno);
    return rc;
}

} // namespace


// handler 内做 protobuf 编解码 + 出站策略 + 真实跨进程调用，默认 12KB 协程栈
// 不足；经公开配置宏在 Start 前放大（process-global）。
void setup_runtime() {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 512 * 1024;
}

int bbtco_main() {
    const std::string mode = Env("BBT_EXAMPLE_MODE");
    const std::string port_file = Env("BBT_EXAMPLE_PORT_FILE");
    const std::string meta_file = Env("BBT_EXAMPLE_META_FILE");
    const std::string journal_path = Env("BBT_EXAMPLE_JOURNAL_FILE");
    if (mode.empty() || port_file.empty() || meta_file.empty() ||
        journal_path.empty())
        return 90; // EX_USAGE

    // 内部 callee 模式：先于 caller 打开 journal（append），不截断。
    if (mode == "callee") {
        g_journal.open(journal_path, std::ios::out | std::ios::app);
        if (!g_journal) {
            std::fprintf(stderr, "callee: cannot open journal %s\n",
                         journal_path.c_str());
            return 6;
        }
        return RunCallee(port_file, meta_file);
    }

    if (!MakePolicy(mode)) {
        std::fprintf(stderr, "fixture: unknown mode %s\n", mode.c_str());
        return 90;
    }

    g_journal.open(journal_path, std::ios::out | std::ios::app);
    if (!g_journal) {
        std::fprintf(stderr, "fixture: cannot open journal %s\n",
                     journal_path.c_str());
        return 6;
    }

    return RunCaller(mode, port_file, meta_file, journal_path);
}

// 入口 native host hook：共享 Engine 的 Engine::Run 在主线程执行一次。
int bbtco_main_host_hook(bbt::coroutine::HostHookContext& ctx) {
    return bbt::framework::HostGate::RunHostGate(ctx);
}

BBTCO_MAIN_WITH_SETUP_AND_HOST_HOOK();
