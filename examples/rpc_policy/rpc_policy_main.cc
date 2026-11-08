// bbt-framework 公开出站策略 fixture。
//
// V2 会话拒绝 self-connection，框架又约束进程内同一时刻至多一个活跃 CoApp，
// 因此本 fixture 由 caller 进程派生一个互异 identity 的 callee 进程：
//
//   caller 进程（local_identity=rpc-policy-caller）
//     CoApp 宿主 PolicyCallerService；其 handler 内经受保护的
//     `this->call<Req,Resp>` 出站，出站目标解析成功后、真实 socket 发送之前
//     调用公开策略 `CoAppOptions::rpc_policy`
//   callee 进程（local_identity=rpc-policy-callee）
//     CoApp 宿主 PolicyCalleeService；入站经真实 V2 会话分发
//
// 只用公开面：`bbt/framework/CoApp.hpp`、`bbt/framework/RpcPolicy.hpp`、
// `RequestContext.hpp` + getvalue 的 proto/codec。**不** include internal/、
// **不**用 CoAppSeam/MakeCoAppForTest/DispatchInboundForTest/INetworkHost 桩/
// 发送注入——编译期即证明「不依赖 internal 测试缝」。
//
// callee 在自己的 handler 内读公开 `RequestContext::deadline` 并 journal 可见
// 剩余预算，作为「期限只缩不放」的真实 oracle（不依赖 reply envelope 预算，
// 后者只反映 caller 自己的入站预算）。
//
// argv: rpc_policy <mode> <port_file> <meta_file> <journal_file>
//   mode ∈ allow | reject | tighten | widen | expired | decorator
//   - allow     ：策略允许（只计数）
//   - reject    ：策略拒绝（Cancelled）：`_MakeEgress` 在 Send 之前返回错误
//   - tighten   ：策略收紧出站期限到 now+300ms（应传播到 callee 可见预算）
//   - widen     ：策略返回「放大」期限 now+3600s（框架必须忽略）
//   - expired   ：策略收紧到 now-1ms（框架须在 I/O 前 TimedOut）
//   - decorator ：外层策略包内层策略，两个虚 Preflight 各调一次、顺序固定
//   内部模式 `--callee <port_file> <meta_file> <journal_file>` 由 caller 派生，
//   不对外使用。
//
// journal 事件：preflight name=<n> seq=<k> / callee_handler_entered
//   deadline_ms=<n> / listener_probe phase=after_run_before_exit port=<n>
//   result=refused|connected|error errno=<n> / resource_close /
//   shutdown rc=.. state=.. owner_closed=.. / callee_shutdown rc=..
//
// 关闭后验收（B1）：`app.run()` 返回、runner 已 join、进程与 `CoApp` 对象均仍
// 存活时，由本进程对自己的 bound 端口做一次**有界** nonblocking connect 并入
// journal。只有 ECONNREFUSED 记 result=refused（唯一判绿值）；超时或其它 errno
// 记 result=error（不算绿）。该探针只证明「存活期 listener 已关」——它不证明
// 关闭期零额外 I/O，也不替代在途 I/O 排空的判定。

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
#include <thread>
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

#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/infra/ICoCloseable.hpp>

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/RpcPolicy.hpp>

#include "getvalue_service.hpp" // ProtoCodec<GetValueRequest/Response>（公开面）

namespace fw = bbt::framework;
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

class CloseJournal final : public inf::ICoCloseable {
public:
    void Close() noexcept override {
        bool first = false;
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            first = !m_closed;
            m_closed = true;
        }
        if (first)
            Journal(std::string("event=resource_close auth_state=") + kAuthState);
    }
    bool IsClosed() const noexcept override {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_closed;
    }

private:
    mutable std::mutex m_mtx;
    bool m_closed = false;
};

// —— 业务（公开面）：caller 出站 + callee 观测 ——
//
// callee：journal handler 进入与**公开可见**的 handler 期限剩余（RequestContext
// ::deadline - now），作为「期限只缩不放」的真实 oracle。
class PolicyCalleeService final : public fw::CoService<PolicyCalleeService> {
public:
    static constexpr std::string_view kServiceName = "bbt.example.v1.PolicyCallee";

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
};

// caller：经公开 typed 出站 `this->call<Req,Resp>` 转发到 callee（静态路由）或
// 未配置路由目标（find_route 门拒绝，策略不被询问）。
class PolicyCallerService final : public fw::CoService<PolicyCallerService> {
public:
    static constexpr std::string_view kServiceName = "bbt.example.v1.PolicyCaller";
    static constexpr std::string_view kCalleeService = "bbt.example.v1.PolicyCallee";
    static constexpr std::string_view kNoRouteService = "bbt.example.v1.PolicyUnrouted";

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
    return fw::ServiceOptions{fw::ExecutionPolicy::Concurrent,
                              /*max_inflight*/ 64,
                              /*mailbox_capacity*/ 0,
                              /*max_actors*/ 0,
                              /*ordered_ingress*/ false,
                              /*max_ordered_streams*/ 0,
                              /*max_cached_results*/ 0,
                              /*max_cached_result_bytes*/ 0};
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

std::string BoundEndpointOrEmpty(fw::CoApp& app, int timeout_ms) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds{timeout_ms};
    while (std::chrono::steady_clock::now() < deadline) {
        std::string bound = app.bound_endpoint();
        if (!bound.empty())
            return bound;
        std::this_thread::sleep_for(std::chrono::milliseconds{25});
    }
    return {};
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

void ServeUntilEof(fw::CoApp& app) {
    char buf[8];
    while (::read(STDIN_FILENO, buf, sizeof(buf)) > 0) {
    }
    app.request_shutdown();
}

// —— callee 进程：由 caller 派生，独立 CoApp（互异 identity） ——
int RunCallee(const std::string& port_file, const std::string& meta_file) {
    fw::CoAppOptions options;
    options.network_limits = inf::NetworkLimits{/*max_connections*/ 64, /*max_inflight*/ 64,
                                                /*max_header_bytes*/ 16 * 1024, /*max_body_bytes*/ 64 * 1024,
                                                /*incoming_timeout*/ std::chrono::milliseconds{30000}};
    options.listen = inf::ListenAddress{"127.0.0.1", 0};
    options.shutdown_step_budget = std::chrono::milliseconds{3000};
    options.local_identity = kCalleeIdentity;

    fw::CoApp app{std::move(options)};
    if (auto r = app.add_service<PolicyCalleeService>(ConcurrentOpts()); !r) {
        std::fprintf(stderr, "callee: add_service failed: %s\n", r.error().message.c_str());
        return 4;
    }

    int rc = -1;
    std::thread runner([&] { rc = app.run(); });
    const std::string bound = BoundEndpointOrEmpty(app, 10000);
    if (bound.empty()) {
        app.request_shutdown();
        if (runner.joinable())
            runner.join();
        std::fprintf(stderr, "callee: bind did not become ready rc=%d\n", rc);
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

    ServeUntilEof(app);
    if (runner.joinable())
        runner.join();
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
        ::execl(exe.data(), exe.data(), "--callee", out.port_file.c_str(),
                out.meta_file.c_str(), journal_path.c_str(),
                static_cast<char*>(nullptr));
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
        std::this_thread::sleep_for(std::chrono::milliseconds{25});
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
        std::this_thread::sleep_for(std::chrono::milliseconds{20});
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

    fw::CoAppOptions options;
    options.network_limits = inf::NetworkLimits{/*max_connections*/ 64, /*max_inflight*/ 64,
                                                /*max_header_bytes*/ 16 * 1024, /*max_body_bytes*/ 64 * 1024,
                                                /*incoming_timeout*/ std::chrono::milliseconds{30000}};
    options.listen = inf::ListenAddress{"127.0.0.1", 0};
    options.shutdown_step_budget = std::chrono::milliseconds{3000};
    // V2 会话身份必填；caller/callee/驱动器三方互异。
    options.local_identity = kCallerIdentity;
    options.static_routes = {
        fw::StaticRoute{std::string(PolicyCallerService::kCalleeService), callee_endpoint}};
    // 唯一组装点——构造期选择公开策略。
    options.rpc_policy = MakePolicy(mode);

    fw::CoApp app{std::move(options)};
    if (auto r = app.add_service<PolicyCallerService>(ConcurrentOpts()); !r) {
        std::fprintf(stderr, "fixture: add_service(caller) failed: %s\n", r.error().message.c_str());
        ReapCallee(callee);
        return 4;
    }
    auto owner = std::make_shared<CloseJournal>();
    if (auto r = app.add_resource<CloseJournal>(owner); !r) {
        std::fprintf(stderr, "fixture: add_resource failed: %s\n", r.error().message.c_str());
        ReapCallee(callee);
        return 4;
    }

    int rc = -1;
    std::thread runner([&] { rc = app.run(); });

    const std::string bound = BoundEndpointOrEmpty(app, 10000);
    if (bound.empty()) {
        app.request_shutdown();
        if (runner.joinable())
            runner.join();
        ReapCallee(callee);
        std::fprintf(stderr, "fixture: bind did not become ready rc=%d\n", rc);
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

    ServeUntilEof(app);
    if (runner.joinable())
        runner.join();

    const int state = static_cast<int>(app.shutdown_state());
    Journal(std::string("event=shutdown rc=") + std::to_string(rc) + " state=" + std::to_string(state) + " owner_closed=" + (owner->IsClosed() ? "1" : "0") + " auth_state=" + kAuthState);

    // B1：run 已返回、runner 已 join，而本进程与 app 对象仍存活——此刻对 bound
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
                 rc, state, owner->IsClosed() ? 1 : 0, ProbeResultName(probe), probe_errno);
    return rc;
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc < 5)
        return 90; // EX_USAGE

    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 512 * 1024;

    // 内部 callee 模式：先于 caller 打开 journal（append），不截断。
    if (std::strcmp(argv[1], "--callee") == 0) {
        const std::string port_file = argv[2];
        const std::string meta_file = argv[3];
        const std::string journal_path = argv[4];
        g_journal.open(journal_path, std::ios::out | std::ios::app);
        if (!g_journal) {
            std::fprintf(stderr, "callee: cannot open journal %s\n", journal_path.c_str());
            return 6;
        }
        return RunCallee(port_file, meta_file);
    }

    const std::string mode = argv[1];
    const std::string port_file = argv[2];
    const std::string meta_file = argv[3];
    const std::string journal_path = argv[4];
    if (!MakePolicy(mode)) {
        std::fprintf(stderr, "fixture: unknown mode %s\n", mode.c_str());
        return 90;
    }

    g_journal.open(journal_path, std::ios::out | std::ios::app);
    if (!g_journal) {
        std::fprintf(stderr, "fixture: cannot open journal %s\n", journal_path.c_str());
        return 6;
    }

    return RunCaller(mode, port_file, meta_file, journal_path);
}
