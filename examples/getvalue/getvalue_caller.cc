// bbt-framework Service→Service 正式出站 caller 进程。
//
// 与 getvalue_server 同装配口径：公开 v2::CoApp（local_identity 显式、V2 入站
// 经 InstallRpcAdmission）+ 公开 v2::CoService，不使用测试缝。宿主
// GetValueCallerService；其 handler 内经 CoServiceBase::call<Req,Resp> 向另一个
// framework v2::CoApp（callee 进程）发起真实 V2 Call 出站。主内容在业务主协程里：
// 装配 → CoStart → 写端口/元数据 → 等关闭触发 → CoStop；入口是 runtime 的
// 生成式 `bbtco_main`，不写 native main、不建 runner 线程、不 join；共享
// Engine 的唯一一次 Engine::Run 由 bbtco_main_host_hook 执行。
//
// 参数经环境变量传入（生成式入口不暴露 argv）：
//   BBT_EXAMPLE_MODE=caller|badroute
//   caller:   BBT_EXAMPLE_PORT_FILE / BBT_EXAMPLE_CALLEE_EP /
//             BBT_EXAMPLE_BLACKHOLE_EP / BBT_EXAMPLE_META_FILE
//             callee_ep / blackhole_ep 形如 127.0.0.1:<port>（静态路由 endpoint 形态）。
//   badroute: BBT_EXAMPLE_KIND / BBT_EXAMPLE_RESULT_FILE
//     以公开入口验证三类非法静态路由在启动任何组件前被拒绝。
//     kind ∈ {empty-service, empty-endpoint, duplicate}；
//     写出机器可读观测（rc/lifecycle_failures/bound_endpoint/组件启动计数）。

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <poll.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>

#include <bbt/example/v1/get_value.pb.h>

#include <bbt/framework/Component.hpp>
#include <bbt/framework/HostGate.hpp>
#include <bbt/framework/V2.hpp>

#include "getvalue_caller.hpp"

namespace {

namespace fw = bbt::framework;
namespace v2 = bbt::framework::v2;

// 「启动任何组件前拒绝」的公开面额外证据：该组件的 OnStart 只在 CoStart 通过
// AppOptions::Validate、进入组件启动相位时被调用一次。被拒绝的配置下计数
// 必须保持 0——证明拒绝发生在组件/网络启动之前。
class ProbeComponent final : public v2::CoComponent {
public:
    struct Options {};
    static std::atomic<int> start_calls;

    explicit ProbeComponent(fw::detail::ConstructKey k, Options)
        : v2::CoComponent(k) {}

protected:
    ~ProbeComponent() noexcept override = default;
    fw::result<void> OnStart() override {
        start_calls.fetch_add(1);
        return fw::result<void>::ok();
    }
    friend class fw::detail::Registry;
};
std::atomic<int> ProbeComponent::start_calls{0};

std::string Env(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? std::string{} : std::string{value};
}

// 在当前协程内非阻塞地观察 stdin：读到 EOF 即返回（驱动器关管道即关闭触发）。
// 不开 native 线程：示例的生命周期只有业务主协程一条驱动线。
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

// 生命周期结果 → 进程退出码，沿用既有验收口径：
//   0 按期优雅关闭 / 1 配置校验拒绝（未启动任何组件）/ 2 启动中途失败回退 /
//   3 关闭曾超预算（StopIncomplete）/ 4 关闭收尾存在失败项。
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

// caller 与 badroute 共用的基础装配（除静态路由外完全一致）。
fw::AppOptions BaseOptions() {
    fw::AppNetworkOptions network;
    network.SetLimits(bbt::infra::NetworkLimits{
                          64, 64, 16 * 1024, 64 * 1024,
                          std::chrono::milliseconds{30000}})
        .SetListenAddress(bbt::infra::ListenAddress{"127.0.0.1", 0})
        // V2 会话身份必填；caller 与 callee/badroute 进程使用互异身份。
        .SetLocalAppId("getvalue-caller");
    return fw::AppOptions{}
        .SetLimits(64, 64, 8, 64)
        .SetShutdownBudget(std::chrono::milliseconds{3000})
        .SetLifecycleBudget(std::chrono::milliseconds{3000})
        .SetNetworkOptions(std::move(network));
}

bool IsKnownBadRouteKind(const std::string& kind) {
    return kind == "empty-service" || kind == "empty-endpoint" ||
           kind == "duplicate";
}

// 三类非法静态路由（其余字段保持与正常 caller 相同的合法口径），供公开
// v2::CoApp 装配后在 CoStart 前被 AppOptions::Validate 拒绝：
//   empty-service  空 service 名
//   empty-endpoint 空 endpoint（V2 只校验 endpoint 非空，地址语法归 infra）
//   duplicate      两条相同 service 名
// （transport 维度随 V2 endpoint 字符串收敛而消失。）
fw::AppOptions BadRouteOptions(const std::string& kind) {
    auto options = BaseOptions();
    if (kind == "empty-service")
        return std::move(options.AddRoute("", "127.0.0.1:1"));
    if (kind == "empty-endpoint")
        return std::move(options.AddRoute("svc.route", ""));
    // duplicate：同名两条 endpoint。
    return std::move(options.AddRoute("svc.route", "127.0.0.1:1")
                         .AddRoute("svc.route", "127.0.0.1:2"));
}

std::string JsonEscape(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (const char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:   out += c;      break;
        }
    }
    return out;
}

// 公开入口：装配非法静态路由 → CoInstallService/CoInstallComponent → CoStart。
// 断言依据是 CoStart 的真实结果与公开观测口（LifecycleFailures/
// BoundEndpoint/组件启动计数），本进程只如实写观测，不打印硬编码期望值。
int RunBadRoute(const std::string& kind, const std::string& result_file) {
    ProbeComponent::start_calls.store(0);

    auto app = v2::CoApp::Create(BadRouteOptions(kind), "getvalue-badroute");
    if (auto r = bbt_co_await
            app->CoInstallService<getvalue::GetValueCallerService>(
                fw::ServiceOptions{}.SetMaxInflight(64));
        !r) {
        std::fprintf(stderr, "badroute CoInstallService failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }
    if (auto r = bbt_co_await app->CoInstallComponent<ProbeComponent>(
            "probe", ProbeComponent::Options{});
        !r) {
        std::fprintf(stderr, "badroute CoInstallComponent failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }

    // 坏配置 → CoStart 在任何组件启动前以校验错误返回（退出码 1）。
    // 调用点就在业务主协程里，故这一 await 不会产生额外线程或调度胶水。
    auto started = bbt_co_await app->CoStart();
    const int rc = CoRc(started);
    if (started) {
        // 校验通过时也不能留下未收口的宿主。
        auto closed = bbt_co_await app->CoStop();
        (void)closed;
    }
    // 失败证据：v2 的 AppOptions::Validate 直接把原因写进 CoStart 的返回错误；
    // lifecycle_failures 字段如实记录 CoStart 返回的错误原文（同一来源，
    // 不是 LifecycleFailures 观察口——校验在副作用前失败，无对象级失败项）。
    const std::string failure_msg =
        started ? std::string{} : started.error().message;
    const auto failures = app->LifecycleFailures();
    const std::string endpoint = app->BoundEndpoint();

    std::ofstream f(result_file, std::ios::trunc);
    f << "{\n"
      << "  \"mode\": \"badroute\",\n"
      << "  \"kind\": \"" << JsonEscape(kind) << "\",\n"
      << "  \"rc\": " << rc << ",\n"
      << "  \"bound_endpoint\": \"" << JsonEscape(endpoint) << "\",\n"
      << "  \"resource_factory_calls\": "
      << ProbeComponent::start_calls.load() << ",\n"
      << "  \"lifecycle_failures\": ["
      << (failure_msg.empty() ? "" : "\"" + JsonEscape(failure_msg) + "\"")
      << "],\n"
      << "  \"auth_state\": \"unauthenticated_loopback\"\n"
      << "}\n";
    if (!f) {
        std::fprintf(stderr, "badroute result write failed: %s\n",
                     result_file.c_str());
        return 4;
    }
    std::fprintf(stderr,
                 "badroute kind=%s rc=%d failures=%zu endpoint='%s' probe=%d\n",
                 kind.c_str(), rc, failures.size(), endpoint.c_str(),
                 ProbeComponent::start_calls.load());
    return rc;
}
int RunCaller(const std::string& port_file, const std::string& callee_ep,
              const std::string& black_ep, const std::string& meta_file) {
    auto options = BaseOptions();
    options.AddRoute(
        std::string(getvalue::GetValueCallerService::kCalleeService),
        callee_ep);
    options.AddRoute(
        std::string(getvalue::GetValueCallerService::kBlackholeService),
        black_ep);

    auto app = v2::CoApp::Create(std::move(options), "getvalue-caller");
    if (auto r = bbt_co_await
            app->CoInstallService<getvalue::GetValueCallerService>(
                fw::ServiceOptions{}.SetMaxInflight(64));
        !r) {
        std::fprintf(stderr, "caller CoInstallService failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }

    auto started = bbt_co_await app->CoStart();
    if (!started) {
        std::fprintf(stderr, "caller start failed: %s\n",
                     started.error().message.c_str());
        return CoRc(started);
    }

    const std::string endpoint = app->BoundEndpoint();
    if (endpoint.empty()) {
        std::fprintf(stderr, "caller did not bind\n");
        auto closed = bbt_co_await app->CoStop();
        (void)closed;
        return 5;
    }
    const auto colon = endpoint.rfind(':');
    {
        std::ofstream f(port_file, std::ios::trunc);
        f << endpoint.substr(colon + 1);
    }
    {
        std::ofstream f(meta_file, std::ios::trunc);
        f << "service=" << getvalue::GetValueCallerService::kServiceName << "\n";
        f << "callee_service="
          << getvalue::GetValueCallerService::kCalleeService << "\n";
        f << "request_schema="
          << bbt::example::v1::GetValueRequest::descriptor()->full_name() << "\n";
        f << "response_schema="
          << bbt::example::v1::GetValueResponse::descriptor()->full_name() << "\n";
        f << "local_identity=getvalue-caller\n";
        f << "auth_state=unauthenticated_loopback\n";
    }
    std::fprintf(stderr, "getvalue caller up endpoint=%s callee=%s blackhole=%s\n",
                 endpoint.c_str(), callee_ep.c_str(), black_ep.c_str());

    // 关闭触发只有一个：驱动器关 stdin。请求槽是持久位，先置位再等待不丢通知。
    WaitStdinEof();
    app->RequestShutdown();
    auto requested = bbt_co_await app->CoWaitShutdownRequest();
    if (!requested) {
        std::fprintf(stderr, "caller wait shutdown request failed: %s\n",
                     requested.error().message.c_str());
    }

    auto stopped = bbt_co_await app->CoStop();
    const int rc = CoRc(stopped);
    if (rc != 0)
        std::fprintf(stderr, "caller stop: %s\n",
                     stopped.error().message.c_str());
    std::fprintf(stderr, "getvalue caller rc=%d\n", rc);
    return rc;
}

} // namespace

// caller handler 内做 typed 出站（protobuf 编解码 + 框架分发），显式把协程栈
// 调大（公开配置宏，只能在 Start 前的 setup_runtime 阶段写；process-global）。
// Two server drivers + one client-only driver + one application worker.
void setup_runtime() {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 512 * 1024;
}

int bbtco_main() {
    const std::string mode = Env("BBT_EXAMPLE_MODE");

    // 非法静态路由的公开入口验收模式。
    if (mode == "badroute") {
        const std::string kind = Env("BBT_EXAMPLE_KIND");
        const std::string result_file = Env("BBT_EXAMPLE_RESULT_FILE");
        if (!IsKnownBadRouteKind(kind) || result_file.empty())
            return 90;
        return RunBadRoute(kind, result_file);
    }

    if (mode != "caller")
        return 90;
    const std::string port_file = Env("BBT_EXAMPLE_PORT_FILE");
    const std::string callee_ep = Env("BBT_EXAMPLE_CALLEE_EP");
    const std::string black_ep = Env("BBT_EXAMPLE_BLACKHOLE_EP");
    const std::string meta_file = Env("BBT_EXAMPLE_META_FILE");
    if (port_file.empty() || callee_ep.empty() || black_ep.empty() ||
        meta_file.empty())
        return 90;
    return RunCaller(port_file, callee_ep, black_ep, meta_file);
}

// 入口 native host hook：共享 Engine 的 Engine::Run 在主线程执行一次。
int bbtco_main_host_hook(bbt::coroutine::HostHookContext& ctx) {
    return bbt::framework::HostGate::RunHostGate(ctx);
}

BBTCO_MAIN_WITH_SETUP_AND_HOST_HOOK();
