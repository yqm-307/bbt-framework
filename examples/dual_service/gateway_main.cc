// examples/dual_service/gateway_main.cc — svc_b：网关服务进程。
//
// 业务形态：单进程托管一个 Concurrent 服务 "gateway"，handler 内经
// this->call 真实跨进程调用 storage（静态路由显式白名单，目标
// endpoint 由命令行传入）。演示：
//   fetch(key)          → storage.get 透传（含 miss → NotFound 透传）
//   store(key, value)   → storage.put 透传
//   explode()           → 业务错误（InternalError）不经网络直达客户端
//   slowcall(ms)        → storage.sleep_ms；显式子预算到期，服务端继承后 TimedOut
//   missing()           → 调用未路由服务名 → 路由门 NotFound 失败路径
//
// 协议：V2 会话（SessionFrame + u32be + Hello/HelloAck，与 examples/getvalue
// 同口径）。V2 的 CallOptions.deadline 同时是客户端等待上限与 wire
// remaining_budget_ms 的来源，出站子预算/父预算才能真正到达对端服务端
// （见 SlowCall 注释）。
//
// 优雅关闭同 svc_a：SIGINT/SIGTERM → 信号处理只置位标志，业务主协程观察到后
// RequestShutdown → CoStop（不建 watcher 线程）。入口是 runtime 的生成式
// `bbtco_main`，参数经环境变量传入；共享 Engine 的唯一一次 Engine::Run 由
// bbtco_main_host_hook 执行：
//   BBT_EXAMPLE_LISTEN_PORT / BBT_EXAMPLE_STORAGE_ENDPOINT

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/HostGate.hpp>
#include <bbt/framework/V2.hpp>

namespace fw = bbt::framework;
namespace v2 = bbt::framework::v2;

namespace {

// slowcall 对 storage 的显式子预算：经 V2 Call 的 remaining_budget_ms
// 传到对端，storage handler 可见前收敛为 min(传输硬看门, now+budget) 并据此
// 中断 sleep_ms。driver 用远大于它的客户端预算，故收到的 TimedOut 只能由
// 服务端自产——这是「服务端继承预算」而非「仅客户端超时」的可判定证据。
constexpr std::int64_t kSlowCallSubBudgetMs = 600;

class GatewaySvc final : public v2::CoService<> {
public:
    static constexpr std::string_view kServiceName = "gateway";

    explicit GatewaySvc(fw::detail::ConstructKey k) : v2::CoService<>(k) {}

    fw::CoRpcResp Fetch(fw::CoRpcReq req) {
        // 跨进程调用：AddRoute("storage") → tcp://<svc_a>/。
        // 远端业务错误（NotFound）原样透传给本进程调用方。
        auto resp = this->call("storage", "get", req);
        if (!resp) return fw::CoRpcResp::Error(resp.error());
        return resp.value();
    }

    fw::CoRpcResp Store(fw::CoRpcReq req) {
        auto resp = this->call("storage", "put", req);
        if (!resp) return fw::CoRpcResp::Error(resp.error());
        return resp.value();
    }

    fw::CoRpcResp Del(fw::CoRpcReq req) {
        // 清理本轮 key 用：透传 storage.del（Redis DEL + Mongo DeleteOne）。
        auto resp = this->call("storage", "del", req);
        if (!resp) return fw::CoRpcResp::Error(resp.error());
        return resp.value();
    }

    fw::CoRpcResp Explode(fw::CoRpcReq req) {
        (void)req;
        return fw::CoRpcResp::Error(fw::MakeError(
            fw::ErrorCode::InternalError,
            "gateway.explode: deliberate business failure"));
    }

    fw::CoRpcResp SlowCall(fw::CoRpcReq req) {
        // 显式给 storage 一个 600ms 子预算（两参数重载：CallOptions 经
        // AdaptCallOptions 与父预算取 min，不会延长父预算）。预算随
        // V2 Call 的 remaining_budget_ms 到达 storage，服务端继承后
        // 在其 deadline 到期时如实 TimedOut；本进程再把该错误透传给 driver。
        fw::CallOptions o;
        o.deadline = std::chrono::steady_clock::now() +
            std::chrono::milliseconds{kSlowCallSubBudgetMs};
        auto resp = this->call("storage", "sleep_ms", req, {}, o);
        if (!resp) return fw::CoRpcResp::Error(resp.error());
        return resp.value();
    }

    fw::CoRpcResp Missing(fw::CoRpcReq req) {
        // 失败路径：路由未配置 "no.such.service" → NotFound，不发起任何网络 I/O。
        auto resp = this->call("no.such.service", "ping", req);
        if (!resp) return fw::CoRpcResp::Error(resp.error());
        return resp.value();
    }

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&GatewaySvc::Fetch>("fetch"),
        fw::Method<&GatewaySvc::Store>("store"),
        fw::Method<&GatewaySvc::Del>("del"),
        fw::Method<&GatewaySvc::Explode>("explode"),
        fw::Method<&GatewaySvc::SlowCall>("slowcall"),
        fw::Method<&GatewaySvc::Missing>("missing"));

protected:
    ~GatewaySvc() noexcept override = default;
    friend class fw::detail::Registry;
};

std::atomic_bool g_stop{false};

void InstallSignals() {
    std::signal(SIGINT,  [](int) { g_stop.store(true); });
    std::signal(SIGTERM, [](int) { g_stop.store(true); });
}

// 十进制端口解析（同 svc_a）：非法值给诊断后正常退出，不走 std::stoi 抛异常。
bool ParsePort(const char* text, std::uint16_t* out) {
    if (text == nullptr || *text == '\0') return false;
    unsigned long value = 0;
    for (const char* p = text; *p != '\0'; ++p) {
        if (*p < '0' || *p > '9') return false;
        value = value * 10 + static_cast<unsigned long>(*p - '0');
        if (value > 65535) return false;
    }
    if (value == 0) return false;
    *out = static_cast<std::uint16_t>(value);
    return true;
}

} // namespace

// handler 内嵌套出站调用（入站 → this->call → V2 会话 client → 等回包），
// 协程栈按实测放大到 64KB。
void setup_runtime() {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
}

int bbtco_main() {
    // 参数（生成式入口不暴露 argv）：BBT_EXAMPLE_LISTEN_PORT /
    // BBT_EXAMPLE_STORAGE_ENDPOINT（host:port）。
    const char* port_text = std::getenv("BBT_EXAMPLE_LISTEN_PORT");
    const char* storage_text = std::getenv("BBT_EXAMPLE_STORAGE_ENDPOINT");
    if (port_text == nullptr || storage_text == nullptr) {
        std::fprintf(stderr,
                     "usage: svc_b with env BBT_EXAMPLE_LISTEN_PORT=<port> "
                     "BBT_EXAMPLE_STORAGE_ENDPOINT=<host:port>\n");
        return 64;
    }
    std::uint16_t port = 0;
    if (!ParsePort(port_text, &port)) {
        std::fprintf(stderr,
            "[svc_b] FATAL: 非法 listen_port \"%s\"（需要 1..65535 的十进制端口）\n",
            port_text);
        return 64;   // EX_USAGE
    }
    const std::string storage_ep = storage_text;

    fw::AppNetworkOptions network;
    network.SetLimits(bbt::infra::NetworkLimits{
                          /*max_connections*/ 128,
                          /*max_inflight*/    64,
                          /*max_header_bytes*/16384,
                          /*max_body_bytes*/  65536,
                          /*incoming_timeout*/
                          std::chrono::milliseconds{30000}})
        .SetListenAddress(bbt::infra::ListenAddress{"127.0.0.1", port})
        // V2 会话身份必填；gateway 与 storage 进程使用互异身份。
        .SetLocalAppId("dual-gateway");

    auto options = fw::AppOptions{}
        .SetLimits(64, 64, 8, 64)
        .SetShutdownBudget(std::chrono::milliseconds{2000})
        .SetLifecycleBudget(std::chrono::milliseconds{2000})
        .SetNetworkOptions(std::move(network))
        // 出站白名单：仅允许名为 "storage" 的逻辑目标，endpoint 为真实
        // host:port——没有自动发现，也未配置的目标一律 NotFound。
        .AddRoute("storage", storage_ep);

    auto app = v2::CoApp::Create(std::move(options), "dual-gateway");
    if (auto r = bbt_co_await app->CoInstallService<GatewaySvc>(
            fw::ServiceOptions{}.SetMaxInflight(64));
        !r) {
        std::fprintf(stderr, "[svc_b] CoInstallService failed: %s\n",
                     r.error().message.c_str());
        return 65;
    }

    InstallSignals();

    auto started = bbt_co_await app->CoStart();
    if (!started) {
        std::fprintf(stderr, "[svc_b] start failed: %s\n",
                     started.error().message.c_str());
        return started.error().code == fw::ErrorCode::InvalidArgument ? 1 : 2;
    }

    std::printf("[svc_b] gateway service on 127.0.0.1:%u (storage -> %s)\n",
                port, storage_ep.c_str());
    std::fflush(stdout);

    // 关闭触发只有信号：处理函数只置位标志，主协程在 Co 域轮询到后请求关闭。
    while (!g_stop.load(std::memory_order_acquire))
        bbtco_sleep(20);
    app->RequestShutdown();
    auto requested = bbt_co_await app->CoWaitShutdownRequest();
    if (!requested) {
        std::fprintf(stderr, "[svc_b] wait shutdown request failed: %s\n",
                     requested.error().message.c_str());
    }
    auto stopped = bbt_co_await app->CoStop();
    const int rc = stopped ? 0
                           : (stopped.error().code == fw::ErrorCode::TimedOut
                                  ? 3
                                  : 4);
    std::printf("[svc_b] CoStop returned %d, state=%d\n",
                rc, static_cast<int>(app->State()));
    std::fflush(stdout);
    return rc;
}

// 入口 native host hook：共享 Engine 的 Engine::Run 在主线程执行一次。
int bbtco_main_host_hook(bbt::coroutine::HostHookContext& ctx) {
    return bbt::framework::HostGate::RunHostGate(ctx);
}

BBTCO_MAIN_WITH_SETUP_AND_HOST_HOOK();
