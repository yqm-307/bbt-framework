// examples/dual_service/gateway_main.cc — svc_b：网关服务进程。
//
// 业务形态：单进程托管一个 Concurrent 服务 "gateway"，handler 内经
// this->call 真实跨进程调用 storage（static_routes 显式白名单，目标
// endpoint 由命令行传入）。演示：
//   fetch(key)          → storage.get 透传（含 miss → NotFound 透传）
//   store(key, value)   → storage.put 透传
//   explode()           → 业务错误（InternalError）不经网络直达客户端
//   slowcall(ms)        → storage.sleep_ms；父预算到期 → TimedOut 透传
//   missing()           → 调用未路由服务名 → find_route NotFound 失败路径
//
// 优雅关闭同 svc_a：SIGINT/SIGTERM → watcher 线程 request_shutdown。

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>

namespace fw = bbt::framework;

namespace {

class GatewaySvc final : public fw::CoService<GatewaySvc> {
public:
    static constexpr std::string_view kServiceName = "gateway";

    fw::CoRpcResp Fetch(fw::CoRpcReq req) {
        // 跨进程调用：static_routes["storage"] → http://<svc_a>/rpc。
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

    fw::CoRpcResp Explode(fw::CoRpcReq req) {
        (void)req;
        return fw::CoRpcResp::Error(fw::MakeError(
            fw::ErrorCode::InternalError,
            "gateway.explode: deliberate business failure"));
    }

    fw::CoRpcResp SlowCall(fw::CoRpcReq req) {
        // 不显式给 CallOptions：出站期限继承入站上下文（父预算），
        // driver 给短 deadline 时这里如实 TimedOut。
        auto resp = this->call("storage", "sleep_ms", req);
        if (!resp) return fw::CoRpcResp::Error(resp.error());
        return resp.value();
    }

    fw::CoRpcResp Missing(fw::CoRpcReq req) {
        // 失败路径：static_routes 未配置 "no.such.service" →
        // find_route NotFound，不发起任何网络 I/O。
        auto resp = this->call("no.such.service", "ping", req);
        if (!resp) return fw::CoRpcResp::Error(resp.error());
        return resp.value();
    }

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&GatewaySvc::Fetch>("fetch"),
        fw::Method<&GatewaySvc::Store>("store"),
        fw::Method<&GatewaySvc::Explode>("explode"),
        fw::Method<&GatewaySvc::SlowCall>("slowcall"),
        fw::Method<&GatewaySvc::Missing>("missing"));
};

std::atomic_bool g_stop{false};

void InstallSignals() {
    std::signal(SIGINT,  [](int) { g_stop.store(true); });
    std::signal(SIGTERM, [](int) { g_stop.store(true); });
}

} // namespace

int main(int argc, char** argv) {
    // 用法: svc_b <listen_port> <storage_endpoint host:port>
    if (argc != 3) {
        std::fprintf(stderr,
            "usage: svc_b <listen_port> <storage_endpoint host:port>\n");
        return 64;
    }
    const auto port = static_cast<std::uint16_t>(std::stoi(argv[1]));
    const std::string storage_ep = argv[2];

    // handler 内嵌套出站调用（入站 → this->call → HTTP client →
    // 等回包），协程栈按 F1-b2 实测放大到 64KB。
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;

    fw::CoAppOptions opts;
    opts.network_limits = bbt::infra::NetworkLimits{
        /*max_connections*/ 128,
        /*max_inflight*/    64,
        /*max_header_bytes*/16384,
        /*max_body_bytes*/  65536,
        /*incoming_timeout*/std::chrono::milliseconds{30000}};
    opts.listen = bbt::infra::ListenAddress{"127.0.0.1", port};
    // 出站白名单：仅允许名为 "storage" 的逻辑目标，endpoint 为真实
    // host:port——没有自动发现，也未配置的目标一律 NotFound。
    opts.static_routes = {
        fw::StaticRoute{"storage",
                        bbt::infra::RpcAddress{"http", storage_ep}}};
    opts.shutdown_step_budget = std::chrono::milliseconds{2000};

    fw::CoApp app{opts};
    if (auto r = app.add_service<GatewaySvc>(fw::ServiceOptions{
            fw::ExecutionPolicy::Concurrent,
            /*max_inflight*/ 64, 0, 0, false, 0, 0, 0});
        !r) {
        std::fprintf(stderr, "[svc_b] add_service failed: %s\n",
                     r.error().message.c_str());
        return 65;
    }

    InstallSignals();
    std::thread watcher([&app] {
        while (!g_stop.load(std::memory_order_acquire))
            std::this_thread::sleep_for(std::chrono::milliseconds{20});
        app.request_shutdown();
    });

    std::printf("[svc_b] gateway service starting on 127.0.0.1:%u "
                "(storage -> %s)\n", port, storage_ep.c_str());
    std::fflush(stdout);
    const int rc = app.run();
    g_stop.store(true, std::memory_order_release);
    watcher.join();
    std::printf("[svc_b] run() returned %d, shutdown_state=%d\n",
                rc, static_cast<int>(app.shutdown_state()));
    std::fflush(stdout);
    return rc;
}
