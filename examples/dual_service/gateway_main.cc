// examples/dual_service/gateway_main.cc — svc_b：网关服务进程。
//
// 业务形态：单进程托管一个 Concurrent 服务 "gateway"，handler 内经
// this->call 真实跨进程调用 storage（static_routes 显式白名单，目标
// endpoint 由命令行传入）。演示：
//   fetch(key)          → storage.get 透传（含 miss → NotFound 透传）
//   store(key, value)   → storage.put 透传
//   explode()           → 业务错误（InternalError）不经网络直达客户端
//   slowcall(ms)        → storage.sleep_ms；显式子预算到期，服务端继承后 TimedOut
//   missing()           → 调用未路由服务名 → find_route NotFound 失败路径
//
// 协议：inbound_bridge=ProtoWireV1（与 examples/getvalue 同口径）。选正式 body
// wire 而非迁移期 header 桥，是因为只有 wire body 携带 remaining_budget_ms，
// 出站子预算/父预算才能真正到达对端服务端（见 SlowCall 注释）。
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

// slowcall 对 storage 的显式子预算：经 ProtoWireV1 body 的 remaining_budget_ms
// 传到对端，storage handler 可见前收敛为 min(传输硬看门, now+budget) 并据此
// 中断 sleep_ms。driver 用远大于它的客户端预算，故收到的 TimedOut 只能由
// 服务端自产——这是「服务端继承预算」而非「仅客户端超时」的可判定证据。
constexpr std::int64_t kSlowCallSubBudgetMs = 600;

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
        // ProtoWireV1 body 的 remaining_budget_ms 到达 storage，服务端继承后
        // 在其 deadline 到期时如实 TimedOut；本进程再把该错误透传给 driver。
        fw::CallOptions o;
        o.deadline = std::chrono::steady_clock::now() +
            std::chrono::milliseconds{kSlowCallSubBudgetMs};
        auto resp = this->call("storage", "sleep_ms", req, {}, o);
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
        fw::Method<&GatewaySvc::Del>("del"),
        fw::Method<&GatewaySvc::Explode>("explode"),
        fw::Method<&GatewaySvc::SlowCall>("slowcall"),
        fw::Method<&GatewaySvc::Missing>("missing"));
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

int main(int argc, char** argv) {
    // 用法: svc_b <listen_port> <storage_endpoint host:port>
    if (argc != 3) {
        std::fprintf(stderr,
            "usage: svc_b <listen_port> <storage_endpoint host:port>\n");
        return 64;
    }
    std::uint16_t port = 0;
    if (!ParsePort(argv[1], &port)) {
        std::fprintf(stderr,
            "[svc_b] FATAL: 非法 listen_port \"%s\"（需要 1..65535 的十进制端口）\n",
            argv[1]);
        return 64;   // EX_USAGE
    }
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
    // 正式 body wire（与 examples/getvalue 同口径）：预算字段随 wire 到达对端。
    opts.inbound_bridge = fw::RpcInboundBridge::ProtoWireV1;
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
