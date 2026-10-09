// bbt-framework Issue #4（P0-A）：Service→Service 正式出站 caller 进程。
//
// 与 getvalue_server 同装配口径：公开 CoApp（inbound_bridge=ProtoWireV1）
// + 公开 CoService，不使用测试缝。宿主 GetValueCallerService；其 handler 内
// 经 ICoService::call<Req,Resp> 向另一个 framework CoApp（callee 进程）发起
// 真实正式 body 出站。监听 loopback 动态端口，写端口/元数据文件，阻塞等待
// stdin EOF 后按序优雅关闭。
//
// argv:
//   getvalue_caller caller <port_file> <callee_ep> <blackhole_ep> <meta_file>
//     callee_ep / blackhole_ep 形如 127.0.0.1:<port>（静态路由 endpoint 形态）。
//   getvalue_caller badroute <kind> <result_file>
//     #5 EX-T3：以公开入口验证四类非法静态路由在 run 启动任何组件前被拒绝。
//     kind ∈ {empty-service, empty-transport, empty-endpoint, duplicate}；
//     写出机器可读观测（rc/lifecycle_failures/bound_endpoint/资源工厂计数）。

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include <bbt/coroutine/detail/GlobalConfig.hpp>

#include <bbt/example/v1/get_value.pb.h>

#include <bbt/framework/CoApp.hpp>

#include "getvalue_caller.hpp"

namespace {

// 「启动任何组件前拒绝」的公开面额外证据：该延迟工厂只在 run() 通过配置
// 校验、进入资源启动相位（on_scheduler_started）时被调用一次。被拒绝的
// 配置下计数必须保持 0——证明拒绝发生在资源/网络/服务启动之前。
std::atomic<int> g_probe_factory_calls{0};
struct ProbeResource {};

// caller 与 badroute 共用的基础装配（除 static_routes 外完全一致）。
bbt::framework::CoAppOptions BaseOptions() {
    bbt::framework::CoAppOptions options;
    options.network_limits = bbt::infra::NetworkLimits{
        64, 64, 16 * 1024, 64 * 1024, std::chrono::milliseconds{30000}};
    options.listen = bbt::infra::ListenAddress{"127.0.0.1", 0};
    options.shutdown_step_budget = std::chrono::milliseconds{3000};
    options.inbound_bridge = bbt::framework::RpcInboundBridge::ProtoWireV1;
    return options;
}

bool IsKnownBadRouteKind(const std::string& kind) {
    return kind == "empty-service" || kind == "empty-transport" ||
           kind == "empty-endpoint" || kind == "duplicate";
}

// 四类非法静态路由（其余字段保持与正常 caller 相同的合法口径），供公开
// CoApp 装配后在 run 前被 _ValidateConfig 拒绝：
//   empty-service   空 service_name
//   empty-transport 空 transport（endpoint 非空）
//   empty-endpoint  空 endpoint（transport 非空）
//   duplicate       两条相同 service_name
std::vector<bbt::framework::StaticRoute> BadRoutes(const std::string& kind) {
    using bbt::framework::StaticRoute;
    using bbt::infra::RpcAddress;
    if (kind == "empty-service")
        return {StaticRoute{"", RpcAddress{"http", "127.0.0.1:1"}}};
    if (kind == "empty-transport")
        return {StaticRoute{"svc.route", RpcAddress{"", "127.0.0.1:1"}}};
    if (kind == "empty-endpoint")
        return {StaticRoute{"svc.route", RpcAddress{"http", ""}}};
    if (kind == "duplicate")
        return {StaticRoute{"svc.route", RpcAddress{"http", "127.0.0.1:1"}},
                StaticRoute{"svc.route", RpcAddress{"http", "127.0.0.1:2"}}};
    return {};
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

// #5 EX-T3 公开入口：装配非法静态路由 → add_service/add_resource → run()。
// 断言依据是 run() 的返回码与公开观测口（lifecycle_failures/bound_endpoint/
// 资源工厂计数），本进程只如实写观测，不打印硬编码期望值。
int RunBadRoute(const std::string& kind, const std::string& result_file) {
    bbt::framework::CoAppOptions options = BaseOptions();
    options.static_routes = BadRoutes(kind);

    bbt::framework::CoApp app(options);
    const bbt::framework::ServiceOptions svc_opts{
        bbt::framework::ExecutionPolicy::Concurrent, 64, 0, 0, false, 0, 0, 0};
    if (auto r = app.add_service<getvalue::GetValueCallerService>(svc_opts);
        !r) {
        std::fprintf(stderr, "badroute add_service failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }
    if (auto r = app.add_resource<ProbeResource>("probe",
            []() -> bbt::framework::result<std::shared_ptr<ProbeResource>> {
                g_probe_factory_calls.fetch_add(1);
                return bbt::framework::result<
                    std::shared_ptr<ProbeResource>>::ok(
                        std::make_shared<ProbeResource>());
            });
        !r) {
        std::fprintf(stderr, "badroute add_resource failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }

    // 坏配置 → _ValidateConfig 在任何组件启动前返回 kExitRejected(1)。
    const int rc = app.run();
    const std::vector<std::string> failures = app.lifecycle_failures();
    const std::string endpoint = app.bound_endpoint();

    std::ofstream f(result_file, std::ios::trunc);
    f << "{\n"
      << "  \"mode\": \"badroute\",\n"
      << "  \"kind\": \"" << JsonEscape(kind) << "\",\n"
      << "  \"rc\": " << rc << ",\n"
      << "  \"bound_endpoint\": \"" << JsonEscape(endpoint) << "\",\n"
      << "  \"resource_factory_calls\": "
      << g_probe_factory_calls.load() << ",\n"
      << "  \"lifecycle_failures\": [";
    for (std::size_t i = 0; i < failures.size(); ++i) {
        if (i != 0)
            f << ", ";
        f << "\"" << JsonEscape(failures[i]) << "\"";
    }
    f << "],\n"
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
                 g_probe_factory_calls.load());
    return rc;
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc < 2)
        return 90;
    const std::string mode = argv[1];

    // #5 EX-T3：非法静态路由的公开入口验收模式。
    if (mode == "badroute") {
        if (argc < 4 || !IsKnownBadRouteKind(argv[2]))
            return 90;
        return RunBadRoute(argv[2], argv[3]);
    }

    if (mode != "caller" || argc < 6)
        return 90;
    const std::string port_file = argv[2];
    const std::string callee_ep = argv[3];
    const std::string black_ep  = argv[4];
    const std::string meta_file = argv[5];

    // caller handler 内做 typed 出站（protobuf 编解码 + 框架分发），显式把
    // 协程栈调大（公开配置宏，须在调度器 Start 前设置；process-global）。
    g_bbt_coroutine_config->m_cfg_stack_size = 512 * 1024;

    bbt::framework::CoAppOptions options = BaseOptions();
    options.static_routes = {
        bbt::framework::StaticRoute{
            std::string(getvalue::GetValueCallerService::kCalleeService),
            bbt::infra::RpcAddress{"http", callee_ep}},
        bbt::framework::StaticRoute{
            std::string(getvalue::GetValueCallerService::kBlackholeService),
            bbt::infra::RpcAddress{"http", black_ep}},
    };

    bbt::framework::CoApp app(options);
    if (auto r = app.add_service<getvalue::GetValueCallerService>(
            bbt::framework::ServiceOptions{
                bbt::framework::ExecutionPolicy::Concurrent,
                64, 0, 0, false, 0, 0, 0});
        !r) {
        std::fprintf(stderr, "caller add_service failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }

    int rc = -1;
    std::thread runner([&] { rc = app.run(); });

    std::string endpoint;
    for (int i = 0; i < 400; ++i) {
        endpoint = app.bound_endpoint();
        if (!endpoint.empty())
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds{25});
    }
    if (endpoint.empty()) {
        app.request_shutdown();
        if (runner.joinable()) runner.join();
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
        f << "inbound_bridge=proto_wire_v1\n";
        f << "auth_state=unauthenticated_loopback\n";
    }
    std::fprintf(stderr, "getvalue caller up endpoint=%s callee=%s blackhole=%s\n",
                 endpoint.c_str(), callee_ep.c_str(), black_ep.c_str());

    char buf[8];
    while (::read(STDIN_FILENO, buf, sizeof(buf)) > 0) {
    }
    app.request_shutdown();
    if (runner.joinable())
        runner.join();
    std::fprintf(stderr, "getvalue caller rc=%d\n", rc);
    return rc;
}
