// bbt-framework Issue #4（P0-A）：Service→Service 正式出站 caller 进程。
//
// 与 getvalue_server 同装配口径：公开 CoApp（inbound_bridge=ProtoWireV1）
// + 公开 CoService，不使用测试缝。宿主 GetValueCallerService；其 handler 内
// 经 ICoService::call<Req,Resp> 向另一个 framework CoApp（callee 进程）发起
// 真实正式 body 出站。监听 loopback 动态端口，写端口/元数据文件，阻塞等待
// stdin EOF 后按序优雅关闭。
//
// argv: getvalue_caller caller <port_file> <callee_ep> <blackhole_ep> <meta_file>
//   callee_ep / blackhole_ep 形如 127.0.0.1:<port>（静态路由 endpoint 形态）。

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <thread>

#include <unistd.h>

#include <bbt/coroutine/detail/GlobalConfig.hpp>

#include <bbt/example/v1/get_value.pb.h>

#include <bbt/framework/CoApp.hpp>

#include "getvalue_caller.hpp"

int main(int argc, char* argv[]) {
    if (argc < 6 || std::strcmp(argv[1], "caller") != 0)
        return 90;
    const std::string port_file = argv[2];
    const std::string callee_ep = argv[3];
    const std::string black_ep  = argv[4];
    const std::string meta_file = argv[5];

    // caller handler 内做 typed 出站（protobuf 编解码 + 框架分发），显式把
    // 协程栈调大（公开配置宏，须在调度器 Start 前设置；process-global）。
    g_bbt_coroutine_config->m_cfg_stack_size = 512 * 1024;

    bbt::framework::CoAppOptions options;
    options.network_limits = bbt::infra::NetworkLimits{
        64, 64, 16 * 1024, 64 * 1024, std::chrono::milliseconds{30000}};
    options.listen = bbt::infra::ListenAddress{"127.0.0.1", 0};
    options.shutdown_step_budget = std::chrono::milliseconds{3000};
    options.inbound_bridge = bbt::framework::RpcInboundBridge::ProtoWireV1;
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
