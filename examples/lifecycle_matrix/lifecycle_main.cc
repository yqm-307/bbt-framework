// bbt-framework Issue #5（EX-T4）：容量 / 并发上下文 / 关闭生命周期验收 fixture。
//
// 形态：**一个自有进程**（真实公开 `CoApp` + 真实 infra HTTP loopback 宿主 +
// 正式 ProtoWireV1 body profile）注册一个公开 `CoService<T>`
// （lifecycle::LifecycleGateService，复用 #4 getvalue schema/codec）。外部
// Python 驱动器用 #4 的手写 proto3 wire codec 打真实 POST /rpc。
//
// 覆盖（真实公开入口；不使用 internal/、Binder、CoAppSeam、MakeCoAppForTest、
// DispatchInboundForTest、INetworkHost 桩或任何发送注入；不额外创建 io_thread）：
//   R3 并发挂起 / 上下文跨 worker 不串 / 迟到完成不悬空；
//   R4 服务容量（max_inflight）耗尽 → Overloaded（不无界排队）/ 字节上限；
//   R5 正常拒新→drain→同步 Close→release；ShutdownIncomplete 保留强持有、
//      迟到完成；外部 supervisor 硬杀只留「未完成」证据、不称优雅。
//
// 验收控制面（**仅验收用**，不是生产注入）：stdin 逐行命令
//   `shutdown` → request_shutdown()；`open` → 放行验收闩；`quit`/EOF → 收尾。
// 断言全部基于 journal 事件与进程退出码，不用固定 sleep 猜时序。
//
// argv: fixture <port_file> <meta_file> <journal_file> <max_inflight> <step_budget_ms>
// 全部结构化结果标注 auth_state=unauthenticated_loopback。

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/infra/ICoCloseable.hpp>

#include <bbt/framework/CoApp.hpp>

#include "lifecycle_service.hpp"   // 验收服务 + 验收闩（复用 #4 getvalue codec）

namespace fw  = bbt::framework;
namespace inf = bbt::infra;

namespace {

using lifecycle::kAuthState;

std::mutex    g_journal_mtx;
std::ofstream g_journal;

void Journal(const std::string& line) {
    std::lock_guard<std::mutex> lk(g_journal_mtx);
    g_journal << line << '\n';
    g_journal.flush();
}

// 生命周期观测资源（公开 add_resource<ICoCloseable> 面）：断言关闭序列在
// handler 排空后**同步** Close 一次；ShutdownIncomplete 期间不得被 Close。
// 不伪造任何后端。
class CloseJournal final : public inf::ICoCloseable {
public:
    void Close() noexcept override {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_closed = true;
        }
        Journal(std::string("event=resource_close auth_state=") + kAuthState);
    }
    bool IsClosed() const noexcept override {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_closed;
    }

private:
    mutable std::mutex m_mtx;
    bool               m_closed = false;
};

// 动态 loopback 端口：内核分配后立即释放，供 listen 使用。就绪后严格核对
// bound_endpoint 端口等于所选端口，不符即失败（竞态落败报错，不假通过）。
std::uint16_t PickLoopbackPort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return 0;
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(fd);
        return 0;
    }
    const std::uint16_t port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

fw::ServiceOptions ConcurrentOpts(std::size_t max_inflight) {
    return fw::ServiceOptions{fw::ExecutionPolicy::Concurrent,
                              max_inflight,
                              /*mailbox_capacity*/ 0, /*max_actors*/ 0,
                              /*ordered_ingress*/ false,
                              /*max_ordered_streams*/ 0,
                              /*max_cached_results*/ 0,
                              /*max_cached_result_bytes*/ 0};
}

std::string Trim(const std::string& s) {
    const auto b = s.find_first_not_of(" \t\r\n");
    if (b == std::string::npos) return {};
    const auto e = s.find_last_not_of(" \t\r\n");
    return s.substr(b, e - b + 1);
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc < 7 || std::strcmp(argv[1], "fixture") != 0)
        return 90;   // EX_USAGE
    const std::string port_file     = argv[2];
    const std::string meta_file     = argv[3];
    const std::string journal_path  = argv[4];
    const std::size_t max_inflight  = std::strtoul(argv[5], nullptr, 10);
    const long        step_budget   = std::strtol(argv[6], nullptr, 10);

    g_journal.open(journal_path, std::ios::out | std::ios::trunc);
    if (!g_journal) {
        std::fprintf(stderr, "fixture: cannot open journal %s\n",
                     journal_path.c_str());
        return 6;
    }
    lifecycle::SetJournal(&Journal);

    // handler 内做 protobuf 编解码 + 入站分发，默认 12KB 协程栈不足；经
    // coroutine 公开配置宏（须在调度器 Start 前设置；process-global）放大到
    // 512KB 并给足静态线程（4），使并发挂起与入站接纳互不阻塞。本进程在
    // app.run() 前设置——此刻调度器尚未初始化（同 #4 示例口径）。
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size        = 512 * 1024;

    const std::uint16_t port = PickLoopbackPort();
    if (port == 0) {
        std::fprintf(stderr, "fixture: cannot pick a free loopback port\n");
        return 6;
    }
    const std::string endpoint = "127.0.0.1:" + std::to_string(port);

    fw::CoAppOptions options;
    options.network_limits = inf::NetworkLimits{
        /*max_connections*/ 64, /*max_inflight*/ 64,
        /*max_header_bytes*/ 16 * 1024,
        /*max_body_bytes*/ 4096,   // R4「字节上限」维度（功能检查非压测）
        /*incoming_timeout*/ std::chrono::milliseconds{30000}};
    options.listen               = inf::ListenAddress{"127.0.0.1", port};
    options.static_routes        = {};   // 无出站目标：未配置即 NotFound
    options.shutdown_step_budget = std::chrono::milliseconds{step_budget};
    options.inbound_bridge       = fw::RpcInboundBridge::ProtoWireV1;

    fw::CoApp app{options};
    if (auto r = app.add_service<lifecycle::LifecycleGateService>(
            ConcurrentOpts(max_inflight));
        !r) {
        std::fprintf(stderr, "fixture: add_service failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }
    auto owner = std::make_shared<CloseJournal>();
    if (auto r = app.add_resource<CloseJournal>(owner); !r) {
        std::fprintf(stderr, "fixture: add_resource failed: %s\n",
                     r.error().message.c_str());
        return 4;
    }

    int rc = -1;
    std::atomic<bool> run_done{false};
    std::thread runner([&] {
        rc = app.run();
        run_done.store(true, std::memory_order_release);
    });

    std::string bound;
    for (int i = 0; i < 400; ++i) {
        bound = app.bound_endpoint();
        if (!bound.empty()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds{25});
    }
    if (bound != endpoint) {
        app.request_shutdown();
        lifecycle::Gate().Open();
        if (runner.joinable()) runner.join();
        std::fprintf(stderr,
                     "fixture: bind mismatch: picked=%s bound='%s' rc=%d\n",
                     endpoint.c_str(), bound.c_str(), rc);
        return 5;
    }

    {
        std::ofstream f(port_file, std::ios::out | std::ios::trunc);
        f << port;
    }
    {
        std::ofstream f(meta_file, std::ios::out | std::ios::trunc);
        f << "process=lifecycle\n";
        f << "service=" << lifecycle::LifecycleGateService::kServiceName
          << "\n";
        f << "method_hold=Hold\nmethod_fast=Fast\n";
        f << "request_schema="
          << bbt::example::v1::GetValueRequest::descriptor()->full_name()
          << "\n";
        f << "response_schema="
          << bbt::example::v1::GetValueResponse::descriptor()->full_name()
          << "\n";
        f << "inbound_bridge=proto_wire_v1\n";
        f << "max_inflight=" << max_inflight << "\n";
        f << "step_budget_ms=" << step_budget << "\n";
        f << "auth_state=" << kAuthState << "\n";
    }
    Journal(std::string("event=ready endpoint=") + endpoint +
            " max_inflight=" + std::to_string(max_inflight) +
            " step_budget_ms=" + std::to_string(step_budget) +
            " auth_state=" + kAuthState);
    std::fprintf(stderr, "lifecycle fixture up endpoint=%s max_inflight=%zu\n",
                 endpoint.c_str(), max_inflight);

    // 关闭相位观测线程：轮询 shutdown_state 并在**变化时**记录（观察即成事件，
    // 驱动器据 journal 事件推进，不靠固定 sleep 猜时序）。run 结束即退出。
    std::thread observer([&] {
        int last = -1;
        while (!run_done.load(std::memory_order_acquire) ||
               app.shutdown_state() != fw::ShutdownState::Running) {
            const int now = static_cast<int>(app.shutdown_state());
            if (now != last) {
                last = now;
                Journal("event=shutdown_state state=" + std::to_string(now) +
                        " pending=" + std::to_string(app.pending_cleanup().size()) +
                        " auth_state=" + kAuthState);
            }
            if (run_done.load(std::memory_order_acquire)) break;
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
        }
    });

    // 验收控制面：stdin 逐行命令（见文件头）。
    std::string line;
    while (std::getline(std::cin, line)) {
        const std::string cmd = Trim(line);
        if (cmd == "quit") {
            break;
        } else if (cmd == "shutdown") {
            Journal(std::string("event=shutdown_requested auth_state=") +
                    kAuthState);
            app.request_shutdown();
        } else if (cmd == "open") {
            Journal(std::string("event=gate_opened auth_state=") + kAuthState);
            lifecycle::Gate().Open();
        }
        // 未知命令忽略（不改变状态）。
    }

    app.request_shutdown();
    if (runner.joinable()) runner.join();
    run_done.store(true, std::memory_order_release);
    if (observer.joinable()) observer.join();

    const int state = static_cast<int>(app.shutdown_state());
    Journal(std::string("event=shutdown rc=") + std::to_string(rc) +
            " state=" + std::to_string(state) + " owner_closed=" +
            (owner->IsClosed() ? "1" : "0") + " pending=" +
            std::to_string(app.pending_cleanup().size()) + " failures=" +
            std::to_string(app.lifecycle_failures().size()) +
            " auth_state=" + kAuthState);
    std::fprintf(stderr,
                 "lifecycle fixture rc=%d state=%d owner_closed=%d\n", rc,
                 state, owner->IsClosed() ? 1 : 0);
    return rc;
}
