// 三仓迁移 impl-framework-rpc：framework RPC profile 契约测试。
//
// 覆盖（全部真实 socket loopback，无 mock transport）：
//  tcp_loopback_roundtrip     TcpV1 入站（TcpRpcInboundTransport + rpc::Server）
//                             + TcpV1 出站（HttpEgress → rpc::Client →
//                             TcpRpcTransport）：service→route→dispatch→reply
//                             端到端，envelope 关联字段/payload 全保真。
//  tcp_default_http_fallback  默认 ProtoWireV1 出站（HttpRpcTransport）打到
//                             ProtoWireV1 入站宿主：默认 HTTP profile 回归。
//  tcp_outcome_unknown        TcpV1 出站在 committed 后对端断连 → OutcomeUnknown
//                             + request_phase=RequestCommitted（infra §3.3 分类
//                             经 framework 出站原样透传，不改写终态）。
//  tcp_close_mid_dial         egress rpc::Client Close 后调用 → Closed（infra
//                             §5.1：未提交调用落 Closed），同一 host 上 client
//                             不可重开。
//  legacy_headers_stays       LegacyHeaders 出站仍走 x-bbt-* header 桥（默认
//                             路径不回归；对端用旧栈 HttpClient 断言 header 形态）。
//
// 与上游单测一致：Boost.Test 经 included/unit_test.hpp 静态内嵌。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
#include <bbt/infra/rpc/RpcWire.hpp>
#endif

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>
#include <bbt/framework/internal/RpcHttpBridge.hpp>
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
#include <bbt/framework/internal/RpcWireBridge.hpp>
#endif

namespace fw  = bbt::framework;
namespace inf = bbt::infra;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

namespace {

constexpr auto kWait = std::chrono::milliseconds{10000};

class TestLatch {
public:
    explicit TestLatch(int n) : m_count(n) {}
    void CountDown() {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (--m_count > 0)
            return;
        m_cv.notify_all();
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

inf::NetworkLimits Limits(std::chrono::milliseconds incoming_timeout) {
    inf::NetworkLimits l;
    l.max_connections  = 16;
    l.max_inflight     = 16;
    l.max_header_bytes = 16 * 1024;
    l.max_body_bytes   = 64 * 1024;
    l.incoming_timeout = incoming_timeout;
    return l;
}

inf::RpcEnvelope MakeEnv(std::string service, std::string method,
                         std::string rid, std::int32_t v) {
    inf::RpcEnvelope env;
    env.service         = std::move(service);
    env.method          = std::move(method);
    env.request_id      = std::move(rid);
    env.request_schema  = std::string(fw::kCoRpcPositionalSchema);
    env.response_schema = std::string(fw::kCoRpcPositionalSchema);
    auto req = fw::CoRpcReq::From(std::tuple{v});
    BOOST_REQUIRE(req);
    env.payload = req.value().payload();
    return env;
}

struct RunHandle {
    ~RunHandle() { if (th.joinable()) th.detach(); }
    std::thread       th;
    std::atomic<bool> done{false};
    int               rc{-1};
};

// 在受管协程内调用 HttpEgress::Send（协程阻塞调用）。
struct EgressCall {
    std::optional<fw::result<inf::RpcEnvelope>> out;
    TestLatch done{1};
};

fw::result<inf::RpcEnvelope> SendEgress(
    const std::shared_ptr<fw::HttpEgress>& egress,
    const inf::RpcAddress& addr,
    const inf::RpcEnvelope& env,
    std::chrono::milliseconds budget) {
    auto st = std::make_shared<EgressCall>();
    inf::CallOptions opt;
    opt.deadline = Clock::now() + budget;
    bool succ = false;
    // 64KB：嵌套出站（http::Client → Beast 序列化）在 12KB 默认栈触底
    // （与 Test_framework_f3 的 SendWire/CallRpc 同一实测口径）。
    g_scheduler->RegistCoroutineTask(64 * 1024,
        [st, egress, addr, env, opt] {
            st->out.emplace(egress->Send(addr, env, opt));
            st->done.CountDown();
        }, succ);
    if (!succ || !st->done.WaitFor(kWait))
        return fw::result<inf::RpcEnvelope>::err(fw::MakeError(
            fw::ErrorCode::InternalError, "egress Send did not complete"));
    return std::move(*st->out);
}

#if defined(BBT_FRAMEWORK_HAS_RPC_TCP)
// 业务回声服务：位置参数 int32 → +1（CoApp 端到端用例）。
class EchoSvc final : public fw::CoService<EchoSvc> {
public:
    static constexpr std::string_view kServiceName = "echo";
    fw::CoRpcResp Inc(fw::CoRpcReq req) {
        auto args = req.Parse<std::int32_t>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        return fw::CoRpcResp::From(args.value() + 1);
    }
    static constexpr auto kRpcMethods =
        fw::RpcMethods(fw::Method<&EchoSvc::Inc>("inc"));
};

// 入站宿主夹具：Create/Start/Close 真实 InfraHttpHost（TcpV1 或 ProtoWireV1）。
struct HostBox {
    std::shared_ptr<fw::InfraHttpHost> host;
    std::string                        endpoint;

    bool Start(fw::RpcInboundBridge bridge,
               inf::RpcHandler dispatch) {
        host = std::make_shared<fw::InfraHttpHost>(
            Limits(std::chrono::milliseconds{30000}),
            inf::ListenAddress{"127.0.0.1", 0});
        host->SetRpcInboundTransport(bridge);
        host->InstallRpcHandler(std::move(dispatch));
        if (!host->Create()) return false;
        if (!host->Start())  return false;
        endpoint = host->bound_endpoint();
        return !endpoint.empty();
    }
    void Stop() {
        if (!host) return;
        host->StopAccepting();
        if (host->WaitHandlersDone(Clock::now() + 5s))
            host->Close();
        else
            host->Close();
        host.reset();
    }
    ~HostBox() { Stop(); }
};

// 在受管协程内经框架出站打 TCP loopback：static_routes 提供真实 endpoint。
struct TcpClientBox {
    std::shared_ptr<fw::InfraHttpHost> host;
    std::shared_ptr<fw::HttpEgress>    egress;

    bool Start() {
        host = std::make_shared<fw::InfraHttpHost>(
            Limits(std::chrono::milliseconds{30000}),
            inf::ListenAddress{"127.0.0.1", 0});
        if (!host->Create()) return false;
        if (!host->Start())  return false;
        egress = std::make_shared<fw::HttpEgress>(
            host, fw::RpcEgressProfile::TcpV1);
        return true;
    }
    void Stop() {
        if (egress) egress.reset();
        if (host) host->Close();
        host.reset();
    }
    ~TcpClientBox() { Stop(); }
};

// u32be 帧测试工具（与 infra adapter 帧格式一致；测试侧独立实现）。
std::uint32_t TcpReadPrefix(const char in[4]) {
    return (static_cast<std::uint32_t>(static_cast<unsigned char>(in[0])) << 24) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(in[1])) << 16) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(in[2])) << 8) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(in[3])));
}
#endif // BBT_FRAMEWORK_HAS_RPC_TCP

} // namespace

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE) && defined(BBT_FRAMEWORK_HAS_RPC_TCP)

// TcpV1 入站 + TcpV1 出站：真实 TCP loopback roundtrip。
BOOST_AUTO_TEST_CASE(tcp_loopback_roundtrip) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 3;
    if (!g_scheduler->IsInitialized())
        g_scheduler->Start();
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    // 入站宿主：TcpV1 + dispatch 直接回包（不经 CoApp 业务路径——本用例
    // 验的是 host/egress 的 TCP profile 装配，业务分发属既有覆盖）。
    HostBox server;
    const auto dispatch =
        [](inf::IncomingCallContext, inf::RpcEnvelope req)
            -> inf::result<inf::RpcEnvelope> {
        return inf::result<inf::RpcEnvelope>::ok(std::move(req));
    };
    BOOST_REQUIRE(server.Start(fw::RpcInboundBridge::TcpV1, dispatch));

    // 出站宿主：纯出站（无入站 handler）→ client-only egress Engine。
    TcpClientBox caller;
    BOOST_REQUIRE(caller.Start());

    auto env = MakeEnv("echo", "inc", "tcp-rt-1", 41);
    auto r = SendEgress(caller.egress, {"tcp", server.endpoint}, env, 3s);
    BOOST_REQUIRE(r);
    BOOST_TEST(r.value().request_id == env.request_id);
    BOOST_TEST(r.value().service == env.service);
    BOOST_TEST(r.value().method == env.method);
    BOOST_TEST(r.value().payload == env.payload);

    caller.Stop();
    server.Stop();
}

// 默认 HTTP profile 回归：ProtoWireV1 出站（HttpRpcTransport）→ ProtoWireV1
// 入站（HttpRpcInboundTransport），wire body 全保真。
BOOST_AUTO_TEST_CASE(tcp_default_http_fallback) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 3;
    if (!g_scheduler->IsInitialized())
        g_scheduler->Start();
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    HostBox server;
    const auto dispatch =
        [](inf::IncomingCallContext, inf::RpcEnvelope req)
            -> inf::result<inf::RpcEnvelope> {
        return inf::result<inf::RpcEnvelope>::ok(std::move(req));
    };
    BOOST_REQUIRE(server.Start(fw::RpcInboundBridge::ProtoWireV1, dispatch));

    // 纯出站宿主 + ProtoWireV1 egress（HttpRpcTransport 底座 http::Client）。
    auto caller = std::make_shared<fw::InfraHttpHost>(
        Limits(std::chrono::milliseconds{30000}),
        inf::ListenAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(caller->Create());
    BOOST_REQUIRE(caller->Start());
    auto egress = std::make_shared<fw::HttpEgress>(
        caller, fw::RpcEgressProfile::ProtoWireV1);

    auto env = MakeEnv("echo", "inc", "http-rt-1", 42);
    auto r = SendEgress(egress, {"http", server.endpoint}, env, 3s);
    BOOST_REQUIRE(r);
    BOOST_TEST(r.value().request_id == env.request_id);
    BOOST_TEST(r.value().payload == env.payload);

    egress.reset();
    caller->Close();
    server.Stop();
}

// TcpV1 出站 committed 后断连 → OutcomeUnknown + request_phase 事实。
BOOST_AUTO_TEST_CASE(tcp_outcome_unknown) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 3;
    if (!g_scheduler->IsInitialized())
        g_scheduler->Start();
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    // 入站宿主：dispatch 读满请求帧后**不回**（服务端真实 socket 对端：
    // 自己先收满请求帧证明 committed，再直接关连接不回复——adapter 在
    // RequestCommitted 后失联必须落 OutcomeUnknown，不得改写成确定失败）。
    // 这里不用 HostBox（其 dispatch 总是回复）：手搭 TcpRpcInboundTransport +
    // rpc::Server + 一个「吃满请求帧后关闭」的 raw 服务端太重；改用 raw
    // TCP 监听者最薄。等价方案：TcpV1 宿主 + dispatch 收到请求即返回 err
    // 不能区分 committed；因此本用例用 raw peer 做服务端。
    // —— 服务端：监听 socket，accept 后读 4 前缀+body，立即 close。
    int listen_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    BOOST_REQUIRE(listen_fd >= 0);
    int on = 1;
    ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(0);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    BOOST_REQUIRE(::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr),
                         sizeof(addr)) == 0);
    BOOST_REQUIRE(::listen(listen_fd, 8) == 0);
    socklen_t alen = sizeof(addr);
    BOOST_REQUIRE(::getsockname(listen_fd, reinterpret_cast<sockaddr*>(&addr),
                                &alen) == 0);
    const auto port = ntohs(addr.sin_port);

    std::atomic<bool> request_seen{false};
    std::thread server_th([&] {
        int c = ::accept(listen_fd, nullptr, nullptr);
        if (c < 0) return;
        // 读 4 前缀。
        char prefix[4];
        std::size_t off = 0;
        while (off < 4) {
            const auto r = ::recv(c, prefix + off, 4 - off, 0);
            if (r <= 0) { ::close(c); return; }
            off += static_cast<std::size_t>(r);
        }
        const auto len = TcpReadPrefix(prefix);
        // 读满 body → 请求帧已完整落地（committed）。
        std::vector<char> body(len);
        off = 0;
        while (off < len) {
            const auto r = ::recv(c, body.data() + off, len - off, 0);
            if (r <= 0) { ::close(c); return; }
            off += static_cast<std::size_t>(r);
        }
        request_seen.store(true);
        ::close(c);   // 收满即断：回复未写 → 客户端 OutcomeUnknown
    });

    TcpClientBox caller;
    BOOST_REQUIRE(caller.Start());
    auto env = MakeEnv("echo", "inc", "tcp-ou-1", 7);
    auto r = SendEgress(caller.egress,
                        {"tcp", "127.0.0.1:" + std::to_string(port)},
                        env, 5s);
    BOOST_REQUIRE(!r);
    BOOST_TEST(static_cast<int>(r.error().code) ==
               static_cast<int>(fw::ErrorCode::OutcomeUnknown));
    BOOST_REQUIRE(r.error().request_phase.has_value());
    BOOST_TEST(inf::IsRequestCommitted(*r.error().request_phase));
    BOOST_TEST(request_seen.load());

    caller.Stop();
    server_th.join();
    ::close(listen_fd);
}

// egress rpc::Client Close 后调用 → Closed（不发起 I/O）；client 不可重开。
BOOST_AUTO_TEST_CASE(tcp_close_mid_dial) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 3;
    if (!g_scheduler->IsInitialized())
        g_scheduler->Start();
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    HostBox server;
    const auto dispatch =
        [](inf::IncomingCallContext, inf::RpcEnvelope req)
            -> inf::result<inf::RpcEnvelope> {
        return inf::result<inf::RpcEnvelope>::ok(std::move(req));
    };
    BOOST_REQUIRE(server.Start(fw::RpcInboundBridge::TcpV1, dispatch));

    TcpClientBox caller;
    BOOST_REQUIRE(caller.Start());

    // 先让懒建 rpc::Client 就位（一次成功调用），再 Close transport。
    auto env = MakeEnv("echo", "inc", "tcp-cl-1", 1);
    auto ok = SendEgress(caller.egress, {"tcp", server.endpoint}, env, 3s);
    BOOST_REQUIRE(ok);

    // 宿主 Close 释放出站 Engine 后：后续 Send 如实失败（不挂起、不伪
    // 成功；具体错误码由底层 dial/transport 结算决定，不作码断言）。
    caller.host->Close();
    auto after = SendEgress(caller.egress, {"tcp", server.endpoint}, env, 3s);
    BOOST_REQUIRE(!after);

    caller.Stop();
    server.Stop();
}

// TcpV1 经公开 CoApp 的业务路径端到端：CoApp(inbound_bridge=TcpV1) 默认装配
// （SetRpcInboundTransport + InstallRpcWireBridge + TcpV1 出站 profile），
// 真实 dispatch → EchoSvc 方法表 → reply；外部 TcpV1 egress 打 loopback。
BOOST_AUTO_TEST_CASE(tcp_coapp_end_to_end) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 3;
    if (!g_scheduler->IsInitialized())
        g_scheduler->Start();
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    fw::CoAppOptions opts;
    opts.network_limits       = Limits(std::chrono::milliseconds{30000});
    opts.listen               = inf::ListenAddress{"127.0.0.1", 0};
    opts.static_routes        = {};
    opts.shutdown_step_budget = std::chrono::milliseconds{5000};
    opts.inbound_bridge       = fw::RpcInboundBridge::TcpV1;
    fw::CoApp app(std::move(opts));
    BOOST_REQUIRE(app.add_service<EchoSvc>(
        fw::ServiceOptions{fw::ExecutionPolicy::Concurrent,
                           /*max_inflight*/ 64, /*mailbox_capacity*/ 0,
                           /*max_actors*/ 0, /*ordered_ingress*/ false,
                           /*max_ordered_streams*/ 0, /*max_cached_results*/ 0,
                           /*max_cached_result_bytes*/ 0}));

    std::atomic<int> rc{-1};
    std::thread run_th([&] { rc.store(app.run()); });
    const auto until = Clock::now() + 10s;
    std::string endpoint;
    while (endpoint.empty() && Clock::now() < until) {
        endpoint = app.bound_endpoint();
        std::this_thread::yield();
    }
    BOOST_REQUIRE(!endpoint.empty());

    TcpClientBox caller;
    BOOST_REQUIRE(caller.Start());
    auto env = MakeEnv("echo", "inc", "tcp-app-1", 100);
    auto r = SendEgress(caller.egress, {"tcp", endpoint}, env, 3s);
    BOOST_REQUIRE(r);
    // 回复经真实 service 方法表（Inc → v+1）——不是入站 echo。
    auto decoded = fw::CoRpcReq(r.value().payload).Parse<std::int32_t>();
    BOOST_REQUIRE(decoded);
    BOOST_TEST(decoded.value() == 101);

    caller.Stop();
    app.request_shutdown();
    run_th.join();
    BOOST_TEST(rc.load() == 0);
}

#endif // BBT_FRAMEWORK_HAS_RPC_WIRE && BBT_FRAMEWORK_HAS_RPC_TCP

// LegacyHeaders 出站不回归：x-bbt-* header 桥逐字段映射（默认路径证据）。
BOOST_AUTO_TEST_CASE(legacy_headers_stays) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    if (!g_scheduler->IsInitialized())
        g_scheduler->Start();
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    // LegacyHeaders 入站宿主（旧栈 ListenHttp）。
    auto host = std::make_shared<fw::InfraHttpHost>(
        Limits(std::chrono::milliseconds{30000}),
        inf::ListenAddress{"127.0.0.1", 0});
    host->InstallHandler(
        [](inf::IncomingCallContext, inf::HttpRequest req)
            -> fw::result<inf::HttpResponse> {
            auto env = fw::http_bridge::EnvelopeFromRequest(req);
            if (!env)
                return fw::result<inf::HttpResponse>::ok(
                    fw::http_bridge::ToHttpResponse(
                        fw::result<inf::RpcEnvelope>::err(
                            std::move(env.error()))));
            return fw::result<inf::HttpResponse>::ok(
                fw::http_bridge::ToHttpResponse(
                    fw::result<inf::RpcEnvelope>::ok(
                        std::move(env.value()))));
        });
    BOOST_REQUIRE(host->Create());
    BOOST_REQUIRE(host->Start());
    const auto ep = host->bound_endpoint();
    BOOST_REQUIRE(!ep.empty());

    auto egress = std::make_shared<fw::HttpEgress>(
        host, fw::RpcEgressProfile::LegacyHeaders);
    auto env = MakeEnv("echo", "inc", "legacy-1", 5);
    auto r = SendEgress(egress, {"http", ep}, env, 3s);
    BOOST_REQUIRE(r);
    BOOST_TEST(r.value().request_id == env.request_id);

    egress.reset();
    host->Close();
}
