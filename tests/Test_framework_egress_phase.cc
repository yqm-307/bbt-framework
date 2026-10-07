// bbt-framework Issue #4 下游 request-phase/OutcomeUnknown 消费验收：
// R5 committed→OutcomeUnknown；另覆盖发送前过期、未提交失败、remaining budget；
// 不覆盖 R6/R7/R8 并发/容量/关闭矩阵。
// 消费已合入 infra 的公开 request-phase / OutcomeUnknown 契约
// （bbt::infra::RequestPhase、IsRequestCommitted、Error::request_phase），
// 在真实 framework HttpEgress（ProtoWireV1）上机器断言：
//   T1 发送前已过期     → TimedOut，request_phase=nullopt，服务端零连接（无 I/O）；
//   T2 连接被拒         → 未提交的确定失败（TransportError 保留），phase 未提交；
//   T3 完整写出后丢 reply → 升级为 OutcomeUnknown 且 phase=RequestCommitted，
//                        服务端只被连接一次（一次终态、无 retry）；
//   T4 remaining budget → 发送端从本地 deadline 换算出 remaining_budget_ms；
//                        deadline 明显超过 wire 上限时 clamp 到 kMaxRemainingBudgetMs。
//
// 对端是测试自有的裸 TCP loopback 端点（读完整个请求后不回复直接断开 / 拒绝连接），
// 不使用固定 sleep、不放宽断言、不解析错误消息猜阶段。断言只读公开
// Error / request_phase，不接触 Beast/Asio/socket 类型。
//
// 与上游单测一致：Boost.Test 经 included/unit_test.hpp 静态内嵌。

#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/RpcWire.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>
#include <bbt/framework/internal/RpcHttpBridge.hpp>

namespace fw  = bbt::framework;
namespace inf = bbt::infra;
using Clock = std::chrono::steady_clock;

namespace {

// 协程用例需要真实调度器；2 个静态 worker 足够（用例内不并发压测）。
// runtime 是进程寿命单例：只 Start 一次、无 Stop；已初始化则复用。
struct SchedulerFixture {
    SchedulerFixture() {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
        // 用例在协程内做 proto wire 编解码与嵌套调用；默认 12KB 栈在
        // 同仓 Test_framework_f1b2 的同一条出站链上已实测触底（guard 页
        // SIGSEGV）。按同口径在 Start() 前放大到 64 KiB。
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
        if (!g_scheduler->IsInitialized())
            g_scheduler->Start();
    }
};

class TestLatch {
public:
    explicit TestLatch(int n) : m_count(n) {}
    void CountDown() {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (--m_count > 0) return;
        }
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

constexpr std::chrono::milliseconds kWait{15000};

std::string ToLower(std::string s) {
    for (char& c : s)
        if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
    return s;
}

// 读一条完整 HTTP/1.1 请求（头部 + Content-Length body）。收回整条即表示请求
// 字节已完整到达对端；返回 false 表示读失败/连接被中断。
bool ReadHttpRequest(int fd, std::string& body_out) {
    // 有界收口：对已接受连接设接收超时，避免「连接后不发完整请求」时
    // ::recv 永久阻塞，导致 Stop() 的 join 只能靠 ctest 超时兜底。
    // 超时（EAGAIN/EWOULDBLOCK）经下方 `n <= 0` 收口为 false。
    timeval rcvto{1, 0};   // 1s
    if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &rcvto, sizeof(rcvto)) != 0)
        return false;
    std::string buf;
    char        tmp[4096];
    std::size_t header_end = std::string::npos;
    long        content_length = -1;
    for (;;) {
        if (header_end == std::string::npos) {
            header_end = buf.find("\r\n\r\n");
            if (header_end != std::string::npos) {
                const std::string head =
                    ToLower(buf.substr(0, header_end));
                const auto p = head.find("content-length:");
                content_length = (p == std::string::npos)
                    ? 0
                    : std::strtol(head.c_str() + p + 15, nullptr, 10);
            }
        }
        if (header_end != std::string::npos && content_length >= 0) {
            const std::size_t have_body = buf.size() - (header_end + 4);
            if (have_body >= static_cast<std::size_t>(content_length)) {
                body_out = buf.substr(header_end + 4,
                                      static_cast<std::size_t>(content_length));
                return true;
            }
        }
        const ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
        if (n <= 0) return false;
        buf.append(tmp, static_cast<std::size_t>(n));
        if (buf.size() > (1u << 20)) return false;   // 防御：不无界增长
    }
}

// 测试自有裸 TCP loopback 端点：接受连接 → 读完整个请求 → 不回复直接断开。
// 这是唯一形态（没有可切换的丢弃开关）：只有完整收下请求才置 commit 标记，
// 读失败/超时则不计。记录被连接次数，用于断言一次终态/无 retry。
struct LoopbackPeer {
    int                listen_fd = -1;
    std::uint16_t      port      = 0;
    std::atomic<int>   accepts{0};
    std::atomic<bool>  got_commit{false};
    std::atomic<bool>  body_ready{false};
    std::mutex         mtx;
    std::string        received_body;
    std::thread        th;
    std::atomic<bool>  stop{false};

    ~LoopbackPeer() { Stop(); }

    bool Start() {
        listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd < 0) return false;
        int one = 1;
        ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in a{};
        a.sin_family      = AF_INET;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        a.sin_port        = 0;
        if (::bind(listen_fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0)
            return false;
        socklen_t alen = sizeof(a);
        if (::getsockname(listen_fd, reinterpret_cast<sockaddr*>(&a), &alen) != 0)
            return false;
        port = ntohs(a.sin_port);
        if (::listen(listen_fd, 4) != 0) return false;
        const int flags = ::fcntl(listen_fd, F_GETFL, 0);
        if (flags < 0 || ::fcntl(listen_fd, F_SETFL, flags | O_NONBLOCK) != 0)
            return false;
        th = std::thread([this] { Serve(); });
        return true;
    }

    void Serve() {
        while (!stop.load(std::memory_order_acquire)) {
            fd_set rf;
            FD_ZERO(&rf);
            FD_SET(listen_fd, &rf);
            timeval tv{0, 100000};   // 100ms 轮询，仅为可停止；不用于推断时序
            const int r = ::select(listen_fd + 1, &rf, nullptr, nullptr, &tv);
            if (r <= 0) continue;
            const int c = ::accept(listen_fd, nullptr, nullptr);
            if (c < 0) continue;
            accepts.fetch_add(1, std::memory_order_acq_rel);
            std::string body;
            const bool ok = ReadHttpRequest(c, body);
            if (ok) {
                {
                    std::lock_guard<std::mutex> lk(mtx);
                    received_body = body;
                }
                body_ready.store(true, std::memory_order_release);
                got_commit.store(true, std::memory_order_release);
            }
            ::close(c);   // 不回复，直接断开（丢 reply）
        }
    }

    void Stop() {
        stop.store(true, std::memory_order_release);
        if (th.joinable()) th.join();
        if (listen_fd >= 0) {
            ::close(listen_fd);
            listen_fd = -1;
        }
    }

    std::string Endpoint() const {
        return "127.0.0.1:" + std::to_string(port);
    }
};

// 绑定后立即关闭、不 listen：该端口对 connect 回 ECONNREFUSED（未建立连接）。
std::uint16_t ReservedRefusedPort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;
    sockaddr_in a{};
    a.sin_family      = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port        = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) {
        ::close(fd);
        return 0;
    }
    socklen_t alen = sizeof(a);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &alen) != 0) {
        ::close(fd);
        return 0;
    }
    const std::uint16_t p = ntohs(a.sin_port);
    ::close(fd);
    return p;
}

inf::RpcEnvelope MakeEnvelope(const char* rid) {
    inf::RpcEnvelope env;
    env.service         = "probe.svc";
    env.method          = "Probe";
    env.request_id      = rid;
    env.request_schema  = "probe.v1.ProbeRequest";
    env.response_schema = "probe.v1.ProbeResponse";
    env.payload         = {0x08, 0x01};   // 任意载荷字节；wire 只做搬运
    return env;
}

struct EgressHost {
    std::shared_ptr<fw::InfraHttpHost> host;

    bool Start() {
        inf::NetworkLimits limits{};
        limits.max_connections  = 16;
        limits.max_inflight     = 16;
        limits.max_header_bytes = 16 * 1024;
        limits.max_body_bytes   = 64 * 1024;
        limits.incoming_timeout = std::chrono::milliseconds{5000};
        host = std::make_shared<fw::InfraHttpHost>(
            limits, inf::ListenAddress{"127.0.0.1", 0});
        if (!host->Create()) return false;
        if (!host->Start())  return false;
        return true;
    }
    void Stop() { if (host) host->Close(); }
};

// 在真实协程内调用框架出站（HttpEgress::Send 是协程阻塞调用）。
//
// 超时安全：协程侧经 shared_ptr 按值捕获一块独立堆状态，不引用调用者栈上
// 对象。若 WaitFor 超时，BOOST_REQUIRE 返回后迟到的协程仍写该堆状态，不会
// 形成栈悬垂（与 infra F-2 同类的迟到回调访问问题）。成功路径的时序与断言
// 与修前一致。
struct EgressCallState {
    std::shared_ptr<fw::InfraHttpHost> host;     // 迟到返回前 runtime 不释放
    std::shared_ptr<fw::HttpEgress>    egress;
    inf::RpcAddress addr;
    inf::RpcEnvelope env;
    inf::CallOptions opt;
    std::optional<fw::result<inf::RpcEnvelope>> out;
    TestLatch done{1};
};

std::optional<fw::result<inf::RpcEnvelope>> CallEgress(
    std::shared_ptr<fw::InfraHttpHost> host, inf::RpcAddress addr,
    inf::RpcEnvelope env, inf::CallOptions opt) {
    auto state = std::make_shared<EgressCallState>();
    state->host   = std::move(host);
    state->egress = std::make_shared<fw::HttpEgress>(
        state->host, fw::RpcEgressProfile::ProtoWireV1);
    state->addr = std::move(addr);
    state->env  = std::move(env);
    state->opt  = opt;
    bool succ = false;
    g_scheduler->RegistCoroutineTask([state] {
        state->out.emplace(
            state->egress->Send(state->addr, state->env, state->opt));
        state->done.CountDown();
    }, succ);
    BOOST_REQUIRE_MESSAGE(succ, "RegistCoroutineTask rejected");
    BOOST_REQUIRE_MESSAGE(state->done.WaitFor(kWait), "egress Send did not return");
    return std::move(state->out);
}

} // namespace

BOOST_TEST_GLOBAL_FIXTURE(SchedulerFixture);

BOOST_AUTO_TEST_SUITE(framework_egress_phase)

// T1：发送前已过期 → TimedOut，request_phase 无值；对端零连接（不做任何 I/O）。
BOOST_AUTO_TEST_CASE(expired_budget_no_io_not_unknown) {
    EgressHost eh;
    BOOST_REQUIRE(eh.Start());
    LoopbackPeer peer;
    BOOST_REQUIRE(peer.Start());

    {
        inf::CallOptions opt;
        opt.deadline = Clock::now() - std::chrono::milliseconds{1};
        auto res = CallEgress(eh.host, inf::RpcAddress{"http", peer.Endpoint()},
                              MakeEnvelope("t1-expired"), opt);
        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE(!static_cast<bool>(*res));
        const inf::Error& e = res->error();
        BOOST_TEST(static_cast<int>(e.code) ==
                   static_cast<int>(fw::ErrorCode::TimedOut));
        BOOST_TEST(!e.request_phase.has_value());
    }

    BOOST_TEST(peer.accepts.load() == 0);
    BOOST_TEST(peer.got_commit.load() == false);
    peer.Stop();
    eh.Stop();
}

// T2：连接被拒 → 未提交的确定失败；保留 TransportError，阶段未提交（非未知）。
BOOST_AUTO_TEST_CASE(connection_refused_is_transport_not_unknown) {
    EgressHost eh;
    BOOST_REQUIRE(eh.Start());
    const std::uint16_t refused = ReservedRefusedPort();
    BOOST_REQUIRE(refused != 0);

    {
        inf::CallOptions opt;
        opt.deadline = Clock::now() + std::chrono::milliseconds{3000};
        auto res = CallEgress(
            eh.host,
            inf::RpcAddress{"http",
                            "127.0.0.1:" + std::to_string(refused)},
            MakeEnvelope("t2-refused"), opt);
        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE(!static_cast<bool>(*res));
        const inf::Error& e = res->error();
        BOOST_TEST(static_cast<int>(e.code) ==
                   static_cast<int>(fw::ErrorCode::TransportError));
        BOOST_TEST(static_cast<int>(e.code) !=
                   static_cast<int>(fw::ErrorCode::OutcomeUnknown));
        BOOST_REQUIRE(e.request_phase.has_value());
        BOOST_TEST(!inf::IsRequestCommitted(*e.request_phase));
    }

    eh.Stop();
}

// T3：完整写出后丢 reply → OutcomeUnknown，phase=RequestCommitted；对端只连接一次。
BOOST_AUTO_TEST_CASE(committed_reply_loss_is_outcome_unknown) {
    EgressHost eh;
    BOOST_REQUIRE(eh.Start());
    LoopbackPeer peer;
    BOOST_REQUIRE(peer.Start());

    {
        inf::CallOptions opt;
        opt.deadline = Clock::now() + std::chrono::milliseconds{5000};
        auto res = CallEgress(eh.host, inf::RpcAddress{"http", peer.Endpoint()},
                              MakeEnvelope("t3-drop"), opt);
        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE(!static_cast<bool>(*res));
        const inf::Error& e = res->error();
        // 真实 loopback：对端已收全请求（commit 标记）后不回复。
        BOOST_REQUIRE_MESSAGE(peer.got_commit.load(),
                              "server did not receive a complete request");
        BOOST_TEST(static_cast<int>(e.code) ==
                   static_cast<int>(fw::ErrorCode::OutcomeUnknown));
        BOOST_REQUIRE(e.request_phase.has_value());
        BOOST_TEST(static_cast<int>(*e.request_phase) ==
                   static_cast<int>(inf::RequestPhase::RequestCommitted));
        BOOST_TEST(inf::IsRequestCommitted(*e.request_phase));
    }

    // 一次终态、无 retry：对端只被连接一次。
    BOOST_TEST(peer.accepts.load() == 1);
    peer.Stop();
    eh.Stop();
}

// T4：发送端 remaining budget 由本地 deadline 换算；deadline 明显超过 wire 上限时被
// clamp 到 kMaxRemainingBudgetMs（可判别常量哨兵实现）。
BOOST_AUTO_TEST_CASE(remaining_budget_propagated_within_bounds) {
    EgressHost eh;
    BOOST_REQUIRE(eh.Start());
    LoopbackPeer peer;
    BOOST_REQUIRE(peer.Start());

    {
        inf::CallOptions opt;
        // 声明预算明显超过 wire 上限（kMaxRemainingBudgetMs + 1min），
        // 使 clamp 结果唯一确定为 kMaxRemainingBudgetMs。
        opt.deadline = Clock::now() +
            std::chrono::milliseconds{inf::rpc::kMaxRemainingBudgetMs} +
            std::chrono::minutes{1};
        auto res = CallEgress(eh.host, inf::RpcAddress{"http", peer.Endpoint()},
                              MakeEnvelope("t4-budget"), opt);
        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE(!static_cast<bool>(*res));   // 对端丢 reply
    }

    BOOST_REQUIRE(peer.body_ready.load());
    std::string body;
    {
        std::lock_guard<std::mutex> lk(peer.mtx);
        body = peer.received_body;
    }
    inf::HttpRequest req;
    req.method  = "POST";
    req.url     = inf::rpc::kRpcWireHttpPath;
    req.headers = {{std::string(inf::rpc::kRpcWireContentTypeKey),
                    std::string(inf::rpc::kRpcWireContentType)}};
    req.body    = body;
    auto wire = inf::rpc::ParseRpcWireHttpRequest(req);
    BOOST_REQUIRE_MESSAGE(wire, "framework egress request is not a valid wire envelope");
    BOOST_TEST(wire.value().request_id == std::string("t4-budget"));
    // 声明 deadline 明显超过 wire 上限：剩余预算必被 clamp 到 kMaxRemainingBudgetMs。
    // 该等式可证伪「写常量哨兵」的实现（例如固定 1），常量实现无法满足。
    BOOST_TEST(wire.value().remaining_budget_ms >= 1u);
    BOOST_TEST(wire.value().remaining_budget_ms <=
               inf::rpc::kMaxRemainingBudgetMs);
    BOOST_TEST(wire.value().remaining_budget_ms ==
               inf::rpc::kMaxRemainingBudgetMs);

    peer.Stop();
    eh.Stop();
}

BOOST_AUTO_TEST_SUITE_END()
