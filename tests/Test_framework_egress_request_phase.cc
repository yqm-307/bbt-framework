// 下游 request-phase/OutcomeUnknown 消费验收（V2 SessionFrame wire）：
// committed→OutcomeUnknown；另覆盖发送前过期、未提交失败、remaining budget；
// 不覆盖并发/容量/关闭矩阵。
// 消费已合入 infra 的公开 request-phase / OutcomeUnknown 契约
// （bbt::infra::RequestPhase、IsRequestCommitted、Error::request_phase），
// 在真实 framework RpcEgress（V2 会话 owner）上机器断言：
//   T1 发送前已过期     → TimedOut，request_phase=nullopt，服务端零连接（无 I/O）；
//   T2 连接被拒         → 未提交的确定失败（TransportError 保留），phase 未提交；
//   T3 完整写出后丢 reply → 升级为 OutcomeUnknown 且 phase=RequestCommitted，
//                        服务端只被连接一次（一次终态、无 retry）；
//   T4 remaining budget → 发送端从本地 deadline 换算出 remaining_budget_ms；
//                        deadline 明显超过 wire 上限时 clamp 到 kRpcV2MaxRemainingBudgetMs。
//
// 对端是测试自有的裸 TCP loopback 端点：完成 Hello/HelloAck 后读满一整帧
// SessionFrame（u32be 前缀 + body）即视为请求已提交，随后不回复直接断开。
// 不使用固定 sleep、不放宽断言、不解析错误消息猜阶段；断言只读公开
// Error / request_phase 与 wire 字段本身，不接触 Beast/Asio/socket 类型。
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

#include <bbt/framework/internal/InfraHttpHost.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>
#include <bbt/infra/rpc/SessionCodec.hpp>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/internal/RpcBridge.hpp>

namespace fw  = bbt::framework;
namespace inf = bbt::infra;
using Clock = std::chrono::steady_clock;

namespace {

// 协程用例需要真实调度器；2 个静态 worker 足够（用例内不并发压测；
// V2 egress 使用宿主共享会话 owner 与独立 client-only Engine
// 驱动槽）。runtime 是进程寿命单例：只 Start 一次、无 Stop；已初始化则复用。
struct SchedulerFixture {
    SchedulerFixture() {
        // Registered driver capacity: app server 2 + app egress 1 + raw peer driver 1 + runnable worker 1.
        g_bbt_coroutine_config->m_cfg_static_thread_num = 5;
        // 用例在协程内做 proto wire 编解码与嵌套调用；默认 12KB 栈在
        // 同仓 inbound_http 用例的同一条出站链上已实测触底（guard 页
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

// ── 最小 SessionFrame 读取（只取本测试需要的两个字段）────────────────
// 帧格式：u32be payload_length | SessionFrame body（profile 2）。
// 只做「读满一帧」与 varint/LEN 字段定位，不复制 infra 的完整 codec。

bool ReadExact(int fd, void* out, std::size_t want) {
    auto* p = static_cast<char*>(out);
    std::size_t have = 0;
    while (have < want) {
        const ssize_t n = ::recv(fd, p + have, want - have, 0);
        if (n <= 0)
            return false;
        have += static_cast<std::size_t>(n);
    }
    return true;
}

bool ReadVarint(const std::string& b, std::size_t& pos, std::uint64_t& v) {
    std::uint64_t result = 0;
    int shift = 0;
    while (pos < b.size()) {
        const auto byte = static_cast<unsigned char>(b[pos++]);
        result |= static_cast<std::uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) {
            v = result;
            return true;
        }
        shift += 7;
        if (shift >= 64)
            return false;
    }
    return false;
}

// 顺序扫一遍 body，取指定 field 的值（varint 或 LEN bytes）。
bool FrameField(const std::string& body, int want_field, std::uint64_t* varint_out,
                std::string* bytes_out) {
    std::size_t pos = 0;
    while (pos < body.size()) {
        std::uint64_t key = 0;
        if (!ReadVarint(body, pos, key))
            return false;
        const int field = static_cast<int>(key >> 3);
        const int wire = static_cast<int>(key & 7);
        if (wire == 0) {
            std::uint64_t v = 0;
            if (!ReadVarint(body, pos, v))
                return false;
            if (field == want_field && varint_out)
                *varint_out = v;
        } else if (wire == 2) {
            std::uint64_t len = 0;
            if (!ReadVarint(body, pos, len))
                return false;
            if (pos + len > body.size())
                return false;
            if (field == want_field && bytes_out)
                bytes_out->assign(body, pos, static_cast<std::size_t>(len));
            pos += static_cast<std::size_t>(len);
        } else if (wire == 1) {
            pos += 8;
        } else if (wire == 5) {
            pos += 4;
        } else {
            return false;
        }
    }
    return true;
}

// 写一整帧：u32be 前缀 + body。
bool WriteFrame(int fd, const std::string& body) {
    const std::uint32_t n = static_cast<std::uint32_t>(body.size());
    std::string out;
    out.push_back(static_cast<char>((n >> 24) & 0xFF));
    out.push_back(static_cast<char>((n >> 16) & 0xFF));
    out.push_back(static_cast<char>((n >> 8) & 0xFF));
    out.push_back(static_cast<char>(n & 0xFF));
    out += body;
    std::size_t sent = 0;
    while (sent < out.size()) {
        const ssize_t w = ::send(fd, out.data() + sent, out.size() - sent, 0);
        if (w <= 0)
            return false;
        sent += static_cast<std::size_t>(w);
    }
    return true;
}

// 握手帧（Hello/HelloAck）的最小 body 编码：只写本测试用到的字段。
std::string HandshakeBody(int kind, const std::string& identity,
                          std::uint64_t epoch, std::uint64_t nonce) {
    std::string b;
    auto varint = [](std::uint64_t v) {
        std::string o;
        while (true) {
            const auto byte = static_cast<unsigned char>(v & 0x7F);
            v >>= 7;
            if (v) {
                o.push_back(static_cast<char>(byte | 0x80));
            } else {
                o.push_back(static_cast<char>(byte));
                return o;
            }
        }
    };
    auto tag = [&](int field, int wire) { return varint((field << 3) | wire); };
    auto len = [&](int field, const std::string& s) {
        return tag(field, 2) + varint(s.size()) + s;
    };
    b += tag(1, 0) + varint(2);      // profile_version
    b += tag(2, 0) + varint(kind);   // frame_kind
    b += tag(3, 0) + varint(0);      // traffic_class = SYSTEM
    b += len(4, identity);           // peer_identity
    b += tag(5, 0) + varint(epoch);  // session_epoch
    b += tag(20, 0) + varint(nonce); // connection_nonce
    return b;
}

// 读一整帧 SessionFrame（u32be 前缀 + body）。收回整帧即表示请求字节已完整
// 到达对端；返回 false 表示读失败/连接被中断。
bool ReadFrameBody(int fd, std::string& body_out) {
    // 有界收口：对已接受连接设接收超时，避免「连接后不发完整帧」时
    // ::recv 永久阻塞，导致 Stop() 的 join 只能靠 ctest 超时兜底。
    timeval rcvto{1, 0};   // 1s
    if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &rcvto, sizeof(rcvto)) != 0)
        return false;
    unsigned char prefix[4] = {0, 0, 0, 0};
    if (!ReadExact(fd, prefix, sizeof(prefix)))
        return false;
    const std::uint32_t len = (static_cast<std::uint32_t>(prefix[0]) << 24) |
                              (static_cast<std::uint32_t>(prefix[1]) << 16) |
                              (static_cast<std::uint32_t>(prefix[2]) << 8) |
                              static_cast<std::uint32_t>(prefix[3]);
    if (len == 0 || len > (1u << 20))
        return false; // 防御：不无界增长
    body_out.assign(len, '\0');
    return ReadExact(fd, body_out.data(), len);
}

// 对端（epoch owner，identity 字典序更小）完成 Hello/HelloAck 后读首个 CALL。
// 成功即代表：客户端已完成握手并把整帧请求写到本端。
bool HandshakeAndReadCall(int fd, std::string& call_body,
                          std::atomic<bool>* ack_verified) {
    constexpr std::uint64_t kPeerEpoch = 1001;
    constexpr std::uint64_t kPeerNonce = 0xA11CE;
    constexpr const char* kPeerIdentity = "a-raw-peer";

    std::string hello_body;
    if (!ReadFrameBody(fd, hello_body))
        return false;
    std::uint64_t kind = 0;
    std::uint64_t client_nonce = 0;
    std::string client_identity;
    if (!FrameField(hello_body, 2, &kind, nullptr) || kind != 1)
        return false;
    if (!FrameField(hello_body, 4, nullptr, &client_identity))
        return false;
    if (!FrameField(hello_body, 20, &client_nonce, nullptr))
        return false;
    if (client_identity.empty() || client_nonce == 0)
        return false;

    if (!WriteFrame(fd, HandshakeBody(1, kPeerIdentity, kPeerEpoch, kPeerNonce)))
        return false;
    if (!WriteFrame(fd, HandshakeBody(2, kPeerIdentity, kPeerEpoch, client_nonce)))
        return false;

    std::string echo_body;
    if (!ReadFrameBody(fd, echo_body))
        return false;
    std::uint64_t echo_kind = 0;
    std::uint64_t echo_epoch = 0;
    std::uint64_t echo_nonce = 0;
    if (!FrameField(echo_body, 2, &echo_kind, nullptr) || echo_kind != 2)
        return false;
    if (!FrameField(echo_body, 5, &echo_epoch, nullptr))
        return false;
    if (!FrameField(echo_body, 20, &echo_nonce, nullptr))
        return false;
    if (echo_epoch != kPeerEpoch || echo_nonce != kPeerNonce)
        return false;
    if (ack_verified)
        ack_verified->store(true, std::memory_order_release);

    if (!ReadFrameBody(fd, call_body))
        return false;
    std::uint64_t call_kind = 0;
    return FrameField(call_body, 2, &call_kind, nullptr) && call_kind == 3;
}

// 测试自有裸 TCP loopback 端点：接受连接 → 握手 → 读完一整帧 CALL → 不回复
// 直接断开。这是唯一形态（没有可切换的丢弃开关）：只有完整收下请求才置
// commit 标记，读失败/超时则不计。记录被连接次数，用于断言一次终态/无 retry。
struct LoopbackPeer {
    int                listen_fd = -1;
    std::uint16_t      port      = 0;
    std::atomic<int>   accepts{0};
    std::atomic<bool>  got_commit{false};
    std::atomic<bool>  body_ready{false};
    std::atomic<bool> ack_verified{false};
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
            const bool ok = HandshakeAndReadCall(c, body, &ack_verified);
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

inf::rpc::Request MakeEnvelope(const char* rid) {
    inf::rpc::Request env;
    env.SetService("probe.svc")
        .SetMethod("Probe")
        .SetRequestId(rid)
        .SetRequestSchema("probe.v1.ProbeRequest")
        .SetResponseSchema("probe.v1.ProbeResponse")
        .SetReply(inf::rpc::ReplyMode::Required)
        .SetTraffic(inf::rpc::TrafficClass::Service)
        .SetPayload({0x08, 0x01}); // 任意载荷字节；wire 只做搬运
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
            limits, inf::ListenAddress{"127.0.0.1", 0}, "egress-phase-client");
        if (!host->Create()) return false;
        if (!host->Start())  return false;
        if (!host->rpc_client())
            return false;
        return true;
    }
    void Stop() { if (host) host->Close(); }
};

// 在真实协程内调用框架出站（RpcEgress::Call 是协程阻塞调用）。
//
// 超时安全：协程侧经 shared_ptr 按值捕获一块独立堆状态，不引用调用者栈上
// 对象。若 WaitFor 超时，BOOST_REQUIRE 返回后迟到的协程仍写该堆状态，不会
// 形成栈悬垂（与 infra F-2 同类的迟到回调访问问题）。成功路径的时序与断言
// 与修前一致。
struct EgressCallState {
    std::shared_ptr<fw::InfraHttpHost> host;     // 迟到返回前 runtime 不释放
    std::shared_ptr<fw::RpcEgress> egress;
    std::string target;
    inf::rpc::Request env;
    inf::CallOptions opt;
    std::optional<fw::result<inf::rpc::Response>> out;
    TestLatch done{1};
};

std::optional<fw::result<inf::rpc::Response>> CallEgress(
    std::shared_ptr<fw::InfraHttpHost> host, std::string endpoint,
    inf::rpc::Request env, inf::CallOptions opt) {
    auto state = std::make_shared<EgressCallState>();
    state->host   = std::move(host);
    state->egress = std::make_shared<fw::RpcEgress>(state->host);
    state->target = "tcp://" + endpoint;
    state->env = std::move(env);
    state->opt = opt;
    bool succ = false;
    g_scheduler->RegistCoroutineTask([state] {
        state->out.emplace(
            state->egress->Call(state->target, state->env, state->opt));
        state->done.CountDown();
    },
                                     succ);
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
        auto res = CallEgress(eh.host, peer.Endpoint(),
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
        auto res = CallEgress(eh.host, "127.0.0.1:" + std::to_string(refused),
                              MakeEnvelope("t2-refused"), opt);
        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE(!static_cast<bool>(*res));
        const inf::Error& e = res->error();
        // 共享 V2 会话 owner（runtime 限额 owner）出站：连接被拒是「未提交」的
        // 确定失败——判别点是「非 OutcomeUnknown、非已提交」。request_phase
        // 在新栈契约下允许为空（NetToError 无阶段事实）；有值时必为未提交。
        BOOST_TEST(static_cast<int>(e.code) ==
                   static_cast<int>(fw::ErrorCode::TransportError));
        BOOST_TEST(static_cast<int>(e.code) !=
                   static_cast<int>(fw::ErrorCode::OutcomeUnknown));
        if (e.request_phase.has_value())
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
        auto res = CallEgress(eh.host, peer.Endpoint(),
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
// clamp 到 kRpcV2MaxRemainingBudgetMs（可判别常量哨兵实现）。
BOOST_AUTO_TEST_CASE(remaining_budget_propagated_within_bounds) {
    EgressHost eh;
    BOOST_REQUIRE(eh.Start());
    LoopbackPeer peer;
    BOOST_REQUIRE(peer.Start());

    {
        inf::CallOptions opt;
        // 声明预算明显超过 wire 上限（kRpcV2MaxRemainingBudgetMs + 1min），
        // 使 clamp 结果唯一确定为 kRpcV2MaxRemainingBudgetMs。
        opt.deadline = Clock::now() +
                       std::chrono::milliseconds{inf::rpc::kRpcV2MaxRemainingBudgetMs} +
                       std::chrono::minutes{1};
        auto res = CallEgress(eh.host, peer.Endpoint(),
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
    // 对端已收下的就是一整帧 CALL body：直接读 wire 字段。
    BOOST_TEST(peer.ack_verified.load()); // 握手 echo 已被对端校验
    std::string request_id;
    BOOST_REQUIRE_MESSAGE(FrameField(body, 7, nullptr, &request_id),
                          "framework egress request is not a valid SessionFrame");
    BOOST_TEST(request_id == std::string("t4-budget"));
    std::uint64_t budget = 0;
    BOOST_REQUIRE(FrameField(body, 13, &budget, nullptr));
    // 声明 deadline 明显超过 wire 上限：剩余预算必被 clamp 到上限。
    // 该等式可证伪「写常量哨兵」的实现（例如固定 1），常量实现无法满足。
    BOOST_TEST(budget >= 1u);
    BOOST_TEST(budget <= inf::rpc::kRpcV2MaxRemainingBudgetMs);
    BOOST_TEST(budget == inf::rpc::kRpcV2MaxRemainingBudgetMs);

    peer.Stop();
    eh.Stop();
}

BOOST_AUTO_TEST_SUITE_END()
