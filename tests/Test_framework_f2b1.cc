// co-service-actor/v1 F2-b1：OrderedSession 发送侧测试。
// 用例真实断言；并发用屏障构造同时进入，不用 sleep 凑时序。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <bbt/framework/OrderedSession.hpp>
#include <bbt/framework/OrderedTypes.hpp>
#include <bbt/framework/internal/ErrorDomainRule.hpp>
#include <bbt/framework/internal/RpcHttpBridge.hpp>

namespace fw = bbt::framework;
namespace inf = bbt::infra;

namespace {

fw::OrderedGrant MakeGrant(const std::string& tag) {
    fw::OrderedGrant g;
    g.service        = "svc." + tag;
    g.actor_key      = "actor." + tag;
    g.producer_id    = "producer." + tag;
    g.producer_epoch = "pepoch." + tag;
    g.receiver_epoch = "repoch." + tag;
    g.peer_principal = "peer." + tag;
    return g;
}

fw::OrderedTicket::BoundContent MakeContent(const std::string& tag) {
    fw::OrderedTicket::BoundContent c;
    c.service        = "svc";
    c.method         = "method." + tag;
    c.actor_key      = "actor";
    c.custom         = "custom." + tag;
    c.request_bytes  = "bytes." + tag;
    return c;
}

// 手工屏障：N 个线程全部到达后同时放行。
class Barrier {
public:
    explicit Barrier(std::size_t total) : total_(total) {}
    void ArriveAndWait() {
        std::unique_lock<std::mutex> lk(mtx_);
        if (++arrived_ == total_) {
            lk.unlock();
            cv_.notify_all();
        } else {
            cv_.wait(lk, [this] { return arrived_ == total_; });
        }
    }
private:
    const std::size_t total_;
    std::size_t arrived_ = 0;
    std::mutex mtx_;
    std::condition_variable cv_;
};

const std::string* DetailValue(const fw::Error& e, const std::string& key) {
    for (const auto& kv : e.details) {
        if (kv.first == key) return &kv.second;
    }
    return nullptr;
}

} // namespace

BOOST_AUTO_TEST_SUITE(framework_f2b1)

// 用例 1：序号从 1 单调递增、无重复、无跳号。
BOOST_AUTO_TEST_CASE(sequence_starts_at_one_and_monotonic) {
    auto s = fw::OrderedSession::Open(MakeGrant("t1"));
    BOOST_REQUIRE(s);
    std::set<std::string> ids;
    for (std::uint64_t i = 1; i <= 64; ++i) {
        auto r = s.value()->prepare();
        BOOST_REQUIRE(r);
        BOOST_CHECK_EQUAL(r.value().sequence(), i);
        BOOST_CHECK(!r.value().request_id().empty());
        ids.insert(r.value().request_id());
        // 已准备未发送可被表达
        BOOST_CHECK(!s.value()->IsTicketSent(r.value()));
    }
    BOOST_CHECK_EQUAL(ids.size(), 64u);
}

// 用例 2：prepare 并发——屏障放行后同时分配，序号两两不同、
// request_id 两两不同、序号集合恰为 1..N。
BOOST_AUTO_TEST_CASE(prepare_concurrent_unique_and_dense) {
    constexpr int kThreads = 8;
    constexpr int kPerThread = 32;
    constexpr std::uint64_t kTotal =
        static_cast<std::uint64_t>(kThreads) * kPerThread;

    auto s = fw::OrderedSession::Open(MakeGrant("t2")).value();
    Barrier barrier(kThreads);
    std::vector<std::vector<fw::OrderedStamp>> out(kThreads);
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            barrier.ArriveAndWait();
            for (int k = 0; k < kPerThread; ++k) {
                auto r = s->prepare();
                if (!r) { ++failures; continue; }
                out[t].push_back(r.value());
            }
        });
    }
    for (auto& th : threads) th.join();

    BOOST_CHECK_EQUAL(failures.load(), 0);
    std::vector<std::uint64_t> seqs;
    std::set<std::string> ids;
    for (const auto& v : out) {
        for (const auto& st : v) {
            seqs.push_back(st.sequence());
            ids.insert(st.request_id());
        }
    }
    BOOST_CHECK_EQUAL(seqs.size(), kTotal);
    BOOST_CHECK_EQUAL(ids.size(), kTotal);
    std::sort(seqs.begin(), seqs.end());
    for (std::uint64_t i = 0; i < kTotal; ++i) {
        BOOST_CHECK_EQUAL(seqs[i], i + 1);  // 恰为 1..N，无重无跳
    }
}

// 用例 3：同一票据重发同一请求 → 允许；复用票据发不同内容 →
// SequenceConflict（RemoteError + domain="framework.actor"）。
BOOST_AUTO_TEST_CASE(ticket_resend_same_request_conflict_rejected) {
    auto s = fw::OrderedSession::Open(MakeGrant("t3")).value();
    auto stamp = s->prepare().value();
    const auto content = MakeContent("a");

    BOOST_CHECK(!s->IsTicketSent(stamp));
    BOOST_CHECK(s->BindForSend(stamp, content));   // 首次发送绑定
    BOOST_CHECK(s->IsTicketSent(stamp));
    BOOST_CHECK(s->BindForSend(stamp, content));   // 重发同一请求

    auto other = content;
    other.request_bytes = "different-bytes";
    auto bad = s->BindForSend(stamp, other);
    BOOST_REQUIRE(!bad);
    BOOST_CHECK(bad.error().code == fw::ErrorCode::RemoteError);
    BOOST_CHECK_EQUAL(bad.error().domain, "framework.actor");
    BOOST_CHECK_EQUAL(bad.error().domain_code, "SequenceConflict");

    // 任一边界字段不同均属不同请求
    auto m = content; m.method = "m2";
    BOOST_CHECK(!s->BindForSend(stamp, m));
    auto a = content; a.actor_key = "actor2";
    BOOST_CHECK(!s->BindForSend(stamp, a));
    auto cu = content; cu.custom = "custom2";
    BOOST_CHECK(!s->BindForSend(stamp, cu));
    auto sv = content; sv.service = "svc2";
    BOOST_CHECK(!s->BindForSend(stamp, sv));
}

// 用例 4：序号不回绕——计数器置于 UINT64_MAX 附近，耗尽后 prepare
// 明确失败而非回绕到小序号。
BOOST_AUTO_TEST_CASE(sequence_never_wraps) {
    const std::uint64_t kMax = std::numeric_limits<std::uint64_t>::max();
    auto s = fw::OrderedSession::OpenForTest(MakeGrant("t4"), kMax - 1);
    BOOST_REQUIRE(s);
    auto p1 = s.value()->prepare();
    BOOST_REQUIRE(p1);
    BOOST_CHECK_EQUAL(p1.value().sequence(), kMax - 1);
    auto p2 = s.value()->prepare();
    BOOST_REQUIRE(p2);
    BOOST_CHECK_EQUAL(p2.value().sequence(), kMax);
    auto p3 = s.value()->prepare();
    BOOST_CHECK(!p3);                       // 明确失败
    BOOST_CHECK(p3.error().code == fw::ErrorCode::InternalError);
    auto p4 = s.value()->prepare();
    BOOST_CHECK(!p4);                       // 持续失败，不回绕

    // 起始序号 0 属非法注入
    BOOST_CHECK(!fw::OrderedSession::OpenForTest(MakeGrant("t4b"), 0));
}

// 用例 5：同一授权流进程内至多一个发送实例——同一完整 grant 重复
// Open 返回同一共享会话；流标识相同但 grant 冲突 → StreamRejected；
// 会话销毁后 grant 释放，可重开且序号重起。
BOOST_AUTO_TEST_CASE(one_sender_per_grant) {
    const auto g = MakeGrant("t5");
    auto r1 = fw::OrderedSession::Open(g);
    BOOST_REQUIRE(r1);
    auto r2 = fw::OrderedSession::Open(g);
    BOOST_REQUIRE(r2);
    BOOST_CHECK(r1.value().get() == r2.value().get());  // 同一实例

    auto conflict = g;
    conflict.peer_principal = "other-peer";
    auto rc = fw::OrderedSession::Open(conflict);
    BOOST_REQUIRE(!rc);
    BOOST_CHECK(rc.error().code == fw::ErrorCode::RemoteError);
    BOOST_CHECK_EQUAL(rc.error().domain, "framework.actor");
    BOOST_CHECK_EQUAL(rc.error().domain_code, "StreamRejected");

    // 不同流标识 → 独立会话、独立序号空间
    auto ro = fw::OrderedSession::Open(MakeGrant("t5b"));
    BOOST_REQUIRE(ro);
    BOOST_CHECK(ro.value().get() != r1.value().get());
    BOOST_CHECK_EQUAL(ro.value()->prepare().value().sequence(), 1u);

    // 释放全部引用后 grant 解除占用，重开得到新实例
    fw::OrderedSession::SPtr keep = r1.value();
    auto r3 = fw::OrderedSession::Open(g);
    BOOST_REQUIRE(r3);
    BOOST_CHECK(r3.value().get() == keep.get());  // 仍有存活实例 → 共享
    keep.reset();
    r1.value().reset();
    r2.value().reset();
    r3.value().reset();
    auto r4 = fw::OrderedSession::Open(g);
    BOOST_REQUIRE(r4);
    BOOST_CHECK_EQUAL(r4.value()->prepare().value().sequence(), 1u);
}

// 用例 6（契约第 209 行）：首次发送不得跳过未发送票据——越序首发
// 本地拒绝 SequenceGap 并给出 expected_sequence；按序补齐后可发；
// 已发送票据的重发不受水位影响。
BOOST_AUTO_TEST_CASE(first_send_cannot_skip_unsent_ticket) {
    auto s = fw::OrderedSession::Open(MakeGrant("t6")).value();
    auto t1 = s->prepare().value();
    auto t2 = s->prepare().value();
    auto t3 = s->prepare().value();
    const auto content = MakeContent("x");

    auto g1 = s->BindForSend(t3, content);  // 跳过 1、2 → SequenceGap
    BOOST_REQUIRE(!g1);
    BOOST_CHECK_EQUAL(g1.error().domain_code, "SequenceGap");
    const auto* e1 = DetailValue(g1.error(), "expected_sequence");
    BOOST_REQUIRE(e1 != nullptr);
    BOOST_CHECK_EQUAL(*e1, "1");
    BOOST_CHECK(!s->IsTicketSent(t3));       // 未发出

    BOOST_CHECK(s->BindForSend(t1, content));
    auto g2 = s->BindForSend(t3, content);   // t2 仍未发 → 仍拒绝
    BOOST_REQUIRE(!g2);
    BOOST_CHECK_EQUAL(g2.error().domain_code, "SequenceGap");
    const auto* e2 = DetailValue(g2.error(), "expected_sequence");
    BOOST_REQUIRE(e2 != nullptr);
    BOOST_CHECK_EQUAL(*e2, "2");

    BOOST_CHECK(s->BindForSend(t2, content));
    BOOST_CHECK(s->BindForSend(t3, content));
    BOOST_CHECK(s->BindForSend(t1, content));  // 重发旧票不受水位限制
}

// 用例 7：六个 domain_code 与契约第 213 行逐字一致。
BOOST_AUTO_TEST_CASE(domain_code_constants_match_contract) {
    BOOST_CHECK_EQUAL(std::string(fw::ordered_domain_code::kSequenceGap),
                      "SequenceGap");
    BOOST_CHECK_EQUAL(std::string(fw::ordered_domain_code::kInProgress),
                      "InProgress");
    BOOST_CHECK_EQUAL(std::string(fw::ordered_domain_code::kSequenceConflict),
                      "SequenceConflict");
    BOOST_CHECK_EQUAL(std::string(fw::ordered_domain_code::kResultExpired),
                      "ResultExpired");
    BOOST_CHECK_EQUAL(
        std::string(fw::ordered_domain_code::kOrderedStampRequired),
        "OrderedStampRequired");
    BOOST_CHECK_EQUAL(std::string(fw::ordered_domain_code::kStreamRejected),
                      "StreamRejected");
    BOOST_CHECK_EQUAL(std::string(fw::kOrderedErrorDomain), "framework.actor");
}

// 用例 8（infra #39）：actor 域专属语义校验迁回 framework 边界后的归属回归。
// infra::ValidateErrorDetails 只做通用结构校验，不再拒绝非法 actor 键；
// framework 边界（ValidateErrorAtBoundary）必须维持旧版拒绝集：
//   合法 actor 错误通过、非 actor 合法扩展通过、跨域写保留键拒绝、
//   保留键非法值拒绝、非法编码/重复键/越界长度仍拒绝。
BOOST_AUTO_TEST_CASE(error_domain_rules_at_framework_boundary) {
    using fw::ValidateErrorAtBoundary;
    using fw::ValidateErrorDomainRules;

    // 正常 actor 错误：域归属正确 + 无符号十进制 → 通过
    {
        fw::Error e = fw::MakeError(fw::ErrorCode::RemoteError, "gap");
        e.domain = std::string(fw::kErrorDomainFrameworkActor);
        e.domain_code = fw::ordered_domain_code::kSequenceGap;
        e.details = {{std::string(fw::kErrorDetailExpectedSequence), "7"}};
        auto r = ValidateErrorAtBoundary(e);
        BOOST_CHECK(r);
    }

    // 合法非 actor 扩展：自定义域 + 自定义键 → 通过（不被 actor 规则误伤）
    {
        fw::Error e = fw::MakeError(fw::ErrorCode::RemoteError, "app");
        e.domain = "app.billing";
        e.details = {{"invoice_id", "INV-1"}, {"attempt", "2"}};
        BOOST_CHECK(ValidateErrorAtBoundary(e));
    }

    // 非法值：actor 域的 expected_sequence 非无符号十进制 → 拒绝
    {
        fw::Error e = fw::MakeError(fw::ErrorCode::RemoteError, "gap");
        e.domain = std::string(fw::kErrorDomainFrameworkActor);
        e.details = {{std::string(fw::kErrorDetailExpectedSequence), "x7"}};
        auto r = ValidateErrorAtBoundary(e);
        BOOST_REQUIRE(!r);
        BOOST_CHECK(r.error().code == fw::ErrorCode::ProtocolError);
    }

    // 跨域写保留键：infra 域/其他域携带 expected_sequence → 拒绝
    {
        fw::Error e = fw::MakeError(fw::ErrorCode::ProtocolError, "x");
        e.domain = std::string(bbt::infra::kErrorDomainInfra);
        e.details = {{std::string(fw::kErrorDetailExpectedSequence), "7"}};
        auto r = ValidateErrorAtBoundary(e);
        BOOST_REQUIRE(!r);
        BOOST_CHECK(r.error().code == fw::ErrorCode::ProtocolError);
    }

    // 结构违规仍由 infra 通用层拒绝：重复键 / 非法 UTF-8 / 越界长度
    {
        fw::Error e = fw::MakeError(fw::ErrorCode::RemoteError, "s");
        e.domain = std::string(fw::kErrorDomainFrameworkActor);
        e.details = {{"a", "1"}, {"a", "2"}};
        auto r = ValidateErrorAtBoundary(e);
        BOOST_REQUIRE(!r);
        BOOST_CHECK(r.error().code == fw::ErrorCode::ProtocolError);

        e.details = {{"k", "\xff\xfe"}};
        r = ValidateErrorAtBoundary(e);
        BOOST_REQUIRE(!r);

        e.details = {{std::string(65, 'k'), "v"}};
        r = ValidateErrorAtBoundary(e);
        BOOST_REQUIRE(!r);
    }

    // 分层确认：同一非法 actor 错误，infra 通用层不再拒绝、
    // framework 边界仍拒绝——旧版拒绝集没有因迁移被静默放宽。
    {
        fw::Error e = fw::MakeError(fw::ErrorCode::RemoteError, "gap");
        e.domain = std::string(fw::kErrorDomainFrameworkActor);
        e.details = {{std::string(fw::kErrorDetailExpectedSequence), "x7"}};
        BOOST_CHECK(bbt::infra::ValidateErrorDetails(e));
        BOOST_CHECK(!ValidateErrorDomainRules(e));
        BOOST_CHECK(!ValidateErrorAtBoundary(e));
    }
}

// 用例 9（infra #39 R2）：真实 ToHttpResponse 出站边界的降级回归。
// 不走 helper 直连——构造 result<RpcEnvelope>::err(...) 喂给
// http_bridge::ToHttpResponse，断言 wire 输出本身：
//   a) actor 域非法 expected_sequence → 降级为 ProtocolError，且
//      原始非法值/保留键不出现在任何 header；
//   b) 跨域携带保留键 → 同样降级；
//   c) 合法 actor 错误 → 既有 code/domain/domain_code/message 映射不变；
//   d) 合法错误的 details 不上 wire（#8 未实现，不得宣称传递）。
BOOST_AUTO_TEST_CASE(to_http_response_boundary_downgrades_invalid_error) {
    namespace Wire = fw::http_bridge;
    using inf::RpcEnvelope;

    auto header_of = [](const inf::HttpResponse& r,
                        std::string_view name) -> std::optional<std::string> {
        for (const auto& [k, v] : r.headers)
            if (k == name) return v;
        return std::nullopt;
    };

    // a) actor 域非法 expected_sequence：真实出站必须降级且不泄露原值
    {
        fw::Error e = fw::MakeError(fw::ErrorCode::RemoteError,
                                    "actor gap detail");
        e.domain = std::string(fw::kErrorDomainFrameworkActor);
        e.domain_code = fw::ordered_domain_code::kSequenceGap;
        e.details = {{std::string(fw::kErrorDetailExpectedSequence), "x7"}};

        const auto res = Wire::ToHttpResponse(
            fw::result<RpcEnvelope>::err(std::move(e)));

        BOOST_REQUIRE_EQUAL(res.status, 400u);
        const auto code = header_of(res, "x-bbt-err-code");
        BOOST_REQUIRE(code);
        BOOST_CHECK_EQUAL(*code,
            std::to_string(static_cast<int>(fw::ErrorCode::ProtocolError)));
        // 降级后是通用 ProtocolError：不保留原 domain/domain_code/message
        BOOST_CHECK(header_of(res, "x-bbt-err-domain"));
        BOOST_CHECK(!header_of(res, "x-bbt-err-domain-code") ||
                    header_of(res, "x-bbt-err-domain-code")->empty());
        // 原始非法值 "x7" 与保留键名不得出现在任何 header 值/键中
        for (const auto& [k, v] : res.headers) {
            BOOST_CHECK(k.find("expected_sequence") == std::string::npos);
            BOOST_CHECK(v.find("x7") == std::string::npos);
            BOOST_CHECK(v.find("SequenceGap") == std::string::npos);
            BOOST_CHECK(v.find("actor gap detail") == std::string::npos);
        }
    }

    // b) 跨域携带保留键：infra 域错误带 expected_sequence → 出站降级
    {
        fw::Error e = fw::MakeError(fw::ErrorCode::ProtocolError, "x");
        e.domain = std::string(bbt::infra::kErrorDomainInfra);
        e.details = {{std::string(fw::kErrorDetailExpectedSequence), "7"}};

        const auto res = Wire::ToHttpResponse(
            fw::result<RpcEnvelope>::err(std::move(e)));

        BOOST_REQUIRE_EQUAL(res.status, 400u);
        const auto code = header_of(res, "x-bbt-err-code");
        BOOST_REQUIRE(code);
        BOOST_CHECK_EQUAL(*code,
            std::to_string(static_cast<int>(fw::ErrorCode::ProtocolError)));
        for (const auto& [k, v] : res.headers)
            BOOST_CHECK(k.find("expected_sequence") == std::string::npos);
    }

    // c) 合法 actor 错误：既有 wire 映射不回归——code/domain/domain_code/
    //    message 原样落 header，details 本身不上 wire
    {
        fw::Error e = fw::MakeError(fw::ErrorCode::RemoteError, "gap msg");
        e.domain = std::string(fw::kErrorDomainFrameworkActor);
        e.domain_code = fw::ordered_domain_code::kSequenceGap;
        e.details = {{std::string(fw::kErrorDetailExpectedSequence), "42"}};

        const auto res = Wire::ToHttpResponse(
            fw::result<RpcEnvelope>::err(std::move(e)));

        BOOST_REQUIRE_EQUAL(res.status, 400u);
        BOOST_CHECK_EQUAL(
            header_of(res, "x-bbt-err-code").value_or(""),
            std::to_string(static_cast<int>(fw::ErrorCode::RemoteError)));
        BOOST_CHECK_EQUAL(header_of(res, "x-bbt-err-domain").value_or(""),
                          "framework.actor");
        BOOST_CHECK_EQUAL(
            header_of(res, "x-bbt-err-domain-code").value_or(""),
            "SequenceGap");
        BOOST_CHECK_EQUAL(header_of(res, "x-bbt-err-message").value_or(""),
                          "gap msg");
        // details 不经现有 wire 传递（#8 范围）：无 header 携带 42
        for (const auto& [k, v] : res.headers) {
            BOOST_CHECK(k.find("expected_sequence") == std::string::npos);
            BOOST_CHECK(v != "42");
        }
    }
}

BOOST_AUTO_TEST_SUITE_END()
