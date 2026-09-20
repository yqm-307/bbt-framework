// co-service-actor/v1 F2-b2：有序入口接收侧测试。
// 覆盖契约 §F2「有序入口与重试」接收侧语义与 F-04 有序入口验收项中
// 属本切片的部分：乱序缺口、重复/冲突、在途、缓存淘汰、授权流校验、
// epoch 切换与旧会话拒绝、无票据拒绝。
// 用例真实断言；并发用屏障构造同时进入，不用 sleep 凑时序。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <bbt/framework/OrderedTypes.hpp>
#include <bbt/framework/internal/OrderedIngress.hpp>

namespace fw = bbt::framework;

namespace {

constexpr const char* kService = "svc.ordered";

fw::OrderedIngressConfig MakeConfig(std::size_t streams,
                                    std::size_t results,
                                    std::size_t bytes) {
    fw::OrderedIngressConfig c;
    c.service                 = kService;
    c.max_ordered_streams     = streams;
    c.max_cached_results      = results;
    c.max_cached_result_bytes = bytes;
    return c;
}

fw::OrderedGrant MakeGrant(const std::string& actor_key,
                           const std::string& producer_id,
                           const std::string& producer_epoch,
                           const std::string& receiver_epoch,
                           const std::string& peer = "peer.a") {
    fw::OrderedGrant g;
    g.service        = kService;
    g.actor_key      = actor_key;
    g.producer_id    = producer_id;
    g.producer_epoch = producer_epoch;
    g.receiver_epoch = receiver_epoch;
    g.peer_principal = peer;
    return g;
}

// 默认 request_id 只由 (producer_id, sequence) 决定——与发送侧一致：
// 同一票据重发保持同一 request_id，请求摘要区分内容。
fw::OrderedIngressRequest MakeRequest(const fw::OrderedGrant& g,
                                      std::uint64_t sequence,
                                      const std::string& digest = "d1") {
    fw::OrderedIngressRequest r;
    r.method            = "method";
    r.actor_key         = g.actor_key;
    r.peer_principal    = g.peer_principal;
    r.has_ordered_stamp = true;
    r.producer_id       = g.producer_id;
    r.producer_epoch    = g.producer_epoch;
    r.receiver_epoch    = g.receiver_epoch;
    r.sequence          = sequence;
    r.request_id = "req." + g.producer_id + "." + std::to_string(sequence);
    r.request_digest = digest;
    return r;
}

fw::OrderedTerminalReply OkReply(const std::string& payload) {
    fw::OrderedTerminalReply r;
    r.is_ok  = true;
    r.payload = payload;
    return r;
}

fw::OrderedTerminalReply ErrReply(fw::ErrorCode code,
                                  const std::string& message,
                                  const std::string& domain = "framework.actor",
                                  const std::string& domain_code = "BusinessFailed") {
    fw::OrderedTerminalReply r;
    r.is_ok          = false;
    r.error          = fw::MakeError(code, message);
    r.error.domain   = domain;
    r.error.domain_code = domain_code;
    return r;
}

fw::OrderedIngress::SPtr MakeIngress(std::size_t streams = 8,
                                     std::size_t results = 64,
                                     std::size_t bytes = 4096) {
    auto r = fw::OrderedIngress::Create(MakeConfig(streams, results, bytes));
    BOOST_REQUIRE_MESSAGE(static_cast<bool>(r), "ingress create must succeed");
    return r.value();
}

// 接纳并要求 Execute 决策。
fw::OrderedIngress::Admission AdmitExecute(
    const fw::OrderedIngress::SPtr& ingress,
    const fw::OrderedIngressRequest& request) {
    auto r = ingress->Admit(request);
    BOOST_REQUIRE_MESSAGE(static_cast<bool>(r), "admit must be accepted");
    BOOST_REQUIRE(r.value().kind == fw::OrderedIngress::DecisionKind::Execute);
    return r.value();
}

// 接纳并要求 Replay 决策（已完成重复请求）。
fw::OrderedIngress::Admission AdmitReplay(
    const fw::OrderedIngress::SPtr& ingress,
    const fw::OrderedIngressRequest& request) {
    auto r = ingress->Admit(request);
    BOOST_REQUIRE_MESSAGE(static_cast<bool>(r), "duplicate must be accepted");
    BOOST_REQUIRE(r.value().kind == fw::OrderedIngress::DecisionKind::Replay);
    return r.value();
}

void CompleteOk(const fw::OrderedIngress::SPtr& ingress,
                const fw::OrderedIngress::Admission& a,
                const std::string& payload) {
    auto r = ingress->Complete(a, OkReply(payload));
    BOOST_REQUIRE_MESSAGE(static_cast<bool>(r), "complete must succeed");
}

void CompleteErr(const fw::OrderedIngress::SPtr& ingress,
                 const fw::OrderedIngress::Admission& a,
                 const fw::OrderedTerminalReply& reply) {
    auto r = ingress->Complete(a, reply);
    BOOST_REQUIRE_MESSAGE(static_cast<bool>(r), "complete must succeed");
}

// 契约第 213 行：统一 RemoteError + domain="framework.actor" + 固定
// domain_code。
void ExpectOrderedError(const fw::Error& e, const char* domain_code) {
    BOOST_CHECK(e.code == fw::ErrorCode::RemoteError);
    BOOST_CHECK_EQUAL(e.domain, "framework.actor");
    BOOST_CHECK_EQUAL(e.domain_code, domain_code);
}

std::uint64_t NextSequenceOf(const fw::OrderedIngress::SPtr& ingress,
                             const fw::OrderedGrant& g) {
    auto obs = ingress->ObserveStream(g.actor_key, g.producer_id,
                                      g.producer_epoch);
    BOOST_REQUIRE_MESSAGE(static_cast<bool>(obs), "stream must be installed");
    return obs.value().next_sequence;
}

const std::string* DetailValue(const fw::Error& e, const std::string& key) {
    for (const auto& kv : e.details) {
        if (kv.first == key) return &kv.second;
    }
    return nullptr;
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

} // namespace

BOOST_AUTO_TEST_SUITE(framework_f2b2)

// 用例 1：装配期校验——空服务名、任一上限为 0 均拒绝。
BOOST_AUTO_TEST_CASE(create_validates_config) {
    auto empty_service = MakeConfig(1, 1, 1);
    empty_service.service.clear();
    auto r1 = fw::OrderedIngress::Create(empty_service);
    BOOST_REQUIRE(!r1);
    BOOST_CHECK(r1.error().code == fw::ErrorCode::InvalidArgument);

    BOOST_CHECK(!fw::OrderedIngress::Create(MakeConfig(0, 1, 1)));
    BOOST_CHECK(!fw::OrderedIngress::Create(MakeConfig(1, 0, 1)));
    BOOST_CHECK(!fw::OrderedIngress::Create(MakeConfig(1, 1, 0)));
    BOOST_CHECK(fw::OrderedIngress::Create(MakeConfig(1, 1, 1)));
}

// 用例 2：序号从 1 起、逐条递增；不同 Actor/不同 producer 各自独立
// 计数（契约第 253 行：接收端为该 producer/epoch/Actor 维护下一个
// sequence，不承诺跨 producer 全序）。
BOOST_AUTO_TEST_CASE(sequence_starts_at_one_per_stream) {
    auto ing = MakeIngress();
    const auto g_a  = MakeGrant("actor.a", "producer.1", "pe1", "re1");
    const auto g_b  = MakeGrant("actor.b", "producer.1", "pe1", "re1");
    const auto g_c  = MakeGrant("actor.a", "producer.2", "pe1", "re1");
    BOOST_REQUIRE(ing->GrantStream(g_a));
    BOOST_REQUIRE(ing->GrantStream(g_b));
    BOOST_REQUIRE(ing->GrantStream(g_c));

    for (std::uint64_t i = 1; i <= 5; ++i) {
        auto a = AdmitExecute(ing, MakeRequest(g_a, i));
        BOOST_CHECK_EQUAL(a.sequence, i);
        CompleteOk(ing, a, "ok");
    }
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g_a), 6u);

    // 另一个 Actor 的流与另一 producer 的流独立从 1 起
    BOOST_CHECK_EQUAL(AdmitExecute(ing, MakeRequest(g_b, 1)).sequence, 1u);
    BOOST_CHECK_EQUAL(AdmitExecute(ing, MakeRequest(g_c, 1)).sequence, 1u);
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g_b), 2u);
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g_c), 2u);

    auto obs = ing->ObserveStream(g_a.actor_key, g_a.producer_id,
                                  g_a.producer_epoch);
    BOOST_REQUIRE(obs);
    BOOST_CHECK_EQUAL(obs.value().next_sequence, 6u);
    BOOST_CHECK_EQUAL(obs.value().in_flight, 0u);
    BOOST_CHECK_EQUAL(obs.value().cached, 5u);
}

// 用例 3：未来序号 → SequenceGap + expected_sequence，不缓冲缺口、
// 不执行 handler、不消费序号；按序补齐后可正常接纳（契约第 253 行）。
BOOST_AUTO_TEST_CASE(future_sequence_returns_gap_with_expected) {
    auto ing = MakeIngress();
    const auto g = MakeGrant("actor.a", "producer.1", "pe1", "re1");
    BOOST_REQUIRE(ing->GrantStream(g));
    CompleteOk(ing, AdmitExecute(ing, MakeRequest(g, 1)), "ok");

    auto gap = ing->Admit(MakeRequest(g, 3));
    BOOST_REQUIRE(!gap);
    ExpectOrderedError(gap.error(), "SequenceGap");
    const std::string* expected = DetailValue(gap.error(), "expected_sequence");
    BOOST_REQUIRE(expected != nullptr);
    BOOST_CHECK_EQUAL(*expected, "2");
    // 期望序号是 infra 保留键要求的无符号十进制，且域归属正确
    BOOST_CHECK(bbt::infra::ValidateErrorDetails(gap.error()));
    // 未消费序号、未登记在途
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g), 2u);

    CompleteOk(ing, AdmitExecute(ing, MakeRequest(g, 2)), "ok");
    CompleteOk(ing, AdmitExecute(ing, MakeRequest(g, 3)), "ok");
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g), 4u);
}

// 用例 4：已接纳序号在途 → InProgress；同序号但 request_id 或请求摘要
// 不同 → SequenceConflict（契约第 255 行）。
BOOST_AUTO_TEST_CASE(in_progress_and_sequence_conflict) {
    auto ing = MakeIngress();
    const auto g = MakeGrant("actor.a", "producer.1", "pe1", "re1");
    BOOST_REQUIRE(ing->GrantStream(g));
    AdmitExecute(ing, MakeRequest(g, 1, "digest.1"));

    auto dup = ing->Admit(MakeRequest(g, 1, "digest.1"));   // 同一请求重发
    BOOST_REQUIRE(!dup);
    ExpectOrderedError(dup.error(), "InProgress");

    auto other_digest = ing->Admit(MakeRequest(g, 1, "digest.2"));
    BOOST_REQUIRE(!other_digest);
    ExpectOrderedError(other_digest.error(), "SequenceConflict");

    auto other_id = MakeRequest(g, 1, "digest.1");
    other_id.request_id = "req.other";
    auto bad_id = ing->Admit(other_id);
    BOOST_REQUIRE(!bad_id);
    ExpectOrderedError(bad_id.error(), "SequenceConflict");

    // 冲突与在途都不改变序号状态
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g), 2u);
}

// 用例 5：已完成重复请求重放原终态结果，不重新执行；成功与失败
// （业务失败/取消/排队到期）都保留终态（契约第 255/257 行）。
BOOST_AUTO_TEST_CASE(completed_duplicate_replays_terminal_state) {
    auto ing = MakeIngress();
    const auto g = MakeGrant("actor.a", "producer.1", "pe1", "re1");
    BOOST_REQUIRE(ing->GrantStream(g));

    const auto req1 = MakeRequest(g, 1, "digest.1");
    CompleteOk(ing, AdmitExecute(ing, req1), "payload-1");
    auto replay_ok = AdmitReplay(ing, req1);
    BOOST_CHECK(replay_ok.replay.is_ok);
    BOOST_CHECK_EQUAL(replay_ok.replay.payload, "payload-1");

    const auto req2 = MakeRequest(g, 2, "digest.2");
    const auto business_failure = ErrReply(fw::ErrorCode::RemoteError,
                                           "business rejected",
                                           "framework.actor",
                                           "BusinessFailed");
    CompleteErr(ing, AdmitExecute(ing, req2), business_failure);
    auto replay_err = AdmitReplay(ing, req2);
    BOOST_CHECK(!replay_err.replay.is_ok);
    BOOST_CHECK(replay_err.replay.error.code == fw::ErrorCode::RemoteError);
    BOOST_CHECK_EQUAL(replay_err.replay.error.domain, "framework.actor");
    BOOST_CHECK_EQUAL(replay_err.replay.error.domain_code, "BusinessFailed");
    BOOST_CHECK_EQUAL(replay_err.replay.error.message, "business rejected");

    const auto req3 = MakeRequest(g, 3, "digest.3");
    CompleteErr(ing, AdmitExecute(ing, req3),
                ErrReply(fw::ErrorCode::Cancelled, "queue wait cancelled",
                         "infra", ""));
    auto replay_cancel = AdmitReplay(ing, req3);
    BOOST_CHECK(!replay_cancel.replay.is_ok);
    BOOST_CHECK(replay_cancel.replay.error.code == fw::ErrorCode::Cancelled);

    // 三次接纳都消耗了序号；重复请求没有再消耗
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g), 4u);
    auto obs = ing->ObserveStream(g.actor_key, g.producer_id, g.producer_epoch);
    BOOST_REQUIRE(obs);
    BOOST_CHECK_EQUAL(obs.value().cached, 3u);
    BOOST_CHECK_EQUAL(obs.value().in_flight, 0u);

    // 已完成序号仍要求内容一致
    auto conflict = ing->Admit(MakeRequest(g, 1, "digest.other"));
    BOOST_REQUIRE(!conflict);
    ExpectOrderedError(conflict.error(), "SequenceConflict");
}

// 用例 6：结果缓存条数上限 FIFO 淘汰；淘汰后的旧序号 → ResultExpired，
// 不重新执行；淘汰不回退序号高水位（契约第 207/255 行）。
BOOST_AUTO_TEST_CASE(cache_eviction_by_count_then_expired) {
    auto ing = MakeIngress(8, 2, 4096);
    const auto g = MakeGrant("actor.a", "producer.1", "pe1", "re1");
    BOOST_REQUIRE(ing->GrantStream(g));

    for (std::uint64_t i = 1; i <= 3; ++i) {
        CompleteOk(ing, AdmitExecute(ing, MakeRequest(g, i)), "payload");
    }
    BOOST_CHECK_EQUAL(ing->CachedResultCount(), 2u);

    // 淘汰的是最早的 1 号
    auto expired = ing->Admit(MakeRequest(g, 1, "d1"));
    BOOST_REQUIRE(!expired);
    ExpectOrderedError(expired.error(), "ResultExpired");
    // 仍保留的 2、3 号重放原结果
    BOOST_CHECK(AdmitReplay(ing, MakeRequest(g, 2, "d1")).replay.is_ok);
    BOOST_CHECK(AdmitReplay(ing, MakeRequest(g, 3, "d1")).replay.is_ok);

    // 高水位不回退：下一个新序号仍是 4
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g), 4u);
    CompleteOk(ing, AdmitExecute(ing, MakeRequest(g, 4)), "payload");
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g), 5u);
    BOOST_CHECK_EQUAL(ing->CachedResultCount(), 2u);
}

// 用例 7：结果缓存字节上限 FIFO 淘汰（双限的第二维）。
BOOST_AUTO_TEST_CASE(cache_eviction_by_bytes) {
    auto ing = MakeIngress(8, 64, 10);
    const auto g = MakeGrant("actor.a", "producer.1", "pe1", "re1");
    BOOST_REQUIRE(ing->GrantStream(g));

    for (std::uint64_t i = 1; i <= 3; ++i) {
        CompleteOk(ing, AdmitExecute(ing, MakeRequest(g, i)),
                   std::string(4, 'a'));  // 每条 4 字节
    }
    BOOST_CHECK_EQUAL(ing->CachedResultBytes(), 8u);
    BOOST_CHECK_EQUAL(ing->CachedResultCount(), 2u);

    auto expired = ing->Admit(MakeRequest(g, 1, "d1"));
    BOOST_REQUIRE(!expired);
    ExpectOrderedError(expired.error(), "ResultExpired");
    BOOST_CHECK(AdmitReplay(ing, MakeRequest(g, 3, "d1")).replay.is_ok);
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g), 4u);
}

// 用例 8：单条终态本身超过字节上限时无法保留——序号仍被消耗，后续
// 重复请求一律 ResultExpired，不重新执行。
BOOST_AUTO_TEST_CASE(oversized_single_result_is_not_retained) {
    auto ing = MakeIngress(8, 64, 4);
    const auto g = MakeGrant("actor.a", "producer.1", "pe1", "re1");
    BOOST_REQUIRE(ing->GrantStream(g));

    const auto req = MakeRequest(g, 1, "d1");
    CompleteOk(ing, AdmitExecute(ing, req), std::string(10, 'x'));
    BOOST_CHECK_EQUAL(ing->CachedResultCount(), 0u);
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g), 2u);  // 序号已消耗

    auto expired = ing->Admit(req);
    BOOST_REQUIRE(!expired);
    ExpectOrderedError(expired.error(), "ResultExpired");
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g), 2u);
}

// 用例 9：服务级缓存上限跨流聚合——max_cached_results 是本组件（本服务）
// 跨流保留终态的总条数上限，不是每流上限。两条流各缓存若干条后，聚合
// CachedResultCount 不得超过上限；按全局完成序淘汰最旧条目（契约第 207
// 行把三项有序上限并列在 ServiceOptions，与 max_ordered_streams 同为
// 服务级）。
BOOST_AUTO_TEST_CASE(cache_limit_is_service_wide_across_streams) {
    auto ing = MakeIngress(8, 3, 4096);   // 服务级上限 3 条
    const auto g_a = MakeGrant("actor.a", "producer.1", "pe1", "re1");
    const auto g_b = MakeGrant("actor.b", "producer.1", "pe1", "re1");
    BOOST_REQUIRE(ing->GrantStream(g_a));
    BOOST_REQUIRE(ing->GrantStream(g_b));

    // 交错完成：a1, b1, a2, b2（完成序 a1<b1<a2<b2）
    CompleteOk(ing, AdmitExecute(ing, MakeRequest(g_a, 1)), "a1");
    CompleteOk(ing, AdmitExecute(ing, MakeRequest(g_b, 1)), "b1");
    CompleteOk(ing, AdmitExecute(ing, MakeRequest(g_a, 2)), "a2");
    CompleteOk(ing, AdmitExecute(ing, MakeRequest(g_b, 2)), "b2");

    // 服务级上界真实：聚合条数 = 3，不是 流数×上限 = 8
    BOOST_CHECK_EQUAL(ing->CachedResultCount(), 3u);

    // 全局完成序 FIFO 淘汰：最旧的 a1 被淘汰 → ResultExpired
    auto a1 = ing->Admit(MakeRequest(g_a, 1, "d1"));
    BOOST_REQUIRE(!a1);
    ExpectOrderedError(a1.error(), "ResultExpired");
    // b1/a2/b2 仍保留 → Replay
    BOOST_CHECK(AdmitReplay(ing, MakeRequest(g_b, 1, "d1")).replay.is_ok);
    BOOST_CHECK(AdmitReplay(ing, MakeRequest(g_a, 2, "d1")).replay.is_ok);
    BOOST_CHECK(AdmitReplay(ing, MakeRequest(g_b, 2, "d1")).replay.is_ok);

    // 跨流淘汰不回退任一序号高水位
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g_a), 3u);
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g_b), 3u);
}

// 用例 10：授权流校验——未授权/不匹配字段一律 StreamRejected，不自动
// 建流、不消费序号（契约第 247 行）。
BOOST_AUTO_TEST_CASE(grant_mismatch_is_stream_rejected) {
    auto ing = MakeIngress();
    const auto g = MakeGrant("actor.a", "producer.1", "pe1", "re1",
                             "peer.a");
    BOOST_REQUIRE(ing->GrantStream(g));

    const auto expect_rejected =
        [&](const fw::OrderedIngressRequest& req, const std::string& what) {
            auto r = ing->Admit(req);
            BOOST_REQUIRE_MESSAGE(!r, what);
            ExpectOrderedError(r.error(), "StreamRejected");
        };

    auto unknown_producer = MakeRequest(g, 1);
    unknown_producer.producer_id = "producer.other";
    expect_rejected(unknown_producer, "unknown producer");

    auto unknown_actor = MakeRequest(g, 1);
    unknown_actor.actor_key = "actor.other";
    expect_rejected(unknown_actor, "unknown actor key");

    auto unknown_epoch = MakeRequest(g, 1);
    unknown_epoch.producer_epoch = "pe2";
    expect_rejected(unknown_epoch, "unknown producer epoch");

    auto unknown_receiver = MakeRequest(g, 1);
    unknown_receiver.receiver_epoch = "re2";
    expect_rejected(unknown_receiver, "unknown receiver epoch");

    auto other_peer = MakeRequest(g, 1);
    other_peer.peer_principal = "peer.b";
    expect_rejected(other_peer, "unauthorized peer principal");

    auto empty_producer = MakeRequest(g, 1);
    empty_producer.producer_id.clear();
    expect_rejected(empty_producer, "incomplete stamp");

    // 全部拒绝后序号状态未动、无在途项
    auto obs = ing->ObserveStream(g.actor_key, g.producer_id, g.producer_epoch);
    BOOST_REQUIRE(obs);
    BOOST_CHECK_EQUAL(obs.value().next_sequence, 1u);
    BOOST_CHECK_EQUAL(obs.value().in_flight, 0u);
    CompleteOk(ing, AdmitExecute(ing, MakeRequest(g, 1)), "ok");
}

// 用例 10：sequence == 0 不是有效分配值（发送侧从 1 起），按缺口拒绝
// 且给出期望序号 1，不消费序号。
BOOST_AUTO_TEST_CASE(zero_sequence_is_gap) {
    auto ing = MakeIngress();
    const auto g = MakeGrant("actor.a", "producer.1", "pe1", "re1");
    BOOST_REQUIRE(ing->GrantStream(g));

    auto r = ing->Admit(MakeRequest(g, 0));
    BOOST_REQUIRE(!r);
    ExpectOrderedError(r.error(), "SequenceGap");
    const std::string* expected = DetailValue(r.error(), "expected_sequence");
    BOOST_REQUIRE(expected != nullptr);
    BOOST_CHECK_EQUAL(*expected, "1");
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g), 1u);
    CompleteOk(ing, AdmitExecute(ing, MakeRequest(g, 1)), "ok");
}

// 用例 11：无票据调用启用有序入口的目标 → OrderedStampRequired，
// 不消费序号、不执行（契约第 249 行）。
BOOST_AUTO_TEST_CASE(ticket_less_call_requires_ordered_stamp) {
    auto ing = MakeIngress();
    const auto g = MakeGrant("actor.a", "producer.1", "pe1", "re1");
    BOOST_REQUIRE(ing->GrantStream(g));

    auto bare = MakeRequest(g, 1);
    bare.has_ordered_stamp = false;
    auto r = ing->Admit(bare);
    BOOST_REQUIRE(!r);
    ExpectOrderedError(r.error(), "OrderedStampRequired");
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g), 1u);

    CompleteOk(ing, AdmitExecute(ing, MakeRequest(g, 1)), "ok");
}

// 用例 12：授权安装——幂等重装不重置序号；同流标识冲突 grant →
// StreamRejected；超 max_ordered_streams → Overloaded；非法字段/
// 服务不符 → InvalidArgument。
BOOST_AUTO_TEST_CASE(grant_install_idempotent_conflict_and_capacity) {
    auto ing = MakeIngress(2, 64, 4096);
    const auto g1 = MakeGrant("actor.a", "producer.1", "pe1", "re1");
    BOOST_REQUIRE(ing->GrantStream(g1));
    CompleteOk(ing, AdmitExecute(ing, MakeRequest(g1, 1)), "ok");

    // 幂等重装：不重置序号
    BOOST_REQUIRE(ing->GrantStream(g1));
    BOOST_CHECK_EQUAL(ing->StreamCount(), 1u);
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g1), 2u);

    // 同流标识（五字段）但完整 grant 不同 → StreamRejected
    auto conflicting = g1;
    conflicting.peer_principal = "peer.b";
    auto conflict = ing->GrantStream(conflicting);
    BOOST_REQUIRE(!conflict);
    ExpectOrderedError(conflict.error(), "StreamRejected");

    // 不同 producer 流 → 独立授权
    const auto g2 = MakeGrant("actor.a", "producer.2", "pe1", "re1");
    BOOST_REQUIRE(ing->GrantStream(g2));
    BOOST_CHECK_EQUAL(ing->StreamCount(), 2u);

    // 容量耗尽：不淘汰活动流，明确失败
    const auto g3 = MakeGrant("actor.a", "producer.3", "pe1", "re1");
    auto over = ing->GrantStream(g3);
    BOOST_REQUIRE(!over);
    BOOST_CHECK(over.error().code == fw::ErrorCode::Overloaded);
    BOOST_CHECK_EQUAL(ing->StreamCount(), 2u);

    // 字段非法
    auto empty_field = MakeGrant("", "producer.9", "pe1", "re1");
    auto r_empty = ing->GrantStream(empty_field);
    BOOST_REQUIRE(!r_empty);
    BOOST_CHECK(r_empty.error().code == fw::ErrorCode::InvalidArgument);

    auto foreign = MakeGrant("actor.a", "producer.9", "pe1", "re1");
    foreign.service = "svc.other";
    auto r_foreign = ing->GrantStream(foreign);
    BOOST_REQUIRE(!r_foreign);
    BOOST_CHECK(r_foreign.error().code == fw::ErrorCode::InvalidArgument);
}

// 用例 13：epoch 切换——同一 producer 流安装新 receiver_epoch 授权即
// 撤销旧授权与旧序号状态：旧 epoch 请求一律 StreamRejected，新 epoch
// 重新从序号 1 开始（契约第 251 行）。
BOOST_AUTO_TEST_CASE(old_receiver_epoch_session_rejected) {
    auto ing = MakeIngress();
    const auto g_old = MakeGrant("actor.a", "producer.1", "pe1", "re1");
    BOOST_REQUIRE(ing->GrantStream(g_old));
    CompleteOk(ing, AdmitExecute(ing, MakeRequest(g_old, 1)), "ok");
    CompleteOk(ing, AdmitExecute(ing, MakeRequest(g_old, 2)), "ok");
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g_old), 3u);

    const auto g_new = MakeGrant("actor.a", "producer.1", "pe1", "re2");
    BOOST_REQUIRE(ing->GrantStream(g_new));
    BOOST_CHECK_EQUAL(ing->StreamCount(), 1u);

    auto stale = ing->Admit(MakeRequest(g_old, 3));
    BOOST_REQUIRE(!stale);
    ExpectOrderedError(stale.error(), "StreamRejected");

    // 新会话独立序号空间，从 1 起
    auto fresh = AdmitExecute(ing, MakeRequest(g_new, 1));
    BOOST_CHECK_EQUAL(fresh.sequence, 1u);
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g_new), 2u);
}

// 用例 14：未接纳请求不消耗序号——连续拒绝后，下一个新请求仍按
// 原期望序号接纳（契约第 257 行）。
BOOST_AUTO_TEST_CASE(rejected_admission_does_not_consume_sequence) {
    auto ing = MakeIngress();
    const auto g = MakeGrant("actor.a", "producer.1", "pe1", "re1");
    BOOST_REQUIRE(ing->GrantStream(g));

    auto bare = MakeRequest(g, 1);
    bare.has_ordered_stamp = false;
    BOOST_CHECK(!ing->Admit(bare));

    auto unknown = MakeRequest(g, 1);
    unknown.producer_epoch = "pe9";
    BOOST_CHECK(!ing->Admit(unknown));

    BOOST_CHECK(!ing->Admit(MakeRequest(g, 5)));   // 缺口

    auto a = AdmitExecute(ing, MakeRequest(g, 1));  // 仍从 1 开始
    BOOST_CHECK_EQUAL(a.sequence, 1u);
}

// 用例 15a：单流保序——发送侧按序发出 seq=1..N，接收端按到达顺序消耗，
// 全部 Execute、next 前进到 N+1、无重复无跳号。契约：同一条流序号由发送
// 侧单点分配且天然稠密（TCP 字节流有序），接收端不做现场重排。
BOOST_AUTO_TEST_CASE(sequential_stream_is_dense) {
    constexpr std::uint64_t kTotal = 128;
    auto ing = MakeIngress(4, 4096, 1u << 20);
    const auto g = MakeGrant("actor.a", "producer.1", "pe1", "re1");
    BOOST_REQUIRE(ing->GrantStream(g));

    for (std::uint64_t i = 1; i <= kTotal; ++i) {
        auto a = AdmitExecute(ing, MakeRequest(g, i));
        BOOST_CHECK_EQUAL(a.sequence, i);
        CompleteOk(ing, a, "ok");
    }
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g), kTotal + 1);
    BOOST_CHECK_EQUAL(ing->CachedResultCount(), kTotal);
}

// 用例 15b：并发幂等——N 个线程同时向同一流 Admit【同一个】序号 k，
// 序号状态机不撕裂：恰好一个返回 Execute（消耗序号、进入在途），其余在
// 该请求在途时返回 InProgress；首个 Complete 落地后再来的重复返回
// Replay 原终态。全程只有一个序号被消耗，next 只前进一格。
// 这证的是并发下的幂等去重与状态机线程安全，而非"并发分配序号"——
// 客户端序号由发送侧票据分配，不是服务端现场发的。
BOOST_AUTO_TEST_CASE(concurrent_duplicate_admit_is_idempotent) {
    constexpr int kThreads = 128;
    auto ing = MakeIngress(4, 4096, 1u << 20);
    const auto g = MakeGrant("actor.a", "producer.1", "pe1", "re1");
    BOOST_REQUIRE(ing->GrantStream(g));

    // 先把序号推进到 k=5：seq 1..4 顺序完成，next 变为 5。
    for (std::uint64_t i = 1; i <= 4; ++i) {
        CompleteOk(ing, AdmitExecute(ing, MakeRequest(g, i)), "warm");
    }
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g), 5u);

    const std::uint64_t k = 5;
    Barrier barrier(kThreads);
    std::atomic<int> exec_count{0};
    std::atomic<int> inprogress_count{0};
    std::atomic<int> replay_count{0};
    std::atomic<int> other_fail{0};
    std::vector<fw::OrderedIngress::Admission> winner(kThreads);
    std::vector<int> outcome(kThreads, -1);  // 0=Execute 1=InProgress 2=Replay
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            barrier.ArriveAndWait();
            // 同流同序号同摘要：语义上的"同一请求重发"。
            auto r = ing->Admit(MakeRequest(g, k, "dup.digest"));
            if (!r) {
                const auto& e = r.error();
                if (e.code == fw::ErrorCode::RemoteError &&
                    e.domain_code == fw::ordered_domain_code::kInProgress) {
                    outcome[t] = 1;
                    inprogress_count.fetch_add(1);
                } else {
                    other_fail.fetch_add(1);
                }
                return;
            }
            auto kind = r.value().kind;
            if (kind == fw::OrderedIngress::DecisionKind::Execute) {
                outcome[t] = 0;
                winner[t] = r.value();
                exec_count.fetch_add(1);
            } else if (kind == fw::OrderedIngress::DecisionKind::Replay) {
                outcome[t] = 2;
                replay_count.fetch_add(1);
            } else {
                other_fail.fetch_add(1);
            }
        });
    }
    for (auto& th : threads) th.join();

    // 恰好一个线程拿到 Execute，其余都在在途窗口内被判 InProgress
    // （在首个 Complete 之前全部并发到达，不会出现 Replay）。
    BOOST_CHECK_EQUAL(exec_count.load(), 1);
    BOOST_CHECK_EQUAL(inprogress_count.load(), kThreads - 1);
    BOOST_CHECK_EQUAL(replay_count.load(), 0);
    BOOST_CHECK_EQUAL(other_fail.load(), 0);

    // 序号只被消耗一次：Complete 胜出的票据后，next 从 5 → 6。
    int w = -1;
    for (int t = 0; t < kThreads; ++t) if (outcome[t] == 0) w = t;
    BOOST_REQUIRE_GE(w, 0);
    CompleteOk(ing, winner[w], "done");
    BOOST_CHECK_EQUAL(NextSequenceOf(ing, g), 6u);
    BOOST_CHECK_EQUAL(ing->CachedResultCount(), 5u);  // 4 warm + 1 done

    // Complete 之后，同一请求再到达 → Replay 原终态（幂等去重）。
    auto dup = ing->Admit(MakeRequest(g, k, "dup.digest"));
    BOOST_REQUIRE(dup);
    BOOST_CHECK(dup.value().kind == fw::OrderedIngress::DecisionKind::Replay);
}

// 用例 16：Complete 只接受本组件签发的 Execute 在途票据。
BOOST_AUTO_TEST_CASE(complete_rejects_foreign_admission) {
    auto ing = MakeIngress();
    const auto g = MakeGrant("actor.a", "producer.1", "pe1", "re1");
    BOOST_REQUIRE(ing->GrantStream(g));

    auto a = AdmitExecute(ing, MakeRequest(g, 1));
    CompleteOk(ing, a, "ok");
    // 重复完成 → 拒绝（已不在途）
    auto again = ing->Complete(a, OkReply("ok"));
    BOOST_REQUIRE(!again);
    BOOST_CHECK(again.error().code == fw::ErrorCode::InvalidArgument);

    // 伪造票据（序号不在途、流不存在）→ 拒绝
    fw::OrderedIngress::Admission forged;
    forged.kind           = fw::OrderedIngress::DecisionKind::Execute;
    forged.sequence       = 99;
    forged.actor_key      = g.actor_key;
    forged.producer_id    = g.producer_id;
    forged.producer_epoch = g.producer_epoch;
    auto r = ing->Complete(forged, OkReply("ok"));
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == fw::ErrorCode::InvalidArgument);

    // Replay 决策不是可完成的票据
    CompleteOk(ing, AdmitExecute(ing, MakeRequest(g, 2)), "ok");
    auto replay = AdmitReplay(ing, MakeRequest(g, 1, "d1"));
    auto r2 = ing->Complete(replay, OkReply("ok"));
    BOOST_REQUIRE(!r2);
    BOOST_CHECK(r2.error().code == fw::ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_SUITE_END()
