// co-service-actor/v1 F1-a：RequestContext/RequestScope 与
// CallOptionsAdapter 的 deadline/cancel 继承语义测试。
// 全部确定性可测：通过注入点断言「未发起 I/O / 未消耗序号」，
// 不用 sleep 凑时序、无空断言。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <string>
#include <type_traits>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/framework/OrderedSession.hpp>
#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/internal/CallOptionsAdapter.hpp>
#include <bbt/framework/internal/RequestScope.hpp>

namespace fw = bbt::framework;
namespace co = bbt::coroutine;
using Clock = std::chrono::steady_clock;

// 用例 9 的编译期部分：framework::CallOptions 与 infra::CallOptions
// 是分层类型，不可直接互换（契约第 143 行）。
static_assert(!std::is_same<fw::CallOptions, bbt::infra::CallOptions>::value,
    "framework::CallOptions and infra::CallOptions must be distinct types");
static_assert(
    !std::is_convertible<fw::CallOptions, bbt::infra::CallOptions>::value &&
    !std::is_convertible<bbt::infra::CallOptions, fw::CallOptions>::value,
    "framework::CallOptions and infra::CallOptions must not be convertible");

namespace {

std::shared_ptr<const fw::RequestContext> MakeCtx(
    const std::string&       request_id,
    co::Deadline             deadline,
    co::CancellationToken    cancel = co::CancellationToken{})
{
    auto ctx = std::make_shared<fw::RequestContext>();
    ctx->request_id = request_id;
    ctx->deadline   = deadline;
    ctx->cancel     = cancel;
    return ctx;
}

// 注入点记录仪：io_calls/seq_calls 断言「动作是否发生」，
// last_io_options 捕获实际传给下游的 infra::CallOptions。
struct SpyHooks {
    int                     io_calls = 0;
    int                     seq_calls = 0;
    bbt::infra::CallOptions last_io_options{};
    fw::CallEgressHooks     hooks;

    SpyHooks() {
        hooks.initiate_io = [this](const bbt::infra::CallOptions& o) {
            ++io_calls;
            last_io_options = o;
            return fw::result<void>::ok();
        };
        hooks.consume_sequence = [this](const fw::OrderedStamp&) {
            ++seq_calls;
            return fw::result<void>::ok();
        };
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
    void Wait() {
        std::unique_lock<std::mutex> lk(m_mtx);
        m_cv.wait(lk, [this] { return m_count == 0; });
    }
private:
    std::mutex              m_mtx;
    std::condition_variable m_cv;
    int                     m_count;
};

// 协程用例需要真实调度器；2 个静态 worker 足够（用例内不并发压测）。
struct SchedulerFixture {
    SchedulerFixture() {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
        g_scheduler->Start();
    }
    ~SchedulerFixture() { g_scheduler->Stop(); }
};

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

} // namespace

BOOST_TEST_GLOBAL_FIXTURE(SchedulerFixture);

BOOST_AUTO_TEST_SUITE(framework_f1a)

// 用例 1：缺省 deadline 继承父请求剩余预算；父预算变小后取到的值随之变小。
BOOST_AUTO_TEST_CASE(default_deadline_inherits_parent_remaining_budget) {
    SpyHooks spy;
    const auto d_far  = Clock::now() + std::chrono::seconds{60};
    const auto d_near = Clock::now() + std::chrono::seconds{5};
    auto parent_far  = MakeCtx("r-far",  d_far);
    auto parent_near = MakeCtx("r-near", d_near);

    fw::CallOptions opt;    // deadline 缺省
    auto r1 = fw::AdaptCallOptions(parent_far.get(), opt, spy.hooks);
    BOOST_REQUIRE(r1);
    BOOST_CHECK(r1.value().deadline == d_far);

    auto r2 = fw::AdaptCallOptions(parent_near.get(), opt, spy.hooks);
    BOOST_REQUIRE(r2);
    BOOST_CHECK(r2.value().deadline == d_near);
    BOOST_CHECK(spy.io_calls == 2);
}

// 用例 2：显式期限不得延长父预算（取最小值）；短于父预算则取显式值。
BOOST_AUTO_TEST_CASE(explicit_deadline_cannot_extend_parent_budget) {
    SpyHooks spy;
    const auto parent_d = Clock::now() + std::chrono::seconds{10};
    auto parent = MakeCtx("p", parent_d);

    fw::CallOptions longer;
    longer.deadline = parent_d + std::chrono::seconds{50};
    auto r1 = fw::AdaptCallOptions(parent.get(), longer, spy.hooks);
    BOOST_REQUIRE(r1);
    BOOST_CHECK(r1.value().deadline == parent_d);

    fw::CallOptions shorter;
    shorter.deadline = parent_d - std::chrono::seconds{5};
    auto r2 = fw::AdaptCallOptions(parent.get(), shorter, spy.hooks);
    BOOST_REQUIRE(r2);
    BOOST_CHECK(r2.value().deadline == *shorter.deadline);
}

// 用例 3：顶层入口无有限期限 → 配置失败（明确错误，不是默认无限）。
BOOST_AUTO_TEST_CASE(top_level_requires_finite_deadline) {
    SpyHooks spy;

    fw::CallOptions none;
    auto r1 = fw::AdaptCallOptions(nullptr, none, spy.hooks);
    BOOST_REQUIRE(!r1);
    BOOST_CHECK(r1.error().code == fw::ErrorCode::InvalidArgument);
    BOOST_CHECK(spy.io_calls == 0);
    BOOST_CHECK(spy.seq_calls == 0);

    fw::CallOptions inf;
    inf.deadline = co::Deadline::max();   // 显式无限同样拒绝
    auto r2 = fw::AdaptCallOptions(nullptr, inf, spy.hooks);
    BOOST_REQUIRE(!r2);
    BOOST_CHECK(r2.error().code == fw::ErrorCode::InvalidArgument);
    BOOST_CHECK(spy.io_calls == 0);
}

// 用例 4：已过期 → TimedOut，且下游 I/O 未发起、序号未消耗。
BOOST_AUTO_TEST_CASE(expired_returns_timedout_before_any_io) {
    SpyHooks spy;
    auto parent = MakeCtx("p", Clock::now() - std::chrono::milliseconds{1});
    fw::CallOptions opt;
    auto r1 = fw::AdaptCallOptions(parent.get(), opt, spy.hooks);
    BOOST_REQUIRE(!r1);
    BOOST_CHECK(r1.error().code == fw::ErrorCode::TimedOut);
    BOOST_CHECK(spy.io_calls == 0);
    BOOST_CHECK(spy.seq_calls == 0);

    // 顶层显式期限已过期同样 TimedOut。
    fw::CallOptions past;
    past.deadline = Clock::now() - std::chrono::milliseconds{1};
    auto r2 = fw::AdaptCallOptions(nullptr, past, spy.hooks);
    BOOST_REQUIRE(!r2);
    BOOST_CHECK(r2.error().code == fw::ErrorCode::TimedOut);
    BOOST_CHECK(spy.io_calls == 0);
}

// 用例 5：合并后 token 任一侧取消即取消，两侧独立触发均生效。
BOOST_AUTO_TEST_CASE(combined_cancel_either_side_works) {
    // 5a：父 token 取消 → 合并 token 取消。
    co::CancellationSource src_parent, src_call;
    SpyHooks spy_a;
    auto parent_a = MakeCtx("pa",
        Clock::now() + std::chrono::seconds{30}, src_parent.Token());
    fw::CallOptions opt_a;
    opt_a.cancel = src_call.Token();
    auto ra = fw::AdaptCallOptions(parent_a.get(), opt_a, spy_a.hooks);
    BOOST_REQUIRE(ra);
    auto combined_a = ra.value().cancel;
    BOOST_CHECK(!combined_a.IsCancellationRequested());
    src_parent.RequestCancel();
    BOOST_CHECK(combined_a.IsCancellationRequested());

    // 5b：本次 options.cancel 取消 → 合并 token 取消，父侧不受影响。
    co::CancellationSource src_parent2, src_call2;
    SpyHooks spy_b;
    auto parent_b = MakeCtx("pb",
        Clock::now() + std::chrono::seconds{30}, src_parent2.Token());
    fw::CallOptions opt_b;
    opt_b.cancel = src_call2.Token();
    auto rb = fw::AdaptCallOptions(parent_b.get(), opt_b, spy_b.hooks);
    BOOST_REQUIRE(rb);
    auto combined_b = rb.value().cancel;
    src_call2.RequestCancel();
    BOOST_CHECK(combined_b.IsCancellationRequested());
    BOOST_CHECK(!src_parent2.Token().IsCancellationRequested());
}

// 用例 6：传入 infra 的是合并后 token——撤销父预算后 infra 侧 token
// 也取消；未合并的用户 token 不随父取消。
BOOST_AUTO_TEST_CASE(infra_receives_combined_not_user_token) {
    co::CancellationSource src_parent, src_call;
    SpyHooks spy;
    auto parent = MakeCtx("p",
        Clock::now() + std::chrono::seconds{30}, src_parent.Token());
    fw::CallOptions opt;
    opt.cancel = src_call.Token();
    auto r = fw::AdaptCallOptions(parent.get(), opt, spy.hooks);
    BOOST_REQUIRE(r);
    BOOST_CHECK(spy.io_calls == 1);
    src_parent.RequestCancel();
    BOOST_CHECK(spy.last_io_options.cancel.IsCancellationRequested());
    BOOST_CHECK(!opt.cancel.IsCancellationRequested());
}

// 用例 7：非受管执行 → InvalidContext；受管作用域登记后可取出，
// 嵌套内层遮蔽外层、退出后恢复。
BOOST_AUTO_TEST_CASE(unmanaged_context_returns_invalid_context) {
    auto r0 = fw::CurrentRequestContext();
    BOOST_REQUIRE(!r0);
    BOOST_CHECK(r0.error().code == fw::ErrorCode::InvalidContext);

    // 非受管协程同样 InvalidContext。
    TestLatch latch(1);
    std::atomic<int> seen{0};   // 0=未运行 1=InvalidContext 2=其它
    bbtco_ref {
        auto r = fw::CurrentRequestContext();
        seen.store(!r && r.error().code == fw::ErrorCode::InvalidContext
                      ? 1 : 2);
        latch.CountDown();
    };
    latch.Wait();
    BOOST_CHECK(seen.load() == 1);

    // 受管作用域：登记后可取出且字段一致；嵌套遮蔽与退出恢复。
    auto outer = MakeCtx("outer", Clock::now() + std::chrono::seconds{30});
    {
        fw::RequestScope scope_outer(outer);
        auto r1 = fw::CurrentRequestContext();
        BOOST_REQUIRE(r1);
        BOOST_CHECK(r1.value()->request_id == "outer");
        {
            auto inner = MakeCtx("inner",
                Clock::now() + std::chrono::seconds{30});
            fw::RequestScope scope_inner(inner);
            auto r2 = fw::CurrentRequestContext();
            BOOST_REQUIRE(r2);
            BOOST_CHECK(r2.value()->request_id == "inner");
        }
        auto r3 = fw::CurrentRequestContext();
        BOOST_REQUIRE(r3);
        BOOST_CHECK(r3.value()->request_id == "outer");
    }
    auto r4 = fw::CurrentRequestContext();
    BOOST_REQUIRE(!r4);
    BOOST_CHECK(r4.error().code == fw::ErrorCode::InvalidContext);

    // 协程内建立的受管上下文经协程 id 绑定，可取出。
    TestLatch latch2(1);
    std::atomic<int> seen2{0};
    bbtco_ref {
        auto ctx = MakeCtx("co-scope",
            Clock::now() + std::chrono::seconds{30});
        fw::RequestScope scope(ctx);
        auto r = fw::CurrentRequestContext();
        seen2.store(r && r.value()->request_id == "co-scope" ? 1 : 2);
        latch2.CountDown();
    };
    latch2.Wait();
    BOOST_CHECK(seen2.load() == 1);
}

// 用例 8：装配抛异常 → InternalError，下游未调用、序号未消耗、
// 票据仍为未发送。
BOOST_AUTO_TEST_CASE(assembly_exception_internal_error_no_side_effects) {
    auto session_r = fw::OrderedSession::Open(MakeGrant("f1a8"));
    BOOST_REQUIRE(session_r);
    auto session = session_r.value();
    auto stamp_r = session->prepare();
    BOOST_REQUIRE(stamp_r);
    auto stamp = stamp_r.value();
    BOOST_CHECK(!session->IsTicketSent(stamp));

    SpyHooks spy;
    spy.hooks.combine_cancel =
        [](co::CancellationToken, co::CancellationToken)
            -> co::CancellationToken {
            throw std::bad_alloc();
        };
    auto parent = MakeCtx("p", Clock::now() + std::chrono::seconds{30});
    fw::CallOptions opt;
    opt.ordered = stamp;
    auto r = fw::AdaptCallOptions(parent.get(), opt, spy.hooks);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == fw::ErrorCode::InternalError);
    BOOST_CHECK(spy.io_calls == 0);
    BOOST_CHECK(spy.seq_calls == 0);
    BOOST_CHECK(!session->IsTicketSent(stamp));
}

// 用例 9：分层类型不可互换（编译期 static_assert 见文件头，
// 此处给出运行期可报告断言）。
BOOST_AUTO_TEST_CASE(layered_types_not_interchangeable) {
    BOOST_CHECK(
        (!std::is_same<fw::CallOptions, bbt::infra::CallOptions>::value));
    BOOST_CHECK(
        (!std::is_convertible<fw::CallOptions,
                              bbt::infra::CallOptions>::value));
}

// 补充正路径：成功装配 → 消耗序号（真实票据绑定）→ 发起 I/O，
// infra::CallOptions 携带生效期限与合并 token。
BOOST_AUTO_TEST_CASE(success_path_consumes_sequence_and_initiates_io) {
    auto session_r = fw::OrderedSession::Open(MakeGrant("f1a9"));
    BOOST_REQUIRE(session_r);
    auto session = session_r.value();
    auto stamp_r = session->prepare();
    BOOST_REQUIRE(stamp_r);
    auto stamp = stamp_r.value();

    SpyHooks spy;
    spy.hooks.consume_sequence =
        [&session, &spy](const fw::OrderedStamp& s) {
            ++spy.seq_calls;
            fw::OrderedTicket::BoundContent content;
            content.service = "svc";
            content.method  = "m";
            return session->BindForSend(s, content);
        };

    const auto parent_d = Clock::now() + std::chrono::seconds{30};
    auto parent = MakeCtx("p", parent_d);
    fw::CallOptions opt;
    opt.ordered = stamp;
    auto r = fw::AdaptCallOptions(parent.get(), opt, spy.hooks);
    BOOST_REQUIRE(r);
    BOOST_CHECK(spy.seq_calls == 1);
    BOOST_CHECK(spy.io_calls == 1);
    BOOST_CHECK(session->IsTicketSent(stamp));
    BOOST_CHECK(spy.last_io_options.deadline == parent_d);
}

BOOST_AUTO_TEST_SUITE_END()
