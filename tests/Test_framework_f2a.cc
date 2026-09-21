// F2-a 切片验证：执行策略校验矩阵、ActorSerial 缺 key 提取器启动失败、
// Actor 邮箱保序/非重入/容量/异常续跑/Close、跨 Actor 并发、
// ActorRegistry 至多一次与上限。
// 并发用例用条件变量屏障构造真实重叠窗口：handler 在 drain 协程内被
// 屏障拦停，另一个执行体获得完整进入机会；禁止 sleep 凑时序。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>

// 邮箱/注册表/方法表是机器面：显式包含 internal 头。
#include <bbt/framework/internal/ActorMailbox.hpp>
#include <bbt/framework/internal/ActorRegistry.hpp>
#include <bbt/framework/internal/MethodTable.hpp>

namespace fw = bbt::framework;

namespace {

// ── 测试屏障原语（std CV：线程与协程内 handler 均可用；协程内等待会占住
//    当前 worker 线程，测试固定 4 个静态 worker，屏障设计保证不死锁）──

// one-shot 门闩：Open 后所有 Wait 通过；WaitFor 返回是否及时等到。
class TestGate {
public:
    void Open() {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_open = true;
        }
        m_cv.notify_all();
    }
    void Wait() {
        std::unique_lock<std::mutex> lk(m_mtx);
        m_cv.wait(lk, [this] { return m_open; });
    }
    bool WaitFor(std::chrono::milliseconds ms) {
        std::unique_lock<std::mutex> lk(m_mtx);
        return m_cv.wait_for(lk, ms, [this] { return m_open; });
    }
private:
    std::mutex              m_mtx;
    std::condition_variable m_cv;
    bool                    m_open = false;
};

// count-down 闩：计满后 Wait 通过。
class TestLatch {
public:
    explicit TestLatch(int n) : m_count(n) {}
    void CountDown() {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (--m_count > 0)
                return;
        }
        m_cv.notify_all();
    }
    void Wait() {
        std::unique_lock<std::mutex> lk(m_mtx);
        m_cv.wait(lk, [this] { return m_count == 0; });
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

// 顺序记录仪：handler 并发/串行写都安全。
struct OrderRecorder {
    void Push(int i) {
        std::lock_guard<std::mutex> lk(m_mtx);
        values.push_back(i);
    }
    std::vector<int> Snapshot() const {
        std::lock_guard<std::mutex> lk(m_mtx);
        return values;
    }
    mutable std::mutex m_mtx;
    std::vector<int>   values;
};

// 全局 fixture：调度器只起一次，4 个静态 worker 保证跨 Actor 用例有
// 真实并行度；Stop 在所有用例之后（各用例已通过 latch 确认任务跑完）。
struct SchedulerFixture {
    SchedulerFixture() {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
        g_scheduler->Start();
    }
    ~SchedulerFixture() {
        g_scheduler->Stop();
    }
};

} // namespace

BOOST_TEST_GLOBAL_FIXTURE(SchedulerFixture);

namespace {

// 混合声明：一个无 key 的普通方法 + 一个 ActorMethodAt。
class MixedService final : public fw::CoService<MixedService> {
public:
    static constexpr std::string_view kServiceName = "mixed";
    fw::CoRpcResp Unkeyed(fw::CoRpcReq) {
        return fw::CoRpcResp::From(std::tuple<>{});
    }
    fw::CoRpcResp Keyed(fw::CoRpcReq) {
        return fw::CoRpcResp::From(std::tuple<>{});
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&MixedService::Unkeyed>("unkeyed"),
        fw::ActorMethodAt<&MixedService::Keyed, std::int32_t>("keyed"));
};

// 全部方法都经 ActorMethodAt 声明。
class AllKeyedService final : public fw::CoService<AllKeyedService> {
public:
    static constexpr std::string_view kServiceName = "keyed";
    fw::CoRpcResp A(fw::CoRpcReq) {
        return fw::CoRpcResp::From(std::tuple<>{});
    }
    fw::CoRpcResp B(fw::CoRpcReq) {
        return fw::CoRpcResp::From(std::tuple<>{});
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::ActorMethodAt<&AllKeyedService::A, std::int32_t>("a"),
        fw::ActorMethodAt<&AllKeyedService::B, std::int32_t>("b"));
};

// 注册表测试服务：构造计数验证「至多一次」。无 RPC 方法声明 → 空表。
class CountingService final : public fw::CoService<CountingService> {
public:
    CountingService() { s_ctor_count.fetch_add(1); }
    static constexpr std::string_view kServiceName = "counting";
    static std::atomic<int> s_ctor_count;
};
std::atomic<int> CountingService::s_ctor_count{0};

constexpr std::chrono::milliseconds kWait{5000};

BOOST_AUTO_TEST_SUITE(framework_f2a)

// ValidateServiceOptions 合法/非法矩阵。
BOOST_AUTO_TEST_CASE(validate_service_options_rules) {
    const fw::ServiceOptions actor_base{
        fw::ExecutionPolicy::ActorSerial,
        /*max_inflight*/ 64,
        /*mailbox_capacity*/ 16,
        /*max_actors*/ 8,
        /*ordered_ingress*/ false,
        /*max_ordered_streams*/ 0,
        /*max_cached_results*/ 0,
        /*max_cached_result_bytes*/ 0};
    // BOOST_CHECK 只记录表达式文本，不要求操作数可流式输出（result<T>）。
    BOOST_CHECK(fw::ValidateServiceOptions(actor_base));

    fw::ServiceOptions concurrent = actor_base;
    concurrent.execution = fw::ExecutionPolicy::Concurrent;
    concurrent.mailbox_capacity = 0;
    concurrent.max_actors = 0;
    BOOST_CHECK(fw::ValidateServiceOptions(concurrent));

    fw::ServiceOptions ordered = actor_base;
    ordered.ordered_ingress = true;
    ordered.max_ordered_streams = 4;
    ordered.max_cached_results = 32;
    ordered.max_cached_result_bytes = 4096;
    BOOST_CHECK(fw::ValidateServiceOptions(ordered));

    auto expect_invalid = [](fw::ServiceOptions o, const char* field) {
        auto r = fw::ValidateServiceOptions(o);
        BOOST_REQUIRE(!r);
        BOOST_CHECK(r.error().code == fw::ErrorCode::InvalidArgument);
        BOOST_TEST(r.error().message.find(field) != std::string::npos);
    };

    { auto o = actor_base; o.max_inflight = 0;
      expect_invalid(o, "max_inflight"); }
    { auto o = actor_base; o.mailbox_capacity = 0;
      expect_invalid(o, "mailbox_capacity"); }
    { auto o = actor_base; o.max_actors = 0;
      expect_invalid(o, "max_actors"); }
    { auto o = concurrent; o.mailbox_capacity = 1;
      expect_invalid(o, "mailbox_capacity"); }
    { auto o = concurrent; o.max_actors = 1;
      expect_invalid(o, "max_actors"); }
    // ordered_ingress 用在 Concurrent 上必须被拒。
    { auto o = concurrent; o.ordered_ingress = true;
      o.max_ordered_streams = 1; o.max_cached_results = 1;
      o.max_cached_result_bytes = 1;
      expect_invalid(o, "ordered_ingress"); }
    { auto o = ordered; o.max_ordered_streams = 0;
      expect_invalid(o, "max_ordered_streams"); }
    { auto o = ordered; o.max_cached_results = 0;
      expect_invalid(o, "max_cached_results"); }
    { auto o = ordered; o.max_cached_result_bytes = 0;
      expect_invalid(o, "max_cached_result_bytes"); }
    { auto o = actor_base; o.max_ordered_streams = 1;
      expect_invalid(o, "max_ordered_streams"); }
    { auto o = actor_base; o.max_cached_results = 1;
      expect_invalid(o, "max_cached_results"); }
    { auto o = actor_base; o.max_cached_result_bytes = 1;
      expect_invalid(o, "max_cached_result_bytes"); }
}

// ActorSerial 服务存在无 key 提取器的方法 → 启动期校验失败。
BOOST_AUTO_TEST_CASE(actor_serial_requires_key_extractor) {
    const auto mixed = fw::BuildMethodTable<MixedService>();
    auto bad = fw::ValidateActorKeying(mixed, fw::ExecutionPolicy::ActorSerial);
    BOOST_REQUIRE(!bad);
    BOOST_CHECK(bad.error().code == fw::ErrorCode::InvalidArgument);
    BOOST_TEST(bad.error().message.find("unkeyed") != std::string::npos);

    BOOST_CHECK(
        fw::ValidateActorKeying(mixed, fw::ExecutionPolicy::Concurrent));
    BOOST_CHECK(fw::ValidateActorKeying(
        fw::BuildMethodTable<AllKeyedService>(),
        fw::ExecutionPolicy::ActorSerial));
}

// 保序：首项用屏障拦在 handler 内，保证后续项全部排队后仍按接纳顺序处理。
BOOST_AUTO_TEST_CASE(mailbox_preserves_admission_order) {
    auto mb = fw::ActorMailbox::Create(8);
    TestGate entered, release;
    TestLatch done(6);
    OrderRecorder rec;

    BOOST_REQUIRE(mb->TryEnqueue([&] {
        entered.Open();
        release.Wait();
        rec.Push(0);
        done.CountDown();
    }));
    // 首项已进入 handler（执行中），此时再接纳的项必然全部落在等待队列。
    BOOST_REQUIRE(entered.WaitFor(kWait));
    for (int i = 1; i <= 5; ++i)
        BOOST_REQUIRE(mb->TryEnqueue([&rec, &done, i] {
            rec.Push(i);
            done.CountDown();
        }));
    release.Open();
    BOOST_REQUIRE(done.WaitFor(kWait));
    BOOST_TEST(rec.Snapshot() == std::vector<int>({0, 1, 2, 3, 4, 5}));
}

// 非重入：handler1 被屏障拦在执行中时再接纳 handler2；
// 重入实现会让 handler2 在 handler1 阻塞期间进入（leaked=true 即失败）。
BOOST_AUTO_TEST_CASE(mailbox_non_reentrant) {
    auto mb = fw::ActorMailbox::Create(4);
    std::atomic<int> in_exec{0}, max_exec{0};
    TestGate entered1, entered2, release1;
    TestLatch done(2);
    OrderRecorder rec;
    auto note_entry = [&] {
        const int n = in_exec.fetch_add(1) + 1;
        int prev = max_exec.load();
        while (n > prev && !max_exec.compare_exchange_weak(prev, n)) {
        }
    };

    BOOST_REQUIRE(mb->TryEnqueue([&] {
        note_entry();
        entered1.Open();
        release1.Wait();          // 执行中阻塞：给 handler2 完整重叠窗口
        rec.Push(1);
        in_exec.fetch_sub(1);
        done.CountDown();
    }));
    BOOST_REQUIRE(entered1.WaitFor(kWait));   // handler1 确认在执行中

    BOOST_REQUIRE(mb->TryEnqueue([&] {
        note_entry();
        entered2.Open();
        rec.Push(2);
        in_exec.fetch_sub(1);
        done.CountDown();
    }));
    // 正确实现下 handler2 此刻只能排队，entered2 不可能提前打开；
    // 若实现重入（每条任务一个协程），200ms 内必然进入并打开 entered2。
    const bool leaked = entered2.WaitFor(std::chrono::milliseconds(200));
    release1.Open();
    BOOST_REQUIRE(done.WaitFor(kWait));

    BOOST_TEST(!leaked);
    BOOST_TEST(max_exec.load() == 1);
    BOOST_TEST(rec.Snapshot() == std::vector<int>({1, 2}));
    BOOST_TEST(!mb->IsDraining());            // 执行资格已释放
}

// 容量：等待队列满后再接纳返回 Overloaded，被拒任务不得执行。
BOOST_AUTO_TEST_CASE(mailbox_full_returns_overloaded) {
    auto mb = fw::ActorMailbox::Create(2);
    TestGate entered, release;
    TestLatch done(3);
    OrderRecorder rec;
    std::atomic<bool> rejected_ran{false};

    BOOST_REQUIRE(mb->TryEnqueue([&] {
        entered.Open();
        release.Wait();
        rec.Push(0);
        done.CountDown();
    }));
    BOOST_REQUIRE(entered.WaitFor(kWait));    // 执行中项不占容量
    BOOST_REQUIRE(mb->TryEnqueue([&] { rec.Push(1); done.CountDown(); }));
    BOOST_REQUIRE(mb->TryEnqueue([&] { rec.Push(2); done.CountDown(); }));
    BOOST_TEST(mb->Pending() == std::size_t{2});

    auto r = mb->TryEnqueue([&] { rejected_ran = true; done.CountDown(); });
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == fw::ErrorCode::Overloaded);
    BOOST_TEST(r.error().message.find("mailbox_capacity")
               != std::string::npos);

    release.Open();
    BOOST_REQUIRE(done.WaitFor(kWait));
    BOOST_TEST(rec.Snapshot() == std::vector<int>({0, 1, 2}));
    BOOST_TEST(!rejected_ran.load());
}

// 异常路径：handler 抛异常后下一条仍被处理，且异常经 hook 可观测。
BOOST_AUTO_TEST_CASE(mailbox_exception_continues_queue) {
    std::atomic<int> abnormal{0};
    auto mb = fw::ActorMailbox::Create(4,
        [&abnormal](std::exception_ptr) { abnormal.fetch_add(1); });
    TestLatch done12(2), done3(1);
    OrderRecorder rec;

    BOOST_REQUIRE(mb->TryEnqueue([&] {
        rec.Push(0);
        done12.CountDown();
        throw std::runtime_error("handler boom");
    }));
    BOOST_REQUIRE(mb->TryEnqueue([&] { rec.Push(1); done12.CountDown(); }));
    BOOST_REQUIRE(done12.WaitFor(kWait));
    BOOST_TEST(abnormal.load() == 1);
    BOOST_TEST(rec.Snapshot() == std::vector<int>({0, 1}));

    // 异常后邮箱仍可继续接纳。
    BOOST_REQUIRE(mb->TryEnqueue([&] { rec.Push(2); done3.CountDown(); }));
    BOOST_REQUIRE(done3.WaitFor(kWait));
    BOOST_TEST(rec.Snapshot() == std::vector<int>({0, 1, 2}));
}

// 跨 Actor 并发：两个邮箱的 handler 互相等对方进入执行，
// 两侧都等到才证明执行真实重叠，而非各自跑完。
BOOST_AUTO_TEST_CASE(mailboxes_progress_concurrently) {
    auto mb_a = fw::ActorMailbox::Create(4);
    auto mb_b = fw::ActorMailbox::Create(4);
    TestGate a_in, b_in;
    TestLatch done(2);
    std::atomic<bool> a_saw_b{false}, b_saw_a{false};

    BOOST_REQUIRE(mb_a->TryEnqueue([&] {
        a_in.Open();
        if (b_in.WaitFor(kWait))
            a_saw_b = true;
        done.CountDown();
    }));
    BOOST_REQUIRE(mb_b->TryEnqueue([&] {
        b_in.Open();
        if (a_in.WaitFor(kWait))
            b_saw_a = true;
        done.CountDown();
    }));
    BOOST_REQUIRE(done.WaitFor(kWait));
    BOOST_TEST(a_saw_b.load());
    BOOST_TEST(b_saw_a.load());
}

// Close 停止接纳；已接纳项仍被消费完。
BOOST_AUTO_TEST_CASE(mailbox_close_stops_admission) {
    auto mb = fw::ActorMailbox::Create(4);
    TestGate entered, release;
    TestLatch done(1);

    BOOST_REQUIRE(mb->TryEnqueue([&] {
        entered.Open();
        release.Wait();
        done.CountDown();
    }));
    BOOST_REQUIRE(entered.WaitFor(kWait));
    mb->Close();
    BOOST_TEST(mb->IsClosed());

    auto r = mb->TryEnqueue([] {});
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == fw::ErrorCode::Closed);

    release.Open();
    BOOST_REQUIRE(done.WaitFor(kWait));
}

// Registry：同 key 至多一次（返回同一实例、工厂只调一次）、key 可读回、
// max_actors 超限拒绝、未注册服务拒绝。
BOOST_AUTO_TEST_CASE(registry_at_most_once_and_max_actors) {
    fw::ActorRegistry reg;
    BOOST_REQUIRE(reg.RegisterService<CountingService>("svc", 2));
    const int before = CountingService::s_ctor_count.load();

    auto a1 = reg.GetOrCreate("svc", "k1");
    BOOST_REQUIRE(a1);
    auto a1b = reg.GetOrCreate("svc", "k1");
    BOOST_REQUIRE(a1b);
    BOOST_TEST(a1.value().get() == a1b.value().get());   // 同一实例
    BOOST_TEST(CountingService::s_ctor_count.load() == before + 1);
    BOOST_REQUIRE(a1.value()->co_actor_key().has_value());
    BOOST_TEST(a1.value()->co_actor_key().value() == "k1");

    auto a2 = reg.GetOrCreate("svc", "k2");
    BOOST_REQUIRE(a2);
    BOOST_TEST(a2.value().get() != a1.value().get());

    auto a3 = reg.GetOrCreate("svc", "k3");
    BOOST_REQUIRE(!a3);
    BOOST_CHECK(a3.error().code == fw::ErrorCode::Overloaded);
    BOOST_TEST(a3.error().message.find("max_actors") != std::string::npos);
    BOOST_TEST(reg.ActorCount("svc") == std::size_t{2});

    auto unreg = reg.GetOrCreate("no-such-service", "k");
    BOOST_REQUIRE(!unreg);
    BOOST_CHECK(unreg.error().code == fw::ErrorCode::NotFound);
}

// 激活失败不留半激活对象：不占位不计数，同 key 重试可成功。
BOOST_AUTO_TEST_CASE(registry_failed_activation_leaves_no_partial) {
    fw::ActorRegistry reg;
    std::atomic<int> calls{0};
    BOOST_REQUIRE(reg.RegisterFactory("boom",
        [&calls]() -> std::shared_ptr<fw::ICoService> {
            if (calls.fetch_add(1) == 0)
                throw std::runtime_error("ctor fail");
            return std::static_pointer_cast<fw::ICoService>(
                std::make_shared<CountingService>());
        }, 1));

    auto r1 = reg.GetOrCreate("boom", "k");
    BOOST_REQUIRE(!r1);
    BOOST_CHECK(r1.error().code == fw::ErrorCode::InternalError);
    BOOST_TEST(reg.ActorCount("boom") == std::size_t{0});

    auto r2 = reg.GetOrCreate("boom", "k");
    BOOST_REQUIRE(r2);
    BOOST_TEST(calls.load() == 2);
    BOOST_REQUIRE(r2.value()->co_actor_key().has_value());
    BOOST_TEST(r2.value()->co_actor_key().value() == "k");
    BOOST_TEST(reg.ActorCount("boom") == std::size_t{1});

    auto r3 = reg.GetOrCreate("boom", "k");
    BOOST_REQUIRE(r3);
    BOOST_TEST(r3.value().get() == r2.value().get());
    BOOST_TEST(calls.load() == 2);
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace
