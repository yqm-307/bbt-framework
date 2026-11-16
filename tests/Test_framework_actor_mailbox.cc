/**
 * 执行策略校验矩阵、ActorSerial 缺 key 提取器启动失败、
 * Actor 邮箱保序/非重入/容量/异常续跑/Close、跨 Actor 并发。
 * 并发用例用条件变量屏障构造真实重叠窗口：handler 在 drain 协程内被
 * 屏障拦停，另一个执行体获得完整进入机会；禁止 sleep 凑时序。
 */
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

/**
 * 邮箱/方法表是机器面：显式包含 internal 头。
 */
#include <bbt/framework/internal/ActorMailbox.hpp>
#include <bbt/framework/internal/MethodTable.hpp>

namespace fw = bbt::framework;

namespace {

/**
 * ── 测试屏障原语（std CV：线程与协程内 handler 均可用；协程内等待会占住
 * 当前 worker 线程，测试固定 4 个静态 worker，屏障设计保证不死锁）──
 */

/**
 * one-shot 门闩：Open 后所有 Wait 通过；WaitFor 返回是否及时等到。
 */
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

/**
 * count-down 闩：计满后 Wait 通过。
 */
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

/**
 * 轮询等待谓词成立（带超时）：用于「最终收敛」类断言——被测动作与观测
 * 点之间有调度间隙，立即检查会在慢 runner 上抖动。
 */
template <typename Pred>
bool WaitUntil(Pred&& pred, std::chrono::milliseconds ms) {
    const auto deadline = std::chrono::steady_clock::now() + ms;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred())
            return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return pred();
}

/**
 * 顺序记录仪：handler 并发/串行写都安全。
 */
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

/**
 * 全局 fixture：调度器是进程寿命单例，只 Start 一次、无 Stop；4 个静态
 * worker 保证跨 Actor 用例有真实并行度（各用例已通过 latch 确认任务跑完）。
 */
struct SchedulerFixture {
    SchedulerFixture() {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
        if (!g_scheduler->IsInitialized())
            g_scheduler->Start();
    }
};

} // namespace

BOOST_TEST_GLOBAL_FIXTURE(SchedulerFixture);

namespace {

/**
 * 混合声明：一个无 key 的普通方法 + 一个 ActorMethodAt。
 */
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

/**
 * 全部方法都经 ActorMethodAt 声明。
 */
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

constexpr std::chrono::milliseconds kWait{5000};

BOOST_AUTO_TEST_SUITE(framework_f2a)

/**
 * Options::Validate 合法/非法矩阵：执行策略（ServiceOptions）与
 * Actor 类型额度（ActorOptions）分别校验；错误信息指明违规字段。
 */
BOOST_AUTO_TEST_CASE(validate_service_options_rules) {
    const auto actor_base = fw::ServiceOptions{}
        .SetExecution(fw::ExecutionPolicy::ActorSerial)
        .SetMaxInflight(64)
        .SetMaxSerialKeys(16)
        .SetSerialQueueCapacity(64);
    /**
     * BOOST_CHECK 只记录表达式文本，不要求操作数可流式输出（result<T>）。
     */
    BOOST_CHECK(actor_base.Validate());

    auto concurrent = fw::ServiceOptions{}
        .SetExecution(fw::ExecutionPolicy::Concurrent)
        .SetMaxInflight(64);
    BOOST_CHECK(concurrent.Validate());

    auto ordered = fw::ServiceOptions{}
        .SetExecution(fw::ExecutionPolicy::ActorSerial)
        .SetMaxInflight(64)
        .SetMaxSerialKeys(16)
        .SetSerialQueueCapacity(64)
        .SetOrderedIngress(fw::OrderedIngressOptions{
            true, /*max_ordered_streams*/ 4,
            /*max_cached_results*/ 32,
            /*max_cached_result_bytes*/ 4096});
    BOOST_CHECK(ordered.Validate());

    const auto actor_opts = fw::ActorOptions{}
        .SetMaxActors(8)
        .SetMailboxCapacity(16)
        .SetAggregateQueueCapacity(64);
    BOOST_CHECK(actor_opts.Validate());

    auto expect_invalid = [](auto r, const char* field) {
        BOOST_REQUIRE(!r);
        BOOST_CHECK(r.error().code == fw::ErrorCode::InvalidArgument);
        BOOST_TEST(r.error().message.find(field) != std::string::npos);
    };

    { auto o = actor_base; o.SetMaxInflight(0);
      expect_invalid(o.Validate(), "max_inflight"); }
    { auto o = actor_base; o.SetMaxSerialKeys(0);
      expect_invalid(o.Validate(), "max_serial_keys"); }
    { auto o = actor_base; o.SetSerialQueueCapacity(0);
      expect_invalid(o.Validate(), "serial_queue_capacity"); }
    /**
     * Concurrent 不得创建串行队列
     */
    { auto o = concurrent; o.SetMaxSerialKeys(1);
      expect_invalid(o.Validate(), "serial"); }
    { auto o = concurrent; o.SetSerialQueueCapacity(1);
      expect_invalid(o.Validate(), "serial"); }
    /**
     * ordered_ingress 用在 Concurrent 上必须被拒。
     */
    { auto o = concurrent;
      o.SetOrderedIngress(fw::OrderedIngressOptions{true, 1, 1, 1});
      expect_invalid(o.Validate(), "ordered_ingress"); }
    { auto o = ordered;
      o.SetOrderedIngress(fw::OrderedIngressOptions{true, 0, 32, 4096});
      expect_invalid(o.Validate(), "ordered_ingress"); }
    { auto o = ordered;
      o.SetOrderedIngress(fw::OrderedIngressOptions{true, 4, 0, 4096});
      expect_invalid(o.Validate(), "ordered_ingress"); }
    { auto o = ordered;
      o.SetOrderedIngress(fw::OrderedIngressOptions{true, 4, 32, 0});
      expect_invalid(o.Validate(), "ordered_ingress"); }
    /**
     * 禁用 ordered_ingress 时三项上限必须为 0。
     */
    { auto o = actor_base;
      o.SetOrderedIngress(fw::OrderedIngressOptions{false, 1, 0, 0});
      expect_invalid(o.Validate(), "ordered_ingress"); }
    { auto o = actor_base;
      o.SetOrderedIngress(fw::OrderedIngressOptions{false, 0, 1, 0});
      expect_invalid(o.Validate(), "ordered_ingress"); }
    { auto o = actor_base;
      o.SetOrderedIngress(fw::OrderedIngressOptions{false, 0, 0, 1});
      expect_invalid(o.Validate(), "ordered_ingress"); }
    /**
     * Actor 类型额度三项均为有限正值。
     */
    { auto o = actor_opts; o.SetMaxActors(0);
      expect_invalid(o.Validate(), "max_actors"); }
    { auto o = actor_opts; o.SetMailboxCapacity(0);
      expect_invalid(o.Validate(), "mailbox_capacity"); }
    { auto o = actor_opts; o.SetAggregateQueueCapacity(0);
      expect_invalid(o.Validate(), "aggregate_queue_capacity"); }
}

/**
 * ActorSerial 服务存在无 key 提取器的方法 → 启动期校验失败。
 */
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

/**
 * 保序：首项用屏障拦在 handler 内，保证后续项全部排队后仍按接纳顺序处理。
 */
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
    /**
     * 首项已进入 handler（执行中），此时再接纳的项必然全部落在等待队列。
     */
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

/**
 * 非重入：handler1 被屏障拦在执行中时再接纳 handler2；
 * 重入实现会让 handler2 在 handler1 阻塞期间进入（leaked=true 即失败）。
 */
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
        /**
         * 执行中阻塞：给 handler2 完整重叠窗口
         */
        release1.Wait();
        rec.Push(1);
        in_exec.fetch_sub(1);
        done.CountDown();
    }));
    /**
     * handler1 确认在执行中
     */
    BOOST_REQUIRE(entered1.WaitFor(kWait));

    BOOST_REQUIRE(mb->TryEnqueue([&] {
        note_entry();
        entered2.Open();
        rec.Push(2);
        in_exec.fetch_sub(1);
        done.CountDown();
    }));
    /**
     * 正确实现下 handler2 此刻只能排队，entered2 不可能提前打开；
     * 若实现重入（每条任务一个协程），200ms 内必然进入并打开 entered2。
     */
    const bool leaked = entered2.WaitFor(std::chrono::milliseconds(200));
    release1.Open();
    BOOST_REQUIRE(done.WaitFor(kWait));

    BOOST_TEST(!leaked);
    BOOST_TEST(max_exec.load() == 1);
    BOOST_TEST(rec.Snapshot() == std::vector<int>({1, 2}));
    /**
     * done 归零（handler 内 CountDown）早于 drain 协程回环清 m_draining；
     * 立即断言 IsDraining() 会撞上窗口，改为带超时等收敛。
     */
    BOOST_TEST(WaitUntil([&] { return !mb->IsDraining(); }, kWait));
}

/**
 * 容量：等待队列满后再接纳返回 Overloaded，被拒任务不得执行。
 */
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
    /**
     * 执行中项不占容量
     */
    BOOST_REQUIRE(entered.WaitFor(kWait));
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

/**
 * 异常路径：handler 抛异常后下一条仍被处理，且异常经 hook 可观测。
 */
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

    /**
     * 异常后邮箱仍可继续接纳。
     */
    BOOST_REQUIRE(mb->TryEnqueue([&] { rec.Push(2); done3.CountDown(); }));
    BOOST_REQUIRE(done3.WaitFor(kWait));
    BOOST_TEST(rec.Snapshot() == std::vector<int>({0, 1, 2}));
}

/**
 * 跨 Actor 并发：两个邮箱的 handler 互相等对方进入执行，
 * 两侧都等到才证明执行真实重叠，而非各自跑完。
 */
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

/**
 * Close 停止接纳；已接纳项仍被消费完。
 */
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

/**
 * 旧 ActorRegistry::GetOrCreate 工厂面（同 key 至多一次、max_actors 上限、
 * 激活失败不留半实例）在唯一 API 中已删除：CoInstallActor 只登记类型，
 * Actor 实例动态创建经后续 CoCreateActor 切片。被删能力在当前公开面上
 * 不存在可断言的实现——按「不把未实现能力改成成功」，原
 * registry_at_most_once_and_max_actors / registry_failed_activation_leaves_no_partial
 * 两个用例随旧面删除，不迁移为空断言；类型登记/查找语义由
 * framework.v2_core 的 actor_type_registration 覆盖。
 */

BOOST_AUTO_TEST_SUITE_END()

} // namespace
