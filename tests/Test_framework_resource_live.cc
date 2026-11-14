// 组件化 live 验收（真实后端）：framework 唯一受管能力路径
// `v2::RedisComponent` / `v2::MongoDBComponent`。
//
// 覆盖：
//  - Redis：真实连接池 Connect + PING/SET/GET 往返，命令面经
//    `ComponentHandle::Borrow` 在组件寿命内使用；
//  - Mongo：真实 owner Start + 集合句柄 InsertOne/FindOne 往返；
//  - 联合：同一 App 上 2 个 RedisComponent（互异池配置）+ 1 个
//    MongoDBComponent + 5 个 Service 的 3+2 绑定，全部经真实 Registry/Handle/
//    Borrow 消费；CoStop 后组件只关闭自己拥有的池/owner，共享 Engine 不被
//    组件或 App 关闭（Run 由等价入口 hook 在主任务结束后收口）。
//
// 环境变量（缺失 → 进程 exit(77)，由 CTest SKIP_RETURN_CODE 标 Skipped：
// 缺后端必须显式 skipped，不得记为 Passed）：
//   BBT_TEST_REDIS_ADDR=host:port
//   BBT_TEST_MONGO_URI=mongodb://h:p/?directConnection=true
//
// 三种构建形态由编译开关选择：`BBT_LIVE_HAS_REDIS` / `BBT_LIVE_HAS_MONGO`，
// 两者同时开启即联合验收。本文件是三个 live 目标的唯一源；测试进程的 native
// 线程扮演入口 host hook（真实入口里该 Run 由 bbtco_main_host_hook 执行）。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/infra/CloseOptions.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Engine.hpp>

#include <bbt/framework/V2.hpp>
#include <bbt/framework/MongoDBComponent.hpp>
#include <bbt/framework/RedisComponent.hpp>

namespace fw = bbt::framework;
namespace v2 = bbt::framework::v2;
namespace inf = bbt::infra;
namespace co = bbt::coroutine;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

namespace {

constexpr std::chrono::milliseconds kWait{60s};

const char* EnvOr(const char* key) {
    return std::getenv(key);
}

std::pair<std::string, std::uint16_t> SplitAddr(std::string_view text) {
    const auto pos = text.rfind(':');
    if (pos == std::string_view::npos)
        return {std::string(text), 0};
    const auto port_text = std::string(text.substr(pos + 1));
    const auto port = std::strtoul(port_text.c_str(), nullptr, 10);
    return {std::string(text.substr(0, pos)), static_cast<std::uint16_t>(port)};
}

// 缺环境在进程启动时显式 Skipped（退出码 77），不给不完整的假证据。
struct LiveEnvGuard {
    LiveEnvGuard() {
#if defined(BBT_LIVE_HAS_REDIS)
        if (EnvOr("BBT_TEST_REDIS_ADDR") == nullptr) {
            std::fputs("live: BBT_TEST_REDIS_ADDR 未设置，标记 Skipped\n",
                       stderr);
            std::exit(77);
        }
#endif
#if defined(BBT_LIVE_HAS_MONGO)
        if (EnvOr("BBT_TEST_MONGO_URI") == nullptr) {
            std::fputs("live: BBT_TEST_MONGO_URI 未设置，标记 Skipped\n",
                       stderr);
            std::exit(77);
        }
#endif
    }
} g_live_env;

void EnsureRuntime() {
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
        g_bbt_coroutine_config->m_cfg_stack_size = 128 * 1024;
        g_scheduler->Start();
    }
}

struct Latch {
    std::mutex mtx;
    std::condition_variable cv;
    bool done{false};
    void Settle() {
        {
            std::lock_guard<std::mutex> lk(mtx);
            done = true;
        }
        cv.notify_all();
    }
    bool Wait(std::chrono::milliseconds ms) {
        std::unique_lock<std::mutex> lk(mtx);
        return cv.wait_for(lk, ms, [this] { return done; });
    }
};

// 在受管协程内执行 fn；native 测试线程有界等待。返回 false 必须当失败。
bool RunInCoTask(std::function<void()> fn,
                 std::chrono::milliseconds timeout = kWait) {
    EnsureRuntime();
    auto box = std::make_shared<Latch>();
    bool registered = false;
    g_scheduler->RegistCoroutineTask(
        [box, fn = std::move(fn)] {
            try {
                fn();
            } catch (...) {
            }
            box->Settle();
        },
        registered);
    return registered && box->Wait(timeout);
}

// 等价入口 host hook：Engine 的 native 执行只由该线程调用一次 Run。
class EngineRun {
public:
    EngineRun() = default;
    ~EngineRun() { Stop(); }

    bool Start(const std::shared_ptr<inf::net::Engine>& engine,
               inf::net::EngineConfig config) {
        if (!engine)
            return false;
        m_engine = engine;
        m_thread = std::thread([engine, config] { (void)engine->Run(config); });
        return true;
    }

    // 主任务结束后收口：RequestClose → Run 返回（物理凭据 Phase()==Stopped）。
    void Stop() {
        if (m_engine)
            m_engine->RequestClose();
        if (m_thread.joinable())
            m_thread.join();
        m_engine.reset();
    }

private:
    std::shared_ptr<inf::net::Engine> m_engine;
    std::thread m_thread;
};

inf::net::EngineConfig SingleLoopConfig() {
    inf::net::EngineConfig config;
    config.runtime_progress = inf::net::RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    return config;
}

// 真实命令往返的观察面：Service 在 OnStart 内经 Borrow 执行真实命令，结果由
// 这里记录（不持 Service 指针，避免与收口竞争）。
struct LiveProbe {
    std::mutex mtx;
    int ping_ok{0};
    int set_ok{0};
    int get_ok{0};
    int mongo_insert_ok{0};
    int mongo_find_ok{0};
    std::vector<std::string> errors;

    void Fail(std::string what) {
        std::lock_guard<std::mutex> lk(mtx);
        errors.push_back(std::move(what));
    }
    void Ping() {
        std::lock_guard<std::mutex> lk(mtx);
        ++ping_ok;
    }
    void Set() {
        std::lock_guard<std::mutex> lk(mtx);
        ++set_ok;
    }
    void Get() {
        std::lock_guard<std::mutex> lk(mtx);
        ++get_ok;
    }
    void MongoInsert() {
        std::lock_guard<std::mutex> lk(mtx);
        ++mongo_insert_ok;
    }
    void MongoFind() {
        std::lock_guard<std::mutex> lk(mtx);
        ++mongo_find_ok;
    }
    int Errors() {
        std::lock_guard<std::mutex> lk(mtx);
        return static_cast<int>(errors.size());
    }
    std::string FirstError() {
        std::lock_guard<std::mutex> lk(mtx);
        return errors.empty() ? std::string{} : errors.front();
    }
};

std::shared_ptr<LiveProbe> g_probe;

fw::AppOptions AppOpts(std::chrono::milliseconds lifecycle) {
    fw::AppOptions opts;
    opts.SetLimits(64, 64, 8, 16)
        .SetShutdownBudget(2000ms)
        .SetLifecycleBudget(lifecycle);
    return opts;
}

} // namespace

#if defined(BBT_LIVE_HAS_REDIS)

// Redis 组件 live：真实连接池命令往返；组件只关闭自己的池，不关闭共享 Engine。
class LiveRedisSvc final : public v2::CoService<v2::RedisComponent> {
public:
    static constexpr std::string_view kServiceName = "live.redis";
    explicit LiveRedisSvc(fw::detail::ConstructKey k)
        : v2::CoService<v2::RedisComponent>(k) {}

protected:
    ~LiveRedisSvc() noexcept override = default;
    fw::result<void> OnStart() override {
        auto handle = this->template GetComponent<v2::RedisComponent>("cache");
        if (!handle)
            return fw::result<void>::err(handle.error());
        auto borrow = handle.value().Borrow();
        if (!borrow)
            return fw::result<void>::err(borrow.error());
        auto& pool = borrow.value().Get().Pool();
        inf::CallOptions options;
        options.deadline = Clock::now() + 5s;
        auto pong = bbt_co_await pool.Ping(options);
        if (!pong) {
            g_probe->Fail("live.redis ping: " + pong.error().message);
            return fw::result<void>::err(pong.error());
        }
        g_probe->Ping();
        const std::string key = "live:redis:v1";
        auto written = bbt_co_await pool.Set(key, "v1", options);
        if (!written) {
            g_probe->Fail("live.redis set: " + written.error().message);
            return fw::result<void>::err(written.error());
        }
        g_probe->Set();
        auto read = bbt_co_await pool.Get(key, options);
        if (!read) {
            g_probe->Fail("live.redis get: " + read.error().message);
            return fw::result<void>::err(read.error());
        }
        if (!read.value().has_value() || *read.value() != "v1") {
            g_probe->Fail("live.redis get: readback mismatch");
            return fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::ProtocolError, "redis live readback mismatch"));
        }
        g_probe->Get();
        return fw::result<void>::ok();
    }
    friend class fw::detail::Registry;
};

BOOST_AUTO_TEST_CASE(redis_component_live_command_roundtrip) {
    EnsureRuntime();
    g_probe = std::make_shared<LiveProbe>();

    const auto [rhost, rport] = SplitAddr(EnvOr("BBT_TEST_REDIS_ADDR"));
    BOOST_REQUIRE_MESSAGE(rport != 0, "BBT_TEST_REDIS_ADDR 需为 host:port");
    std::error_code address_ec;
    auto address = inf::net::Address::Ip(rhost, rport, address_ec);
    BOOST_REQUIRE_MESSAGE(!address_ec,
                          "bad redis address: " << address_ec.message());

    std::shared_ptr<v2::CoApp> app;
    std::shared_ptr<inf::net::Engine> engine;
    EngineRun runner;
    bool installed = false;
    bool started = false;
    BOOST_REQUIRE(RunInCoTask([&] {
        app = v2::CoApp::Create(AppOpts(5000ms), "live-redis");
        engine = inf::net::Engine::Create(nullptr);
        runner.Start(engine, SingleLoopConfig());
        co::WaitOptions wait;
        wait.deadline = Clock::now() + 10s;
        if (!static_cast<bool>(engine->CoWaitRunning(wait)))
            return;
        v2::RedisComponent::Options options;
        options.engine = engine;
        options.address = address;
        options.pool.connections = 1;
        options.pool.max_waiters = 0;
        options.connect_timeout = 5000ms;
        installed = static_cast<bool>(
            bbt_co_await app->CoInstallComponent<v2::RedisComponent>(
                "cache", options));
        if (!installed)
            return;
        fw::ServiceOptions svc;
        svc.SetMaxInflight(8);
        svc.BindComponent<v2::RedisComponent>("cache");
        started = static_cast<bool>(
                      bbt_co_await app->CoInstallService<LiveRedisSvc>(svc)) &&
                  static_cast<bool>(bbt_co_await app->CoStart());
    }));
    BOOST_REQUIRE_MESSAGE(installed, "install redis component failed");
    BOOST_REQUIRE_MESSAGE(started, "app start failed");
    BOOST_TEST_MESSAGE("redis live probe error: " << g_probe->FirstError());
    BOOST_CHECK_EQUAL(g_probe->Errors(), 0);
    BOOST_CHECK_EQUAL(g_probe->ping_ok, 1);
    BOOST_CHECK_EQUAL(g_probe->set_ok, 1);
    BOOST_CHECK_EQUAL(g_probe->get_ok, 1);

    bool stopped = false;
    BOOST_REQUIRE(RunInCoTask([&] {
        stopped = static_cast<bool>(bbt_co_await app->CoStop(
            inf::CloseOptions::Graceful(Clock::now() + 5s)));
    }));
    BOOST_REQUIRE(stopped);
    // 组件关闭后共享 Engine 仍归应用根：Run 尚未收口。
    BOOST_CHECK(engine->Phase() == inf::net::EnginePhase::Running);
    runner.Stop();
    BOOST_CHECK(engine->Phase() == inf::net::EnginePhase::Stopped);
    app.reset();
    engine.reset();
}

#endif // BBT_LIVE_HAS_REDIS

#if defined(BBT_LIVE_HAS_MONGO)

class LiveMongoSvc final : public v2::CoService<v2::MongoDBComponent> {
public:
    static constexpr std::string_view kServiceName = "live.mongo";
    explicit LiveMongoSvc(fw::detail::ConstructKey k)
        : v2::CoService<v2::MongoDBComponent>(k) {}

protected:
    ~LiveMongoSvc() noexcept override = default;
    fw::result<void> OnStart() override {
        auto handle = this->template GetComponent<v2::MongoDBComponent>("docs");
        if (!handle)
            return fw::result<void>::err(handle.error());
        auto borrow = handle.value().Borrow();
        if (!borrow)
            return fw::result<void>::err(borrow.error());
        auto& coll = borrow.value().Get().Collection();
        inf::CallOptions options;
        options.deadline = Clock::now() + 10s;
        inf::mongo::MongoDocument doc;
        doc.bytes = LiveDoc();
        auto inserted = bbt_co_await coll.InsertOne(doc, options);
        if (!inserted) {
            g_probe->Fail("live.mongo insert: " + inserted.error().message);
            return fw::result<void>::err(inserted.error());
        }
        g_probe->MongoInsert();
        auto found = bbt_co_await coll.FindOne(doc, options);
        if (!found) {
            g_probe->Fail("live.mongo find: " + found.error().message);
            return fw::result<void>::err(found.error());
        }
        if (!found.value().has_value()) {
            g_probe->Fail("live.mongo find: document missing");
            return fw::result<void>::err(fw::MakeError(
                fw::ErrorCode::NotFound, "mongo live document missing"));
        }
        g_probe->MongoFind();
        return fw::result<void>::ok();
    }

public:
    // 最小 BSON：{ _id: "live-1", v: "1" }。
    static std::vector<std::uint8_t> LiveDoc() {
        std::vector<std::uint8_t> body;
        const auto put_i32 = [](std::vector<std::uint8_t>& out, std::int32_t v) {
            for (int i = 0; i < 4; ++i)
                out.push_back(static_cast<std::uint8_t>((v >> (8 * i)) & 0xFF));
        };
        const auto put_str = [&put_i32](std::vector<std::uint8_t>& out,
                                        std::string_view k,
                                        std::string_view v) {
            out.push_back(0x02); // string
            out.insert(out.end(), k.begin(), k.end());
            out.push_back(0x00);
            put_i32(out, static_cast<std::int32_t>(v.size() + 1));
            out.insert(out.end(), v.begin(), v.end());
            out.push_back(0x00);
        };
        put_str(body, "_id", "live-1");
        put_str(body, "v", "1");
        std::vector<std::uint8_t> doc;
        put_i32(doc, static_cast<std::int32_t>(body.size() + 5));
        doc.insert(doc.end(), body.begin(), body.end());
        doc.push_back(0x00);
        return doc;
    }

private:
    friend class fw::detail::Registry;
};

BOOST_AUTO_TEST_CASE(mongo_component_live_command_roundtrip) {
    EnsureRuntime();
    g_probe = std::make_shared<LiveProbe>();

    std::shared_ptr<v2::CoApp> app;
    bool installed = false;
    bool started = false;
    BOOST_REQUIRE(RunInCoTask([&] {
        app = v2::CoApp::Create(AppOpts(20000ms), "live-mongo");
        v2::MongoDBComponent::Options options;
        options.config.uri = EnvOr("BBT_TEST_MONGO_URI");
        options.config.worker_threads = 1;
        options.config.max_queue = 8;
        options.config.server_selection_timeout = 5000ms;
        options.config.connect_timeout = 5000ms;
        options.config.socket_timeout = 5000ms;
        options.config.wait_queue_timeout = 5000ms;
        options.target.database = "bbt_framework_live";
        options.target.collection = "component";
        installed = static_cast<bool>(
            bbt_co_await app->CoInstallComponent<v2::MongoDBComponent>(
                "docs", options));
        if (!installed)
            return;
        fw::ServiceOptions svc;
        svc.SetMaxInflight(8);
        svc.BindComponent<v2::MongoDBComponent>("docs");
        started = static_cast<bool>(
                      bbt_co_await app->CoInstallService<LiveMongoSvc>(svc)) &&
                  static_cast<bool>(bbt_co_await app->CoStart());
    }));
    BOOST_REQUIRE_MESSAGE(installed, "install mongo component failed");
    BOOST_REQUIRE_MESSAGE(started, "app start failed");
    BOOST_TEST_MESSAGE("mongo live probe error: " << g_probe->FirstError());
    BOOST_CHECK_EQUAL(g_probe->Errors(), 0);
    BOOST_CHECK_EQUAL(g_probe->mongo_insert_ok, 1);
    BOOST_CHECK_EQUAL(g_probe->mongo_find_ok, 1);

    bool stopped = false;
    BOOST_REQUIRE(RunInCoTask([&] {
        stopped = static_cast<bool>(bbt_co_await app->CoStop(
            inf::CloseOptions::Graceful(Clock::now() + 10s)));
    }));
    BOOST_REQUIRE(stopped);
    app.reset();
}

#endif // BBT_LIVE_HAS_MONGO

#if defined(BBT_LIVE_HAS_REDIS) && defined(BBT_LIVE_HAS_MONGO)

#define DEFINE_LIVE_REDIS_SVC(CLASS, NAME, COMP)                        \
    class CLASS final : public v2::CoService<v2::RedisComponent> {      \
    public:                                                             \
        static constexpr std::string_view kServiceName = NAME;          \
        explicit CLASS(fw::detail::ConstructKey k)                      \
            : v2::CoService<v2::RedisComponent>(k) {}                   \
                                                                        \
    protected:                                                          \
        ~CLASS() noexcept override = default;                           \
        fw::result<void> OnStart() override {                           \
            auto handle =                                               \
                this->template GetComponent<v2::RedisComponent>(COMP);  \
            if (!handle)                                                \
                return fw::result<void>::err(handle.error());           \
            auto borrow = handle.value().Borrow();                      \
            if (!borrow)                                                \
                return fw::result<void>::err(borrow.error());           \
            inf::CallOptions options;                                   \
            options.deadline = Clock::now() + 5s;                       \
            auto pong =                                                 \
                bbt_co_await borrow.value().Get().Pool().Ping(options); \
            if (!pong) {                                                \
                g_probe->Fail(std::string(NAME) + ": " +                \
                              pong.error().message);                    \
                return fw::result<void>::err(pong.error());             \
            }                                                           \
            g_probe->Ping();                                            \
            return fw::result<void>::ok();                              \
        }                                                               \
        friend class fw::detail::Registry;                              \
    }

DEFINE_LIVE_REDIS_SVC(LiveSvcA, "live.svc-a", "redis-a");
DEFINE_LIVE_REDIS_SVC(LiveSvcB, "live.svc-b", "redis-a");
DEFINE_LIVE_REDIS_SVC(LiveSvcC, "live.svc-c", "redis-a");
DEFINE_LIVE_REDIS_SVC(LiveSvcD, "live.svc-d", "redis-b");
DEFINE_LIVE_REDIS_SVC(LiveSvcE, "live.svc-e", "redis-b");

#undef DEFINE_LIVE_REDIS_SVC

fw::ServiceOptions BindRedis(std::string_view component) {
    fw::ServiceOptions svc;
    svc.SetMaxInflight(8);
    if (component == "redis-a")
        svc.BindComponent<v2::RedisComponent>("redis-a");
    else
        svc.BindComponent<v2::RedisComponent>("redis-b");
    return svc;
}

// 联合 live：同一 App 上 2 个 RedisComponent（互异池配置）+ 1 个
// MongoDBComponent + 5 个 Service 的 3+2 绑定 + 1 个 Mongo 消费 Service；
// 每个 Service 在 OnStart 里经自己的 ComponentHandle::Borrow 执行真实命令。
BOOST_AUTO_TEST_CASE(shared_engine_two_redis_five_services_live) {
    EnsureRuntime();
    g_probe = std::make_shared<LiveProbe>();
    const auto [rhost, rport] = SplitAddr(EnvOr("BBT_TEST_REDIS_ADDR"));
    BOOST_REQUIRE_MESSAGE(rport != 0, "BBT_TEST_REDIS_ADDR 需为 host:port");
    std::error_code address_ec;
    auto address = inf::net::Address::Ip(rhost, rport, address_ec);
    BOOST_REQUIRE_MESSAGE(!address_ec,
                          "bad redis address: " << address_ec.message());

    std::shared_ptr<v2::CoApp> app;
    std::shared_ptr<inf::net::Engine> engine;
    EngineRun runner;
    bool ok = false;
    BOOST_REQUIRE(RunInCoTask([&] {
        app = v2::CoApp::Create(AppOpts(20000ms), "live-combined");
        engine = inf::net::Engine::Create(nullptr);
        runner.Start(engine, SingleLoopConfig());
        co::WaitOptions engine_wait;
        engine_wait.deadline = Clock::now() + 10s;
        if (!static_cast<bool>(engine->CoWaitRunning(engine_wait)))
            return;
        const auto install_redis = [&](const char* name, std::size_t conns) {
            v2::RedisComponent::Options options;
            options.engine = engine;
            options.address = address;
            options.pool.connections = conns;
            options.pool.max_waiters = 0;
            options.connect_timeout = 5000ms;
            return static_cast<bool>(
                bbt_co_await app->CoInstallComponent<v2::RedisComponent>(
                    name, options));
        };
        if (!install_redis("redis-a", 2) || !install_redis("redis-b", 3))
            return;
        v2::MongoDBComponent::Options mongo;
        mongo.config.uri = EnvOr("BBT_TEST_MONGO_URI");
        mongo.config.worker_threads = 1;
        mongo.config.max_queue = 8;
        mongo.config.server_selection_timeout = 5000ms;
        mongo.config.connect_timeout = 5000ms;
        mongo.config.socket_timeout = 5000ms;
        mongo.config.wait_queue_timeout = 5000ms;
        mongo.target.database = "bbt_framework_live";
        mongo.target.collection = "shared";
        if (!bbt_co_await app->CoInstallComponent<v2::MongoDBComponent>(
                "docs", mongo))
            return;
        fw::ServiceOptions mongo_svc;
        mongo_svc.SetMaxInflight(8);
        mongo_svc.BindComponent<v2::MongoDBComponent>("docs");
        if (!bbt_co_await app->CoInstallService<LiveMongoSvc>(mongo_svc))
            return;
        if (!bbt_co_await app->CoInstallService<LiveSvcA>(BindRedis("redis-a")) ||
            !bbt_co_await app->CoInstallService<LiveSvcB>(BindRedis("redis-a")) ||
            !bbt_co_await app->CoInstallService<LiveSvcC>(BindRedis("redis-a")) ||
            !bbt_co_await app->CoInstallService<LiveSvcD>(BindRedis("redis-b")) ||
            !bbt_co_await app->CoInstallService<LiveSvcE>(BindRedis("redis-b")))
            return;
        ok = static_cast<bool>(bbt_co_await app->CoStart());
    }));
    BOOST_REQUIRE_MESSAGE(ok, "combined start failed");

    // 身份与配置隔离：两个 Redis 组件是不同对象，池参数各自生效。
    auto a = app->FindComponent<v2::RedisComponent>("redis-a");
    auto b = app->FindComponent<v2::RedisComponent>("redis-b");
    auto docs = app->FindComponent<v2::MongoDBComponent>("docs");
    BOOST_REQUIRE(a);
    BOOST_REQUIRE(b);
    BOOST_REQUIRE(docs);
    BOOST_CHECK_NE(a.value().Id(), b.value().Id());
    BOOST_CHECK(a.value().IsValid());
    BOOST_CHECK(b.value().IsValid());
    BOOST_CHECK(docs.value().IsValid());

    BOOST_TEST_MESSAGE("combined live probe error: " << g_probe->FirstError());
    BOOST_CHECK_EQUAL(g_probe->Errors(), 0);
    // 5 个 3+2 Service 各做一次真实 PING，加 Redis 单例探针（同一 App 内的
    // Mongo 服务只做 Mongo）：每个绑定都存在且命令成功。
    BOOST_CHECK_EQUAL(g_probe->ping_ok, 5);
    BOOST_CHECK_EQUAL(g_probe->mongo_insert_ok, 1);
    BOOST_CHECK_EQUAL(g_probe->mongo_find_ok, 1);

    bool stopped = false;
    BOOST_REQUIRE(RunInCoTask([&] {
        stopped = static_cast<bool>(bbt_co_await app->CoStop(
            inf::CloseOptions::Graceful(Clock::now() + 10s)));
    }));
    BOOST_REQUIRE(stopped);
    BOOST_CHECK(engine->Phase() == inf::net::EnginePhase::Running);
    runner.Stop();
    app.reset();
    engine.reset();
}

#endif // BBT_LIVE_HAS_REDIS && BBT_LIVE_HAS_MONGO
