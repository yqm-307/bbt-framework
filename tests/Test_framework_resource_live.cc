// issue #8 资源生命周期 live 验收：真实 Redis/MongoDB 容器经
// add_resource<R>(name, factory) 装配进 CoApp，service handler 在受管
// 请求上下文（协程 + InboundDispatcher 注入的 RequestScope）内经
// context().resource<R>(name) 消费真实命令往返；request_shutdown 后
// 关闭序列对资源 RequestClose/WaitClosed 到 IsClosed 终态。
//
// 环境变量驱动（与 infra live 同一约定）：
//   BBT_TEST_REDIS_ADDR=host:port         缺 → redis 用例 skip
//   BBT_TEST_MONGO_URI=mongodb://h:p/     缺 → mongo 用例 skip
// 不伪造：无真实服务时不构造桩替代，跳过由环境决定。
//
// 入站驱动经 DispatchInboundForTest（CoApp::dispatch_inbound 的测试
// 转发口）：envelope → InboundDispatcher → RequestScope → handler →
// 资源缝 → reply 全链，与真实线桥入站同源；不经 HTTP 栈也不绕过
// Dispatcher 直接触 handler。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/coroutine/sync/Cancellation.hpp>
#include <bbt/coroutine/sync/CompletionSignal.hpp>

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/NetworkTypes.hpp>

#ifdef BBT_LIVE_HAS_REDIS
#include <bbt/infra/CoRedisCli.hpp>
#endif
#ifdef BBT_LIVE_HAS_MONGO
#include <bbt/infra/CoMongoCli.hpp>
#endif

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/HostLifecycle.hpp>

namespace fw  = bbt::framework;
namespace co  = bbt::coroutine;
namespace inf = bbt::infra;
using Clock = std::chrono::steady_clock;

namespace {

constexpr std::chrono::milliseconds kWait{15000};

const char* EnvOr(const char* k) { return std::getenv(k); }

#ifdef BBT_LIVE_HAS_REDIS
bool RedisEnabled() {
    const char* a = EnvOr("BBT_TEST_REDIS_ADDR");
    return a != nullptr && *a != '\0';
}
#endif
#ifdef BBT_LIVE_HAS_MONGO
bool MongoEnabled() {
    const char* u = EnvOr("BBT_TEST_MONGO_URI");
    return u != nullptr && *u != '\0';
}
#endif

// 缺环境必须显式标记 Skipped（CTest SKIP_RETURN_CODE 77），不能以退出码 0
// 冒充通过——与 infra live 同一约定：进程退出码 77 即标记，否则就算
// case 内 return 也会被 CTest 记为 Passed。
struct RequireLiveEnv {
    RequireLiveEnv() {
#if defined(BBT_LIVE_HAS_REDIS)
        if (!RedisEnabled()) {
            std::fputs("live.redis: BBT_TEST_REDIS_ADDR 未设置，"
                       "标记 Skipped\n", stderr);
            std::exit(77);
        }
#elif defined(BBT_LIVE_HAS_MONGO)
        if (!MongoEnabled()) {
            std::fputs("live.mongo: BBT_TEST_MONGO_URI 未设置，"
                       "标记 Skipped\n", stderr);
            std::exit(77);
        }
#else
        // 编译期兜底：本 target 必须至少带一个模块宏，否则无意义。
        std::fputs("live: 未启用 BBT_LIVE_HAS_* 宏，target 配置错误\n",
                   stderr);
        std::exit(1);
#endif
    }
};
BOOST_GLOBAL_FIXTURE(RequireLiveEnv);

#ifdef BBT_LIVE_HAS_REDIS
// "host:port" → (host, port)
std::pair<std::string, std::uint16_t> SplitAddr(const std::string& addr) {
    const auto pos = addr.rfind(':');
    if (pos == std::string::npos) return {addr, 0};
    return {addr.substr(0, pos),
            static_cast<std::uint16_t>(std::stoi(addr.substr(pos + 1)))};
}
#endif // BBT_LIVE_HAS_REDIS

class TestLatch {
public:
    explicit TestLatch(int n) : m_count(n) {}
    void CountDown() {
        { std::lock_guard<std::mutex> lk(m_mtx); if (--m_count > 0) return; }
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

// ── INetworkHost 桩：live 用例不经真实 socket；宿主只提供生命周期骨架，
//    入站由 dispatch_inbound 直接驱动。──
class NullNetHost final : public fw::INetworkHost {
public:
    TestLatch start_seen{1};
    fw::result<void> Create() override { return fw::result<void>::ok(); }
    fw::result<void> Start() override {
        start_seen.CountDown(); return fw::result<void>::ok(); }
    void StopAccepting() noexcept override {}
    fw::result<void> WaitHandlersDone(co::Deadline,
                                      co::CancellationToken) override {
        return fw::result<void>::ok();
    }
    void RequestClose() noexcept override {}
    inf::CloseStatus WaitClosed(co::Deadline,
                                co::CancellationToken) override {
        return inf::CloseStatus::Closed;
    }
    void ReleaseClosed() noexcept override {}
};

// ── BSON 裸字节编码（MongoDocument 是 owning bytes；只编码本测试用的
//    最小文档形态：string 与 int32 元素）。仅 BBT_LIVE_HAS_MONGO 编译。──
#ifdef BBT_LIVE_HAS_MONGO
void BsonPutI32(std::vector<std::uint8_t>& b, std::int32_t v) {
    b.push_back(static_cast<std::uint8_t>(v & 0xFF));
    b.push_back(static_cast<std::uint8_t>((v >> 8) & 0xFF));
    b.push_back(static_cast<std::uint8_t>((v >> 16) & 0xFF));
    b.push_back(static_cast<std::uint8_t>((v >> 24) & 0xFF));
}
void BsonPutCStr(std::vector<std::uint8_t>& b, std::string_view s) {
    for (char c : s) b.push_back(static_cast<std::uint8_t>(c));
    b.push_back(0x00);
}
void BsonElemString(std::vector<std::uint8_t>& b,
                    std::string_view key, std::string_view val) {
    b.push_back(0x02);                       // type: string
    BsonPutCStr(b, key);
    BsonPutI32(b, static_cast<std::int32_t>(val.size() + 1));
    BsonPutCStr(b, val);
}
void BsonElemI32(std::vector<std::uint8_t>& b,
                 std::string_view key, std::int32_t v) {
    b.push_back(0x10);                       // type: int32
    BsonPutCStr(b, key);
    BsonPutI32(b, v);
}
inf::MongoDocument BsonDoc(
    const std::vector<std::pair<std::string_view,
        std::pair<bool, std::pair<std::string_view, std::int32_t>>>>& elems) {
    // elems: (key, (is_string, (sval, ival)))
    std::vector<std::uint8_t> body;
    for (const auto& [k, spec] : elems) {
        if (spec.first) BsonElemString(body, k, spec.second.first);
        else            BsonElemI32(body, k, spec.second.second);
    }
    inf::MongoDocument doc;
    const std::int32_t total =
        static_cast<std::int32_t>(body.size() + 5);   // len + elems + 0x00
    doc.bytes.reserve(static_cast<std::size_t>(total));
    BsonPutI32(doc.bytes, total);
    doc.bytes.insert(doc.bytes.end(), body.begin(), body.end());
    doc.bytes.push_back(0x00);
    return doc;
}
inf::MongoDocument DocIdVal(std::string_view id, std::int32_t v) {
    return BsonDoc({{"_id", {true,  {id, 0}}},
                    {"v",   {false, {"", v}}}});
}
inf::MongoDocument DocId(std::string_view id) {
    return BsonDoc({{"_id", {true, {id, 0}}}});
}
#endif // BBT_LIVE_HAS_MONGO

// ── CoRpc 打包/拆包（positional schema）。──
fw::CoRpcReq RequestStr(std::string a, std::string b) {
    auto req = fw::CoRpcReq::From(std::tuple{std::move(a), std::move(b)});
    if (!req) throw std::logic_error("request encode failed");
    return std::move(req).value();
}
fw::CoRpcReq RequestOne(std::string a) {
    auto req = fw::CoRpcReq::From(std::move(a));
    if (!req) throw std::logic_error("request encode failed");
    return std::move(req).value();
}

// 解码 handler 回包：T=int32 状态码 + string 值。
fw::result<std::tuple<std::int32_t, std::string>> DecodeStrReply(
    const fw::CoRpcResp& r) {
    if (!r.ok())
        return fw::result<std::tuple<std::int32_t, std::string>>::err(
            r.error());
    auto d = fw::CoRpcReq(r.payload()).Parse<std::int32_t, std::string>();
    if (!d)
        return fw::result<std::tuple<std::int32_t, std::string>>::err(
            d.error());
    return fw::result<std::tuple<std::int32_t, std::string>>::ok(
        d.value());
}

#ifdef BBT_LIVE_HAS_REDIS
// 资源消费服务：handler 在受管上下文内经资源缝拿到 CoRedisCli 做
// Set/Get/Ping；结果打包 (status, value) 回复。
class RedisSvc final : public fw::CoService<RedisSvc> {
public:
    static constexpr std::string_view kServiceName = "cache";

    fw::CoRpcResp Set(fw::CoRpcReq req) {
        auto args = req.Parse<std::string, std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        auto cli = context().resource<inf::CoRedisCli>("cache");
        if (!cli)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::Unavailable, "resource 'cache' missing"));
        inf::CallOptions o;
        o.deadline = co::Deadline::max();
        auto r = cli->Set(std::get<0>(args.value()),
                          std::get<1>(args.value()), o);
        if (!r) return fw::CoRpcResp::Error(r.error());
        return fw::CoRpcResp::From(std::tuple{0, std::string{"OK"}});
    }

    fw::CoRpcResp Get(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        auto cli = context().resource<inf::CoRedisCli>("cache");
        if (!cli)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::Unavailable, "resource 'cache' missing"));
        inf::CallOptions o;
        o.deadline = co::Deadline::max();
        auto r = cli->Get(args.value(), o);
        if (!r) return fw::CoRpcResp::Error(r.error());
        return fw::CoRpcResp::From(std::tuple{
            0, r.value().value_or(std::string{"<nil>"})});
    }

    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        auto cli = context().resource<inf::CoRedisCli>("cache");
        if (!cli)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::Unavailable, "resource 'cache' missing"));
        inf::CallOptions o;
        o.deadline = co::Deadline::max();
        auto r = cli->Ping(o);
        if (!r) return fw::CoRpcResp::Error(r.error());
        return fw::CoRpcResp::From(std::tuple{0, std::string{"PONG"}});
    }

    // 资源缝观察口（测试断言 IsClosed 终态用）。
    std::shared_ptr<inf::CoRedisCli> Cache() {
        return context().resource<inf::CoRedisCli>("cache");
    }

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&RedisSvc::Set>("set"),
        fw::Method<&RedisSvc::Get>("get"),
        fw::Method<&RedisSvc::Ping>("ping"));
};
#endif

#ifdef BBT_LIVE_HAS_MONGO
class DocSvc final : public fw::CoService<DocSvc> {
public:
    static constexpr std::string_view kServiceName = "docs";

    fw::CoRpcResp Put(fw::CoRpcReq req) {
        auto args = req.Parse<std::string, std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        auto cli = context().resource<inf::CoMongoCli>("docs");
        if (!cli)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::Unavailable, "resource 'docs' missing"));
        inf::CallOptions o;
        o.deadline = co::Deadline::max();
        const auto& [id, sval] = args.value();
        // 值以十进制 int32 编入文档，便于断言。
        const std::int32_t v = static_cast<std::int32_t>(
            std::stoi(sval));
        auto doc = DocIdVal(id, v);
        auto r = cli->InsertOne(doc, o);
        if (!r) return fw::CoRpcResp::Error(r.error());
        return fw::CoRpcResp::From(std::tuple{0, std::string{"INS"}});
    }

    fw::CoRpcResp Get(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        auto cli = context().resource<inf::CoMongoCli>("docs");
        if (!cli)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::Unavailable, "resource 'docs' missing"));
        inf::CallOptions o;
        o.deadline = co::Deadline::max();
        auto r = cli->FindOne(DocId(args.value()), o);
        if (!r) return fw::CoRpcResp::Error(r.error());
        return fw::CoRpcResp::From(std::tuple{
            0, r.value().has_value() ? std::string{"HIT"}
                                     : std::string{"MISS"}});
    }

    std::shared_ptr<inf::CoMongoCli> Docs() {
        return context().resource<inf::CoMongoCli>("docs");
    }

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&DocSvc::Put>("put"),
        fw::Method<&DocSvc::Get>("get"));
};
#endif

fw::ServiceOptions ConcurrentOpts() {
    return fw::ServiceOptions{
        fw::ExecutionPolicy::Concurrent, 64, 0, 0, false, 0, 0, 0};
}

fw::CoAppOptions AppOpts() {
    return fw::CoAppOptions{
        inf::NetworkLimits{64, 64, 8192, 65536,
                           std::chrono::milliseconds{30000}},
        inf::ListenAddress{"127.0.0.1", 0},
        {},
        std::chrono::milliseconds{3000}};
}

std::unique_ptr<fw::CoApp> MakeApp(const std::shared_ptr<NullNetHost>& host) {
    return fw::MakeCoAppForTest(AppOpts(), fw::CoAppSeam{host, {}});
}

struct RunHandle {
    std::thread       th;
    std::atomic<bool> done{false};
    int               rc{-1};
};
std::unique_ptr<RunHandle> RunApp(fw::CoApp& app) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    auto h = std::make_unique<RunHandle>();
    RunHandle* p = h.get();
    h->th = std::thread([p, &app] {
        p->rc = app.run();
        p->done.store(true, std::memory_order_release);
    });
    return h;
}
void JoinRun(const std::unique_ptr<RunHandle>& h) {
    if (h->th.joinable()) h->th.join();
}

// 经 DispatchInboundForTest 在受管协程内分发：envelope → InboundDispatcher
// → RequestScope → handler → 资源缝 → reply，与真实线桥入站同源。
// Dispatch 内的 handler 调用要求协程上下文（CompletionSignal::Wait 仅
// 协程内合法），故本条路径必须经 g_scheduler->RegistCoroutineTask。
template <class Fn>
auto DispatchRpc(fw::CoApp& app, Fn&& fn,
                 std::chrono::milliseconds budget = kWait)
    -> std::optional<decltype(fn(std::declval<fw::CoApp&>()))> {
    using Out = decltype(fn(std::declval<fw::CoApp&>()));
    std::optional<Out> out;
    TestLatch done{1};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&app, &fn, &out, &done] {
            out.emplace(fn(app));
            done.CountDown();
        },
        succ);
    if (!succ || !done.WaitFor(budget)) return std::nullopt;
    return out;
}

BOOST_AUTO_TEST_SUITE(framework_resource_live)

#ifdef BBT_LIVE_HAS_REDIS
// L-R1：redis 资源工厂装配 → service handler 内经资源缝做真实
// Set/Get/Ping 往返 → request_shutdown 后资源 IsClosed 终态。
// 入站经 DispatchInboundForTest（InboundDispatcher+RequestScope 真实
// 链路），不绕过 Dispatcher 直接调 handler。
BOOST_AUTO_TEST_CASE(live_redis_resource_lifecycle) {
    // 全局 fixture 已保证 BBT_TEST_REDIS_ADDR 存在；缺环境进程已 exit(77)。
    const auto [rhost, rport] = SplitAddr(EnvOr("BBT_TEST_REDIS_ADDR"));
    auto host = std::make_shared<NullNetHost>();
    auto app  = MakeApp(host);
    BOOST_REQUIRE(app->add_service<RedisSvc>(ConcurrentOpts()));

    inf::RedisClientConfig cfg;
    cfg.host         = rhost;
    cfg.port         = rport;
    cfg.max_inflight = 256;
    cfg.max_queue    = 256;
    // 工厂形态装配：Create 要求 Scheduler 已启动，本契约由框架在
    // on_scheduler_started 相位保证。
    BOOST_REQUIRE(app->add_resource<inf::CoRedisCli>("cache",
        [cfg] { return inf::CoRedisCli::Create(cfg); }));

    auto h = RunApp(*app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

    // 真实往返：经 DispatchInboundForTest（envelope → Dispatcher →
    // RequestScope → handler → 资源缝 → reply）驱动 Ping/Set/Get。
    auto rpc = [&app](const char* method, fw::CoRpcReq req)
        -> std::optional<fw::result<inf::RpcEnvelope>> {
        inf::RpcEnvelope env;
        env.service         = "cache";
        env.method          = method;
        env.request_id      = std::string{"r-"} + method;
        env.request_schema  = std::string(fw::kCoRpcPositionalSchema);
        env.response_schema = std::string(fw::kCoRpcPositionalSchema);
        env.payload         = req.payload();
        return DispatchRpc(*app, [&env](fw::CoApp& a) {
            inf::IncomingCallContext incoming;
            incoming.deadline = Clock::now() + kWait;
            return fw::DispatchInboundForTest(a, incoming, env);
        });
    };

    auto ping = rpc("ping", RequestOne("x"));
    BOOST_REQUIRE(ping.has_value());
    BOOST_REQUIRE(*ping);
    auto p = DecodeStrReply(fw::CoRpcResp::FromPayload((*ping).value().payload));
    BOOST_REQUIRE(p);
    BOOST_CHECK_EQUAL(std::get<1>(p.value()), "PONG");

    auto set = rpc("set", RequestStr("bbt:f8:k", "v-42"));
    BOOST_REQUIRE(set.has_value());
    BOOST_REQUIRE(*set);
    auto s = DecodeStrReply(fw::CoRpcResp::FromPayload((*set).value().payload));
    BOOST_REQUIRE(s);
    BOOST_CHECK_EQUAL(std::get<1>(s.value()), "OK");

    auto get = rpc("get", RequestOne("bbt:f8:k"));
    BOOST_REQUIRE(get.has_value());
    BOOST_REQUIRE(*get);
    auto g = DecodeStrReply(fw::CoRpcResp::FromPayload((*get).value().payload));
    BOOST_REQUIRE(g);
    BOOST_CHECK_EQUAL(std::get<1>(g.value()), "v-42");

    // run 期抓出 service 的资源句柄：_ReleaseServices 在 run 返回时清
    // m_instances，此处用长寿命 shared_ptr 持有 cli 以便 shutdown 后断言
    // IsClosed 终态（对象本身仍存活——资源所有权属 spec.instance）。
    std::shared_ptr<inf::CoRedisCli> cli;
    {
        auto svc = app->find_service("cache");
        BOOST_REQUIRE(svc);
        auto* rsvc = static_cast<RedisSvc*>(svc.value().get());
        cli = rsvc->Cache();
        BOOST_REQUIRE(cli);
    }

    app->request_shutdown();
    JoinRun(h);
    BOOST_TEST(h->rc == fw::HostLifecycle::kExitOk);
    // 关闭序列对资源收束到 IsClosed 终态。
    BOOST_TEST(cli->IsClosed());
    BOOST_TEST(g_scheduler->GetRunGeneration() == 0);
}
#endif

#ifdef BBT_LIVE_HAS_MONGO
// L-M1：mongo 资源工厂装配 → handler 内经资源缝做真实 InsertOne/
// FindOne → 关闭后资源 IsClosed。入站同样经 DispatchInboundForTest。
BOOST_AUTO_TEST_CASE(live_mongo_resource_lifecycle) {
    // 全局 fixture 已保证 BBT_TEST_MONGO_URI 存在；缺环境进程已 exit(77)。
    auto host = std::make_shared<NullNetHost>();
    auto app  = MakeApp(host);
    BOOST_REQUIRE(app->add_service<DocSvc>(ConcurrentOpts()));

    // 唯一 _id：容器是共享状态，避免上次运行的文档触发 duplicate key。
    const std::string doc_id =
        "doc-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());

    inf::MongoClientConfig cfg;
    cfg.uri                      = EnvOr("BBT_TEST_MONGO_URI");
    cfg.database                 = "bbt_f8";
    cfg.collection               = "docs";
    cfg.worker_threads           = 2;
    cfg.max_queue                = 256;
    cfg.server_selection_timeout = std::chrono::milliseconds{5000};
    cfg.connect_timeout          = std::chrono::milliseconds{5000};
    cfg.socket_timeout           = std::chrono::milliseconds{5000};
    cfg.wait_queue_timeout       = std::chrono::milliseconds{5000};
    BOOST_REQUIRE(app->add_resource<inf::CoMongoCli>("docs",
        [cfg] { return inf::CoMongoCli::Create(cfg); }));

    auto h = RunApp(*app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

    auto rpc = [&app](const char* method, fw::CoRpcReq req)
        -> std::optional<fw::result<inf::RpcEnvelope>> {
        inf::RpcEnvelope env;
        env.service         = "docs";
        env.method          = method;
        env.request_id      = std::string{"m-"} + method;
        env.request_schema  = std::string(fw::kCoRpcPositionalSchema);
        env.response_schema = std::string(fw::kCoRpcPositionalSchema);
        env.payload         = req.payload();
        return DispatchRpc(*app, [&env](fw::CoApp& a) {
            inf::IncomingCallContext incoming;
            incoming.deadline = Clock::now() + kWait;
            return fw::DispatchInboundForTest(a, incoming, env);
        });
    };

    auto put = rpc("put", RequestStr(doc_id, "42"));
    BOOST_REQUIRE(put.has_value());
    BOOST_REQUIRE(*put);
    auto p = DecodeStrReply(fw::CoRpcResp::FromPayload((*put).value().payload));
    if (!p)
        BOOST_TEST_MESSAGE("put error: " << p.error().message
            << " code=" << static_cast<int>(p.error().code));
    BOOST_REQUIRE(p);
    BOOST_CHECK_EQUAL(std::get<1>(p.value()), "INS");

    auto get = rpc("get", RequestOne(doc_id));
    BOOST_REQUIRE(get.has_value());
    BOOST_REQUIRE(*get);
    auto g = DecodeStrReply(fw::CoRpcResp::FromPayload((*get).value().payload));
    BOOST_REQUIRE(g);
    BOOST_CHECK_EQUAL(std::get<1>(g.value()), "HIT");

    std::shared_ptr<inf::CoMongoCli> cli;
    {
        auto svc = app->find_service("docs");
        BOOST_REQUIRE(svc);
        auto* dsvc = static_cast<DocSvc*>(svc.value().get());
        cli = dsvc->Docs();
        BOOST_REQUIRE(cli);
    }

    app->request_shutdown();
    JoinRun(h);
    BOOST_TEST(h->rc == fw::HostLifecycle::kExitOk);
    BOOST_TEST(cli->IsClosed());
    BOOST_TEST(g_scheduler->GetRunGeneration() == 0);
}
#endif

BOOST_AUTO_TEST_SUITE_END()
} // namespace
