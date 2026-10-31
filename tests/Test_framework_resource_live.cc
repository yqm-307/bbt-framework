// 资源生命周期 live 验收：真实 Redis/MongoDB 容器经
// add_resource<R>(name, factory) 装配进 CoApp，service handler 在受管
// 请求上下文（协程 + InboundDispatcher 注入的 RequestScope）内经
// context().resource<R>(name) 消费真实命令往返；request_shutdown 后
// 关闭序列对资源同步 Close 到 IsClosed 终态。
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
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/NetworkTypes.hpp>

#ifdef BBT_LIVE_HAS_REDIS
// 新 net Redis 入口：框架侧资源适配器（engine+显式 Connect 收口归资源本体，
// 公共头不含 hiredis/底层连接类型）。
#include <bbt/infra/net/Address.hpp>
#include <bbt/framework/RedisNetResource.hpp>
#endif
#ifdef BBT_LIVE_HAS_MONGO
// 新 mongo owner/句柄入口：框架侧资源适配器（owner Close 是同步物理收口，
// 句柄 Close 只封该集合接纳——关闭序列证据见 L-M1 断言）。
#include <bbt/framework/MongoDbResource.hpp>
#endif

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/HostLifecycle.hpp>

namespace fw  = bbt::framework;
namespace co  = bbt::coroutine;
namespace inf = bbt::infra;
using Clock = std::chrono::steady_clock;

namespace {

constexpr std::chrono::milliseconds kWait{15000};

// 在途派发协程计数：DispatchRpc 超预算提前返回时，被投递协程可能仍在跑并
// 持有 CoApp 原始指针；RunHandle 析构在有界窗口内等它归零后再 join，避免
// 销毁 CoApp 后悬垂访问。到期未归零时保留 RunState（含 App 所有权）并 detach，
// 线程写的是被保留的 state（不再向裸 RunHandle* 写）。访问 Service/资源的派发
// 回调另受测试协议约束：必须在 request_shutdown 之前完成（见 L-D2）。
std::atomic<int> g_dispatch_pending{0};

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
    fw::result<void> WaitHandlersDone(co::Deadline) override {
        return fw::result<void>::ok();
    }
    void Close() noexcept override {}
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
inf::mongo::MongoDocument BsonDoc(
    const std::vector<std::pair<std::string_view,
        std::pair<bool, std::pair<std::string_view, std::int32_t>>>>& elems) {
    // elems: (key, (is_string, (sval, ival)))
    std::vector<std::uint8_t> body;
    for (const auto& [k, spec] : elems) {
        if (spec.first) BsonElemString(body, k, spec.second.first);
        else            BsonElemI32(body, k, spec.second.second);
    }
    inf::mongo::MongoDocument doc;
    const std::int32_t total =
        static_cast<std::int32_t>(body.size() + 5);   // len + elems + 0x00
    doc.bytes.reserve(static_cast<std::size_t>(total));
    BsonPutI32(doc.bytes, total);
    doc.bytes.insert(doc.bytes.end(), body.begin(), body.end());
    doc.bytes.push_back(0x00);
    return doc;
}
inf::mongo::MongoDocument DocIdVal(std::string_view id, std::int32_t v) {
    return BsonDoc({{"_id", {true,  {id, 0}}},
                    {"v",   {false, {"", v}}}});
}
inf::mongo::MongoDocument DocId(std::string_view id) {
    return BsonDoc({{"_id", {true, {id, 0}}}});
}
// update 文档：{"$set":{"v":<int32>}}（update 算子文档，UpdateOne/
// UpdateMany 要求；裸 {field:value} 是替换文档而非更新算子）。
// $set 的值是**完整嵌入文档**（len+elems+0x00），不是裸元素字节。
inf::mongo::MongoDocument DocSetV(std::int32_t v) {
    std::vector<std::uint8_t> inner;
    BsonElemI32(inner, "v", v);
    // 嵌入文档本体：len(=inner.size()+5) + inner elems + 0x00。
    std::vector<std::uint8_t> subdoc;
    BsonPutI32(subdoc, static_cast<std::int32_t>(inner.size() + 5));
    subdoc.insert(subdoc.end(), inner.begin(), inner.end());
    subdoc.push_back(0x00);
    std::vector<std::uint8_t> body;
    body.push_back(0x03);                        // embedded document
    BsonPutCStr(body, "$set");
    body.insert(body.end(), subdoc.begin(), subdoc.end());
    inf::mongo::MongoDocument doc;
    BsonPutI32(doc.bytes, static_cast<std::int32_t>(body.size() + 5));
    doc.bytes.insert(doc.bytes.end(), body.begin(), body.end());
    doc.bytes.push_back(0x00);
    return doc;
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
// 资源消费服务：handler 在受管上下文内经资源缝拿到 RedisNetResource 做
// Set/Get/Ping；结果打包 (status, value) 回复。
class RedisSvc final : public fw::CoService<RedisSvc> {
public:
    static constexpr std::string_view kServiceName = "cache";

    fw::CoRpcResp Set(fw::CoRpcReq req) {
        auto args = req.Parse<std::string, std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        auto cli = context().resource<fw::RedisNetResource>("cache");
        if (!cli)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::Unavailable, "resource 'cache' missing"));
        auto ctx = context().request();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        inf::CallOptions o;
        o.deadline = ctx.value()->deadline;
        auto r = bbt_co_await cli->Set(std::get<0>(args.value()),
                                       std::get<1>(args.value()), o);
        if (!r) return fw::CoRpcResp::Error(r.error());
        return fw::CoRpcResp::From(std::tuple{0, std::string{"OK"}});
    }

    fw::CoRpcResp Get(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        auto cli = context().resource<fw::RedisNetResource>("cache");
        if (!cli)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::Unavailable, "resource 'cache' missing"));
        auto ctx = context().request();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        inf::CallOptions o;
        o.deadline = ctx.value()->deadline;
        auto r = bbt_co_await cli->Get(args.value(), o);
        if (!r) return fw::CoRpcResp::Error(r.error());
        return fw::CoRpcResp::From(std::tuple{
            0, r.value().value_or(std::string{"<nil>"})});
    }

    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        if (pings_observer)                               // L-D2：handler 进入计数
            pings_observer->fetch_add(1, std::memory_order_acq_rel);
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        auto cli = context().resource<fw::RedisNetResource>("cache");
        if (!cli)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::Unavailable, "resource 'cache' missing"));
        auto ctx = context().request();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        inf::CallOptions o;
        o.deadline = ctx.value()->deadline;
        auto r = bbt_co_await cli->Ping(o);
        if (!r) return fw::CoRpcResp::Error(r.error());
        return fw::CoRpcResp::From(std::tuple{0, std::string{"PONG"}});
    }

    // 资源缝观察口（测试断言 IsClosed 终态用）。
    std::shared_ptr<fw::RedisNetResource> Cache() {
        return context().resource<fw::RedisNetResource>("cache");
    }

    // L-D2：Ping handler 进入计数（外部观察状态，宿主安装；关闭后不得再增长）。
    // 观察者本体由宿主持有，Service 只持同一 shared_ptr——因此即使 Service 实例
    // 析构，宿主仍能读到计数，测试无需保留 Service 强引用。
    std::shared_ptr<std::atomic<int>> pings_observer;

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&RedisSvc::Set>("set"),
        fw::Method<&RedisSvc::Get>("get"),
        fw::Method<&RedisSvc::Ping>("ping"));
};

// L-R1 用例内清理：唯一键 Delete 后同一资源上 EXISTS 必须读回 false。
// 在受管协程内调用（DispatchRpc 投递）；bbt_co_await 的真实往返，不是
// 「命令提交成功即认为已清理」——Delete 与 Exists 各自等完整回复。
inline bool RunRedisCleanup(const std::shared_ptr<fw::RedisNetResource>& cli,
                            const std::string& key,
                            const inf::CallOptions& o) {
    auto removed = bbt_co_await cli->Delete({key}, o);
    if (!removed || removed.value() != 1) return false;
    auto remaining = bbt_co_await cli->Exists(key, o);
    return remaining && !remaining.value();
}
#endif

#ifdef BBT_LIVE_HAS_MONGO
class DocSvc final : public fw::CoService<DocSvc> {
public:
    static constexpr std::string_view kServiceName = "docs";

    fw::CoRpcResp Put(fw::CoRpcReq req) {
        auto args = req.Parse<std::string, std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        auto cli = context().resource<fw::MongoDbResource>("docs");
        if (!cli)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::Unavailable, "resource 'docs' missing"));
        auto ctx = context().request();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        inf::CallOptions o;
        o.deadline = ctx.value()->deadline;
        const auto& [id, sval] = args.value();
        // 值以十进制 int32 编入文档，便于断言。
        const std::int32_t v = static_cast<std::int32_t>(
            std::stoi(sval));
        auto doc = DocIdVal(id, v);
        auto r = bbt_co_await cli->InsertOne(doc, o);
        if (!r) return fw::CoRpcResp::Error(r.error());
        return fw::CoRpcResp::From(std::tuple{0, std::string{"INS"}});
    }

    fw::CoRpcResp Get(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        auto cli = context().resource<fw::MongoDbResource>("docs");
        if (!cli)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::Unavailable, "resource 'docs' missing"));
        auto ctx = context().request();
        if (!ctx) return fw::CoRpcResp::Error(ctx.error());
        inf::CallOptions o;
        o.deadline = ctx.value()->deadline;
        auto r = bbt_co_await cli->FindOne(DocId(args.value()), o);
        if (!r) return fw::CoRpcResp::Error(r.error());
        return fw::CoRpcResp::From(std::tuple{
            0, r.value().has_value() ? std::string{"HIT"}
                                     : std::string{"MISS"}});
    }

    std::shared_ptr<fw::MongoDbResource> Docs() {
        return context().resource<fw::MongoDbResource>("docs");
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

std::shared_ptr<fw::CoApp> MakeApp(const std::shared_ptr<NullNetHost>& host) {
    return fw::MakeCoAppForTest(AppOpts(), fw::CoAppSeam{host, {}});
}

// run 线程与 App 的共享收尾状态。线程 lambda **按值持 shared_ptr<RunState>**
// （绝不捕获裸 RunHandle*）；RunHandle 只持同一 shared state。线程按 state 写
// rc/done，因此即使 RunHandle 在 detach 路径上被销毁，写入目标仍是仍存活的
// state（不再有指向已析构 RunHandle 的悬垂写）。RunState 含 App 的 shared_ptr
// 所有权：state 活着即 App 活着。state 由线程持有的 shared_ptr 保活，静态保留
// 区析构只丢弃一份引用，不会销毁线程仍在写的对象。
struct RunState {
    std::shared_ptr<fw::CoApp> app;
    std::atomic<bool>          done{false};
    int                        rc{-1};
};

std::vector<std::shared_ptr<RunState>>& RetainedRunStates() {
    static std::vector<std::shared_ptr<RunState>> states;
    return states;
}

// run 线程 + App 的 RAII 所有权。收尾规则统一为：request_shutdown → 在有限
// 窗口内等所有在途派发协程归零 → 归零才 join。到期仍未归零则**保留 RunState
// （连同 App 所有权）**并 detach 线程，明确报错；绝不 destroy 一个仍可能被回调
// 访问的 CoApp、不向已析构对象写、不强杀进程、不无限 spin。Redis 与 Mongo
// 两个 live 分支共用。
//
// 寿命边界（测试侧最小协议，见 L-D2）：本排空只保证「RunHandle 析构前归零」，
// 即归零后不存在仍在运行的派发协程；它不改变 run 线程 on_release 中
// _ReleaseServices 的时点（可能先于某个在途回调结束）。因此访问 Service/资源
// 的派发回调必须在 request_shutdown **之前**真实完成（L-D2 断言此次序）；唯一
// 的故意超窗口回调（L-D1）不触达 App/Service 状态，已在用例内文档化。
struct RunHandle {
    std::shared_ptr<RunState> state = std::make_shared<RunState>();
    std::thread               th;
    bool                      drained{false};
    bool                      finished{false};

    bool _Drain(std::chrono::milliseconds budget) {
        const auto until = Clock::now() + budget;
        while (g_dispatch_pending.load(std::memory_order_acquire) > 0 &&
               Clock::now() < until) {
            std::this_thread::yield();
        }
        return g_dispatch_pending.load(std::memory_order_acquire) == 0;
    }

    // 幂等收尾；返回是否已归零（true ⇒ 已 join，App 可安全销毁）。
    bool Finish() {
        if (finished) return drained;
        finished = true;
        if (!th.joinable()) { drained = true; return true; }
        if (!state->done.load(std::memory_order_acquire))
            state->app->request_shutdown();
        drained = _Drain(kWait);
        if (!drained) {
            std::fputs("framework.resource_live: FATAL 在途派发协程未在窗口内"
                       "归零；保留 RunState（含 App 所有权）以避免悬垂"
                       "（不 destroy/不强杀）\n",
                       stderr);
            RetainedRunStates().push_back(state);
            th.detach();
            return false;
        }
        th.join();
        return true;
    }

    ~RunHandle() { (void)Finish(); }
};
std::shared_ptr<RunHandle> RunApp(std::shared_ptr<fw::CoApp> app) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    // 嵌套出站链的协程栈需求（infra hiredis/asio 出站命令）：默认 12KB 触底；
    // 与 combined/dual_service 同口径调 64KiB——Redis 新 net 路径在受管
    // handler 内发起真实出站，不足会真实栈溢出。
    g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
    auto h = std::make_shared<RunHandle>();
    h->state->app = std::move(app);
    // 先完成 state 的共享所有权构造，再启动线程：线程不会在目标对象尚未
    // 就绪时执行，也不捕获裸 RunHandle*。
    auto st = h->state;
    h->th = std::thread([st] {
        st->rc = st->app->run();
        st->done.store(true, std::memory_order_release);
    });
    return h;
}
// 显式正常收尾：含同一有界排空（覆盖所有退出路径与 JoinRun 自身）。
// 返回是否已归零（可安全销毁 App）。
bool JoinRun(const std::shared_ptr<RunHandle>& h) { return h->Finish(); }

// 经 DispatchInboundForTest 在受管协程内分发：envelope → InboundDispatcher
// → RequestScope → handler → 资源缝 → reply，与真实线桥入站同源。
// Dispatch 内的 handler 调用要求协程上下文（CoWaiter::WaitWithCallback 仅
// 协程内合法），故本条路径必须经 g_scheduler->RegistCoroutineTask。
template <class Fn>
auto DispatchRpc(fw::CoApp& app, Fn&& fn,
                 std::chrono::milliseconds budget = kWait)
    -> std::optional<decltype(fn(std::declval<fw::CoApp&>()))> {
    using Out = decltype(fn(std::declval<fw::CoApp&>()));
    // 被投递的协程可能超出预算仍在途：结果与完成信号按值放进 shared state，
    // DispatchRpc 超时提前返回或栈展开时，不会留下指向栈对象的悬垂引用。
    // 协程体一律经 try/catch 收口并通知等待者（异常不吞、不漏唤醒），
    // 并在 RunHandle 的有界排空窗口内真实结束。
    struct State {
        explicit State(std::decay_t<Fn> f) : fn(std::move(f)) {}
        std::decay_t<Fn>   fn;
        std::optional<Out> out;
        std::exception_ptr eptr;
        TestLatch          done{1};
    };
    auto st = std::make_shared<State>(std::forward<Fn>(fn));
    fw::CoApp* app_ptr = &app;
    g_dispatch_pending.fetch_add(1, std::memory_order_acq_rel);
    bool succ = false;
    try {
        g_scheduler->RegistCoroutineTask(
            [st, app_ptr] {
                try {
                    st->out.emplace(st->fn(*app_ptr));
                } catch (...) {
                    st->eptr = std::current_exception();
                }
                g_dispatch_pending.fetch_sub(1, std::memory_order_acq_rel);
                st->done.CountDown();
            },
            succ);
    } catch (...) {
        // 注册抛出（而非返回 false）时也必须回滚计数，避免析构白等满窗口。
        g_dispatch_pending.fetch_sub(1, std::memory_order_acq_rel);
        throw;
    }
    if (!succ) {
        g_dispatch_pending.fetch_sub(1, std::memory_order_acq_rel);
        return std::nullopt;
    }
    if (!st->done.WaitFor(budget)) return std::nullopt;
    if (st->eptr) std::rethrow_exception(st->eptr);   // 协程异常真实通知等待者
    return std::move(st->out);
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

    // 新入口只接受数值 IP：loopback 地址经 net::Address::Ip 装配（不解析
    // 主机名）。测试变量注入仍是 host:port 形态——若注入主机名会在此如实
    // 失败（InvalidArgument），不静默猜测。
    std::error_code addr_ec;
    auto address = inf::net::Address::Ip(rhost, rport, addr_ec);
    BOOST_REQUIRE_MESSAGE(!addr_ec,
        "BBT_TEST_REDIS_ADDR 需为数值 IPv4/IPv6: " << addr_ec.message());

    // 每次运行唯一 key 前缀：真实后端是保留状态的实例，避免与上次运行冲突；
    // 清理只删本用例写入的键并以 EXISTS 读回证明。
    const std::string key_prefix =
        "bbt:fw-live:" + std::to_string(
            Clock::now().time_since_epoch().count()) + ":";
    const std::string test_key = key_prefix + "k";

    // 工厂形态装配：RedisNetResource::Create 在控制线程完成 Engine Run 线程
    // 拉起 + 等 Running + client 装配；显式 Connect 只允许在协程上下文调用。
    // 故工厂再投递一个 Connect 协程并在控制线程等它结束——只有 Connected
    // 实例才交框架绑定 Service，使 ready 先于入站接纳。deadline 有界、无
    // 自动重试、无固定 sleep；交付前任何失败/异常都由下方 RAII 收束为
    // 同步 Close，不留半装配句柄。
    BOOST_REQUIRE(app->add_resource<fw::RedisNetResource>("cache",
        [address]() -> inf::result<std::shared_ptr<fw::RedisNetResource>> {
        auto created = fw::RedisNetResource::Create(address);
        if (!created)
            return created;
        auto client = created.value();
        bool delivered = false;
        struct CloseUnlessDelivered {
            std::shared_ptr<fw::RedisNetResource> client;
            const bool*                           released;
            ~CloseUnlessDelivered() { if (!*released) client->Close(); }
        } guard{client, &delivered};
        auto promise = std::make_shared<std::promise<inf::result<void>>>();
        auto done    = promise->get_future();
        const inf::CallOptions call_options{Clock::now() + kWait};
        bool submitted = false;
        g_scheduler->RegistCoroutineTask([client, promise, call_options] {
            try {
                promise->set_value(bbt_co_await client->Connect(call_options));
            } catch (...) {
                promise->set_exception(std::current_exception());
            }
        }, submitted);
        if (!submitted)
            return inf::result<std::shared_ptr<fw::RedisNetResource>>::err(
                fw::MakeError(fw::ErrorCode::Unavailable,
                    "redis connect task registration failed"));
        if (done.wait_for(kWait) != std::future_status::ready)
            return inf::result<std::shared_ptr<fw::RedisNetResource>>::err(
                fw::MakeError(fw::ErrorCode::TimedOut,
                    "redis connect wait timed out"));
        auto connected = done.get();   // 协程异常经 future 重新抛出
        if (!connected)
            return inf::result<std::shared_ptr<fw::RedisNetResource>>::err(
                connected.error());
        delivered = true;
        return created;
    }));

    auto h = RunApp(app);
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
        return DispatchRpc(*app, [env](fw::CoApp& a) {
            inf::IncomingCallContext incoming;
            incoming.deadline = Clock::now() + kWait;
            return fw::DispatchInboundForTest(a, incoming, env);
        });
    };

    auto ping = rpc("ping", RequestOne("x"));
    if (ping.has_value() && !*ping)
        std::fprintf(stderr, "ping error: %s code=%d\n",
            (*ping).error().message.c_str(),
            static_cast<int>((*ping).error().code));
    BOOST_REQUIRE(ping.has_value());
    BOOST_REQUIRE(*ping);
    auto p = DecodeStrReply(fw::CoRpcResp::FromPayload((*ping).value().payload));
    BOOST_REQUIRE(p);
    BOOST_CHECK_EQUAL(std::get<1>(p.value()), "PONG");

    auto set = rpc("set", RequestStr(test_key, "v-42"));
    BOOST_REQUIRE(set.has_value());
    BOOST_REQUIRE(*set);
    auto s = DecodeStrReply(fw::CoRpcResp::FromPayload((*set).value().payload));
    BOOST_REQUIRE(s);
    BOOST_CHECK_EQUAL(std::get<1>(s.value()), "OK");

    auto get = rpc("get", RequestOne(test_key));
    BOOST_REQUIRE(get.has_value());
    BOOST_REQUIRE(*get);
    auto g = DecodeStrReply(fw::CoRpcResp::FromPayload((*get).value().payload));
    BOOST_REQUIRE(g);
    BOOST_CHECK_EQUAL(std::get<1>(g.value()), "v-42");

    // run 期抓出 service 的资源句柄：_ReleaseServices 在 run 返回时清
    // m_instances，此处用长寿命 shared_ptr 持有 cli 以便 shutdown 后断言
    // IsClosed 终态与 Engine 物理收口（对象本身仍存活——资源所有权属
    // spec.instance）。
    std::shared_ptr<fw::RedisNetResource> cli;
    {
        auto svc = app->find_service("cache");
        BOOST_REQUIRE(svc);
        auto* rsvc = static_cast<RedisSvc*>(svc.value().get());
        cli = rsvc->Cache();
        BOOST_REQUIRE(cli);
    }

    // 清理本用例写入的唯一键并读回证明：Delete 后在同一资源上 EXISTS
    // 必须为 false。清理失败如实断言，不静默跳过。
    {
        inf::CallOptions o{Clock::now() + kWait};
        auto done = DispatchRpc(*app, [&](fw::CoApp&) {
            return RunRedisCleanup(cli, test_key, o);
        });
        BOOST_REQUIRE_MESSAGE(done.has_value() && done.value(),
            "live.redis: 唯一键清理/读回失败");
    }

    app->request_shutdown();
    BOOST_REQUIRE(JoinRun(h));                       // 排空归零后才 join
    BOOST_TEST(h->state->rc == fw::HostLifecycle::kExitOk);
    // 关闭序列对资源同步 Close：IsClosed 逻辑封口 + Engine Run 已 join 且
    // Phase==Stopped（engine_stopped 观测口），与新入口关闭契约逐项对应。
    BOOST_TEST(cli->IsClosed());
    BOOST_TEST(cli->engine_stopped());
}
#endif

#ifdef BBT_LIVE_HAS_MONGO
// Mongo 清理：经 owner 的兄弟句柄对**本用例独占的 collection** 做
// DeleteMany + CountDocuments 读回证明（命名空间隔离：独占库
// bbt_f8_fw_live_<pid>、独占集合 docs_<ts>，每轮一个一次性集合，
// drop 等价为删除该集合内全部文档）。清理只在协程内执行，返回
// 是否清空；失败如实断言，不静默跳过。
inline bool RunMongoCleanup(
    const std::shared_ptr<fw::MongoDbResource>& res,
    const inf::mongo::MongoDocument& filter,
    const inf::CallOptions& o) {
    auto removed = bbt_co_await res->DeleteMany(filter, o);
    if (!removed) return false;
    auto remaining = bbt_co_await res->CountDocuments(filter, o);
    return remaining && remaining.value() == 0;
}

// L-M1：mongo owner/句柄资源工厂装配 → handler 内经资源缝做真实
// InsertOne/FindOne → 多文档面（InsertMany/Find/UpdateMany/DeleteMany/
// CountDocuments）真实往返 → owner Close 同步物理收口证据 + 兄弟句柄
// owner-close 后如实 Closed → 关闭后资源 IsClosed 终态。
// 入站同样经 DispatchInboundForTest；命名空间为独占临时集合，清理经
// DeleteMany + CountDocuments 读回证明（等价 infra DpNsGuard 的 drop+读回，
// 只清理自有命名空间）。
BOOST_AUTO_TEST_CASE(live_mongo_resource_lifecycle) {
    // 全局 fixture 已保证 BBT_TEST_MONGO_URI 存在；缺环境进程已 exit(77)。
    auto host = std::make_shared<NullNetHost>();
    auto app  = MakeApp(host);
    BOOST_REQUIRE(app->add_service<DocSvc>(ConcurrentOpts()));

    // 独占命名空间：库名按 pid 区分、集合名按时间戳区分；不触碰共享
    // bbt_live/bbt_f8 等持久集合（等价 infra 数据面用例的独占临时库口径，
    // 这里按集合粒度独占）。
    const std::string db_name =
        "bbt_f8_fw_live_" + std::to_string(::getpid());
    const std::string coll_name =
        "docs_" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
    const std::string doc_id = coll_name + "-k1";

    inf::mongo::MongoRuntimeConfig rcfg;
    rcfg.uri                      = EnvOr("BBT_TEST_MONGO_URI");
    rcfg.worker_threads           = 2;
    rcfg.max_queue                = 256;
    rcfg.server_selection_timeout = std::chrono::milliseconds{5000};
    rcfg.connect_timeout          = std::chrono::milliseconds{5000};
    rcfg.socket_timeout           = std::chrono::milliseconds{5000};
    rcfg.wait_queue_timeout       = std::chrono::milliseconds{5000};
    const inf::mongo::MongoTarget target{db_name, coll_name};
    BOOST_REQUIRE(app->add_resource<fw::MongoDbResource>("docs",
        [rcfg, target] {
            return fw::MongoDbResource::Create(rcfg, target);
        }));

    auto h = RunApp(app);
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
        return DispatchRpc(*app, [env](fw::CoApp& a) {
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

    std::shared_ptr<fw::MongoDbResource> res;
    {
        auto svc = app->find_service("docs");
        BOOST_REQUIRE(svc);
        auto* dsvc = static_cast<DocSvc*>(svc.value().get());
        res = dsvc->Docs();
        BOOST_REQUIRE(res);
    }

    // ── 多文档数据面真实往返（同资源、受管协程内）：InsertMany → Find →
    //    UpdateMany → CountDocuments → DeleteMany。命名空间独占，谓词只
    //    覆盖本用例写入的 _id 前缀。
    {
        inf::CallOptions o{Clock::now() + kWait};
        auto batch = DispatchRpc(*app, [&](fw::CoApp&) {
            std::vector<inf::mongo::MongoDocument> docs;
            docs.push_back(DocIdVal(coll_name + "-b1", 1));
            docs.push_back(DocIdVal(coll_name + "-b2", 2));
            docs.push_back(DocIdVal(coll_name + "-b3", 3));
            return bbt_co_await res->InsertMany(docs, o);
        });
        BOOST_REQUIRE(batch.has_value());
        BOOST_REQUIRE(batch.value());
        BOOST_CHECK_EQUAL(batch.value().value().inserted, std::uint64_t{2} +
            std::uint64_t{1});   // 3 docs inserted（显式拆值防写错期望）
        BOOST_CHECK_EQUAL(batch.value().value().inserted, std::uint64_t{3});
    }
    {
        inf::CallOptions o{Clock::now() + kWait};
        auto found = DispatchRpc(*app, [&](fw::CoApp&) {
            inf::mongo::FindOptions fo;
            fo.limit = 10;
            return bbt_co_await res->Find(DocId(coll_name + "-b1"), fo, o);
        });
        BOOST_REQUIRE(found.has_value());
        BOOST_REQUIRE(found.value());
        BOOST_CHECK_EQUAL(found.value().value().size(), std::size_t{1});
    }
    {
        inf::CallOptions o{Clock::now() + kWait};
        // UpdateMany 用显式前缀谓词（空 filter 一律 InvalidArgument）与
        // $set 更新算子文档。
        auto updated = DispatchRpc(*app, [&](fw::CoApp&) {
            return bbt_co_await res->UpdateMany(
                DocId(coll_name + "-b2"), DocSetV(7), o);
        });
        BOOST_REQUIRE(updated.has_value());
        if (!updated.value())
            BOOST_TEST_MESSAGE("UpdateMany error: "
                << updated.value().error().message
                << " code=" << static_cast<int>(updated.value().error().code)
                << " domain=" << updated.value().error().domain_code);
        BOOST_REQUIRE(updated.value());
        BOOST_CHECK_EQUAL(updated.value().value().matched, std::uint64_t{1});
    }
    {
        inf::CallOptions o{Clock::now() + kWait};
        auto count = DispatchRpc(*app, [&](fw::CoApp&) {
            return bbt_co_await res->CountDocuments(
                DocId(coll_name + "-b3"), o);
        });
        BOOST_REQUIRE(count.has_value());
        BOOST_REQUIRE(count.value());
        BOOST_CHECK_EQUAL(count.value().value(), std::uint64_t{1});
    }

    // 清理本用例独占集合内的全部文档并读回证明：DeleteMany 以显式谓词
    // {$exists:true} 形态匹配本集合所有文档（集合本身独占——不会误伤其他
    // 集合/库；空 filter 拒绝语义不受影响）。读回 0 才算清理完成。
    {
        inf::CallOptions o{Clock::now() + kWait};
        // {"_id":{"$exists":true}}：$exists 的值是**完整嵌入文档**
        // （len+elems+0x00），不是裸元素字节——与 DocSetV 同一形态。
        std::vector<std::uint8_t> inner;
        inner.push_back(0x08);                    // bool
        BsonPutCStr(inner, "$exists");
        inner.push_back(0x01);                    // true
        std::vector<std::uint8_t> subdoc;
        BsonPutI32(subdoc, static_cast<std::int32_t>(inner.size() + 5));
        subdoc.insert(subdoc.end(), inner.begin(), inner.end());
        subdoc.push_back(0x00);
        std::vector<std::uint8_t> body;
        body.push_back(0x03);                     // embedded document
        BsonPutCStr(body, "_id");
        body.insert(body.end(), subdoc.begin(), subdoc.end());
        inf::mongo::MongoDocument match_all;
        BsonPutI32(match_all.bytes,
                   static_cast<std::int32_t>(body.size() + 5));
        match_all.bytes.insert(match_all.bytes.end(),
                               body.begin(), body.end());
        match_all.bytes.push_back(0x00);
        auto done = DispatchRpc(*app, [&](fw::CoApp&) {
            return RunMongoCleanup(res, match_all, o);
        });
        BOOST_REQUIRE_MESSAGE(done.has_value() && done.value(),
            "live.mongo: 独占集合清理/读回失败");
    }

    // owner-handle close 证据（关闭序列之前，直接验证新入口契约）：
    // 同 owner 的兄弟句柄在 owner Close 后如实返回 Closed——句柄不新建
    // 线程、共享 owner worker，owner 收口即兄弟句柄不再可用。
    {
        auto sib = res->SiblingCollection({db_name, "docs_sibling_probe"});
        BOOST_REQUIRE(sib);                            // owner Running 时可建
        res->Close();                                  // owner 同步物理收口
        BOOST_TEST(res->IsClosed());                   // 幂等观测：逻辑+物理
        BOOST_TEST(res->coll_closed());                // 句柄门禁已执行
        inf::CallOptions o{Clock::now() + kWait};
        auto coll = sib.value();
        auto after = DispatchRpc(*app, [&](fw::CoApp&) {
            return bbt_co_await coll->FindOne(DocId("x"), o);
        });
        // owner 关闭后兄弟句柄命令如实失败（Closed）——不放宽为任意错误：
        // 只接受 Closed；DispatchRpc 层异常/超时同样判失败。
        BOOST_REQUIRE(after.has_value());
        BOOST_REQUIRE_MESSAGE(!after.value(),
            "owner Close 后兄弟句柄命令应失败（Closed），实际成功");
        BOOST_CHECK_EQUAL(
            static_cast<int>(after.value().error().code),
            static_cast<int>(inf::ErrorCode::Closed));
    }

    app->request_shutdown();
    BOOST_REQUIRE(JoinRun(h));                       // 排空归零后才 join
    BOOST_TEST(h->state->rc == fw::HostLifecycle::kExitOk);
    BOOST_TEST(res->IsClosed());
}
#endif

// L-D1：派发超预算 + 在途回调寿命。DispatchRpc 超预算提前返回时在途协程仍
// pending（可观察）；收尾必须等它真实结束才 join，App 不得在其回调仍可能运行
// 时被销毁。真实对象寿命证据：JoinRun 只有在 g_dispatch_pending 归零后才返回
// true 并 join，且回调确实跑完（ran==true），非「无信号即安全」。
//
// 寿命边界（刻意留白，见 L-D2）：本用例的回调体只等自身 CoWaiter 落定并置位
// 标志，**不触达 App/Service/资源状态**——因此它在 request_shutdown 之后仍可能
// 在途，也不会与 run 线程 on_release 的 _ReleaseServices 竞争任何 Service 状态。
// 若要让访问 Service 的回调跨越 request_shutdown 仍安全，需要核心侧支持
// （drain 覆盖 _ReleaseServices，或用 shared owner 保活 Service 访问），
// 属本轮授权之外，本测试不伪造该保证。
BOOST_AUTO_TEST_CASE(live_dispatch_drain_outlives_budget) {
    auto host = std::make_shared<NullNetHost>();
    auto app  = MakeApp(host);
    auto h = RunApp(app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

    std::atomic<bool> ran{false};
    // 协程内等到自身 deadline 才返回（无 sleep，CoWaiter 在 deadline 处落定）。
    auto out = DispatchRpc(*app, [&ran](fw::CoApp&) -> int {
        auto w = co::sync::CoWaiter::Create();
        co::WaitOptions wo;
        wo.deadline = Clock::now() + std::chrono::milliseconds{600};
        (void)w->Wait(wo);
        ran.store(true, std::memory_order_release);
        return 7;
    }, std::chrono::milliseconds{100});
    BOOST_CHECK(!out.has_value());                    // 超预算提前返回
    BOOST_CHECK(g_dispatch_pending.load(std::memory_order_acquire) > 0);

    app->request_shutdown();
    BOOST_REQUIRE(JoinRun(h));                        // 在途归零后才 join
    BOOST_CHECK(ran.load(std::memory_order_acquire)); // 回调真实完成
    BOOST_TEST(h->state->rc == fw::HostLifecycle::kExitOk);
}

#ifdef BBT_LIVE_HAS_REDIS
// L-D2：Service 寿命边界（确定性次序；关闭后入口拒绝 + Service 实例已释放）。
// 测试侧最小寿命协议：访问 Service/资源的派发回调必须在 request_shutdown
// **之前**真实完成——此时 RunHandle 仍持有 App、Service 仍绑定（find_service
// 成功）。测试**不保留 Service 强引用**：只用外部观察状态（RedisSvc 的
// pings_observer：宿主安装、handler 自增的 shared_ptr<atomic<int>>）与 weak_ptr
// 观察——即使 Service 实例析构，宿主仍能读到计数。关闭后 m_instances 与
// dispatcher 里的实例所有权一并释放：同一入站被确定性拒绝（RuntimeUnavailable）、
// handler 计数不再增长、weak_ptr 过期（无其它强引用）。不把裸 Service 指针跨关闭
// 使用。
// 说明：本用例不执行真实后端命令（资源缺席即如实返回错误）；它证明的是寿命
// 次序与关闭后入口拒绝，真实后端往返证据在 L-R1/L-M1。
BOOST_AUTO_TEST_CASE(live_service_lifetime_bounded_by_run) {
    auto host = std::make_shared<NullNetHost>();
    auto app  = MakeApp(host);
    BOOST_REQUIRE(app->add_service<RedisSvc>(ConcurrentOpts()));

    auto h = RunApp(app);
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));
    // 等生命周期进入 Running（非关停中）再断言寿命次序。
    const auto until_running = Clock::now() + kWait;
    while (app->shutdown_state() != fw::ShutdownState::Running) {
        BOOST_REQUIRE(Clock::now() < until_running);
        std::this_thread::yield();
    }

    // 安装外部观察状态后立即释放对 Service 的强引用（weak_ptr 仅观察）。
    auto observer = std::make_shared<std::atomic<int>>(0);
    std::weak_ptr<fw::ICoService> weak;
    {
        auto svc = app->find_service("cache");
        BOOST_REQUIRE(svc);
        static_cast<RedisSvc*>(svc.value().get())->pings_observer = observer;
        weak = svc.value();
    }   // svc 出作用域：测试不再持有 Service 强引用

    auto dispatch = [&app] {
        inf::RpcEnvelope env;
        env.service         = "cache";
        env.method          = "ping";
        env.request_id      = "l-d2";
        env.request_schema  = std::string(fw::kCoRpcPositionalSchema);
        env.response_schema = std::string(fw::kCoRpcPositionalSchema);
        env.payload         = RequestOne("x").payload();
        return DispatchRpc(*app, [env](fw::CoApp& a) {
            inf::IncomingCallContext incoming;
            incoming.deadline = Clock::now() + kWait;
            return fw::DispatchInboundForTest(a, incoming, env);
        });
    };

    // (1) 运行期：派发回调经真实 Dispatcher 进入 Service handler 并真实结束
    //     （观察者 +1）；此刻 RunHandle 仍持有 App/Service（weak 未过期）。
    auto during = dispatch();
    BOOST_REQUIRE(during.has_value());                // 回调已真实返回
    BOOST_CHECK_EQUAL(g_dispatch_pending.load(std::memory_order_acquire), 0);
    BOOST_CHECK_EQUAL(observer->load(std::memory_order_acquire), 1);
    BOOST_CHECK(!weak.expired());                     // Service 仍被 run 持有
    BOOST_REQUIRE(h->state->app.get() == app.get());  // RunHandle 仍拥有 App

    // (2) 关闭：归零后才 join；RunHandle 在 run 返回前持续拥有 App。
    app->request_shutdown();
    BOOST_REQUIRE(JoinRun(h));
    BOOST_TEST(h->state->rc == fw::HostLifecycle::kExitOk);

    // (3) 关闭后：入站被确定性拒绝、handler 不再被进入；测试未持强引用，
    //     Service 实例已无所有者（weak 过期）。不触达任何已释放 Service。
    auto released = app->find_service("cache");
    BOOST_REQUIRE(!released);
    BOOST_CHECK_EQUAL(static_cast<int>(released.error().code),
                      static_cast<int>(fw::ErrorCode::RuntimeUnavailable));
    auto after = dispatch();
    BOOST_REQUIRE(after.has_value());                 // 派发回调仍能返回（无悬挂）
    BOOST_REQUIRE(!*after);
    BOOST_CHECK_EQUAL(static_cast<int>((*after).error().code),
                      static_cast<int>(fw::ErrorCode::RuntimeUnavailable));
    BOOST_CHECK_EQUAL(observer->load(std::memory_order_acquire), 1);  // handler 未被触达
    // 释放是收敛事件（在途 SDK 帧可能短暂持引用）：有限窗口内等其归零再断言。
    const auto until_gone = Clock::now() + kWait;
    while (!weak.expired() && Clock::now() < until_gone) std::this_thread::yield();
    BOOST_CHECK(weak.expired());                      // Service 实例已释放
}
#endif

BOOST_AUTO_TEST_SUITE_END()
} // namespace
