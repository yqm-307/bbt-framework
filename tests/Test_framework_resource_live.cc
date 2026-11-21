/**
 * 资源生命周期 live 验收：真实 Redis/MongoDB 后端经
 * CoInstallComponent<R>(name, Options) 装配进 CoApp（受管组件），
 * service handler 在受管请求上下文（协程 + InboundDispatcher 注入的
 * RequestScope）内经 GetComponent<R>(name)->Borrow() 消费真实命令
 * 往返；RequestShutdown 后 CoStop 对资源同步收口到 Closed 终态。
 *
 * 环境变量驱动（与 infra live 同一约定）：
 * BBT_TEST_REDIS_ADDR=host:port         缺 → redis 用例 skip
 * BBT_TEST_MONGO_URI=mongodb://h:p/     缺 → mongo 用例 skip
 * 不伪造：无真实服务时不构造桩替代，跳过由环境决定。
 *
 * 入站驱动经 DispatchInboundForTest（CoApp::dispatch_inbound 的测试
 * 转发口）：V2 Request → InboundDispatcher → RequestScope → handler →
 * 组件借用 → Response 全链，与真实入站同源；不经网络栈也不绕过
 * Dispatcher 直接触 handler。Context 由 infra 填充，测试不伪造入站事实。
 *
 * 生命周期范式（迁移语义，无 run()/HostLifecycle）：测试进程自带
 * coroutine runtime；装配/CoStart/CoWaitShutdownRequest/CoStop 全部
 * 在受管协程内执行（RunInManagedCoroutine 投递），外部线程
 * RequestShutdown 唤醒等待。
 *
 * 「资源同步 Close 到终态」的断言映射（新模型如实落地，非削弱）：
 * 受管组件被借出期间 CoStop 如实落 StopIncomplete——borrow 排空是
 * 收口前提，owner 与资源版本保留（IsClosed 仍为 false、真实命令仍
 * 可完成），该窗口本身是「不提前收口」的正面证据；借用归还后再次
 * CoStop 推进至 Stopped，组件销毁意味着其同步 OnStop 已落定
 * （RedisNetResource::OnStop = client Close + Engine Close/
 * join/Stopped 核验；MongoDbResource::OnStop = 集合句柄门禁 + owner
 * Close 同步物理收口）。IsClosed/engine_stopped/coll_closed 只读
 * 观测口在对象销毁后不可读——旧「shutdown 后 IsClosed」断言映射为
 * 「owner 保留窗口 IsClosed==false + 真实命令仍通 + 销毁完成
 * （FindComponent→NotFound）+ Mongo 兄弟句柄在 owner Close 后命令
 * 如实失败（只接受 Closed）」。
 */
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
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>

#ifdef BBT_LIVE_HAS_REDIS
/**
 * 新 net Redis 入口：框架侧资源适配器（engine+显式 Connect 收口归资源本体，
 * 公共头不含 hiredis/底层连接类型）。
 */
#include <bbt/infra/net/Address.hpp>
#include <bbt/framework/RedisNetResource.hpp>
#endif
#ifdef BBT_LIVE_HAS_MONGO
/**
 * 新 mongo owner/句柄入口：框架侧资源适配器（owner Close 是同步物理收口，
 * 句柄 Close 只封该集合接纳——兄弟句柄 owner-close 后命令失败证据见
 * L-M1 断言）。
 */
#include <bbt/framework/MongoDbResource.hpp>
#endif

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/NetworkHost.hpp>

namespace fw = bbt::framework;
namespace co = bbt::coroutine;
namespace inf = bbt::infra;
using Clock = std::chrono::steady_clock;

namespace {

constexpr std::chrono::milliseconds kWait{15000};

/**
 * 在途派发协程计数：DispatchRpc 超预算提前返回时，被投递协程可能仍在跑并
 * 持有 CoApp 原始指针；用例收尾在有界窗口内等它归零后再继续，避免销毁
 * CoApp 后悬垂访问。访问 Service/资源的派发回调另受测试协议约束：必须在
 * RequestShutdown 之前完成（见 L-D2）。
 */
std::atomic<int> g_dispatch_pending{0};

const char* EnvOr(const char* k) {
    return std::getenv(k);
}

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

/**
 * 缺环境必须显式标记 Skipped（CTest SKIP_RETURN_CODE 77），不能以退出码 0
 * 冒充通过——与 infra live 同一约定：进程退出码 77 即标记，否则就算
 * case 内 return 也会被 CTest 记为 Passed。
 */
struct RequireLiveEnv {
    RequireLiveEnv() {
#if defined(BBT_LIVE_HAS_REDIS)
        if (!RedisEnabled()) {
            std::fputs("live.redis: BBT_TEST_REDIS_ADDR 未设置，"
                       "标记 Skipped\n",
                       stderr);
            std::exit(77);
        }
#elif defined(BBT_LIVE_HAS_MONGO)
        if (!MongoEnabled()) {
            std::fputs("live.mongo: BBT_TEST_MONGO_URI 未设置，"
                       "标记 Skipped\n",
                       stderr);
            std::exit(77);
        }
#else
        /**
         * 编译期兜底：本 target 必须至少带一个模块宏，否则无意义。
         */
        std::fputs("live: 未启用 BBT_LIVE_HAS_* 宏，target 配置错误\n",
                   stderr);
        std::exit(1);
#endif
    }
};
BOOST_GLOBAL_FIXTURE(RequireLiveEnv);

#ifdef BBT_LIVE_HAS_REDIS
/**
 * "host:port" → (host, port)
 */
std::pair<std::string, std::uint16_t> SplitAddr(const std::string& addr) {
    const auto pos = addr.rfind(':');
    if (pos == std::string::npos)
        return {addr, 0};
    return {addr.substr(0, pos),
            static_cast<std::uint16_t>(std::stoi(addr.substr(pos + 1)))};
}
/**
 * BBT_LIVE_HAS_REDIS
 */
#endif

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
    bool WaitFor(std::chrono::milliseconds ms) {
        std::unique_lock<std::mutex> lk(m_mtx);
        return m_cv.wait_for(lk, ms, [this] { return m_count == 0; });
    }

private:
    std::mutex m_mtx;
    std::condition_variable m_cv;
    int m_count;
};

/**
 * ── INetworkHost 桩：live 用例不经真实 socket；宿主只提供生命周期骨架，
 * 入站由 dispatch_inbound 直接驱动。──
 */
class NullNetHost final : public fw::INetworkHost {
public:
    TestLatch start_seen{1};
    fw::result<void> Create() override { return fw::result<void>::ok(); }
    fw::result<void> Start() override {
        start_seen.CountDown();
        return fw::result<void>::ok();
    }
    void StopAccepting() noexcept override {}
    fw::result<void> WaitHandlersDone(co::Deadline) override {
        return fw::result<void>::ok();
    }
    void Close() noexcept override {}
};

/**
 * ── BSON 裸字节编码（MongoDocument 是 owning bytes；只编码本测试用的
 * 最小文档形态：string 与 int32 元素）。仅 BBT_LIVE_HAS_MONGO 编译。──
 */
#ifdef BBT_LIVE_HAS_MONGO
void BsonPutI32(std::vector<std::uint8_t>& b, std::int32_t v) {
    b.push_back(static_cast<std::uint8_t>(v & 0xFF));
    b.push_back(static_cast<std::uint8_t>((v >> 8) & 0xFF));
    b.push_back(static_cast<std::uint8_t>((v >> 16) & 0xFF));
    b.push_back(static_cast<std::uint8_t>((v >> 24) & 0xFF));
}
void BsonPutCStr(std::vector<std::uint8_t>& b, std::string_view s) {
    for (char c : s)
        b.push_back(static_cast<std::uint8_t>(c));
    b.push_back(0x00);
}
void BsonElemString(std::vector<std::uint8_t>& b,
                    std::string_view key, std::string_view val) {
    /**
     * type: string
     */
    b.push_back(0x02);
    BsonPutCStr(b, key);
    BsonPutI32(b, static_cast<std::int32_t>(val.size() + 1));
    BsonPutCStr(b, val);
}
void BsonElemI32(std::vector<std::uint8_t>& b,
                 std::string_view key, std::int32_t v) {
    /**
     * type: int32
     */
    b.push_back(0x10);
    BsonPutCStr(b, key);
    BsonPutI32(b, v);
}
inf::mongo::MongoDocument BsonDoc(
    const std::vector<std::pair<std::string_view,
                                std::pair<bool, std::pair<std::string_view, std::int32_t>>>>& elems) {
    /**
     * elems: (key, (is_string, (sval, ival)))
     */
    std::vector<std::uint8_t> body;
    for (const auto& [k, spec] : elems) {
        if (spec.first)
            BsonElemString(body, k, spec.second.first);
        else
            BsonElemI32(body, k, spec.second.second);
    }
    inf::mongo::MongoDocument doc;
    const std::int32_t total =
        /**
         * len + elems + 0x00
         */
        static_cast<std::int32_t>(body.size() + 5);
    doc.bytes.reserve(static_cast<std::size_t>(total));
    BsonPutI32(doc.bytes, total);
    doc.bytes.insert(doc.bytes.end(), body.begin(), body.end());
    doc.bytes.push_back(0x00);
    return doc;
}
inf::mongo::MongoDocument DocIdVal(std::string_view id, std::int32_t v) {
    return BsonDoc({{"_id", {true, {id, 0}}},
                    {"v", {false, {"", v}}}});
}
inf::mongo::MongoDocument DocId(std::string_view id) {
    return BsonDoc({{"_id", {true, {id, 0}}}});
}
/**
 * update 文档：{"$set":{"v":<int32>}}（update 算子文档，UpdateOne/
 * UpdateMany 要求；裸 {field:value} 是替换文档而非更新算子）。
 * $set 的值是**完整嵌入文档**（len+elems+0x00），不是裸元素字节。
 */
inf::mongo::MongoDocument DocSetV(std::int32_t v) {
    std::vector<std::uint8_t> inner;
    BsonElemI32(inner, "v", v);
    /**
     * 嵌入文档本体：len(=inner.size()+5) + inner elems + 0x00。
     */
    std::vector<std::uint8_t> subdoc;
    BsonPutI32(subdoc, static_cast<std::int32_t>(inner.size() + 5));
    subdoc.insert(subdoc.end(), inner.begin(), inner.end());
    subdoc.push_back(0x00);
    std::vector<std::uint8_t> body;
    /**
     * embedded document
     */
    body.push_back(0x03);
    BsonPutCStr(body, "$set");
    body.insert(body.end(), subdoc.begin(), subdoc.end());
    inf::mongo::MongoDocument doc;
    BsonPutI32(doc.bytes, static_cast<std::int32_t>(body.size() + 5));
    doc.bytes.insert(doc.bytes.end(), body.begin(), body.end());
    doc.bytes.push_back(0x00);
    return doc;
}
/**
 * BBT_LIVE_HAS_MONGO
 */
#endif

/**
 * ── CoRpc 打包/拆包（positional schema）。──
 */
fw::CoRpcReq RequestStr(std::string a, std::string b) {
    auto req = fw::CoRpcReq::From(std::tuple{std::move(a), std::move(b)});
    if (!req)
        throw std::logic_error("request encode failed");
    return std::move(req).value();
}
fw::CoRpcReq RequestOne(std::string a) {
    auto req = fw::CoRpcReq::From(std::move(a));
    if (!req)
        throw std::logic_error("request encode failed");
    return std::move(req).value();
}

/**
 * 解码 handler 回包：T=int32 状态码 + string 值。
 */
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
/**
 * 资源消费服务：handler 在受管上下文内经声明式绑定 + Borrow 拿到
 * RedisNetResource 做 Set/Get/Ping；结果打包 (status, value) 回复。
 */
class RedisSvc final : public fw::CoService<fw::RedisNetResource> {
public:
    static constexpr std::string_view kServiceName = "cache";

    explicit RedisSvc(fw::detail::ConstructKey k)
        : fw::CoService<fw::RedisNetResource>(k) {}

    fw::CoRpcResp Set(fw::CoRpcReq req) {
        auto args = req.Parse<std::string, std::string>();
        if (!args)
            return fw::CoRpcResp::Error(args.error());
        auto cli_r = Res();
        if (!cli_r)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::Unavailable, "resource 'cache' missing"));
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        inf::CallOptions o;
        o.deadline = ctx.value()->deadline;
        auto r = bbt_co_await cli_r.value().Get().Set(
            std::get<0>(args.value()), std::get<1>(args.value()), o);
        if (!r)
            return fw::CoRpcResp::Error(r.error());
        return fw::CoRpcResp::From(std::tuple{0, std::string{"OK"}});
    }

    fw::CoRpcResp Get(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args)
            return fw::CoRpcResp::Error(args.error());
        auto cli_r = Res();
        if (!cli_r)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::Unavailable, "resource 'cache' missing"));
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        inf::CallOptions o;
        o.deadline = ctx.value()->deadline;
        auto r = bbt_co_await cli_r.value().Get().Get(args.value(), o);
        if (!r)
            return fw::CoRpcResp::Error(r.error());
        return fw::CoRpcResp::From(std::tuple{
            0, r.value().value_or(std::string{"<nil>"})});
    }

    fw::CoRpcResp Ping(fw::CoRpcReq req) {
        /**
         * L-D2：handler 进入计数
         */
        if (s_pings_observer)
            s_pings_observer->fetch_add(1, std::memory_order_acq_rel);
        auto args = req.Parse<std::string>();
        if (!args)
            return fw::CoRpcResp::Error(args.error());
        auto cli_r = Res();
        if (!cli_r)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::Unavailable, "resource 'cache' missing"));
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        inf::CallOptions o;
        o.deadline = ctx.value()->deadline;
        auto r = bbt_co_await cli_r.value().Get().Ping(o);
        if (!r)
            return fw::CoRpcResp::Error(r.error());
        return fw::CoRpcResp::From(std::tuple{0, std::string{"PONG"}});
    }

    /**
     * L-D2：Ping handler 进入计数（外部观察状态，宿主测试装配；关闭后不得
     * 再增长）。观察者本体由宿主持有，Service 只持同一 shared_ptr——因此
     * 即使 Service 实例析构，宿主仍能读到计数。新模型下测试拿不到 Service
     * 实例（FindService 只发非拥有 handle），改静态成员注入。
     */
    static inline std::shared_ptr<std::atomic<int>> s_pings_observer;

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&RedisSvc::Set>("set"),
        fw::Method<&RedisSvc::Get>("get"),
        fw::Method<&RedisSvc::Ping>("ping"));

protected:
    ~RedisSvc() noexcept override = default;

private:
    fw::result<fw::ComponentBorrow<fw::RedisNetResource>> Res() {
        auto h = GetComponent<fw::RedisNetResource>("cache");
        if (!h)
            return fw::result<fw::ComponentBorrow<fw::RedisNetResource>>::err(
                std::move(h.error()));
        return h.value().Borrow();
    }
    friend class fw::detail::Registry;
};

/**
 * 测试侧借用服务：类型声明集合含 RedisNetResource，但不声明
 * BindComponent——由用例在运行期经 CoBindComponent 生成第二条 binding
 * （同名同 subject 的声明式绑定被 duplicate binding 拒绝，故必须走
 * 独立 subject）。借出的 ComponentBorrow 持有对象版本 lease：跨 CoStop
 * 持有即「资源未收口」的正面证据（borrow 排空先于 OnStop，owner 保留
 * 落 StopIncomplete），同时保活对象本体供 IsClosed/engine_stopped
 * 观测口读取。
 */
class RedisPeek final : public fw::CoService<fw::RedisNetResource> {
public:
    static constexpr std::string_view kServiceName = "peek";
    explicit RedisPeek(fw::detail::ConstructKey k)
        : fw::CoService<fw::RedisNetResource>(k) {}

protected:
    ~RedisPeek() noexcept override = default;

private:
    friend class fw::detail::Registry;
};

/**
 * 经 CoBindComponent 给 PeekSvc 生成 binding 并发放 handle。
 * 须在受管协程内调用。
 */
fw::result<fw::ComponentHandle<fw::RedisNetResource>> BindRedisPeek(
    fw::CoApp& app) {
    auto svc = app.FindService("peek");
    if (!svc)
        return fw::result<fw::ComponentHandle<fw::RedisNetResource>>::err(
            std::move(svc.error()));
    return bbt_co_await app.CoBindComponent<fw::RedisNetResource>(
        svc.value(), "cache");
}
#endif

#ifdef BBT_LIVE_HAS_MONGO
/**
 * 资源消费服务：handler 在受管上下文内经声明式绑定 + Borrow 拿到
 * MongoDbResource 做 InsertOne/FindOne；结果打包 (status, value) 回复。
 */
class DocSvc final : public fw::CoService<fw::MongoDbResource> {
public:
    static constexpr std::string_view kServiceName = "docs";

    explicit DocSvc(fw::detail::ConstructKey k)
        : fw::CoService<fw::MongoDbResource>(k) {}

    fw::CoRpcResp Put(fw::CoRpcReq req) {
        auto args = req.Parse<std::string, std::string>();
        if (!args)
            return fw::CoRpcResp::Error(args.error());
        auto cli_r = Res();
        if (!cli_r)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::Unavailable, "resource 'docs' missing"));
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        inf::CallOptions o;
        o.deadline = ctx.value()->deadline;
        const auto& [id, sval] = args.value();
        /**
         * 值以十进制 int32 编入文档，便于断言。
         */
        const std::int32_t v = static_cast<std::int32_t>(
            std::stoi(sval));
        auto doc = DocIdVal(id, v);
        auto r = bbt_co_await cli_r.value().Get().InsertOne(doc, o);
        if (!r)
            return fw::CoRpcResp::Error(r.error());
        return fw::CoRpcResp::From(std::tuple{0, std::string{"INS"}});
    }

    fw::CoRpcResp Get(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args)
            return fw::CoRpcResp::Error(args.error());
        auto cli_r = Res();
        if (!cli_r)
            return fw::CoRpcResp::Error(fw::MakeError(
                fw::ErrorCode::Unavailable, "resource 'docs' missing"));
        auto ctx = fw::CurrentRequestContext();
        if (!ctx)
            return fw::CoRpcResp::Error(ctx.error());
        inf::CallOptions o;
        o.deadline = ctx.value()->deadline;
        auto r = bbt_co_await cli_r.value().Get().FindOne(
            DocId(args.value()), o);
        if (!r)
            return fw::CoRpcResp::Error(r.error());
        return fw::CoRpcResp::From(std::tuple{
            0, r.value().has_value() ? std::string{"HIT"}
                                     : std::string{"MISS"}});
    }

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&DocSvc::Put>("put"),
        fw::Method<&DocSvc::Get>("get"));

protected:
    ~DocSvc() noexcept override = default;

private:
    fw::result<fw::ComponentBorrow<fw::MongoDbResource>> Res() {
        auto h = GetComponent<fw::MongoDbResource>("docs");
        if (!h)
            return fw::result<fw::ComponentBorrow<fw::MongoDbResource>>::err(
                std::move(h.error()));
        return h.value().Borrow();
    }
    friend class fw::detail::Registry;
};

/**
 * 测试侧借用服务（同 RedisPeek 的设计动机：同 subject 同组件的第二条
 * binding 只有独立 subject 才能建立；借出的 borrow 跨 CoStop 持有即
 * owner 保留窗口的观测与保活句柄）。
 */
class MongoPeek final : public fw::CoService<fw::MongoDbResource> {
public:
    static constexpr std::string_view kServiceName = "peek";
    explicit MongoPeek(fw::detail::ConstructKey k)
        : fw::CoService<fw::MongoDbResource>(k) {}

protected:
    ~MongoPeek() noexcept override = default;

private:
    friend class fw::detail::Registry;
};

/**
 * 经 CoBindComponent 给 PeekSvc 生成 binding 并发放 handle。
 * 须在受管协程内调用。
 */
fw::result<fw::ComponentHandle<fw::MongoDbResource>> BindMongoPeek(
    fw::CoApp& app) {
    auto svc = app.FindService("peek");
    if (!svc)
        return fw::result<fw::ComponentHandle<fw::MongoDbResource>>::err(
            std::move(svc.error()));
    return bbt_co_await app.CoBindComponent<fw::MongoDbResource>(
        svc.value(), "docs");
}
#endif

fw::ServiceOptions ConcurrentOpts() {
    return fw::ServiceOptions{}
        .SetExecution(fw::ExecutionPolicy::Concurrent)
        .SetMaxInflight(64);
}

#ifdef BBT_LIVE_HAS_REDIS
fw::ServiceOptions RedisSvcOpts() {
    return ConcurrentOpts().BindComponent<fw::RedisNetResource>("cache");
}
#endif
#ifdef BBT_LIVE_HAS_MONGO
fw::ServiceOptions DocSvcOpts() {
    return ConcurrentOpts().BindComponent<fw::MongoDbResource>("docs");
}
#endif

fw::AppOptions AppOpts() {
    return fw::AppOptions{}
        .SetLimits(64, 64, 64, 64)
        .SetShutdownBudget(std::chrono::milliseconds{3000})
        .SetLifecycleBudget(std::chrono::milliseconds{3000})
        .SetNetworkLimits(inf::NetworkLimits{
            64, 64, 8192, 65536, std::chrono::milliseconds{30000}})
        .SetLocalIdentity(std::string{"resource-live-test"});
}

std::shared_ptr<fw::CoApp> MakeApp(const std::shared_ptr<NullNetHost>& host) {
    return fw::MakeCoAppForTest(AppOpts(), fw::CoAppSeam{host, {}});
}

/**
 * 在受管协程内执行 fn：经 g_scheduler 投递协程任务，宿主线程用
 * 限时 std::cv 等其完成（宿主线程的 CV 等待不违反 worker 阻塞禁令）。
 * 返回 false 表示任务未在预算内完成（防挂死测试进程）。
 * 与 resource_lifecycle 同范式；stack_size 同旧 RunApp 的 64KiB
 * （嵌套出站链的协程栈需求：infra hiredis/asio 出站命令）。
 */
template <class F>
bool RunInManagedCoroutine(F&& fn,
                           std::chrono::milliseconds budget = std::chrono::milliseconds{30000}) {
    if (bbt::coroutine::IsInCoroutineContext()) {
        fn();
        return true;
    }
    /**
     * native 测试宿主负责初始化唯一 runtime，产品 App 不启动 runtime。
     */
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
        g_bbt_coroutine_config->m_cfg_stack_size = 64 * 1024;
        g_scheduler->Start();
    }
    struct Slot {
        std::mutex mtx;
        std::condition_variable cv;
        bool done = false;
        std::exception_ptr error;
    };
    auto slot = std::make_shared<Slot>();
    auto f = std::forward<F>(fn);
    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [slot, f = std::move(f)]() mutable {
            try {
                f();
            } catch (...) { slot->error = std::current_exception(); }
            std::lock_guard<std::mutex> lk(slot->mtx);
            slot->done = true;
            slot->cv.notify_all();
        },
        submitted);
    if (!submitted)
        throw std::runtime_error("test coroutine submission rejected");
    std::unique_lock<std::mutex> lk(slot->mtx);
    const bool done = slot->cv.wait_for(lk, budget, [&] { return slot->done; });
    /**
     * 不让仍在途任务使用已退出的测试栈。
     */
    if (!done)
        std::terminate();
    if (slot->error)
        std::rethrow_exception(slot->error);
    return done;
}

/**
 * 经 DispatchInboundForTest 在受管协程内分发：V2 Request → InboundDispatcher
 * → RequestScope → handler → 组件借用 → Response，与真实入站同源。
 * Dispatch 内的 handler 调用要求协程上下文（CoWaiter::WaitWithCallback 仅
 * 协程内合法），故本条路径必须经 g_scheduler->RegistCoroutineTask。
 */
template <class Fn>
auto DispatchRpc(fw::CoApp& app, Fn&& fn,
                 std::chrono::milliseconds budget = kWait)
    -> std::optional<decltype(fn(std::declval<fw::CoApp&>()))> {
    using Out = decltype(fn(std::declval<fw::CoApp&>()));
    /**
     * 被投递的协程可能超出预算仍在途：结果与完成信号按值放进 shared state，
     * DispatchRpc 超时提前返回或栈展开时，不会留下指向栈对象的悬垂引用。
     * 协程体一律经 try/catch 收口并通知等待者（异常不吞、不漏唤醒），
     * 并在用例的有界排空窗口内真实结束。
     */
    struct State {
        explicit State(std::decay_t<Fn> f) : fn(std::move(f)) {}
        std::decay_t<Fn> fn;
        std::optional<Out> out;
        std::exception_ptr eptr;
        TestLatch done{1};
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
        /**
         * 注册抛出（而非返回 false）时也必须回滚计数，避免析构白等满窗口。
         */
        g_dispatch_pending.fetch_sub(1, std::memory_order_acq_rel);
        throw;
    }
    if (!succ) {
        g_dispatch_pending.fetch_sub(1, std::memory_order_acq_rel);
        return std::nullopt;
    }
    if (!st->done.WaitFor(budget))
        return std::nullopt;
    /**
     * 协程异常真实通知等待者
     */
    if (st->eptr)
        std::rethrow_exception(st->eptr);
    return std::move(st->out);
}

/**
 * 用例收尾：在有限窗口内等所有在途派发协程归零（旧 RunHandle 排空的
 * 对应物），归零才允许进入 CoStop；到期未归零如实断言失败（CoApp 由
 * shared_ptr 强持有，不归零不销毁）。访问 Service/资源的派发回调必须在
 * RequestShutdown 之前真实完成（L-D2 协议）。
 */
bool DrainDispatch(std::chrono::milliseconds budget) {
    const auto until = Clock::now() + budget;
    while (g_dispatch_pending.load(std::memory_order_acquire) > 0 &&
           Clock::now() < until) {
        std::this_thread::yield();
    }
    return g_dispatch_pending.load(std::memory_order_acquire) == 0;
}

BOOST_AUTO_TEST_SUITE(framework_resource_live)

#ifdef BBT_LIVE_HAS_REDIS
/**
 * L-R1 用例内清理：唯一键 Delete 后同一资源上 EXISTS 必须读回 false。
 * 在受管协程内调用（DispatchRpc 投递）；bbt_co_await 的真实往返，不是
 * 「命令提交成功即认为已清理」——Delete 与 Exists 各自等完整回复。
 */
inline bool RunRedisCleanup(fw::RedisNetResource& cli,
                            const std::string& key,
                            const inf::CallOptions& o) {
    auto removed = bbt_co_await cli.Delete({key}, o);
    if (!removed || removed.value() != 1)
        return false;
    auto remaining = bbt_co_await cli.Exists(key, o);
    return remaining && !remaining.value();
}

/**
 * L-R1：redis 组件装配 → 测试侧借用服务 Connect（新入口显式 Connect 在
 * 协程上下文）→ service handler 内经组件借用做真实 Set/Get/Ping 往返 →
 * RequestShutdown 后 CoStop 收口；borrow 持有期间 CoStop 如实落
 * StopIncomplete（owner/资源版本保留、IsClosed 仍为 false、真实命令
 * 仍通）→ 归还借用再次 CoStop 推进至 Stopped、组件销毁。
 * 入站经 DispatchInboundForTest（InboundDispatcher+RequestScope 真实
 * 链路），不绕过 Dispatcher 直接调 handler。
 */
BOOST_AUTO_TEST_CASE(live_redis_resource_lifecycle) {
    /**
     * 全局 fixture 已保证 BBT_TEST_REDIS_ADDR 存在；缺环境进程已 exit(77)。
     */
    const auto [rhost, rport] = SplitAddr(EnvOr("BBT_TEST_REDIS_ADDR"));
    auto host = std::make_shared<NullNetHost>();
    std::shared_ptr<fw::CoApp> app;
    std::optional<fw::ComponentBorrow<fw::RedisNetResource>> cli;
    std::optional<fw::ComponentHandle<fw::RedisNetResource>> peek_h;
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        app = MakeApp(host);
        std::error_code addr_ec;
        auto address = inf::net::Address::Ip(rhost, rport, addr_ec);
        BOOST_REQUIRE_MESSAGE(!addr_ec,
                              "BBT_TEST_REDIS_ADDR 需为数值 IPv4/IPv6: " << addr_ec.message());

        /**
         * 新入口只接受数值 IP：loopback 地址经 net::Address::Ip 装配（不解析
         * 主机名）。组件装配只做 Engine Run + client 装配不拨号；显式
         * Connect 只允许协程上下文，由 PeekSvc 借出后在协程内执行（与
         * dual_service 的 OnStart Connect 同一契约）。
         */
        BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<fw::RedisNetResource>(
            "cache", fw::RedisNetResource::Options{}.SetAddress(address)));
        BOOST_REQUIRE(bbt_co_await app->CoInstallService<RedisSvc>(
            RedisSvcOpts()));
        BOOST_REQUIRE(bbt_co_await app->CoInstallService<RedisPeek>(
            ConcurrentOpts()));
        BOOST_REQUIRE(bbt_co_await app->CoStart());
        auto bh = BindRedisPeek(*app);
        BOOST_REQUIRE(bh);
        peek_h.emplace(std::move(bh).value());
        auto br = peek_h->Borrow();
        BOOST_REQUIRE(br);
        cli.emplace(std::move(br).value());
        /**
         * 只有 Connected 实例才允许后续命令面（ready 先于入站接纳）；
         * deadline 有界、无自动重试、无固定 sleep。
         */
        inf::CallOptions connect_options{Clock::now() + kWait};
        auto connected = bbt_co_await cli->Get().Connect(connect_options);
        /**
         * BOOST_REQUIRE_MESSAGE 的消息在断言通过时同样求值，ok result 读
         * error() 会抛 logic_error；失败细节只在失败分支取出。
         */
        if (!connected) {
            BOOST_FAIL("redis explicit Connect 失败: "
                       << connected.error().message);
        }
    }));
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

    /**
     * 每次运行唯一 key 前缀：真实后端是保留状态的实例，避免与上次运行冲突；
     * 清理只删本用例写入的键并以 EXISTS 读回证明。
     */
    const std::string key_prefix =
        "bbt:fw-live:" + std::to_string(Clock::now().time_since_epoch().count()) + ":";
    const std::string test_key = key_prefix + "k";

    /**
     * 真实往返：经 DispatchInboundForTest（V2 Request → Dispatcher →
     * RequestScope → handler → 组件借用 → Response）驱动 Ping/Set/Get。
     */
    auto rpc = [&app](const char* method, fw::CoRpcReq req)
        -> std::optional<fw::result<inf::rpc::Response>> {
        inf::rpc::Request env;
        env.SetService("cache")
            .SetMethod(method)
            .SetRequestId(std::string{"r-"} + method)
            .SetRequestSchema(std::string(fw::kCoRpcPositionalSchema))
            .SetResponseSchema(std::string(fw::kCoRpcPositionalSchema))
            .SetReply(inf::rpc::ReplyMode::Required)
            .SetTraffic(inf::rpc::TrafficClass::Service)
            .SetPayload(req.payload());
        return DispatchRpc(*app, [env](fw::CoApp& a) {
            return fw::DispatchInboundForTest(a, env);
        });
    };

    auto ping = rpc("ping", RequestOne("x"));
    if (ping.has_value() && !*ping)
        std::fprintf(stderr, "ping error: %s code=%d\n",
                     (*ping).error().message.c_str(),
                     static_cast<int>((*ping).error().code));
    BOOST_REQUIRE(ping.has_value());
    BOOST_REQUIRE(*ping);
    auto p = DecodeStrReply(fw::CoRpcResp::FromPayload((*ping).value().Payload()));
    BOOST_REQUIRE(p);
    BOOST_CHECK_EQUAL(std::get<1>(p.value()), "PONG");

    auto set = rpc("set", RequestStr(test_key, "v-42"));
    BOOST_REQUIRE(set.has_value());
    BOOST_REQUIRE(*set);
    auto s = DecodeStrReply(fw::CoRpcResp::FromPayload((*set).value().Payload()));
    BOOST_REQUIRE(s);
    BOOST_CHECK_EQUAL(std::get<1>(s.value()), "OK");

    auto get = rpc("get", RequestOne(test_key));
    BOOST_REQUIRE(get.has_value());
    BOOST_REQUIRE(*get);
    auto g = DecodeStrReply(fw::CoRpcResp::FromPayload((*get).value().Payload()));
    BOOST_REQUIRE(g);
    BOOST_CHECK_EQUAL(std::get<1>(g.value()), "v-42");

    /**
     * 清理本用例写入的唯一键并读回证明：Delete 后在同一资源上 EXISTS
     * 必须为 false。清理失败如实断言，不静默跳过。
     */
    {
        inf::CallOptions o{Clock::now() + kWait};
        auto done = DispatchRpc(*app, [&](fw::CoApp&) {
            return RunRedisCleanup(cli->Get(), test_key, o);
        });
        BOOST_REQUIRE_MESSAGE(done.has_value() && done.value(),
                              "live.redis: 唯一键清理/读回失败");
    }

    /**
     * 关闭收尾：借用未归还时 CoStop 落 StopIncomplete（borrow 排空是
     * 收口前提），owner/资源版本保留——IsClosed 仍为 false、同一
     * borrow 上的真实命令仍通（不提前收口的正面证据，即旧「资源仍
     * 活跃」断言）。归还后再次 CoStop 推进至 Stopped，组件销毁意味着
     * 同步 OnStop 已落定。
     */
    std::thread req([&app] { app->RequestShutdown(); });
    req.join();
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        bbt::coroutine::WaitOptions wait;
        wait.deadline = Clock::now() + std::chrono::seconds(10);
        BOOST_REQUIRE(bbt_co_await app->CoWaitShutdownRequest(wait));
        BOOST_REQUIRE(bbt_co_await app->CoStop(
            fw::LifecycleOptions{}.SetDeadline(
                Clock::now() + std::chrono::seconds(2))));
    }));
    BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);
    BOOST_CHECK(!cli->Get().IsClosed());
    BOOST_CHECK(!cli->Get().engine_stopped());
    /**
     * owner 保留窗口的真实命令仍通（资源未收口的正面证据）。
     */
    {
        inf::CallOptions o{Clock::now() + kWait};
        auto alive = DispatchRpc(*app, [&](fw::CoApp&) {
            return bbt_co_await cli->Get().Ping(o);
        });
        BOOST_REQUIRE_MESSAGE(alive.has_value() && alive.value(),
                              "StopIncomplete 期间真实 Ping 应成功（owner 保留）");
    }
    cli.reset();
    peek_h.reset();
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        BOOST_REQUIRE(bbt_co_await app->CoStop(
            fw::LifecycleOptions{}.SetDeadline(
                Clock::now() + std::chrono::seconds(5))));
    }));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    /**
     * 组件销毁：同步 OnStop（client Close + Engine 物理收口核验）已落定；
     * 销毁后 FindComponent 不再查到登记项。
     */
    auto gone = app->FindComponent<fw::RedisNetResource>("cache");
    BOOST_CHECK(!gone);
}
#endif

#ifdef BBT_LIVE_HAS_MONGO
/**
 * Mongo 清理：经兄弟句柄对**本用例独占的 collection** 做
 * DeleteMany + CountDocuments 读回证明（命名空间隔离：独占库
 * bbt_f8_fw_live_<pid>、独占集合 docs_<ts>，每轮一个一次性集合，
 * drop 等价为删除该集合内全部文档）。清理只在协程内执行，返回
 * 是否清空；失败如实断言，不静默跳过。
 */
inline bool RunMongoCleanup(
    fw::MongoDbResource& res,
    const inf::mongo::MongoDocument& filter,
    const inf::CallOptions& o) {
    auto removed = bbt_co_await res.DeleteMany(filter, o);
    if (!removed)
        return false;
    auto remaining = bbt_co_await res.CountDocuments(filter, o);
    return remaining && remaining.value() == 0;
}

/**
 * L-M1：mongo owner/句柄组件装配 → handler 内经组件借用做真实
 * InsertOne/FindOne → 多文档面（InsertMany/Find/UpdateMany/DeleteMany/
 * CountDocuments）真实往返 → 借用持有时 CoStop 如实落 StopIncomplete
 * （owner/资源版本保留）→ 归还后再次 CoStop 至 Stopped；同 owner
 * 兄弟句柄在组件销毁（owner 同步物理收口）后命令如实失败（只接受
 * Closed，不放宽为任意错误）。
 * 入站同样经 DispatchInboundForTest；命名空间为独占临时集合，清理经
 * DeleteMany + CountDocuments 读回证明（等价 infra DpNsGuard 的 drop+读回，
 * 只清理自有命名空间）。
 */
BOOST_AUTO_TEST_CASE(live_mongo_resource_lifecycle) {
    /**
     * 全局 fixture 已保证 BBT_TEST_MONGO_URI 存在；缺环境进程已 exit(77)。
     */
    auto host = std::make_shared<NullNetHost>();
    std::shared_ptr<fw::CoApp> app;
    std::optional<fw::ComponentBorrow<fw::MongoDbResource>> res;
    std::optional<fw::ComponentHandle<fw::MongoDbResource>> peek_h;
    /** 跨关闭保活的普通强引用：有效期内持有、关闭后仍可直调组件公开 API */
    std::shared_ptr<fw::MongoDbResource> strong;

    /**
     * 独占命名空间：库名按 pid 区分、集合名按时间戳区分；不触碰共享
     * bbt_live/bbt_f8 等持久集合（等价 infra 数据面用例的独占临时库口径，
     * 这里按集合粒度独占）。
     */
    const std::string db_name =
        "bbt_f8_fw_live_" + std::to_string(::getpid());
    const std::string coll_name =
        "docs_" + std::to_string(
                      std::chrono::steady_clock::now().time_since_epoch().count());
    const std::string doc_id = coll_name + "-k1";

    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        app = MakeApp(host);
        inf::mongo::MongoRuntimeConfig rcfg;
        rcfg.uri = EnvOr("BBT_TEST_MONGO_URI");
        rcfg.worker_threads = 2;
        rcfg.max_queue = 256;
        rcfg.server_selection_timeout = std::chrono::milliseconds{5000};
        rcfg.connect_timeout = std::chrono::milliseconds{5000};
        rcfg.socket_timeout = std::chrono::milliseconds{5000};
        rcfg.wait_queue_timeout = std::chrono::milliseconds{5000};
        const inf::mongo::MongoTarget target{db_name, coll_name};
        BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<fw::MongoDbResource>(
            "docs", fw::MongoDbResource::Options{}
                        .SetConfig(rcfg)
                        .SetTarget(target)));
        BOOST_REQUIRE(bbt_co_await app->CoInstallService<DocSvc>(DocSvcOpts()));
        BOOST_REQUIRE(bbt_co_await app->CoInstallService<MongoPeek>(
            ConcurrentOpts()));
        BOOST_REQUIRE(bbt_co_await app->CoStart());
        auto bh = BindMongoPeek(*app);
        BOOST_REQUIRE(bh);
        peek_h.emplace(std::move(bh).value());
        auto br = peek_h->Borrow();
        BOOST_REQUIRE(br);
        res.emplace(std::move(br).value());
        /**
         * 有效期内经 peek_h->lock() 持有普通强引用，并显式断言非空；
         * 该引用保活组件本体，供归还 Borrow 与两阶段 CoStop 之后
         * 直调组件公开 API。
         */
        strong = peek_h->lock();
        BOOST_REQUIRE(static_cast<bool>(strong));
    }));
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

    auto rpc = [&app](const char* method, fw::CoRpcReq req)
        -> std::optional<fw::result<inf::rpc::Response>> {
        inf::rpc::Request env;
        env.SetService("docs")
            .SetMethod(method)
            .SetRequestId(std::string{"m-"} + method)
            .SetRequestSchema(std::string(fw::kCoRpcPositionalSchema))
            .SetResponseSchema(std::string(fw::kCoRpcPositionalSchema))
            .SetReply(inf::rpc::ReplyMode::Required)
            .SetTraffic(inf::rpc::TrafficClass::Service)
            .SetPayload(req.payload());
        return DispatchRpc(*app, [env](fw::CoApp& a) {
            return fw::DispatchInboundForTest(a, env);
        });
    };

    auto put = rpc("put", RequestStr(doc_id, "42"));
    BOOST_REQUIRE(put.has_value());
    BOOST_REQUIRE(*put);
    auto p = DecodeStrReply(fw::CoRpcResp::FromPayload((*put).value().Payload()));
    if (!p)
        BOOST_TEST_MESSAGE("put error: " << p.error().message
                                         << " code=" << static_cast<int>(p.error().code));
    BOOST_REQUIRE(p);
    BOOST_CHECK_EQUAL(std::get<1>(p.value()), "INS");

    auto get = rpc("get", RequestOne(doc_id));
    BOOST_REQUIRE(get.has_value());
    BOOST_REQUIRE(*get);
    auto g = DecodeStrReply(fw::CoRpcResp::FromPayload((*get).value().Payload()));
    BOOST_REQUIRE(g);
    BOOST_CHECK_EQUAL(std::get<1>(g.value()), "HIT");

    /**
     * ── 多文档数据面真实往返（同资源、受管协程内）：InsertMany → Find →
     * UpdateMany → CountDocuments → DeleteMany。命名空间独占，谓词只
     * 覆盖本用例写入的 _id 前缀。
     */
    {
        inf::CallOptions o{Clock::now() + kWait};
        auto batch = DispatchRpc(*app, [&](fw::CoApp&) {
            std::vector<inf::mongo::MongoDocument> docs;
            docs.push_back(DocIdVal(coll_name + "-b1", 1));
            docs.push_back(DocIdVal(coll_name + "-b2", 2));
            docs.push_back(DocIdVal(coll_name + "-b3", 3));
            return bbt_co_await res->Get().InsertMany(docs, o);
        });
        BOOST_REQUIRE(batch.has_value());
        BOOST_REQUIRE(batch.value());
        BOOST_CHECK_EQUAL(batch.value().value().inserted, std::uint64_t{3});
    }
    {
        inf::CallOptions o{Clock::now() + kWait};
        auto found = DispatchRpc(*app, [&](fw::CoApp&) {
            inf::mongo::FindOptions fo;
            fo.limit = 10;
            return bbt_co_await res->Get().Find(
                DocId(coll_name + "-b1"), fo, o);
        });
        BOOST_REQUIRE(found.has_value());
        BOOST_REQUIRE(found.value());
        BOOST_CHECK_EQUAL(found.value().value().size(), std::size_t{1});
    }
    {
        inf::CallOptions o{Clock::now() + kWait};
        /**
         * UpdateMany 用显式前缀谓词（空 filter 一律 InvalidArgument）与
         * $set 更新算子文档。
         */
        auto updated = DispatchRpc(*app, [&](fw::CoApp&) {
            return bbt_co_await res->Get().UpdateMany(
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
            return bbt_co_await res->Get().CountDocuments(
                DocId(coll_name + "-b3"), o);
        });
        BOOST_REQUIRE(count.has_value());
        BOOST_REQUIRE(count.value());
        BOOST_CHECK_EQUAL(count.value().value(), std::uint64_t{1});
    }

    /**
     * 清理本用例独占集合内的全部文档并读回证明：DeleteMany 以显式谓词
     * {$exists:true} 形态匹配本集合所有文档（集合本身独占——不会误伤其他
     * 集合/库；空 filter 拒绝语义不受影响）。读回 0 才算清理完成。
     */
    {
        inf::CallOptions o{Clock::now() + kWait};
        /**
         * {"_id":{"$exists":true}}：$exists 的值是**完整嵌入文档**
         * （len+elems+0x00），不是裸元素字节——与 DocSetV 同一形态。
         */
        std::vector<std::uint8_t> inner;
        /**
         * bool
         */
        inner.push_back(0x08);
        BsonPutCStr(inner, "$exists");
        /**
         * true
         */
        inner.push_back(0x01);
        std::vector<std::uint8_t> subdoc;
        BsonPutI32(subdoc, static_cast<std::int32_t>(inner.size() + 5));
        subdoc.insert(subdoc.end(), inner.begin(), inner.end());
        subdoc.push_back(0x00);
        std::vector<std::uint8_t> body;
        /**
         * embedded document
         */
        body.push_back(0x03);
        BsonPutCStr(body, "_id");
        body.insert(body.end(), subdoc.begin(), subdoc.end());
        inf::mongo::MongoDocument match_all;
        BsonPutI32(match_all.bytes,
                   static_cast<std::int32_t>(body.size() + 5));
        match_all.bytes.insert(match_all.bytes.end(),
                               body.begin(), body.end());
        match_all.bytes.push_back(0x00);
        auto done = DispatchRpc(*app, [&](fw::CoApp&) {
            return RunMongoCleanup(res->Get(), match_all, o);
        });
        BOOST_REQUIRE_MESSAGE(done.has_value() && done.value(),
                              "live.mongo: 独占集合清理/读回失败");
    }

    /**
     * owner-handle close 证据：同 owner 的兄弟句柄在组件销毁后如实失败
     * （Closed）——句柄不新建线程、共享 owner worker，owner 收口即兄弟
     * 句柄不再可用。兄弟句柄为测试持有的 shared_ptr，资源销毁后仍安全。
     */
    std::shared_ptr<inf::mongo::CoMongoColl> coll;
    {
        auto sib = res->Get().SiblingCollection(
            {db_name, "docs_sibling_probe"});
        /**
         * owner Running 时可建
         */
        BOOST_REQUIRE(sib);
        coll = std::move(sib).value();
    }

    /**
     * 关闭收尾：借用未归还时 CoStop 落 StopIncomplete（owner/资源版本
     * 保留，不提前收口），归还后再次 CoStop 推进至 Stopped；同 owner
     * 兄弟句柄在 owner 同步物理收口（OnStop 的 owner Close）后命令
     * 如实失败——不放宽为任意错误：只接受 Closed。
     */
    std::thread req([&app] { app->RequestShutdown(); });
    req.join();
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        bbt::coroutine::WaitOptions wait;
        wait.deadline = Clock::now() + std::chrono::seconds(10);
        BOOST_REQUIRE(bbt_co_await app->CoWaitShutdownRequest(wait));
        BOOST_REQUIRE(bbt_co_await app->CoStop(
            fw::LifecycleOptions{}.SetDeadline(
                Clock::now() + std::chrono::seconds(2))));
    }));
    BOOST_CHECK(app->State() == fw::AppState::StopIncomplete);
    BOOST_CHECK(!res->Get().IsClosed());
    BOOST_CHECK(!res->Get().coll_closed());
    res.reset();
    peek_h.reset();
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        BOOST_REQUIRE(bbt_co_await app->CoStop(
            fw::LifecycleOptions{}.SetDeadline(
                Clock::now() + std::chrono::seconds(5))));
    }));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
    /**
     * 组件销毁：同步 OnStop（集合句柄门禁 + owner Close 物理收口）已
     * 落定；兄弟句柄命令如实失败（Closed），DispatchRpc 层异常/超时
     * 同样判失败。
     */
    {
        inf::CallOptions o{Clock::now() + kWait};
        auto after = DispatchRpc(*app, [&](fw::CoApp&) {
            return bbt_co_await coll->FindOne(DocId("x"), o);
        });
        BOOST_REQUIRE(after.has_value());
        BOOST_REQUIRE_MESSAGE(!after.value(),
                              "owner Close 后兄弟句柄命令应失败（Closed），实际成功");
        BOOST_CHECK_EQUAL(
            static_cast<int>(after.value().error().code),
            static_cast<int>(inf::ErrorCode::Closed));
    }
    /**
     * 经 peek_h->lock() 持有的强引用直调组件公开 FindOne：不走
     * DispatchRpc，故返回 Closed 即组件自身终态，排除 harness 层异常；
     * 只接受 Closed，不放宽为任意错误。
     */
    {
        inf::CallOptions o{Clock::now() + kWait};
        std::optional<fw::result<std::optional<inf::mongo::MongoDocument>>>
            held;
        BOOST_REQUIRE(RunInManagedCoroutine([&] {
            held.emplace(bbt_co_await strong->FindOne(DocId("x"), o));
        }));
        BOOST_REQUIRE(held);
        BOOST_REQUIRE_MESSAGE(!*held,
                              "强引用直调关闭后组件公开 FindOne 应失败，实际成功");
        BOOST_CHECK_EQUAL(
            static_cast<int>((*held).error().code),
            static_cast<int>(inf::ErrorCode::Closed));
    }
}
#endif

/**
 * L-D1：派发超预算 + 在途回调寿命。DispatchRpc 超预算提前返回时在途协程仍
 * pending（可观察）；收尾必须等它真实结束才 join，App 不得在其回调仍可能运行
 * 时被销毁。真实对象寿命证据：DrainDispatch 只有在 g_dispatch_pending 归零后
 * 才返回 true（随后才进入 CoStop），且回调确实跑完（ran==true），
 * 非「无信号即安全」。
 *
 * 寿命边界（刻意留白，见 L-D2）：本用例的回调体只等自身 CoWaiter 落定并置位
 * 标志，**不触达 App/Service/资源状态**——因此它在 RequestShutdown 之后仍可能
 * 在途，也不会与 CoStop 的 _StopAll 收口竞争任何 Service 状态。
 * 若要让访问 Service 的回调跨越 RequestShutdown 仍安全，需要核心侧支持
 * （drain 覆盖对象销毁，或用 shared owner 保活 Service 访问），
 * 属本轮授权之外，本测试不伪造该保证。
 */
BOOST_AUTO_TEST_CASE(live_dispatch_drain_outlives_budget) {
    auto host = std::make_shared<NullNetHost>();
    std::shared_ptr<fw::CoApp> app;
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        app = MakeApp(host);
        BOOST_REQUIRE(bbt_co_await app->CoStart());
    }));
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));

    std::atomic<bool> ran{false};
    /**
     * 协程内等到自身 deadline 才返回（无 sleep，CoWaiter 在 deadline 处落定）。
     */
    auto out = DispatchRpc(*app, [&ran](fw::CoApp&) -> int {
        auto w = co::sync::CoWaiter::Create();
        co::WaitOptions wo;
        wo.deadline = Clock::now() + std::chrono::milliseconds{600};
        (void)w->Wait(wo);
        ran.store(true, std::memory_order_release);
        return 7; }, std::chrono::milliseconds{100});
    /**
     * 超预算提前返回
     */
    BOOST_CHECK(!out.has_value());
    BOOST_CHECK(g_dispatch_pending.load(std::memory_order_acquire) > 0);

    app->RequestShutdown();
    /**
     * 在途归零后才允许收口
     */
    BOOST_REQUIRE(DrainDispatch(kWait));
    /**
     * 回调真实完成
     */
    BOOST_CHECK(ran.load(std::memory_order_acquire));
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        bbt::coroutine::WaitOptions wait;
        wait.deadline = Clock::now() + std::chrono::seconds(10);
        BOOST_REQUIRE(bbt_co_await app->CoWaitShutdownRequest(wait));
        BOOST_REQUIRE(bbt_co_await app->CoStop());
    }));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);
}

#ifdef BBT_LIVE_HAS_REDIS
/**
 * L-D2：Service 寿命边界（确定性次序；关闭后入口拒绝 + Service 实例已释放）。
 * 测试侧最小寿命协议：访问 Service/资源的派发回调必须在 RequestShutdown
 * **之前**真实完成——此时 FindService 句柄仍有效。测试**不保留 Service
 * 实例强引用**（新模型无公开 owner 出口）：只用外部观察状态（RedisSvc 的
 * s_pings_observer：宿主安装、handler 自增的 shared_ptr<atomic<int>>）与
 * 非拥有 ObjectHandle 观察——即使 Service 实例析构，宿主仍能读到计数。
 * 关闭后 Registry 项与分发表项一并摘除：同一入站被确定性拒绝
 * （RuntimeUnavailable）、handler 计数不再增长、句柄 IsValid=false
 * （对象已销毁，无其它强引用）。
 * 说明：本用例不执行真实后端命令（资源缺席即如实返回错误）；它证明的是寿命
 * 次序与关闭后入口拒绝，真实后端往返证据在 L-R1/L-M1。
 */
BOOST_AUTO_TEST_CASE(live_service_lifetime_bounded_by_stop) {
    auto host = std::make_shared<NullNetHost>();
    std::shared_ptr<fw::CoApp> app;
    fw::ObjectHandle svc_h;
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        app = MakeApp(host);
        /**
         * 本用例不安装资源：handler 的 Res() 如实失败回 Unavailable
         * （资源缺席即错误），断言不依赖真实后端。
         */
        BOOST_REQUIRE(bbt_co_await app->CoInstallService<RedisSvc>(
            ConcurrentOpts()));
        BOOST_REQUIRE(bbt_co_await app->CoStart());
        auto h = app->FindService("cache");
        BOOST_REQUIRE(h);
        svc_h = h.value();
    }));
    BOOST_REQUIRE(host->start_seen.WaitFor(kWait));
    BOOST_CHECK(app->State() == fw::AppState::Running);

    /**
     * 安装外部观察状态（新模型无 Service 实例访问路径，经静态成员注入；
     * 用例自身持有 shared_ptr 即保活，无需保留 Service 引用）。
     */
    auto observer = std::make_shared<std::atomic<int>>(0);
    RedisSvc::s_pings_observer = observer;

    auto dispatch = [&app] {
        inf::rpc::Request env;
        env.SetService("cache")
            .SetMethod("ping")
            .SetRequestId("l-d2")
            .SetRequestSchema(std::string(fw::kCoRpcPositionalSchema))
            .SetResponseSchema(std::string(fw::kCoRpcPositionalSchema))
            .SetReply(inf::rpc::ReplyMode::Required)
            .SetTraffic(inf::rpc::TrafficClass::Service)
            .SetPayload(RequestOne("x").payload());
        return DispatchRpc(*app, [env](fw::CoApp& a) {
            return fw::DispatchInboundForTest(a, env);
        });
    };

    /**
     * (1) 运行期：派发回调经真实 Dispatcher 进入 Service handler 并真实结束
     * （观察者 +1）；此刻对象句柄仍有效。
     */
    auto during = dispatch();
    /**
     * 回调已真实返回
     */
    BOOST_REQUIRE(during.has_value());
    BOOST_CHECK_EQUAL(g_dispatch_pending.load(std::memory_order_acquire), 0);
    BOOST_CHECK_EQUAL(observer->load(std::memory_order_acquire), 1);
    /**
     * Service 仍登记
     */
    BOOST_CHECK(svc_h.IsValid());

    /**
     * (2) 关闭：归零后才收口；句柄在 CoStop 前持续有效。
     */
    app->RequestShutdown();
    BOOST_REQUIRE(DrainDispatch(kWait));
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        bbt::coroutine::WaitOptions wait;
        wait.deadline = Clock::now() + std::chrono::seconds(10);
        BOOST_REQUIRE(bbt_co_await app->CoWaitShutdownRequest(wait));
        BOOST_REQUIRE(bbt_co_await app->CoStop());
    }));
    BOOST_CHECK(app->State() == fw::AppState::Stopped);

    /**
     * (3) 关闭后：入站被确定性拒绝、handler 不再被进入；测试未持
     * owner，Service 条目已销毁（句柄失效）。
     */
    auto released = app->FindService("cache");
    BOOST_REQUIRE(!released);
    auto after = dispatch();
    /**
     * 派发回调仍能返回（无悬挂）
     */
    BOOST_REQUIRE(after.has_value());
    BOOST_REQUIRE(!*after);
    BOOST_CHECK_EQUAL(static_cast<int>((*after).error().code),
                      static_cast<int>(fw::ErrorCode::RuntimeUnavailable));
    /**
     * handler 未被触达
     */
    BOOST_CHECK_EQUAL(observer->load(std::memory_order_acquire), 1);
    /**
     * Service 实例已释放（条目销毁 → 句柄失效）
     */
    BOOST_CHECK(!svc_h.IsValid());
    RedisSvc::s_pings_observer.reset();
}
#endif

BOOST_AUTO_TEST_SUITE_END()
} // namespace
