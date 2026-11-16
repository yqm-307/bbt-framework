/**
 * CoRpc 公共面、声明式方法表、错误边界、actor key、路由与组件绑定缝。
 * 不联网、不起真实 I/O；受管对象断言运行在真实受管协程
 * （业务受管类只能经 CoApp/Registry 创建，构造期即要求协程上下文）。
 */
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/Framework.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/MethodTable.hpp>
#include <bbt/framework/internal/NetworkHost.hpp>
#include <bbt/framework/internal/RequestScope.hpp>

namespace fw = bbt::framework;

namespace {

struct PlayerRouteArg { std::string player_id; };

} // namespace

template <>
struct bbt::framework::RouteTraits<PlayerRouteArg> {
    static bbt::framework::result<bbt::framework::RouteFields>
    Encode(const PlayerRouteArg& arg) {
        bbt::framework::RouteFields fields;
        fields.values.emplace_back("player", arg.player_id);
        return bbt::framework::result<bbt::framework::RouteFields>::ok(
            std::move(fields));
    }
};

namespace {

template <class... Ts>
fw::CoRpcReq Request(Ts&&... values) {
    auto req = fw::CoRpcReq::From(
        std::tuple<std::decay_t<Ts>...>{std::forward<Ts>(values)...});
    if (!req)
        throw std::logic_error("test request encoding failed");
    return std::move(req).value();
}

/**
 * 旧「命名资源」的对应物：App scope 受管组件，按逻辑名安装/绑定。
 */
class DummyComponent final : public fw::CoComponent {
public:
    struct Options { int n = 0; };
    explicit DummyComponent(fw::detail::ConstructKey k, Options o)
        : fw::CoComponent(k), n_(o.n) {}
    int Value() const noexcept { return n_; }
protected:
    ~DummyComponent() noexcept override = default;
private:
    int n_;
    friend class fw::detail::Registry;
};

class EchoService final : public fw::CoService<DummyComponent> {
public:
    static constexpr std::string_view kServiceName = "echo";

    explicit EchoService(fw::detail::ConstructKey k)
        : fw::CoService<DummyComponent>(k) {}

    fw::CoRpcResp Echo(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        return fw::CoRpcResp::From("echo:" + args.value());
    }
    fw::CoRpcResp Reset(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        return fw::CoRpcResp::From(std::tuple<>{});
    }
    fw::CoRpcResp Notify(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        m_last_notify = args.value();
        return fw::CoRpcResp::From(std::tuple<>{});
    }
    fw::CoRpcResp Fail(fw::CoRpcReq) {
        return fw::CoRpcResp::Error(
            fw::MakeError(fw::ErrorCode::InvalidArgument, "rejected"));
    }
    fw::CoRpcResp Boom(fw::CoRpcReq) {
        throw std::runtime_error("boom");
    }
    fw::CoRpcResp WhoAmI(fw::CoRpcReq req) {
        auto args = req.Parse<std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        return fw::CoRpcResp::From("player:" + args.value());
    }

    fw::result<fw::CoRpcResp> CallOut(fw::CoRpcReq req) {
        fw::CallOptions opts;
        opts.deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(30);
        return bbt_co_await call("other-service", "get", req, {}, opts);
    }

    const std::string& last_notify() const { return m_last_notify; }

    /**
     * 旧 context().resource<R>(name) 的对应物：声明式组件绑定 + Borrow。
     * 调用方必须在受管协程内（GetComponent/Borrow 的协程上下文要求）。
     */
    template <class R>
    fw::result<fw::ComponentBorrow<R>> Bound(std::string_view name) {
        auto h = GetComponent<R>(name);
        if (!h) return fw::result<fw::ComponentBorrow<R>>::err(
            std::move(h.error()));
        return h.value().Borrow();
    }

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EchoService::Echo>("echo"),
        fw::Method<&EchoService::Reset>("reset"),
        fw::Method<&EchoService::Notify>("notify"),
        fw::Method<&EchoService::Fail>("fail"),
        fw::Method<&EchoService::Boom>("boom"),
        fw::ActorMethodAt<&EchoService::WhoAmI, std::string>("who-am-i"));

protected:
    ~EchoService() noexcept override = default;

private:
    std::string m_last_notify;
    friend class fw::detail::Registry;
};

class DupService final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "dup";
    explicit DupService(fw::detail::ConstructKey k) : fw::CoService<>(k) {}
    fw::CoRpcResp A(fw::CoRpcReq) {
        return fw::CoRpcResp::From(std::tuple<>{});
    }
    fw::CoRpcResp B(fw::CoRpcReq) {
        return fw::CoRpcResp::From(std::tuple<>{});
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&DupService::A>("same"),
        fw::Method<&DupService::B>("same"));
protected:
    ~DupService() noexcept override = default;
    friend class fw::detail::Registry;
};

class EmptyNameService final : public fw::CoService<> {
public:
    static constexpr std::string_view kServiceName = "e";
    explicit EmptyNameService(fw::detail::ConstructKey k)
        : fw::CoService<>(k) {}
    fw::CoRpcResp A(fw::CoRpcReq) {
        return fw::CoRpcResp::From(std::tuple<>{});
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EmptyNameService::A>(""));
protected:
    ~EmptyNameService() noexcept override = default;
    friend class fw::detail::Registry;
};

using EchoTraits = fw::rpc_traits<decltype(&EchoService::Echo)>;
static_assert(std::is_same_v<EchoTraits::service_type, EchoService>);
static_assert(std::is_same_v<EchoTraits::request_type, fw::CoRpcReq>);
static_assert(std::is_same_v<EchoTraits::reply_type, fw::CoRpcResp>);
static_assert(EchoTraits::kCoRpc);
static_assert(fw::has_rpc_methods_v<EchoService>);
static_assert(!fw::has_rpc_methods_v<DummyComponent>);

/**
 * 不接纳请求也不启动的宿主桩：受管对象断言不需要真实 I/O。
 */
class StubNetHost final : public fw::INetworkHost {
public:
    fw::result<void> Create() override { return fw::result<void>::ok(); }
    fw::result<void> Start() override { return fw::result<void>::ok(); }
    void StopAccepting() noexcept override {}
    fw::result<void> WaitHandlersDone(bbt::coroutine::Deadline) override {
        return fw::result<void>::ok();
    }
    void Close() noexcept override {}
};

fw::AppOptions AppOpts() {
    return fw::AppOptions{}
        .SetLimits(64, 64, 64, 64)
        .SetShutdownBudget(std::chrono::milliseconds{2000})
        .SetLifecycleBudget(std::chrono::milliseconds{2000})
        .SetNetworkLimits(bbt::infra::NetworkLimits{
            64, 64, 16384, 65536, std::chrono::milliseconds{30000}})
        .SetLocalIdentity(std::string{"rpc-surface-test"});
}

/**
 * 装配根：测试缝注入桩宿主、不给出站发送（seam_send 为空 → 已解析
 * 路由的出站如实 RuntimeUnavailable）。
 */
std::shared_ptr<fw::CoApp> MakeApp(fw::AppOptions options = AppOpts()) {
    return fw::MakeCoAppForTest(
        std::move(options),
        fw::CoAppSeam{std::make_shared<StubNetHost>(), {}});
}

/**
 * 取已安装服务的受管实例（锁内取 owner+指针，保活纪律与宿主一致；
 * 返回的 owner 由调用方持有，invoke 调用期对象不可销毁）。
 */
template <class S>
S* ManagedService(fw::CoApp& app, std::shared_ptr<void>& owner) {
    const auto reg = app.RegistryForTest();
    std::lock_guard<std::mutex> lk(reg->Mutex());
    auto* e = reg->FindByTypeNameLocked(
        fw::ObjectKind::Service, S::kServiceName);
    if (!e) return nullptr;
    owner = e->object;
    return static_cast<S*>(e->object.get());
}

/**
 * 在受管协程内执行 fn：经 g_scheduler 投递协程任务，宿主线程用
 * 限时 std::cv 等其完成（宿主线程的 CV 等待不违反 worker 阻塞禁令）。
 * 返回 false 表示任务未在预算内完成（防挂死测试进程）。
 */
template <class F>
bool RunInManagedCoroutine(F&& fn,
    std::chrono::milliseconds budget = std::chrono::milliseconds{5000}) {
    if (bbt::coroutine::IsInCoroutineContext()) { fn(); return true; }
    /**
     * native 测试宿主负责初始化唯一 runtime，产品 App 不启动 runtime。
     */
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 3;
        g_scheduler->Start();
    }
    struct Slot {
        std::mutex              mtx;
        std::condition_variable cv;
        bool                    done = false;
        std::exception_ptr      error;
    };
    auto slot = std::make_shared<Slot>();
    auto f = std::forward<F>(fn);
    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [slot, f = std::move(f)]() mutable {
            try { f(); } catch (...) { slot->error = std::current_exception(); }
            std::lock_guard<std::mutex> lk(slot->mtx);
            slot->done = true;
            slot->cv.notify_all();
        }, submitted);
    if (!submitted) throw std::runtime_error("test coroutine submission rejected");
    std::unique_lock<std::mutex> lk(slot->mtx);
    const bool done = slot->cv.wait_for(lk, budget, [&] { return slot->done; });
    /**
     * 不让仍在途任务使用已退出的测试栈。
     */
    if (!done)
        std::terminate();
    if (slot->error) std::rethrow_exception(slot->error);
    return done;
}

#define CO_TEST_CASE(name) \
    static void name##_body(); \
    BOOST_AUTO_TEST_CASE(name) { BOOST_REQUIRE(RunInManagedCoroutine(name##_body)); } \
    static void name##_body()

BOOST_AUTO_TEST_SUITE(framework_f0)

BOOST_AUTO_TEST_CASE(method_table_binding) {
    const auto table = fw::BuildMethodTable<EchoService>();
    BOOST_TEST(table.size() == 6);

    const fw::RpcMethod* echo = table.find("echo");
    BOOST_REQUIRE(echo != nullptr);
    BOOST_TEST(echo->request_schema == std::string(fw::kCoRpcPositionalSchema));
    BOOST_TEST(echo->response_schema == std::string(fw::kCoRpcPositionalSchema));
    BOOST_TEST(!echo->actor_keyed);
    BOOST_CHECK(echo->extract_key == nullptr);

    const fw::RpcMethod* who = table.find("who-am-i");
    BOOST_REQUIRE(who != nullptr);
    BOOST_TEST(who->actor_keyed);
    BOOST_CHECK(who->extract_key != nullptr);
    BOOST_TEST(who->request_schema == std::string(fw::kCoRpcPositionalSchema));
    BOOST_TEST(table.find("no-such-method") == nullptr);
}

CO_TEST_CASE(dispatch_roundtrip) {
    auto app = MakeApp();
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(8)));
    std::shared_ptr<void> owner;
    auto* svc = ManagedService<EchoService>(*app, owner);
    BOOST_REQUIRE(svc != nullptr);

    const auto table = fw::BuildMethodTable<EchoService>();
    auto out = table.find("echo")->invoke(
        *svc, Request(std::string{"hi"}).payload());
    BOOST_REQUIRE(out);
    auto reply = fw::CoRpcReq(std::move(out.value())).Parse<std::string>();
    BOOST_REQUIRE(reply);
    BOOST_TEST(reply.value() == "echo:hi");
}

CO_TEST_CASE(empty_success_and_side_effect) {
    auto app = MakeApp();
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(8)));
    std::shared_ptr<void> owner;
    auto* svc = ManagedService<EchoService>(*app, owner);
    BOOST_REQUIRE(svc != nullptr);

    const auto table = fw::BuildMethodTable<EchoService>();
    auto notify = table.find("notify")->invoke(
        *svc, Request(std::string{"ping"}).payload());
    BOOST_REQUIRE(notify);
    BOOST_TEST(notify.value() == std::vector<std::uint8_t>({0, 0, 0, 0}));
    BOOST_TEST(svc->last_notify() == "ping");

    auto reset = table.find("reset")->invoke(
        *svc, Request(std::string{"x"}).payload());
    BOOST_REQUIRE(reset);
    BOOST_TEST(reset.value() == std::vector<std::uint8_t>({0, 0, 0, 0}));
}

CO_TEST_CASE(error_boundaries) {
    auto app = MakeApp();
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(8)));
    std::shared_ptr<void> owner;
    auto* svc = ManagedService<EchoService>(*app, owner);
    BOOST_REQUIRE(svc != nullptr);

    const auto table = fw::BuildMethodTable<EchoService>();
    auto business = table.find("fail")->invoke(
        *svc, Request(std::string{"x"}).payload());
    BOOST_REQUIRE(!business);
    BOOST_CHECK(business.error().code == fw::ErrorCode::InvalidArgument);
    BOOST_TEST(business.error().message == "rejected");

    auto thrown = table.find("boom")->invoke(
        *svc, Request(std::string{"x"}).payload());
    BOOST_REQUIRE(!thrown);
    BOOST_CHECK(thrown.error().code == fw::ErrorCode::InternalError);

    /**
     * 第二个独立实例：malformed 路径不得污染前一实例的可变状态。
     */
    auto app2 = MakeApp();
    BOOST_REQUIRE(bbt_co_await app2->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(8)));
    std::shared_ptr<void> owner2;
    auto* untouched = ManagedService<EchoService>(*app2, owner2);
    BOOST_REQUIRE(untouched != nullptr);
    auto malformed = table.find("notify")->invoke(*untouched, {});
    BOOST_REQUIRE(!malformed);
    BOOST_CHECK(malformed.error().code == fw::ErrorCode::ProtocolError);
    BOOST_TEST(untouched->last_notify().empty());
}

BOOST_AUTO_TEST_CASE(actor_key_extraction) {
    const auto table = fw::BuildMethodTable<EchoService>();
    const fw::RpcMethod* who = table.find("who-am-i");
    BOOST_REQUIRE(who != nullptr);

    auto good = who->extract_key(Request(std::string{"p-42"}).payload());
    BOOST_REQUIRE(good);
    BOOST_TEST(good.value() == "p-42");

    auto bad = who->extract_key(Request(std::string{}).payload());
    BOOST_REQUIRE(!bad);
    BOOST_CHECK(bad.error().code == fw::ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_CASE(duplicate_method_rejected) {
    BOOST_CHECK_THROW(fw::BuildMethodTable<DupService>(), std::invalid_argument);
    BOOST_CHECK_THROW(fw::BuildMethodTable<EmptyNameService>(),
                      std::invalid_argument);
}

CO_TEST_CASE(service_identity_and_egress_gate) {
    /**
     * 旧 unhosted_identity_and_call 的对应物：受管安装的服务持有
     * Registry 发放的稳定身份；出站调用在受管请求上下文内经路由门
     * 解析后由 seam_send 缺位如实拒绝（RuntimeUnavailable）。
     */
    auto app = MakeApp(
        AppOpts().AddRoute("other-service", "127.0.0.1:9"));
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(8)));
    std::shared_ptr<void> owner;
    auto* svc = ManagedService<EchoService>(*app, owner);
    BOOST_REQUIRE(svc != nullptr);

    const auto info = svc->Info();
    BOOST_TEST(!info.id.empty());
    BOOST_TEST(info.name == "echo");
    BOOST_CHECK(info.kind == fw::ObjectKind::Service);
    BOOST_CHECK(info.state == fw::ObjectState::Installed);

    /**
     * 无受管请求上下文 → InvalidContext（启动代码由宿主显式建立
     * RequestScope，业务侧不伪造）。
     */
    auto no_ctx = svc->CallOut(Request(std::string{"q"}));
    BOOST_REQUIRE(!no_ctx);
    BOOST_CHECK(no_ctx.error().code == fw::ErrorCode::InvalidContext);

    fw::RequestContext ctx;
    ctx.request_id = "rpc-surface-call";
    {
        fw::RequestScope scope(
            std::make_shared<const fw::RequestContext>(ctx));
        auto result = svc->CallOut(Request(std::string{"q"}));
        BOOST_REQUIRE(!result);
        BOOST_CHECK(result.error().code == fw::ErrorCode::RuntimeUnavailable);
    }
}

/**
 * 非协程上下文消费 call：CallOut 内 bbt_co_await 在函数体执行前
 * 完成环境检查，native 线程抛 std::logic_error（AwaitHelper 前置协议），
 * 不触达路由门或发送链。
 */
BOOST_AUTO_TEST_CASE(native_thread_call_await_throws_logic_error) {
    std::shared_ptr<fw::CoApp> app;
    std::shared_ptr<void> owner;
    EchoService* svc = nullptr;
    BOOST_REQUIRE(RunInManagedCoroutine([&] {
        app = MakeApp(AppOpts().AddRoute("other-service", "127.0.0.1:9"));
        BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
            fw::ServiceOptions{}.SetMaxInflight(8)));
        svc = ManagedService<EchoService>(*app, owner);
    }));
    BOOST_REQUIRE(svc != nullptr);
    BOOST_CHECK_THROW(svc->CallOut(Request(std::string{"q"})),
                      std::logic_error);
}

BOOST_AUTO_TEST_CASE(route_fields_and_traits) {
    auto fields = fw::RouteTraits<PlayerRouteArg>::Encode(PlayerRouteArg{"p7"});
    BOOST_REQUIRE(fields);
    BOOST_REQUIRE(fields.value().values.size() == 1);
    BOOST_TEST(fields.value().values[0].first == "player");
    BOOST_TEST(fields.value().values[0].second == "p7");

    fw::RpcRoute route{"echo", "echo", fields.value()};
    BOOST_TEST(route.service_name == "echo");
    BOOST_TEST(route.method_name == "echo");
    BOOST_TEST(route.custom.values.size() == 1);
}

BOOST_AUTO_TEST_CASE(call_options_defaults) {
    fw::CallOptions options;
    BOOST_TEST(!options.deadline.has_value());
    BOOST_TEST(!options.ordered.has_value());
}

BOOST_AUTO_TEST_CASE(positional_roundtrip_all_scalar_types) {
    const std::vector<std::uint8_t> bytes{0, 1, 2, 250, 255};
    auto request = fw::CoRpcReq::From(std::tuple{
        true, std::int8_t{-7}, std::int32_t{-123456},
        std::uint64_t{0xFEDCBA9876543210ULL}, 3.25,
        std::string{"位置参数"}, bytes});
    BOOST_REQUIRE(request);

    auto decoded = request.value().Parse<
        bool, std::int8_t, std::int32_t, std::uint64_t,
        double, std::string, std::vector<std::uint8_t>>();
    BOOST_REQUIRE(decoded);
    const auto& [flag, tiny, n, big, ratio, name, blob] = decoded.value();
    BOOST_TEST(flag);
    BOOST_TEST(tiny == -7);
    BOOST_TEST(n == -123456);
    BOOST_TEST(big == 0xFEDCBA9876543210ULL);
    BOOST_TEST(ratio == 3.25);
    BOOST_TEST(name == "位置参数");
    BOOST_TEST(blob == bytes);
}

BOOST_AUTO_TEST_CASE(positional_decode_rejects_malformed) {
    auto request = fw::CoRpcReq::From(std::int32_t{7});
    BOOST_REQUIRE(request);

    auto trailing = request.value().payload();
    trailing.push_back(0xA5);
    auto trailing_result = fw::CoRpcReq(std::move(trailing)).Parse<std::int32_t>();
    BOOST_REQUIRE(!trailing_result);
    BOOST_CHECK(trailing_result.error().code == fw::ErrorCode::ProtocolError);

    auto truncated = request.value().payload();
    truncated.pop_back();
    auto truncated_result = fw::CoRpcReq(std::move(truncated)).Parse<std::int32_t>();
    BOOST_REQUIRE(!truncated_result);
    BOOST_CHECK(truncated_result.error().code == fw::ErrorCode::ProtocolError);

    auto mismatch = request.value().Parse<std::string>();
    BOOST_REQUIRE(!mismatch);
    BOOST_CHECK(mismatch.error().code == fw::ErrorCode::TypeMismatch);

    auto count = request.value().Parse<std::int32_t, std::int32_t>();
    BOOST_REQUIRE(!count);
    BOOST_CHECK(count.error().code == fw::ErrorCode::InvalidArgument);
}

CO_TEST_CASE(component_binding_unbound_and_registration) {
    auto app = MakeApp();
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(8)));
    std::shared_ptr<void> owner;
    auto* svc = ManagedService<EchoService>(*app, owner);
    BOOST_REQUIRE(svc != nullptr);

    /**
     * 未绑定名 → NotFound（声明集合外的类型由编译期/InvalidArgument
     * 拒绝，另由 actor_mailbox 覆盖）。
     */
    auto unbound = svc->Bound<DummyComponent>("missing");
    BOOST_REQUIRE(!unbound);
    BOOST_CHECK(unbound.error().code == fw::ErrorCode::NotFound);

    /**
     * 旧 add_resource 登记校验的对应物：受管组件同名重复登记 →
     * InvalidArgument；空名 → InvalidArgument。
     */
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<DummyComponent>(
        "res", DummyComponent::Options{7}));
    auto duplicate = bbt_co_await app->CoInstallComponent<DummyComponent>(
        "res", DummyComponent::Options{9});
    BOOST_REQUIRE(!duplicate);
    BOOST_CHECK(duplicate.error().code == fw::ErrorCode::InvalidArgument);
    auto empty_name = bbt_co_await app->CoInstallComponent<DummyComponent>(
        "", DummyComponent::Options{1});
    BOOST_REQUIRE(!empty_name);
    BOOST_CHECK(empty_name.error().code == fw::ErrorCode::InvalidArgument);
}

CO_TEST_CASE(named_component_registration_and_lookup) {
    auto app = MakeApp();
    BOOST_REQUIRE(bbt_co_await app->CoInstallService<EchoService>(
        fw::ServiceOptions{}.SetMaxInflight(8)));
    std::shared_ptr<void> owner;
    auto* svc = ManagedService<EchoService>(*app, owner);
    BOOST_REQUIRE(svc != nullptr);

    auto missing = svc->Bound<DummyComponent>("missing");
    BOOST_REQUIRE(!missing);
    BOOST_CHECK(missing.error().code == fw::ErrorCode::NotFound);

    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<DummyComponent>(
        "primary", DummyComponent::Options{11}));
    BOOST_REQUIRE(bbt_co_await app->CoInstallComponent<DummyComponent>(
        "replica", DummyComponent::Options{22}));

    /**
     * 同名（类型+名）重复登记失败。
     */
    auto duplicate = bbt_co_await app->CoInstallComponent<DummyComponent>(
        "primary", DummyComponent::Options{33});
    BOOST_REQUIRE(!duplicate);
    BOOST_CHECK(duplicate.error().code == fw::ErrorCode::InvalidArgument);

    /**
     * 启动后按名借到对应实例（同名共享、不同名隔离）；
     * Borrow 要求组件 Running，故先 CoStart。
     */
    BOOST_REQUIRE(bbt_co_await app->CoStart());
    auto svc_h = app->FindService("echo");
    BOOST_REQUIRE(svc_h);
    auto primary = bbt_co_await app->CoBindComponent<DummyComponent>(
        svc_h.value(), "primary");
    auto replica = bbt_co_await app->CoBindComponent<DummyComponent>(
        svc_h.value(), "replica");
    BOOST_REQUIRE(primary && replica);
    auto b1 = primary.value().Borrow();
    auto b2 = replica.value().Borrow();
    BOOST_REQUIRE(b1 && b2);
    BOOST_TEST(b1.value().Get().Value() == 11);
    BOOST_TEST(b2.value().Get().Value() == 22);
    BOOST_CHECK(&b1.value().Get() != &b2.value().Get());
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace
