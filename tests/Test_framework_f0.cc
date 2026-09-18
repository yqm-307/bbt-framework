// F0 切片验证：消息 codec 全部成功/失败边界、声明式方法表展开、
// 分发 codec/actor key 提取、错误边界、未托管 call 与资源缝。
// 不联网、不起真实 I/O、不启动 Scheduler。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/CoApp.hpp>

// 方法表是机器面：本测试直接断言展开结果，显式包含 internal 头。
#include <bbt/framework/internal/MethodTable.hpp>

namespace fw = bbt::framework;
using Clock = std::chrono::steady_clock;

namespace {

struct EchoRequest  { std::string text; };
struct EchoReply    { std::string text; };
struct KeyedRequest { std::string player_id; };
struct Inner        { std::int32_t n = 0; std::string note; };
struct NestedMsg {
    bool                      flag = false;
    std::int8_t               tiny = 0;
    std::int32_t              n = 0;
    std::uint64_t             big = 0;
    double                    ratio = 0;
    std::string               name;
    std::vector<std::uint8_t> blob;
    Inner                     inner;
};

} // namespace

// 业务消息只写字段声明与 schema 名；编解码由框架生成的 MessageCodec 供给。
BBT_MESSAGE_FIELDS(EchoRequest, "test.EchoRequest/v1", text)
BBT_MESSAGE_FIELDS(EchoReply, "test.EchoReply/v1", text)
BBT_MESSAGE_FIELDS(KeyedRequest, "test.KeyedRequest/v1", player_id)
BBT_MESSAGE_FIELDS(Inner, "test.Inner/v1", n, note)
BBT_MESSAGE_FIELDS(NestedMsg, "test.NestedMsg/v1",
                   flag, tiny, n, big, ratio, name, blob, inner)

namespace {

// RouteTraits 可选定制点：用户类型 → 固定 RouteFields 载体。
struct PlayerRouteArg { std::string player_id; };

} // namespace

// 匿名命名空间类型在 bbt::framework 内不可命名：显式特化写在全局域。
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

// 假服务：走 CRTP 便利基类；业务也可直接继承 ICoService（见 DupService）。
class EchoService final : public fw::CoService<EchoService> {
public:
    static constexpr std::string_view kServiceName = "echo";

    fw::result<EchoReply> Echo(const EchoRequest& req) {
        return fw::result<EchoReply>::ok(EchoReply{"echo:" + req.text});
    }
    fw::result<void> Reset(const EchoRequest&) {
        return fw::result<void>::ok();
    }
    void Notify(const EchoRequest& req) { m_last_notify = req.text; }
    fw::result<EchoReply> Fail(const EchoRequest&) {
        return fw::result<EchoReply>::err(
            fw::MakeError(fw::ErrorCode::InvalidArgument, "rejected"));
    }
    void Boom(const EchoRequest&) { throw std::runtime_error("boom"); }
    fw::result<EchoReply> WhoAmI(const KeyedRequest& req) {
        return fw::result<EchoReply>::ok(EchoReply{"player:" + req.player_id});
    }

    const std::string& last_notify() const { return m_last_notify; }

    // 未托管对象的 this->call 按契约返回 RuntimeUnavailable。
    fw::result<EchoReply> CallOut(const EchoRequest& req) {
        return this->call<EchoReply>("other-service", "get", req);
    }

    // 资源缝观察口：未绑定宿主时 resource<R>() 如实返回空。
    template <class R>
    std::shared_ptr<R> Resource() { return context().resource<R>(); }

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EchoService::Echo>("echo"),
        fw::Method<&EchoService::Reset>("reset"),
        fw::Method<&EchoService::Notify>("notify"),
        fw::Method<&EchoService::Fail>("fail"),
        fw::Method<&EchoService::Boom>("boom"),
        fw::ActorMethod<&EchoService::WhoAmI,
                        &KeyedRequest::player_id>("who-am-i"));
private:
    std::string m_last_notify;
};

// 直接继承 ICoService（无 CRTP）的重复方法名服务：方法表构建期必须暴露。
class DupService final : public fw::ICoService {
public:
    std::string_view co_service_name() const noexcept override { return "dup"; }
    fw::result<EchoReply> A(const EchoRequest&) {
        return fw::result<EchoReply>::ok(EchoReply{});
    }
    fw::result<EchoReply> B(const EchoRequest&) {
        return fw::result<EchoReply>::ok(EchoReply{});
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&DupService::A>("same"),
        fw::Method<&DupService::B>("same"));
};

// 空方法名：构建期同样拒绝。
class EmptyNameService final : public fw::ICoService {
public:
    std::string_view co_service_name() const noexcept override { return "e"; }
    fw::result<EchoReply> A(const EchoRequest&) {
        return fw::result<EchoReply>::ok(EchoReply{});
    }
    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&EmptyNameService::A>(""));
};

// 资源缝验证用类型（不伪造任何真实客户端）。
struct DummyResource { int n = 7; };

// rpc_traits 编译期提取：参数/响应/编解码绑定。
using EchoTraits = fw::rpc_traits<decltype(&EchoService::Echo)>;
static_assert(std::is_same_v<EchoTraits::service_type,  EchoService>);
static_assert(std::is_same_v<EchoTraits::request_type,  EchoRequest>);
static_assert(std::is_same_v<EchoTraits::reply_type,    EchoReply>);
static_assert(std::is_same_v<EchoTraits::result_type,   fw::result<EchoReply>>);
static_assert(std::is_same_v<EchoTraits::request_codec,
                             fw::MessageCodec<EchoRequest>>);
static_assert(std::is_same_v<EchoTraits::reply_codec,
                             fw::MessageCodec<EchoReply>>);
using NotifyTraits = fw::rpc_traits<decltype(&EchoService::Notify)>;
static_assert(std::is_same_v<NotifyTraits::reply_type,  void>);
static_assert(std::is_same_v<NotifyTraits::result_type, fw::result<void>>);
static_assert(std::is_same_v<NotifyTraits::reply_codec,
                             bbt::infra::Codec<void>>);

// has_rpc_methods：声明即真，未声明即假。
static_assert(fw::has_rpc_methods_v<EchoService>);
static_assert(!fw::has_rpc_methods_v<DummyResource>);

std::vector<std::uint8_t> EncodeText(std::string_view text) {
    auto r = fw::MessageCodec<EchoRequest>::Encode(
        EchoRequest{std::string(text)});
    BOOST_REQUIRE(r);
    return r.value();
}

// ── 线格式手构助手（坏 payload 用例）：count + [u16 tag][u8 type][u32 len]
//    [payload]，类型码见 Message.hpp 的 msg::WireType。──

void PutU16(std::vector<std::uint8_t>& o, std::uint16_t v) {
    o.push_back(static_cast<std::uint8_t>(v));
    o.push_back(static_cast<std::uint8_t>(v >> 8));
}
void PutU32(std::vector<std::uint8_t>& o, std::uint32_t v) {
    for (int i = 0; i < 4; ++i)
        o.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}
void PutField(std::vector<std::uint8_t>& o, std::uint16_t tag,
              std::uint8_t type, const std::vector<std::uint8_t>& body) {
    PutU16(o, tag);
    o.push_back(type);
    PutU32(o, static_cast<std::uint32_t>(body.size()));
    o.insert(o.end(), body.begin(), body.end());
}
// type/len 与体分离，便于构造「len 大于剩余」的截断。
void PutFieldHeader(std::vector<std::uint8_t>& o, std::uint16_t tag,
                    std::uint8_t type, std::uint32_t len) {
    PutU16(o, tag);
    o.push_back(type);
    PutU32(o, len);
}
std::vector<std::uint8_t> Wire(std::uint32_t count) {
    std::vector<std::uint8_t> o;
    PutU32(o, count);
    return o;
}

constexpr std::uint8_t kWireBool    = 1;
constexpr std::uint8_t kWireInt     = 2;
constexpr std::uint8_t kWireString  = 5;
constexpr std::uint8_t kWireMessage = 7;

BOOST_AUTO_TEST_SUITE(framework_f0)

// 声明展开：方法表收录全部方法、schema 来自消息声明、actor_keyed 标记正确、
// 未知方法可检出。
BOOST_AUTO_TEST_CASE(method_table_binding) {
    const auto table = fw::BuildMethodTable<EchoService>();
    BOOST_TEST(table.size() == 6);

    const fw::RpcMethod* echo = table.find("echo");
    BOOST_REQUIRE(echo != nullptr);
    BOOST_TEST(echo->name == "echo");
    BOOST_TEST(echo->request_schema  == "test.EchoRequest/v1");
    BOOST_TEST(echo->response_schema == "test.EchoReply/v1");
    BOOST_TEST(!echo->actor_keyed);
    BOOST_CHECK(echo->extract_key == nullptr);

    const fw::RpcMethod* who = table.find("who-am-i");
    BOOST_REQUIRE(who != nullptr);
    BOOST_TEST(who->actor_keyed);
    BOOST_CHECK(who->extract_key != nullptr);
    BOOST_TEST(who->request_schema == "test.KeyedRequest/v1");

    BOOST_TEST(table.find("no-such-method") == nullptr);
}

// 分发闭环：编码请求 → 描述闭包在真实实例上调用 → 编码回复可解码。
BOOST_AUTO_TEST_CASE(dispatch_roundtrip) {
    EchoService svc;
    const auto table = fw::BuildMethodTable<EchoService>();
    auto out = table.find("echo")->invoke(svc, EncodeText("hi"));
    BOOST_REQUIRE(out);
    auto reply = fw::MessageCodec<EchoReply>::Decode(out.value());
    BOOST_REQUIRE(reply);
    BOOST_TEST(reply.value().text == "echo:hi");
}

// void 与 result<void>：正常返回编码为 result<void>::ok（bbt.void/v1 + 空负载），
// void handler 的副作用确实发生。
BOOST_AUTO_TEST_CASE(void_and_result_void_semantics) {
    EchoService svc;
    const auto table = fw::BuildMethodTable<EchoService>();

    const fw::RpcMethod* notify = table.find("notify");
    BOOST_REQUIRE(notify != nullptr);
    BOOST_TEST(notify->response_schema == "bbt.void/v1");
    auto out = notify->invoke(svc, EncodeText("ping"));
    BOOST_REQUIRE(out);
    BOOST_TEST(out.value().empty());
    BOOST_CHECK(bbt::infra::Codec<void>::Decode(out.value()));
    BOOST_TEST(svc.last_notify() == "ping");

    const fw::RpcMethod* reset = table.find("reset");
    BOOST_REQUIRE(reset != nullptr);
    BOOST_TEST(reset->response_schema == "bbt.void/v1");
    auto out2 = reset->invoke(svc, EncodeText("x"));
    BOOST_REQUIRE(out2);
    BOOST_TEST(out2.value().empty());
}

// 错误边界：业务 err 透传、handler 异常转 InternalError、解码失败不进 handler。
BOOST_AUTO_TEST_CASE(error_boundaries) {
    EchoService svc;
    const auto table = fw::BuildMethodTable<EchoService>();

    auto r1 = table.find("fail")->invoke(svc, EncodeText("x"));
    BOOST_REQUIRE(!r1);
    BOOST_CHECK(r1.error().code == fw::ErrorCode::InvalidArgument);

    auto r2 = table.find("boom")->invoke(svc, EncodeText("x"));
    BOOST_REQUIRE(!r2);
    BOOST_CHECK(r2.error().code == fw::ErrorCode::InternalError);

    EchoService svc2;
    auto r3 = table.find("notify")->invoke(svc2, {});
    BOOST_REQUIRE(!r3);
    BOOST_CHECK(r3.error().code == fw::ErrorCode::ProtocolError);
    BOOST_TEST(svc2.last_notify().empty());
}

// actor 路由：key 取自已声明的 Request 字段成员指针；空 key 拒绝；
// 非 actor 方法不装提取器。
BOOST_AUTO_TEST_CASE(actor_key_extraction) {
    const auto table = fw::BuildMethodTable<EchoService>();
    const fw::RpcMethod* who = table.find("who-am-i");
    BOOST_REQUIRE(who != nullptr);

    auto good = who->extract_key(
        fw::MessageCodec<KeyedRequest>::Encode(KeyedRequest{"p-42"}).value());
    BOOST_REQUIRE(good);
    BOOST_TEST(good.value() == "p-42");

    auto bad = who->extract_key(
        fw::MessageCodec<KeyedRequest>::Encode(KeyedRequest{""}).value());
    BOOST_REQUIRE(!bad);
    BOOST_CHECK(bad.error().code == fw::ErrorCode::InvalidArgument);
}

// 重复/空方法名在方法表构建期暴露（启动期失败），直接继承 ICoService 同样适用。
BOOST_AUTO_TEST_CASE(duplicate_method_rejected) {
    BOOST_CHECK_THROW(fw::BuildMethodTable<DupService>(),
                      std::invalid_argument);
    BOOST_CHECK_THROW(fw::BuildMethodTable<EmptyNameService>(),
                      std::invalid_argument);
}

// 身份与未托管调用：GetObjectInfo 由框架实现；co_actor_key 空；
// this->call 返回 RuntimeUnavailable。
BOOST_AUTO_TEST_CASE(unhosted_identity_and_call) {
    EchoService svc;
    auto info = svc.GetObjectInfo();
    BOOST_TEST(info.id == 0);
    BOOST_TEST(info.name == "echo");
    BOOST_TEST(!svc.co_actor_key().has_value());

    auto r = svc.CallOut(EchoRequest{"q"});
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == fw::ErrorCode::RuntimeUnavailable);
}

// 路由载体与可选定制点：RouteTraits 把用户类型转成固定 RouteFields。
BOOST_AUTO_TEST_CASE(route_fields_and_traits) {
    auto f = fw::RouteTraits<PlayerRouteArg>::Encode(PlayerRouteArg{"p7"});
    BOOST_REQUIRE(f);
    BOOST_REQUIRE(f.value().values.size() == 1);
    BOOST_TEST(f.value().values[0].first  == "player");
    BOOST_TEST(f.value().values[0].second == "p7");

    fw::RpcRoute route{"echo", "echo", f.value()};
    BOOST_TEST(route.service_name == "echo");
    BOOST_TEST(route.method_name  == "echo");
    BOOST_TEST(route.custom.values.size() == 1);
}

// CallOptions 缺省形态：无 deadline、无票据、默认 token 未取消。
BOOST_AUTO_TEST_CASE(call_options_defaults) {
    fw::CallOptions opt;
    BOOST_TEST(!opt.deadline.has_value());
    BOOST_TEST(!opt.ordered.has_value());
    BOOST_TEST(!opt.cancel.IsCancellationRequested());
}

// 消息 roundtrip：全部支持字段类型（含嵌套消息）编码后可无差解码。
BOOST_AUTO_TEST_CASE(message_roundtrip_all_field_types) {
    NestedMsg in;
    in.flag  = true;
    in.tiny  = -7;
    in.n     = -123456;
    in.big   = 0xFEDCBA9876543210ULL;
    in.ratio = 3.25;
    in.name  = "nested-名字";
    in.blob  = {0, 1, 2, 250, 255};
    in.inner = Inner{42, "deep"};

    auto enc = fw::MessageCodec<NestedMsg>::Encode(in);
    BOOST_REQUIRE(enc);
    auto dec = fw::MessageCodec<NestedMsg>::Decode(enc.value());
    BOOST_REQUIRE(dec);
    const auto& out = dec.value();
    BOOST_TEST(out.flag == true);
    BOOST_TEST(out.tiny == -7);
    BOOST_TEST(out.n == -123456);
    BOOST_TEST(out.big == 0xFEDCBA9876543210ULL);
    BOOST_TEST(out.ratio == 3.25);
    BOOST_TEST(out.name == "nested-名字");
    BOOST_TEST(out.blob == in.blob);
    BOOST_TEST(out.inner.n == 42);
    BOOST_TEST(out.inner.note == "deep");
}

// 缺失字段保留类型默认值（向后兼容加字段）：count=0 解码出全默认值。
BOOST_AUTO_TEST_CASE(message_missing_fields_keep_defaults) {
    auto dec = fw::MessageCodec<NestedMsg>::Decode(Wire(0));
    BOOST_REQUIRE(dec);
    BOOST_TEST(dec.value().n == 0);
    BOOST_TEST(dec.value().name.empty());
    BOOST_TEST(!dec.value().flag);
}

// 坏 payload 边界：每一种畸形都在解码边界显式拒绝（ProtocolError），
// 不进 handler、不产生半解码对象。
BOOST_AUTO_TEST_CASE(message_decode_rejects_malformed) {
    auto expect_bad = [](const std::vector<std::uint8_t>& bytes,
                         const char* what) {
        auto r = fw::MessageCodec<NestedMsg>::Decode(bytes);
        BOOST_REQUIRE_MESSAGE(!r, what);
        BOOST_CHECK_MESSAGE(
            r.error().code == fw::ErrorCode::ProtocolError, what);
    };

    // 头部截断：不足 4 字节 count。
    expect_bad({0x01, 0x00}, "header truncated");
    // count 超界：声明 8 个字段，线内声称 9。
    expect_bad(Wire(9), "field count exceeds schema");
    // 未知 tag：声明下标之外。
    {
        auto w = Wire(1);
        PutField(w, /*tag*/ 9, kWireInt, {1, 0, 0, 0});
        expect_bad(w, "unknown field tag");
    }
    // 重复 tag：同一字段出现两次。
    {
        auto w = Wire(2);
        PutField(w, 0, kWireBool, {1});
        PutField(w, 0, kWireBool, {0});
        expect_bad(w, "duplicate field tag");
    }
    // 类型不符：flag 是 Bool，线内声称 Int。
    {
        auto w = Wire(1);
        PutField(w, 0, kWireInt, {1, 0, 0, 0});
        expect_bad(w, "field type mismatch");
    }
    // 字段头截断：count 声明了字段但头部不全。
    {
        auto w = Wire(1);
        PutU16(w, 0);
        w.push_back(kWireBool);
        expect_bad(w, "field header truncated");
    }
    // 字段体截断：len 声明 10 实际只给 2。
    {
        auto w = Wire(1);
        PutFieldHeader(w, 5, kWireString, 10);
        w.push_back('h'); w.push_back('i');
        expect_bad(w, "truncated field");
    }
    // bool 非 0/1：线上值 2 非法。
    {
        auto w = Wire(1);
        PutField(w, 0, kWireBool, {2});
        expect_bad(w, "bool non-0/1");
    }
    // bool len 不为 1。
    {
        auto w = Wire(1);
        PutField(w, 0, kWireBool, {1, 0});
        expect_bad(w, "bool bad len");
    }
    // 整数宽度错：n 是 int32（len 4），线内给 8 字节。
    {
        auto w = Wire(1);
        PutField(w, 2, kWireInt, {1, 0, 0, 0, 0, 0, 0, 0});
        expect_bad(w, "int width mismatch");
    }
    // 尾部垃圾：合法负载之后存在未消费字节。
    {
        auto w = Wire(1);
        PutField(w, 0, kWireBool, {1});
        w.push_back(0xAB);
        expect_bad(w, "trailing garbage");
    }
    // 嵌套消息体自身畸形：错误经嵌套边界传播，不吞。
    {
        auto w = Wire(1);
        PutField(w, 7, kWireMessage, {0xFF, 0xFF});
        expect_bad(w, "nested malformed");
    }
    // 空 payload：连 count 都没有。
    expect_bad({}, "empty payload");
}

// 资源缝：未托管服务取资源返回空；宿主装配侧校验空指针与同类型重复。
// 「可注入可访问」的正路径在 F1-b1 经运行中宿主验证（实例绑定后取回）。
BOOST_AUTO_TEST_CASE(resource_seam_unbound_and_registration) {
    EchoService svc;
    BOOST_CHECK(svc.Resource<DummyResource>() == nullptr);

    fw::CoAppOptions opts;
    opts.network_limits = bbt::infra::NetworkLimits{
        /*max_connections*/ 64, /*max_inflight*/ 64,
        /*max_header_bytes*/ 16384, /*max_body_bytes*/ 65536,
        /*incoming_timeout*/ std::chrono::milliseconds{30000}};
    opts.listen = bbt::infra::ListenAddress{"127.0.0.1", 0};
    opts.shutdown_step_budget = std::chrono::milliseconds{2000};
    fw::CoApp app(opts);

    auto res = std::make_shared<DummyResource>();
    BOOST_CHECK(app.add_resource(res));
    // 同类型重复装配 → InvalidArgument。
    auto dup = app.add_resource(std::make_shared<DummyResource>());
    BOOST_REQUIRE(!dup);
    BOOST_CHECK(dup.error().code == fw::ErrorCode::InvalidArgument);
    // 空指针 → InvalidArgument。
    auto nul = app.add_resource(std::shared_ptr<DummyResource>{});
    BOOST_REQUIRE(!nul);
    BOOST_CHECK(nul.error().code == fw::ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace
