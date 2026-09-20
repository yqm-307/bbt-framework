// CoRpcReq/CoRpcResp 公共面：位置参数、尾部协议错误、业务错误原样传递、
// protobuf codec seam 与 ActorMethodAt 位置 key。
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <bbt/framework/Framework.hpp>
#include <bbt/framework/internal/MethodTable.hpp>

namespace fw = bbt::framework;

namespace {

// protobuf codec seam 的替身：真实适配经 rpc_detail::ProtoCodec<T> 特化
// 接入（CoRpc.hpp 头注释），本测试用确定性字节格式验证 seam 本身——
// [id:4 字节小端][tag 原始字节]，不引入 protobuf 依赖。
struct FakeProto {
    std::int32_t id = 0;
    std::string  tag;
};

} // namespace

// 特化必须写在包络被特化模板的命名空间（匿名命名空间类型可在全局域特化，
// 同 F0 的 RouteTraits 写法）。
template <>
struct bbt::framework::rpc_detail::ProtoCodec<FakeProto> {
    static constexpr bool kDefined = true;

    static bbt::framework::result<std::vector<std::uint8_t>>
    Encode(const FakeProto& v) {
        std::vector<std::uint8_t> out;
        const std::uint32_t u = static_cast<std::uint32_t>(v.id);
        for (int i = 0; i < 4; ++i)
            out.push_back(static_cast<std::uint8_t>(u >> (8 * i)));
        out.insert(out.end(), v.tag.begin(), v.tag.end());
        return bbt::framework::result<std::vector<std::uint8_t>>::ok(
            std::move(out));
    }

    static bbt::framework::result<FakeProto>
    Decode(const std::vector<std::uint8_t>& payload) {
        if (payload.size() < 4)
            return bbt::framework::result<FakeProto>::err(
                bbt::framework::MakeError(
                    bbt::framework::ErrorCode::ProtocolError,
                    "fake proto payload truncated"));
        FakeProto v;
        v.id = static_cast<std::int32_t>(
            static_cast<std::uint32_t>(payload[0]) |
            (static_cast<std::uint32_t>(payload[1]) << 8) |
            (static_cast<std::uint32_t>(payload[2]) << 16) |
            (static_cast<std::uint32_t>(payload[3]) << 24));
        v.tag.assign(reinterpret_cast<const char*>(payload.data() + 4),
                     payload.size() - 4);
        return bbt::framework::result<FakeProto>::ok(std::move(v));
    }
};

namespace {

// Parse 返回类型契约（编译期钉死）：
// 位置参数路径：单参 Parse<T> → result<T>（裸值）；多参 Parse<Ts...>
// → result<tuple<Ts...>>；proto codec 路径 Parse<Proto>/ParseProto<Proto>
// → result<Proto> 整包直出，不包 tuple。错误体系唯一：ErrorType 即
// bbt::infra::Error。
static_assert(std::is_same_v<
    decltype(std::declval<const fw::CoRpcReq&>().Parse<std::int32_t>()),
    fw::result<std::int32_t>>);
static_assert(std::is_same_v<
    decltype(std::declval<const fw::CoRpcReq&>()
                 .Parse<std::int32_t, std::string>()),
    fw::result<std::tuple<std::int32_t, std::string>>>);
static_assert(std::is_same_v<
    decltype(std::declval<const fw::CoRpcReq&>().Parse<FakeProto>()),
    fw::result<FakeProto>>);
static_assert(std::is_same_v<
    decltype(std::declval<const fw::CoRpcReq&>().ParseProto<FakeProto>()),
    fw::result<FakeProto>>);
static_assert(
    std::is_same_v<fw::CoRpcResp::ErrorType, bbt::infra::Error>);

// 请求编码工厂返回类型（编译期钉死）：编码失败经 result 而非异常透出。
static_assert(std::is_same_v<
    decltype(fw::CoRpcReq::From(
        std::declval<const std::int32_t&>())),
    fw::result<fw::CoRpcReq>>);
static_assert(std::is_same_v<
    decltype(fw::CoRpcReq::From(
        std::declval<const std::tuple<std::int32_t, std::string>&>())),
    fw::result<fw::CoRpcReq>>);
static_assert(std::is_same_v<
    decltype(fw::CoRpcReq::FromProto(
        std::declval<const FakeProto&>())),
    fw::result<fw::CoRpcReq>>);

class Calculator final : public fw::CoService<Calculator> {
public:
    static constexpr std::string_view kServiceName = "calculator";

    fw::CoRpcResp Add(fw::CoRpcReq req) {
        auto args = req.Parse<std::int32_t, std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        const auto& [n, name] = args.value();
        return fw::CoRpcResp::From(std::tuple{n + 1, name + "!"});
    }

    fw::CoRpcResp Reject(fw::CoRpcReq) {
        return fw::CoRpcResp::Error(
            fw::MakeError(fw::ErrorCode::InvalidArgument, "rejected"));
    }

    // proto 整包路径：一个方法内只走 Parse<Proto>/FromProto，不混位置参数。
    fw::CoRpcResp ProtoEcho(fw::CoRpcReq req) {
        auto msg = req.Parse<FakeProto>();
        if (!msg) return fw::CoRpcResp::Error(msg.error());
        return fw::CoRpcResp::FromProto(msg.value());
    }

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&Calculator::Add>("add"),
        fw::Method<&Calculator::Reject>("reject"),
        fw::Method<&Calculator::ProtoEcho>("proto_echo"));
};

// ActorMethodAt 位置 key：index>0 的字符串 key 与默认 index=0 的整型 key。
class Directory final : public fw::CoService<Directory> {
public:
    static constexpr std::string_view kServiceName = "directory";

    fw::CoRpcResp Lookup(fw::CoRpcReq req) {
        auto args = req.Parse<std::int32_t, std::string>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        return fw::CoRpcResp::From(std::get<1>(args.value()));
    }
    fw::CoRpcResp ById(fw::CoRpcReq req) {
        auto args = req.Parse<std::int32_t>();
        if (!args) return fw::CoRpcResp::Error(args.error());
        return fw::CoRpcResp::From(args.value() + 1);
    }

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::ActorMethodAt<&Directory::Lookup, std::string, 1>("lookup"),
        fw::ActorMethodAt<&Directory::ById, std::int32_t>("by_id"));
};

fw::CoRpcReq Request(std::int32_t n, std::string name) {
    auto req = fw::CoRpcReq::From(std::tuple{n, std::move(name)});
    BOOST_REQUIRE(req);
    return std::move(req).value();
}

} // namespace

BOOST_AUTO_TEST_CASE(positional_roundtrip_and_method_table) {
    Calculator service;
    const auto table = fw::BuildMethodTable<Calculator>();
    BOOST_REQUIRE(table.find("add") != nullptr);
    BOOST_TEST(table.find("add")->request_schema ==
               std::string(fw::kCoRpcPositionalSchema));

    auto out = table.find("add")->invoke(service,
        Request(41, "answer").payload());
    BOOST_REQUIRE(out);
    fw::CoRpcReq response(std::move(out.value()));
    auto values = response.Parse<std::int32_t, std::string>();
    BOOST_REQUIRE(values);
    const auto& [n, name] = values.value();
    BOOST_TEST(n == 42);
    BOOST_TEST(name == "answer!");
}

BOOST_AUTO_TEST_CASE(trailing_payload_is_protocol_error) {
    auto request = fw::CoRpcReq::From(std::tuple{std::int32_t{1}});
    BOOST_REQUIRE(request);
    auto payload = request.value().payload();
    payload.push_back(0xA5);
    auto parsed = fw::CoRpcReq(std::move(payload)).Parse<std::int32_t>();
    BOOST_REQUIRE(!parsed);
    BOOST_TEST(static_cast<int>(parsed.error().code) ==
               static_cast<int>(fw::ErrorCode::ProtocolError));
}

BOOST_AUTO_TEST_CASE(business_error_is_preserved) {
    Calculator service;
    const auto table = fw::BuildMethodTable<Calculator>();
    auto out = table.find("reject")->invoke(service, {});
    BOOST_REQUIRE(!out);
    BOOST_TEST(static_cast<int>(out.error().code) ==
               static_cast<int>(fw::ErrorCode::InvalidArgument));
    BOOST_TEST(out.error().message == "rejected");
}

BOOST_AUTO_TEST_CASE(parse_error_mapping) {
    // 空 payload → 头部截断 → ProtocolError。
    auto empty = fw::CoRpcReq{}.Parse<std::int32_t>();
    BOOST_REQUIRE(!empty);
    BOOST_TEST(static_cast<int>(empty.error().code) ==
               static_cast<int>(fw::ErrorCode::ProtocolError));

    // 声明参数个数 > 实际个数 → InvalidArgument。
    auto two = fw::CoRpcReq::From(
        std::tuple{std::int32_t{1}, std::int32_t{2}});
    BOOST_REQUIRE(two);
    auto fewer = two.value().Parse<std::int32_t>();
    BOOST_REQUIRE(!fewer);
    BOOST_TEST(static_cast<int>(fewer.error().code) ==
               static_cast<int>(fw::ErrorCode::InvalidArgument));

    // 分发边界：handler 内 Parse 失败经 CoRpcResp::Error 原样透出。
    Calculator service;
    const auto table = fw::BuildMethodTable<Calculator>();
    auto out = table.find("add")->invoke(service, {0xFF});
    BOOST_REQUIRE(!out);
    BOOST_TEST(static_cast<int>(out.error().code) ==
               static_cast<int>(fw::ErrorCode::ProtocolError));
}

BOOST_AUTO_TEST_CASE(proto_codec_seam) {
    static_assert(fw::rpc_detail::is_proto_codec_v<FakeProto>);
    static_assert(!fw::rpc_detail::is_proto_codec_v<std::string>);
    static_assert(!fw::rpc_detail::is_proto_codec_v<std::int32_t>);

    const FakeProto msg{7, "seven"};
    // From(proto)/FromProto 均走 ProtoCodec::Encode，不经位置 codec。
    auto via_from  = fw::CoRpcResp::From(msg);
    auto via_named = fw::CoRpcResp::FromProto(msg);
    BOOST_REQUIRE(via_from.ok());
    BOOST_REQUIRE(via_named.ok());
    BOOST_TEST(via_from.payload() == via_named.payload());
    BOOST_TEST(via_from.payload().size() == 4 + msg.tag.size());

    // 请求侧对称工厂：CoRpcReq::From(proto)/FromProto 共用同一 Encode，
    // 产出的 payload 与响应侧逐字节一致。
    auto req_from  = fw::CoRpcReq::From(msg);
    auto req_named = fw::CoRpcReq::FromProto(msg);
    BOOST_REQUIRE(req_from);
    BOOST_REQUIRE(req_named);
    BOOST_TEST(req_from.value().payload() == via_from.payload());
    BOOST_TEST(req_named.value().payload() == via_from.payload());

    // 位置参数请求工厂与响应编码产出同一线格式。
    auto positional = fw::CoRpcReq::From(
        std::tuple{std::int32_t{3}, std::string{"k"}});
    BOOST_REQUIRE(positional);
    auto roundtrip = positional.value().Parse<std::int32_t, std::string>();
    BOOST_REQUIRE(roundtrip);
    BOOST_TEST(std::get<0>(roundtrip.value()) == 3);
    BOOST_TEST(std::get<1>(roundtrip.value()) == "k");

    // Parse<Proto>/ParseProto 整包解码为 result<Proto>。
    auto direct = fw::CoRpcReq(via_from.payload()).Parse<FakeProto>();
    BOOST_REQUIRE(direct);
    BOOST_TEST(direct.value().id == 7);
    BOOST_TEST(direct.value().tag == "seven");
    auto named = fw::CoRpcReq(via_from.payload()).ParseProto<FakeProto>();
    BOOST_REQUIRE(named);
    BOOST_TEST(named.value().id == 7);
    BOOST_TEST(named.value().tag == "seven");

    // codec 自带错误原样透出，不被位置 codec 的错误语义改写。
    auto bad = fw::CoRpcReq({0x01}).Parse<FakeProto>();
    BOOST_REQUIRE(!bad);
    BOOST_TEST(static_cast<int>(bad.error().code) ==
               static_cast<int>(fw::ErrorCode::ProtocolError));
    BOOST_TEST(bad.error().message == "fake proto payload truncated");

    // 经方法表分发的整包回路：同一服务上 proto 方法与位置方法共存。
    Calculator service;
    const auto table = fw::BuildMethodTable<Calculator>();
    auto out = table.find("proto_echo")->invoke(service, via_from.payload());
    BOOST_REQUIRE(out);
    BOOST_TEST(out.value() == via_from.payload());
}

BOOST_AUTO_TEST_CASE(actor_method_at_positional_key) {
    Directory service;
    const auto table = fw::BuildMethodTable<Directory>();
    const fw::RpcMethod* lookup = table.find("lookup");
    BOOST_REQUIRE(lookup != nullptr);
    BOOST_TEST(lookup->actor_keyed);
    BOOST_TEST(lookup->request_schema ==
               std::string(fw::kCoRpcPositionalSchema));
    BOOST_REQUIRE(lookup->extract_key != nullptr);

    // index=1：key 取第二个位置参数而非第一个。
    auto req = fw::CoRpcReq::From(
        std::tuple{std::int32_t{5}, std::string{"acct-7"}});
    BOOST_REQUIRE(req);
    auto key = lookup->extract_key(req.value().payload());
    BOOST_REQUIRE(key);
    BOOST_TEST(key.value() == "acct-7");

    // 整型 key → to_string。
    const fw::RpcMethod* by_id = table.find("by_id");
    BOOST_REQUIRE(by_id != nullptr);
    auto id_req = fw::CoRpcReq::From(std::int32_t{42});
    BOOST_REQUIRE(id_req);
    auto id_key = by_id->extract_key(id_req.value().payload());
    BOOST_REQUIRE(id_key);
    BOOST_TEST(id_key.value() == "42");

    // 位置缺失（wanted >= count）→ InvalidArgument。
    auto short_req = fw::CoRpcReq::From(std::int32_t{5});
    BOOST_REQUIRE(short_req);
    auto missing = lookup->extract_key(short_req.value().payload());
    BOOST_REQUIRE(!missing);
    BOOST_TEST(static_cast<int>(missing.error().code) ==
               static_cast<int>(fw::ErrorCode::InvalidArgument));

    // 截断 payload → ProtocolError。
    auto truncated = req.value().payload();
    truncated.pop_back();
    auto bad_wire = lookup->extract_key(truncated);
    BOOST_REQUIRE(!bad_wire);
    BOOST_TEST(static_cast<int>(bad_wire.error().code) ==
               static_cast<int>(fw::ErrorCode::ProtocolError));

    // key 位置 wire 类型不符 → TypeMismatch。
    auto wrong_type = fw::CoRpcReq::From(
        std::tuple{std::string{"x"}, std::int32_t{5}});
    BOOST_REQUIRE(wrong_type);
    auto mismatched = lookup->extract_key(wrong_type.value().payload());
    BOOST_REQUIRE(!mismatched);
    BOOST_TEST(static_cast<int>(mismatched.error().code) ==
               static_cast<int>(fw::ErrorCode::TypeMismatch));

    // 空字符串 key → InvalidArgument。
    auto empty_req = fw::CoRpcReq::From(
        std::tuple{std::int32_t{5}, std::string{}});
    BOOST_REQUIRE(empty_req);
    auto empty_key = lookup->extract_key(empty_req.value().payload());
    BOOST_REQUIRE(!empty_key);
    BOOST_TEST(static_cast<int>(empty_key.error().code) ==
               static_cast<int>(fw::ErrorCode::InvalidArgument));
}
