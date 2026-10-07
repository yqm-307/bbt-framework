// bbt-framework Issue #4（P0-A）：GetValue 业务切片的 C++ 侧单测。
//
// 机器断言：
//  - 唯一 schema 真源 = .proto：descriptor full_name / 字段号 / 字段类型；
//  - 方法表 schema 从 descriptor 推导，与手写常量一致（无漂移）；
//  - ProtoCodec golden vectors：C++ 侧字节与 Python 客户端独立断言同一串；
//  - handler 的 known/miss/empty 分支（纯逻辑，不经网络）。

#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <algorithm>
#include <string>
#include <vector>

#include <bbt/example/v1/get_value.pb.h>

#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/RpcMethods.hpp>
#include <bbt/framework/internal/MethodTable.hpp>

#include "getvalue_service.hpp"

namespace fw = bbt::framework;

namespace {

const std::vector<std::uint8_t> kGoldenReqAlpha = {
    0x0a, 0x05, 'a', 'l', 'p', 'h', 'a'};
const std::vector<std::uint8_t> kGoldenRespAlpha = {
    0x08, 0x01, 0x12, 0x0b, 'v', 'a', 'l', 'u', 'e', '-',
    'a', 'l', 'p', 'h', 'a'};

bool PayloadEq(const std::vector<std::uint8_t>& p,
               const std::vector<std::uint8_t>& g) {
    return p.size() == g.size() && std::equal(p.begin(), p.end(), g.begin());
}

} // namespace

BOOST_AUTO_TEST_CASE(schema_truth_is_proto_descriptor) {
    using bbt::example::v1::GetValueRequest;
    using bbt::example::v1::GetValueResponse;
    const auto* req = GetValueRequest::descriptor();
    const auto* resp = GetValueResponse::descriptor();

    BOOST_CHECK_EQUAL(req->full_name(), "bbt.example.v1.GetValueRequest");
    BOOST_CHECK_EQUAL(resp->full_name(), "bbt.example.v1.GetValueResponse");

    const auto* f1 = req->field(0);   // 声明顺序即字段号 1
    BOOST_CHECK_EQUAL(f1->number(), 1);
    BOOST_CHECK_EQUAL(f1->name(), "key");
    BOOST_CHECK(f1->type() == google::protobuf::FieldDescriptor::TYPE_STRING);

    const auto* rf1 = resp->field(0);
    BOOST_CHECK_EQUAL(rf1->number(), 1);
    BOOST_CHECK_EQUAL(rf1->name(), "found");
    BOOST_CHECK(rf1->type() == google::protobuf::FieldDescriptor::TYPE_BOOL);

    const auto* rf2 = resp->field(1);
    BOOST_CHECK_EQUAL(rf2->number(), 2);
    BOOST_CHECK_EQUAL(rf2->name(), "value");
    BOOST_CHECK(rf2->type() == google::protobuf::FieldDescriptor::TYPE_STRING);
}

BOOST_AUTO_TEST_CASE(method_table_schema_derived_from_descriptor) {
    auto table = fw::BuildMethodTable<getvalue::GetValueService>();
    const fw::RpcMethod* m = table.find("GetValue");
    BOOST_REQUIRE(m != nullptr);
    BOOST_CHECK_EQUAL(m->request_schema, "bbt.example.v1.GetValueRequest");
    BOOST_CHECK_EQUAL(m->response_schema, "bbt.example.v1.GetValueResponse");
    BOOST_CHECK(!m->actor_keyed);
}

BOOST_AUTO_TEST_CASE(proto_codec_golden_vectors_roundtrip) {
    bbt::example::v1::GetValueRequest req;
    req.set_key("alpha");
    auto enc = fw::CoRpcReq::FromProto(req);
    BOOST_REQUIRE(enc);
    BOOST_CHECK(PayloadEq(enc.value().payload(), kGoldenReqAlpha));

    auto req2 = fw::CoRpcReq(kGoldenReqAlpha)
                    .ParseProto<bbt::example::v1::GetValueRequest>();
    BOOST_REQUIRE(req2);
    BOOST_CHECK_EQUAL(req2.value().key(), "alpha");

    bbt::example::v1::GetValueResponse resp;
    resp.set_found(true);
    resp.set_value("value-alpha");
    auto enc2 = fw::CoRpcResp::FromProto(resp);
    BOOST_CHECK(enc2.ok());
    BOOST_CHECK(PayloadEq(enc2.payload(), kGoldenRespAlpha));

    auto resp2 = fw::CoRpcReq(enc2.payload())
                     .ParseProto<bbt::example::v1::GetValueResponse>();
    BOOST_REQUIRE(resp2);
    BOOST_CHECK(resp2.value().found());
    BOOST_CHECK_EQUAL(resp2.value().value(), "value-alpha");
}

BOOST_AUTO_TEST_CASE(handler_known_miss_empty) {
    getvalue::GetValueService svc;

    auto call_with_key = [&](const std::string& key) {
        bbt::example::v1::GetValueRequest req;
        req.set_key(key);
        auto rq = fw::CoRpcReq::FromProto(req);
        BOOST_REQUIRE(rq);
        return svc.GetValue(rq.value());
    };

    auto known = call_with_key("alpha");
    BOOST_REQUIRE(known.ok());
    auto kv = fw::CoRpcReq(known.payload())
                  .ParseProto<bbt::example::v1::GetValueResponse>();
    BOOST_REQUIRE(kv);
    BOOST_CHECK(kv.value().found());
    BOOST_CHECK_EQUAL(kv.value().value(), "value-alpha");

    auto miss = call_with_key("no-such-key");
    BOOST_REQUIRE(miss.ok());
    auto mv = fw::CoRpcReq(miss.payload())
                  .ParseProto<bbt::example::v1::GetValueResponse>();
    BOOST_REQUIRE(mv);
    BOOST_CHECK(!mv.value().found());
    BOOST_CHECK_EQUAL(mv.value().value(), "");

    auto empty = call_with_key("");
    BOOST_CHECK(!empty.ok());
    BOOST_CHECK(empty.error().code == fw::ErrorCode::InvalidArgument);
}
