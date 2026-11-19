/**
 * GetValue 业务切片的 C++ 侧单测。
 *
 * 机器断言：
 * - 唯一 schema 真源 = .proto：descriptor full_name / 字段号 / 字段类型；
 * - 方法表 schema 从 descriptor 推导，与手写常量一致（无漂移）；
 * - ProtoCodec golden vectors：C++ 侧字节与 Python 客户端独立断言同一串；
 * - handler 的 known/miss/empty 分支（纯逻辑，不经网络；受管实例经
 *   CoInstallService 创建，断言运行在真实受管协程）。
 */

#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/example/v1/get_value.pb.h>

#include <bbt/framework/CoApp.hpp>
#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/Object.hpp>
#include <bbt/framework/RpcMethods.hpp>
#include <bbt/framework/internal/CoAppSeam.hpp>
#include <bbt/framework/internal/MethodTable.hpp>
#include <bbt/framework/internal/NetworkHost.hpp>

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

std::shared_ptr<fw::CoApp> MakeApp() {
    return fw::MakeCoAppForTest(
        fw::AppOptions{}
            .SetLimits(64, 64, 64, 64)
            .SetShutdownBudget(std::chrono::milliseconds{2000})
            .SetLifecycleBudget(std::chrono::milliseconds{2000}),
        fw::CoAppSeam{std::make_shared<StubNetHost>(), {}});
}

/**
 * 取已安装服务的受管实例（锁内取 owner+指针，保活纪律与宿主一致；
 * 返回的 owner 由调用方持有，调用期对象不可销毁）。
 */
template <class S>
S* ManagedService(fw::CoApp& app, std::shared_ptr<void>& owner) {
    const auto reg = fw::RegistryForTest(app);
    std::lock_guard<std::mutex> lk(reg->Mutex());
    auto* e = reg->FindByTypeNameLocked(
        fw::ObjectKind::Service, S::kServiceName);
    if (!e)
        return nullptr;
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
    if (bbt::coroutine::IsInCoroutineContext()) {
        fn();
        return true;
    }
    /**
     * native 测试宿主负责初始化唯一 runtime，产品 App 不启动 runtime。
     */
    if (!g_scheduler->IsInitialized()) {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 3;
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

} // namespace

BOOST_AUTO_TEST_CASE(schema_truth_is_proto_descriptor) {
    using bbt::example::v1::GetValueRequest;
    using bbt::example::v1::GetValueResponse;
    const auto* req = GetValueRequest::descriptor();
    const auto* resp = GetValueResponse::descriptor();

    BOOST_CHECK_EQUAL(req->full_name(), "bbt.example.v1.GetValueRequest");
    BOOST_CHECK_EQUAL(resp->full_name(), "bbt.example.v1.GetValueResponse");

    /**
     * 声明顺序即字段号 1
     */
    const auto* f1 = req->field(0);
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
    BOOST_CHECK(m->extract_key == nullptr);
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
    BOOST_REQUIRE(RunInManagedCoroutine([] {
        auto app = MakeApp();
        BOOST_REQUIRE(bbt_co_await app->CoInstallService<getvalue::GetValueService>(
            fw::ServiceOptions{}.SetMaxInflight(8)));
        std::shared_ptr<void> owner;
        auto* svc = ManagedService<getvalue::GetValueService>(*app, owner);
        BOOST_REQUIRE(svc != nullptr);

        auto call_with_key = [&](const std::string& key) {
            bbt::example::v1::GetValueRequest req;
            req.set_key(key);
            auto rq = fw::CoRpcReq::FromProto(req);
            BOOST_REQUIRE(rq);
            return svc->GetValue(rq.value());
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
    }));
}
