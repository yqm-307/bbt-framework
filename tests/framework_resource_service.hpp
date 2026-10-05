#pragma once
// framework #8 F8-T3 业务 Service——只依赖 framework/infra/coroutine **公开头**。
//
// 本头与其实现 framework_resource_service.cc 构成独立编译单元，用于提供
// issue #8 验收「干净消费者 Service 只 include framework/infra 公共头即可按
// 名称使用资源」的真实编译证据：实现 .cc 只 include 公开头 + stdlib，编译产
// 生的 include 闭包中不得出现 bbt/framework/internal/* 或任何第三方 driver
// （hiredis/mongocxx/bsoncxx）。测试宿主（真实 HTTP 装配、线桥编码）在
// Test_framework_resource_combined.cc 中另持 internal；二者分属不同 TU，
// 不得以「同一 TU 先 include 公共头」替代隔离证据。
//
// 业务实现只经 context().resource<R>(name) 取用命名资源与 context().request()
// 读请求上下文；不接触连接/线程/连接池，也不持有全局测试状态。

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/sync/WaitTypes.hpp>

#include <bbt/infra/CoMongoCli.hpp>
#include <bbt/infra/CoRedisCli.hpp>
#include <bbt/infra/NetworkTypes.hpp>

#include <bbt/framework/Framework.hpp>

namespace fw8test {

namespace fw  = bbt::framework;
namespace co  = bbt::coroutine;
namespace inf = bbt::infra;

// KvSvc：联合业务 Service（公共面消费 Redis + Mongo）。
// 回复统一为位置参数 (int32 status, string value)：
//   get     status 0=HIT(值在 value) / 1=NOTFOUND
//   insert/update  status 0 或 UpdateOne 的 matched 计数
//   exists  status 0/1
//   ping    成功返回 (0,"PONG")
//   其余    仅成功时返回 (0,...)，错误经 CoRpcResp::Error 原样回传。
class KvSvc final : public fw::CoService<KvSvc> {
public:
    static constexpr std::string_view kServiceName = "kv";

    // 资源缝观察口（宿主断言共享/隔离与 IsClosed/ConnectStatus 用）。
    std::shared_ptr<inf::CoRedisCli> Cache() {
        return context().resource<inf::CoRedisCli>("cache");
    }
    std::shared_ptr<inf::CoMongoCli> Docs() {
        return context().resource<inf::CoMongoCli>("docs");
    }

    // 经 Service 侧真实调用计数，让「cache hit 未再查 Mongo」可观察而非猜测。
    std::atomic<int> mongo_reads{0};

    // 显式测试钩子：关闭中在途用例的闸门与「放行后真实资源操作完成」通知由
    // 宿主测试装配注入/放行。业务实现不持有全局测试状态、不管理线程或连接
    // （HostLifecycle 排空语义由框架保证）。为空表示该钩子未装配。
    //  - hold_gate：Hold 进入后调用（阻塞在宿主闸门上，真实 HTTP handler 在途）；
    //  - hold_op_report：Hold 放行后经 ServiceContext 取 Redis/Mongo 执行真实
    //    操作，操作结果（true=成功）经此回调报告给独立持有的观察者，而不经
    //    Service 实例本身——避免测试持 Service 指针与关闭释放竞争。
    std::function<void()>     hold_gate;
    std::function<void(bool)> hold_op_report;

    fw::CoRpcResp Get(fw::CoRpcReq req);
    fw::CoRpcResp Insert(fw::CoRpcReq req);
    fw::CoRpcResp Update(fw::CoRpcReq req);
    fw::CoRpcResp Exists(fw::CoRpcReq req);
    fw::CoRpcResp Ping(fw::CoRpcReq req);

    // 有限 deadline 探针：把 context().request()->deadline 的剩余预算回传。
    fw::CoRpcResp ProbeDeadline(fw::CoRpcReq req);

    // 过期请求路径：等父预算真实到期后，用父 deadline 调资源，先于 I/O 落定。
    fw::CoRpcResp ExpiredSet(fw::CoRpcReq req);

    // 关闭中在途：停在宿主注入的闸门上直到放行（真实 HTTP handler 在途）。
    fw::CoRpcResp Hold(fw::CoRpcReq req);

    static constexpr auto kRpcMethods = fw::RpcMethods(
        fw::Method<&KvSvc::Get>("get"),
        fw::Method<&KvSvc::Insert>("insert"),
        fw::Method<&KvSvc::Update>("update"),
        fw::Method<&KvSvc::Exists>("exists"),
        fw::Method<&KvSvc::Ping>("ping"),
        fw::Method<&KvSvc::ProbeDeadline>("probe_deadline"),
        fw::Method<&KvSvc::ExpiredSet>("expired_set"),
        fw::Method<&KvSvc::Hold>("hold"));

private:
    struct Resolved {
        std::shared_ptr<inf::CoRedisCli> cache;
        std::shared_ptr<inf::CoMongoCli> docs;
        co::Deadline                     deadline{};
    };
    struct ResolvedOrError {
        bool         ok{false};
        Resolved     res;
        fw::CoRpcResp resp;
    };
    static fw::CoRpcResp _Reply(std::int32_t status, std::string value);
    static fw::CoRpcResp _Missing(const char* name);
    ResolvedOrError _Res();
};

} // namespace fw8test
