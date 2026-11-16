#pragma once
/**
 * 联合 live 验收的业务 Service——只依赖 framework/infra/coroutine **公开头**。
 *
 * 本头与其实现 framework_resource_service.cc 构成独立编译单元，用于提供
 * 「干净消费者 Service 只 include framework/infra 公共头即可经声明式绑定
 * + Borrow 使用资源」的真实编译证据：实现 .cc 只 include 公开头 + stdlib，
 * 编译产生的 include 闭包中不得出现 bbt/framework/internal/* 或任何第三方
 * driver（hiredis/mongocxx/bsoncxx）。测试宿主（真实 HTTP 装配、线桥编码）
 * 在 Test_framework_resource_combined.cc 中另持 internal；二者分属不同 TU，
 * 不得以「同一 TU 先 include 公共头」替代隔离证据。
 *
 * 业务实现只经 GetComponent<R>(name)->Borrow() 取用命名资源（服务以
 * ServiceOptions::BindComponent 声明 "cache"/"docs" 绑定，由宿主安装时
 * 展开）与 CurrentRequestContext() 读请求上下文；不接触连接/线程/连接池，
 * 也不持有全局测试状态。
 */

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

#include <bbt/infra/mongo/Client.hpp>
#include <bbt/infra/NetworkTypes.hpp>

/**
 * 新 mongo owner/句柄入口：框架侧资源适配器（owner Close 是同步物理收口，
 * 句柄 Close 只封该集合接纳；公开头不泄漏 mongocxx/bsoncxx）。
 */
#include <bbt/framework/MongoDbResource.hpp>

/**
 * 新 net Redis 入口：经框架侧资源适配器（公开头，engine+显式 Connect 收口
 * 归资源本体，本 TU 不见 hiredis/Engine 类型实体——头内仅前向声明）。
 */
#include <bbt/framework/RedisNetResource.hpp>

#include <bbt/framework/Framework.hpp>

namespace fw8test {

namespace fw  = bbt::framework;
namespace co  = bbt::coroutine;
namespace inf = bbt::infra;

/**
 * KvSvc：联合业务 Service（公共面消费 Redis + Mongo）。
 * 回复统一为位置参数 (int32 status, string value)：
 * get     status 0=HIT(值在 value) / 1=NOTFOUND
 * insert/update  status 0 或 UpdateOne 的 matched 计数
 * exists  status 0/1
 * ping    成功返回 (0,"PONG")
 * 其余    仅成功时返回 (0,...)，错误经 CoRpcResp::Error 原样回传。
 */
class KvSvc final : public fw::CoService<fw::RedisNetResource,
                                         fw::MongoDbResource> {
public:
    static constexpr std::string_view kServiceName = "kv";

    explicit KvSvc(fw::detail::ConstructKey k)
        : fw::CoService<fw::RedisNetResource, fw::MongoDbResource>(k) {}

    /**
     * 资源缝观察口（宿主断言共享/隔离与 IsClosed 终态用）：返回受管
     * borrow，宿主在协程域内经 Get() 取资源本体读 IsClosed/engine_stopped
     * 等观测口。borrow 用后即弃，不存出。
     */
    fw::result<fw::ComponentBorrow<fw::RedisNetResource>> Cache() {
        return Res<fw::RedisNetResource>("cache");
    }
    fw::result<fw::ComponentBorrow<fw::MongoDbResource>> Docs() {
        return Res<fw::MongoDbResource>("docs");
    }

    /**
     * 经 Service 侧真实调用计数，让「cache hit 未再查 Mongo」可观察而非猜测。
     * 静态成员：新模型下宿主测试拿不到 Service 实例（FindService 只发非
     * 拥有 handle），观察状态经静态成员注入。
     */
    static inline std::atomic<int> s_mongo_reads{0};

    /**
     * 显式测试钩子（静态成员注入，同上原因）：关闭中在途用例的闸门与
     * 「放行后真实资源操作完成」通知由宿主测试装配注入/放行。业务实现
     * 不持有全局测试状态、不管理线程或连接（排空语义由框架保证）。
     * 为空表示该钩子未装配。
     * - s_hold_gate：Hold 进入后调用（阻塞在宿主闸门上，真实 HTTP
     *   handler 在途）；
     * - s_hold_op_report：Hold 放行后经 Borrow 取 Redis/Mongo 执行真实
     *   操作，操作结果（true=成功）经此回调报告给独立持有的观察者，而不经
     *   Service 实例本身——避免测试持 Service 指针与关闭释放竞争。
     */
    static inline std::function<void()>     s_hold_gate;
    static inline std::function<void(bool)> s_hold_op_report;

    fw::CoRpcResp Get(fw::CoRpcReq req);
    fw::CoRpcResp Insert(fw::CoRpcReq req);
    fw::CoRpcResp Update(fw::CoRpcReq req);
    fw::CoRpcResp Exists(fw::CoRpcReq req);
    fw::CoRpcResp Ping(fw::CoRpcReq req);

    /**
     * 有限 deadline 探针：把当前请求 deadline 的剩余预算回传。
     */
    fw::CoRpcResp ProbeDeadline(fw::CoRpcReq req);

    /**
     * 过期请求路径：等父预算真实到期后，用父 deadline 调资源，先于 I/O 落定。
     */
    fw::CoRpcResp ExpiredSet(fw::CoRpcReq req);

    /**
     * 关闭中在途：停在宿主注入的闸门上直到放行（真实 HTTP handler 在途）。
     */
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

    template <class R>
    fw::result<fw::ComponentBorrow<R>> Res(std::string_view name) {
        auto h = GetComponent<R>(name);
        if (!h)
            return fw::result<fw::ComponentBorrow<R>>::err(
                std::move(h.error()));
        return h.value().Borrow();
    }

    static fw::result<co::Deadline> CtxDeadline();

protected:
    ~KvSvc() noexcept override = default;

private:
    static fw::CoRpcResp _Reply(std::int32_t status, std::string value);
    static fw::CoRpcResp _Missing(const char* name);

    friend class fw::detail::Registry;
};

} // namespace fw8test
