#pragma once
// service-actor/v2：CoApp 宿主装配（业务面）。
//
// 业务路径：
//   fw::CoApp app{opts};              // 宿主/出站/线桥全部由框架装配
//   app.add_service<EchoSvc>(opts);   // 服务注册
//   app.run();                        // 启动 → 接纳 → 关闭 → 返回码
//   app.request_shutdown();           // 任意线程可调用
// 业务不注入 network_host、不提供 rpc_send、不见 envelope/Codec——
// 默认装配使用 internal::InfraHttpHost + RpcHttpBridge + HttpEgress
// （真实 infra HTTP loopback 传输）；测试替换经 internal/CoAppSeam.hpp。
//
//  - add_service<T>(ServiceOptions) 注册服务：T 为 ICoService 派生、默认
//    构造；ServiceOptions 必须显式携带执行策略（F2-a 装配期校验 +
//    ValidateActorKeying）。
//  - add_resource<R>(ptr)：按类型装配资源客户端（CoRedisCli 等资源
//    缝的登记口；服务经 context().resource<R>() 取用）。
//  - run() -> int：负责启动、接纳、调度与正常关闭，顺序由
//    internal::HostLifecycle 状态机保证。进程内同一时刻至多一个活跃
//    CoApp；同一 CoApp 至多 run 一次。
//  - 启动前校验：运行时配置与静态路由在 run 前校验；默认不存在
//    「自动连任意服务」——find_route 只认显式配置的静态路由。

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/NetworkTypes.hpp>

#include <bbt/framework/ExecutionPolicy.hpp>
#include <bbt/framework/ICoService.hpp>
#include <bbt/framework/OrderedTypes.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/ServiceContext.hpp>
#include <bbt/framework/ShutdownState.hpp>
#include <bbt/framework/internal/MethodTable.hpp>

namespace bbt::framework {

class ActorRegistry;
class HostLifecycle;
class HttpEgress;
class InboundDispatcher;
class InfraHttpHost;
class OrderedIngress;
class INetworkHost;
struct CoAppOptions;
struct CoAppSeam;

// 内部入口友元（internal/** 声明）：测试缝构造与默认入站线桥安装。
std::unique_ptr<CoApp> MakeCoAppForTest(CoAppOptions options,
                                        CoAppSeam seam);
void InstallRpcHttpBridge(InfraHttpHost& host, CoApp& app);

// 显式静态路由项：service_name → 已配置的出站地址。
struct StaticRoute {
    std::string              service_name;
    bbt::infra::RpcAddress   address;
};

// 运行时配置（全部字段必须显式填写，与 ServiceOptions 同一约定）：
//  - network_limits：透传给网络依赖组件的限额（ValidateNetworkLimits）；
//  - listen：默认宿主的监听地址（port==0 时内核分配，run 后
//    bound_endpoint() 可读）；
//  - static_routes：出站目标白名单，空名单 = 不允许任何出站目标；
//  - shutdown_step_budget：关闭期每个等待步的有限预算（F3）。
struct CoAppOptions {
    bbt::infra::NetworkLimits network_limits;
    bbt::infra::ListenAddress listen;
    std::vector<StaticRoute>  static_routes;
    std::chrono::milliseconds shutdown_step_budget;
};

class CoApp {
    // 测试缝构造仅经 internal MakeCoAppForTest 到达；默认入站线桥
    // 经 InstallRpcHttpBridge 安装，两者均在 internal/** 声明。
    friend std::unique_ptr<CoApp> MakeCoAppForTest(CoAppOptions,
                                                   CoAppSeam);
    friend void InstallRpcHttpBridge(InfraHttpHost&, CoApp&);
    // 测试侧入站分发观察口（internal/CoAppSeam.hpp）：转发私有
    // dispatch_inbound，不新增业务可见面。
    friend result<bbt::infra::RpcEnvelope> DispatchInboundForTest(
        CoApp&, const bbt::infra::IncomingCallContext&,
        bbt::infra::RpcEnvelope);

public:
    // 业务路径：宿主（InfraHttpHost）、入站线桥（RpcHttpBridge）与
    // 出站（HttpEgress）全部由框架默认装配。
    explicit CoApp(CoAppOptions options);
    ~CoApp();

    CoApp(const CoApp&) = delete;
    CoApp& operator=(const CoApp&) = delete;

    // 注册服务。校验：ServiceOptions 合法 + ActorSerial 全方法有 key
    // 提取器 + 服务名非空且不重复 + 注册窗口仍开放（run 开始后拒绝）。
    // 失败返回 err(InvalidArgument/Closed)，不登记半成品。
    template <class T>
    result<void> add_service(ServiceOptions options) {
        static_assert(std::is_base_of_v<ICoService, T>,
            "CoApp::add_service: T 必须继承 ICoService");
        static_assert(std::is_default_constructible_v<T>,
            "CoApp::add_service: 业务类型必须默认可构造");

        if (auto r = ValidateServiceOptions(options); !r)
            return r;

        RpcMethodTable table;
        try {
            table = BuildMethodTable<T>();
        } catch (const std::invalid_argument& e) {
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                std::string("add_service: ") + e.what()));
        } catch (const std::exception& e) {
            return result<void>::err(MakeError(ErrorCode::InternalError,
                std::string("add_service: method table failed: ") +
                e.what()));
        }
        if (auto r = ValidateActorKeying(table, options.execution); !r)
            return r;

        T proto;    // 原型仅取服务名；构造期不得发起 I/O
        const std::string name{proto.co_service_name()};
        if (name.empty())
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "add_service: empty service name"));

        ServiceEntry entry;
        entry.options = options;
        entry.table   = std::move(table);
        entry.factory = []() -> std::shared_ptr<ICoService> {
            return std::static_pointer_cast<ICoService>(
                std::make_shared<T>());
        };

        std::lock_guard<std::mutex> lk(m_mtx);
        if (!m_registration_open)
            return result<void>::err(MakeError(ErrorCode::Closed,
                "add_service: registration closed (run already started)"));
        if (m_services.count(name) != 0)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "add_service: duplicate service '" + name + "'"));
        m_services.emplace(name, std::move(entry));
        return result<void>::ok();
    }

    // 资源缝登记：旧重载装配无名默认资源；命名重载以 (类型, 名称)
    // 为键。公开命名 API 拒绝空名；重复键/空指针 → InvalidArgument；
    // run 开始后 → Closed。本框架不提供数据库客户端实现。
    template <class R>
    result<void> add_resource(std::shared_ptr<R> resource) {
        return _AddResource<R>(kDefaultResourceName,
            std::move(resource));
    }

    template <class R>
    result<void> add_resource(std::string_view name,
                              std::shared_ptr<R> resource) {
        if (name.empty())
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "add_resource: empty resource name"));
        return _AddResource<R>(name, std::move(resource));
    }

    // 延迟工厂装配：factory 的 Create 在 Scheduler 启动后、绑定 Service
    // 之前执行（on_scheduler_started 相位），满足「Create 要求 Scheduler
    // 已启动」的资源形态（如 infra CoRedisCli/CoMongoCli）。factory 须返回
    // result<std::shared_ptr<R>>；创建出的实例若暴露 Start() 且该返回
    // result<void> 则由框架在登记后调用一次，若实现
    // bbt::infra::ICoCloseable 则由关闭序列统一 RequestClose/WaitClosed。
    // 任一资源创建/启动失败 → 启动失败回退，不留半装配资源。
    // name 约束与 add_resource(ptr) 相同；重复键/空工厂 → InvalidArgument。
    template <class R, class F,
              class = std::enable_if_t<
                  std::is_invocable_r_v<result<std::shared_ptr<R>>, F>>>
    result<void> add_resource(std::string_view name, F&& factory) {
        if (name.empty())
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "add_resource: empty resource name"));
        return _AddResourceFactory<R>(name, std::forward<F>(factory));
    }

    // 出站目标解析（「自动连任意服务」不存在的落点）：只认显式配置的
    // 静态路由；未配置目标 → err(NotFound)。run 前后均可调用；
    // 出站分发必须经此门，不允许绕过。
    result<bbt::infra::RpcAddress> find_route(
        std::string_view service_name) const;

    // 安装接收端有序流授权。仅允许在 run() 前调用；run 开始后返回 Closed。
    // service 必须已注册且启用 ordered_ingress，授权字段由调用方提供，
    // 不自动创建未知服务或隐式放宽其他匹配条件。
    result<void> grant_ordered_stream(OrderedGrant grant);

    // 已托管实例观察口：Concurrent 服务返回启动期绑定的单实例；
    // ActorSerial 服务返回 err(InvalidArgument)（实例按 key 经
    // actor_registry() 激活）；未注册名 → err(NotFound)；
    // 尚未 run/已关闭 → err(RuntimeUnavailable)。
    result<std::shared_ptr<ICoService>> find_service(
        std::string_view service_name) const;

    // ActorSerial 实例注册表（分发/测试观察用）；未启动 → nullptr。
    ActorRegistry* actor_registry() const noexcept;

    // 控制线程入口：启动 → 等待关闭请求 → 收束 → 返回。
    // 返回码即 internal::HostLifecycle::kExit*。进程内同一时刻至多一个
    // 活跃 CoApp；同一 CoApp 至多 run 一次；违反 → kExitRejected。
    int run();

    // 契约 F3：外部控制线程请求关闭。幂等、noexcept；早于 run 的调用
    // 使 run 完成启动后立即进入关闭序列。
    void request_shutdown() noexcept;

    // 契约 F3：当前关闭状态。run 未开始/已结束分别报 Running/Closed。
    ShutdownState shutdown_state() const noexcept;

    // 收尾未完成项与失败项观测（转发自生命周期状态机）。
    std::vector<std::string> pending_cleanup() const;
    std::vector<std::string> lifecycle_failures() const;

    // 默认宿主的实际绑定接入点（"host:port"）；仅默认装配路径有值，
    // 未监听/测试缝 → 空串。
    std::string bound_endpoint() const;

private:
    struct ServiceEntry {
        ServiceOptions options;
        RpcMethodTable table;
        std::function<std::shared_ptr<ICoService>()> factory;
        std::shared_ptr<OrderedIngress> ordered_ingress;
    };

    // 资源装配记录：登记期建立，运行期只读（关闭序列消费 closeable）。
    // factory 为空表示 shared_ptr 预创建入口；instance 始终持有已装配实例
    // （预创建即登记时填入，工厂入口在启动成功后填入），供关闭序列与
    // ShutdownIncomplete 期间强持有。
    struct ResourceSpec {
        std::function<result<std::shared_ptr<void>>()> factory;
        // 类型擦除的 Start 调用（仅当 R 提供 result<void> Start()）；
        // 与类型擦除的 ICoCloseable 视图（仅当 R 继承 ICoCloseable）。
        std::function<result<void>(const std::shared_ptr<void>&)> start;
        std::function<std::shared_ptr<bbt::infra::ICoCloseable>(
            const std::shared_ptr<void>&)>                      as_closeable;
        std::shared_ptr<void>                                 instance;
        bool closed{false};   // 关闭序列幂等标记
    };
    using ResourceSpecMap = std::unordered_map<ResourceKey, ResourceSpec,
                                               ResourceKeyHash>;

    // 检测 R 是否提供 result<void> R::Start()（资源可选启动口）。
    template <class R, class = void>
    struct HasResourceStart : std::false_type {};
    template <class R>
    struct HasResourceStart<R,
        std::void_t<decltype(std::declval<R*>()->Start())>>
        : std::is_same<decltype(std::declval<R*>()->Start()), result<void>> {};

    template <class R>
    static ResourceSpec _MakeSpec() {
        ResourceSpec spec;
        if constexpr (HasResourceStart<R>::value) {
            spec.start = [](const std::shared_ptr<void>& p) -> result<void> {
                return std::static_pointer_cast<R>(p)->Start();
            };
        }
        if constexpr (std::is_base_of_v<bbt::infra::ICoCloseable, R>) {
            spec.as_closeable =
                [](const std::shared_ptr<void>& p)
                    -> std::shared_ptr<bbt::infra::ICoCloseable> {
                return std::static_pointer_cast<bbt::infra::ICoCloseable>(
                    std::static_pointer_cast<R>(p));
            };
        }
        return spec;
    }

    template <class R>
    result<void> _AddResource(std::string_view name,
                              std::shared_ptr<R> resource) {
        if (!resource)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "add_resource: null resource"));
        std::lock_guard<std::mutex> lk(m_mtx);
        if (!m_registration_open)
            return result<void>::err(MakeError(ErrorCode::Closed,
                "add_resource: registration closed (run already started)"));
        ResourceKey key{detail::ResourceTypeId<R>(), std::string{name}};
        ResourceSpec spec = _MakeSpec<R>();
        spec.instance = std::move(resource);
        const std::shared_ptr<void> inst = spec.instance;
        if (!m_resource_specs.emplace(key, std::move(spec)).second)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "add_resource: duplicate resource key"));
        // 预创建实例立即可见：m_resources 是 service 的实例视图。
        m_resources->emplace(std::move(key), std::move(inst));
        return result<void>::ok();
    }

    template <class R, class F>
    result<void> _AddResourceFactory(std::string_view name, F&& factory) {
        // 归一成 std::function 再判空：lambda 等 callable 无 operator!，
        // std::function 的显式 bool 转换才表达「空工厂」。
        std::function<result<std::shared_ptr<R>>()> fn =
            std::forward<F>(factory);
        if (!fn)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "add_resource: null factory"));
        std::lock_guard<std::mutex> lk(m_mtx);
        if (!m_registration_open)
            return result<void>::err(MakeError(ErrorCode::Closed,
                "add_resource: registration closed (run already started)"));
        ResourceKey key{detail::ResourceTypeId<R>(), std::string{name}};
        ResourceSpec spec = _MakeSpec<R>();
        spec.factory =
            [f = std::move(fn)]() mutable
                -> result<std::shared_ptr<void>> {
                auto r = f();
                if (!r)
                    return result<std::shared_ptr<void>>::err(
                        std::move(r.error()));
                return result<std::shared_ptr<void>>::ok(
                    std::shared_ptr<void>(std::move(r.value())));
            };
        if (!m_resource_specs.emplace(std::move(key), std::move(spec)).second)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "add_resource: duplicate resource key"));
        return result<void>::ok();
    }

    // 测试缝出站签名（internal/CoAppSeam.hpp 的 RpcSendAppFn）；
    // 本头不复述机器面签名，私有别名保持公共面干净。
    using SeamEgressFn = std::function<result<bbt::infra::RpcEnvelope>(
        const bbt::infra::RpcAddress&,
        const bbt::infra::RpcEnvelope&,
        const bbt::infra::CallOptions&)>;

    // 测试缝构造：显式注入宿主与出站发送；仅 MakeCoAppForTest 可达。
    CoApp(CoAppOptions options, CoAppSeam seam);

    // 入站分发入口（机器面）：由入站线桥（InstallRpcHttpBridge）在
    // 受管协程内调用；未启动/已收束 → err(RuntimeUnavailable)。
    // 业务不直接调用本入口。
    result<bbt::infra::RpcEnvelope> dispatch_inbound(
        const bbt::infra::IncomingCallContext& incoming,
        bbt::infra::RpcEnvelope request);

    result<void> _ValidateConfig() const;   // run 前校验，不启动任何组件
    result<void> _BindServices();           // Scheduler::Start 后建立对象身份
    // 资源生命周期（#8）：on_scheduler_started 相位内先 _StartResources
    // （factory Create→Start→登记实例视图）再 _BindServices；关闭序列在
    // handler 排空后（on_handlers_drained）与回退路径（on_release）统一
    // 经 _CloseResources 收束。两者幂等（ResourceSpec::closed）。
    result<void> _StartResources();
    void         _CloseResources(HostLifecycle& lifecycle) noexcept;
    void         _ReleaseServices() noexcept;
    // find_route 路由门 + 出站实现（ICoService::RpcSendFn 是
    // ICoService 的私有别名；CoApp 经友元命名）。
    ICoService::RpcSendFn _MakeEgress();

    const CoAppOptions              m_options;
    std::shared_ptr<INetworkHost>   m_network_host;
    // 默认装配路径的具体宿主（弱持有观察；生命周期归 m_network_host）
    // 与默认出站实现；测试缝注入路径两者皆空。
    std::shared_ptr<InfraHttpHost>  m_default_host;
    std::shared_ptr<HttpEgress>     m_default_egress;
    SeamEgressFn                    m_seam_send;

    mutable std::mutex              m_mtx;
    std::map<std::string, ServiceEntry>   m_services;
    std::map<std::string, bbt::infra::RpcAddress> m_routes;
    std::shared_ptr<ResourceMap>    m_resources{
        std::make_shared<ResourceMap>()};
    // 资源装配记录表（登记期写入，启动期消费 factory、关闭期消费
    // closeable/instance/closed）；与 m_resources 同受 m_mtx 保护。
    ResourceSpecMap                 m_resource_specs;
    bool                            m_registration_open{true};

    // 运行期持有的受管对象（ShutdownIncomplete 期间仍强持有）。
    std::map<std::string, std::shared_ptr<ICoService>> m_instances;
    std::unique_ptr<ActorRegistry>  m_actor_registry;

    std::atomic_bool                m_ran_once{false};
    std::atomic_bool                m_shutdown_req{false};
    std::atomic<HostLifecycle*>     m_lifecycle{nullptr};
    std::atomic<ShutdownState>      m_parked_state{ShutdownState::Running};
    std::vector<std::string>        m_early_failures;   // run 前校验失败原因
    // 入站分发器：_BindServices 建立、_ReleaseServices 先于
    // 注册表/实例销毁；指针在 m_mtx 下读写。
    std::unique_ptr<InboundDispatcher> m_dispatcher;
};

} // namespace bbt::framework
