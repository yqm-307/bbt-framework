#pragma once
/**
 * @brief 服务共用的生命周期回调和出站 RPC 接口。业务从 CoService 派生。
 */

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <typeinfo>
#include <vector>

#include <bbt/infra/rpc/Options.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>

#include <bbt/coroutine/syntax/Async.hpp>

#include <bbt/framework/CallOptions.hpp>
#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/Object.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/Route.hpp>
#include <bbt/framework/internal/ObjectBase.hpp>

namespace bbt::framework {

class MonitorWriter;

/**
 * @brief Service 的类型擦除基类。
 *
 * @par 职责
 * 提供服务身份观察面、业务生命周期与绑定 hook 的默认实现、组件能力集合的运行时
 * 判定，以及 CoApp 绑定组件后回填的绑定表。
 *
 * @par 不负责
 * 不提供模板化的 GetComponent；类型化访问由 CoService<Components...> 负责。
 *
 * @par 约束
 * 业务通过 CoService<Components...> 派生使用，不得直接构造或销毁
 * CoServiceBase 派生对象。
 */
class CoServiceBase : public detail::ObjectBase {
public:
    /**
     * @brief 返回本服务的非拥有对象句柄。
     */
    ObjectHandle Handle() const { return _ObjectHandle(); }
    /**
     * @brief 返回本服务当前身份与状态的值快照。
     */
    ObjectInfo Info() const;

protected:
    /**
     * @brief 受控构造，只接受 Registry 发放的 ConstructKey。
     */
    explicit CoServiceBase(detail::ConstructKey) {}
    ~CoServiceBase() noexcept override = default;

    /**
     * @brief 按服务名和方法名发送请求，等待回复。custom 只传路由字段。
     * 在受管请求的协程内使用 bbt_co_await；等待时可挂起，不会另起任务。
     * 在普通线程使用 bbt_co_await 会先抛出 std::logic_error，不会进入此函数。
     *
     * @return 服务没有发送通道时返回 RuntimeUnavailable；没有当前受管请求时返回 InvalidContext。
     * 期限、路由、发送和回复错误按实际原因返回。
     */
    bbt_co_async(result<CoRpcResp>) call(
        std::string_view service_name,
        std::string_view method_name,
        const CoRpcReq& request,
        const RouteFields& custom = {},
        const CallOptions& options = {});

    /**
     * @brief 发送 protobuf 请求并将回复解码为 Resp。
     * Req 和 Resp 都须提供 ProtoCodec；类型名取自各自的 protobuf descriptor。
     * 和接受 CoRpcReq 的重载一样，在受管请求的协程中用 bbt_co_await 调用。
     */
    template <class Req, class Resp>
    bbt_co_async(result<Resp>) call(
        std::string_view service_name,
        std::string_view method_name,
        const Req& request,
        const RouteFields& custom = {},
        const CallOptions& options = {});

    /**
     * OnInstall 和 OnUninstall 在发起操作的协程中同步执行。其他生命周期回调在受管协程中执行；
     * 等待超时不会取消正在执行的回调，框架会保留对象直到回调结束。
     */

    /**
     * @brief 安装钩子，在服务登记之后、Installed 之前执行。
     *
     * 调用点：CoApp::CoInstallService，在 ServiceOptions 声明的组件绑定展开
     * 之后运行。同步执行于宿主控制面，不得做 I/O。
     * 失败时服务进入 InstallFailed（仍可 Find/卸载），错误写入 last_error
     * 并记入 LifecycleFailures；之前已成功应用的声明绑定保持登记、
     * 不自动解绑（与契约「不返回全部回滚」一致）。
     *
     * @return 成功返回 ok；失败返回的错误即安装失败原因。
     */
    virtual result<void> OnInstall() { return result<void>::ok(); }
    /**
     * @brief 启动钩子。
     *
     * 调用点：CoApp::CoStart（组件之后，按安装顺序），以及 CoInstallService
     * 时 App 已处于 Running。失败时服务进入 Failed，错误记入 LifecycleFailures。
     *
     * @return 成功返回 ok；失败返回的错误即启动失败原因。
     */
    virtual result<void> OnStart() { return result<void>::ok(); }
    /**
     * @brief 按服务顺序暂停。
     * @return 第一个返回的错误会中止暂停，后续服务保持原状态。
     */
    virtual result<void> OnPause() { return result<void>::ok(); }
    /**
     * @brief 按服务顺序恢复。
     * @return 第一个返回的错误会中止恢复，后续服务保持原状态。
     */
    virtual result<void> OnResume() { return result<void>::ok(); }
    /**
     * @brief 停止应用或卸载服务时，在已有借用归还后调用。
     * @return 失败时记录 LifecycleFailures 并保留服务，以便重试。
     */
    virtual result<void> OnStop() { return result<void>::ok(); }
    /**
     * @brief 卸载钩子，在 OnStop 之后、服务登记移除前执行。
     *
     * 调用点：服务销毁路径（Stop 收口与 CoUninstallService）。无返回值；抛出的
     * 异常由框架捕获并记入 LifecycleFailures。
     */
    virtual void OnUninstall() {}
    /**
     * @brief 预留的 Actor 创建通知；框架目前不会自动调用。
     */
    virtual void OnActorCreated(const ActorInfo&) {}
    /**
     * @brief 预留的 Actor 销毁通知；框架目前不会自动调用。
     */
    virtual void OnActorDestroyed(const ActorInfo&) {}
    /**
     * @brief 预留的监控回调；框架目前不会自动调用。
     */
    virtual void OnMonitorCollect(MonitorWriter&) {}
    /**
     * @brief 绑定生效后通知 bound=true；解绑要等所有借用归还后才通知 bound=false。
     * 回调在注册表锁外同步执行，框架保证当前对象活到回调结束。异常记入 LifecycleFailures，不撤销绑定或解绑。
     *
     * @param event 本次绑定变化的快照。
     */
    virtual void OnBindingChanged(const BindingEvent&) {}

    /**
     * @brief 判定类型是否属于派生模板声明的组件能力集合。
     *
     * 由 CoService<Components...> 展开 typeid 列表实现。
     *
     * @param t 待判定的组件类型。
     * @return 类型在声明集合内时返回 true。
     */
    virtual bool _AllowsComponent(const std::type_info& t) const noexcept = 0;

private:
    /**
     * 宿主提供的发送函数负责查路由并发送请求。等待回复时可挂起当前协程。
     */
    using RpcSendFn = std::function<bbt_co_async(result<bbt::infra::rpc::Response>)(
        const bbt::infra::rpc::Request&,
        const bbt::infra::CallOptions&)>;

    void _BindNetwork(RpcSendFn send) { m_send = std::move(send); }

    /**
     * 两种 call 重载共用此处的期限检查、有序票据绑定、发送和回复校验。
     */
    bbt_co_async(result<std::vector<std::uint8_t>>) _CallSend(
        std::string_view service_name,
        std::string_view method_name,
        std::string_view request_schema,
        std::string_view response_schema,
        std::vector<std::uint8_t> payload,
        const RouteFields& custom,
        const CallOptions& options);

    std::string _NextRequestId() const;

    RpcSendFn m_send;

    /**
     * 进程内单调的调用序号：request_id 的非票据来源（有序调用用票据
     * request_id）。inline 静态成员全体实例化共享，保证跨模板唯一。
     */
    inline static std::atomic<std::uint64_t> s_call_seq{0};

    friend class detail::Registry;
    friend class CoApp;
};

} // namespace bbt::framework

#include <bbt/framework/internal/__TServiceBase.hpp>
