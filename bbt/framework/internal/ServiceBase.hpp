#pragma once
/**
 * framework Service 类型擦除基（internal）。
 *
 * CoServiceBase 只承载与模板无关的面：Handle/Info、业务 hook 默认实现、组件
 * 能力集合的运行时判定与绑定表。类型化的 CoService<Components...> 与
 * GetComponent 在 Service.hpp 中；这里不引入第二套服务抽象。
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

/**
 * 监控写端（后续切片；仅前置声明）
 */
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
    ObjectInfo   Info() const;

protected:
    /**
     * @brief 受控构造，只接受 Registry 发放的 ConstructKey。
     */
    explicit CoServiceBase(detail::ConstructKey) {}
    /**
     * 仅经 Registry 类型化 deleter
     */
    ~CoServiceBase() noexcept override = default;

    /**
     * ---- 受管出站调用（机器面入口见 host/CoServiceCall.cc）----
     */
    /**
     * @brief 受管出站调用：call(service, method, CoRpcReq) → result<CoRpcResp>。
     *
     * 请求经 CoRpcReq::From/FromProto 构造（CoRpc.hpp）；custom 是有上限的
     * 拥有型路由字段；目标不经类型推导决定。envelope、deadline 适配、路由门、
     * 发送与回复校验全部在框架内部完成，业务拿不到发送注入点。
     *
     * 必须在受管协程内经 `bbt_co_await` 调用；`bbt_co_await` 先在协程环境
     * 检查（非协程抛 std::logic_error，发生在右侧实参与本函数体执行之前），
     * 再沿当前协程同步执行，期间可挂起；没有 Future/额外调度。
     *
     * @return 未托管/未启动 → RuntimeUnavailable；非受管协程 → InvalidContext；
     *         其余按出站链路错误边界透出。
     */
    bbt_co_async(result<CoRpcResp>) call(
        std::string_view service_name,
        std::string_view method_name,
        const CoRpcReq& request,
        const RouteFields& custom = {},
        const CallOptions& options = {});

    /**
     * @brief typed proto 出站调用（最小 typed seam）。
     *
     * request/response schema 取自生成物 descriptor::full_name()（.proto 唯一
     * 真源），payload 经业务特化的 rpc_detail::ProtoCodec<Req>/ProtoCodec<Resp>
     * 编解码；其余（envelope、deadline 适配、路由门、发送）与 CoRpcReq 路径
     * 共用 _CallSend 机器面。仅当 Req/Resp 有 ProtoCodec 特化时可用。
     *
     * 与 CoRpcReq 重载同一契约：受管协程内经 `bbt_co_await` 调用。
     */
    template <class Req, class Resp>
    bbt_co_async(result<Resp>) call(
        std::string_view service_name,
        std::string_view method_name,
        const Req& request,
        const RouteFields& custom = {},
        const CallOptions& options = {}) {
        static_assert(rpc_detail::is_proto_codec_v<Req> &&
                      rpc_detail::is_proto_codec_v<Resp>,
            "typed call requires ProtoCodec<Req>/ProtoCodec<Resp> specializations");
        auto encoded = rpc_detail::ProtoCodec<Req>::Encode(request);
        if (!encoded)
            return result<Resp>::err(std::move(encoded.error()));
        auto reply = bbt_co_await _CallSend(
            service_name, method_name,
            Req::descriptor()->full_name(), Resp::descriptor()->full_name(),
            std::move(encoded.value()), custom, options);
        if (!reply)
            return result<Resp>::err(std::move(reply.error()));
        return rpc_detail::ProtoCodec<Resp>::Decode(reply.value());
    }

    /**
     * 业务 hook 约定：OnInstall/OnUninstall 在调用协程内宿主域执行；
     * OnStart/OnPause/OnResume/OnStop 在受管协程执行（hook 任务持有对象
     * owner 作为 callback lease，超时只结束等待不悬空）。
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
     * @brief 暂停钩子。
     *
     * 调用点：CoApp::CoPause，按服务顺序执行。遇到首个失败即停止，并把该错误
     * 返回给调用方，其后的服务不再暂停。
     *
     * @return 成功返回 ok；失败返回的错误即暂停失败原因。
     */
    virtual result<void> OnPause() { return result<void>::ok(); }
    /**
     * @brief 恢复钩子。
     *
     * 调用点：CoApp::CoResume，按服务顺序执行。遇到首个失败即停止，并把该错误
     * 返回给调用方，其后的服务不再恢复。
     *
     * @return 成功返回 ok；失败返回的错误即恢复失败原因。
     */
    virtual result<void> OnResume() { return result<void>::ok(); }
    /**
     * @brief 停止钩子，在服务收口时于 borrow 排空后执行。
     *
     * 调用点：CoApp::CoStop 的逆序收口，以及 CoUninstallService。失败记入
     * LifecycleFailures 并保留 owner 待再次收口，不销毁对象。
     *
     * @return 成功返回 ok；失败返回的错误即停止失败原因。
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
     * @brief Actor 实例创建通知。
     *
     * 当前 HEAD 没有调用点，框架不会触发本 hook。
     */
    virtual void OnActorCreated(const ActorInfo&) {}
    /**
     * @brief Actor 实例销毁通知。
     *
     * 当前 HEAD 没有调用点，框架不会触发本 hook。
     */
    virtual void OnActorDestroyed(const ActorInfo&) {}
    /**
     * @brief 监控采集钩子。
     *
     * 当前 HEAD 没有调用点；监控写端由后续切片接入。
     */
    virtual void OnMonitorCollect(MonitorWriter&) {}
    /**
     * @brief 绑定变化钩子：本服务作为绑定发起方（subject）完成绑定/解绑后调用。
     *
     * CoBindComponent 成功以 bound=true 派发；CoUnbindComponent 在 borrow 排空
     * 完成后以 bound=false 派发。
     * 在 Registry 锁外同步执行（subject owner 以 callback lease 保活），异常被
     * 捕获并记入 LifecycleFailures，绑定不回滚。
     *
     * @param event 绑定事件值快照。
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
     * subject 绑定表与 _FindBoundCopy/_BindComponentEntry 等簿记由
     * detail::ObjectBase 承载（CoComponent/CoActor/CoService 共用同一份实现）。
     */

    /**
     * 出站发送注入点（机器面私有别名）：宿主在启动绑定路径供给，
     * 路由门（AppRuntimeState::FindRoute）在宿主侧实现——业务不可命名、
     * 不可注入。空注入点时 call 如实 RuntimeUnavailable。
     *
     * 返回值是协程等待语法的包装：真实出站会挂起当前协程，注入点与
     * 其整条下游链按协程可挂起函数声明，消费端经 bbt_co_await 解包。
     */
    using RpcSendFn = std::function<bbt_co_async(result<bbt::infra::rpc::Response>)(
        const bbt::infra::rpc::Request&,
        const bbt::infra::CallOptions&)>;

    /**
     * 宿主注入出站发送注入点；仅 CoApp 启动/安装绑定路径可建立。
     */
    void _BindNetwork(RpcSendFn send) { m_send = std::move(send); }

    /**
     * call 的机器面半身（host/CoServiceCall.cc）：受管校验、请求
     * 上下文、envelope 组装、有序票据元数据、deadline 适配、
     * 出站发送与回复校验；成功返回回复 payload 字节。
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
