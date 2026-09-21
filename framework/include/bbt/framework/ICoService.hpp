#pragma once
// service-actor/v2：服务接口边界（业务面）。
//
// 业务只感知三件事：
//  - co_service_name()：服务名（CoService<T> 从 kServiceName 供给）；
//  - this->call<Reply>(service, method, request, ...)：受管出站调用——
//    envelope、deadline/cancel 适配、发送与回复校验全部在框架内部完成
//    （src/host/CoServiceCall.cc），业务拿不到 Codec/envelope/发送注入点；
//  - context()：服务上下文（请求字段只读 + 资源缝）。
// 对象身份/运行时绑定只能由 CoApp 建立；未托管对象调用 this->call
// 返回 RuntimeUnavailable，非受管协程返回 InvalidContext。
//
// 方法表由 T::kRpcMethods 声明、CoApp::add_service 展开（声明糖见
// RpcMethods.hpp），co_bind_rpc/binder 不再是业务面 API。

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include <bbt/infra/Codec.hpp>
#include <bbt/infra/ICoNetwork.hpp>
#include <bbt/infra/NetworkTypes.hpp>

#include <bbt/framework/CallOptions.hpp>
#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/OrderedSession.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/Route.hpp>
#include <bbt/framework/ServiceContext.hpp>

namespace bbt::framework {

class RpcMethodTable;
class CoApp;
class ActorRegistry;

class ICoService : public bbt::infra::ICoNetwork {
public:
    virtual ~ICoService() = default;
    virtual std::string_view co_service_name() const noexcept = 0;
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override;
    std::optional<std::string> co_actor_key() const;

protected:
    // call(service, method, CoRpcReq) → result<CoRpcResp>，请求经
    // CoRpcReq::From/FromProto 构造（CoRpc.hpp）。custom 是有上限的
    // 拥有型路由字段；目标不经类型推导决定。
    [[nodiscard]] result<CoRpcResp> call(
        std::string_view service_name,
        std::string_view method_name,
        const CoRpcReq& request,
        const RouteFields& custom = {},
        const CallOptions& options = {});

    // 服务上下文：请求字段只读 + 资源缝（见 ServiceContext.hpp）。
    ServiceContext&       context() noexcept { return m_context; }
    const ServiceContext& context() const noexcept { return m_context; }

private:
    // 出站发送注入点（机器面私有别名）：宿主在 _bind_runtime 供给，
    // 路由门（find_route）在宿主侧实现——业务不可命名、不可注入。
    using RpcSendFn = std::function<result<bbt::infra::RpcEnvelope>(
        const bbt::infra::RpcEnvelope&,
        const bbt::infra::CallOptions&)>;

    // 宿主注入运行时身份、actor key、出站发送注入点与资源视图；
    // 仅 CoApp 可建立。
    void _bind_runtime(bbt::coroutine::CoObjectInfo info,
                       std::optional<std::string> actor_key,
                       RpcSendFn send,
                       std::shared_ptr<const ResourceMap> resources);
    // F2-a：Actor 实例创建时绑定 key；仅 ActorRegistry 可建立。
    void _bind_actor_key(std::optional<std::string> actor_key);

    std::string _NextRequestId();

    // call 的机器面半身（src/host/CoServiceCall.cc）：受管校验、请求
    // 上下文、envelope 组装、有序票据元数据、deadline/cancel 适配、
    // 出站发送与回复校验；成功返回回复 payload 字节。
    result<std::vector<std::uint8_t>> _CallSend(
        std::string_view service_name,
        std::string_view method_name,
        std::string_view request_schema,
        std::string_view response_schema,
        std::vector<std::uint8_t> payload,
        const RouteFields& custom,
        const CallOptions& options);

    bbt::coroutine::CoObjectInfo m_object_info{};   // id/generation 由宿主赋
    std::optional<std::string>   m_actor_key;
    RpcSendFn                    m_send;
    ServiceContext               m_context;

    // 进程内单调的调用序号：request_id 的非票据来源（有序调用用票据
    // request_id）。inline 静态成员全体实例化共享，保证跨模板唯一。
    inline static std::atomic<std::uint64_t> s_call_seq{0};

    friend class CoApp;
    friend class ActorRegistry;
};

inline bbt::coroutine::CoObjectInfo ICoService::GetObjectInfo() const {
    bbt::coroutine::CoObjectInfo info = m_object_info;
    info.kind = "service";
    info.name = std::string(co_service_name());
    return info;
}

inline std::optional<std::string> ICoService::co_actor_key() const {
    return m_actor_key;
}

inline void ICoService::_bind_runtime(
    bbt::coroutine::CoObjectInfo info,
    std::optional<std::string> actor_key,
    RpcSendFn send,
    std::shared_ptr<const ResourceMap> resources) {
    m_object_info = std::move(info);
    m_actor_key   = std::move(actor_key);
    m_send        = std::move(send);
    m_context._bind_resources(std::move(resources));
}

inline void ICoService::_bind_actor_key(
    std::optional<std::string> actor_key) {
    m_actor_key = std::move(actor_key);
}

inline std::string ICoService::_NextRequestId() {
    return "req-" + std::to_string(m_object_info.id) + "-" +
        std::to_string(
            s_call_seq.fetch_add(1, std::memory_order_relaxed) + 1);
}

inline auto ICoService::call(
    std::string_view service_name,
    std::string_view method_name,
    const CoRpcReq& request,
    const RouteFields& custom,
    const CallOptions& options) -> result<CoRpcResp> {
    auto reply = _CallSend(service_name, method_name,
        kCoRpcPositionalSchema, kCoRpcPositionalSchema,
        std::vector<std::uint8_t>(request.payload().begin(),
                                  request.payload().end()),
        custom, options);
    if (!reply)
        return result<CoRpcResp>::err(std::move(reply.error()));
    return result<CoRpcResp>::ok(CoRpcResp::FromPayload(
        std::move(reply.value())));
}

} // namespace bbt::framework
