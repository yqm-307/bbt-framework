#pragma once
// service-actor/v2：RPC 方法表（机器面，internal/）。
// 业务用 fw::Method / fw::ActorMethodAt / fw::RpcMethods 声明方法清单
// （声明糖见公共头 RpcMethods.hpp，业务在 CoService<T>::kRpcMethods
// 上书写），本头把声明展开为类型擦除的分发闭包：handler 签名统一
// CoRpcResp(CoRpcReq)，负载经位置参数 codec（CoRpc.hpp）；actor key
// 取自位置参数下标（ActorMethodAt），不再有手写提取 lambda。
// 非法签名走模板推导失败的编译错误；重复/空方法名在方法表构建期抛
// std::invalid_argument，不等首个请求才失败。

#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <bbt/infra/Codec.hpp>

#include <bbt/framework/ExecutionPolicy.hpp>
#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/ICoService.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/RpcMethods.hpp>

namespace bbt::framework {

// ---- 成员函数指针 traits：提取 request/reply 类型与编解码绑定 ----
// 主模板只声明不实现；不受支持的签名实例化即编译错误。

template <class MemberPtr>
struct rpc_traits;

template <class Service>
struct rpc_traits<CoRpcResp (Service::*)(CoRpcReq)> {
    using service_type = Service;
    using request_type = CoRpcReq;
    using reply_type = CoRpcResp;
    static constexpr bool kCoRpc = true;
};

template <class Service>
struct rpc_traits<CoRpcResp (Service::*)(const CoRpcReq&)> {
    using service_type = Service;
    using request_type = CoRpcReq;
    using reply_type = CoRpcResp;
    static constexpr bool kCoRpc = true;
};

// ---- 单方法描述：类型擦除后的分发与 key 提取闭包 ----
// invoke：解码负载 → 在传入的真实 Service 实例上调用成员指针 → 编码回复。
// extract_key：仅 ActorMethod 安装，从解码后的 Request 声明字段取 key。

struct RpcMethod {
    using Invoker = std::function<result<std::vector<std::uint8_t>>(
        ICoService&, const std::vector<std::uint8_t>&)>;
    using KeyExtractor = std::function<result<std::string>(
        const std::vector<std::uint8_t>&)>;

    std::string  name;
    std::string  request_schema;
    std::string  response_schema;
    bool         actor_keyed = false;
    Invoker      invoke;
    KeyExtractor extract_key;
};

// 同类服务共享的静态方法描述；所有实例使用同一份，不随实例可变状态改变。
class RpcMethodTable {
public:
    const RpcMethod* find(std::string_view name) const noexcept {
        for (const auto& m : m_methods)
            if (m.name == name)
                return &m;
        return nullptr;
    }
    const std::vector<RpcMethod>& methods() const noexcept { return m_methods; }
    std::size_t size() const noexcept { return m_methods.size(); }

    // 构建期追加：空名/重名 → std::invalid_argument（启动期暴露）。
    void Add(RpcMethod m) {
        if (m.name.empty())
            throw std::invalid_argument("method table: empty method name");
        if (find(m.name) != nullptr)
            throw std::invalid_argument(
                "method table: duplicate method '" + m.name + "'");
        m_methods.push_back(std::move(m));
    }
private:
    std::vector<RpcMethod> m_methods;
};

namespace detail {

// 声明条目类型（MethodDecl/PositionalActorMethodDecl/MethodList）在公共头
// RpcMethods.hpp 定义——业务经 fw::Method/fw::ActorMethodAt/fw::RpcMethods
// 构造，本头只消费已声明的清单。

// ---- key 字段值 → 字符串（声明式 key 的换算规则） ----

template <class K>
result<std::string> ActorKeyToString(const K& key) {
    using D = std::decay_t<K>;
    std::string s;
    if constexpr (std::is_same_v<D, std::string>) {
        s = key;
    } else if constexpr (std::is_convertible_v<const K&,
                                             std::string_view>) {
        s = std::string(std::string_view(key));
    } else if constexpr (std::is_integral_v<D> &&
                         !std::is_same_v<D, bool>) {
        s = std::to_string(key);
    } else {
        static_assert(sizeof(D) == 0,
            "ActorMethod: key 字段类型须为 std::string/string_view 或整型");
    }
    if (s.empty())
        return result<std::string>::err(MakeError(
            ErrorCode::InvalidArgument, "actor key field is empty"));
    return result<std::string>::ok(std::move(s));
}

// ---- 分发闭包构造 ----

template <class MemberPtr>
RpcMethod::Invoker MakeCoRpcInvoker(MemberPtr handler) {
    using Traits = rpc_traits<MemberPtr>;
    using Service = typename Traits::service_type;
    static_assert(std::is_base_of_v<ICoService, Service>,
                  "RpcMethods: Service 必须继承 ICoService");
    return [handler](ICoService& base,
                     const std::vector<std::uint8_t>& payload)
        -> result<std::vector<std::uint8_t>> {
        try {
            CoRpcResp resp = (static_cast<Service&>(base).*handler)(
                CoRpcReq{payload});
            if (!resp.ok())
                return result<std::vector<std::uint8_t>>::err(resp.error());
            return result<std::vector<std::uint8_t>>::ok(resp.payload());
        } catch (...) {
            return result<std::vector<std::uint8_t>>::err(MakeError(
                ErrorCode::InternalError, "rpc handler threw"));
        }
    };
}

template <class MemberPtr>
RpcMethod::Invoker SelectInvoker(MemberPtr handler) {
    using Traits = rpc_traits<MemberPtr>;
    static_assert(Traits::kCoRpc,
        "RpcMethods: handler 签名须为 CoRpcResp(CoRpcReq)");
    return MakeCoRpcInvoker(handler);
}

template <class Key, std::size_t Index>
RpcMethod::KeyExtractor MakePositionalKeyExtractor() {
    return [](const std::vector<std::uint8_t>& payload)
        -> result<std::string> {
        auto decoded = rpc_detail::DecodeAt<Key>(payload, Index);
        if (!decoded)
            return result<std::string>::err(std::move(decoded.error()));
        try {
            return ActorKeyToString(decoded.value());
        } catch (...) {
            return result<std::string>::err(MakeError(
                ErrorCode::InternalError, "actor key extraction threw"));
        }
    };
}

// ---- 声明条目 → RpcMethod 展开 ----

template <class Decl>
struct MethodDeclBuilder;

template <auto Ptr>
struct MethodDeclBuilder<MethodDecl<Ptr>> {
    static RpcMethod Build(const MethodDecl<Ptr>& d) {
        using Traits = rpc_traits<decltype(Ptr)>;
        RpcMethod m;
        m.name = d.name;
        static_assert(Traits::kCoRpc,
            "RpcMethods: handler 签名须为 CoRpcResp(CoRpcReq)");
        m.request_schema = std::string(kCoRpcPositionalSchema);
        m.response_schema = std::string(kCoRpcPositionalSchema);
        m.actor_keyed = false;
        m.invoke          = SelectInvoker(Ptr);
        return m;
    }
};

template <auto Ptr, class Key, std::size_t Index>
struct MethodDeclBuilder<PositionalActorMethodDecl<Ptr, Key, Index>> {
    static RpcMethod Build(
        const PositionalActorMethodDecl<Ptr, Key, Index>& d) {
        using Traits = rpc_traits<decltype(Ptr)>;
        static_assert(Traits::kCoRpc,
                      "ActorMethodAt requires a CoRpcReq/CoRpcResp handler");
        RpcMethod m;
        m.name            = d.name;
        m.request_schema  = std::string(kCoRpcPositionalSchema);
        m.response_schema = std::string(kCoRpcPositionalSchema);
        m.actor_keyed     = true;
        m.invoke          = SelectInvoker(Ptr);
        m.extract_key     = MakePositionalKeyExtractor<Key, Index>();
        return m;
    }
};

} // namespace detail

// 声明清单 → 方法表；重名/空名在构建期抛 std::invalid_argument。
template <class... D>
RpcMethodTable BuildMethodTable(const detail::MethodList<D...>& list) {
    RpcMethodTable table;
    std::apply(
        [&](const auto&... d) {
            (table.Add(detail::MethodDeclBuilder<
                     std::decay_t<decltype(d)>>::Build(d)), ...);
        },
        list.methods);
    return table;
}

template <class T>
RpcMethodTable BuildMethodTable() {
    if constexpr (has_rpc_methods_v<T>)
        return BuildMethodTable(T::kRpcMethods);
    else
        return RpcMethodTable{};
}

// F2-a：启动期校验——ActorSerial 服务的每个方法都必须经 ActorMethod 安装
// key 提取器。缺失 → err(InvalidArgument)，message 指明方法名。
// Concurrent 不要求提取器；Concurrent 下声明 ActorMethod 是否合法契约未
// 冻结，此处不拒绝（见 F2-a 报告的契约澄清项）。
result<void> ValidateActorKeying(const RpcMethodTable& table,
                                 ExecutionPolicy execution);

} // namespace bbt::framework
