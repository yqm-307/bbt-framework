#pragma once

namespace bbt::framework {

/**
 * ---- 成员函数指针 traits：提取 request/reply 类型与编解码绑定 ----
 * 主模板只声明不实现；不受支持的签名实例化即编译错误。
 */

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

namespace detail {

/**
 * 声明条目类型（MethodDecl/PositionalActorMethodDecl/MethodList）在公共头
 * RpcMethods.hpp 定义——业务经 fw::Method/fw::ActorMethodAt/fw::RpcMethods
 * 构造，本头只消费已声明的清单。
 */

/**
 * ---- key 字段值 → 字符串（声明式 key 的换算规则） ----
 */

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
                      "ActorMethod: key field type must be std::string, string_view, or an integer type");
    }
    if (s.empty())
        return result<std::string>::err(MakeError(
            ErrorCode::InvalidArgument, "actor key field is empty"));
    return result<std::string>::ok(std::move(s));
}

/**
 * ---- 分发闭包构造 ----
 */

template <class MemberPtr>
RpcMethod::Invoker MakeCoRpcInvoker(MemberPtr handler) {
    using Traits = rpc_traits<MemberPtr>;
    static_assert(Traits::kCoRpc,
                  "RpcMethods: handler signature must be CoRpcResp(CoRpcReq)");
    using Service = typename Traits::service_type;
    static_assert(std::is_base_of_v<CoServiceBase, Service>,
                  "RpcMethods: Service must derive from CoService<...>");
    return [handler](CoServiceBase& base,
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

template <class Key, std::size_t Index>
RpcMethod::KeyExtractor MakePositionalKeyExtractor() {
    return [](const std::vector<std::uint8_t>& payload)
               -> result<std::string> {
        auto decoded = bbt::infra::rpc::DecodeAt<Key>(payload, Index);
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

/**
 * ---- 声明条目 → RpcMethod 展开 ----
 */

template <class MemberPtr>
RpcMethod MakeRpcMethod(const char* name, MemberPtr handler,
                        std::string_view request_schema, std::string_view response_schema,
                        RpcMethod::KeyExtractor extract_key = {}) {
    return {name, std::string(request_schema), std::string(response_schema),
            MakeCoRpcInvoker(handler), std::move(extract_key)};
}

template <auto Ptr>
RpcMethod BuildMethod(const MethodDecl<Ptr>& decl) {
    return MakeRpcMethod(decl.name, Ptr, kCoRpcPositionalSchema, kCoRpcPositionalSchema);
}

template <auto Ptr, class Key, std::size_t Index>
RpcMethod BuildMethod(const PositionalActorMethodDecl<Ptr, Key, Index>& decl) {
    return MakeRpcMethod(decl.name, Ptr, kCoRpcPositionalSchema, kCoRpcPositionalSchema,
                         MakePositionalKeyExtractor<Key, Index>());
}

/**
 * proto schema 方法。request/response_schema 从生成 message 的
 * descriptor full_name 推导——.proto 是唯一真源，杜绝手写漂移。此重载只在
 * 业务以真实 protobuf 类型实例化 ProtoMethod 时展开，框架库本体不依赖
 * protobuf 头。
 */
template <auto Ptr, class Req, class Resp>
RpcMethod BuildMethod(const ProtoMethodDecl<Ptr, Req, Resp>& decl) {
    const auto& request_schema = Req::descriptor()->full_name();
    const auto& response_schema = Resp::descriptor()->full_name();
    return MakeRpcMethod(decl.name, Ptr, request_schema, response_schema);
}

} // namespace detail

/**
 * 声明清单 → 方法表；重名/空名在构建期抛 std::invalid_argument。
 */
template <class... D>
RpcMethodTable BuildMethodTable(const detail::MethodList<D...>& list) {
    RpcMethodTable table;
    std::apply(
        [&](const auto&... d) {
            (table.Add(detail::BuildMethod(d)), ...);
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

} // namespace bbt::framework
