#pragma once
/**
 * @brief 在服务的 kRpcMethods 中列出允许远程调用的方法。
 * 用 Method 声明普通方法；用 ActorMethodAt 指定哪个位置参数是 Actor 的 key；
 * 用 ProtoMethod 指定 protobuf 请求和回复类型。空方法名或重名会在构建方法表时被拒绝。
 */

#include <cstddef>
#include <tuple>
#include <type_traits>

namespace bbt::framework {

namespace detail {

template <auto Ptr>
struct MethodDecl {
    const char* name;
};

template <auto Ptr, class Key, std::size_t Index>
struct PositionalActorMethodDecl {
    const char* name;
};

/**
 * protobuf 请求和回复的类型名取自各自的 descriptor，不用手写。
 */
template <auto Ptr, class Req, class Resp>
struct ProtoMethodDecl {
    const char* name;
};

template <class... D>
struct MethodList {
    std::tuple<D...> methods;
};

} // namespace detail

/**
 * 声明构造（constexpr）：CoService<T> 经 kRpcMethods 消费。
 * fw::Method\<&Svc::Ping\>("ping")
 * fw::ActorMethodAt<&Svc::Xfer, std::string>("xfer")
 */

template <auto Ptr>
constexpr detail::MethodDecl<Ptr> Method(const char* name);

template <auto Ptr, class Key, std::size_t Index = 0>
constexpr detail::PositionalActorMethodDecl<Ptr, Key, Index>
ActorMethodAt(const char* name);

/**
 * proto schema 方法声明。
 * fw::ProtoMethod<&Svc::GetValue,
 * bbt::example::v1::GetValueRequest,
 * bbt::example::v1::GetValueResponse>("GetValue")
 * handler 仍为 CoRpcResp(CoRpcReq)；负载 codec 由业务经公开
 * CoRpcReq::ParseProto / CoRpcResp::FromProto + rpc_detail::ProtoCodec<T>
 * 特化提供。schema 字符串由生成物的 descriptor 推导，不在此重复。
 */
template <auto Ptr, class Req, class Resp>
constexpr detail::ProtoMethodDecl<Ptr, Req, Resp>
ProtoMethod(const char* name);

template <class... D>
constexpr detail::MethodList<D...> RpcMethods(D... decls);

/**
 * T::kRpcMethods 检测（未声明 → 空表，同旧空 co_bind_rpc 语义）。
 */
template <class T, class = void>
struct has_rpc_methods : std::false_type {};
template <class T>
struct has_rpc_methods<T,
                       std::void_t<decltype(T::kRpcMethods)>> : std::true_type {};
template <class T>
inline constexpr bool has_rpc_methods_v = has_rpc_methods<T>::value;

} // namespace bbt::framework

#include <bbt/framework/__TRpcMethods.hpp>
