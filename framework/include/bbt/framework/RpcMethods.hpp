#pragma once
// service-actor/v2：RPC 方法清单的声明糖（业务面）。
//
// 业务在服务类型上声明一份 constexpr 方法清单：
//   static constexpr auto kRpcMethods = fw::RpcMethods(
//       fw::Method<&EchoSvc::Ping>("ping"),
//       fw::ActorMethodAt<&EchoSvc::Xfer, std::string>("xfer"));
//
//  - Method<成员指针>：普通 RPC 方法；handler 签名 CoRpcResp(CoRpcReq)；
//  - ActorMethodAt<成员指针, Key类型, 位置下标>：ActorSerial 方法，
//    actor key 取自 CoRpcReq 的第 Index 个位置参数，不再写提取 lambda；
//  - RpcMethods(...)：把若干声明条目收进一个 MethodList。
//
// 声明条目是纯值类型，不含逻辑；展开为类型擦除方法表、codec 选择、
// 重复/空名校验在 internal/MethodTable.hpp（机器面）完成——本头只承载
// 业务要写的声明面，业务不应直接 include internal 头。

#include <cstddef>
#include <tuple>
#include <type_traits>

namespace bbt::framework {

namespace detail {

// ---- 声明条目类型：NTTP 携带成员指针，name 为运行期字符串 ----

template <auto Ptr>
struct MethodDecl {
    const char* name;
};

template <auto Ptr, class Key, std::size_t Index>
struct PositionalActorMethodDecl {
    const char* name;
};

template <class... D>
struct MethodList {
    std::tuple<D...> methods;
};

} // namespace detail

// 声明构造（constexpr）：CoService<T> 经 kRpcMethods 消费。
//   fw::Method<&Svc::Ping>("ping")
//   fw::ActorMethodAt<&Svc::Xfer, std::string>("xfer")

template <auto Ptr>
constexpr detail::MethodDecl<Ptr> Method(const char* name) {
    return detail::MethodDecl<Ptr>{name};
}

template <auto Ptr, class Key, std::size_t Index = 0>
constexpr detail::PositionalActorMethodDecl<Ptr, Key, Index>
ActorMethodAt(const char* name) {
    return detail::PositionalActorMethodDecl<Ptr, Key, Index>{name};
}

template <class... D>
constexpr detail::MethodList<D...> RpcMethods(D... decls) {
    return detail::MethodList<D...>{std::tuple<D...>(decls...)};
}

// T::kRpcMethods 检测（未声明 → 空表，同旧空 co_bind_rpc 语义）。
template <class T, class = void>
struct has_rpc_methods : std::false_type {};
template <class T>
struct has_rpc_methods<T,
    std::void_t<decltype(T::kRpcMethods)>> : std::true_type {};
template <class T>
inline constexpr bool has_rpc_methods_v = has_rpc_methods<T>::value;

} // namespace bbt::framework
