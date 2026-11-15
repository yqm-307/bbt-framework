#pragma once
// 路由字段载体与逻辑调用地址（业务面）。
// custom 是拥有内容的、有上限的路由字段，不是裸指针、std::any 或隐式类型名。
// 路由决策与策略校验（未知字段/不支持策略 → UnsupportedRoute）属路由层职责，
// 本头只冻结公共类型。

#include <string>
#include <utility>
#include <vector>

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

// custom 路由字段的固定载体。
struct RouteFields {
    std::vector<std::pair<std::string, std::string>> values;
};

// 统一逻辑地址：service-name + method-name + custom。
// Actor 归属经 actor-key 表达（bind_actor 的 key_fn 提取），不并入本类型。
struct RpcRoute {
    std::string service_name;
    std::string method_name;
    RouteFields custom;
};

// 可选定制点：RouteTraits<T>::Encode(const T&) -> result<RouteFields>
// 把用户类型转换成固定载体。主模板只声明不实现（同 infra::Codec 形态），
// 业务在公开业务协议头给出显式特化；traits 不推导目标服务。
template <class T>
struct RouteTraits;

} // namespace bbt::framework
