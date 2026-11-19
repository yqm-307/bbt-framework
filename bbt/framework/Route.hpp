#pragma once
/**
 * @brief 用服务名和方法名指定调用目标，用 custom 传递有大小限制的路由字段。
 * 不支持的路由策略或字段由路由层以 UnsupportedRoute 拒绝。
 */

#include <string>
#include <utility>
#include <vector>

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

/**
 * custom 路由字段的固定载体。
 */
struct RouteFields {
    std::vector<std::pair<std::string, std::string>> values;
};

/**
 * @brief 用服务名、方法名和可选的路由字段指定目标。Actor 的 key 从请求中提取，不是地址字段。
 */
struct RpcRoute {
    std::string service_name;
    std::string method_name;
    RouteFields custom;
};

/**
 * 可选定制点：RouteTraits<T>::Encode(const T&) -> result<RouteFields>
 * 把用户类型转换成固定载体。主模板只声明不实现（同 infra::Codec 形态），
 * 业务在公开业务协议头给出显式特化；traits 不推导目标服务。
 */
template <class T>
struct RouteTraits;

} // namespace bbt::framework
