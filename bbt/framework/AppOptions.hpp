#pragma once
/** @brief 应用启动、生命周期和出站路由的装配选项。 */

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <bbt/infra/NetworkTypes.hpp>

#include <bbt/framework/DynamicRouteTable.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/RpcPolicy.hpp>

namespace bbt::framework {

/**
 * @brief 显式配置的静态出站路由（服务名 → "host:port" endpoint）。
 */
struct StaticRoute {
    std::string service_name;
    std::string address;
};

/** @brief CoApp 的受管对象、绑定与观察者配置。四项均须为正。 */
struct AppLimits {
    std::size_t max_objects = 0;
    std::size_t max_bindings = 0;
    std::size_t max_observers = 0;
    /// 当前仅在启动校验中要求为正，观察派发不据此限制队列。
    std::size_t observer_queue_capacity = 0;
};

/**
 * @brief CoApp 装配选项。
 *
 * Validate 要求 SetLimits 四项额度为有限正值、shutdown/lifecycle budget
 * 为正。网络面字段（监听/路由/身份/策略/限额）在 CoStart 时校验：
 * 配置的监听地址、local_identity、静态路由与 network_limits 由 CoStart 校验。
 */
class AppOptions {
public:
    /**
     * @brief 配置监听地址："host:port"（port 0 = 随机端口）。格式非法时
     *        登记为空 host，由 CoStart 报告 InvalidArgument。
     */
    AppOptions& Listen(std::string uri);
    /** @brief 设置 App 的额度配置，CoStart 校验四项均为正。 */
    AppOptions& SetAppLimits(AppLimits limits);

    /** @brief 兼容四个位置参数，参数顺序与 AppLimits 字段一致。 */
    AppOptions& SetLimits(std::size_t max_objects, std::size_t max_bindings,
                          std::size_t max_observers,
                          std::size_t observer_queue_capacity) {
        return SetAppLimits(AppLimits{max_objects, max_bindings, max_observers,
                                      observer_queue_capacity});
    }
    /**
     * @brief 关闭收口总预算（须为正）。
     */
    AppOptions& SetShutdownBudget(std::chrono::milliseconds budget) {
        m_shutdown_budget = budget;
        return *this;
    }
    /**
     * @brief 各生命周期步骤默认预算（须为正；未显式 SetDeadline 时使用）。
     */
    AppOptions& SetLifecycleBudget(std::chrono::milliseconds budget) {
        m_lifecycle_budget = budget;
        return *this;
    }
    /**
     * @brief 配置静态出站路由（重复服务名以首次为准，CoStart 校验拒绝重复）。
     */
    AppOptions& AddRoute(std::string service, std::string endpoint) {
        m_static_routes.push_back(StaticRoute{std::move(service),
                                              std::move(endpoint)});
        return *this;
    }
    /**
     * @brief 会话身份（必填当配置了监听；trusted-network identity 不是认证凭据）。
     */
    AppOptions& SetLocalIdentity(std::string identity) {
        m_local_identity = std::move(identity);
        return *this;
    }
    /**
     * @brief 出站策略；nullptr 使用默认策略（App 持有至析构）。
     */
    AppOptions& SetRpcPolicy(std::shared_ptr<RpcPolicy> policy) {
        m_rpc_policy = std::move(policy);
        return *this;
    }
    /**
     * @brief 可选动态路由表：发现提供方发布快照，出站按逻辑服务名解析；
     *        已配置的服务名优先于静态路由且不回退。
     */
    AppOptions& SetDynamicRoutes(std::shared_ptr<DynamicRouteTable> table) {
        m_dynamic_routes = std::move(table);
        return *this;
    }
    /**
     * @brief 网络限额（帧上限/正文上限等；CoStart 校验）。
     */
    AppOptions& SetNetworkLimits(bbt::infra::NetworkLimits limits) {
        m_network_limits = std::move(limits);
        return *this;
    }

    /**
     * @brief 监听地址串（未设为空）。
     */
    const std::optional<std::string>& ListenUri() const noexcept { return m_listen; }
    /**
     * @brief 解析后的监听地址（host 非空即合法；port 0 = 随机端口）。
     */
    const bbt::infra::ListenAddress& ListenAddr() const noexcept {
        return m_listen_addr;
    }
    /**
     * @brief 最大受管对象数。
     */
    std::size_t MaxObjects() const noexcept { return m_max_objects; }
    /**
     * @brief 最大绑定边数。
     */
    std::size_t MaxBindings() const noexcept { return m_max_bindings; }
    /**
     * @brief 最大观察订阅数。
     */
    std::size_t MaxObservers() const noexcept { return m_max_observers; }
    /**
     * @brief 兼容读取第四项配置，当前派发不据此限制队列。
     */
    std::size_t ObserverQueueCapacity() const noexcept {
        return m_observer_queue_capacity;
    }
    /**
     * @brief 关闭收口预算。
     */
    std::chrono::milliseconds ShutdownBudget() const noexcept {
        return m_shutdown_budget;
    }
    /**
     * @brief 生命周期默认预算。
     */
    std::chrono::milliseconds LifecycleBudget() const noexcept {
        return m_lifecycle_budget;
    }
    /**
     * @brief 静态出站路由。
     */
    const std::vector<StaticRoute>& StaticRoutes() const noexcept {
        return m_static_routes;
    }
    /**
     * @brief 会话身份（未设为空）。
     */
    const std::string& LocalIdentity() const noexcept { return m_local_identity; }
    /**
     * @brief 出站策略（未设为空）。
     */
    const std::shared_ptr<RpcPolicy>& RpcPolicyPtr() const noexcept { return m_rpc_policy; }
    /**
     * @brief 动态路由表（未设为空）。
     */
    const std::shared_ptr<DynamicRouteTable>& DynamicRoutes() const noexcept {
        return m_dynamic_routes;
    }
    /**
     * @brief 网络限额。
     */
    const bbt::infra::NetworkLimits& NetworkLimits() const noexcept {
        return m_network_limits;
    }

    /**
     * @brief 校验选项（额度与预算必须为有限正值）。
     */
    result<void> Validate() const;

private:
    std::optional<std::string> m_listen;
    bbt::infra::ListenAddress m_listen_addr{};
    std::vector<StaticRoute> m_static_routes;
    std::string m_local_identity;
    std::shared_ptr<RpcPolicy> m_rpc_policy;
    std::shared_ptr<DynamicRouteTable> m_dynamic_routes;
    bbt::infra::NetworkLimits m_network_limits;
    /**
     * 有限正值才放行；0 表示未设（Validate 拒绝）。
     */
    std::size_t m_max_objects = 0;
    std::size_t m_max_bindings = 0;
    std::size_t m_max_observers = 0;
    std::size_t m_observer_queue_capacity = 0;
    std::chrono::milliseconds m_shutdown_budget{0};
    /**
     * 未设 LifecycleOptions::SetDeadline 时各 lifecycle 步使用的总预算。
     */
    std::chrono::milliseconds m_lifecycle_budget{0};
};

} // namespace bbt::framework
