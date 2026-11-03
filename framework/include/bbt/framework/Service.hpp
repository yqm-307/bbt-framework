#pragma once
// framework v2 CoService<Components...> 模板（契约 §5）。
//
// 本头只承载模板化的 CoService<Components...>：组件能力集合的编译期声明与类型化
// GetComponent。受管基类分别位于独立头：Component.hpp、Actor.hpp，以及
// internal/ServiceBase.hpp（类型擦除基）与 internal/ObjectBase.hpp（身份基）。
// 本头聚合包含这些头，保持既有 include 兼容。

#include <string>
#include <string_view>
#include <type_traits>
#include <typeinfo>

#include <bbt/framework/Actor.hpp>
#include <bbt/framework/Address.hpp>
#include <bbt/framework/Component.hpp>
#include <bbt/framework/Object.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/internal/Registry.hpp>
#include <bbt/framework/internal/ServiceBase.hpp>

namespace bbt::framework {

class MonitorWriter;   // 监控写端（后续切片；仅前置声明）

namespace v2 {

/**
 * @brief 类型化服务基类，声明本服务允许绑定的组件类型集合。
 *
 * @par 职责
 * 以模板参数列出允许绑定的组件类型，并提供类型化的 GetComponent<C>。
 *
 * @par 不负责
 * 模板参数只声明允许的类型，不触发组件创建或绑定；绑定由 v2::CoApp::CoBindComponent 完成。
 *
 * @par 约束
 * 只能经 v2::CoApp::CoInstallService 安装。派生类构造只接受 detail::ConstructKey，
 * 析构为 protected 或 private。
 *
 * @tparam Components 本服务允许绑定的组件类型。
 */
template <class... Components>
class CoService : public CoServiceBase {
public:
    /// @brief 编译期判定 C 是否在本服务的组件声明集合内。
    template <class C>
    static constexpr bool kAllowsComponent =
        (std::is_same_v<C, Components> || ...);

protected:
    /// @brief 受控构造，只接受 Registry 发放的 ConstructKey。
    explicit CoService(detail::ConstructKey key) : CoServiceBase(key) {}
    ~CoService() noexcept override = default;

    /**
     * @brief 取本服务中已绑定组件的类型化句柄。
     *
     * 先判定 C 是否在声明集合内，再按 name 查找本服务的绑定表。绑定表只由
     * v2::CoApp::CoBindComponent 成功后回填，本函数不触发绑定。
     *
     * @tparam C 组件类型，必须在 Components... 中声明。
     * @param name 绑定时使用的组件逻辑名。
     * @return 成功时返回携带当前 binding generation 的 ComponentHandle<C>；
     *         C 未声明返回 InvalidArgument，name 未绑定返回 NotFound。
     */
    template <class C>
    result<ComponentHandle<C>> GetComponent(std::string_view name) const;

private:
    bool _AllowsComponent(const std::type_info& t) const noexcept override {
        return ((t == typeid(Components)) || ...);
    }

    friend class detail::Registry;
    friend class ::bbt::framework::v2::CoApp;
};

// ---- CoService<Components...> 模板定义 ----

template <class... Components>
template <class C>
result<ComponentHandle<C>> CoService<Components...>::GetComponent(
    std::string_view name) const {
    if (!bbt::coroutine::IsInCoroutineContext())
        return result<ComponentHandle<C>>::err(MakeError(
            ErrorCode::InvalidContext, "GetComponent requires coroutine context"));
    // ComponentHandle 的 binding generation 校验由 Registry 在 Borrow() 临界区内完成。
    if (!_AllowsComponent(typeid(C)))
        return result<ComponentHandle<C>>::err(MakeError(
            ErrorCode::InvalidArgument,
            "GetComponent: 组件类型未在本 Service 的允许集合内声明"));
    BoundComponent b;
    if (!_FindBoundCopy(typeid(C), name, &b))
        return result<ComponentHandle<C>>::err(MakeError(
            ErrorCode::NotFound,
            "GetComponent: 组件 '" + std::string(name) + "' 未绑定"));
    return result<ComponentHandle<C>>::ok(
        detail::MakeComponentHandle<C>(b.object, b.binding_generation));
}

/**
 * @brief 读取 Service 类型声明的静态服务名 S::kServiceName。
 *
 * @tparam S Service 类型，可选声明 static constexpr std::string_view kServiceName。
 * @return 声明存在时返回其副本；缺失时返回空串，由安装校验处理。
 */
template <class S, class = void>
struct CoServiceStaticName {
    static std::string Get() { return {}; }
};

/// @brief CoServiceStaticName 的偏特化：S 声明了 kServiceName 时取其值。
template <class S>
struct CoServiceStaticName<S, std::void_t<decltype(S::kServiceName)>> {
    static std::string Get() { return std::string(S::kServiceName); }
};

} // namespace v2
} // namespace bbt::framework
