#pragma once
/**
 * @brief 业务服务从 CoService 派生，用模板参数列出允许绑定的组件类型。
 */

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

class MonitorWriter;

/**
 * @brief 声明服务可以使用哪些组件。
 *
 * 模板参数不会创建或绑定组件；绑定后才能按名称查找和借用。
 * 通过 CoApp::CoInstallService 安装。派生类用 detail::ConstructKey 构造，析构函数不对外开放。
 *
 * @tparam Components 服务允许绑定的组件类型。
 */
template <class... Components>
class CoService : public CoServiceBase {
public:
    /**
     * @brief 编译期判定 C 是否在本服务的组件声明集合内。
     */
    template <class C>
    static constexpr bool kAllowsComponent =
        (std::is_same_v<C, Components> || ...);

protected:
    /**
     * @brief 受控构造，只接受 Registry 发放的 ConstructKey。
     */
    explicit CoService(detail::ConstructKey key);
    ~CoService() noexcept override = default;

    /**
     * @brief 取本服务中已绑定组件的类型化句柄。
     *
     * 先检查协程上下文和组件类型许可，再按 name 查找本服务的绑定表。
     * 绑定在 OnBindingChanged 通知前生效。
     *
     * @tparam C 组件类型，必须在 Components... 中声明。
     * @param name 绑定时使用的组件逻辑名。
     * @return 成功时返回携带当前 binding generation 的 ComponentHandle<C>；
     *         非协程返回 InvalidContext，C 未声明返回 InvalidArgument，
     *         name 未绑定返回 NotFound。
     */
    template <class C>
    result<ComponentHandle<C>> GetComponent(std::string_view name) const;

    /**
     * @brief 按绑定名借用组件。查找或借用失败时返回对应错误；返回的借用对象析构时归还组件。
     */
    template <class C>
    result<ComponentBorrow<C>> BorrowComponent(std::string_view name) const;

private:
    bool _AllowsComponent(const std::type_info& t) const noexcept override;

    friend class detail::Registry;
    friend class CoApp;
};

/**
 * @brief 读取 Service 类型声明的静态服务名 S::kServiceName。
 *
 * @tparam S Service 类型，可选声明 static constexpr std::string_view kServiceName。
 * @return 声明存在时返回其副本；缺失时返回空串，由安装校验处理。
 */
template <class S, class = void>
struct CoServiceStaticName {
    static std::string Get();
};

/**
 * @brief CoServiceStaticName 的偏特化：S 声明了 kServiceName 时取其值。
 */
template <class S>
struct CoServiceStaticName<S, std::void_t<decltype(S::kServiceName)>> {
    static std::string Get();
};

} // namespace bbt::framework

#include <bbt/framework/__TCoService.hpp>
