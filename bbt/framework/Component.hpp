#pragma once
/**
 * @brief 业务组件的基类。CoApp 管理组件的生命周期，服务、Actor 和其他组件通过借用使用它。
 */

#include <string>
#include <string_view>
#include <type_traits>
#include <typeinfo>

#include <bbt/framework/Object.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/internal/ObjectBase.hpp>

namespace bbt::framework {

class MonitorWriter;

/**
 * @brief 可由 CoApp 安装、启动和停止的组件。
 *
 * Handle() 不延长组件寿命；访问组件时须保留 Borrow() 返回的借用对象。
 * 通过 CoApp::CoInstallComponent 安装。派生类用 detail::ConstructKey 构造，析构函数不对外开放。
 */
class CoComponent : public detail::ObjectBase {
public:
    /**
     * @brief 返回本组件的非拥有对象句柄。
     */
    ObjectHandle Handle() const { return _ObjectHandle(); }
    /**
     * @brief 返回本组件当前身份与状态的值快照。
     */
    ObjectInfo Info() const;

protected:
    /**
     * @brief 受控构造，只接受 Registry 发放的 ConstructKey。
     */
    explicit CoComponent(detail::ConstructKey) {}
    ~CoComponent() noexcept override = default;

    /**
     * @brief 取本组件已绑定依赖的类型化句柄。
     *
     * 组件依赖是 CoBindComponent 的 subject；绑定表由 CoApp::CoBindComponent
     * 成功后回填。OnInstall（装配窗口）内可借 Installed 依赖做纯内存装配；
     * 其他阶段仅可借 Running 且 binding active 的组件。
     *
     * @tparam C 依赖组件类型。
     * @param name 绑定时使用的组件逻辑名。
     * @return 成功时返回携带当前 binding generation 的 ComponentHandle<C>；
     *         name 未绑定返回 NotFound。
     */
    template <class C>
    result<ComponentHandle<C>> GetComponent(std::string_view name) const;

    /**
     * @brief 按绑定名借用组件。查找或借用失败时返回对应错误；返回的借用对象析构时归还组件。
     */
    template <class C>
    result<ComponentBorrow<C>> BorrowComponent(std::string_view name) const;

    /**
     * @brief 安装钩子，在组件登记为 Installing 之后、Installed 之前执行。
     *
     * 调用点：CoApp::CoInstallComponent。在调用协程中同步执行，
     * 不得做 I/O。失败时组件进入 InstallFailed（仍可 Find/卸载，不自动回滚），
     * 错误写入 last_error 并记入 LifecycleFailures。
     * 装配窗口借用：本对象处于 Installing 期间，经 GetComponent 取得的依赖
     * 允许借 Installed 状态的组件做纯内存装配（Registry 按 subject 状态放行）。
     *
     * @return 成功返回 ok；失败返回的错误即安装失败原因。
     */
    virtual result<void> OnInstall() { return result<void>::ok(); }
    /**
     * @brief 启动钩子，组件先于服务启动，按依赖拓扑顺序执行。
     *
     * 调用点：CoApp::CoStart；CoInstallComponent 时 App 已处于
     * Running 也会调用。在受管协程中执行，可挂起；失败时组件进入 Failed，
     * 错误记入 LifecycleFailures。
     *
     * @return 成功返回 ok；失败返回的错误即启动失败原因。
     */
    virtual result<void> OnStart() { return result<void>::ok(); }
    /**
     * @brief 停止应用或卸载组件时，在受管协程中停止该组件。
     * @return 失败时框架记录 LifecycleFailures 并保留组件，以便重试。
     */
    virtual result<void> OnStop() { return result<void>::ok(); }
    /**
     * @brief OnStop 之后、移除组件之前调用。抛出的异常记入 LifecycleFailures。
     */
    virtual void OnUninstall() {}
    /**
     * @brief 预留的监控回调；框架目前不会自动调用。
     */
    virtual void OnMonitorCollect(MonitorWriter&) {}
    /**
     * @brief 绑定生效后通知 bound=true；解绑要等所有借用归还后才通知 bound=false。
     * 回调在注册表锁外同步执行，框架保证当前对象活到回调结束。异常记入 LifecycleFailures，不撤销绑定或解绑。
     *
     * @param event 本次绑定变化的快照。
     */
    virtual void OnBindingChanged(const BindingEvent&) {}

private:
    friend class detail::Registry;
    friend class CoApp;
};

} // namespace bbt::framework

#include <bbt/framework/__TComponent.hpp>
