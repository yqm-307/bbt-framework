#pragma once
// framework v2 组件基类 CoComponent（契约 §5）。
//
// CoComponent 本身就是带默认 hook 的能力基类，不另设 IComponent/ComponentBase
// 抽象层。本头只声明身份观察面与生命周期/绑定 hook；受管构造、销毁与 borrow
// 由 Registry、v2::CoApp 和 ComponentHandle 控制。

#include <string>
#include <string_view>
#include <type_traits>
#include <typeinfo>

#include <bbt/framework/Object.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/internal/ObjectBase.hpp>

namespace bbt::framework {

class MonitorWriter;   // 监控写端（后续切片；仅前置声明）

namespace v2 {

/**
 * @brief 能力组件基类，提供组件身份观察面与默认 hook。
 *
 * @par 职责
 * 暴露组件的非拥有句柄与值快照，并承接 v2::CoApp 在安装、启动与绑定变化时调用的
 * 业务扩展 hook。
 *
 * @par 不负责
 * 不对外交出 shared_ptr 或原始指针。借用组件须经 ComponentHandle::Borrow()
 * 取得 ComponentBorrow，其引用只在该 borrow 生存期内有效。
 *
 * @par 约束
 * 只能经 v2::CoApp::CoInstallComponent 安装。派生类构造只接受
 * detail::ConstructKey，析构为 protected 或 private。
 */
class CoComponent : public detail::ObjectBase {
public:
    /// @brief 返回本组件的非拥有对象句柄。
    ObjectHandle Handle() const { return _ObjectHandle(); }
    /// @brief 返回本组件当前身份与状态的值快照。
    ObjectInfo   Info() const;

protected:
    /// @brief 受控构造，只接受 Registry 发放的 ConstructKey。
    explicit CoComponent(detail::ConstructKey) {}
    ~CoComponent() noexcept override = default;

    /**
     * @brief 取本组件已绑定依赖的类型化句柄。
     *
     * 组件依赖是 CoBindComponent 的 subject；绑定表由 v2::CoApp::CoBindComponent
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
     * @brief 安装钩子，在组件登记为 Installing 之后、Installed 之前执行。
     *
     * 调用点：v2::CoApp::CoInstallComponent。在调用协程中同步执行，
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
     * 调用点：v2::CoApp::CoStart；CoInstallComponent 时 App 已处于
     * Running 也会调用。在受管协程中执行，可挂起；失败时组件进入 Failed，
     * 错误记入 LifecycleFailures。
     *
     * @return 成功返回 ok；失败返回的错误即启动失败原因。
     */
    virtual result<void> OnStart() { return result<void>::ok(); }
    /**
     * @brief 停止钩子。
     *
     * 调用点：v2::CoApp::CoStop 的逆序收口与 CoUninstallComponent。在受管协程
     * 中执行；失败记入 LifecycleFailures 且保留 owner 待再次收口，不销毁对象。
     *
     * @return 成功返回 ok；失败返回的错误即停止失败原因。
     */
    virtual result<void> OnStop() { return result<void>::ok(); }
    /**
     * @brief 卸载钩子，在组件登记移除前执行。
     *
     * 调用点：组件销毁路径（Stop 收口与 CoUninstallComponent），在 OnStop 之后。
     * 抛出的异常由框架捕获并记入 LifecycleFailures。
     */
    virtual void OnUninstall() {}
    /**
     * @brief 监控采集钩子。
     *
     * 当前 HEAD 没有调用点；监控写端由后续切片接入。
     */
    virtual void OnMonitorCollect(MonitorWriter&) {}
    /**
     * @brief 绑定变化钩子：本组件作为绑定发起方（subject）的绑定边变化时调用。
     *
     * CoBindComponent 成功后以 bound=true 派发；CoUnbindComponent 在 borrow 排空
     * 完成后以 bound=false 派发。
     * 在 Registry 锁外同步执行（subject owner 以 callback lease 保活），异常被
     * 捕获并记入 LifecycleFailures，绑定不回滚。
     *
     * @param event 绑定事件值快照。
     */
    virtual void OnBindingChanged(const BindingEvent&) {}

private:
    friend class detail::Registry;
    friend class ::bbt::framework::v2::CoApp;
};

// ---- CoComponent 模板定义 ----

template <class C>
result<ComponentHandle<C>> CoComponent::GetComponent(
    std::string_view name) const {
    if (!bbt::coroutine::IsInCoroutineContext())
        return result<ComponentHandle<C>>::err(MakeError(
            ErrorCode::InvalidContext, "GetComponent requires coroutine context"));
    static_assert(std::is_base_of_v<CoComponent, C>,
        "GetComponent: C must derive from framework::v2::CoComponent");
    BoundComponent b;
    if (!_FindBoundCopy(typeid(C), name, &b))
        return result<ComponentHandle<C>>::err(MakeError(
            ErrorCode::NotFound,
            "GetComponent: component '" + std::string(name) + "' is not bound"));
    return result<ComponentHandle<C>>::ok(
        detail::MakeComponentHandle<C>(b.object, b.binding_generation));
}

} // namespace v2
} // namespace bbt::framework
