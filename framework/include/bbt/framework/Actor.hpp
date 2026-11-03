#pragma once
// framework v2 Actor 基类（契约 §5）。
//
// CoActor 与 CoComponent/CoService 共用 internal/ObjectBase.hpp 的身份基。本头只
// 声明 ActorId/MailBox 身份面、生命周期与绑定 hook，以及 kActorType 声明 trait。
// 消息分发、邮箱串行执行和动态 CreateActor 不在本切片。

#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <bbt/framework/Object.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/internal/ObjectBase.hpp>

namespace bbt::framework {

class MonitorWriter;   // 监控写端（后续切片；仅前置声明）

namespace v2 {

class CoComponent;     // GetComponent 静态断言的基类；实例化点须含 Component.hpp

/**
 * @brief 受管 Actor 基类，提供 ActorId 与 MailBox 身份面。
 *
 * @par 职责
 * 暴露 Actor 的稳定 ActorId 与邮箱地址，并承接 v2::CoApp 在 Actor 销毁与绑定变化
 * 时调用的业务 hook。
 *
 * @par 不负责
 * 本切片不执行消息分发、邮箱串行化或动态创建实例。
 *
 * @par 约束
 * 只能经 v2::CoApp::CoInstallActor 登记类型。派生类构造只接受
 * detail::ConstructKey，析构为 protected 或 private。
 */
class CoActor : public detail::ObjectBase {
public:
    /// @brief 返回 Actor 的稳定逻辑标识 (type, key)。
    const ActorId& GetId() const noexcept { return m_actor_id; }
    /// @brief 返回 Actor 的邮箱地址值；它是地址，不是对象引用。
    const MailBox& GetMailBox() const noexcept { return m_mailbox; }
    /// @brief 返回本 Actor 的非拥有对象句柄。
    ObjectHandle   Handle() const { return _ObjectHandle(); }
    /// @brief 返回 Actor 的身份与状态快照，包含 actor_id 与 mailbox。
    ActorInfo      Info() const;

protected:
    /// @brief 受控构造，只接受 Registry 发放的 ConstructKey。
    explicit CoActor(detail::ConstructKey) {}
    ~CoActor() noexcept override = default;

    /**
     * @brief 取本 Actor 实例已绑定依赖的类型化句柄。
     *
     * 绑定表由 v2::CoApp::CoBindComponent 成功后回填（subject=本实例）。
     *
     * @tparam C 依赖组件类型。
     * @param name 绑定时使用的组件逻辑名。
     * @return 成功时返回携带当前 binding generation 的 ComponentHandle<C>；
     *         name 未绑定返回 NotFound。
     */
    template <class C>
    result<ComponentHandle<C>> GetComponent(std::string_view name) const;

    /**
     * @brief Actor 实例创建钩子。
     *
     * 当前 HEAD 没有调用点：实例创建路径尚未接线，框架不会触发本 hook。
     *
     * @return 成功返回 ok；失败返回的错误即创建失败原因。
     */
    virtual result<void> OnCreate() { return result<void>::ok(); }
    /**
     * @brief Actor 实例销毁钩子，在实例登记移除前执行。
     *
     * 调用点：v2::CoApp 的 Stop 收口。同步执行，抛出的异常被捕获并
     * 记入 LifecycleFailures，不向外传播。
     */
    virtual void OnDestroy() {}
    /**
     * @brief 监控采集钩子。
     *
     * 当前 HEAD 没有调用点；监控写端由后续切片接入。
     */
    virtual void OnMonitorCollect(MonitorWriter&) {}
    /**
     * @brief 绑定变化钩子：本 Actor 作为绑定发起方（subject）完成绑定/解绑后调用。
     *
     * CoBindComponent 成功以 bound=true 派发；CoUnbindComponent 在 borrow 排空
     * 完成后以 bound=false 派发。
     * 在 Registry 锁外同步执行（subject owner 以 callback lease 保活），异常被
     * 捕获并记入 LifecycleFailures，绑定不回滚。
     *
     * @param event 绑定事件值快照。
     */
    virtual void OnBindingChanged(const BindingEvent&) {}

private:
    void _BindActor(ActorId id, MailBox mailbox) {
        m_actor_id = std::move(id); m_mailbox = std::move(mailbox);
    }

    ActorId m_actor_id;
    MailBox m_mailbox;

    friend class detail::Registry;
    friend class ::bbt::framework::v2::CoApp;
};

/**
 * @brief 读取 Actor 类型声明的静态类型名 A::kActorType。
 *
 * @tparam A Actor 类型，可选声明 static constexpr std::string_view kActorType。
 * @return 声明存在时返回其副本；缺失时返回空串，CoInstallActor 据此拒绝登记。
 */
template <class A, class = void>
struct CoActorStaticName {
    static std::string Get() { return {}; }
};

/// @brief CoActorStaticName 的偏特化：A 声明了 kActorType 时取其值。
template <class A>
struct CoActorStaticName<A, std::void_t<decltype(A::kActorType)>> {
    static std::string Get() { return std::string(A::kActorType); }
};

// ---- CoActor 模板定义 ----

template <class C>
result<ComponentHandle<C>> CoActor::GetComponent(std::string_view name) const {
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
