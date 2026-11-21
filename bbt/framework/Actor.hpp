#pragma once
/**
 * @brief 业务 Actor 的标识、邮箱地址和生命周期回调。
 */

#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <bbt/framework/Object.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/internal/ObjectBase.hpp>

namespace bbt::framework {

class MonitorWriter;

/**
 * GetComponent 静态断言的基类；实例化点须含 Component.hpp
 */
class CoComponent;

/**
 * @brief 用类型和 key 标识 Actor，提供邮箱地址和组件绑定回调。
 *
 * 通过 CoApp::CoRegisterActorType 登记类型；登记不创建实例。
 * 派生类用 detail::ConstructKey 构造，析构函数不对外开放。
 */
class CoActor : public detail::ObjectBase {
public:
    /**
     * @brief 返回 Actor 的稳定逻辑标识 (type, key)。
     */
    const ActorId& GetId() const noexcept { return m_actor_id; }
    /**
     * @brief 返回 Actor 的邮箱地址值；它是地址，不是对象引用。
     */
    const MailBox& GetMailBox() const noexcept { return m_mailbox; }
    /**
     * @brief 返回本 Actor 的非拥有对象句柄。
     */
    ObjectHandle Handle() const { return _ObjectHandle(); }
    /**
     * @brief 返回 Actor 的身份与状态快照，包含 actor_id 与 mailbox。
     */
    ActorInfo Info() const;

protected:
    /**
     * @brief 受控构造，只接受 Registry 发放的 ConstructKey。
     */
    explicit CoActor(detail::ConstructKey) {}
    ~CoActor() noexcept override = default;

    /**
     * @brief 取本 Actor 实例已绑定依赖的类型化句柄。
     *
     *
     * @tparam C 依赖组件类型。
     * @param name 绑定槽位名。
     * @return 成功时返回指向当前实例的弱句柄；
     *         非协程返回 InvalidContext，name 未绑定返回 NotFound。lock() 只保活对象。
     */
    template <class C>
    result<ComponentHandle<C>> GetComponent(std::string_view name) const;

    /**
     * @brief 按绑定名借用组件。查找或借用失败时返回对应错误；返回的借用对象析构时归还组件。
     */
    template <class C>
    result<ComponentBorrow<C>> BorrowComponent(std::string_view name) const;

    /**
     * @brief 为实例创建预留的回调；只登记 Actor 类型不会触发它。
     * @return 成功返回 ok，失败返回创建错误。
     */
    virtual result<void> OnCreate() { return result<void>::ok(); }
    /**
     * @brief 停止应用时，在移除 Actor 实例前同步调用。异常记入 LifecycleFailures，不向外抛出。
     */
    virtual void OnDestroy() {}
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
    void _BindActor(ActorId id, MailBox mailbox) {
        m_actor_id = std::move(id);
        m_mailbox = std::move(mailbox);
    }

    ActorId m_actor_id;
    MailBox m_mailbox;

    friend class detail::Registry;
    friend class CoApp;
};

/**
 * @brief 读取 Actor 类型声明的静态类型名 A::kActorType。
 *
 * @tparam A Actor 类型，可选声明 static constexpr std::string_view kActorType。
 * @return 声明存在时返回其副本；缺失时返回空串，CoInstallActor 据此拒绝登记。
 */
template <class A, class = void>
struct CoActorStaticName {
    static std::string Get();
};

/**
 * @brief CoActorStaticName 的偏特化：A 声明了 kActorType 时取其值。
 */
template <class A>
struct CoActorStaticName<A, std::void_t<decltype(A::kActorType)>> {
    static std::string Get();
};

} // namespace bbt::framework

#include <bbt/framework/__TActor.hpp>
