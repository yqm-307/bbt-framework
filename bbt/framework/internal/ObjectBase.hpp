#pragma once
/**
 * framework 受管对象身份基类（internal）。
 *
 * Component / ServiceBase / Actor 共用的身份簿记基：保存 Registry 回填的非拥有
 * ObjectHandle（weak Registry + id + generation），用它查当前绑定。
 * 这不是新的公共抽象层；业务经 Component / Service / Actor 使用它。
 */

#include <string>
#include <string_view>
#include <typeinfo>
#include <utility>

#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/coroutine/object/CoRuntimeOnly.hpp>
#include <bbt/framework/Object.hpp>

namespace bbt::framework {

namespace detail {

/**
 * @brief 受管对象的身份与簿记基类。
 *
 * @par 职责
 * 保存 Registry 在安装时回填的非拥有 ObjectHandle，并为派生类的 Handle()/Info()
 * 提供共同实现，并查找该对象当前的组件绑定。
 *
 * @par 不负责
 * 不拥有对象本体，不提供 borrow，也不推进生命周期。
 *
 * @par 约束
 * 不可复制、不可移动。析构为 protected 且 noexcept，对象只能经 Registry 的
 * 类型化 deleter 销毁。
 */
class ObjectBase : public bbt::coroutine::CoObject,
                   public bbt::coroutine::CoRuntimeOnly {
public:
    ObjectBase(const ObjectBase&) = delete;
    ObjectBase& operator=(const ObjectBase&) = delete;

protected:
    ObjectBase() { bbt::coroutine::RequireCoroutineContext(); }
    virtual ~ObjectBase() noexcept = default;

    /**
     * 派生类公共 Handle()/Info() 的身份源（非拥有句柄）。
     */
    const ObjectHandle& _ObjectHandle() const noexcept { return m_handle; }
    ObjectInfo _ObjectInfo() const;

    /**
     * Registry 在安装时回填非拥有身份（weak reg + id + generation）。
     */
    void _Bind(ObjectHandle handle) { m_handle = std::move(handle); }

    static result<void> _CheckGetComponentContext();
    result<ComponentBindingSnapshot> _GetComponentLookup(
        const std::type_info& type, std::string_view name) const;

private:
    ObjectHandle m_handle;

    friend class ::bbt::framework::CoApp;
    friend class detail::Registry;
};

} // namespace detail
} // namespace bbt::framework
