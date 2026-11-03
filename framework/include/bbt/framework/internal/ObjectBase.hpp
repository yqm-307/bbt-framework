#pragma once
// framework v2 受管对象身份基类（internal）。
//
// Component / ServiceBase / Actor 共用的身份簿记基：保存 Registry 回填的非拥有
// ObjectHandle（weak Registry + id + generation），以及「本对象作为绑定发起方」
// 的组件绑定表。不持有对象、不提供 borrow。
// 这不是新的公共抽象层；业务经 Component / Service / Actor 使用它。

#include <mutex>
#include <string>
#include <string_view>
#include <typeinfo>
#include <utility>
#include <vector>

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
 * 提供共同实现；同时承载「本对象作为绑定发起方（subject）」的组件绑定表
 * （BindComponent 成功后由 CoApp 回填，GetComponent 只读）。
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

    // 派生类公共 Handle()/Info() 的身份源（非拥有句柄）。
    const ObjectHandle& _ObjectHandle() const noexcept { return m_handle; }
    ObjectInfo _ObjectInfo() const;

    // Registry 在安装时回填非拥有身份（weak reg + id + generation）。
    void _Bind(ObjectHandle handle) { m_handle = std::move(handle); }

    // ---- subject 绑定表（BindComponent 成功后由 CoApp 回填）----
    // GetComponent<C> 只读；表项携带 binding generation，借用代校验在
    // Registry 的 Borrow 临界区内完成。
    struct BoundComponent {
        const std::type_info* type;
        std::string           name;
        ObjectHandle          object;
        std::uint64_t         binding_generation = 0;
    };
    // 按 (type, name) 取绑定表项副本；未绑定返回 false。返回副本而非指针：
    // 表在 hook（受管协程）读与控制面写之间需要互斥，指针会随写失效。
    bool _FindBoundCopy(const std::type_info& type, std::string_view name,
                        BoundComponent* out) const;
    void _BindComponentEntry(const std::type_info& type, std::string name,
                             ObjectHandle object, std::uint64_t gen);
    void _UnbindComponentEntry(const std::type_info& type,
                               std::string_view name);
    void _InvalidateComponentEntries() noexcept;

private:
    ObjectHandle m_handle;

    // 绑定表短临界区保护：OnBindingChanged/hook 可在受管协程内读表，
    // 绑定/解绑在 Co 控制入口写表；锁内只读写表项，不调用业务代码。
    mutable std::mutex            m_bound_mtx;
    std::vector<BoundComponent>   m_bound_components;

    friend class ::bbt::framework::v2::CoApp;
    friend class detail::Registry;
};

} // namespace detail
} // namespace bbt::framework
