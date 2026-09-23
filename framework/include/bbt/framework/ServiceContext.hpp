#pragma once
// service-actor/v2：服务侧上下文与资源缝（业务面，protected 访问）。
//
// ServiceContext 是框架注入到每个服务实例的上下文对象：
//  - request()：当前受管请求上下文（同一逻辑请求随协程绑定；非受管
//    调用返回 err(InvalidContext)）。handler 用它读 request_id、
//    deadline、cancel、actor_key、peer_principal、trace_id；
//  - resource<R>()：框架/扩展装配的资源客户端缝（如未来的
//    CoRedisCli/CoMysqlCli/CoMongoCli 等 infra client mixin）。资源
//    由 CoApp::add_resource<R> 按类型登记，服务在受管生命周期内取用；
//    未装配 → nullptr（如实返回，不伪造客户端）。
//
// 本类型是轻量值：内部只持有应用级资源表指针，随服务实例的
// _bind_runtime 绑定；不含运行时/网络细节，业务不感知注入点。

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework {

namespace detail {

// 无 RTTI 的类型键：进程内单调分配的静态序号（-fno-rtti 约束下
// typeid 不可用；同类型同键由 inline 静态成员保证）。
inline std::size_t NextResourceTypeId() noexcept {
    static std::atomic<std::size_t> seq{0};
    return ++seq;
}
template <class R>
std::size_t ResourceTypeId() noexcept {
    static const std::size_t id = NextResourceTypeId();
    return id;
}

} // namespace detail

struct ResourceKey {
    std::size_t type_id{0};
    std::string name;

    bool operator==(const ResourceKey& other) const noexcept {
        return type_id == other.type_id && name == other.name;
    }
};

struct ResourceKeyHash {
    std::size_t operator()(const ResourceKey& key) const noexcept {
        const auto name_hash = std::hash<std::string>{}(key.name);
        return key.type_id ^ (name_hash + 0x9e3779b9U +
            (key.type_id << 6U) + (key.type_id >> 2U));
    }
};

inline constexpr std::string_view kDefaultResourceName{""};

// 应用级资源表：(类型键, 名称) → 类型擦除的共享资源。由 CoApp 装配期
// 写入，运行期只读；旧无名 API 使用内部默认槽位。
using ResourceMap = std::unordered_map<ResourceKey, std::shared_ptr<void>,
    ResourceKeyHash>;

class ServiceContext {
public:
    // 当前受管请求上下文；非受管 → err(InvalidContext)。
    result<std::shared_ptr<const RequestContext>> request() const {
        return CurrentRequestContext();
    }

    // 资源缝：取旧无名槽位或按名称取已装配的类型 R 客户端；未装配 → nullptr。
    template <class R>
    std::shared_ptr<R> resource() const {
        return resource<R>(kDefaultResourceName);
    }

    template <class R>
    std::shared_ptr<R> resource(std::string_view name) const {
        if (!m_resources)
            return nullptr;
        const ResourceKey key{detail::ResourceTypeId<R>(), std::string{name}};
        const auto it = m_resources->find(key);
        if (it == m_resources->end())
            return nullptr;
        return std::static_pointer_cast<R>(it->second);
    }

private:
    // 仅宿主（CoApp 绑定路径）建立资源视图；业务不能自造上下文。
    void _bind_resources(std::shared_ptr<const ResourceMap> resources) {
        m_resources = std::move(resources);
    }

    std::shared_ptr<const ResourceMap> m_resources;

    friend class ICoService;
};

} // namespace bbt::framework
