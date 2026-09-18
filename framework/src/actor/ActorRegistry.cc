#include <bbt/framework/internal/ActorRegistry.hpp>

namespace bbt::framework {

result<void> ActorRegistry::RegisterFactory(std::string service_name,
                                            Factory factory,
                                            std::size_t max_actors) {
    if (service_name.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "ActorRegistry: empty service_name"));
    if (!factory)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "ActorRegistry: empty factory"));
    if (max_actors == 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "ActorRegistry: max_actors must be > 0"));

    std::lock_guard<std::mutex> lock(m_mtx);
    if (m_services.count(service_name) != 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "ActorRegistry: duplicate service '" + service_name + "'"));
    m_services.emplace(std::move(service_name),
                       ServiceEntry{std::move(factory), max_actors, 0});
    return result<void>::ok();
}

result<std::shared_ptr<ICoService>> ActorRegistry::GetOrCreate(
    std::string_view service_name, std::string_view actor_key) {
    if (actor_key.empty())
        return result<std::shared_ptr<ICoService>>::err(MakeError(
            ErrorCode::InvalidArgument,
            "ActorRegistry: empty actor_key"));

    const std::pair<std::string, std::string> id{
        std::string(service_name), std::string(actor_key)};

    // 「查重 → 上限 → 构造 → 登记」在同一临界区完成：
    // 至多一次语义无竞态窗口；工厂异常经由 catch 转出，登记尚未发生，
    // 不留下半激活对象。
    std::lock_guard<std::mutex> lock(m_mtx);
    auto found = m_actors.find(id);
    if (found != m_actors.end())
        return result<std::shared_ptr<ICoService>>::ok(found->second);

    auto svc = m_services.find(id.first);
    if (svc == m_services.end())
        return result<std::shared_ptr<ICoService>>::err(MakeError(
            ErrorCode::NotFound,
            "ActorRegistry: service '" + id.first + "' not registered"));
    if (svc->second.live_actors >= svc->second.max_actors)
        return result<std::shared_ptr<ICoService>>::err(MakeError(
            ErrorCode::Overloaded,
            "ActorRegistry: service '" + id.first +
                "' reached max_actors limit"));

    std::shared_ptr<ICoService> instance;
    try {
        instance = svc->second.factory();
    } catch (...) {
        return result<std::shared_ptr<ICoService>>::err(MakeError(
            ErrorCode::InternalError,
            "ActorRegistry: factory threw while activating actor '" +
                id.second + "'"));
    }
    if (!instance)
        return result<std::shared_ptr<ICoService>>::err(MakeError(
            ErrorCode::InternalError,
            "ActorRegistry: factory returned null for actor '" +
                id.second + "'"));

    instance->_bind_actor_key(id.second);
    m_actors.emplace(id, instance);
    ++svc->second.live_actors;
    return result<std::shared_ptr<ICoService>>::ok(std::move(instance));
}

std::size_t ActorRegistry::ActorCount(std::string_view service_name) const {
    std::lock_guard<std::mutex> lock(m_mtx);
    auto it = m_services.find(std::string(service_name));
    return it == m_services.end() ? 0 : it->second.live_actors;
}

} // namespace bbt::framework
