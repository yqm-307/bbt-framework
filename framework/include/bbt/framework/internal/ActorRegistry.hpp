#pragma once
// co-service-actor/v1 F2-a：Actor 实例注册表。
// 落实契约第 167 行实例化规则的服务侧簿记：
//  - 每个 (service-name, actor-key) 至多一次构造/激活；重复取同一 key
//    返回同一实例；
//  - 每个 service 的存活 Actor 数不超过该服务注册的 max_actors，超限
//    拒绝新激活（err(Overloaded)），不静默；
//  - 构造/激活失败不留下半激活对象：工厂抛异常或返回空时不占位、
//    不计数，同 key 可重试；
//  - 激活成功的实例由注册表强持有（停机前不销毁在途对象），并在创建时
//    经 ICoService::_bind_actor_key 绑定 key，使 co_actor_key() 可读回。
//
// 线程安全：全部入口走同一把 m_mtx，「查重 → 上限 → 构造 → 登记」在同一
// 临界区完成，保证至多一次语义无竞态窗口。工厂在锁内被调用——v1 业务类型
// 约定默认可构造且构造期不做 I/O（BindRpc 已 static_assert 同一约束），
// 不会引入长临界区。

#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <bbt/framework/ICoService.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework {

class ActorRegistry {
public:
    using Factory = std::function<std::shared_ptr<ICoService>()>;

    // 按类型注册工厂：T 必须继承 ICoService 且默认可构造。
    // service_name 重复注册 → err(InvalidArgument)；max_actors == 0 → 同上。
    template <class T>
    result<void> RegisterService(std::string service_name, std::size_t max_actors) {
        static_assert(std::is_base_of_v<ICoService, T>,
            "ActorRegistry::RegisterService: T 必须继承 ICoService");
        static_assert(std::is_default_constructible_v<T>,
            "ActorRegistry::RegisterService: T 必须默认可构造");
        return RegisterFactory(std::move(service_name),
            []() -> std::shared_ptr<ICoService> {
                return std::static_pointer_cast<ICoService>(
                    std::make_shared<T>());
            },
            max_actors);
    }

    result<void> RegisterFactory(std::string service_name,
                                 Factory factory, std::size_t max_actors);

    // 取已激活实例或激活新实例：
    //  - 同 (service, key) 第二次起返回同一 shared_ptr，不再调用工厂；
    //  - 未注册 service → err(NotFound)；空 key → err(InvalidArgument)；
    //  - 达 max_actors → err(Overloaded)，message 指明 max_actors；
    //  - 工厂异常/返回空 → err(InternalError)，不留半激活对象。
    result<std::shared_ptr<ICoService>> GetOrCreate(
        std::string_view service_name, std::string_view actor_key);

    std::size_t ActorCount(std::string_view service_name) const;

private:
    struct ServiceEntry {
        Factory     factory;
        std::size_t max_actors;
        std::size_t live_actors = 0;
    };

    mutable std::mutex m_mtx;
    std::map<std::string, ServiceEntry> m_services;
    std::map<std::pair<std::string, std::string>, std::shared_ptr<ICoService>>
        m_actors;
};

} // namespace bbt::framework
