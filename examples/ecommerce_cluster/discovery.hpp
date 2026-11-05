#pragma once
// Consul 发现组件（应用侧组装，不属于框架核心）。
//
// 职责：
//   - 拥有一个专属 net::Engine（独立 Run 线程）+ 专属 http::Client +
//     bbt::infra::consul::Client（infra 已验收的 adapter，不重写协议）；
//   - 对每个 watch 的逻辑服务跑一个阻塞查询循环，把权威快照整体发布到
//     framework::DynamicRouteTable（出站数据面只读内存）；
//   - 可选自注册/注销：业务进程把自身监听端点登记为某个逻辑服务名。
//
// 不承担：业务路由策略、重试业务请求、TTL/业务健康判定（TCP check 只证明端口
// 可达）、把发现结果写进框架核心。
//
// 边界：
//   - 注册中心只接受 loopback 数值地址（infra adapter 的 fail-closed 校验）；
//   - 查询失败不刷新新鲜度，也不清空 last-known-good；超过路由表陈旧边界后
//     由 Resolve 显式失败（RuntimeUnavailable），绝不无限使用陈旧地址；
//   - Close 顺序固定：停 watcher（有界等待当前查询）→ 同步封口 consul Client
//     → 停 Engine 线程；必须在调度器仍能推进在途操作时调用。

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/consul/Client.hpp>
#include <bbt/infra/http/Client.hpp>
#include <bbt/infra/net/Engine.hpp>

#include <bbt/framework/DynamicRouteTable.hpp>

namespace ecommerce {

class ConsulDiscoveryComponent final : public bbt::infra::ICoCloseable {
public:
    struct Options {
        std::string agent_url;   // http://127.0.0.1:<port>
        std::chrono::milliseconds request_timeout{3000};
        std::chrono::milliseconds query_wait{2000};
        std::vector<std::string> watch_services;   // 逻辑服务名
    };

    static bbt::infra::result<std::shared_ptr<ConsulDiscoveryComponent>> Create(
        Options options,
        std::shared_ptr<bbt::framework::DynamicRouteTable> table);

    ~ConsulDiscoveryComponent() noexcept override;

    ConsulDiscoveryComponent(const ConsulDiscoveryComponent&) = delete;
    ConsulDiscoveryComponent& operator=(const ConsulDiscoveryComponent&) = delete;

    /// 框架资源启动口（Scheduler 已启动、网络未创建）：拉起 watcher 并等待
    /// 每个 watch 服务的首个权威快照。任一失败 → App 启动失败。
    bbt::infra::result<void> Start();

    /// 自注册（协程上下文）：成功后记住 instance id 供 DeregisterSelf 使用。
    bbt_co_async(bbt::infra::result<void>) RegisterSelf(
        std::string service, std::string instance_id, std::string address,
        std::uint16_t port);

    /// 注销本进程的登记（协程上下文，幂等）。
    bbt_co_async(bbt::infra::result<void>) DeregisterSelf();

    void Close() noexcept override;
    bool IsClosed() const noexcept override;

    /// 观测口：某服务最近一次成功发布的 revision（0 表示尚无成功查询）。
    std::uint64_t Revision(std::string_view service) const;
    /// 观测口：最近一次查询/注册失败原因（成功后被清空）。
    std::string LastError() const;

private:
    struct Watch {
        std::string service;
        std::uint64_t index{0};        // 下一轮请求使用的 Consul index
        std::uint64_t revision{0};
        std::vector<std::string> endpoints;   // 上一次发布的 endpoint 串
        bool first_done{false};
        bool first_ok{false};
        std::string first_error;
    };

    ConsulDiscoveryComponent(Options options,
                             std::shared_ptr<bbt::framework::DynamicRouteTable> table);

    bbt::infra::result<void> _StartEngine();
    bbt_co_async(void) _WatchLoop(std::size_t watch_index);
    void _SetLastError(std::string message);
    void _SleepWhileActive(std::chrono::milliseconds budget);

    Options m_options;
    std::shared_ptr<bbt::framework::DynamicRouteTable> m_table;

    std::shared_ptr<bbt::infra::net::Engine> m_engine;
    std::thread            m_engine_thread;
    std::error_code        m_engine_error;
    std::atomic<bool>      m_engine_returned{false};
    std::shared_ptr<bbt::infra::http::Client>   m_http;
    std::shared_ptr<bbt::infra::consul::Client> m_consul;

    mutable std::mutex      m_mtx;      // 保护 m_watches/m_last_error/m_registered_id
    std::condition_variable m_cv;       // Start 首快照与 Close watcher 退出
    std::vector<Watch>      m_watches;
    std::size_t             m_watchers_active{0};
    std::string             m_last_error;
    std::string             m_registered_id;
    std::string             m_registered_service;

    std::atomic<bool> m_stop{false};
    std::atomic<bool> m_closed{false};
    std::atomic<bool> m_started{false};
};

} // namespace ecommerce
