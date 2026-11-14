#pragma once
// framework Redis 能力组件：经 v2::CoComponent 装配共享 Engine 上的连接池。
//
// 职责与边界：
//  - Options 只借用宿主拥有的 net::Engine，不 Run、不 RequestClose、不关闭或
//    析构共享 Engine；组件只关闭自己创建的 CoRedisPool。
//  - OnInstall 只做参数校验与 CoRedisPool::Create（Engine 必须仍 Idle，不联网、
//    不建线程）；OnStart 在该组件自己的有限超时内显式 Connect 并等全池就绪；
//    OnStop 把同一个 CloseOptions 原样转发给 Pool()->Close(options)。
//  - 命令面经 Pool() 暴露给同一 ComponentBorrow 寿命内的调用方；组件不包装
//    有限命令子集，也不自建轮询线程。
//
// 用法：
//   app.CoInstallComponent<v2::RedisComponent>("cache",
//       v2::RedisComponent::Options{engine, address, pool_options});
//   service 声明 BindComponent<v2::RedisComponent>("cache") 后经
//   GetComponent("cache")->Borrow().Get().Pool() 消费。

#include <chrono>
#include <memory>
#include <string>
#include <utility>

#include <bbt/coroutine/syntax/Async.hpp>

#include <bbt/infra/CloseOptions.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/framework/internal/CloseOptionsCheck.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/redis/CoRedisPool.hpp>

#include <bbt/framework/Component.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework::v2 {

/**
 * @brief 共享 Engine 上的 Redis 连接池组件。
 *
 * @par 职责
 * 安装期创建连接池，启动期建立并验证连接，停止期把统一 CloseOptions 转发给
 * 池的关闭控制面。
 *
 * @par 不负责
 * 不拥有、不运行、不关闭共享 Engine；不创建等待线程；不替业务决定关闭模式
 * 或期限。
 *
 * @par 约束
 * 只能经 v2::CoApp::CoInstallComponent 安装；Pool() 只在实际 ComponentBorrow
 * 的寿命内使用，业务不得 Close 池或保存引用越过 borrow。
 */
class RedisComponent final : public CoComponent {
public:
    /// @brief 组件装配参数。
    struct Options {
        /// 宿主拥有并推进的共享 Engine；不得为空。
        std::shared_ptr<bbt::infra::net::Engine> engine;
        /// Redis 服务端数值地址。
        bbt::infra::net::Address address;
        /// 池参数（连接数、排队上限与连接级握手）。
        bbt::infra::redis::CoRedisPool::PoolOptions pool;
        /// OnStart 建连的有限绝对预算；<=0 时使用构造函数默认值。
        std::chrono::milliseconds connect_timeout{5000};
    };

    /**
     * @brief 受控构造：只接受 Registry 发放的 ConstructKey。
     * @param key Registry passkey。
     * @param options 装配参数（装配期移动保存）。
     */
    explicit RedisComponent(detail::ConstructKey key, Options options)
        : CoComponent(key), m_options(std::move(options)) {}
    RedisComponent(const RedisComponent&) = delete;
    RedisComponent& operator=(const RedisComponent&) = delete;

    /// @brief 本组件持有的连接池；只在实际 borrow 寿命内使用。
    bbt::infra::redis::CoRedisPool& Pool() const noexcept { return *m_pool; }
    /// @brief 装配参数值快照（地址/池参数用于身份与隔离观察）。
    const Options& Config() const noexcept { return m_options; }
    /// @brief OnStart 是否已成功完成全池建连。
    bool connected() const noexcept { return m_connected; }
    /// @brief 最近一次 OnStop 收到的关闭模式（未调用过为 -1）。
    int last_close_mode() const noexcept { return m_last_close_mode; }
    /// @brief 最近一次 OnStop 收到的绝对期限。
    bbt::coroutine::Deadline last_close_deadline() const noexcept {
        return m_last_close_deadline;
    }
    /// @brief 最近一次 OnStop 收到的 `wait_release`（同值转发观察面）。
    bool last_close_wait_release() const noexcept {
        return m_last_wait_release;
    }
    /// @brief OnStop 成功次数（同一值转发与幂等观察）。
    int close_calls() const noexcept { return m_close_calls; }

protected:
    ~RedisComponent() noexcept override = default;

    result<void> OnInstall() override {
        if (!m_options.engine)
            return result<void>::err(MakeError(
                ErrorCode::InvalidArgument,
                "redis component: shared engine is required"));
        if (m_options.pool.connections == 0)
            return result<void>::err(MakeError(
                ErrorCode::InvalidArgument,
                "redis component: pool.connections must be > 0"));
        auto created = bbt::infra::redis::CoRedisPool::Create(
            m_options.engine, m_options.address, m_options.pool);
        if (!created)
            return result<void>::err(std::move(created.error()));
        m_pool = std::move(created).value();
        return result<void>::ok();
    }

    result<void> OnStart() override {
        if (!m_pool)
            return result<void>::err(MakeError(
                ErrorCode::RuntimeUnavailable,
                "redis component: pool is not installed"));
        bbt::infra::CallOptions options;
        const auto timeout = m_options.connect_timeout > std::chrono::milliseconds{0}
                                 ? m_options.connect_timeout
                                 : std::chrono::milliseconds{5000};
        options.deadline = std::chrono::steady_clock::now() + timeout;
        auto connected = bbt_co_await m_pool->Connect(options);
        if (!connected)
            return result<void>::err(std::move(connected.error()));
        m_connected = true;
        return result<void>::ok();
    }

    result<void> OnStop(const bbt::infra::CloseOptions& options) override {
        // 参数校验先于任何副作用：非法组合不触碰池的任何关闭位。
        if (auto valid = bbt::framework::internal::ValidateCloseValue(options);
            !valid)
            return result<void>::err(std::move(valid.error()));
        m_last_close_mode = static_cast<int>(options.mode);
        m_last_close_deadline = options.deadline;
        m_last_wait_release = options.wait_release;
        if (!m_pool)
            return result<void>::ok();
        auto closed = bbt_co_await m_pool->Close(options);
        if (!closed)
            return result<void>::err(std::move(closed.error()));
        m_connected = false;
        ++m_close_calls;
        return result<void>::ok();
    }

private:
    friend class detail::Registry;

    Options m_options;
    std::shared_ptr<bbt::infra::redis::CoRedisPool> m_pool;
    bool m_connected{false};
    int m_last_close_mode{-1};
    bbt::coroutine::Deadline m_last_close_deadline{
        bbt::coroutine::Deadline::max()};
    bool m_last_wait_release{false};
    int m_close_calls{0};
};

} // namespace bbt::framework::v2
