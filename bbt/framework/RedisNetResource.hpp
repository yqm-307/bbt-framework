#pragma once
/**
 * @brief 多个服务可共享同一个 Redis 客户端。调整绑定只影响后续查找。
 * 组件启动时只启动自己的网络线程，不连接 Redis。业务须在协程中显式 Connect，再执行命令。
 * 地址须为数值 IPv4 或 IPv6。持有组件强引用期间可发起命令，组件关闭后返回 Closed。
 */

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>

#include <optional>
#include <string>
#include <string_view>
#include <system_error>

#include <utility>
#include <vector>

#include <bbt/coroutine/syntax/Async.hpp>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/redis/CoRedisCli.hpp>

#include <bbt/framework/Component.hpp>
#include <bbt/framework/Object.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework {

class RedisNetResource final : public CoComponent {
public:
    /**
     * @brief 组件装配选项：目标地址（数值 IPv4/IPv6）。
     */
    class Options {
    public:
        /**
         * @brief 设置 Redis 服务器地址。
         */
        Options& SetAddress(bbt::infra::net::Address address) {
            m_address = std::move(address);
            return *this;
        }
        /**
         * @brief 目标地址。
         */
        const bbt::infra::net::Address& Address() const noexcept {
            return m_address;
        }

    private:
        bbt::infra::net::Address m_address;
    };

    /**
     * 网络线程进入 Running 的最长等待时间，与 Connect 的调用期限无关。
     */
    static constexpr std::chrono::milliseconds kEngineRunWait{5000};

    /**
     * @brief 受控构造：仅 Registry 安装路径可达（ConstructKey passkey）。
     */
    RedisNetResource(detail::ConstructKey key, Options options);

    /**
     * @brief 已启动时在协程中用 bbt_co_await 执行 Redis 命令，客户端错误原样返回。
     */
    bbt_co_async(bbt::infra::result<void>) Connect(
        const bbt::infra::CallOptions& options);
    bbt_co_async(bbt::infra::result<void>) Ping(
        const bbt::infra::CallOptions& options);
    bbt_co_async(bbt::infra::result<std::optional<std::string>>) Get(
        std::string_view key,
        const bbt::infra::CallOptions& options);
    bbt_co_async(bbt::infra::result<void>) Set(
        std::string_view key, std::string_view value,
        const bbt::infra::CallOptions& options);
    bbt_co_async(bbt::infra::result<bool>) Exists(
        std::string_view key,
        const bbt::infra::CallOptions& options);
    bbt_co_async(bbt::infra::result<std::uint64_t>) Delete(
        std::vector<std::string> keys,
        const bbt::infra::CallOptions& options);

    /**
     * @brief 是否已开始关闭组件；true 不表示网络线程已退出。
     */
    bool IsClosed() const noexcept;
    /**
     * @brief 网络线程是否已退出、Run 未报错且 Engine 已进入 Stopped。
     */
    bool engine_stopped() const noexcept;

protected:
    /**
     * 析构时也会关闭资源；重复关闭无效。
     */
    ~RedisNetResource() noexcept override;
    RedisNetResource(const RedisNetResource&) = delete;
    RedisNetResource& operator=(const RedisNetResource&) = delete;

    /**
     * @brief 启动专用网络线程并创建客户端，不连接 Redis。启动失败时先清理已创建的资源。
     */
    result<void> OnStart() override;

    /**
     * @brief 停止接收新命令并等待网络线程退出。
     * 在途命令可能返回 Closed 或 OutcomeUnknown，不能据此判断 Redis 是否已执行该命令。
     */
    result<void> OnStop() override;

private:
    /**
     * 先关闭客户端，再停止 Engine 并等待线程退出。OnStop 和析构都可调用。
     * Run 返回错误且引擎仍在 Closing 时，首次调用报告原错误并保留引擎。
     * 后续调用在原生线程重试，返回这次清理的结果。
     */
    std::error_code Close() noexcept;

    enum class Closure {
        Open,
        Sealed,
        Stopped
    };

    struct EngineExit;
    const Options m_options;
    std::shared_ptr<bbt::infra::net::Engine> m_engine;
    std::shared_ptr<bbt::infra::redis::CoRedisCli> m_client;
    std::shared_ptr<EngineExit> m_engine_exit;
    std::future<std::error_code> m_engine_result;
    std::shared_ptr<EngineExit> m_engine_finalize_exit;
    std::future<std::error_code> m_engine_finalize;

    std::atomic<Closure> m_closure{Closure::Open};

    friend class detail::Registry;
};

} // namespace bbt::framework
