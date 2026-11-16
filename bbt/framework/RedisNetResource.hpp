#pragma once
/**
 * framework Redis lane：新 net Redis 入口在受管组件模型中的装配适配。
 *
 * 背景：infra 的新入口 bbt::infra::redis::CoRedisCli 消费宿主自有
 * net::Engine + 数值 net::Address + 显式 Connect（bbt_co_await 协程域），
 * 其 Close() 只逻辑封口并请求关闭已发布连接，物理回收由「RequestClose
 * Engine → join Run → Phase==Stopped」的宿主收尾承担（见 bbtools-infra
 * docs/redis-net.md §关闭与宿主收尾）。受管组件收口要求同步物理落定，
 * 因此经本组件装配：
 *
 * RedisNetResource（CoComponent）
 * ├── OnStart 完成 Engine Run 线程拉起 + client 装配（同步拉起自有
 * │   Engine 线程并有界等 Phase==Running——Engine 未起来即组件启动
 * │   失败，进入 Failed 并参与启动失败回退，不留半启动线程）。
 * ├── OnStop 同步物理收口：client Close（逻辑封口 + 已发布连接
 * │   RequestClose）→ Engine RequestClose + join Run + Phase==Stopped
 * │   核验，全部在 OnStop 内完成，不新增框架等待步。
 * └── 命令面原样转发（bbt_co_await）：Connect/Set/Get/Ping/Exists/Delete。
 *
 * 工程边界（ceiling，明示不伪造）：
 * - Engine 归本对象独占（每资源一个 client-only Engine + 自有 Run 线程），
 * 不与框架宿主/其他资源共享——新入口要求 Engine 无 RawHandler，共享会
 * 让关闭次序相互踩界；确需共享 Engine 时另由上层宿主拥有并自证收尾。
 * - 命令只转发本任务需要的入口；新入口其余命令（SetOptions/Expire/Ttl/
 * INCR 族/Hash）后续按需在 adapter 与调用方同步扩展。
 * - Engine::Run 校验宿主控制域+runtime 已初始化；CoStart 的组件 OnStart
 * 相位天然满足 runtime 已初始化。Run 在本对象自有 std::thread 上执行，
 * OnStart 内的有界等待只阻塞当前执行体，不阻塞 Engine 线程。
 * 地址只接受数值 IPv4/IPv6（net::Address::Ip 不解析主机名）。
 * - IsClosed 只表示逻辑封口；物理证据是 OnStop 内记录的
 * engine_stopped（Run 已 join 且 Phase==Stopped 且无 Run 错误）。
 * 物理收口失败时如实记录（不抛——收口 noexcept），IsClosed 仍 true，
 * 观测口 engine_stopped() 暴露该失败。
 *
 * 用法：
 * app->CoInstallComponent<fw::RedisNetResource>("cache",
 *     fw::RedisNetResource::Options{}.SetAddress(address));
 * // 服务声明 BindComponent<fw::RedisNetResource>("cache") 后，handler 内：
 * // auto h = GetComponent<fw::RedisNetResource>("cache");
 * // auto lease = h->Borrow();
 * // auto r = bbt_co_await lease->Get().Get(key, opts);
 *
 * 组件借用只在 borrow 生存期内有效；不得保存引用、不得业务侧 Close
 * （物理收口归 OnStop，业务只读 IsClosed/engine_stopped 观测）。
 */

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
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
            m_address = std::move(address); return *this;
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
     * Engine 发布 Running 的启动窗口（不是建连预算——建连预算由调用方
     * CallOptions 决定）。Run 线程已拉起但窗口内未 Running 即组件启动失败。
     */
    static constexpr std::chrono::milliseconds kEngineRunWait{5000};

    /**
     * @brief 受控构造：仅 Registry 安装路径可达（ConstructKey passkey）。
     */
    RedisNetResource(detail::ConstructKey key, Options options)
        : CoComponent(std::move(key)), m_options(std::move(options)) {}

    /**
     * 命令面：全部要求在受管协程内经 bbt_co_await 调用；语义与底层
     * redis::CoRedisCli 一致（未连接/已封口/竞争等在底层如实返回）。
     * 只在 borrow 生存期内使用。
     */
    bbt_co_async(bbt::infra::result<void>) Connect(
        const bbt::infra::CallOptions& options) {
        return bbt_co_await m_client->Connect(options);
    }
    bbt_co_async(bbt::infra::result<void>) Ping(
        const bbt::infra::CallOptions& options) {
        return bbt_co_await m_client->Ping(options);
    }
    bbt_co_async(bbt::infra::result<std::optional<std::string>>) Get(
        std::string_view key,
        const bbt::infra::CallOptions& options) {
        return bbt_co_await m_client->Get(key, options);
    }
    bbt_co_async(bbt::infra::result<void>) Set(
        std::string_view key, std::string_view value,
        const bbt::infra::CallOptions& options) {
        return bbt_co_await m_client->Set(key, value, options);
    }
    bbt_co_async(bbt::infra::result<bool>) Exists(
        std::string_view key,
        const bbt::infra::CallOptions& options) {
        return bbt_co_await m_client->Exists(key, options);
    }
    bbt_co_async(bbt::infra::result<std::uint64_t>) Delete(
        std::vector<std::string> keys,
        const bbt::infra::CallOptions& options) {
        return bbt_co_await m_client->Delete(std::move(keys), options);
    }

    /**
     * 观测口（测试断言用）：逻辑封口状态（物理收口归 OnStop，业务不触发）。
     */
    bool IsClosed() const noexcept {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_closed;
    }
    /**
     * 观测口（测试断言用）：OnStop 内 Engine 物理收口是否核验成功
     * （Run 已 join、Phase==Stopped 且无 Run 错误）。未收口恒 false。
     */
    bool engine_stopped() const noexcept {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_engine_stopped;
    }

protected:
    /**
     * 仅经 Registry 类型化 deleter 销毁；析构兜底同步收口（幂等）。
     */
    ~RedisNetResource() noexcept override { Close(); }
    RedisNetResource(const RedisNetResource&) = delete;
    RedisNetResource& operator=(const RedisNetResource&) = delete;

    /**
     * @brief 组件启动：创建 client-only Engine + 拉起 Run 线程 + 等
     *        Running + redis::CoRedisCli::Create（只装配不拨号）。任一步
     *        失败收口已建对象（RequestClose+join）后如实返回错误，组件
     *        进入 Failed 并参与启动失败回退，不留半装配句柄。
     */
    result<void> OnStart() override {
        m_engine = bbt::infra::net::Engine::Create(nullptr);
        std::promise<std::error_code> done;
        auto run_result = done.get_future();
        m_engine_thread = std::thread(
            [engine = m_engine, done = std::move(done)]() mutable {
                std::error_code ec;
                try {
                    bbt::infra::net::EngineConfig config;
                    config.runtime_progress =
                        bbt::infra::net::RuntimeProgress::Independent;
                    ec = engine->Run(config);
                } catch (...) {
                    ec = std::make_error_code(std::errc::io_error);
                }
                done.set_value(ec);
            });
        m_engine_result = std::move(run_result);
        /**
         * 有界等 Phase==Running；Run 提前返回（含前置校验失败）即刻可见。
         */
        const auto until = std::chrono::steady_clock::now() + kEngineRunWait;
        while (m_engine->Phase() != bbt::infra::net::EnginePhase::Running) {
            if (m_engine_result.wait_for(std::chrono::milliseconds{0}) ==
                std::future_status::ready) {
                break;
            }
            if (std::chrono::steady_clock::now() >= until) break;
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
        if (m_engine->Phase() != bbt::infra::net::EnginePhase::Running) {
            Close();
            return result<void>::err(MakeError(
                ErrorCode::RuntimeUnavailable,
                "redis-net: engine not running"));
        }
        auto created = bbt::infra::redis::CoRedisCli::Create(
            m_engine, m_options.Address());
        if (!created) {
            Close();
            return result<void>::err(std::move(created.error()));
        }
        m_client = std::move(created.value());
        return result<void>::ok();
    }

    /**
     * @brief 组件停止：同步物理收口（见 engine_stopped 观测口）。幂等；
     *         在途命令按底层语义以 Closed/OutcomeUnknown 落定——本收口
     *         只保证「不再接纳 + Engine Run 已返回」，不承诺在途命令的
     *         后端结果。
     */
    result<void> OnStop() override {
        Close();
        return result<void>::ok();
    }

private:
    /**
     * 同步收口：先 client Close（逻辑封口 + 已发布连接 RequestClose），再
     * Engine RequestClose + join Run + Phase==Stopped 核验。幂等、noexcept；
     * 在 OnStop 与析构兜底两条路径复用。
     */
    void Close() noexcept {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (m_closed) return;
            m_closed = true;
        }
        /**
         * client 层先停：不再接纳新命令，已发布连接 RequestClose。
         */
        if (m_client) m_client->Close();
        /**
         * Engine 物理收口：RequestClose → join Run → 结果 + Phase 核验。
         */
        if (m_engine) m_engine->RequestClose();
        if (m_engine_thread.joinable()) m_engine_thread.join();
        std::error_code ec{};
        if (m_engine_result.valid()) ec = m_engine_result.get();
        const bool stopped = !ec && m_engine &&
            (m_engine->Phase() == bbt::infra::net::EnginePhase::Stopped);
        std::lock_guard<std::mutex> lk(m_mtx);
        m_engine_stopped = stopped;
    }

    const Options m_options;
    std::shared_ptr<bbt::infra::net::Engine>       m_engine;
    std::shared_ptr<bbt::infra::redis::CoRedisCli> m_client;
    std::thread                  m_engine_thread;
    std::future<std::error_code> m_engine_result;

    mutable std::mutex           m_mtx;
    bool                         m_closed{false};
    bool                         m_engine_stopped{false};

    friend class detail::Registry;
};

} // namespace bbt::framework
