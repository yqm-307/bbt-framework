#pragma once
// framework Redis lane：新 net Redis 资源在框架资源缝中的装配适配。
//
// 背景：infra 的新入口 bbt::infra::redis::CoRedisCli 消费宿主自有
// net::Engine + 数值 net::Address + 显式 Connect（bbt_co_await 协程域），
// 不继承旧 ICoCloseable——其 Close() 只逻辑封口并请求关闭已发布连接，
// 物理回收由「RequestClose Engine → join Run → Phase==Stopped」的宿主收尾
// 承担（见 bbtools-infra docs/redis-net.md §关闭与宿主收尾）。框架关闭序列
// 只认 ICoCloseable 同步收口，因此经本适配器装配：
//
//   RedisNetResource
//     ├── 继承 ICoCloseable：框架关闭序列照常收 spec.as_closeable → Close()
//     │   （停止接纳/关闭 client——内层 redis::CoRedisCli::Close + Engine
//     │   RequestClose + join Run + Phase==Stopped 核验，全部在本 Close()
//     │   内同步完成，不新增框架等待步）。
//     └── 命令面原样转发（bbt_co_await）：Connect/Set/Get/Ping/Exists/Delete。
//
// 工程边界（ceiling，明示不伪造）：
//  - Engine 归本对象独占（每资源一个 client-only Engine + 自有 Run 线程），
//    不与框架宿主/其他资源共享——新入口要求 Engine 无 RawHandler，共享会
//    让关闭次序相互踩界；确需共享 Engine 时另由上层宿主拥有并自证收尾。
//  - 命令只转发本任务需要的入口；新入口其余命令（SetOptions/Expire/Ttl/
//    INCR 族/Hash）后续按需在 adapter 与调用方同步扩展。
//  - Create 在 native 控制线程调用且要求 coroutine Scheduler 已启动
//    （Engine::Run 校验宿主控制域+runtime 已初始化；add_resource factory
//    的 on_scheduler_started 相位天然满足）。Create 内同步拉起 Engine Run
//    线程并有界等 Phase==Running——Engine 未起来即装配失败，不留半启动
//    线程。地址只接受数值 IPv4/IPv6（net::Address::Ip 不解析主机名）。
//  - IsClosed 只表示逻辑封口；物理证据是 Close() 内记录的
//    engine_stopped（Run 已 join 且 Phase==Stopped 且无 Run 错误）。
//    物理收口失败时 Close() 如实记录（不抛——ICoCloseable 是 noexcept），
//    IsClosed 仍 true，观测口 engine_stopped() 暴露该失败。
//  - 本类不参与框架公共头的 add_resource<R> 语义推断：R=RedisNetResource
//    继承 ICoCloseable，故 spec.as_closeable 照常由模板推导。
//
// 用法：
//    app.add_resource<fw::RedisNetResource>("cache",
//        [addr] { return fw::RedisNetResource::Create(addr); });
//    // handler 内：auto r = bbt_co_await cli->Get(key, opts);
//
// 资源类型名即框架资源键的类型段；service 经
// context().resource<fw::RedisNetResource>(name) 取用。

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

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/redis/CoRedisCli.hpp>

namespace bbt::framework {

class RedisNetResource final : public bbt::infra::ICoCloseable {
public:
    // Engine 发布 Running 的启动窗口（不是建连预算——建连预算由调用方
    // CallOptions 决定）。Run 线程已拉起但窗口内未 Running 即装配失败。
    static constexpr std::chrono::milliseconds kEngineRunWait{5000};

    // 装配：创建 client-only Engine + 拉起 Run 线程 + 等 Running +
    // redis::CoRedisCli::Create（只装配不拨号）。任一步失败收口已建对象
    // （RequestClose+join）后如实返回错误，不留半装配句柄。
    // 要求 native 线程 + coroutine Scheduler 已启动。
    static bbt::infra::result<std::shared_ptr<RedisNetResource>> Create(
        bbt::infra::net::Address address) {
        auto engine = std::make_shared<bbt::infra::net::Engine>(nullptr);
        std::promise<std::error_code> done;
        auto run_result = done.get_future();
        std::thread run_thread(
            [engine, done = std::move(done)]() mutable {
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
        // 有界等 Phase==Running；Run 提前返回（含前置校验失败）即刻可见。
        const auto until = std::chrono::steady_clock::now() + kEngineRunWait;
        while (engine->Phase() != bbt::infra::net::EnginePhase::Running) {
            if (run_result.wait_for(std::chrono::milliseconds{0}) ==
                std::future_status::ready) {
                break;
            }
            if (std::chrono::steady_clock::now() >= until) break;
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
        if (engine->Phase() != bbt::infra::net::EnginePhase::Running) {
            engine->RequestClose();
            run_thread.join();
            std::error_code ec{};
            if (run_result.valid()) ec = run_result.get();
            return bbt::infra::result<std::shared_ptr<RedisNetResource>>::err(
                bbt::infra::MakeError(
                    bbt::infra::ErrorCode::RuntimeUnavailable,
                    std::string{"redis-net: engine not running: "} +
                        ec.message()));
        }
        auto created = bbt::infra::redis::CoRedisCli::Create(engine, address);
        if (!created) {
            engine->RequestClose();
            run_thread.join();
            return bbt::infra::result<std::shared_ptr<RedisNetResource>>::err(
                std::move(created.error()));
        }
        return bbt::infra::result<std::shared_ptr<RedisNetResource>>::ok(
            std::shared_ptr<RedisNetResource>(new RedisNetResource(
                std::move(engine), std::move(created.value()),
                std::move(run_thread), std::move(run_result))));
    }

    ~RedisNetResource() noexcept override { Close(); }
    RedisNetResource(const RedisNetResource&) = delete;
    RedisNetResource& operator=(const RedisNetResource&) = delete;

    // 命令面：全部要求在受管协程内经 bbt_co_await 调用；语义与底层
    // redis::CoRedisCli 一致（未连接/已封口/竞争等在底层如实返回）。
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

    // ICoCloseable 同步收口：先 client Close（逻辑封口 + 已发布连接
    // RequestClose），再 Engine RequestClose + join Run + Phase==Stopped
    // 核验。幂等；任意线程可调。在途命令按底层语义以 Closed/OutcomeUnknown
    // 落定——本收口只保证「不再接纳 + Engine Run 已返回」，不承诺在途
    // 命令的后端结果。
    void Close() noexcept override {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (m_closed) return;
            m_closed = true;
        }
        // client 层先停：不再接纳新命令，已发布连接 RequestClose。
        m_client->Close();
        // Engine 物理收口：RequestClose → join Run → 结果 + Phase 核验。
        m_engine->RequestClose();
        if (m_engine_thread.joinable()) m_engine_thread.join();
        std::error_code ec{};
        if (m_engine_result.valid()) ec = m_engine_result.get();
        const bool stopped = !ec &&
            (m_engine->Phase() == bbt::infra::net::EnginePhase::Stopped);
        std::lock_guard<std::mutex> lk(m_mtx);
        m_engine_stopped = stopped;
    }

    bool IsClosed() const noexcept override {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_closed;
    }

    // 观测口（测试断言用）：Close() 内 Engine 物理收口是否核验成功
    // （Run 已 join、Phase==Stopped 且无 Run 错误）。未 Close 恒 false。
    bool engine_stopped() const noexcept {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_engine_stopped;
    }

private:
    RedisNetResource(std::shared_ptr<bbt::infra::net::Engine> engine,
                     std::shared_ptr<bbt::infra::redis::CoRedisCli> client,
                     std::thread run_thread,
                     std::future<std::error_code> run_result)
        : m_engine(std::move(engine)),
          m_client(std::move(client)),
          m_engine_thread(std::move(run_thread)),
          m_engine_result(std::move(run_result)) {}

    std::shared_ptr<bbt::infra::net::Engine>       m_engine;
    std::shared_ptr<bbt::infra::redis::CoRedisCli> m_client;
    std::thread                  m_engine_thread;
    std::future<std::error_code> m_engine_result;

    mutable std::mutex           m_mtx;
    bool                         m_closed{false};
    bool                         m_engine_stopped{false};
};

} // namespace bbt::framework
