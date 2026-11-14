#pragma once
// framework MongoDB 能力组件：经 v2::CoComponent 装配 CoMongoDb owner/集合句柄。
//
// 职责与边界：
//  - Options 只含 MongoRuntimeConfig 与 MongoTarget；组件不拥有 Engine（Mongo
//    自有 driver worker），也不因此获得第二个 Engine。
//  - OnInstall 只做参数校验与 CoMongoDb::Create（不建 worker、不联网）；
//    OnStart 调 owner Start，并按 infra 公共契约（Collection 要求 owner
//    Running）绑定集合句柄；OnStop 把同一个 CloseOptions 原样转发给 owner
//    Close(options)：wait_release=true 时由 owner 的 infra native finalizer
//    完成 worker 退出/JoinWorkers/lease 归还，组件 hook、Co worker 与析构都
//    不执行 join、不新建等待线程。
//  - 命令面经 Collection() 暴露给同一 ComponentBorrow 寿命内的调用方。

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <bbt/coroutine/syntax/Async.hpp>

#include <bbt/infra/CloseOptions.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/framework/internal/CloseOptionsCheck.hpp>
#include <bbt/infra/mongo/Bson.hpp>
#include <bbt/infra/mongo/Client.hpp>

#include <bbt/framework/Component.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework::v2 {

/**
 * @brief Mongo owner + 集合句柄组件。
 *
 * @par 职责
 * 安装期校验并创建 owner，启动期启动 owner 并绑定集合句柄，停止期把统一
 * CloseOptions 转发给 owner。
 *
 * @par 不负责
 * 不新建 worker 线程池、不 join、不改 Mongo 命令协议；不替业务决定关闭模式
 * 或期限。
 *
 * @par 约束
 * 只能经 v2::CoApp::CoInstallComponent 安装；Collection() 只在实际
 * ComponentBorrow 的寿命内使用。
 */
class MongoDBComponent final : public CoComponent {
public:
    /// @brief 组件装配参数。
    struct Options {
        /// owner 装配参数（uri、worker/queue/超时）。
        bbt::infra::mongo::MongoRuntimeConfig config;
        /// 绑定集合（database + collection）。
        bbt::infra::mongo::MongoTarget target;
    };

    /**
     * @brief 受控构造：只接受 Registry 发放的 ConstructKey。
     * @param key Registry passkey。
     * @param options 装配参数（装配期移动保存）。
     */
    explicit MongoDBComponent(detail::ConstructKey key, Options options)
        : CoComponent(key), m_options(std::move(options)) {}
    MongoDBComponent(const MongoDBComponent&) = delete;
    MongoDBComponent& operator=(const MongoDBComponent&) = delete;

    /// @brief 本组件的集合句柄；只在实际 borrow 寿命内使用。
    bbt::infra::mongo::CoMongoColl& Collection() const noexcept {
        return *m_coll;
    }
    /// @brief 装配参数值快照（配置/目标用于身份与隔离观察）。
    const Options& Config() const noexcept { return m_options; }
    /// @brief owner 是否已成功启动并绑定集合句柄。
    bool started() const noexcept { return m_started; }
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
    ~MongoDBComponent() noexcept override = default;

    result<void> OnInstall() override {
        if (auto valid = bbt::infra::mongo::ValidateMongoRuntimeConfig(
                m_options.config);
            !valid)
            return result<void>::err(std::move(valid.error()));
        if (auto valid = bbt::infra::mongo::ValidateMongoTarget(
                m_options.target);
            !valid)
            return result<void>::err(std::move(valid.error()));
        auto created =
            bbt::infra::mongo::CoMongoDb::Create(m_options.config);
        if (!created)
            return result<void>::err(std::move(created.error()));
        m_db = std::move(created).value();
        return result<void>::ok();
    }

    result<void> OnStart() override {
        if (!m_db)
            return result<void>::err(MakeError(
                ErrorCode::RuntimeUnavailable,
                "mongodb component: owner is not installed"));
        if (m_started)
            return result<void>::ok();
        if (auto started = m_db->Start(); !started)
            return result<void>::err(std::move(started.error()));
        // Collection 的 infra 公共契约要求 owner Running；句柄本身不建线程，
        // 只共享 owner 的 worker/队列/pool。
        auto coll = m_db->Collection(m_options.target);
        if (!coll)
            return result<void>::err(std::move(coll.error()));
        m_coll = std::move(coll).value();
        m_started = true;
        return result<void>::ok();
    }

    result<void> OnStop(const bbt::infra::CloseOptions& options) override {
        // 参数校验先于任何副作用：非法组合不触碰 owner 的关闭位。
        if (auto valid = bbt::framework::internal::ValidateCloseValue(options);
            !valid)
            return result<void>::err(std::move(valid.error()));
        m_last_close_mode = static_cast<int>(options.mode);
        m_last_close_deadline = options.deadline;
        m_last_wait_release = options.wait_release;
        if (!m_db)
            return result<void>::ok();
        // owner Close 在 Co 域按 options 挂起（Immediate 立刻返回；Graceful
        // 等已发布责任；wait_release 等 native finalizer 的真实回收）。
        auto closed = m_db->Close(options);
        if (!closed)
            return result<void>::err(std::move(closed.error()));
        m_started = false;
        ++m_close_calls;
        return result<void>::ok();
    }

private:
    friend class detail::Registry;

    Options m_options;
    std::shared_ptr<bbt::infra::mongo::CoMongoDb> m_db;
    std::shared_ptr<bbt::infra::mongo::CoMongoColl> m_coll;
    bool m_started{false};
    int m_last_close_mode{-1};
    bbt::coroutine::Deadline m_last_close_deadline{
        bbt::coroutine::Deadline::max()};
    bool m_last_wait_release{false};
    int m_close_calls{0};
};

} // namespace bbt::framework::v2
