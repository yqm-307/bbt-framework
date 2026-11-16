#pragma once
/**
 * framework Mongo lane：新 mongo owner/句柄入口在受管组件模型中的装配适配。
 *
 * 背景：infra 的新入口 bbt::infra::mongo::CoMongoDb（资源 owner）+
 * CoMongoColl（集合句柄）取代旧 bbt::infra::CoMongoCli「一 client 绑定一
 * db.collection」。owner 本身已是同步物理收口：其 Close() 返回即
 * 接纳封口、挂起等待者已落定、ops 清空且 worker 全退——受管组件收口
 * 直接复用，不需要 Redis 侧那种 Engine/join 适配。句柄 Close() 只是该
 * 句柄的接纳门禁（幂等、任意线程），不动兄弟句柄与 owner 物理收口。
 *
 * 资源归属：
 * MongoDbResource（CoComponent）
 * ├── OnStart 完成 Validate + CoMongoDb::Create + Start + Collection
 * │   绑定一次完成，任一步失败收口已建对象后如实返回错误（组件进入
 * │   Failed 并参与启动失败回退，不留半装配句柄）。
 * ├── OnStop 同步物理收口：先句柄 Close 封该集合接纳，再 owner Close
 * │   （返回即物理落定）；全部在本 OnStop 内完成，不新增框架等待步。
 * └── 命令面经集合句柄原样转发（bbt_co_await）：
 *     InsertOne/FindOne/UpdateOne/DeleteOne 与多文档入口
 *     InsertMany/Find/UpdateMany/DeleteMany/CountDocuments。
 *
 * 工程边界（ceiling，明示不伪造）：
 * - 一个 MongoDbResource 绑定一个 MongoTarget{database, collection}——
 * 与旧 CoMongoCli 的「一 client 一集合」消费形态对齐。同 owner 多集合
 * 共享 worker/队列的形态保留在上层：需要时由上层自己 Create 一次
 * CoMongoDb 并经 Collection() 取多个句柄，本资源不复制该能力。
 * - Close() 顺序固定为「句柄门禁 → owner 物理收口」。owner Close 返回
 * 即物理落定（不是固定窗口），故 IsClosed 真即可作为物理证据；
 * coll_closed 观测口只记录句柄门禁是否走到（诊断用）。
 * - 句柄级 Close 只封本集合接纳：同 owner 的兄弟句柄（若有）不受影响。
 * - 命令只能在协程上下文内经 bbt_co_await 调用（新入口的 AwaitHelper
 * 在协程外先抛 std::logic_error，先于函数体；漏写 await 直接持有
 * 返回值不被静态拦住——语义同 infra mongo/Client.hpp 注释）。
 *
 * 用法：
 * app->CoInstallComponent<fw::MongoDbResource>("docs",
 *     fw::MongoDbResource::Options{}.SetConfig(cfg).SetTarget(target));
 * // 服务声明 BindComponent<fw::MongoDbResource>("docs") 后，handler 内：
 * // auto lease = GetComponent<fw::MongoDbResource>("docs")->Borrow();
 * // auto r = bbt_co_await lease->Get().FindOne(filter, opts);
 *
 * 组件借用只在 borrow 生存期内有效；不得保存引用、不得业务侧 Close
 * （物理收口归 OnStop，业务只读 IsClosed/coll_closed 观测）。
 */

#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <bbt/coroutine/syntax/Async.hpp>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/mongo/Bson.hpp>
#include <bbt/infra/mongo/Client.hpp>

#include <bbt/framework/Component.hpp>
#include <bbt/framework/Object.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework {

class MongoDbResource final : public CoComponent {
public:
    /**
     * @brief 组件装配选项：Mongo runtime 配置与目标集合。
     */
    class Options {
    public:
        /**
         * @brief 设置 runtime 配置（worker/队列等；CoMongoDb::Create 消费）。
         */
        Options& SetConfig(bbt::infra::mongo::MongoRuntimeConfig config) {
            m_config = std::move(config); return *this;
        }
        /**
         * @brief 设置目标集合（database + collection）。
         */
        Options& SetTarget(bbt::infra::mongo::MongoTarget target) {
            m_target = std::move(target); return *this;
        }
        /**
         * @brief runtime 配置。
         */
        const bbt::infra::mongo::MongoRuntimeConfig& Config() const noexcept {
            return m_config;
        }
        /**
         * @brief 目标集合。
         */
        const bbt::infra::mongo::MongoTarget& Target() const noexcept {
            return m_target;
        }
    private:
        bbt::infra::mongo::MongoRuntimeConfig m_config;
        bbt::infra::mongo::MongoTarget        m_target;
    };

    /**
     * @brief 受控构造：仅 Registry 安装路径可达（ConstructKey passkey）。
     */
    MongoDbResource(detail::ConstructKey key, Options options)
        : CoComponent(std::move(key)), m_options(std::move(options)) {}

    /**
     * 命令面：全部要求在受管协程内经 bbt_co_await 调用；语义与底层
     * mongo::CoMongoColl 一致（空 filter 的 UpdateMany/DeleteMany 拒绝、
     * Find 的 limit 必填、上限越界接纳前 InvalidArgument 等在底层如实返回）。
     * 只在 borrow 生存期内使用。
     */
    bbt_co_async(bbt::infra::result<void>) InsertOne(
        const bbt::infra::mongo::MongoDocument& doc,
        const bbt::infra::CallOptions&          options) {
        return bbt_co_await m_coll->InsertOne(doc, options);
    }
    bbt_co_async(
        bbt::infra::result<std::optional<bbt::infra::mongo::MongoDocument>>)
    FindOne(const bbt::infra::mongo::MongoDocument& filter,
            const bbt::infra::CallOptions&          options) {
        return bbt_co_await m_coll->FindOne(filter, options);
    }
    bbt_co_async(bbt::infra::result<bbt::infra::mongo::MongoUpdateResult>)
    UpdateOne(const bbt::infra::mongo::MongoDocument& filter,
              const bbt::infra::mongo::MongoDocument& update,
              const bbt::infra::CallOptions&          options) {
        return bbt_co_await m_coll->UpdateOne(filter, update, options);
    }
    bbt_co_async(bbt::infra::result<std::uint64_t>) DeleteOne(
        const bbt::infra::mongo::MongoDocument& filter,
        const bbt::infra::CallOptions&          options) {
        return bbt_co_await m_coll->DeleteOne(filter, options);
    }

    /**
     * 多文档数据面（CoMongoColl data-plane 入口原样转发）。
     */
    bbt_co_async(bbt::infra::result<bbt::infra::mongo::MongoInsertManyResult>)
    InsertMany(const std::vector<bbt::infra::mongo::MongoDocument>& docs,
               const bbt::infra::CallOptions&                       options) {
        return bbt_co_await m_coll->InsertMany(docs, options);
    }
    bbt_co_async(
        bbt::infra::result<std::vector<bbt::infra::mongo::MongoDocument>>)
    Find(const bbt::infra::mongo::MongoDocument& filter,
         const bbt::infra::mongo::FindOptions&   find_options,
         const bbt::infra::CallOptions&          options) {
        return bbt_co_await m_coll->Find(filter, find_options, options);
    }
    bbt_co_async(bbt::infra::result<bbt::infra::mongo::MongoUpdateResult>)
    UpdateMany(const bbt::infra::mongo::MongoDocument& filter,
               const bbt::infra::mongo::MongoDocument& update,
               const bbt::infra::CallOptions&          options) {
        return bbt_co_await m_coll->UpdateMany(filter, update, options);
    }
    bbt_co_async(bbt::infra::result<std::uint64_t>) DeleteMany(
        const bbt::infra::mongo::MongoDocument& filter,
        const bbt::infra::CallOptions&          options) {
        return bbt_co_await m_coll->DeleteMany(filter, options);
    }
    bbt_co_async(bbt::infra::result<std::uint64_t>) CountDocuments(
        const bbt::infra::mongo::MongoDocument& filter,
        const bbt::infra::CallOptions&          options) {
        return bbt_co_await m_coll->CountDocuments(filter, options);
    }

    /**
     * 观测口（测试断言用）：逻辑封口状态（物理收口归 OnStop，业务不触发）。
     */
    bool IsClosed() const noexcept {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_closed;
    }
    /**
     * 观测口（测试断言用）：OnStop 内集合句柄门禁是否已执行。owner 物理
     * 收口证据即 OnStop 返回后的 IsClosed（owner Close 返回即 ops 清空 +
     * worker 全退），不另设近似观测。
     */
    bool coll_closed() const noexcept {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_coll_closed;
    }

    /**
     * 测试钩子：同 owner 另开集合句柄（owner 必须 Running；owner 关闭后
     * 底层如实返回 Closed）。用于「同 owner 多集合句柄共享 worker」的
     * 框架侧证据——句柄不新建线程，Close 只封该句柄接纳。
     */
    bbt::infra::result<std::shared_ptr<bbt::infra::mongo::CoMongoColl>>
    SiblingCollection(bbt::infra::mongo::MongoTarget target) {
        return m_db->Collection(std::move(target));
    }

protected:
    /**
     * 仅经 Registry 类型化 deleter 销毁；析构兜底同步收口（幂等）。
     */
    ~MongoDbResource() noexcept override { Close(); }
    MongoDbResource(const MongoDbResource&) = delete;
    MongoDbResource& operator=(const MongoDbResource&) = delete;

    /**
     * @brief 组件启动：ValidateMongoRuntimeConfig + ValidateMongoTarget +
     *        CoMongoDb::Create + Start + Collection 绑定，任一步失败如实
     *        返回错误（已建 owner 由成员析构兜底，MongoRuntime 析构即收口），
     *        组件进入 Failed 并参与启动失败回退。
     */
    result<void> OnStart() override {
        if (auto v = bbt::infra::mongo::ValidateMongoRuntimeConfig(
                m_options.Config()); !v)
            return result<void>::err(std::move(v.error()));
        if (auto v = bbt::infra::mongo::ValidateMongoTarget(
                m_options.Target()); !v)
            return result<void>::err(std::move(v.error()));
        auto created = bbt::infra::mongo::CoMongoDb::Create(
            m_options.Config());
        if (!created)
            return result<void>::err(std::move(created.error()));
        m_db = std::move(created).value();
        if (auto started = m_db->Start(); !started) {
            m_db.reset();
            return result<void>::err(std::move(started.error()));
        }
        auto coll = m_db->Collection(m_options.Target());
        if (!coll) {
            Close();
            return result<void>::err(std::move(coll.error()));
        }
        m_coll = std::move(coll).value();
        return result<void>::ok();
    }

    /**
     * @brief 组件停止：先集合句柄 Close（只封该集合接纳，幂等），再
     *         owner Close——owner 语义是同步物理收口：返回即接纳封口、
     *         挂起等待者已落定、ops 清空且 worker 全退（不提前宣告）。
     *         幂等；在途命令按底层语义以 Closed/终态落定，本收口不承诺
     *         在途命令的后端结果。
     */
    result<void> OnStop() override {
        Close();
        return result<void>::ok();
    }

private:
    void Close() noexcept {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (m_closed) return;
            m_closed = true;
        }
        if (m_coll) {
            m_coll->Close();
            std::lock_guard<std::mutex> lk(m_mtx);
            m_coll_closed = true;
        }
        if (m_db) m_db->Close();
    }

    const Options m_options;
    std::shared_ptr<bbt::infra::mongo::CoMongoDb>   m_db;
    std::shared_ptr<bbt::infra::mongo::CoMongoColl> m_coll;

    mutable std::mutex m_mtx;
    bool               m_closed{false};
    bool               m_coll_closed{false};

    friend class detail::Registry;
};

} // namespace bbt::framework
