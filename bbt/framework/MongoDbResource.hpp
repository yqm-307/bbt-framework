#pragma once
/**
 * @brief 把一个 MongoDB 集合绑定为组件，供业务借用后读写。
 * 在协程内用 bbt_co_await 执行命令，不要保存超出借用期的组件引用。
 * 组件管理数据库工作线程；关闭单个集合句柄只影响该句柄，不会停止同一数据库的其他集合。
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
            m_config = std::move(config);
            return *this;
        }
        /**
         * @brief 设置目标集合（database + collection）。
         */
        Options& SetTarget(bbt::infra::mongo::MongoTarget target) {
            m_target = std::move(target);
            return *this;
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
        bbt::infra::mongo::MongoTarget m_target;
    };

    /**
     * @brief 受控构造：仅 Registry 安装路径可达（ConstructKey passkey）。
     */
    MongoDbResource(detail::ConstructKey key, Options options);

    /**
     * @brief 在持有组件借用的协程中，用 bbt_co_await 调用集合命令。
     * UpdateMany 和 DeleteMany 不接受空过滤条件；Find 必须指定 limit。超过允许范围返回 InvalidArgument。
     */
    bbt_co_async(bbt::infra::result<void>) InsertOne(
        const bbt::infra::mongo::MongoDocument& doc,
        const bbt::infra::CallOptions& options);
    bbt_co_async(
        bbt::infra::result<std::optional<bbt::infra::mongo::MongoDocument>>)
        FindOne(const bbt::infra::mongo::MongoDocument& filter,
                const bbt::infra::CallOptions& options);
    bbt_co_async(bbt::infra::result<bbt::infra::mongo::MongoUpdateResult>)
        UpdateOne(const bbt::infra::mongo::MongoDocument& filter,
                  const bbt::infra::mongo::MongoDocument& update,
                  const bbt::infra::CallOptions& options);
    bbt_co_async(bbt::infra::result<std::uint64_t>) DeleteOne(
        const bbt::infra::mongo::MongoDocument& filter,
        const bbt::infra::CallOptions& options);

    bbt_co_async(bbt::infra::result<bbt::infra::mongo::MongoInsertManyResult>)
        InsertMany(const std::vector<bbt::infra::mongo::MongoDocument>& docs,
                   const bbt::infra::CallOptions& options);
    bbt_co_async(
        bbt::infra::result<std::vector<bbt::infra::mongo::MongoDocument>>)
        Find(const bbt::infra::mongo::MongoDocument& filter,
             const bbt::infra::mongo::FindOptions& find_options,
             const bbt::infra::CallOptions& options);
    bbt_co_async(bbt::infra::result<bbt::infra::mongo::MongoUpdateResult>)
        UpdateMany(const bbt::infra::mongo::MongoDocument& filter,
                   const bbt::infra::mongo::MongoDocument& update,
                   const bbt::infra::CallOptions& options);
    bbt_co_async(bbt::infra::result<std::uint64_t>) DeleteMany(
        const bbt::infra::mongo::MongoDocument& filter,
        const bbt::infra::CallOptions& options);
    bbt_co_async(bbt::infra::result<std::uint64_t>) CountDocuments(
        const bbt::infra::mongo::MongoDocument& filter,
        const bbt::infra::CallOptions& options);

    /**
     * @brief 是否已开始关闭组件。此标志变为 true 时，工作线程未必已经退出。
     */
    bool IsClosed() const noexcept;
    /**
     * @brief 主集合句柄是否已关闭；这不表示数据库工作线程已经退出。
     */
    bool coll_closed() const noexcept;

    /**
     * @brief 从已启动的数据库取得另一个集合句柄；这些句柄共用工作线程，关闭一个不影响其他句柄。
     */
    bbt::infra::result<std::shared_ptr<bbt::infra::mongo::CoMongoColl>>
    SiblingCollection(bbt::infra::mongo::MongoTarget target);

protected:
    /**
     * 析构时也会关闭资源；重复关闭无效。
     */
    ~MongoDbResource() noexcept override;
    MongoDbResource(const MongoDbResource&) = delete;
    MongoDbResource& operator=(const MongoDbResource&) = delete;

    /**
     * @brief 校验配置，启动数据库并获取目标集合；任一步失败都返回相应错误。
     */
    result<void> OnStart() override;

    /**
     * @brief 先关闭集合句柄，再关闭数据库并等待工作线程退出。
     * 停止完成不表示已经提交的写操作在数据库端一定成功或失败。
     */
    result<void> OnStop() override;

private:
    void Close() noexcept;

    const Options m_options;
    std::shared_ptr<bbt::infra::mongo::CoMongoDb> m_db;
    std::shared_ptr<bbt::infra::mongo::CoMongoColl> m_coll;

    mutable std::mutex m_mtx;
    bool m_closed{false};
    bool m_coll_closed{false};

    friend class detail::Registry;
};

} // namespace bbt::framework
