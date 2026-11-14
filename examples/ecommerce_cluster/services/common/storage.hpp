#pragma once
// 存储模式配置与 Mongo 订单记录转换，不承载服务编排。
#include <string>
#include <bbt/example/ecommerce/v1/ecommerce.pb.h>
#include <bbt/framework/ServiceContext.hpp>
#include <bbt/framework/CoRpc.hpp>
#ifdef EC_STORAGE_AVAILABLE
#include <bbt/infra/mongo/Client.hpp>
#endif
namespace ecommerce {
namespace v1 = bbt::example::ecommerce::v1;
/** @brief 装配前设置，运行期只读。 */
struct StorageConfig {
    bool enabled{false};
    std::string key_prefix;
};
StorageConfig& Storage();
bbt::framework::result<void> ValidateOrder(const v1::OrderRequest& request);
bool SamePayload(const v1::Record& record, const v1::OrderRequest& request);
void StorageHop(const bbt::framework::ServiceContext& context, const char* service,
                const char* method, const std::string& id, bool ok, int code = 0);
#ifdef EC_STORAGE_AVAILABLE
bbt::infra::CallOptions StorageBudget(const bbt::framework::ServiceContext& context);
// 集合句柄只在同一 ComponentBorrow 寿命内传入：调用方持 borrow，本函数不越过它。
bbt::framework::result<v1::Record> ReadRecord(bbt::infra::mongo::CoMongoColl& coll,
                                              const std::string& id, const bbt::infra::CallOptions& options);
bbt::framework::result<v1::Record> InsertRecord(bbt::infra::mongo::CoMongoColl& coll,
                                                const v1::Record& record, const bbt::infra::CallOptions& options);
bbt::framework::result<void> MarkSettled(bbt::infra::mongo::CoMongoColl& coll,
                                         const v1::Record& record, const bbt::infra::CallOptions& options);
#endif
} // namespace ecommerce
