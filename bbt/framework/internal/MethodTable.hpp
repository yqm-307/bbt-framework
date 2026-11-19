#pragma once
/**
 * @brief 把服务声明的 RPC 方法注册为可调用的处理函数。
 * ActorMethodAt 从指定位置参数取 key。签名不受支持会编译失败；
 * 方法名为空或重复会在构建方法表时抛 std::invalid_argument。
 */

#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <bbt/infra/Codec.hpp>

#include <bbt/infra/rpc/codec/PositionalCodec.hpp>

#include <bbt/framework/ExecutionPolicy.hpp>
#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/RpcMethods.hpp>
#include <bbt/framework/internal/ServiceBase.hpp>

namespace bbt::framework {

/**
 * invoke 调用实际服务对象的方法；需要按 key 派发时，extract_key 从请求中取 key。
 */

struct RpcMethod {
    using Invoker = std::function<result<std::vector<std::uint8_t>>(
        CoServiceBase&, const std::vector<std::uint8_t>&)>;
    using KeyExtractor = std::function<result<std::string>(
        const std::vector<std::uint8_t>&)>;

    std::string name;
    std::string request_schema;
    std::string response_schema;
    Invoker invoke;
    KeyExtractor extract_key;
};

/**
 * 同类服务共享的静态方法描述；所有实例使用同一份，不随实例可变状态改变。
 */
class RpcMethodTable {
public:
    const RpcMethod* find(std::string_view name) const noexcept;
    const std::vector<RpcMethod>& methods() const noexcept { return m_methods; }
    std::size_t size() const noexcept { return m_methods.size(); }

    /**
     * 构建期追加：空名/重名 → std::invalid_argument（启动期暴露）。
     */
    void Add(RpcMethod m);

private:
    std::vector<RpcMethod> m_methods;
};

/// 展开声明清单。空名和重名在构建期抛 std::invalid_argument。
template <class... D>
RpcMethodTable BuildMethodTable(const detail::MethodList<D...>& list);

template <class T>
RpcMethodTable BuildMethodTable();

/**
 * 启动期校验——ActorSerial 服务的每个方法都必须经 ActorMethod 安装
 * key 提取器。缺失 → err(InvalidArgument)，message 指明方法名。
 * Concurrent 不要求提取器；Concurrent 下声明 ActorMethod 此处不拒绝。
 */
result<void> ValidateActorKeying(const RpcMethodTable& table,
                                 ExecutionPolicy execution);

} // namespace bbt::framework

#include <bbt/framework/internal/__TMethodTable.hpp>
