#pragma once
// 执行策略与服务级装配选项（业务面）。
// 所有字段必须显式填写，不提供默认值。
// ValidateServiceOptions 落实装配期规则；「容量耗尽返回 Overloaded」
// 属运行期行为，不在此检查。

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

enum class ExecutionPolicy { Concurrent, ActorSerial };
class ServiceOptions;

// 装配期校验：
//  - max_inflight 必须 > 0；
//  - ActorSerial 要求 mailbox_capacity > 0 且 max_actors > 0；
//    Concurrent 要求两者均为 0（不创建邮箱）；
//  - ordered_ingress 只允许 ActorSerial；启用时三项有序上限均 > 0，
//    禁用时均为 0。
// 违规返回 err(InvalidArgument)，message 中指明违规字段名。
result<void> ValidateServiceOptions(const ServiceOptions& options);

} // namespace bbt::framework
