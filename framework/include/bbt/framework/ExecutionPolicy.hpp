#pragma once
// co-service-actor/v1 F2-a：执行策略与服务级装配选项。
// ServiceOptions 字段与契约 §F2（decisions/0001，第 153-162 行）逐字一致；
// 所有字段必须显式填写，不提供默认值。
// ValidateServiceOptions 落实契约第 165 行的装配期规则；「容量耗尽返回
// Overloaded」属运行期行为，不在此检查。

#include <cstddef>

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

enum class ExecutionPolicy { Concurrent, ActorSerial };

struct ServiceOptions {
    ExecutionPolicy execution;
    std::size_t max_inflight;
    std::size_t mailbox_capacity;
    std::size_t max_actors;
    bool ordered_ingress;
    std::size_t max_ordered_streams;
    std::size_t max_cached_results;
    std::size_t max_cached_result_bytes;
};

// 装配期校验：
//  - max_inflight 必须 > 0；
//  - ActorSerial 要求 mailbox_capacity > 0 且 max_actors > 0；
//    Concurrent 要求两者均为 0（不创建邮箱）；
//  - ordered_ingress 只允许 ActorSerial；启用时三项有序上限均 > 0，
//    禁用时均为 0。
// 违规返回 err(InvalidArgument)，message 中指明违规字段名。
result<void> ValidateServiceOptions(const ServiceOptions& options);

} // namespace bbt::framework
