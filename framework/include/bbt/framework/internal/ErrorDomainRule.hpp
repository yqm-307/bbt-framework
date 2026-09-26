#pragma once
// infra #39：framework.actor 域专属错误语义校验。
//
// 分层约定（协调 infra 决策 0002 修订后文本）：
//  - infra::ValidateErrorDetails 只做通用结构校验：条目/键/值上限、UTF-8
//    良构、重复键；不登记任何上层域的保留键表。
//  - 本头承载 framework.actor 域的保留键语义：`expected_sequence` 只允许
//    出现在该域、值必须是无符号十进制字符串；其他域写入该键按未知保留键
//    拒绝（语义与 infra 旧实现逐字对齐，不静默放宽）。
//  - 落实边界：本模块在 actor 错误的真实构造点（OrderedSession /
//    OrderedIngress::Admit 的 _GapError）、终态入缓存点
//    （OrderedIngress::Complete）、首发错误与终态重放出组件点
//    （InboundDispatcher 的 finish_ordered_error / Replay 分支）及
//    框架回复出站边界（http_bridge::ToHttpResponse）校验；
//    EnvelopeFromResponse 只做既有 wire 字段
//    （code/domain/domain_code/message）的信封还原——
//    Error.details 当前不上 HTTP wire（属 #8 协议范围），还原侧因此也
//    不重放域规则。
//
// 不引入动态 validator 注册平台或全局回调：规则是编译期固定的静态表驱动。

#include <bbt/framework/Result.hpp>
#include <bbt/infra/Result.hpp>

#include <string_view>
#include <vector>

namespace bbt::framework {

// framework.actor 域的保留键常量。域字符串本身与 OrderedTypes.hpp 的
// kOrderedErrorDomain 同源；此处重复为 string_view 常量，便于不依赖
// OrderedTypes.hpp 的校验调用方使用。
inline constexpr std::string_view kErrorDomainFrameworkActor =
    "framework.actor";
inline constexpr std::string_view kErrorDetailExpectedSequence =
    "expected_sequence";

namespace detail {

inline bool IsUnsignedDecimal(std::string_view s) noexcept {
    if (s.empty()) return false;
    for (char ch : s)
        if (ch < '0' || ch > '9') return false;
    return true;
}

// 域保留键表：{键, 所属域, 值校验}。新增上层域保留键在此追加一行，
// 校验语义集中可读；不开放运行时注册。
struct ReservedKeyRule {
    std::string_view key;
    std::string_view domain;
    bool (*value_ok)(std::string_view) noexcept;
};

inline constexpr ReservedKeyRule kReservedKeyRules[] = {
    {kErrorDetailExpectedSequence, kErrorDomainFrameworkActor,
     &IsUnsignedDecimal},
};

} // namespace detail

// 上层域语义校验：在 infra 通用结构校验之外，检查域专属保留键的归属与
// 值格式。违规拒绝为 ProtocolError。调用前不要求先过
// infra::ValidateErrorDetails——本函数不替代结构校验。
inline result<void> ValidateErrorDomainRules(const Error& error) {
    for (const auto& [key, val] : error.details) {
        for (const auto& rule : detail::kReservedKeyRules) {
            if (key != rule.key) continue;
            if (error.domain != rule.domain)
                return result<void>::err(MakeError(ErrorCode::ProtocolError,
                    "reserved key outside its owning domain"));
            if (!rule.value_ok(val))
                return result<void>::err(MakeError(ErrorCode::ProtocolError,
                    "reserved key value has invalid format"));
        }
    }
    return result<void>::ok();
}

// 完整边界校验 = infra 通用结构校验 + framework 域语义校验。
// actor 错误构造点与出站边界统一走本函数；任何违规整体拒绝为
// ProtocolError，不截断后继续。
inline result<void> ValidateErrorAtBoundary(const Error& error) {
    auto r = bbt::infra::ValidateErrorDetails(error);
    if (!r) return r;
    return ValidateErrorDomainRules(error);
}

} // namespace bbt::framework
