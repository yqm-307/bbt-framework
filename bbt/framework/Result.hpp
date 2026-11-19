#pragma once
/**
 * @brief 框架与 infra 共用 result、Error 和 ErrorCode，错误无需转换。
 */

#include <bbt/infra/Result.hpp>

namespace bbt::framework {

template <class T>
using result = bbt::infra::result<T>;

using bbt::infra::Error;
using bbt::infra::ErrorCode;
using bbt::infra::ErrorDetails;
using bbt::infra::MakeError;

} // namespace bbt::framework
