#pragma once
// co-service-actor/v1 F0：result<T> 是 bbt::infra::result<T> 的 using 引用，
// 不另造错误体系；Error/ErrorCode/MakeError 一并按别名透出供业务使用。

#include <bbt/infra/Result.hpp>

namespace bbt::framework {

template <class T>
using result = bbt::infra::result<T>;

using bbt::infra::Error;
using bbt::infra::ErrorCode;
using bbt::infra::ErrorDetails;
using bbt::infra::MakeError;

} // namespace bbt::framework
