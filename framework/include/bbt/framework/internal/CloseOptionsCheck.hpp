#pragma once
// CloseOptions 值校验（本仓局部 helper，机器面 internal/）。
//
// framework 只消费 infra 的**公开值契约** `CloseOptions{mode, wait_release,
// reason, deadline}`：需要等待的组合（`Graceful` 或 `wait_release=true`）必须给
// 有限绝对期限，`Deadline::max()` 在副作用之前被拒绝。模块私有的关闭等待夹具
// （`bbt/infra/detail/*`）不是公共 API，framework 不引用它；各 Close/OnStop 入口
// 共用本函数，避免每个调用点各写一份判定。

#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/infra/CloseOptions.hpp>

#include <bbt/framework/Result.hpp>

namespace bbt::framework::internal {

/// @brief 校验 CloseOptions 值；不合法组合返回 InvalidArgument。
///
/// 只做与模块无关的值判定：模块不支持的组合（例如某模块只承诺强制关闭）由各
/// 模块在自己的副作用之前补判，本函数不代替模块语义。
inline result<void> ValidateCloseValue(
    const bbt::infra::CloseOptions& options) {
    const bool waits = options.mode == bbt::infra::CloseMode::Graceful ||
                       options.wait_release;
    if (waits && options.deadline == bbt::coroutine::Deadline::max())
        return result<void>::err(MakeError(
            ErrorCode::InvalidArgument,
            "close with waiting requires a finite absolute deadline"));
    return result<void>::ok();
}

} // namespace bbt::framework::internal
