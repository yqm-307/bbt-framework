#pragma once
// 契约 F3 冻结的关闭状态枚举（公共）：CoApp::shutdown_state() 返回。
// 内部状态机（internal/HostLifecycle.hpp）有更细的相位，归并到这四态。

namespace bbt::framework {

enum class ShutdownState { Running, Closing, ShutdownIncomplete, Closed };

} // namespace bbt::framework
