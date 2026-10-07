#pragma once
// service-actor/v2 #8：资源关闭（机器面，internal/）。
//
// infra 关闭是同步契约：ICoCloseable::Close() 返回即物理资源已释放、后端
// 不再访问，无 RequestClose/WaitClosed 等待，也不绑定 coroutine runtime。
// 资源收口在 CoApp 控制线程（run() 所在线程）直接调用 Close()——不做
// 协程等待桥接、不伪造等待。

#include <memory>

#include <bbt/infra/ICoCloseable.hpp>

namespace bbt::framework::internal {

// 同步收口：对 closeable 调用 Close()（幂等、noexcept）。空指针为空操作。
void CloseResource(
    const std::shared_ptr<bbt::infra::ICoCloseable>& closeable) noexcept;

} // namespace bbt::framework::internal
