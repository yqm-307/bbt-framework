#pragma once
/**
 * @brief CoApp 通过网络宿主启动监听、停止接收请求并等待处理结束。调用方不一定在协程中。
 * Create 成功后，即使 Start 失败，也必须能依次调用 StopAccepting、WaitHandlersDone、Close。
 * WaitHandlersDone 超时返回 TimedOut；任何错误都不能当作请求已处理完。
 * StopAccepting 和 Close 均可重复调用且不抛异常。Close 不先等待排空，返回时资源已释放；
 * Close(reason) 可将原因通知等待方，默认实现直接调用 Close()。
 */

#include <memory>
#include <optional>

#include <bbt/coroutine/sync/WaitTypes.hpp>

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

class INetworkHost {
public:
    virtual ~INetworkHost() = default;

    /**
     * 创建网络组件（真实实现对应 InfraHttpHost::Create + 监听装配）。
     */
    virtual result<void> Create() = 0;
    /**
     * 启动组件并开始接纳。同一实例至多成功一次。
     */
    virtual result<void> Start() = 0;
    /**
     * 停止新入站接纳；不取消已接纳 handler、不断开其回复路径。
     */
    virtual void StopAccepting() noexcept = 0;
    /**
     * 等已接纳 handler 结束（排空）。超时 → err(TimedOut)；宿主已被
     * Close 打断 → 立即返回关闭原因，不无限等。
     */
    virtual result<void> WaitHandlersDone(
        bbt::coroutine::Deadline deadline) = 0;
    /**
     * 同步关闭并释放网络组件：返回即物理资源已释放、后端不再访问。
     * 幂等、noexcept；在控制流调用（真实实现对应 InfraHttpHost::Close）。
     */
    virtual void Close() noexcept = 0;
    /**
     * 带原因的立即关闭；默认丢弃原因转 Close()，既有桩实现保持源兼容。
     */
    virtual void Close(bbt::infra::Error reason) noexcept {
        (void)reason;
        Close();
    }
    /**
     * Close 发布的原因；未 Close 为空。排空等待用「原因是否存在」这个
     * 明确状态判断「排空被关闭打断」，不按具体 ErrorCode 猜测。
     */
    virtual std::optional<bbt::infra::Error> CloseReason() const noexcept {
        return std::nullopt;
    }
};

} // namespace bbt::framework
