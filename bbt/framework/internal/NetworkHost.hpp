#pragma once
/**
 * 网络宿主注入点（机器面，internal/）：CoApp 的网络依赖组件
 * （Server/Client/连接池由它拥有，不是 Service 的父类）。
 *
 * 全部方法在装配/收口控制流内调用；实现不得依赖「运行在协程内」。
 * 真实实现见 internal/InfraHttpHost.hpp（桥接 infra net::Engine/
 * http::Server 的同步关闭语义）；桩实现用于测试断言调用顺序、
 * 异常路径与超时路径，不需要真实 socket。
 *
 * 实现约束：
 * - Create 成功后，无论 Start 是否成功，关闭序列（StopAccepting →
 *   WaitHandlersDone → Close）必须安全可调（未启动部分视为空操作）；
 * - WaitHandlersDone 在 deadline 耗尽时必须返回超时（TimedOut），
 *   不得无限阻塞；非预算性错误按 err(其它 code) 如实返回，调用方
 *   不得把任何错误当作「已排空」；
 * - StopAccepting/Close 幂等、noexcept。Close 同步完成物理释放
 *   （对应 infra ICoCloseable::Close：返回即资源已释放、后端不再访问）。
 * - Close(reason) 是「立即关闭、不先排空」的同义入口：默认丢弃原因转
 *   Close()，需要把原因发布给排空等待者的实现覆盖。
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
