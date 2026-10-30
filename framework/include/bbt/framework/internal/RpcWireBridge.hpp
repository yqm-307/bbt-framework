#pragma once
// bbt-framework Issue #4（P0-A）：正式 RPC body bridge（机器面，internal/）。
//
// 与默认 RpcHttpBridge（x-bbt-* header 承载 envelope，迁移期绑定）不同，本桥
// 使用 bbtools-infra Issue #8 冻结的版本化 wire profile：HTTP POST /rpc，
// Content-Type: application/x-protobuf，body = RpcEnvelopeMsg；不读写任何
// x-bbt-* 头。
//
// 3-2 下沉后：wire 编解码、接收端 local-min 预算、成功/错误信封回显、五字段
// 关联与 remaining 回填**全部**由 infra 承担（rpc::Server →
// HandleRpcRequest；上游 NetworkRuntime::ListenRpc 已删除）。本桥的 handler
// 只是把 infra 已解析的 IncomingCallContext +
// RpcEnvelope 交给公开的 app.dispatch_inbound，并在错误返回给 infra 之前过一遍
// framework 错误边界（ValidateErrorAtBoundary）——infra 无 framework.actor 保留
// 键表，非法 details 若不降级会被 infra 编码期拒绝或静默绕过域校验。
//
// 依赖 bbt::infra_rpc_net（仅在 BBT_INFRA_RPC_NET + protobuf 前缀锁定可用时编译）。

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

class CoApp;
class InfraHttpHost;

// 把正式 body bridge 安装到真实 HTTP 宿主：宿主以 ProtoWireV1（RpcHandler）
// 模式监听，入站 RpcEnvelope 经 app.dispatch_inbound 走真实分发链，回复/错误
// 交回 infra 封包。仅填标准传输字段，不设置任何 x-bbt-* 头。
void InstallRpcWireBridge(InfraHttpHost& host, CoApp& app);

} // namespace bbt::framework
