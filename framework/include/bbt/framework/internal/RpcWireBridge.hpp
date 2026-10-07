#pragma once
// bbt-framework Issue #4（P0-A）：正式 RPC body bridge（机器面，internal/）。
//
// 与默认 RpcHttpBridge（x-bbt-* header 承载 envelope，迁移期绑定）不同，本桥
// 使用 bbtools-infra Issue #8 冻结的版本化 wire profile：
//   - HTTP POST /rpc，Content-Type: application/x-protobuf，body = RpcEnvelopeMsg；
//   - envelope 内承载 service/method/request_id/request_schema/response_schema/
//     remaining_budget_ms/outcome；不读写任何 x-bbt-* 头。
//
// 入站：ParseRpcWireHttpRequest → 接收端 local-min 预算 → app.dispatch_inbound
// → wire 响应 envelope（成功/错误信封不变成功）。业务经默认 CoApp 装配选择本桥
// （CoAppOptions::inbound_bridge = RpcInboundBridge::ProtoWireV1），不使用测试缝。
//
// 依赖 bbt::infra_rpc（仅在 protobuf 前缀锁定可用时编译）。

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/rpc/RpcWire.hpp>

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

class CoApp;
class InfraHttpHost;

// 把正式 body bridge 安装到真实 HTTP 宿主：入站请求 → ParseRpcWireHttpRequest →
// dispatch_inbound → MakeRpcWireHttpResponse。仅填标准传输字段，不设置任何
// x-bbt-* 头。
void InstallRpcWireBridge(InfraHttpHost& host, CoApp& app);

} // namespace bbt::framework
