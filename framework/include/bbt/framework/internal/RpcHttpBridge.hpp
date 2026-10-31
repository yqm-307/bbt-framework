#pragma once
// service-actor/v2：RPC envelope ↔ infra HTTP 的默认桥（机器面，internal/）。
//
// 这是框架的默认 HTTP 绑定：envelope 字段一对一映射到 x-bbt-* header，
// metadata 映射到 x-bbt-meta-<key>，payload 为 body；回复错误映射到
// x-bbt-err-* header + 非 200 状态。业务不感知本层——默认装配的
// CoApp 自动安装；测试缝可经 InstallRpcHttpBridge 复用同一映射。
//
// HttpEgress 是默认出站实现：按 RpcAddress.transport 选择 infra 公开
// rpc::Client + transport adapter（HTTP/TCP）；宿主释放后出站如实
// RuntimeUnavailable。

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>

#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
#include <bbt/infra/rpc/Client.hpp>
#endif

#include <bbt/framework/Result.hpp>

namespace bbt::framework {

class CoApp;
class InfraHttpHost;

namespace http_bridge {

// envelope → 出站 HttpRequest（url 形如 http://host:port/rpc）。
bbt::infra::HttpRequest ToHttpRequest(
    const bbt::infra::RpcEnvelope& env, std::string url);

// 入站 HttpRequest → envelope；header 不完整 → err(InvalidArgument)。
result<bbt::infra::RpcEnvelope> EnvelopeFromRequest(
    const bbt::infra::HttpRequest& req);

// 分发结果（envelope 或错误）→ HttpResponse。
bbt::infra::HttpResponse ToHttpResponse(
    const result<bbt::infra::RpcEnvelope>& r);

// 回复 HttpResponse → envelope/错误（状态 200 → envelope；否则
// x-bbt-err-* 还原 Error）。
result<bbt::infra::RpcEnvelope> EnvelopeFromResponse(
    const bbt::infra::HttpResponse& res);

} // namespace http_bridge

// 把默认桥安装到真实 HTTP 宿主：入站请求 → EnvelopeFromRequest →
// app.dispatch_inbound → ToHttpResponse。测试缝与业务默认路径共用。
void InstallRpcHttpBridge(InfraHttpHost& host, CoApp& app);

// 默认出站：transport 选择 infra 公开 rpc::Client + adapter seam；
// 宿主执行底座（Engine/NetworkRuntime）不可得 → RuntimeUnavailable。
// Issue #4 + 三仓 §5/§6 迁移后的 profile 语义：
//  - LegacyHeaders 走 x-bbt-* header 迁移期桥（旧栈 HttpClient；
//    http_bridge::ToHttpRequest/EnvelopeFromResponse），仅 HTTP。
//  - ProtoWireV1 走 infra 公开 rpc::Client + HttpRpcTransport：wire
//    编解码/关联/预算换算全部由 infra 承担（POST /rpc 固定 profile）。
//  - TcpV1 走 infra 公开 rpc::Client + TcpRpcTransport：u32be 长度前缀
//    帧（architecture §6），endpoint 必须 numeric-ip:port。
// 后两者共用同一份懒建 rpc::Client 缓存与同一个 client-only Engine
// （InfraHttpHost::egress_engine()）；LegacyHeaders 仍用旧栈
// NetworkRuntime::CreateHttpClient，不占用新栈 Engine。
enum class RpcEgressProfile { LegacyHeaders, ProtoWireV1, TcpV1 };

class HttpEgress {
public:
    // host 弱持有：宿主对象本身可空（纯出站宿主）或经 Close() 释放；
    // 两者都以 RuntimeUnavailable 如实返回。
    explicit HttpEgress(std::weak_ptr<InfraHttpHost> host,
                        RpcEgressProfile profile =
                            RpcEgressProfile::LegacyHeaders);

    // 协程内调用。addr.transport ∈ {"http","tcp"}（其余 → InvalidArgument）；
    // LegacyHeaders profile 仅接受 "http"。endpoint 合法性归 adapter。
    result<bbt::infra::RpcEnvelope> Send(
        const bbt::infra::RpcAddress& addr,
        const bbt::infra::RpcEnvelope& env,
        const bbt::infra::CallOptions& opt);

private:
    // ProtoWireV1/TcpV1 分支：懒建 rpc::Client（HttpRpcTransport 或
    // TcpRpcTransport 底座）→ Call(addr, env, opt)；wire/关联/预算/错误
    // 终态全部透传 infra，不在本层改写。BBT_FRAMEWORK_HAS_RPC_WIRE 不可用时
    // 返回 RuntimeUnavailable。
    result<bbt::infra::RpcEnvelope> _SendRpcProfile(const bbt::infra::RpcAddress& addr,
                                                    const bbt::infra::RpcEnvelope& env,
                                                    const bbt::infra::CallOptions& opt);

    std::weak_ptr<InfraHttpHost> m_host;
    RpcEgressProfile m_profile;
    std::mutex m_mtx;
    // 懒建共享出站通道：LegacyHeaders 用旧栈 HttpClient（runtime 限额 owner
    // 共享 max_connections/max_inflight 与请求/响应字节校验）；新栈
    // profile 共用同一个 client-only Engine 上的 rpc::Client。
    std::shared_ptr<bbt::infra::HttpClient> m_client;
#if defined(BBT_FRAMEWORK_HAS_RPC_WIRE)
    std::shared_ptr<bbt::infra::rpc::Client> m_rpc_client;
#endif
};

} // namespace bbt::framework
