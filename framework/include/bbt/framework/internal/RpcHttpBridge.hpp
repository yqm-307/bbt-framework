#pragma once
// service-actor/v2：RPC envelope ↔ infra HTTP 的默认桥（机器面，internal/）。
//
// 这是框架的默认 HTTP 绑定：envelope 字段一对一映射到 x-bbt-* header，
// metadata 映射到 x-bbt-meta-<key>，payload 为 body；回复错误映射到
// x-bbt-err-* header + 非 200 状态。业务不感知本层——默认装配的
// CoApp 自动安装；测试缝可经 InstallRpcHttpBridge 复用同一映射。
//
// HttpEgress 是默认出站实现：按 RpcAddress 组 URL、经宿主 runtime 的
// HttpClient 发送并解码回复；宿主释放后出站如实 RuntimeUnavailable。

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

// 默认出站：client 懒建（首个发送时创建，协程约束与 infra 一致）；
// runtime 不可得 → RuntimeUnavailable。
// Issue #4：出站协议 profile 与入站一致：
//  - LegacyHeaders 走 x-bbt-* header 迁移期桥（HttpClient；
//    http_bridge::ToHttpRequest/EnvelopeFromResponse）。
//  - ProtoWireV1 走 infra 正式 body 信封：同一
//    NetworkRuntime::CreateHttpClient 出站通道承载（runtime 限额 owner
//    共享 max_connections/max_inflight，请求/响应字节按宿主
//    NetworkLimits 校验），wire 编码/关联/预算换算经 infra 公开
//    RpcWire 面（POST /rpc 固定 profile）。两个 profile 共用同一份懒建
//    HttpClient 缓存（同一个 runtime、同一个限额 owner，profile 只决定
//    wire 编解码）。
enum class RpcEgressProfile { LegacyHeaders, ProtoWireV1 };

class HttpEgress {
public:
    // host 弱持有：宿主对象本身可空（纯出站宿主）或经 Close() 释放；
    // 两者都以 RuntimeUnavailable 如实返回。
    explicit HttpEgress(std::weak_ptr<InfraHttpHost> host,
                        RpcEgressProfile profile =
                            RpcEgressProfile::LegacyHeaders);

    // 协程内调用。addr.transport 仅支持 "http"（其余 → InvalidArgument）。
    result<bbt::infra::RpcEnvelope> Send(
        const bbt::infra::RpcAddress& addr,
        const bbt::infra::RpcEnvelope& env,
        const bbt::infra::CallOptions& opt);

private:
    // ProtoWireV1 分支：endpoint 边界检查 → 本地期限→remaining_budget_ms
    // （已过期 → TimedOut 零 I/O）→ ToWireEnvelope/EncodeRpcWireEnvelope →
    // 共享 HttpClient POST /rpc → 状态/Content-Type/五字段关联校验 →
    // DecodeRpcWireEnvelope/FromWireEnvelope。仅 BBT_FRAMEWORK_HAS_RPC_WIRE
    // 时可用。
    result<bbt::infra::RpcEnvelope> _SendWireProfile(const bbt::infra::RpcAddress& addr,
                                                     const bbt::infra::RpcEnvelope& env,
                                                     const bbt::infra::CallOptions& opt);

    std::weak_ptr<InfraHttpHost> m_host;
    RpcEgressProfile m_profile;
    std::mutex m_mtx;
    // 懒建共享 HttpClient：runtime 限额 owner（max_connections/max_inflight
    // 与请求/响应字节校验）由 NetworkRuntime 统一持有；两个 profile 共用
    // 这一个发送 root，不另建独立 client/engine/pool。
    std::shared_ptr<bbt::infra::HttpClient> m_client;
};

} // namespace bbt::framework
