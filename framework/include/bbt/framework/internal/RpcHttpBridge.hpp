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

// 默认出站：HttpClient 懒建（首个发送时在协程内创建，协程约束与
// infra 一致）；runtime 不可得 → RuntimeUnavailable。
class HttpEgress {
public:
    // host 弱持有：宿主对象本身可空（纯出站宿主）或由 ReleaseClosed
    // 释放；两者都以 RuntimeUnavailable 如实返回。
    explicit HttpEgress(std::weak_ptr<InfraHttpHost> host);

    // 协程内调用。addr.transport 仅支持 "http"（其余 → InvalidArgument）。
    result<bbt::infra::RpcEnvelope> Send(
        const bbt::infra::RpcAddress& addr,
        const bbt::infra::RpcEnvelope& env,
        const bbt::infra::CallOptions& opt);

private:
    std::weak_ptr<InfraHttpHost>            m_host;
    std::mutex                              m_mtx;
    std::shared_ptr<bbt::infra::HttpClient> m_client;
};

} // namespace bbt::framework
