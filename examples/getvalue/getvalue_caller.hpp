#pragma once
// bbt-framework Issue #4（P0-A）：Service→Service 出站 caller 业务（应用层）。
//
// 只用公开面：CoApp 装配的显式静态路由 + CoService + ICoService::call<Req,Resp>
// （typed 出站 seam，schema 由生成物 descriptor 推导）。业务不接触 envelope/
// codec/发送注入点/HttpClient。
//
// 四个方法承载不同真实出站场景（供跨进程 driver 断言）：
//   Forward          —— 正常 typed call（预算继承父请求，不放大）
//   ForwardExpired   —— 显式已过期 deadline；应在发起任何 I/O 前 TimedOut
//   ForwardNoRoute   —— 目标无静态路由；find_route 门拒绝，不发起 I/O
//   ForwardBlackhole —— 目标是「接受后立即断开」的真实端点（丢 reply）

#include <chrono>
#include <string_view>

#include <bbt/example/v1/get_value.pb.h>

#include <bbt/framework/CallOptions.hpp>
#include <bbt/framework/CoRpc.hpp>
#include <bbt/framework/CoService.hpp>
#include <bbt/framework/Result.hpp>
#include <bbt/framework/RpcMethods.hpp>

// ProtoCodec<GetValueRequest/Response> 特化（schema 真源 = .proto descriptor）
// 与 callee 服务同源定义；caller 复用同一 codec，不另造一套。
#include "getvalue_service.hpp"

namespace getvalue {

class GetValueCallerService final
    : public bbt::framework::CoService<GetValueCallerService> {
public:
    static constexpr std::string_view kServiceName =
        "bbt.example.v1.GetValueCallerService";
    // caller 默认转发目标（callee 进程宿主）。schema 真源 = .proto descriptor。
    static constexpr std::string_view kCalleeService =
        "bbt.example.v1.GetValueService";
    // 故意不配置静态路由的目标：find_route 门拒绝。
    static constexpr std::string_view kNoRouteService =
        "bbt.example.v1.UnroutedService";
    // 配置到「接受后立即断开」端点的目标：真实传输错误。
    static constexpr std::string_view kBlackholeService =
        "bbt.example.v1.BlackholeService";

    bbt::framework::CoRpcResp Forward(bbt::framework::CoRpcReq req) {
        return _ForwardTo(req, kCalleeService, bbt::framework::CallOptions{});
    }

    bbt::framework::CoRpcResp ForwardExpired(bbt::framework::CoRpcReq req) {
        bbt::framework::CallOptions opt;
        opt.deadline = std::chrono::steady_clock::now() -
            std::chrono::milliseconds{1};
        return _ForwardTo(req, kCalleeService, opt);
    }

    bbt::framework::CoRpcResp ForwardNoRoute(bbt::framework::CoRpcReq req) {
        return _ForwardTo(req, kNoRouteService, bbt::framework::CallOptions{});
    }

    bbt::framework::CoRpcResp ForwardBlackhole(bbt::framework::CoRpcReq req) {
        return _ForwardTo(req, kBlackholeService, bbt::framework::CallOptions{});
    }

    static constexpr auto kRpcMethods = bbt::framework::RpcMethods(
        bbt::framework::ProtoMethod<&GetValueCallerService::Forward,
                                    bbt::example::v1::GetValueRequest,
                                    bbt::example::v1::GetValueResponse>(
            "Forward"),
        bbt::framework::ProtoMethod<&GetValueCallerService::ForwardExpired,
                                    bbt::example::v1::GetValueRequest,
                                    bbt::example::v1::GetValueResponse>(
            "ForwardExpired"),
        bbt::framework::ProtoMethod<&GetValueCallerService::ForwardNoRoute,
                                    bbt::example::v1::GetValueRequest,
                                    bbt::example::v1::GetValueResponse>(
            "ForwardNoRoute"),
        bbt::framework::ProtoMethod<&GetValueCallerService::ForwardBlackhole,
                                    bbt::example::v1::GetValueRequest,
                                    bbt::example::v1::GetValueResponse>(
            "ForwardBlackhole"));

private:
    // 真实 typed 出站：解析入站业务 payload → call<Req,Resp>（descriptor 推导
    // schema、预算继承父请求、路由门、发送由框架完成）→ 回填 callee 响应。
    bbt::framework::CoRpcResp _ForwardTo(
        bbt::framework::CoRpcReq req, std::string_view target,
        const bbt::framework::CallOptions& opt) {
        using bbt::framework::CoRpcResp;
        auto parsed = req.ParseProto<bbt::example::v1::GetValueRequest>();
        if (!parsed)
            return CoRpcResp::Error(parsed.error());
        auto reply = this->call<bbt::example::v1::GetValueRequest,
                                bbt::example::v1::GetValueResponse>(
            target, "GetValue", parsed.value(), {}, opt);
        if (!reply)
            return CoRpcResp::Error(reply.error());
        return CoRpcResp::FromProto(reply.value());
    }
};

} // namespace getvalue
