#pragma once
// service-actor/v2：业务服务基类（CRTP，可选便利层）。
//
// 业务服务形如：
//   class EchoSvc final : public fw::CoService<EchoSvc> {
//   public:
//       static constexpr std::string_view kServiceName = "echo";
//       fw::result<EchoReply> Ping(const EchoReq& req);
//       static constexpr auto kRpcMethods = fw::RpcMethods(
//           fw::Method<&EchoSvc::Ping>("ping"),
//           fw::ActorMethod<&EchoSvc::Xfer, &XferReq::acct>("xfer"));
//   };
// CoService<T> 只供给服务名。方法表由 CoApp::add_service 在内部展开，
// 业务不写 binder，也不拿 RpcMethodTable。
// 直接继承 ICoService 仍合法（自行提供 co_service_name 与
// kRpcMethods 声明即可）。

#include <string_view>
#include <type_traits>

#include <bbt/framework/ICoService.hpp>
#include <bbt/framework/RpcMethods.hpp>

namespace bbt::framework {

template <class T>
class CoService : public ICoService {
public:
    // 服务名来自类型级声明：kServiceName 缺失即编译期暴露。
    std::string_view co_service_name() const noexcept override {
        return T::kServiceName;
    }
};

} // namespace bbt::framework
