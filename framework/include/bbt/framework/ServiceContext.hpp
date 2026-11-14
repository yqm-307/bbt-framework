#pragma once
// 服务侧上下文（业务面，protected 访问）。
//
// ServiceContext 是框架注入到每个服务实例的上下文对象，只承载当前受管请求上下文：
// `request()` 返回本次逻辑请求的只读快照（request_id、deadline、actor_key、
// peer_principal、trace_id），非受管调用返回 err(InvalidContext)。
//
// 能力对象（Redis/Mongo 等）不在这里：framework 只有一条受管能力路径——组件经
// `v2::CoApp::CoInstallComponent` 安装、`ServiceOptions::BindComponent` 绑定、
// `ComponentHandle::Borrow` 借用。本类型是轻量值，不含运行时/网络细节。

#include <memory>

#include <bbt/framework/RequestContext.hpp>
#include <bbt/framework/Result.hpp>

namespace bbt::framework {

class ServiceContext {
public:
    // 当前受管请求上下文；非受管 → err(InvalidContext)。
    result<std::shared_ptr<const RequestContext>> request() const {
        return CurrentRequestContext();
    }
};

} // namespace bbt::framework
