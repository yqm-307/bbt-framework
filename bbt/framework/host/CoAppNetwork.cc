#include <bbt/framework/CoApp.hpp>

#include <bbt/framework/internal/AppRuntimeState.hpp>
#include <bbt/framework/internal/InfraHttpHost.hpp>
#include <bbt/framework/internal/OrderedIngress.hpp>

namespace bbt::framework {

result<void> CoApp::_ValidateNetworkConfig() const {
    if (!m_network_host)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "CoApp: network host is not configured"));
    if (m_options.ListenUri() && m_options.ListenAddr().host.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "CoApp: listen host is empty"));
    if (auto r = bbt::infra::ValidateNetworkLimits(m_options.NetworkLimits()); !r)
        return r;
    bbt::infra::rpc::ClientOptions client_options;
    client_options.SetLocalIdentity(m_options.LocalIdentity())
        .SetTrustedNetworkPlaintext(true);
    if (auto valid = client_options.Validate(); !valid)
        return valid;
    std::map<std::string, int> seen;
    for (const auto& r : m_options.StaticRoutes()) {
        if (r.service_name.empty())
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                               "CoApp: static route with empty service_name"));
        if (r.address.empty())
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                               "CoApp: static route '" + r.service_name +
                                                   "' has empty endpoint"));
        if (seen.count(r.service_name) != 0)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                               "CoApp: duplicate static route service_name"));
        seen.emplace(r.service_name, 1);
    }
    return result<void>::ok();
}

CoServiceBase::RpcSendFn CoApp::_MakeEgress() {
    /**
     * 出站回调复制运行时状态的 shared_ptr——不捕获 this、不借用 CoApp
     * 成员；路由门/策略/发送实现全部在状态内（见 AppRuntimeState::Send）。
     * 返回类型即服务侧受管出站注入点；真实出站可挂起当前协程。
     */
    auto state = m_runtime;
    return [state = std::move(state)](const bbt::infra::rpc::Request& env,
                                      const bbt::infra::CallOptions& options)
               -> bbt_co_async(result<bbt::infra::rpc::Response>) {
        return state->Send(env, options);
    };
}

void CoApp::_BindEgressToService(const std::string& service_name) {
    /**
     * 出站注入点经 Registry 受管对象安装：锁内取对象指针，锁外绑定
     * （对象销毁排在 owner 释放之后；安装路径刚刚经 owner 创建成功）。
     */
    std::unique_lock<std::mutex> lk(m_registry->Mutex());
    auto* e = m_registry->FindByTypeNameLocked(ObjectKind::Service,
                                               service_name);
    if (!e)
        return;
    auto* svc = static_cast<CoServiceBase*>(e->object.get());
    lk.unlock();
    svc->_BindNetwork(_MakeEgress());
}

result<void> CoApp::_StartNetworkPhase(bbt::coroutine::Deadline deadline) {
    std::map<std::string, InboundDispatcher::ServiceBinding> dispatch;
    {
        std::lock_guard<std::mutex> lk(m_net_mtx);
        for (const auto& [name, description] : m_service_descriptions) {
            const auto& ordered = description->options.OrderedIngress();
            // 首次装配才安装流授权。后续发布共享已有入口及其流状态。
            if (ordered.enabled && !description->ordered_ingress) {
                auto ingress = OrderedIngress::Create(OrderedIngressConfig{
                    name, ordered.max_ordered_streams, ordered.max_cached_results,
                    ordered.max_cached_result_bytes});
                if (!ingress)
                    return result<void>::err(std::move(ingress.error()));
                for (const auto& g : m_pending_ordered_grants) {
                    if (g.service != name)
                        continue;
                    if (auto r = ingress.value()->GrantStream(g); !r)
                        return r;
                }
                description->ordered_ingress = std::move(ingress.value());
            }
            /**
             * 实例与 id 同在 Registry 锁内取得。每次发布单独绑定实例和租约，
             * 已持有旧记录的派发继续使用原绑定。
             */
            std::unique_lock<std::mutex> rlk(m_registry->Mutex());
            auto* e = m_registry->FindByTypeNameLocked(ObjectKind::Service,
                                                       name);
            if (!e)
                return result<void>::err(MakeError(ErrorCode::InternalError,
                                                   "network bind: service entry vanished: " + name));
            auto* svc = static_cast<CoServiceBase*>(e->object.get());
            const ObjectId service_id = e->id;
            rlk.unlock();
            auto reg = m_registry;
            dispatch.emplace(name, InboundDispatcher::ServiceBinding{
                                       description,
                                       {svc,
                                        [reg, service_id] { return reg->AcquireDispatchLease(service_id); },
                                        [reg, service_id] { reg->NoteHookSettled(service_id); }}});
        }
        /**
         * 授权已由各服务的有序入口持有，清掉启动前暂存。
         */
        m_pending_ordered_grants.clear();
    }

    /**
     * Publish 最后开放接纳：宿主 Start 之前 dispatcher 已可分发
     * （测试缝入站路径在宿主外驱动），真实入站在宿主 Start 后到达。
     */
    m_runtime->Publish(std::make_shared<InboundDispatcher>(std::move(dispatch)));

    // RPC 接纳启动要求 native host context。等待结束后仍须回收辅助线程。
    auto r = _RunNetworkTask([host = m_network_host] {
        auto started = host->Create();
        return started ? host->Start() : started;
    },
                             deadline, "start: network host create/start wait did not complete");
    if (!r) {
        m_runtime->Release();
        return r;
    }
    m_network_published = true;
    return result<void>::ok();
}

result<void> CoApp::_DrainNetwork(bbt::coroutine::Deadline deadline) {
    // WaitHandlersDone 在 native 线程等待。协程等待结束后仍须确认线程退出。
    return _RunNetworkTask([host = m_network_host, deadline] {
        return host->WaitHandlersDone(deadline);
    },
                           deadline, "stop: handler drain wait did not complete");
}

result<bbt::infra::rpc::Response> CoApp::dispatch_inbound(
    const bbt::infra::rpc::Request& request) {
    return m_runtime->Dispatch(request);
}

result<void> CoApp::grant_ordered_stream(OrderedGrant grant) {
    std::lock_guard<std::mutex> lk(m_net_mtx);
    if (m_state.load() != AppState::Created)
        return result<void>::err(MakeError(
            ErrorCode::Closed,
            "grant_ordered_stream: registration closed (app already started)"));
    auto it = m_service_descriptions.find(grant.service);
    if (it == m_service_descriptions.end())
        return result<void>::err(MakeError(
            ErrorCode::NotFound,
            "grant_ordered_stream: service '" + grant.service +
                "' is not registered"));
    if (!it->second->options.OrderedIngress().enabled)
        return result<void>::err(MakeError(
            ErrorCode::InvalidArgument,
            "grant_ordered_stream: service '" + grant.service +
                "' does not enable ordered_ingress"));
    /**
     * 重复登记相同授权不报错。同一条流的授权内容若不同，则返回 StreamRejected；不同流互不影响。
     */
    for (const auto& existing : m_pending_ordered_grants) {
        if (!SameOrderedStreamId(existing, grant))
            continue;
        if (existing == grant)
            return result<void>::ok();
        return result<void>::err(MakeError(
            ErrorCode::InvalidArgument,
            "grant_ordered_stream: conflicting grant for stream '" +
                grant.service + "/" + grant.actor_key + "'"));
    }
    m_pending_ordered_grants.push_back(std::move(grant));
    return result<void>::ok();
}

std::string CoApp::BoundEndpoint() const {
    return m_default_host ? m_default_host->bound_endpoint()
                          : std::string{};
}

} // namespace bbt::framework
