# 框架引入编译产物：bbt_framework 由 INTERFACE 改为 STATIC，
# 仍对外暴露 framework/include/。链接 bbt::infra_common 获得
# result/Codec 与 coroutine 运行时（含 include 路径传播）；
# InfraHttpHost 消费 infra 已验收的 HTTP 实现目标。
add_library(bbt_framework STATIC
    ExecutionPolicy.cc
    MethodTable.cc
    actor/ActorMailbox.cc
    actor/ActorRegistry.cc
    actor/OrderedSession.cc
    actor/OrderedIngress.cc
    host/App.cc
    host/AppRuntimeState.cc
    host/CallOptionsAdapter.cc
    host/CoApp.cc
    host/CoServiceCall.cc
    host/DynamicRouteTable.cc
    host/HostLifecycle.cc
    host/InboundDispatcher.cc
    host/InfraHttpHost.cc
    host/Object.cc
    host/Registry.cc
    host/RequestScope.cc
    host/ResourceClose.cc
    host/RpcBridge.cc
    host/Service.cc)
add_library(bbt::framework ALIAS bbt_framework)
target_include_directories(bbt_framework PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
    $<INSTALL_INTERFACE:include>)
if(NOT TARGET bbt::infra_http_net)
    message(FATAL_ERROR "bbt-framework requires bbt::infra_http_net (BBT_INFRA_HTTP_NET=ON)")
endif()
target_link_libraries(bbt_framework PUBLIC bbt::infra_common bbt::infra_http_net)
target_compile_features(bbt_framework PUBLIC cxx_std_17)
target_compile_options(bbt_framework PRIVATE -Wall -Wextra)

# RPC 是框架必需能力：只有公开 V2 target，没有 header 或旧传输降级。
if(NOT TARGET bbt::infra_rpc_net)
    message(FATAL_ERROR "bbt-framework requires bbt::infra_rpc_net (BBT_INFRA_RPC_NET=ON and pinned protobuf prefix)")
endif()
target_link_libraries(bbt_framework PUBLIC bbt::infra_rpc_net)
