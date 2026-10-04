# F2-a 起框架引入编译产物：bbt_framework 由 INTERFACE 改为 STATIC，
# 仍对外暴露 framework/include/。链接 bbt::infra_common 获得
# result/Codec 与 coroutine 运行时（含 include 路径传播）；
# F1-b2 的 InfraHttpHost 消费 infra 已验收的 HTTP 实现目标。
add_library(bbt_framework STATIC
    ExecutionPolicy.cc
    MethodTable.cc
    actor/ActorMailbox.cc
    actor/ActorRegistry.cc
    actor/OrderedSession.cc
    actor/OrderedIngress.cc
    host/CallOptionsAdapter.cc
    host/RequestScope.cc
    host/CoApp.cc
    host/CoServiceCall.cc
    host/HostLifecycle.cc
    host/InboundDispatcher.cc
    host/InfraHttpHost.cc
    host/ResourceClose.cc
    host/RpcHttpBridge.cc)
add_library(bbt::framework ALIAS bbt_framework)
target_include_directories(bbt_framework PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
    $<INSTALL_INTERFACE:include>)
target_link_libraries(bbt_framework PUBLIC bbt::infra_common bbt::infra_http)
target_compile_features(bbt_framework PUBLIC cxx_std_17)
target_compile_options(bbt_framework PRIVATE -Wall -Wextra)

# Issue #4（P0-A）：正式 RPC body bridge 只在 protobuf 前缀锁定、infra 注册
# bbt_infra_rpc 目标时编译。未配置时框架仅提供迁移期 header 桥；显式选择
# ProtoWireV1 在 run 前校验期非零失败（不静默降级）。
if(TARGET bbt_infra_rpc)
    target_sources(bbt_framework PRIVATE host/RpcWireBridge.cc)
    target_link_libraries(bbt_framework PUBLIC bbt::infra_rpc)
    target_compile_definitions(bbt_framework PUBLIC BBT_FRAMEWORK_HAS_RPC_WIRE=1)
    message(STATUS "bbt-framework: 启用正式 RPC wire bridge (bbt::infra_rpc)")
else()
    message(STATUS
        "bbt-framework: 未注册 bbt_infra_rpc（缺 BBT_PROTOBUF_PREFIX）："
        "只有迁移期 header 入站桥")
endif()
