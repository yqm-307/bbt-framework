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
    host/CallOptionsAdapter.cc
    host/RequestScope.cc
    host/CoApp.cc
    host/CoServiceCall.cc
    host/HostLifecycle.cc
    host/InboundDispatcher.cc
    host/InfraHttpHost.cc
    host/RpcHttpBridge.cc)
add_library(bbt::framework ALIAS bbt_framework)
target_include_directories(bbt_framework PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
    $<INSTALL_INTERFACE:include>)
target_link_libraries(bbt_framework PUBLIC bbt::infra_common bbt::infra_http)
target_compile_features(bbt_framework PUBLIC cxx_std_17)
target_compile_options(bbt_framework PRIVATE -Wall -Wextra)
