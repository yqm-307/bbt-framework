# Boost.Test 静态内嵌写法（同 bbtools-coroutine 单测约定）：
# BOOST_TEST_MAIN + boost/test/included/unit_test.hpp，
# 不得再链接 libboost_unit_test_framework.so——共享库与二进制各持一份
# 框架全局状态，ELF 符号插入会在 __cxa_finalize 阶段 double-free。
#
# 命名契约：文件 Test_<module>_<行为>.cc，ctest 名 <module>.<行为>，
# label 只用 smoke / unit / integration / live / perf：
#   unit        纯语义/状态机断言，无 socket
#   integration 真实 loopback 或多组件协作，不依赖外部服务
#   live        需要真实 Redis/Mongo 等外部服务（本地不注册）
#   smoke       构建后快速可用性子集（unit + 快速 integration）

# CoRpc 公共面、声明式方法表、错误边界、actor key、路由与资源缝；
# 不联网、不起真实 I/O、不启动 Scheduler。
add_executable(Test_framework_rpc_surface Test_framework_rpc_surface.cc)
target_link_libraries(Test_framework_rpc_surface PRIVATE bbt::framework)
target_compile_options(Test_framework_rpc_surface PRIVATE -Wall -Wextra)
add_test(NAME framework.rpc_surface COMMAND Test_framework_rpc_surface)
set_tests_properties(framework.rpc_surface PROPERTIES
    TIMEOUT 15 LABELS "smoke;unit")

# 执行策略校验矩阵、ActorSerial 缺 key 提取器启动失败、Actor 邮箱
# 保序/非重入/容量/异常续跑/Close、跨 Actor 并发、ActorRegistry 至多
# 一次与上限。真实协程调度 + 屏障构造并发窗口；与端口无关。
add_executable(Test_framework_actor_mailbox Test_framework_actor_mailbox.cc)
target_link_libraries(Test_framework_actor_mailbox PRIVATE bbt::framework)
target_compile_options(Test_framework_actor_mailbox PRIVATE -Wall -Wextra)
add_test(NAME framework.actor_mailbox COMMAND Test_framework_actor_mailbox)
set_tests_properties(framework.actor_mailbox PROPERTIES
    TIMEOUT 30 LABELS "smoke;unit")

# OrderedSession 发送侧：授权流校验、票据绑定、序号签发。
add_executable(Test_framework_ordered_egress Test_framework_ordered_egress.cc)
target_link_libraries(Test_framework_ordered_egress PRIVATE bbt::framework)
target_compile_options(Test_framework_ordered_egress PRIVATE -Wall -Wextra)
add_test(NAME framework.ordered_egress COMMAND Test_framework_ordered_egress)
set_tests_properties(framework.ordered_egress PROPERTIES
    TIMEOUT 30 LABELS "smoke;unit")

# 有序入口接收侧：授权流校验、每流序号状态机与去重终态缓存；
# 缺口/在途/冲突/淘汰/epoch 切换/无票据均由组件语义核直接断言，
# 不依赖入站接线（接线见 framework.ordered_ingress_http）。
add_executable(Test_framework_ordered_ingress Test_framework_ordered_ingress.cc)
target_link_libraries(Test_framework_ordered_ingress PRIVATE bbt::framework)
target_compile_options(Test_framework_ordered_ingress PRIVATE -Wall -Wextra)
add_test(NAME framework.ordered_ingress COMMAND Test_framework_ordered_ingress)
set_tests_properties(framework.ordered_ingress PROPERTIES
    TIMEOUT 30 LABELS "smoke;unit")

# 有序入口真实 HTTP loopback 接线验收。
add_executable(Test_framework_ordered_ingress_http
    Test_framework_ordered_ingress_http.cc)
target_link_libraries(Test_framework_ordered_ingress_http PRIVATE bbt::framework)
target_compile_options(Test_framework_ordered_ingress_http PRIVATE -Wall -Wextra)
add_test(NAME framework.ordered_ingress_http
    COMMAND Test_framework_ordered_ingress_http)
set_tests_properties(framework.ordered_ingress_http PROPERTIES
    TIMEOUT 60 LABELS "smoke;integration")

# deadline 继承与受管请求上下文（RequestContext/RequestScope/
# CallOptionsAdapter）；真实协程调度器 + 注入点断言。
add_executable(Test_framework_request_context Test_framework_request_context.cc)
target_link_libraries(Test_framework_request_context PRIVATE bbt::framework)
target_compile_options(Test_framework_request_context PRIVATE -Wall -Wextra)
add_test(NAME framework.request_context COMMAND Test_framework_request_context)
set_tests_properties(framework.request_context PROPERTIES
    TIMEOUT 30 LABELS "smoke;unit")

# CoApp 装配/校验 + INetworkHost 桩驱动的生命周期状态机断言。
add_executable(Test_framework_coapp_lifecycle Test_framework_coapp_lifecycle.cc)
target_link_libraries(Test_framework_coapp_lifecycle PRIVATE bbt::framework)
target_compile_options(Test_framework_coapp_lifecycle PRIVATE -Wall -Wextra)
add_test(NAME framework.coapp_lifecycle COMMAND Test_framework_coapp_lifecycle)
set_tests_properties(framework.coapp_lifecycle PROPERTIES
    TIMEOUT 30 LABELS "smoke;unit")

# 真实 HTTP loopback 入站→上下文落地→分发→回复；
# InvalidContext 端到端、同步 Close、跨 worker 不串扰。
add_executable(Test_framework_inbound_http Test_framework_inbound_http.cc)
target_link_libraries(Test_framework_inbound_http PRIVATE bbt::framework)
target_compile_options(Test_framework_inbound_http PRIVATE -Wall -Wextra)
add_test(NAME framework.inbound_http COMMAND Test_framework_inbound_http)
set_tests_properties(framework.inbound_http PROPERTIES
    TIMEOUT 60 LABELS "smoke;integration")

# 关闭与排空：外部线程 request_shutdown、关闭期间在途 handler
# 真实出站、ShutdownIncomplete 可观察与迟到收尾、逻辑结束≠物理清理。
add_executable(Test_framework_shutdown Test_framework_shutdown.cc)
target_link_libraries(Test_framework_shutdown PRIVATE bbt::framework)
target_compile_options(Test_framework_shutdown PRIVATE -Wall -Wextra)
add_test(NAME framework.shutdown COMMAND Test_framework_shutdown)
# 各用例的真实时长由 infra keep-alive 连接的 runtime 排空决定
# （单用例实测 ~30s，全套 ~107s），60s 限时必然误杀，按实测×1.5 留裕量。
set_tests_properties(framework.shutdown PROPERTIES
    TIMEOUT 180 LABELS "integration")

# 关闭门回归：StopAccepting 闭门——「infra 已接纳、尚未进入入站包装」
# 的迟到进入者被拒且不触达业务、WaitHandlersDone 不漏记、已过门者不被
# 抢占；另有独立宿主的直接 Close 兼容小断言。一组用例走 internal 测试缝
# （确定性、无 socket），一组走真实 HTTP loopback，失败路径由 RAII 夹具收尾。
add_executable(Test_framework_close_gate Test_framework_close_gate.cc)
target_link_libraries(Test_framework_close_gate PRIVATE bbt::framework)
target_compile_options(Test_framework_close_gate PRIVATE -Wall -Wextra)
add_test(NAME framework.close_gate COMMAND Test_framework_close_gate)
set_tests_properties(framework.close_gate PROPERTIES
    TIMEOUT 30 LABELS "smoke;integration")

# CoRpcReq/CoRpcResp 公共面：位置参数、尾部协议错误、业务错误原样
# 传递、protobuf codec seam 与 ActorMethodAt 位置 key。
add_executable(Test_framework_rpc_codec Test_framework_rpc_codec.cc)
target_link_libraries(Test_framework_rpc_codec PRIVATE bbt::framework)
target_compile_options(Test_framework_rpc_codec PRIVATE -Wall -Wextra)
add_test(NAME framework.rpc_codec COMMAND Test_framework_rpc_codec)
set_tests_properties(framework.rpc_codec PROPERTIES
    TIMEOUT 15 LABELS "smoke;unit")

# 命名资源 Registry：工厂装配、Create/Start 相位、失败回滚与
# 关闭序列下的同步 Close 收束（INetworkHost 桩驱动）。
add_executable(Test_framework_resource_lifecycle
    Test_framework_resource_lifecycle.cc)
target_link_libraries(Test_framework_resource_lifecycle PRIVATE bbt::framework)
target_compile_options(Test_framework_resource_lifecycle PRIVATE -Wall -Wextra)
add_test(NAME framework.resource_lifecycle
    COMMAND Test_framework_resource_lifecycle)
set_tests_properties(framework.resource_lifecycle PROPERTIES
    TIMEOUT 30 LABELS "smoke;unit")

# 强制 live 验收开关（默认 OFF）：live runner/CI 显式
# -DBBT_REQUIRE_RESOURCE_LIVE_TESTS=ON 时，Redis/Mongo 任一 infra 模块未注册即
# configure 阶段 FATAL——不允许静默缺模块导致验收证据缺口（与 framework.rpc_xlang
# 对 bbt_infra_rpc_net 的 fail-closed 同一口径）。OFF 时维持可选 target：缺模块走
# 下面的 STATUS 提示，环境缺后端时进程 77 退出配合 SKIP_RETURN_CODE 标 Skipped，
# 均不宣称 live 通过。
option(BBT_REQUIRE_RESOURCE_LIVE_TESTS
    "强制真实 Redis+Mongo live 验收（缺模块即 configure 失败）" OFF)
if(BBT_REQUIRE_RESOURCE_LIVE_TESTS)
    if(NOT TARGET bbt::infra_redis OR NOT TARGET bbt::infra_mongo)
        message(FATAL_ERROR
            "BBT_REQUIRE_RESOURCE_LIVE_TESTS=ON 但 bbt::infra_redis/"
            "bbt::infra_mongo 未全部注册：live 验收为强制项，不允许"
            "静默缺模块。请提供 -DBBT_HIREDIS_PREFIX=<hiredis 前缀> 与 "
            "-DBBT_MONGOC_PREFIX=/-DBBT_MONGOCXX_PREFIX=<mongoc/mongocxx 前缀>"
            " 使两个目标注册后重新配置。")
    endif()
endif()

# live 验收：真实 Redis/MongoDB 容器经工厂装配 + service 资源缝
# 消费 + 关闭收束到 IsClosed。Redis 与 Mongo 是两个独立 target/CTest 用例，
# 各自显式要求对应 infra 模块已构建（缺 target → 编译期失败而不是静默
# 消失）；环境变量缺失时进程以 77 退出，配合 SKIP_RETURN_CODE 标为
# CTest Skipped——缺后端必须显式 skipped 或非零失败，不得记为 Passed。
if(TARGET bbt::infra_redis)
    add_executable(Test_framework_resource_live_redis
        Test_framework_resource_live.cc)
    target_link_libraries(Test_framework_resource_live_redis PRIVATE
        bbt::framework bbt::infra_redis)
    target_compile_options(Test_framework_resource_live_redis PRIVATE
        -Wall -Wextra)
    target_compile_definitions(Test_framework_resource_live_redis PRIVATE
        BBT_LIVE_HAS_REDIS=1)
    add_test(NAME framework.resource_live_redis
        COMMAND Test_framework_resource_live_redis)
    set_tests_properties(framework.resource_live_redis PROPERTIES
        TIMEOUT 120
        SKIP_RETURN_CODE 77
        LABELS "live")
else()
    message(STATUS
        "framework.resource_live_redis: 未构建 bbt::infra_redis "
        "(BBT_HIREDIS_PREFIX)，本配置无 Redis live 门禁")
endif()

if(TARGET bbt::infra_mongo)
    add_executable(Test_framework_resource_live_mongo
        Test_framework_resource_live.cc)
    target_link_libraries(Test_framework_resource_live_mongo PRIVATE
        bbt::framework bbt::infra_mongo)
    target_compile_options(Test_framework_resource_live_mongo PRIVATE
        -Wall -Wextra)
    target_compile_definitions(Test_framework_resource_live_mongo PRIVATE
        BBT_LIVE_HAS_MONGO=1)
    add_test(NAME framework.resource_live_mongo
        COMMAND Test_framework_resource_live_mongo)
    set_tests_properties(framework.resource_live_mongo PROPERTIES
        TIMEOUT 120
        SKIP_RETURN_CODE 77
        LABELS "live")
else()
    message(STATUS
        "framework.resource_live_mongo: 未构建 bbt::infra_mongo "
        "(BBT_MONGOC_PREFIX+BBT_MONGOCXX_PREFIX)，本配置无 Mongo live 门禁")
endif()

# 联合 live 验收：真实 HTTP loopback → 框架入站 → 业务 Service →
# 真实 Redis + Mongo 联合消费。要求两个 infra 资源模块都在场（任一缺失
# 即不注册本 target——单资源 live 不能替代联合验收），缺环境时进程 77
# 退出配合 SKIP_RETURN_CODE 标 Skipped。
if(TARGET bbt::infra_redis AND TARGET bbt::infra_mongo)
    # 业务 Service 在独立 TU framework_resource_service.cc 中实现（只 include
    # framework/infra/coroutine 公开头），与测试宿主分属不同编译单元——这是
    # 「干净消费者 Service 只公共头即可按名称使用资源」的真实编译证据。
    add_executable(Test_framework_resource_live_combined
        Test_framework_resource_combined.cc
        framework_resource_service.cc)
    target_link_libraries(Test_framework_resource_live_combined PRIVATE
        bbt::framework bbt::infra_redis bbt::infra_mongo)
    target_compile_options(Test_framework_resource_live_combined PRIVATE
        -Wall -Wextra)
    target_compile_definitions(Test_framework_resource_live_combined PRIVATE
        BBT_LIVE_HAS_REDIS=1 BBT_LIVE_HAS_MONGO=1)
    add_test(NAME framework.resource_live_combined
        COMMAND Test_framework_resource_live_combined)
    set_tests_properties(framework.resource_live_combined PROPERTIES
        TIMEOUT 180
        SKIP_RETURN_CODE 77
        LABELS "live")
else()
    message(STATUS
        "framework.resource_live_combined: 需要 bbt::infra_redis 与 "
        "bbt::infra_mongo 同时在场，本配置无联合 live 门禁")
endif()

# RpcEnvelope wire profile 的跨语言互通验证：C++ 服务端消费
# bbt::infra_rpc_net + bbt::infra_http；Python 客户端经 http.client
# 真实 POST /rpc 收发 protobuf body。
# fail-closed：本测试是 wire 契约验收的强制项。infra 未注册
# bbt_infra_rpc_net 目标（例如接入时漏给 BBT_PROTOBUF_PREFIX）时必须在
# configure 阶段非零失败，不允许静默跳过导致验收证据缺口。
if(NOT TARGET bbt_infra_rpc_net)
    message(FATAL_ERROR
        "bbt_infra_rpc_net 目标未注册：framework.rpc_xlang 是强制验收测试，"
        "不允许静默跳过。请为接入的 infra 提供 "
        "-DBBT_PROTOBUF_PREFIX=<protobuf-3.21.12 前缀>（须含 protoc 与 "
        "libprotobuf，详见 infra docs/dependencies/protobuf-3.21.12.provenance.md），"
        "使 bbt_infra_rpc_net 目标被注册后重新配置。")
endif()
find_package(Python3 COMPONENTS Interpreter REQUIRED)
find_package(Boost REQUIRED)
find_package(Threads REQUIRED)

add_executable(rpc_xlang_server rpc_xlang_server.cc)
target_link_libraries(rpc_xlang_server PRIVATE
    bbt::infra_rpc_net bbt::infra_http Boost::boost Threads::Threads)
target_compile_options(rpc_xlang_server PRIVATE -Wall -Wextra)

# Python 客户端零依赖（手写 proto3 wire format），仅需解释器本体；
# 该手写解码同时验证了 wire 字段的字节级布局，不依赖同一 protobuf 库。
add_test(
    NAME framework.rpc_xlang
    COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/rpc_xlang_run.py"
            --server-bin "$<TARGET_FILE:rpc_xlang_server>"
            --client-py "${CMAKE_CURRENT_SOURCE_DIR}/rpc_xlang_client.py")
set_tests_properties(framework.rpc_xlang PROPERTIES
    TIMEOUT 60 LABELS "smoke;integration")

# 下游 request-phase/OutcomeUnknown 消费验收：在真实 framework
# ProtoWireV1 egress 上消费 bbtools-infra 的公开 request-phase/
# OutcomeUnknown 契约（RequestPhase / IsRequestCommitted /
# Error::request_phase）。覆盖 committed→OutcomeUnknown；另覆盖发送前过期、
# 未提交失败、remaining budget 换算与 clamp。不覆盖并发/容量/关闭矩阵。
# 需要 bbt::infra_rpc_net 与 protobuf 前缀——上方已 fail-closed 保证该目标在场。
# 对端是测试自有裸 TCP loopback，不依赖实例。
add_executable(Test_framework_egress_request_phase
    Test_framework_egress_request_phase.cc)
target_link_libraries(Test_framework_egress_request_phase PRIVATE bbt::framework)
target_compile_options(Test_framework_egress_request_phase PRIVATE -Wall -Wextra)
add_test(NAME framework.egress_request_phase
    COMMAND Test_framework_egress_request_phase)
set_tests_properties(framework.egress_request_phase PROPERTIES
    TIMEOUT 60 LABELS "smoke;integration")

# GetValue 业务切片单测（唯一 .proto 真源 → descriptor/方法表
# schema/typed codec golden vectors/handler 分支）。仅在示例目标构建时注册。
if(TARGET getvalue_proto)
    add_executable(Test_framework_getvalue_codec
        Test_framework_getvalue_codec.cc)
    target_link_libraries(Test_framework_getvalue_codec PRIVATE
        bbt::framework getvalue_proto)
    target_include_directories(Test_framework_getvalue_codec PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/../examples/getvalue")
    target_compile_options(Test_framework_getvalue_codec PRIVATE -Wall -Wextra)
    add_test(NAME framework.getvalue_codec
        COMMAND Test_framework_getvalue_codec)
    set_tests_properties(framework.getvalue_codec PROPERTIES
        TIMEOUT 30 LABELS "smoke;unit")
endif()

# 正式 wire 路径上的并发隔离/容量/迟到寿命与关闭矩阵。
# 服务端：公开 CoApp（inbound_bridge=ProtoWireV1）+ 真实 InfraHttpHost 监听
# 127.0.0.1 动态端口；客户端：infra 公开 NetworkRuntime/HttpClient + rpc::RpcWire
# 构造真实 POST /rpc protobuf body，走真实 socket loopback。不使用
# MakeCoAppForTest / DispatchInboundForTest / INetworkHost 桩 / LegacyHeaders。
# 不覆盖跨 hop 预算与跨进程 supervisor 硬停。
if(TARGET getvalue_proto)
    add_executable(Test_framework_rpc_matrix Test_framework_rpc_matrix.cc)
    target_link_libraries(Test_framework_rpc_matrix PRIVATE
        bbt::framework getvalue_proto Boost::boost Threads::Threads)
    target_include_directories(Test_framework_rpc_matrix PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/../examples/getvalue")
    target_compile_options(Test_framework_rpc_matrix PRIVATE -Wall -Wextra)
    add_test(NAME framework.rpc_matrix COMMAND Test_framework_rpc_matrix)
    set_tests_properties(framework.rpc_matrix PROPERTIES
        TIMEOUT 120 LABELS "smoke;integration")
else()
    message(STATUS
        "framework.rpc_matrix: 未构建 getvalue_proto（需 -DNEED_TEST=ON 且 "
        "BBT_PROTOBUF_PREFIX 可用），本配置无正式 wire 矩阵用例")
endif()
