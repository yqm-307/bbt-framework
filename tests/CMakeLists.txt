# Boost.Test 静态内嵌写法（同 bbtools-coroutine 单测约定）：
# BOOST_TEST_MAIN + boost/test/included/unit_test.hpp，
# 不得再链接 libboost_unit_test_framework.so——共享库与二进制各持一份
# 框架全局状态，ELF 符号插入会在 __cxa_finalize 阶段 double-free。
add_executable(Test_framework_f0 Test_framework_f0.cc)
target_link_libraries(Test_framework_f0 PRIVATE bbt::framework)
target_compile_options(Test_framework_f0 PRIVATE -Wall -Wextra)
add_test(NAME Test_framework_f0 COMMAND Test_framework_f0)
set_tests_properties(Test_framework_f0 PROPERTIES TIMEOUT 15)

# F2-a：真实协程调度 + 屏障构造并发窗口；留给 worker 竞争与端口无关。
add_executable(Test_framework_f2a Test_framework_f2a.cc)
target_link_libraries(Test_framework_f2a PRIVATE bbt::framework)
target_compile_options(Test_framework_f2a PRIVATE -Wall -Wextra)
add_test(NAME Test_framework_f2a COMMAND Test_framework_f2a)
set_tests_properties(Test_framework_f2a PROPERTIES TIMEOUT 30)

add_executable(Test_framework_f2b1 Test_framework_f2b1.cc)
target_link_libraries(Test_framework_f2b1 PRIVATE bbt::framework)
target_compile_options(Test_framework_f2b1 PRIVATE -Wall -Wextra)
add_test(NAME Test_framework_f2b1 COMMAND Test_framework_f2b1)
set_tests_properties(Test_framework_f2b1 PROPERTIES TIMEOUT 30)

# F2-b2：有序入口接收侧——授权流校验、每流序号状态机与去重终态缓存；
# 缺口/在途/冲突/淘汰/epoch 切换/无票据均由组件语义核直接断言，
# 不依赖入站接线（接线属 F2-b3）。
add_executable(Test_framework_f2b2 Test_framework_f2b2.cc)
target_link_libraries(Test_framework_f2b2 PRIVATE bbt::framework)
target_compile_options(Test_framework_f2b2 PRIVATE -Wall -Wextra)
add_test(NAME Test_framework_f2b2 COMMAND Test_framework_f2b2)
set_tests_properties(Test_framework_f2b2 PROPERTIES TIMEOUT 30)

add_executable(Test_framework_f2b3 Test_framework_f2b3.cc)
target_link_libraries(Test_framework_f2b3 PRIVATE bbt::framework)
target_compile_options(Test_framework_f2b3 PRIVATE -Wall -Wextra)
add_test(NAME Test_framework_f2b3 COMMAND Test_framework_f2b3)
set_tests_properties(Test_framework_f2b3 PROPERTIES TIMEOUT 60)

# F1-a：deadline/cancel 继承与受管请求上下文；真实协程调度器 + 注入点断言。
add_executable(Test_framework_f1a Test_framework_f1a.cc)
target_link_libraries(Test_framework_f1a PRIVATE bbt::framework)
target_compile_options(Test_framework_f1a PRIVATE -Wall -Wextra)
add_test(NAME Test_framework_f1a COMMAND Test_framework_f1a)
set_tests_properties(Test_framework_f1a PROPERTIES TIMEOUT 30)

# F1-b1：CoApp 装配/校验 + INetworkHost 桩驱动的生命周期状态机断言。
add_executable(Test_framework_f1b1 Test_framework_f1b1.cc)
target_link_libraries(Test_framework_f1b1 PRIVATE bbt::framework)
target_compile_options(Test_framework_f1b1 PRIVATE -Wall -Wextra)
add_test(NAME Test_framework_f1b1 COMMAND Test_framework_f1b1)
set_tests_properties(Test_framework_f1b1 PROPERTIES TIMEOUT 30)

# F1-b2：真实 HTTP loopback 入站→上下文落地→分发→回复；
# InvalidContext 端到端、同步 Close、跨 worker 不串扰。
add_executable(Test_framework_f1b2 Test_framework_f1b2.cc)
target_link_libraries(Test_framework_f1b2 PRIVATE bbt::framework)
target_compile_options(Test_framework_f1b2 PRIVATE -Wall -Wextra)
add_test(NAME Test_framework_f1b2 COMMAND Test_framework_f1b2)
set_tests_properties(Test_framework_f1b2 PROPERTIES TIMEOUT 60)

# F3：资源管理与关闭——外部线程 request_shutdown、关闭期间在途 handler
# 真实出站、ShutdownIncomplete 可观察与迟到收尾、逻辑结束≠物理清理。
add_executable(Test_framework_f3 Test_framework_f3.cc)
target_link_libraries(Test_framework_f3 PRIVATE bbt::framework)
target_compile_options(Test_framework_f3 PRIVATE -Wall -Wextra)
add_test(NAME Test_framework_f3 COMMAND Test_framework_f3)
# 各用例的真实时长由 infra keep-alive 连接的 runtime 排空决定
# （单用例实测 ~30s，全套 ~107s），60s 限时必然误杀，按实测×1.5 留裕量。
set_tests_properties(Test_framework_f3 PROPERTIES TIMEOUT 180)

add_executable(Test_framework_rpc Test_framework_rpc.cc)
target_link_libraries(Test_framework_rpc PRIVATE bbt::framework)
target_compile_options(Test_framework_rpc PRIVATE -Wall -Wextra)
add_test(NAME Test_framework_rpc COMMAND Test_framework_rpc)
set_tests_properties(Test_framework_rpc PROPERTIES TIMEOUT 15)

# issue #8：资源生命周期——工厂装配、Create/Start 相位、失败回滚与
# 关闭序列下的同步 Close 收束（INetworkHost 桩驱动）。
add_executable(Test_framework_resource Test_framework_resource.cc)
target_link_libraries(Test_framework_resource PRIVATE bbt::framework)
target_compile_options(Test_framework_resource PRIVATE -Wall -Wextra)
add_test(NAME Test_framework_resource COMMAND Test_framework_resource)
set_tests_properties(Test_framework_resource PROPERTIES TIMEOUT 30)

# issue #8 强制 live 验收开关（默认 OFF）：live runner/CI 显式
# -DBBT_REQUIRE_RESOURCE_LIVE_TESTS=ON 时，Redis/Mongo 任一 infra 模块未注册即
# configure 阶段 FATAL——不允许静默缺模块导致验收证据缺口（与 framework.rpc_xlang
# 对 bbt_infra_rpc 的 fail-closed 同一口径）。OFF 时维持可选 target：缺模块走
# 下面的 STATUS 提示，环境缺后端时进程 77 退出配合 SKIP_RETURN_CODE 标 Skipped，
# 均不宣称 live 通过。
option(BBT_REQUIRE_RESOURCE_LIVE_TESTS
    "issue #8: 强制真实 Redis+Mongo live 验收（缺模块即 configure 失败）" OFF)
if(BBT_REQUIRE_RESOURCE_LIVE_TESTS)
    if(NOT TARGET bbt::infra_redis OR NOT TARGET bbt::infra_mongo)
        message(FATAL_ERROR
            "BBT_REQUIRE_RESOURCE_LIVE_TESTS=ON 但 bbt::infra_redis/"
            "bbt::infra_mongo 未全部注册：issue #8 live 验收为强制项，不允许"
            "静默缺模块。请提供 -DBBT_HIREDIS_PREFIX=<hiredis 前缀> 与 "
            "-DBBT_MONGOC_PREFIX=/-DBBT_MONGOCXX_PREFIX=<mongoc/mongocxx 前缀>"
            " 使两个目标注册后重新配置。")
    endif()
endif()

# issue #8 live 验收：真实 Redis/MongoDB 容器经工厂装配 + service 资源缝
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
    add_test(NAME framework.resource.live.redis
        COMMAND Test_framework_resource_live_redis)
    set_tests_properties(framework.resource.live.redis PROPERTIES
        TIMEOUT 120
        SKIP_RETURN_CODE 77)
else()
    message(STATUS
        "framework.resource.live.redis: 未构建 bbt::infra_redis "
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
    add_test(NAME framework.resource.live.mongo
        COMMAND Test_framework_resource_live_mongo)
    set_tests_properties(framework.resource.live.mongo PROPERTIES
        TIMEOUT 120
        SKIP_RETURN_CODE 77)
else()
    message(STATUS
        "framework.resource.live.mongo: 未构建 bbt::infra_mongo "
        "(BBT_MONGOC_PREFIX+BBT_MONGOCXX_PREFIX)，本配置无 Mongo live 门禁")
endif()

# issue #8 F8-T3 联合验收：真实 HTTP loopback → 框架入站 → 业务 Service →
# 真实 Redis + Mongo 联合消费。要求两个 infra 资源模块都在场（任一缺失
# 即不注册本 target——单资源 live 不能替代联合验收），缺环境时进程 77
# 退出配合 SKIP_RETURN_CODE 标 Skipped。
if(TARGET bbt::infra_redis AND TARGET bbt::infra_mongo)
    # 业务 Service 在独立 TU framework_resource_service.cc 中实现（只 include
    # framework/infra/coroutine 公开头），与测试宿主分属不同编译单元——这是
    # 「干净消费者 Service 只公共头即可按名称使用资源」的真实编译证据。
    add_executable(Test_framework_resource_combined
        Test_framework_resource_combined.cc
        framework_resource_service.cc)
    target_link_libraries(Test_framework_resource_combined PRIVATE
        bbt::framework bbt::infra_redis bbt::infra_mongo)
    target_compile_options(Test_framework_resource_combined PRIVATE
        -Wall -Wextra)
    target_compile_definitions(Test_framework_resource_combined PRIVATE
        BBT_LIVE_HAS_REDIS=1 BBT_LIVE_HAS_MONGO=1)
    add_test(NAME framework.resource.live.combined
        COMMAND Test_framework_resource_combined)
    set_tests_properties(framework.resource.live.combined PROPERTIES
        TIMEOUT 180
        SKIP_RETURN_CODE 77)
else()
    message(STATUS
        "framework.resource.live.combined: 需要 bbt::infra_redis 与 "
        "bbt::infra_mongo 同时在场，本配置无联合 live 门禁")
endif()

# framework #4：G5 RpcEnvelope wire profile 的跨语言互通验证。
# C++ 服务端消费 bbt::infra_rpc + bbt::infra_http；Python 客户端经
# http.client 真实 POST /rpc 收发 protobuf body。
# fail-closed：本测试是 issue #4 验收的强制项。infra 未注册
# bbt_infra_rpc 目标（例如接入时漏给 BBT_PROTOBUF_PREFIX）时必须在
# configure 阶段非零失败，不允许静默跳过导致验收证据缺口。
if(NOT TARGET bbt_infra_rpc)
    message(FATAL_ERROR
        "bbt_infra_rpc 目标未注册：framework.rpc_xlang 是强制验收测试，"
        "不允许静默跳过。请为接入的 infra 提供 "
        "-DBBT_PROTOBUF_PREFIX=<protobuf-3.21.12 前缀>（须含 protoc 与 "
        "libprotobuf，详见 infra docs/dependencies/protobuf-3.21.12.provenance.md），"
        "使 bbt_infra_rpc 目标被注册后重新配置。")
endif()
find_package(Python3 COMPONENTS Interpreter REQUIRED)
find_package(Boost REQUIRED)
find_package(Threads REQUIRED)

add_executable(rpc_xlang_server rpc_xlang_server.cc)
target_link_libraries(rpc_xlang_server PRIVATE
    bbt::infra_rpc bbt::infra_http Boost::boost Threads::Threads)
target_compile_options(rpc_xlang_server PRIVATE -Wall -Wextra)

# Python 客户端零依赖（手写 proto3 wire format），仅需解释器本体；
# 该手写解码同时验证了 wire 字段的字节级布局，不依赖同一 protobuf 库。
add_test(
    NAME framework.rpc_xlang
    COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/rpc_xlang_run.py"
            --server-bin "$<TARGET_FILE:rpc_xlang_server>"
            --client-py "${CMAKE_CURRENT_SOURCE_DIR}/rpc_xlang_client.py")
set_tests_properties(framework.rpc_xlang PROPERTIES TIMEOUT 60)

# framework #4 下游 request-phase/OutcomeUnknown 消费验收：在真实 framework
# ProtoWireV1 egress 上消费 bbtools-infra 已合入的公开 request-phase/
# OutcomeUnknown 契约（RequestPhase / IsRequestCommitted / Error::request_phase）。
# 覆盖：R5 committed→OutcomeUnknown；另覆盖发送前过期、未提交失败、remaining
# budget 换算与 clamp。不覆盖 R6/R7/R8 并发/容量/关闭矩阵。
# 需要 bbt::infra_rpc 与 protobuf 前缀——上方已 fail-closed 保证该目标在场。
# 对端是测试自有裸 TCP loopback，不依赖实例。
add_executable(Test_framework_egress_phase Test_framework_egress_phase.cc)
target_link_libraries(Test_framework_egress_phase PRIVATE bbt::framework)
target_compile_options(Test_framework_egress_phase PRIVATE -Wall -Wextra)
add_test(NAME framework.egress.phase COMMAND Test_framework_egress_phase)
set_tests_properties(framework.egress.phase PROPERTIES TIMEOUT 60)

# framework #4 P0-A：GetValue 业务切片单测（唯一 .proto 真源 → descriptor/方法表
# schema/typed codec golden vectors/handler 分支）。仅在示例目标构建时注册。
if(TARGET getvalue_proto)
    add_executable(Test_framework_getvalue_codec
        Test_framework_getvalue_codec.cc)
    target_link_libraries(Test_framework_getvalue_codec PRIVATE
        bbt::framework getvalue_proto)
    target_include_directories(Test_framework_getvalue_codec PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/../examples/getvalue")
    target_compile_options(Test_framework_getvalue_codec PRIVATE -Wall -Wextra)
    add_test(NAME framework.getvalue.codec
        COMMAND Test_framework_getvalue_codec)
    set_tests_properties(framework.getvalue.codec PROPERTIES TIMEOUT 30)
endif()

# framework #4 RPC-T4（修偏轮）：并发隔离/容量/迟到寿命与关闭矩阵（R6/R7/R8）。
# 服务端：公开 CoApp（inbound_bridge=ProtoWireV1）+ 真实 InfraHttpHost 监听
# 127.0.0.1 动态端口；客户端：infra 公开 NetworkRuntime/HttpClient + rpc::RpcWire
# 构造真实 POST /rpc protobuf body，走真实 socket loopback。不使用
# MakeCoAppForTest / DispatchInboundForTest / INetworkHost 桩 / LegacyHeaders。
# 不覆盖跨 hop 预算（R4/S4）与跨进程 supervisor 硬停（R8/S8）。
if(TARGET getvalue_proto)
    add_executable(Test_framework_matrix Test_framework_matrix.cc)
    target_link_libraries(Test_framework_matrix PRIVATE
        bbt::framework getvalue_proto Boost::boost Threads::Threads)
    target_include_directories(Test_framework_matrix PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/../examples/getvalue")
    target_compile_options(Test_framework_matrix PRIVATE -Wall -Wextra)
    add_test(NAME framework.matrix COMMAND Test_framework_matrix)
    set_tests_properties(framework.matrix PROPERTIES TIMEOUT 120)
else()
    message(STATUS
        "framework.matrix: 未构建 getvalue_proto（需 -DNEED_TEST=ON 且 "
        "BBT_PROTOBUF_PREFIX 可用），本配置无 RPC-T4 正式 wire 矩阵用例")
endif()
