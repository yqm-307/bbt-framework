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
# InvalidContext 端到端、WaitClosed 桥接、跨 worker 不串扰。
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
# shutdown budget 下的 RequestClose/WaitClosed 收束（INetworkHost 桩驱动）。
add_executable(Test_framework_resource Test_framework_resource.cc)
target_link_libraries(Test_framework_resource PRIVATE bbt::framework)
target_compile_options(Test_framework_resource PRIVATE -Wall -Wextra)
add_test(NAME Test_framework_resource COMMAND Test_framework_resource)
set_tests_properties(Test_framework_resource PROPERTIES TIMEOUT 30)

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
