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
