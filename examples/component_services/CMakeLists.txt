# examples/component_services — 组件式服务：共享组件、热拔插、hook、observer。
#
# 只依赖 bbt::framework（v2 对象面）与标准库/线程；不需要 RPC/protobuf、
# Redis/Mongo 或任何外部服务。fixture 是普通 main + 受管协程，Python 标准库
# 驱动器并发跑独立 node 进程并断言 journal，`examples.component_services`
# 注册为 integration 测试。

find_package(Threads REQUIRED)
find_package(Python3 COMPONENTS Interpreter REQUIRED)

add_executable(component_services component_services_main.cc)
target_link_libraries(component_services PRIVATE
    bbt::framework Threads::Threads)
target_compile_options(component_services PRIVATE -Wall -Wextra)

add_test(
    NAME examples.component_services
    COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/component_services_run.py"
            --bin "$<TARGET_FILE:component_services>"
            --results "${CMAKE_CURRENT_BINARY_DIR}/component_services-results.json")
set_tests_properties(examples.component_services PROPERTIES
    TIMEOUT 180 LABELS "integration")
