# examples/lifecycle_matrix — Issue #5（EX-T4）：容量 / 并发上下文 / 关闭生命周期验收。
#
# 一个自有 fixture 进程：公开 CoApp + 真实 infra HTTP loopback 宿主 + 正式
# ProtoWireV1 body profile，注册公开 CoService<T>（lifecycle::LifecycleGateService，
# 复用 examples/getvalue 的 getvalue_proto 与 ProtoCodec 特化——#5 不另造第二套
# schema/codec）。外部 Python 驱动器用 #4 的手写 proto3 wire codec（getvalue_client.py）
# 打真实 POST /rpc，覆盖并发挂起/上下文不串/容量 Overloaded/字节上限/drain/
# ShutdownIncomplete/外部硬杀 journal。
#
# 本目录只在 examples/ 入口被加入时构建（根 CMakeLists：NEED_TEST=ON 且
# bbt_infra_rpc 已注册），与 examples/getvalue、examples/two_service 同一前置，
# 无额外依赖（无 Redis/Mongo/Compose/认证）。

# find_package 作用域按目录，不跨兄弟目录传播，故各自声明。
find_package(Boost REQUIRED)
find_package(Threads REQUIRED)
find_package(Python3 COMPONENTS Interpreter REQUIRED)

add_executable(lifecycle_fixture lifecycle_main.cc)
target_include_directories(lifecycle_fixture PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../getvalue")   # 复用 #4 公开业务头/codec
target_link_libraries(lifecycle_fixture PRIVATE
    bbt::framework getvalue_proto Boost::boost Threads::Threads)
target_compile_options(lifecycle_fixture PRIVATE -Wall -Wextra)

# 真实运行验收：驱动器 spawn 自有 fixture 进程（动态 loopback 端口）、用 #4
# wire codec 打真实请求、读 journal 断言、异常路径回收自有进程。
add_test(
    NAME examples.lifecycle_matrix
    COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/lifecycle_run.py"
            --bin "$<TARGET_FILE:lifecycle_fixture>"
            --results "${CMAKE_CURRENT_BINARY_DIR}/lifecycle-results.json")
set_tests_properties(examples.lifecycle_matrix PROPERTIES TIMEOUT 240)
