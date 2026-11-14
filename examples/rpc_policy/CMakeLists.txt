# examples/rpc_policy — 公开出站策略 fixture。
#
# caller/callee 为互异 identity 的公开 CoApp 进程；AppOptions::SetRpcPolicy
# 接入既有 Preflight，驱动实际断言六种策略，不使用 internal seam。
# 本目录与 getvalue 共用 NEED_TEST 与 RPC 目标前置条件。

# find_package 作用域按目录，不跨兄弟目录传播，故这里各自声明。
find_package(Boost REQUIRED)
find_package(Threads REQUIRED)
find_package(Python3 COMPONENTS Interpreter REQUIRED)

add_executable(rpc_policy rpc_policy_main.cc)
target_include_directories(rpc_policy PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../getvalue")
target_link_libraries(rpc_policy PRIVATE
    bbt::framework getvalue_proto Boost::boost Threads::Threads)
target_compile_options(rpc_policy PRIVATE -Wall -Wextra)

# 真实运行验收：自有进程 + 动态 loopback 端口 + getvalue 切片的 Python 标准库 wire codec；
# 每场景独立进程（一 CoApp 一策略），断言 Allow/Reject/期限只缩不放/收紧过期/
# 路由先于策略/装饰器两虚调用/关闭后存活期 listener 探针 refused（run 返回后、
# 进程退出前 fixture 自发探针；只证存活期 listener 已关，不证零额外 I/O）。
add_test(
    NAME examples.rpc_policy
    COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/rpc_policy_run.py"
            --bin "$<TARGET_FILE:rpc_policy>"
            --results "${CMAKE_CURRENT_BINARY_DIR}/rpc_policy-results.json")
set_tests_properties(examples.rpc_policy PROPERTIES TIMEOUT 180 LABELS "integration")
