# examples/rpc_policy — R8（3-5）：公开出站策略 fixture。
#
# 一个公开 CoApp（caller+callee 两服务 + 自环静态路由）经正式 ProtoWireV1 body
# wire 真实 socket 回环；caller handler 出站经公开策略 CoAppOptions::rpc_policy。
# 只用公开面（bbt/framework/CoApp.hpp + RpcPolicy.hpp + getvalue 业务），不触
# internal seam。本目录只在 examples/ 入口被加入时构建（根 CMakeLists：
# NEED_TEST=ON 且 bbt_infra_rpc 已注册），与 examples/getvalue 同一前置。
#
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

# 真实运行验收：自有进程 + 动态 loopback 端口 + #4 的 Python 标准库 wire codec；
# 每场景独立进程（一 CoApp 一策略），断言 Allow/Reject/期限只缩不放/收紧过期/
# 路由先于策略/装饰器两虚调用/关闭后存活期 listener 探针 refused（run 返回后、
# 进程退出前 fixture 自发探针；只证存活期 listener 已关，不证零额外 I/O）。
add_test(
    NAME examples.rpc_policy
    COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/rpc_policy_run.py"
            --bin "$<TARGET_FILE:rpc_policy>"
            --results "${CMAKE_CURRENT_BINARY_DIR}/rpc_policy-results.json")
set_tests_properties(examples.rpc_policy PROPERTIES TIMEOUT 180)
