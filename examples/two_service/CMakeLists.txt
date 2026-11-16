# examples/two_service — 双进程 Service→Service fixture。
#
# 两个互异 identity 的公开 CoApp 进程：caller 宿主 getvalue 切片的
# GetValueCallerService，其 handler 经受保护 CoServiceBase::call<Req,Resp>
# 调 callee 进程宿主的 GetValueService；唯一静态路由指向 callee 的真实
# loopback endpoint，调用经真实 V2 会话（self-connection 被拒，故必须两个进程）。
# 业务 schema/codec 复用 examples/getvalue 的 getvalue_proto 与 ProtoCodec 特化，
# 不另造第二套。
#
# 本目录只在 examples/ 入口被加入时构建（根 CMakeLists：NEED_TEST=ON 且
# bbt_infra_rpc_net 已注册），与 examples/getvalue 同一前置，无额外依赖。

# 复用 examples/getvalue 的业务服务头（公开面；与 tests/Test_framework_rpc_matrix.cc
# 同一 include 口径）。
# find_package 的作用域按目录，不跨兄弟目录传播，故这里各自声明。
find_package(Boost REQUIRED)
find_package(Threads REQUIRED)
find_package(Python3 COMPONENTS Interpreter REQUIRED)

add_executable(two_service two_service_main.cc)
target_include_directories(two_service PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../getvalue")
target_link_libraries(two_service PRIVATE
    bbt::framework getvalue_proto Boost::boost Threads::Threads)
target_compile_options(two_service PRIVATE -Wall -Wextra)

# 双进程 fixture 的真实运行验收：两个自有进程 + 动态 loopback 端口 + getvalue 切片的
# Python 标准库 wire codec。断言正常/业务错误/有限 deadline/未配置路由/关闭与组件
# OnStop 同步收口。
find_package(Python3 COMPONENTS Interpreter REQUIRED)
add_test(
    NAME examples.two_service
    COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/two_service_run.py"
            --bin "$<TARGET_FILE:two_service>"
            --results "${CMAKE_CURRENT_BINARY_DIR}/two_service-results.json")
set_tests_properties(examples.two_service PROPERTIES TIMEOUT 120 LABELS "integration")
