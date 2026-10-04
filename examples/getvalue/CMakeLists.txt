# Issue #4（P0-A）：GetValue 业务切片。
#
# - 业务 .proto 是唯一 schema 真源；protoc 生成到构建目录（不进仓库）；
# - C++ server 经公开 CoApp/CoService + 正式 body wire bridge 装配；
# - Python 标准库客户端做真实跨进程验收（手写 proto3 wire 编解码，无
#   google.protobuf runtime）。
#
# protoc/runtime 来自 infra 锁定的 3.21.12 前缀（BBT_PROTOBUF_PREFIX）。

set(GV_PROTO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(GV_PROTO "${GV_PROTO_ROOT}/bbt/example/v1/get_value.proto")
set(GV_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${GV_GEN_DIR}")

# protoc/库目录取自 infra 锁定前缀（BBT_PROTOBUF_PREFIX 是 CACHE 变量，
# 跨目录可见）。
set(_gv_protoc "${BBT_PROTOBUF_PREFIX}/usr/bin/protoc")
if(NOT EXISTS "${_gv_protoc}")
    set(_gv_protoc "${BBT_PROTOBUF_PREFIX}/bin/protoc")
endif()
set(_gv_protoc_libdir "")
foreach(_cand usr/lib/x86_64-linux-gnu usr/lib/aarch64-linux-gnu lib)
    if(EXISTS "${BBT_PROTOBUF_PREFIX}/${_cand}/libprotobuf.a" OR
       EXISTS "${BBT_PROTOBUF_PREFIX}/${_cand}/libprotobuf.so")
        set(_gv_protoc_libdir "${BBT_PROTOBUF_PREFIX}/${_cand}")
    endif()
endforeach()
if(NOT _gv_protoc_libdir)
    message(FATAL_ERROR "getvalue: 无法在前缀内定位 protobuf 库目录")
endif()

add_custom_command(
    OUTPUT "${GV_GEN_DIR}/bbt/example/v1/get_value.pb.cc"
           "${GV_GEN_DIR}/bbt/example/v1/get_value.pb.h"
    COMMAND "${CMAKE_COMMAND}" -E env
            "LD_LIBRARY_PATH=${_gv_protoc_libdir}"
            "${_gv_protoc}"
            --proto_path "${GV_PROTO_ROOT}"
            --cpp_out "${GV_GEN_DIR}"
            "${GV_PROTO}"
    DEPENDS "${GV_PROTO}" "${_gv_protoc}"
    COMMENT "protoc generate get_value.pb.{h,cc} (build dir only)"
    VERBATIM)

add_library(getvalue_proto STATIC
    "${GV_GEN_DIR}/bbt/example/v1/get_value.pb.cc")
target_include_directories(getvalue_proto PUBLIC "${GV_GEN_DIR}")
# bbt_infra_rpc_proto 公共面提供生成物目录 + protobuf include 与 libprotobuf。
target_link_libraries(getvalue_proto PUBLIC bbt_infra_rpc_proto)
target_compile_features(getvalue_proto PUBLIC cxx_std_17)

add_executable(getvalue_server getvalue_server.cc)
target_link_libraries(getvalue_server PRIVATE
    bbt::framework getvalue_proto Boost::boost Threads::Threads)
target_compile_options(getvalue_server PRIVATE -Wall -Wextra)

# 跨进程验收测试（ctest：examples.getvalue.xlang）。
find_package(Python3 COMPONENTS Interpreter REQUIRED)
find_package(Boost REQUIRED)
find_package(Threads REQUIRED)
add_test(
    NAME examples.getvalue.xlang
    COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/getvalue_run.py"
            --server-bin "$<TARGET_FILE:getvalue_server>"
            --client-py "${CMAKE_CURRENT_SOURCE_DIR}/getvalue_client.py"
            --proto "${GV_PROTO}"
            --proto-root "${GV_PROTO_ROOT}"
            --protoc "${_gv_protoc}"
            --protoc-libdir "${_gv_protoc_libdir}")
set_tests_properties(examples.getvalue.xlang PROPERTIES TIMEOUT 120)

# Issue #4（P0-A）：正式 Service→Service 出站 caller 进程。宿主
# GetValueCallerService，其 handler 经公开 typed service.call<Req,Resp> 向另一
# framework CoApp（callee）发起真实 body 出站。与 server 同装配口径。
add_executable(getvalue_caller getvalue_caller.cc)
target_link_libraries(getvalue_caller PRIVATE
    bbt::framework getvalue_proto Boost::boost Threads::Threads)
target_compile_options(getvalue_caller PRIVATE -Wall -Wextra)

# 跨进程验收测试（ctest：examples.getvalue.s2s）：callee + caller 两个真实
# framework CoApp 进程 + 真实黑障端点；驱动器断言出站 typed 调用与预算/无路由/
# 传输断开的 callee-side 零 I/O 观察。
add_test(
    NAME examples.getvalue.s2s
    COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/getvalue_s2s_run.py"
            --callee-bin "$<TARGET_FILE:getvalue_server>"
            --caller-bin "$<TARGET_FILE:getvalue_caller>"
            --workdir "${CMAKE_CURRENT_BINARY_DIR}/s2s-work")
set_tests_properties(examples.getvalue.s2s PROPERTIES TIMEOUT 120)
