# examples/ecommerce_cluster — 真实 Consul 注册中心 + 多进程动态发现的电商集群。
#
# 前置（父级 examples/ 入口已保证 NEED_TEST=ON 且 bbt_infra_rpc_net 在场）：
#   - bbt::infra_consul（infra 的 Consul adapter，需 BBT_INFRA_CONSUL=ON）；
#   - protobuf 3.21.12 前缀（BBT_PROTOBUF_PREFIX，protoc 生成业务 schema）；
#   - 一个可执行的 consul 二进制（仅 --consul/CTest 时传入，不做系统安装）。
#
# 业务 .proto 是唯一 schema 真源；生成物只落构建目录。

find_package(Boost REQUIRED)
find_package(Threads REQUIRED)
find_package(Python3 COMPONENTS Interpreter REQUIRED)

set(EC_PROTO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(EC_PROTO "${EC_PROTO_ROOT}/bbt/example/ecommerce/v1/ecommerce.proto")
set(EC_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${EC_GEN_DIR}")

set(_ec_protoc "${BBT_PROTOBUF_PREFIX}/usr/bin/protoc")
if(NOT EXISTS "${_ec_protoc}")
    set(_ec_protoc "${BBT_PROTOBUF_PREFIX}/bin/protoc")
endif()
set(_ec_protoc_libdir "")
foreach(_cand usr/lib/x86_64-linux-gnu usr/lib/aarch64-linux-gnu lib)
    if(EXISTS "${BBT_PROTOBUF_PREFIX}/${_cand}/libprotobuf.a" OR
       EXISTS "${BBT_PROTOBUF_PREFIX}/${_cand}/libprotobuf.so")
        set(_ec_protoc_libdir "${BBT_PROTOBUF_PREFIX}/${_cand}")
    endif()
endforeach()
if(NOT _ec_protoc_libdir)
    message(FATAL_ERROR "ecommerce_cluster: 无法在前缀内定位 protobuf 库目录")
endif()

add_custom_command(
    OUTPUT "${EC_GEN_DIR}/bbt/example/ecommerce/v1/ecommerce.pb.cc"
           "${EC_GEN_DIR}/bbt/example/ecommerce/v1/ecommerce.pb.h"
    COMMAND "${CMAKE_COMMAND}" -E env
            "LD_LIBRARY_PATH=${_ec_protoc_libdir}"
            "${_ec_protoc}"
            --proto_path "${EC_PROTO_ROOT}"
            --cpp_out "${EC_GEN_DIR}"
            "${EC_PROTO}"
    DEPENDS "${EC_PROTO}" "${_ec_protoc}"
    COMMENT "protoc generate ecommerce.pb.{h,cc} (build dir only)"
    VERBATIM)

add_library(ecommerce_cluster_proto STATIC
    "${EC_GEN_DIR}/bbt/example/ecommerce/v1/ecommerce.pb.cc")
target_include_directories(ecommerce_cluster_proto PUBLIC "${EC_GEN_DIR}")
target_link_libraries(ecommerce_cluster_proto PUBLIC bbt_infra_rpc_proto)
target_compile_features(ecommerce_cluster_proto PUBLIC cxx_std_17)

# 一个二进制四个角色（gateway/order/catalog/settlement），按 argv 选择装配；服务实现按
# services/<角色>/ 分文件，main 只做配置与装配。
add_executable(ecommerce_cluster
    ecommerce_cluster_main.cc
    discovery.cc
    services/common/hop_log.cc
    services/common/node_identity.cc
    services/common/storage.cc
    services/settlement/settlement_service.cc
    services/catalog/catalog_service.cc
    services/order/order_service.cc
    services/gateway/gateway_service.cc)
target_include_directories(ecommerce_cluster PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(ecommerce_cluster PRIVATE
    bbt::framework
    ecommerce_cluster_proto
    bbt::infra_consul
    Boost::boost
    Threads::Threads)
target_compile_options(ecommerce_cluster PRIVATE -Wall -Wextra)
# 直接消费已有资源目标；未构建资源时 --storage 显式失败，原报价模式仍可用。
if(TARGET bbt::infra_redis_net AND TARGET bbt::infra_mongo)
    target_link_libraries(ecommerce_cluster PRIVATE bbt::infra_redis_net bbt::infra_mongo)
    target_compile_definitions(ecommerce_cluster PRIVATE EC_STORAGE_AVAILABLE=1)
endif()

# CTest 只在显式给出 consul 可执行文件时注册（与 Redis/Mongo live 门禁同一口径：
# 没有后端就不假装有 live 门禁）。驱动器仍可手工直接跑，见 README。
set(BBT_CONSUL_BINARY "" CACHE FILEPATH
    "consul 可执行文件路径（examples.ecommerce_cluster 的 CTest 与手工验收用）")
if(BBT_CONSUL_BINARY AND EXISTS "${BBT_CONSUL_BINARY}")
    add_test(
        NAME examples.ecommerce_cluster
        COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/run.py"
                --binary "$<TARGET_FILE:ecommerce_cluster>"
                --consul "${BBT_CONSUL_BINARY}"
                --output "${CMAKE_CURRENT_BINARY_DIR}/cluster-run/result.json"
                --workdir "${CMAKE_CURRENT_BINARY_DIR}/cluster-run")
    set_tests_properties(examples.ecommerce_cluster PROPERTIES
        TIMEOUT 180 LABELS "integration")
else()
    message(STATUS
        "ecommerce_cluster: 未设置 -DBBT_CONSUL_BINARY=<consul>，"
        "不注册 examples.ecommerce_cluster（不静默降级为无后端用例）")
endif()
