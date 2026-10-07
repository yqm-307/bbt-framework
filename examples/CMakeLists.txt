# Issue #4（P0-A）：业务切片示例（唯一业务 .proto + C++ server + Python client）。
# 仅当 bbt_infra_rpc 可用时被父级加入（见根 CMakeLists）。
add_subdirectory(getvalue)

# Issue #5：双服务资源示例（svc_a/svc_b/driver）。只在显式
# BBT_ENABLE_DUAL_SERVICE_EXAMPLE=ON 时加入——默认 OFF 时本仓不引入
# hiredis/mongo-cxx-driver。四项前置条件（NEED_TEST、bbt_infra_rpc、
# bbt::infra_redis、bbt::infra_mongo）与 fail-closed 门禁都在根 CMakeLists
# 判定（那里能看到全部条件），此处不重复判定、不做降级跳过。
if(BBT_ENABLE_DUAL_SERVICE_EXAMPLE)
    add_subdirectory(dual_service)
endif()
