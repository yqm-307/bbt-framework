# examples/dual_service — Issue #5 阶段 2：两进程静态路由真实 client/server。
#
# 三个独立进程：
#   svc_a  storage 服务（put/get/del/sleep_ms；add_resource 工厂装配真实
#          CoRedisCli "cache" + CoMongoCli "docs" 后端，无 KvStore 替身）
#   svc_b  gateway 服务（handler 内 this->call 跨进程到 svc_a；
#          static_routes 显式白名单唯一出口）
#   driver 演示客户端（NetworkRuntime + HttpClient，x-bbt-* 线格式
#          在本地重新实现——不引用 framework internal 头）
#
# svc_a 消费 G6 已验收的真实 Redis/Mongo Service 资源生命周期：两个后端
# 都是必需 target，缺 bbt::infra_redis / bbt::infra_mongo 时编译期失败
# （不静默降级为替身）。源码只 include bbt/framework 公共面与 bbt/infra、
# bbt/coroutine 公共头，无 internal/、无 Binder/Envelope。

if(NOT TARGET bbt::infra_redis)
    message(FATAL_ERROR
        "svc_a 需要真实 Redis 后端：未构建 bbt::infra_redis "
        "(BBT_HIREDIS_PREFIX)。本示例禁止 KvStore 替身，"
        "请配置 -DBBT_HIREDIS_PREFIX=<hiredis 前缀> 后重试。")
endif()
if(NOT TARGET bbt::infra_mongo)
    message(FATAL_ERROR
        "svc_a 需要真实 Mongo 后端：未构建 bbt::infra_mongo "
        "(BBT_MONGOC_PREFIX+BBT_MONGOCXX_PREFIX)。本示例禁止 KvStore 替身，"
        "请配置 mongo 前缀后重试。")
endif()

add_executable(svc_a dual_service/storage_main.cc)
target_link_libraries(svc_a PRIVATE
    bbt::framework bbt::infra_redis bbt::infra_mongo)
target_compile_options(svc_a PRIVATE -Wall -Wextra)

add_executable(svc_b dual_service/gateway_main.cc)
target_link_libraries(svc_b PRIVATE bbt::framework)
target_compile_options(svc_b PRIVATE -Wall -Wextra)

add_executable(driver dual_service/driver_main.cc)
target_link_libraries(driver PRIVATE bbt::framework)
target_compile_options(driver PRIVATE -Wall -Wextra)
