# examples/dual_service — Issue #5：双服务 / 跨进程示例（真实 Redis+Mongo 后端）。
#
# 三个独立进程（framework 默认 HTTP 线桥做真实 client/server）：
#   svc_a  storage（put/get/del/sleep_ms；经 CoApp::add_resource 工厂装配真实
#          CoRedisCli "cache" + CoMongoCli "docs"，无 KvStore 替身）
#   svc_b  gateway（handler 内 this->call 跨进程到 svc_a；static_routes 显式
#          白名单唯一出口）
#   driver 演示客户端（NetworkRuntime + HttpClient，x-bbt-* 线格式在本地重新
#          实现——不引用 framework internal 头）
#
# 本目录只在 BBT_ENABLE_DUAL_SERVICE_EXAMPLE=ON 且根 CMakeLists 的 fail-closed
# 门禁通过时被加入：NEED_TEST、bbt_infra_rpc、bbt::infra_redis、bbt::infra_mongo
# 四项齐备，缺任一条件在 configure 阶段即 FATAL。因此这里的三个 target 不需要
# 再做条件判定，也不会出现「缺依赖时静默少编译一个二进制」。

add_executable(svc_a storage_main.cc)
target_link_libraries(svc_a PRIVATE
    bbt::framework bbt::infra_redis bbt::infra_mongo)
target_compile_options(svc_a PRIVATE -Wall -Wextra)

add_executable(svc_b gateway_main.cc)
target_link_libraries(svc_b PRIVATE bbt::framework)
target_compile_options(svc_b PRIVATE -Wall -Wextra)

add_executable(driver driver_main.cc)
target_link_libraries(driver PRIVATE bbt::framework)
target_compile_options(driver PRIVATE -Wall -Wextra)

# 不在此注册 ctest：三个二进制需要真实 Redis/Mongo 与多进程编排，验收入口是
# examples/dual_service/run_demo.sh（资源验收 job 调用），避免把 live 依赖
# 混进默认测试集造成 skip 冒充通过。
