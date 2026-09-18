cmake_minimum_required(VERSION 3.16)
project(bbt-framework LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(NEED_TEST "编译本仓 unit test 文件" OFF)

# 框架库目标：F0 为 header-only INTERFACE；F2-a 起含编译产物，
# 目标在 framework/src/CMakeLists.txt 定义（需先接入 bbt::infra_common，
# 见下方依赖块后的 add_subdirectory）。

# --- bbtools-infra / bbtools-coroutine 依赖接入 -------------------------------
# 形态沿用 bbtools-infra 顶层 CMakeLists。固定 SHA 的正式接入待上游可读后收敛；
# 当前仅允许本机/候选分支经显式路径覆盖接入。禁止把本机私有绝对路径写进默认值
# 或任何提交内容。
set(BBT_INFRA_SOURCE_DIR "" CACHE PATH
    "本机/候选分支开发用：bbtools-infra 源码树路径（含顶层 CMakeLists.txt 与 include/）")
set(BBT_COROUTINE_SOURCE_DIR "" CACHE PATH
    "本机/候选分支开发用：bbtools-coroutine 源码树路径（infra 以源码树接入时由其消费）")

if(BBT_INFRA_SOURCE_DIR)
    if(NOT EXISTS "${BBT_INFRA_SOURCE_DIR}/CMakeLists.txt")
        message(FATAL_ERROR
            "BBT_INFRA_SOURCE_DIR='${BBT_INFRA_SOURCE_DIR}' 无效："
            "未找到顶层 CMakeLists.txt")
    endif()
    # coroutine/infra 与本仓共用 NEED_TEST 开关名，infra 顶层用 BUILD_TESTING：
    # 在目录作用域局部置 OFF，避免本仓测试开关连带拉起上游测试目标。
    # 只遮蔽本作用域变量、不动 CACHE：infra 接入 coroutine 时会临时改 NEED_TEST
    # 的 CACHE 并按 CACHE 原值还原（见 bbtools-infra 顶层 CMakeLists），
    # 因此调用方传入的 -DNEED_TEST=ON 不会被改掉。
    if(DEFINED CACHE{NEED_TEST})
        set(_bbt_fw_need_test "$CACHE{NEED_TEST}")
    else()
        set(_bbt_fw_need_test "${NEED_TEST}")
    endif()
    set(NEED_TEST OFF)
    set(BUILD_TESTING OFF)
    add_subdirectory("${BBT_INFRA_SOURCE_DIR}"
                     "${CMAKE_BINARY_DIR}/_deps/bbtools-infra"
                     EXCLUDE_FROM_ALL)
    set(NEED_TEST "${_bbt_fw_need_test}")
    unset(_bbt_fw_need_test)
    unset(BUILD_TESTING)
else()
    # 回退：查找已安装的 bbt_infra 包（需导出 bbt::infra_common 目标）。
    find_package(bbt_infra CONFIG QUIET)
    if(NOT TARGET bbt::infra_common)
        message(FATAL_ERROR
            "未找到 bbt::infra_common。本机/候选分支开发请显式传 "
            "-DBBT_INFRA_SOURCE_DIR=<bbtools-infra 源码树路径>；"
            "其 coroutine 依赖经 -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树路径> "
            "一并指定。已安装包需提供 bbt_infraConfig.cmake 并导出 bbt::infra_common 目标。")
    endif()
endif()

# bbt::infra_common 就绪后定义框架库目标（STATIC + bbt::framework 别名）
add_subdirectory(framework/src)

# 每个公共头单独编译 TU，验证头文件自给
set(BBT_FRAMEWORK_PUBLIC_HEADERS
    Result.hpp
    Message.hpp
    Route.hpp
    CallOptions.hpp
    OrderedTypes.hpp
    OrderedSession.hpp
    RequestContext.hpp
    ServiceContext.hpp
    ExecutionPolicy.hpp
    ShutdownState.hpp
    RpcMethods.hpp
    ICoService.hpp
    CoService.hpp
    CoApp.hpp
    Framework.hpp)
# internal/ 机器面头文件同样做自给自检（Framework.hpp 不传递包含它们，
# 但仍要求独立可编译）。CoApp.hpp 因 add_service 展开方法表而包含
# MethodTable.hpp，属宿主头而非本汇总头。
set(BBT_FRAMEWORK_INTERNAL_HEADERS
    internal/MethodTable.hpp
    internal/RequestScope.hpp
    internal/CallOptionsAdapter.hpp
    internal/HostLifecycle.hpp
    internal/InboundDispatcher.hpp
    internal/InfraHttpHost.hpp
    internal/ActorMailbox.hpp
    internal/ActorRegistry.hpp
    internal/RpcHttpBridge.hpp
    internal/CoAppSeam.hpp)
set(_selfcheck_dir "${CMAKE_CURRENT_BINARY_DIR}/header_selfcheck")
file(MAKE_DIRECTORY "${_selfcheck_dir}")
set(_selfcheck_srcs "")
foreach(_hdr IN LISTS BBT_FRAMEWORK_PUBLIC_HEADERS
                      BBT_FRAMEWORK_INTERNAL_HEADERS)
    get_filename_component(_name "${_hdr}" NAME_WE)
    set(_tu "${_selfcheck_dir}/${_name}.cc")
    file(WRITE "${_tu}" "#include <bbt/framework/${_hdr}>\n")
    list(APPEND _selfcheck_srcs "${_tu}")
endforeach()
add_library(bbt_framework_header_selfcheck STATIC ${_selfcheck_srcs})
target_link_libraries(bbt_framework_header_selfcheck PRIVATE bbt::framework)
target_compile_options(bbt_framework_header_selfcheck PRIVATE -Wall -Wextra)
set_target_properties(bbt_framework_header_selfcheck PROPERTIES LINKER_LANGUAGE CXX)

if(NEED_TEST)
    enable_testing()
    add_subdirectory(tests)
endif()
