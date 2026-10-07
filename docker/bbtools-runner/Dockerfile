# bbtools-runner v2：纯工具链 runner 镜像（Issue #37）
# 在纯工具链之上叠加 memcheck 专用 Valgrind 与 glibc 调试符号，供专用
# memcheck scale set 使用（对齐 main #36，不改变「只提供工具」的边界）。
#
# 基座 = 官方 actions-runner（保证与 ARC 控制器兼容），叠加：
# - 编译器 / cmake / ninja / git / ccache / pkg-config：apt 安装；
# - Valgrind + libc6-dbg：apt 安装，供 memcheck job 使用；
# - Boost 1.90：从 bbtools-common-image COPY（不重编）；
# - 锁定 protobuf 3.21.12 隔离前缀 /opt/protobuf-3.21.12：
#   复用 #30 的 scripts/prepare_protobuf.sh（不重写配方）。
#
# 刻意不烘焙 bbtools-core / infra / coroutine / framework 业务库，也不写 /usr/local
# （旧配方把 core@master 编进 /usr/local 会被运行时抢绑，与本工单 R4/S5 相悖）。
# 业务与 bbt 库在 job 内按 deps.lock 固定 SHA 隔离构建，镜像只提供工具。
#
# 构建入口（仓库根为 context，--network=host 以经宿主机代理取包）：
#   docker/build_toolchain.sh
#
# 固定输入（基础镜像 digest / Boost / protobuf）只认 docker/toolchain.lock，
# 由构建入口传入；本文件不设默认值，缺参即失败（fail-closed）。

# COMMON_IMAGE 由 build_toolchain.sh 传入**本次刚构建出的 common imageID**
# （不是悬浮 tag）：见下方 AS common 阶段。
ARG RUNNER_BASE
ARG COMMON_IMAGE
# 代理默认空（noProxy）：仅当调用方显式传入时才注入，不假定本机恒有代理。
ARG HTTP_PROXY=

# 把 common 作为命名阶段暴露 /opt/boost；COPY --from 用阶段名（常量，
# 规避旧构建器不接受 ARG 直入 COPY --from 的限制），同时以 FROM ${COMMON_IMAGE}
# 真正消费构建入口刚产出的精确 imageID —— 消除「build A、copy B」的悬浮路径。
FROM ${COMMON_IMAGE} AS common
FROM ${RUNNER_BASE}

# 全局 ARG 在 FROM 后超出作用域，此处重新声明以继承构建参数
ARG HTTP_PROXY

USER root

# 仅工具链：不加 libevent-dev（infra 已摘除该依赖），不加 core/git clone。
# 追加 memcheck 专用 valgrind + glibc 调试符号（对齐 main #36），并在构建期跑一次
# 真实 memcheck 冒烟（-error-exitcode=42）以自证可用；profile 标记本镜像身份。
RUN export http_proxy=${HTTP_PROXY} https_proxy=${HTTP_PROXY} && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        g++ cmake ninja-build make git python3 ccache pkg-config \
        ca-certificates curl xz-utils valgrind libc6-dbg && \
    valgrind --version && \
    valgrind --tool=memcheck --error-exitcode=42 \
        --log-file=/tmp/bbttools-valgrind-probe.log /bin/true && \
    test -s /tmp/bbttools-valgrind-probe.log && \
    ! grep -Eq 'valgrind: ?Fatal error|Cannot continue' \
        /tmp/bbttools-valgrind-probe.log && \
    mkdir -p /etc/bbttools-runner && \
    printf '%s\n' 'bbtools-runner:v2-memcheck' > /etc/bbttools-runner/profile && \
    rm -f /tmp/bbttools-valgrind-probe.log && \
    rm -rf /var/lib/apt/lists/*

# Boost：复用 common-image 成果，不重编
COPY --from=common /opt/boost /opt/boost

# 锁定 protobuf 3.21.12 隔离前缀：复用 scripts/prepare_protobuf.sh，不重写配方。
COPY scripts/prepare_protobuf.sh /usr/local/share/bbt/prepare_protobuf.sh
COPY docker/toolchain.lock /usr/local/share/bbt/toolchain.lock
RUN export http_proxy=${HTTP_PROXY} https_proxy=${HTTP_PROXY} && \
    bash /usr/local/share/bbt/prepare_protobuf.sh /opt/protobuf-3.21.12 && \
    echo /opt/protobuf-3.21.12/usr/lib/x86_64-linux-gnu > /etc/ld.so.conf.d/bbt-protobuf.conf && \
    ldconfig

# 工具链身份探针：可在镜像内独立运行（构建期与 CI 复用同一断言）
COPY scripts/verify_toolchain.sh /usr/local/bin/verify-toolchain

# Boost + protobuf 都不在默认路径，用环境变量指过去，不改业务 CMakeLists
# （必须放在探针之前，探针与后续 job 都依赖这些变量）。
ENV CPLUS_INCLUDE_PATH=/opt/boost/include:/opt/protobuf-3.21.12/usr/include \
    LIBRARY_PATH=/opt/boost/lib:/opt/protobuf-3.21.12/usr/lib/x86_64-linux-gnu \
    LD_LIBRARY_PATH=/opt/boost/lib:/opt/protobuf-3.21.12/usr/lib/x86_64-linux-gnu \
    PATH=/opt/protobuf-3.21.12/usr/bin:$PATH \
    BBT_TOOLCHAIN_PROTOBUF_PREFIX=/opt/protobuf-3.21.12

# 构建期自检：protoc 版本 / libprotobuf.a checksum / Boost 成品与版本 /
# 无 /usr/local bbt。探针自身会优先读镜像内 /usr/local/share/bbt/toolchain.lock
# （构建上下文里就是本仓 lock），并以实际 version.hpp 内容核对 Boost 版本。
RUN /usr/local/bin/verify-toolchain --in-image

# 切回基座默认用户（以 docker inspect 基座 Config.User 为准，官方镜像为 runner）
USER runner
