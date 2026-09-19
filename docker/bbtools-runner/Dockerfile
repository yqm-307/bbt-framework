# bbtools-runner v1：coroutine CI 专用 runner 镜像
#
# 基座 = 官方 actions-runner（保证与 ARC 控制器兼容），叠加工具链：
# - 编译器/cmake/ninja：apt 安装
# - Boost 1.90：从 bbtools-common-image:v1 COPY（不重编）
# - bbtools-core：构建时按 CORE_REF 编进 /usr/local（与 s3 口径一致）；
#   core 有更新时重编本镜像即取最新
# - 业务代码（coroutine/infra）仍在 Job 运行时 checkout，不烘进镜像
#
# 构建（s3 上，经 mihomo）：
#   docker build --network=host -t bbtools-runner:v1 \
#     --build-arg RUNNER_BASE=ghcr.io/actions/actions-runner:<pin> -f Dockerfile .

ARG RUNNER_BASE=ghcr.io/actions/actions-runner:latest
ARG COMMON_IMAGE=bbtools-common-image:v1
ARG CORE_REF=master
ARG HTTP_PROXY=http://127.0.0.1:7890

FROM ${RUNNER_BASE}

# 全局 ARG 在 FROM 后超出作用域，此处重新声明以继承构建参数
ARG COMMON_IMAGE
ARG CORE_REF
ARG HTTP_PROXY

USER root

RUN export http_proxy=${HTTP_PROXY} https_proxy=${HTTP_PROXY} && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        g++ cmake ninja-build make git python3 ccache pkg-config libevent-dev && \
    rm -rf /var/lib/apt/lists/*

# Boost：复用 common-image 成果，不重编
# 注：s3 的 docker 不支持 COPY --from 中用 ARG 变量，此处硬编码镜像名
COPY --from=bbtools-common-image:v1 /opt/boost /opt/boost

# common-image 的 Boost 不在默认路径，用编译器环境变量指过去，不改业务 CMakeLists
# （必须放在 core 构建之前，core 的编译也要找 Boost 头文件）
ENV CPLUS_INCLUDE_PATH=/opt/boost/include \
    LIBRARY_PATH=/opt/boost/lib \
    LD_LIBRARY_PATH=/opt/boost/lib:/usr/local/lib

# bbtools-core：编进 /usr/local（复刻 s3 的 sudo make install 口径；
# 上游 CMake 无 install target，故手动拷 .so + 头文件树）
RUN export http_proxy=${HTTP_PROXY} https_proxy=${HTTP_PROXY} && \
    git clone --depth 1 --branch ${CORE_REF} \
        https://github.com/yqm-307/bbtools-core.git /tmp/core && \
    cmake -S /tmp/core -B /tmp/core/build -G Ninja \
        -DCMAKE_BUILD_TYPE=Release -DNEED_TEST=OFF -DENABLE_EXAMPLE=OFF && \
    cmake --build /tmp/core/build -j$(nproc) && \
    cp /tmp/core/build/lib/libbbt_core.so /usr/local/lib/ && \
    cp -r /tmp/core/bbt /usr/local/include/bbt && \
    ldconfig && \
    ls /usr/local/lib/libbbt_core.so \
       /usr/local/include/bbt/core/Attribute.hpp \
       /opt/boost/lib/libboost_context.so && \
    rm -rf /tmp/core

# 切回基座默认用户（以 docker inspect 基座 Config.User 为准，官方镜像为 runner）
USER runner
