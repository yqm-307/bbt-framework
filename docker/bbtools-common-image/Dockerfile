# bbtools 共用 CI 构建镜像 v1（coroutine / infra / framework 三仓共用）
#
# 决策（2026-09-18，用户确认）：
# - Boost 1.90：trixie 官方源只有 1.83，必须源码构建；
#   包地址 https://archives.boost.io/release/1.90.0/source/boost_1_90_0.tar.gz（已验证可达）
# - libevent 已摘除依赖，不进镜像；bbtools-core 已拆解，不进镜像
# - 基座对齐 s3 现役 runner：Debian 13 + g++14 + cmake 3.31 + ninja
# - 只存算力节点本地 containerd（k3s ctr images import），无外部 registry；
#   workflow 侧 imagePullPolicy 取 IfNotPresent
# - 自研单测框架零第三方依赖；Boost 只用 context 编译库 + 头文件部分，
#   ci-guide 把 Boost.Test 列为新单测写法故把 test 编译库也烘进去
# - 某仓新增依赖时在此追加并 bump TAG，重新 import 即可

ARG BOOST_VERSION=1.90.0

# ---- 构建 Boost（只编需要的 6 个库，头文件全量保留） ----
FROM debian:trixie-slim AS boost-builder
ARG BOOST_VERSION
# 代理穿透：直连慢时用 --network=host + --build-arg http_proxy/https_proxy 注入
ARG http_proxy=
ARG https_proxy=
ARG HTTP_PROXY=
ARG HTTPS_PROXY=
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential wget ca-certificates python3 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN set -e; \
    V_US=$(echo "$BOOST_VERSION" | tr '.' '_'); \
    wget -q "https://archives.boost.io/release/${BOOST_VERSION}/source/boost_${V_US}.tar.gz" -O boost.tar.gz; \
    mkdir boost && tar xzf boost.tar.gz -C boost --strip-components=1; \
    cd boost; \
    ./bootstrap.sh --prefix=/opt/boost \
        --with-libraries=context,filesystem,program_options,system,test,thread; \
    ./b2 -j"$(nproc)" variant=release link=shared,static threading=multi install; \
    rm -rf /src/boost /src/boost.tar.gz

# ---- 最终镜像：toolchain + 成品 Boost ----
FROM debian:trixie-slim
ENV DEBIAN_FRONTEND=noninteractive \
    CC=gcc \
    CXX=g++ \
    CMAKE_GENERATOR=Ninja \
    CMAKE_PREFIX_PATH=/opt/boost \
    BOOST_ROOT=/opt/boost
COPY --from=boost-builder /opt/boost /opt/boost
RUN echo "/opt/boost/lib" > /etc/ld.so.conf.d/boost.conf \
    && apt-get update && apt-get install -y --no-install-recommends \
        g++ \
        cmake \
        ninja-build \
        make \
        git \
        python3 \
        ccache \
        pkg-config \
    && ldconfig \
    && rm -rf /var/lib/apt/lists/*
CMD ["bash"]
