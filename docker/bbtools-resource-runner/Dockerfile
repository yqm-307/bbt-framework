# bbt-framework 双服务资源 runner 候选镜像（resource-job）
#
# 目的：在现役工具链 runner 基座之上增量叠加 Redis/Mongo 私有依赖前缀，使
# dual_service 资源验收 job 不必每 job 从浮动上游重编驱动。
#
# 边界（与 bbtools-infra docs/dependencies/resource-prefixes.provenance.md
# 的「镜像层集成」一致）：
#  - **builder 与 final 同基座**（同 OS/ABI）：二进制把构建期 glibc/libstdc++
#    符号版本烘进产物，换 ABI 的 runner 会 GLIBC_2.xx not found；两个 FROM 都用
#    同一个基座，由 docker/build_resource_runner.sh 按 docker/resource-runner.lock
#    传入（本文件不设默认值，缺参即失败）。
#  - **第三方资源构建的唯一 recipe 归 bbtools-infra**：镜像只 COPY 并运行其固定
#    commit 的 scripts/prepare_resource_deps.sh，不在本仓复制下载/构建逻辑。
#  - 前缀装在 /opt/bbt-resource-deps，builder 与 final **同路径**：manifest 的
#    install_root 与镜像最终前缀一致，不 relocate 已有 prefix；产物 RUNPATH 已
#    指向该前缀，runner 内自定位。
#  - final 只从 builder 拷 /opt 资源前缀；不带入 git 源码、服务端、私密配置或
#    bbt 业务库。基座 protobuf（/opt/protobuf-3.21.12）由既有 recipe 产出，
#    final 直接继承并复核，不重写、不重复拷贝、不 relocate。
#  - 只在 final 写**显式前缀**环境变量（指针，不是搜索路径）；**不**把资源 lib
#    目录塞进全局 LD_LIBRARY_PATH（避免抢绑；消费者按契约显式传前缀）。
#  - 基座 CMD/USER 及 /opt/boost、protobuf、arc runner 相关资产保持不变。

ARG RUNNER_BASE
ARG RESOURCE_JOBS=3
ARG HTTP_PROXY=

# ---- builder：在基座内运行 infra 固定 recipe，产出资源前缀 ----------------------
FROM ${RUNNER_BASE} AS resource-builder
ARG RESOURCE_JOBS
ARG HTTP_PROXY
# 基座 Config.User=runner，构建 RUN 默认以 runner 执行；recipe 需写 /opt，切 root。
USER root

# 唯一 recipe 与消费者自检工程：来自 bbtools-infra 固定 commit（构建入口用
# git archive <commit> 精确落盘到构建上下文 infra/ 下，不复制其下载/构建逻辑）。
COPY infra/scripts/prepare_resource_deps.sh /usr/local/share/bbt-resource/prepare_resource_deps.sh
COPY infra/scripts/resource_prefix_verify /usr/local/share/bbt-resource/resource_prefix_verify

# recipe 的硬依赖，缺任一即 fail-closed（objdump 用于生成 ABI 证据，不写 unknown）。
RUN command -v objdump >/dev/null 2>&1 \
 && command -v cmake   >/dev/null 2>&1 \
 && command -v ninja   >/dev/null 2>&1 \
 && command -v g++     >/dev/null 2>&1

# 构建三份私有前缀（hiredis/mongoc/mongocxx）。网络只在 clone 上游固定 tag 源码时
# 需要；recipe 不读取任何凭据类环境变量。
RUN export http_proxy=${HTTP_PROXY} https_proxy=${HTTP_PROXY} \
 && bash /usr/local/share/bbt-resource/prepare_resource_deps.sh \
        /opt/bbt-resource-deps --jobs ${RESOURCE_JOBS}

# 构建期 consumer verifier：按 infra 消费契约 find_package，真实链接并运行，
# 断言 imported target 存在、运行版本等于锁定值、ldd 解析全在本次前缀内。
RUN cmake -S /usr/local/share/bbt-resource/resource_prefix_verify \
        -B /tmp/bbt-resource-verify -G Ninja \
        -DRESOURCE_PREFIX_ROOT=/opt/bbt-resource-deps \
 && cmake --build /tmp/bbt-resource-verify \
 && rm -rf /tmp/bbt-resource-verify

# ---- final：只叠加资源前缀，其余全部继承基座 -----------------------------------
FROM ${RUNNER_BASE} AS final

# 只从 builder 拷 /opt 资源前缀（不带入 git 源码/服务端/私密配置/bbt 业务库）。
COPY --from=resource-builder /opt/bbt-resource-deps /opt/bbt-resource-deps

USER root
# 消费者自检工程随镜像保留（小工具，供 CI/运维复跑）；非第三方源码树。
COPY infra/scripts/resource_prefix_verify /usr/local/share/bbt-resource/resource_prefix_verify

# 显式前缀环境变量（消费者按契约使用）；**不**改全局 LD_LIBRARY_PATH。
ENV BBT_RESOURCE_PREFIX_ROOT=/opt/bbt-resource-deps \
    BBT_HIREDIS_PREFIX=/opt/bbt-resource-deps/hiredis \
    BBT_MONGOC_PREFIX=/opt/bbt-resource-deps/mongoc \
    BBT_MONGOCXX_PREFIX=/opt/bbt-resource-deps/mongocxx

# 复核基座既有 protobuf：只断言 protoc 版本 + libprotobuf.a checksum。
# 注意：**不代表** docker/toolchain.lock（v2，COMMON_BASE=debian:trixie-slim）
# 整体满足——只取其对本基座适用的两项断言。
ARG PROTOC_VERSION
ARG LIBPROTOBUF_A_SHA256
RUN set -eux; \
    test -x "/opt/protobuf-${PROTOC_VERSION}/usr/bin/protoc"; \
    ver="$(LD_LIBRARY_PATH=/opt/protobuf-${PROTOC_VERSION}/usr/lib/x86_64-linux-gnu \
           /opt/protobuf-${PROTOC_VERSION}/usr/bin/protoc --version)"; \
    test "$ver" = "libprotoc ${PROTOC_VERSION}"; \
    printf '%s  %s\n' "${LIBPROTOBUF_A_SHA256}" \
        "/opt/protobuf-${PROTOC_VERSION}/usr/lib/x86_64-linux-gnu/libprotobuf.a" \
        | sha256sum --check --status

# final 以 runner 用户复跑 consumer verifier（与 job 运行态一致）：
# 错链接 / 版本不符 / ldd 命中前缀外同名库 / 缺 objdump 都立即失败。
USER runner
RUN cmake -S /usr/local/share/bbt-resource/resource_prefix_verify \
        -B /tmp/bbt-resource-verify -G Ninja \
        -DRESOURCE_PREFIX_ROOT=/opt/bbt-resource-deps \
 && cmake --build /tmp/bbt-resource-verify \
 && rm -rf /tmp/bbt-resource-verify
