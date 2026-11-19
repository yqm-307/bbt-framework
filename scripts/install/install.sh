#!/usr/bin/env bash
# 构建并安装 bbt-framework 到指定前缀（BBT_INSTALL_PREFIX，默认 ~/.local）。
# 默认显式使用同级 bbtools-infra 与 bbtools-coroutine 的 main 工作树；
# 依赖源码/前缀用环境变量固定：BBT_INFRA_SOURCE_DIR、BBT_COROUTINE_SOURCE_DIR、
# BBT_PROTOBUF_PREFIX、BBT_INFRA_HTTP_NET、BBT_INFRA_RPC_NET（后两者默认 ON，
# 框架链接 bbt::infra_http_net/bbt::infra_rpc_net 是硬前置，不降级）。
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
infra_dir="${BBT_INFRA_SOURCE_DIR:-${repo_root}/../bbtools-infra}"
coroutine_dir="${BBT_COROUTINE_SOURCE_DIR:-${repo_root}/../bbtools-coroutine}"
prefix="${BBT_INSTALL_PREFIX:-${HOME}/.local}"
build_dir="${BBT_BUILD_DIR:-${repo_root}/build-install}"
jobs="${BBT_JOBS:-2}"

for dir in "${infra_dir}" "${coroutine_dir}"; do
    if [ ! -f "${dir}/CMakeLists.txt" ]; then
        echo "源码树无效：${dir}" >&2
        exit 1
    fi
done

cmake_args=(
    -DCMAKE_BUILD_TYPE=Release
    "-DBBT_INFRA_SOURCE_DIR=${infra_dir}"
    "-DBBT_COROUTINE_SOURCE_DIR=${coroutine_dir}"
    "-DBBT_INFRA_HTTP_NET=${BBT_INFRA_HTTP_NET:-ON}"
    "-DBBT_INFRA_RPC_NET=${BBT_INFRA_RPC_NET:-ON}"
    -DNEED_TEST=OFF -DNEED_EXAMPLE=OFF
    -DBUILD_TESTING=OFF
)
# 可选资源按实际存在的 infra 链接目标决定是否装入框架静态库。
for option in BBT_INFRA_REDIS_NET BBT_HIREDIS_PREFIX BBT_INFRA_MONGO BBT_MONGOC_PREFIX BBT_MONGOCXX_PREFIX; do
    if [ -n "${!option:-}" ]; then
        cmake_args+=("-D${option}=${!option}")
    fi
done

# 固定 protobuf 前缀时才显式传递；不传则沿用 infra 的默认发现逻辑。
if [ -n "${BBT_PROTOBUF_PREFIX:-}" ]; then
    cmake_args+=("-DBBT_PROTOBUF_PREFIX=${BBT_PROTOBUF_PREFIX}")
fi

cmake -S "${repo_root}" -B "${build_dir}" -G Ninja "${cmake_args[@]}"
cmake --build "${build_dir}" --parallel "${jobs}"

install -d "${prefix}/include" "${prefix}/lib"

# 公共头与实现已收拢到 bbt/framework/（内部头在 internal/ 子目录），
# 安装布局保持 include/bbt/framework/... 不变。
while IFS= read -r -d '' file; do
    case "${file##*/}" in
        RedisNetResource.hpp|MongoDbResource.hpp)
            if ! grep -Fxq "${file##*/}" "${build_dir}/framework-resource-headers.txt"; then
                continue
            fi
            ;;
    esac
    target="${prefix}/include/${file#"${repo_root}/"}"
    install -D -m 0644 "${file}" "${target}"
done < <(find "${repo_root}/bbt" -type f \( -name '*.h' -o -name '*.hh' -o -name '*.hpp' \) -print0)

install -m 0644 "${build_dir}/bbt/framework/libbbt_framework.a" \
    "${prefix}/lib/libbbt_framework.a"
printf '安装完成：%s\n' "${prefix}"
