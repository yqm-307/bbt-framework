#!/usr/bin/env bash
# 构建并安装 bbt-framework 到用户前缀（默认 ~/.local）。
# 默认显式使用同级 bbtools-infra 与 bbtools-coroutine 的 main 工作树。
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

cmake -S "${repo_root}" -B "${build_dir}" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DBBT_INFRA_SOURCE_DIR="${infra_dir}" \
    -DBBT_COROUTINE_SOURCE_DIR="${coroutine_dir}" \
    -DNEED_TEST=OFF -DNEED_EXAMPLE=OFF \
    -DBUILD_TESTING=OFF
cmake --build "${build_dir}" --parallel "${jobs}"

install -d "${prefix}/include" "${prefix}/lib"

while IFS= read -r -d '' file; do
    target="${prefix}/include/${file#"${repo_root}/framework/include/"}"
    install -D -m 0644 "${file}" "${target}"
done < <(find "${repo_root}/framework/include" -type f \( -name '*.h' -o -name '*.hh' -o -name '*.hpp' \) -print0)

install -m 0644 "${build_dir}/framework/src/libbbt_framework.a" \
    "${prefix}/lib/libbbt_framework.a"
printf '安装完成：%s\n' "${prefix}"
