#!/usr/bin/env bash
# 本机构建入口：调用 scripts/build_stack.sh 做 Release + NEED_TEST=ON 的完整构建，
# 与 CI workflow 走同一配方（CI 只是把依赖树换成正式 clone）。
set -euo pipefail
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export BBT_NEED_TEST="${BBT_NEED_TEST:-ON}"
export BBT_JOBS="${BBT_JOBS:-1}"

# 剥离镜像预装旧产物对运行的遮蔽：runner 可能把旧版 bbt 库装进
# /usr/local，并设置 LD_LIBRARY_PATH。ld.so 顺序里 LD_LIBRARY_PATH 先于
# RUNPATH，可能把测试的依赖抢先绑到旧产物，遮蔽构建树内的新编产物。
# 剔除 /usr/local（保留 boost 等其余项），让运行时依赖命中当前构建树。
# 统一在入口脚本 export，保证 build_stack 与 run_ctest 两个子进程都生效。
if [ -n "${LD_LIBRARY_PATH:-}" ]; then
    # 剔除 /usr/local 整棵子树（/usr/local、/usr/local/lib、/usr/local/lib64 等），
    # 保留 boost 等其余项——镜像中的旧 bbt 库可能通过 /usr/local/lib
    # 遮蔽当前构建产物。
    _clean_ld="$(printf '%s' "$LD_LIBRARY_PATH" | tr ':' '\n' \
                 | grep -vE '^/usr/local(/|$)' | paste -sd: -)"
    if [ "$_clean_ld" != "$LD_LIBRARY_PATH" ]; then
        printf '[local_build] LD_LIBRARY_PATH 剔除 /usr/local: %s -> %s\n' \
            "$LD_LIBRARY_PATH" "$_clean_ld"
        export LD_LIBRARY_PATH="$_clean_ld"
    fi
fi

bash "$REPO_DIR/scripts/build_stack.sh"

if [ "${BBT_SKIP_CTEST:-0}" = "1" ]; then
    exit 0
fi

BBT_WORK_DIR="${BBT_WORK_DIR:-$REPO_DIR/build-deps}"
BBT_BUILD_DIR="${BBT_BUILD_DIR:-$BBT_WORK_DIR/project-build}"

bash "$REPO_DIR/scripts/run_ctest.sh" "$BBT_BUILD_DIR"
