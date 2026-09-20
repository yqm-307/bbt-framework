#!/usr/bin/env bash
# 本机构建入口：调用 scripts/build_stack.sh 做 Release + NEED_TEST=ON 的完整构建，
# 与 CI workflow 走同一配方（CI 只是把依赖树换成正式 clone）。
set -euo pipefail
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export BBT_NEED_TEST="${BBT_NEED_TEST:-ON}"
export BBT_JOBS="${BBT_JOBS:-1}"
bash "$REPO_DIR/scripts/build_stack.sh"

if [ "${BBT_SKIP_CTEST:-0}" = "1" ]; then
    exit 0
fi

BBT_WORK_DIR="${BBT_WORK_DIR:-$REPO_DIR/build-deps}"
BBT_BUILD_DIR="${BBT_BUILD_DIR:-$BBT_WORK_DIR/project-build}"

bash "$REPO_DIR/scripts/run_ctest.sh" "$BBT_BUILD_DIR"
