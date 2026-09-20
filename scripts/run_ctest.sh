#!/usr/bin/env bash
# CTest 门禁：失败阻断；SKIP / Not Run 不得被当成全绿。
set -euo pipefail

BUILD_DIR="${1:?usage: run_ctest.sh <build-dir>}"
if [ ! -d "$BUILD_DIR" ]; then
    echo "[run_ctest] FATAL: 构建目录不存在: $BUILD_DIR" >&2
    exit 2
fi

LOG="${2:-$BUILD_DIR/ctest.log}"
set +e
ctest --test-dir "$BUILD_DIR" -j1 --output-on-failure --no-tests=error | tee "$LOG"
rc=${PIPESTATUS[0]}
set -e

if grep -Eq '(Not Run|Skipped|\*\*\*Skipped)' "$LOG"; then
    echo "[run_ctest] FATAL: 检测到 SKIP/Not Run，禁止报全绿" >&2
    grep -En '(Not Run|Skipped|\*\*\*Skipped)' "$LOG" >&2 || true
    exit 8
fi
if [ "$rc" -ne 0 ]; then
    echo "[run_ctest] FATAL: ctest 退出码 $rc" >&2
    exit "$rc"
fi
echo "[run_ctest] OK"
