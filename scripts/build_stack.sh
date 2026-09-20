#!/usr/bin/env bash
# 依赖与构建唯一配方：core/coroutine/infra 全部经 add_subdirectory 源码接入，
# 由 infra 顶层统一编排（core → coroutine → infra → 目标工程）。
#
# 为什么需要它：bbtools-coroutine 按名字链接 bbt_core 并按 <bbt/core/...>
# 包含 core 头；三个上游仓都不导出可消费 CMake 包（无 install/export），
# 源码消费只能经显式 *_SOURCE_DIR 路径接入。infra（94a835a 起）在校验
# coroutine 仍引用 bbt_core 时要求本构建已有真实 bbt_core target，否则
# fail-closed——因此必须把 BBT_CORE_SOURCE_DIR 传给 infra 由它 add_subdirectory，
# 不能用「装 .so+头到前缀」的方式绕过（prefix 不产生 target）。
#
# 可用环境变量（均有默认值）：
#   BBT_DEPS_DIR             依赖源码树父目录，默认 <repo>/../deps
#   BBT_CORE_SOURCE_DIR      core 源码树，默认 $BBT_DEPS_DIR/core-master
#   BBT_COROUTINE_SOURCE_DIR coroutine 源码树，默认 $BBT_DEPS_DIR/coroutine-main
#   BBT_INFRA_SOURCE_DIR     infra 源码树，默认 $BBT_DEPS_DIR/infra-main
#   BBT_WORK_DIR             构建根，默认 <repo>/build-deps
#   BBT_PROJECT_DIR          被配置的 CMake 工程，默认 <repo>
#   BBT_BUILD_DIR            该工程的构建目录，默认 $BBT_WORK_DIR/project-build
#   BBT_NEED_TEST            传给工程的 -DNEED_TEST，默认 ON
#   BBT_JOBS                 并行度，默认 4
#   BBT_CMAKE_ARGS           追加的 cmake 参数（空白分隔）
#   BBT_SKIP_BUILD=1         只配置不构建
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BBT_DEPS_DIR="${BBT_DEPS_DIR:-$REPO_DIR/../deps}"
BBT_CORE_SOURCE_DIR="${BBT_CORE_SOURCE_DIR:-$BBT_DEPS_DIR/core-master}"
BBT_COROUTINE_SOURCE_DIR="${BBT_COROUTINE_SOURCE_DIR:-$BBT_DEPS_DIR/coroutine-main}"
BBT_INFRA_SOURCE_DIR="${BBT_INFRA_SOURCE_DIR:-$BBT_DEPS_DIR/infra-main}"
BBT_WORK_DIR="${BBT_WORK_DIR:-$REPO_DIR/build-deps}"
BBT_PROJECT_DIR="${BBT_PROJECT_DIR:-$REPO_DIR}"
BBT_BUILD_DIR="${BBT_BUILD_DIR:-$BBT_WORK_DIR/project-build}"
BBT_NEED_TEST="${BBT_NEED_TEST:-ON}"
BBT_JOBS="${BBT_JOBS:-4}"
BBT_CMAKE_ARGS="${BBT_CMAKE_ARGS:-}"

MANIFEST="$BBT_WORK_DIR/deps-manifest.txt"

log() { printf '[build_stack] %s\n' "$*"; }

for d in "$BBT_CORE_SOURCE_DIR" "$BBT_COROUTINE_SOURCE_DIR" "$BBT_INFRA_SOURCE_DIR"; do
    if [ ! -f "$d/CMakeLists.txt" ]; then
        echo "[build_stack] FATAL: 依赖源码树无效（缺 CMakeLists.txt）: $d" >&2
        exit 2
    fi
done

# 工作目录必须先建：manifest/log 都直接写它下面。历史上靠已有 build-deps
# 目录掩盖了这一步，全新 BBT_WORK_DIR（干净 runner / 容器内）会直接失败。
mkdir -p "$BBT_WORK_DIR"

# ---- 0) 依赖来源清单（SHA 必须可核对，日志即证据） --------------------------
{
    echo "# deps manifest  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    for pair in "core=$BBT_CORE_SOURCE_DIR" "coroutine=$BBT_COROUTINE_SOURCE_DIR" \
                "infra=$BBT_INFRA_SOURCE_DIR"; do
        name="${pair%%=*}"; dir="${pair#*=}"
        sha="$(git -C "$dir" rev-parse HEAD 2>/dev/null || echo 'not-a-git-tree')"
        echo "$name path=$dir sha=$sha"
    done
} | tee "$MANIFEST"

# ---- 0b) 依赖树与 deps.lock 对账：本机/候选分支的依赖漂移必须被拦住 ---------
# deps.lock 是唯一事实源（"CI 与 fetch_deps.sh 只认本文件"）。本机依赖树若被
# 别处（共享 ../deps、手动 checkout）改到非 pin SHA，构建照常绿却与 pin 不符；
# 此处把 manifest 的实测 SHA 与 deps.lock 逐条比对，不一致即 fail-closed。
# 逃生阀：BBT_ALLOW_OFF_PIN=1 跳过（仅本机调试用）。
LOCK="$REPO_DIR/deps.lock"
if [ "${BBT_ALLOW_OFF_PIN:-0}" != "1" ] && [ -f "$LOCK" ]; then
    while read -r name rest || [ -n "$name$rest" ]; do
        case "$name" in ''|'#'*) continue ;; esac
        want_sha=""; want_dir=""
        for kv in $rest; do
            case "$kv" in
                sha=*) want_sha="${kv#sha=}" ;;
                dir=*) want_dir="${kv#dir=}" ;;
            esac
        done
        [ -z "$want_sha" ] && continue
        have_sha="$(git -C "$BBT_DEPS_DIR/$want_dir" rev-parse HEAD 2>/dev/null || echo MISSING)"
        if [ "$have_sha" != "$want_sha" ]; then
            echo "[build_stack] FATAL: 依赖 '$name' 漂移: pin=$want_sha 实际=$have_sha" >&2
            echo "[build_stack]   修法: 跑 scripts/fetch_deps.sh 拉齐 deps.lock；或 BBT_ALLOW_OFF_PIN=1 跳过（本机调试）" >&2
            exit 8
        fi
    done < "$LOCK"
    log "依赖树与 deps.lock 一致"
fi

# ---- 1) core：经 BBT_CORE_SOURCE_DIR 由 infra add_subdirectory 接入 ----------
# infra（94a835a 起）的 fail-closed 契约要求 coroutine 按名字链 bbt_core 时
# 本构建必须已有真实 bbt_core target——prefix（只给 .so+头）不产生 target，
# 无法通过该校验。因此改为把 core 源码树显式传给 infra，由它 add_subdirectory
# 建立真实 target；coroutine 的 -lbbt_core 与 <bbt/core/...> 头都由该 target
# 的 usage requirements 解析，不再依赖本脚本前缀或 /usr/local。
_need() { # $1=必须存在的路径  $2=角色说明
    if [ ! -e "$1" ]; then
        echo "[build_stack] FATAL: 依赖闭包缺口: $1 —— $2" >&2
        echo "[build_stack]   修法: 跑 scripts/fetch_deps.sh 拉齐 deps.lock 的依赖树。" >&2
        exit 6
    fi
}
_need "$BBT_CORE_SOURCE_DIR/CMakeLists.txt"                 "core 源码树（infra 经 BBT_CORE_SOURCE_DIR add_subdirectory）"
_need "$BBT_CORE_SOURCE_DIR/bbt/core/log/DebugPrint.hpp"    "core 头前缀（coroutine 的 <bbt/core/...> 包含依赖）"
_need "$BBT_COROUTINE_SOURCE_DIR/bbt/coroutine/object/interface/ICoObject.hpp" "infra 需要的 coroutine service-runtime 公共面"
_need "$BBT_COROUTINE_SOURCE_DIR/bbt/coroutine/sync/Cancellation.hpp" "infra 需要的 coroutine Cancellation 公共面"

# ---- 2) 目标工程配置/构建 ---------------------------------------------------
# core/coroutine/infra 全经 add_subdirectory 源码接入（infra 内部先接 core
# 再接 coroutine）；产物链接到 build 树内的真实 target，不写 /usr/local。
log "配置: $BBT_PROJECT_DIR -> $BBT_BUILD_DIR (BBT_NEED_TEST=$BBT_NEED_TEST)"
# shellcheck disable=SC2086
cmake -S "$BBT_PROJECT_DIR" -B "$BBT_BUILD_DIR" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DNEED_TEST="$BBT_NEED_TEST" \
    -DBBT_CORE_SOURCE_DIR="$BBT_CORE_SOURCE_DIR" \
    -DBBT_INFRA_SOURCE_DIR="$BBT_INFRA_SOURCE_DIR" \
    -DBBT_COROUTINE_SOURCE_DIR="$BBT_COROUTINE_SOURCE_DIR" \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    $BBT_CMAKE_ARGS >"$BBT_WORK_DIR/project-configure.log" 2>&1 || {
        tail -40 "$BBT_WORK_DIR/project-configure.log"; exit 4; }

if [ "${BBT_SKIP_BUILD:-0}" != "1" ]; then
    log "构建（-j$BBT_JOBS）"
    cmake --build "$BBT_BUILD_DIR" -j "$BBT_JOBS" >"$BBT_WORK_DIR/project-build.log" 2>&1 || {
        tail -60 "$BBT_WORK_DIR/project-build.log"; exit 5; }
fi

# ---- 3) 实际链接来源核对（证明没有吃到 /usr/local 旧产物） ------------------
LINK_REPORT="$BBT_WORK_DIR/link-sources.txt"
: >"$LINK_REPORT"
while IFS= read -r bin; do
    {
        echo "## $bin"
        ldd "$bin" 2>/dev/null | grep -E "bbt_|boost_context|libevent" || true
    } >>"$LINK_REPORT"
done < <(find "$BBT_BUILD_DIR" -maxdepth 3 -type f -perm -u+x \
             \( -name 'Test_framework_*' -o -name 'svc_a' -o -name 'svc_b' -o -name 'driver' \) 2>/dev/null)
log "链接来源报告: $LINK_REPORT"
cat "$LINK_REPORT"

# 诊断（不进 link-sources.txt，避免污染门禁）：core 是否以 target 全路径链接。
# 若链接退化成 -lbbt_core，运行时 ld.so 按 SONAME 落到系统 /usr/local 旧产物。
echo "## [diag] bbt_core link line (build.ninja)"
grep -oE '[^ ]*libbbt_core[^ ]*' "$BBT_BUILD_DIR/build.ninja" 2>/dev/null | sort -u | head
echo "## [diag] Test_framework_f0 RUNPATH"
readelf -d "$BBT_BUILD_DIR/tests/Test_framework_f0" 2>/dev/null | grep -iE 'RPATH|RUNPATH' || true
echo "## [diag] built core lib on disk"
ls -la "$BBT_BUILD_DIR/_deps/bbtools-core/lib/" 2>/dev/null || echo "(no core lib dir)"
echo "## [diag] ld.so search trace for libbbt_core"
LD_DEBUG=libs ldd "$BBT_BUILD_DIR/tests/Test_framework_f0" 2>&1 | grep -iE 'libbbt_core|trying|search path' | head -30 || true

# 可判定门禁：干净 runner 不许依赖 /usr/local 旧产物；core 必须经
# add_subdirectory 解析到 build 树内的真实 target（路径含 $BBT_BUILD_DIR）。
if grep -q "/usr/local/" "$LINK_REPORT"; then
    echo "[build_stack] FATAL: 构建产物链接到 /usr/local 下的库（干净 runner 要求不依赖旧安装）" >&2
    grep -n "/usr/local/" "$LINK_REPORT" >&2
    exit 7
fi
# core 断言必须是路径断言，不能只匹配子串：非 /usr/local 的旧前缀（/opt、~/prefix）
# 也会产生 libbbt_core.so 行；报告为空（目标改名/移深/NEED_TEST=OFF）时整条
# 门禁不得静默失效。
if [ ! -s "$LINK_REPORT" ]; then
    echo "[build_stack] FATAL: 链接来源报告为空——未收集到任何可执行产物，无法证明依赖来源" >&2
    exit 7
fi
if ! grep -q "libbbt_core.*$BBT_BUILD_DIR" "$LINK_REPORT"; then
    echo "[build_stack] FATAL: 构建产物未解析到 build 树内的 bbt_core（应含 $BBT_BUILD_DIR 路径）" >&2
    exit 7
fi
log "完成：build=$BBT_BUILD_DIR（core/coroutine/infra 均源码接入，无外部前缀）"
