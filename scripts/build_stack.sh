#!/usr/bin/env bash
# 依赖与构建唯一配方：固定的 coroutine/infra 源码树经 add_subdirectory 接入，
# 由 infra 顶层编排后构建目标工程。当前锁定的 coroutine 已自带 core/pollevent
# 闭包，不再生成或链接独立的 bbt_core；脚本末尾对构建树内 coroutine 做来源门禁。
#
# 两个上游仓都不导出可消费 CMake 包（无 install/export），源码消费只能经显式
# *_SOURCE_DIR 路径接入，不能依赖本机前缀或历史安装产物。
#
# 可用环境变量（均有默认值）：
#   BBT_DEPS_DIR             依赖源码树父目录，默认 <repo>/../deps
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

for d in "$BBT_COROUTINE_SOURCE_DIR" "$BBT_INFRA_SOURCE_DIR"; do
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
    for pair in "coroutine=$BBT_COROUTINE_SOURCE_DIR" \
                "infra=$BBT_INFRA_SOURCE_DIR"; do
        name="${pair%%=*}"; dir="${pair#*=}"
        sha="$(git -C "$dir" rev-parse HEAD 2>/dev/null || echo 'not-a-git-tree')"
        echo "$name path=$dir sha=$sha"
    done
} | tee "$MANIFEST"

# ---- 0b) 实际编译依赖与 deps.lock 对账：依赖漂移必须被拦住 ---------------
# deps.lock 是唯一事实源（"CI 与 fetch_deps.sh 只认本文件"）。本机依赖树若被
# 别处（共享 ../deps、手动 checkout）改到非 pin SHA，构建照常绿却与 pin 不符；
# 此处把实际传给 CMake 的源码树 SHA 与 deps.lock 逐条比对，不一致即
# fail-closed；不能只检查 BBT_DEPS_DIR 下的同名目录，否则会出现「检查 A、
# 编译 B」的假通过/假失败。
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
        case "$name" in
            coroutine) actual_dir="$BBT_COROUTINE_SOURCE_DIR" ;;
            infra) actual_dir="$BBT_INFRA_SOURCE_DIR" ;;
            *) echo "[build_stack] FATAL: deps.lock 中存在未接入的依赖: $name" >&2; exit 8 ;;
        esac
        have_sha="$(git -C "$actual_dir" rev-parse HEAD 2>/dev/null || echo MISSING)"
        if [ "$have_sha" != "$want_sha" ]; then
            echo "[build_stack] FATAL: 实际编译依赖 '$name' 漂移: pin=$want_sha 实际=$have_sha 路径=$actual_dir" >&2
            echo "[build_stack]   修法: 跑 scripts/fetch_deps.sh 拉齐 deps.lock；或 BBT_ALLOW_OFF_PIN=1 跳过（本机调试）" >&2
            exit 8
        fi
    done < "$LOCK"
    log "依赖树与 deps.lock 一致"
fi

# ---- 1) coroutine 公共面门禁 -----------------------------------------------
# 当前锁定的 coroutine 自带 core/pollevent；这里只核对 framework/infra 所需的
# service-runtime 公共头，避免重新引入已删除的取消头或旧 core 前缀。
_need() { # $1=必须存在的路径  $2=角色说明
    if [ ! -e "$1" ]; then
        echo "[build_stack] FATAL: 依赖闭包缺口: $1 —— $2" >&2
        echo "[build_stack]   修法: 跑 scripts/fetch_deps.sh 拉齐 deps.lock 的依赖树。" >&2
        exit 6
    fi
}
_need "$BBT_COROUTINE_SOURCE_DIR/bbt/coroutine/object/interface/ICoObject.hpp" "infra 需要的 coroutine service-runtime 公共面"

# ---- 2) 目标工程配置/构建 ---------------------------------------------------
# coroutine/infra 经 add_subdirectory 源码接入；产物链接到 build 树内的真实
# target，不写 /usr/local。
log "配置: $BBT_PROJECT_DIR -> $BBT_BUILD_DIR (BBT_NEED_TEST=$BBT_NEED_TEST)"
# shellcheck disable=SC2086
cmake -S "$BBT_PROJECT_DIR" -B "$BBT_BUILD_DIR" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DNEED_TEST="$BBT_NEED_TEST" \
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

echo "## [diag] Test_framework_f0 RUNPATH"
readelf -d "$BBT_BUILD_DIR/tests/Test_framework_f0" 2>/dev/null | grep -iE 'RPATH|RUNPATH' || true
echo "## [diag] coroutine runtime dependencies"
readelf -d "$BBT_BUILD_DIR/_deps/bbtools-coroutine/lib/libbbt_coroutine.so" 2>/dev/null | grep -iE 'RPATH|RUNPATH|NEEDED' || true

# 可判定门禁：干净 runner 不许依赖 /usr/local 旧产物；当前架构要求
# 所有可执行产物解析到本构建树内的 coroutine runtime，报告为空或未命中时失败。
if grep -q "/usr/local/" "$LINK_REPORT"; then
    echo "[build_stack] FATAL: 构建产物链接到 /usr/local 下的库（干净 runner 要求不依赖旧安装）" >&2
    grep -n "/usr/local/" "$LINK_REPORT" >&2
    exit 7
fi
if [ ! -s "$LINK_REPORT" ]; then
    echo "[build_stack] FATAL: 链接来源报告为空——未收集到任何可执行产物，无法证明依赖来源" >&2
    exit 7
fi
if grep -q "libbbt_coroutine.*$BBT_BUILD_DIR" "$LINK_REPORT"; then
    log "链接门禁：检测到 build 树内 bbt_coroutine（直依赖架构）"
else
    echo "[build_stack] FATAL: 构建产物未解析到 build 树内的 bbt_coroutine" >&2
    exit 7
fi
log "完成：build=$BBT_BUILD_DIR（coroutine/infra 均源码接入，无外部前缀）"
