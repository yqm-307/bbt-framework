#!/usr/bin/env bash
#
# 来源与局限：本文件为**通用 recipe**，逐字复制自已独立审查的 bbtools-infra #50 影子候选
#   infra-worktree/scripts/ci/prepare_boost.sh
# （来源 sha256 26f44c18331539a4bdbb830aae739842fc7b25a4d625c356e3de6d5a6ac5f9e0；本仓新增
# 上方针注后内容 sha256 以本文件实际值为准）。与仓库无关（只按锁定版本/sha256 源码构建 Boost
# 到私有前缀），故原样复用。局限：只编 cmake 实际消费的 `context` 组件（对应本仓 deps.lock 的
# coroutine `find_package(Boost 1.90 REQUIRED COMPONENTS context)`）；若上游 CMakeLists 新增
# Boost 编译组件需在此追加，不预先扩编。
#
# 影子 hosted 工具链配方：源码构建**锁定版 Boost 1.90**到给定私有前缀。
#
# 为什么需要它：现役 ci.yml 运行在 ARC 镜像内，镜像自带 /opt/boost（Boost 1.90），
# hosted runner 只有系统预装 Boost（版本不同）。按 Issue #50/R3「工具链必须按实际
# 需求固定/复现，不能用 hosted 预装版本假替代」，此处把锁定版 Boost 真的编到工作区
# 前缀，不改 runner 镜像、不装系统包、不写 /usr/local。
#
# 锁定输入来自 framework 已独立审查发布的 docker/toolchain.lock（BOOST_VERSION /
# BOOST_SHA256），与 bbt-framework docker/bbtools-common-image 的 Boost 配方同源同值：
#   BOOST_VERSION=1.90.0
#   BOOST_SHA256=5e93d582aff26868d581a52ae78c7d8edf3f3064742c6e77901a1f18a437eea9
# 下载后必须 sha256 命中，否则 fail-closed。
#
# 编译组件：只编 gate 实际消费的 `context`（现役真实 CI 日志实证
# `-- Found Boost: .../Boost-1.90.0/BoostConfig.cmake ... found components: context`）。
# 若将来实际需求新增 Boost 编译组件，在 BOOST_LIBS 追加并同步验证，不预先扩编。
#
# 用法：scripts/ci/prepare_boost.sh <empty-prefix> [--jobs N] [--print-plan]
#   <empty-prefix> 必须不存在；已存在（含空目录/悬浮符号链接）一律拒绝。
#   --print-plan    只打印将下载的 URL/校验值/组件，不产生任何副作用（离线冒烟用）。
#
# 可选环境变量 BOOST_SOURCE_CACHE（不新增命令行参数，保持现役调用字面量不变）：
#   指向一个源码归档缓存目录。命中条件严格：`$BOOST_SOURCE_CACHE/boost_1_90_0.tar.gz`
#   必须是普通文件且通过锁定 sha256 校验；否则告警并回退在线下载（不跳过校验）。
#   缓存根目录本身是符号链接时整条忽略（读与回写前各查一次，不沿链接读写）。
#   下载成功后再回写该目录；回写失败只告警，不影响本次构建。
#   只缓存**原始 tar.gz**：不缓存解包目录、不缓存 b2 安装出的 prefix——prefix 字节
#   没有任何签名/哈希锚点，跨 run 复用等于把不可验的编译产物放进可信构建。
set -euo pipefail

BOOST_VERSION="1.90.0"
BOOST_SHA256="5e93d582aff26868d581a52ae78c7d8edf3f3064742c6e77901a1f18a437eea9"
BOOST_LIBS="context"
BOOST_VERSION_NUM=109000  # 1*100000 + 90*100 + 0

JOBS_DEFAULT=$(( $(nproc) / 4 )); [ "$JOBS_DEFAULT" -lt 2 ] && JOBS_DEFAULT=2

V_US="${BOOST_VERSION//./_}"
ARCHIVE_URL="https://archives.boost.io/release/${BOOST_VERSION}/source/boost_${V_US}.tar.gz"

usage() {
    cat <<'EOF'
用法：scripts/ci/prepare_boost.sh <empty-prefix> [--jobs N] [--print-plan]
  <empty-prefix>  目标安装前缀；必须不存在（fail-closed，不覆盖）。
  --jobs N        并行度，默认 nproc/4（最小 2）。
  --print-plan    只打印计划（URL/sha/组件），无副作用。
环境变量 BOOST_SOURCE_CACHE（可选）：源码归档缓存目录；仅复用通过锁定 sha256
  校验的 boost_1_90_0.tar.gz，损坏/不可读/不可写一律回退下载，不改变 fail-closed。
EOF
}

PREFIX=""
JOBS="$JOBS_DEFAULT"
PRINT_PLAN=0
while [ $# -gt 0 ]; do
    case "$1" in
        --jobs)
            [ $# -ge 2 ] || { echo "[prepare-boost] FATAL: --jobs 需要参数" >&2; exit 2; }
            JOBS="$2"; shift 2 ;;
        --print-plan) PRINT_PLAN=1; shift ;;
        -h|--help) usage; exit 0 ;;
        -*) echo "[prepare-boost] FATAL: 未知选项: $1" >&2; usage >&2; exit 2 ;;
        *) [ -z "$PREFIX" ] || { echo "[prepare-boost] FATAL: 多余参数: $1" >&2; exit 2; }
           PREFIX="$1"; shift ;;
    esac
done
case "$JOBS" in ''|*[!0-9]*) echo "[prepare-boost] FATAL: --jobs 必须为正整数" >&2; exit 2 ;; esac
[ "$JOBS" -ge 1 ] || { echo "[prepare-boost] FATAL: --jobs 必须为正整数" >&2; exit 2; }

if [ "$PRINT_PLAN" = 1 ]; then
    printf 'boost_version=%s\n' "$BOOST_VERSION"
    printf 'boost_sha256=%s\n' "$BOOST_SHA256"
    printf 'boost_libs=%s\n' "$BOOST_LIBS"
    printf 'boost_version_num=%s\n' "$BOOST_VERSION_NUM"
    printf 'archive_url=%s\n' "$ARCHIVE_URL"
    exit 0
fi

[ -n "$PREFIX" ] || { echo "[prepare-boost] FATAL: 缺少 <empty-prefix>" >&2; usage >&2; exit 2; }

# fail-closed：目标前缀已存在即拒绝，绝不覆盖。
if [ -e "$PREFIX" ] || [ -L "$PREFIX" ]; then
    echo "[prepare-boost] FATAL: 目标前缀已存在，拒绝覆盖: $PREFIX" >&2
    exit 2
fi
PREFIX="$(realpath -m "$PREFIX")"
if [ -e "$PREFIX" ] || [ -L "$PREFIX" ]; then
    echo "[prepare-boost] FATAL: 目标前缀已存在，拒绝覆盖: $PREFIX" >&2
    exit 2
fi

STAGING=""
WORK=""
cleanup() { [ -z "$STAGING" ] || rm -rf "$STAGING"; [ -z "$WORK" ] || rm -rf "$WORK"; }
trap cleanup EXIT INT TERM HUP

log() { printf '[prepare-boost] %s\n' "$*"; }

mkdir -p "$(dirname "$PREFIX")"
STAGING="$(mktemp -d "${PREFIX}.tmp.XXXXXX")"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/bbt-boost-src.XXXXXX")"

ARCHIVE_NAME="boost_${V_US}.tar.gz"
SOURCE_CACHE="${BOOST_SOURCE_CACHE:-}"
CACHE_HIT=0
# 根目录本身是符号链接时整条缓存不可信：读会沿链接取到别处，写也会沿链接落点，
# 直接整条忽略。条目级 symlink 另有检查（不跟随、不用）。
if [ -n "$SOURCE_CACHE" ] && [ -L "$SOURCE_CACHE" ]; then
    log "警告：BOOST_SOURCE_CACHE 是符号链接，忽略源码缓存: $SOURCE_CACHE"
    SOURCE_CACHE=""
fi
if [ -n "$SOURCE_CACHE" ]; then
    CACHED="$SOURCE_CACHE/$ARCHIVE_NAME"
    # 只接受普通文件：缓存条目是 symlink/目录/设备时一律忽略（不跟随、不用）。
    if [ -L "$CACHED" ] || { [ -e "$CACHED" ] && [ ! -f "$CACHED" ]; }; then
        log "源码缓存条目不是普通文件，忽略并重新下载: $CACHED"
    elif [ -f "$CACHED" ] \
        && printf '%s  %s\n' "$BOOST_SHA256" "$CACHED" | sha256sum --check --status; then
        if cp -- "$CACHED" "$WORK/boost.tar.gz" 2>/dev/null; then
            CACHE_HIT=1
            log "命中源码缓存（已按锁定 sha256 校验）: $CACHED"
        else
            log "源码缓存拷贝失败，回退下载: $CACHED"
        fi
    elif [ -e "$CACHED" ]; then
        log "源码缓存 sha256 不符，忽略并重新下载: $CACHED"
    fi
fi

if [ "$CACHE_HIT" = 0 ]; then
    log "下载 $ARCHIVE_URL（sha256 校验 fail-closed）"
    curl --fail --location --retry 3 --retry-all-errors --silent --show-error \
        "$ARCHIVE_URL" -o "$WORK/boost.tar.gz" \
        || { echo "[prepare-boost] FATAL: Boost 归档下载失败" >&2; exit 3; }
fi

# 命中与下载两条路径过同一道锁定 sha256 门：缓存内容本身不可信。
printf '%s  %s\n' "$BOOST_SHA256" "$WORK/boost.tar.gz" | sha256sum --check --status \
    || { echo "[prepare-boost] FATAL: Boost 归档 sha256 不符（拒绝继续）" >&2; exit 3; }

# 回写源码缓存：尽力而为。缓存目录不可写/回写失败只告警，本次构建继续。
if [ -n "$SOURCE_CACHE" ] && [ "$CACHE_HIT" = 0 ]; then
    CACHE_TMP="$SOURCE_CACHE/.${ARCHIVE_NAME}.$$.tmp"
    # 回写前再查一次根目录 symlink：恢复/下载期间目录可能才出现或被替换。
    if [ -L "$SOURCE_CACHE" ]; then
        log "警告：源码缓存目录是符号链接，跳过回写: $SOURCE_CACHE"
    elif mkdir -p "$SOURCE_CACHE" 2>/dev/null \
        && cp -- "$WORK/boost.tar.gz" "$CACHE_TMP" 2>/dev/null \
        && mv -f -- "$CACHE_TMP" "$SOURCE_CACHE/$ARCHIVE_NAME" 2>/dev/null; then
        log "已写入源码缓存: $SOURCE_CACHE/$ARCHIVE_NAME"
    else
        rm -f -- "$CACHE_TMP" 2>/dev/null || true
        log "警告：源码缓存不可写，跳过回写（不影响本次构建）"
    fi
fi

mkdir -p "$WORK/boost"
tar xzf "$WORK/boost.tar.gz" -C "$WORK/boost" --strip-components=1
cd "$WORK/boost"
log "bootstrap（--with-libraries=$BOOST_LIBS）"
./bootstrap.sh --prefix="$STAGING" --with-libraries="$BOOST_LIBS" >/dev/null
log "b2 install（-j$JOBS, release, shared+static）"
./b2 -j"$JOBS" variant=release link=shared,static threading=multi install >/dev/null

# 版本身份取自实际 version.hpp 内容，不由 lock/echo 自证。
VH="$STAGING/include/boost/version.hpp"
[ -f "$VH" ] || { echo "[prepare-boost] FATAL: 缺 Boost 版本头: $VH" >&2; exit 4; }
GOT="$(awk '$1=="#define" && $2=="BOOST_VERSION"{print $3; exit}' "$VH")"
[ "$GOT" = "$BOOST_VERSION_NUM" ] \
    || { echo "[prepare-boost] FATAL: Boost 版本不符: $VH BOOST_VERSION=$GOT 期望 $BOOST_VERSION_NUM" >&2; exit 4; }
[ -e "$STAGING/lib/libboost_context.so" ] \
    || { echo "[prepare-boost] FATAL: 缺 libboost_context.so（实际消费组件）" >&2; exit 4; }

# staging 就位后再原子改名，避免半成品前缀被当成已完成。
if [ -e "$PREFIX" ] || [ -L "$PREFIX" ]; then
    echo "[prepare-boost] FATAL: 准备期间目标前缀已出现，拒绝覆盖: $PREFIX" >&2
    exit 5
fi
mv -T "$STAGING" "$PREFIX"
STAGING=""
log "完成：BOOST_ROOT=$PREFIX（Boost $BOOST_VERSION，components=$BOOST_LIBS）"
