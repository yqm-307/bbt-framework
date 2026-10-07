#!/usr/bin/env bash
# 工具链身份探针（Issue #37 CI-T2）：证明当前环境确实提供锁定工具链，
# 且没有旧 /usr/local bbt 产物或系统 protoc 回退。fail-closed，非零即失败。
#
# 用法：
#   verify_toolchain.sh --in-image                  # 在 runner 镜像内自检（构建期/CI）
#   verify_toolchain.sh --prefix <protobuf-prefix>  # 校验一个 prepare_protobuf.sh 前缀
#   verify_toolchain.sh --image <docker-ref>        # 宿主机：docker run 后自检
# 选项：
#   --lock <file>   指定 toolchain.lock。缺省解析顺序（F4）：
#                   显式 --lock > $BBT_TOOLCHAIN_LOCK > 仓内 <script>/../docker/toolchain.lock
#                   > 镜像内 /usr/local/share/bbt/toolchain.lock。
#                   存在仓内 checkout 时优先它，避免镜像自带旧 lock 冒充当前仓契约。
#                   --image 模式会把解析到的宿主 lock 只读挂入容器并以它为准。
#
# 可配环境变量：BBT_TOOLCHAIN_BOOST_PREFIX（默认 /opt/boost）、
#   BBT_TOOLCHAIN_PROTOBUF_PREFIX（默认 /opt/protobuf-<PROTOC_VERSION>）。
set -euo pipefail

MODE=""
PREFIX=""
IMAGE=""
LOCK=""
BOOST_PREFIX="${BBT_TOOLCHAIN_BOOST_PREFIX:-/opt/boost}"

while [ $# -gt 0 ]; do
    case "$1" in
        --in-image) MODE="in-image"; shift ;;
        --prefix)   MODE="prefix"; PREFIX="${2:?--prefix 需要目录}"; shift 2 ;;
        --image)    MODE="image";  IMAGE="${2:?--image 需要镜像引用}"; shift 2 ;;
        --lock)     LOCK="${2:?--lock 需要文件}"; shift 2 ;;
        -h|--help)  sed -n '2,18p' "$0"; exit 0 ;;
        *) echo "[verify-toolchain] FATAL: 未知参数 $1" >&2; exit 2 ;;
    esac
done
[ -n "$MODE" ] || { echo "[verify-toolchain] FATAL: 需指定 --in-image/--prefix/--image" >&2; exit 2; }

# ---- 定位 lock（显式 --lock > 环境 > 仓内同级 > 镜像内置）---------------------
if [ -z "$LOCK" ]; then
    if [ -n "${BBT_TOOLCHAIN_LOCK:-}" ]; then
        LOCK="$BBT_TOOLCHAIN_LOCK"
    else
        _here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
        _repo_lock="$_here/../docker/toolchain.lock"
        if [ -f "$_repo_lock" ]; then
            LOCK="$_repo_lock"
        elif [ -f /usr/local/share/bbt/toolchain.lock ]; then
            LOCK=/usr/local/share/bbt/toolchain.lock
        else
            LOCK="$_repo_lock"
        fi
    fi
fi
[ -f "$LOCK" ] || { echo "[verify-toolchain] FATAL: 缺 toolchain.lock: $LOCK" >&2; exit 2; }

# 解析锁：不 source（禁止执行任意内容）；重复键 fail-closed，首值不再静默生效。
lock_dup="$(awk -F= '/^[A-Za-z_][A-Za-z0-9_]*=/ {c[$1]++} END {for (k in c) if (c[k]>1) print k}' "$LOCK" | sort | paste -sd, -)"
[ -z "$lock_dup" ] || { echo "[verify-toolchain] FATAL: lock 重复键: $lock_dup" >&2; exit 2; }
lock_get() { sed -n "s/^$1=//p" "$LOCK" | head -n1; }

# ---- 镜像模式：把宿主 lock 只读挂入容器，令镜像内探针以本次仓 lock 为准 ------
if [ "$MODE" = "image" ]; then
    lock_abs="$(cd "$(dirname "$LOCK")" && pwd)/$(basename "$LOCK")"
    exec docker run --rm \
        --mount "type=bind,src=$lock_abs,dst=/usr/local/share/bbt/verify-input.lock,ro" \
        -e BBT_TOOLCHAIN_LOCK=/usr/local/share/bbt/verify-input.lock \
        "$IMAGE" /usr/local/bin/verify-toolchain --in-image
fi

PROTOC_VERSION="$(lock_get PROTOC_VERSION)"
LIBPROTOBUF_A_SHA256="$(lock_get LIBPROTOBUF_A_SHA256)"
BOOST_VERSION="$(lock_get BOOST_VERSION)"
# 必填键（F3）：缺任一即 FATAL，不允许「少一个键即少一项校验」静默降级。
[ -n "$PROTOC_VERSION" ] || { echo "[verify-toolchain] FATAL: lock 缺 PROTOC_VERSION" >&2; exit 2; }
[ -n "$LIBPROTOBUF_A_SHA256" ] || { echo "[verify-toolchain] FATAL: lock 缺 LIBPROTOBUF_A_SHA256" >&2; exit 2; }
[ -n "$BOOST_VERSION" ] || { echo "[verify-toolchain] FATAL: lock 缺 BOOST_VERSION" >&2; exit 2; }

fail() { echo "[verify-toolchain] FAIL: $*" >&2; exit 1; }

# ---- 1) 旧 bbt 产物否决门禁（禁止旧 /usr/local bbt 抢绑） -------------------
old="$(ls /usr/local/lib/libbbt_* 2>/dev/null | head -n5 || true)"
[ -z "$old" ] || fail "发现 /usr/local 下旧 bbt 产物: $(echo "$old" | tr '\n' ' ')"

# ---- 2) protobuf 前缀与版本（禁止系统 protoc 回退） ------------------------
if [ "$MODE" = "prefix" ]; then
    proto_prefix="$PREFIX"
else
    proto_prefix="${BBT_TOOLCHAIN_PROTOBUF_PREFIX:-/opt/protobuf-${PROTOC_VERSION}}"
fi
protoc="$proto_prefix/usr/bin/protoc"
libdir="$proto_prefix/usr/lib/x86_64-linux-gnu"
[ -x "$protoc" ] || fail "缺锁定 protoc: $protoc（禁止系统 protoc 回退）"
ver="$(LD_LIBRARY_PATH="$libdir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" "$protoc" --version 2>/dev/null || true)"
[ "$ver" = "libprotoc $PROTOC_VERSION" ] || fail "protoc 版本不符: 期望 'libprotoc $PROTOC_VERSION' 实际 '$ver'"
[ -f "$libdir/libprotobuf.a" ] || fail "缺 $libdir/libprotobuf.a"
printf '%s  %s\n' "$LIBPROTOBUF_A_SHA256" "$libdir/libprotobuf.a" | sha256sum --check --status \
    || fail "libprotobuf.a checksum 不符"

# ---- 3) Boost 成品与版本（仅镜像自检；protobuf 前缀本身不含 Boost） ---------
if [ "$MODE" = "in-image" ]; then
    [ -f "$BOOST_PREFIX/lib/libboost_context.so" ] || fail "缺 Boost 成品: $BOOST_PREFIX/lib/libboost_context.so"
    # 版本身份取自实际 version.hpp 内容（F5）：不由 lock/echo 自证。
    vh="$BOOST_PREFIX/include/boost/version.hpp"
    [ -f "$vh" ] || fail "缺 Boost 版本头: $vh"
    got="$(awk '$1=="#define" && $2=="BOOST_VERSION"{print $3; exit}' "$vh")"
    bmajor="${BOOST_VERSION%%.*}"; brest="${BOOST_VERSION#*.}"
    bminor="${brest%%.*}"; bpatch="${brest#*.}"
    expect=$(( bmajor * 100000 + bminor * 100 + bpatch ))
    [ "$got" = "$expect" ] || fail "Boost 版本不符: $vh BOOST_VERSION=$got 期望 $expect (lock $BOOST_VERSION)"
fi

echo "[verify-toolchain] OK  mode=$MODE  $ver  boost=$BOOST_VERSION  prefix=$proto_prefix"
