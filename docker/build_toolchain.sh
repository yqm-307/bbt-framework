#!/usr/bin/env bash
# 工具链镜像构建入口（Issue #37 CI-T2）。只认 docker/toolchain.lock 的固定输入，
# 产出的镜像 ID 写入 manifest。只构建，不部署、不 import、不切换 runner
# （部署/切换权限与流程见 docker/README.md）。
#
# 用法：docker/build_toolchain.sh
# 可配：
#   BBT_COMMON_IMAGE_TAG / BBT_RUNNER_IMAGE_TAG  自有候选 tag（默认见下；
#                     刻意不覆盖共享 bbtools-common-image:v1 / bbtools-runner:v1）
#   BBT_TOOLCHAIN_OUT                            产物目录
#   BBT_HTTP_PROXY                               构建期代理（默认空 = noProxy；
#                     仅显式给出才注入，不假定本机恒有代理）
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCK="$REPO_DIR/docker/toolchain.lock"
OUT_DIR="${BBT_TOOLCHAIN_OUT:-$REPO_DIR/build-toolchain}"
# 自有候选 tag：旧共享 v1 产物保留作对照证据，不被覆盖。
COMMON_TAG="${BBT_COMMON_IMAGE_TAG:-bbt-p0a-ci-7c74c41d85c2-common:v2}"
RUNNER_TAG="${BBT_RUNNER_IMAGE_TAG:-bbt-p0a-ci-7c74c41d85c2-runner:v2}"

[ -f "$LOCK" ] || { echo "[build-toolchain] FATAL: 缺 $LOCK" >&2; exit 2; }

# 解析锁：不 source（禁止执行任意内容，锁不是可执行配置）；重复键 fail-closed
# （首个重复键静默生效 = 后写的新值被旧值遮蔽，F3）。
dup="$(awk -F= '/^[A-Za-z_][A-Za-z0-9_]*=/ {c[$1]++} END {for (k in c) if (c[k]>1) print k}' "$LOCK" | sort | paste -sd, -)"
[ -z "$dup" ] || { echo "[build-toolchain] FATAL: lock 重复键: $dup" >&2; exit 2; }
lock_get() { sed -n "s/^$1=//p" "$LOCK" | head -n1; }

COMMON_BASE_REF="$(lock_get COMMON_BASE_REF)"
COMMON_BASE_DIGEST="$(lock_get COMMON_BASE_DIGEST)"
RUNNER_BASE_REF="$(lock_get RUNNER_BASE_REF)"
RUNNER_BASE_DIGEST="$(lock_get RUNNER_BASE_DIGEST)"
BOOST_VERSION="$(lock_get BOOST_VERSION)"
BOOST_SHA256="$(lock_get BOOST_SHA256)"
PROTOC_VERSION="$(lock_get PROTOC_VERSION)"
LIBPROTOBUF_A_SHA256="$(lock_get LIBPROTOBUF_A_SHA256)"

# 每个必填键各自非空校验（F3）：拼接串非空不算过 —— 缺 *_REF 时 "@sha256:…"
# 仍非空会被放行。任一键缺失/清空即 FATAL。
for kv in \
    "COMMON_BASE_REF=$COMMON_BASE_REF" \
    "COMMON_BASE_DIGEST=$COMMON_BASE_DIGEST" \
    "RUNNER_BASE_REF=$RUNNER_BASE_REF" \
    "RUNNER_BASE_DIGEST=$RUNNER_BASE_DIGEST" \
    "BOOST_VERSION=$BOOST_VERSION" \
    "BOOST_SHA256=$BOOST_SHA256" \
    "PROTOC_VERSION=$PROTOC_VERSION" \
    "LIBPROTOBUF_A_SHA256=$LIBPROTOBUF_A_SHA256"; do
    if [ -z "${kv#*=}" ]; then
        echo "[build-toolchain] FATAL: lock 缺 ${kv%%=*}（或为空）" >&2
        exit 2
    fi
done

COMMON_BASE="$COMMON_BASE_REF@$COMMON_BASE_DIGEST"
RUNNER_BASE="$RUNNER_BASE_REF@$RUNNER_BASE_DIGEST"

mkdir -p "$OUT_DIR"

PROXY_ARGS=()
if [ -n "${BBT_HTTP_PROXY:-}" ]; then
    PROXY_ARGS=(--build-arg "HTTP_PROXY=$BBT_HTTP_PROXY" --build-arg "http_proxy=$BBT_HTTP_PROXY" \
                --build-arg "HTTPS_PROXY=$BBT_HTTP_PROXY" --build-arg "https_proxy=$BBT_HTTP_PROXY")
fi

echo "[build-toolchain] common-image <- $COMMON_BASE (boost $BOOST_VERSION)"
docker build --network=host \
    -f "$REPO_DIR/docker/bbtools-common-image/Dockerfile" \
    --build-arg "COMMON_BASE=$COMMON_BASE" \
    --build-arg "BOOST_VERSION=$BOOST_VERSION" \
    --build-arg "BOOST_SHA256=$BOOST_SHA256" \
    ${PROXY_ARGS[@]+"${PROXY_ARGS[@]}"} \
    -t "$COMMON_TAG" "$REPO_DIR"

# 以本次 common 的精确 imageID（daemon 实读，不是环境自报）作为 runner 的
# Boost 来源，消除悬浮 tag（F1）。
COMMON_ID="$(docker image inspect "$COMMON_TAG" --format '{{.Id}}')"
if [ -z "$COMMON_ID" ]; then
    echo "[build-toolchain] FATAL: 取不到本次 common imageID: $COMMON_TAG" >&2
    exit 2
fi

echo "[build-toolchain] runner <- $RUNNER_BASE (boost copy from $COMMON_ID)"
docker build --network=host \
    -f "$REPO_DIR/docker/bbtools-runner/Dockerfile" \
    --build-arg "RUNNER_BASE=$RUNNER_BASE" \
    --build-arg "COMMON_IMAGE=$COMMON_ID" \
    ${PROXY_ARGS[@]+"${PROXY_ARGS[@]}"} \
    -t "$RUNNER_TAG" "$REPO_DIR"

# 记录实际镜像身份（imageID 来自 daemon，不是环境自报）
img_id()  { docker image inspect "$1" --format '{{.Id}}'; }
img_rdg() { docker image inspect "$1" --format '{{join .RepoDigests ","}}' 2>/dev/null || true; }
{
    echo "# toolchain build manifest  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "common_tag=$COMMON_TAG"
    echo "common_image_id=$(img_id "$COMMON_TAG")"
    echo "common_repo_digests=$(img_rdg "$COMMON_TAG")"
    echo "runner_tag=$RUNNER_TAG"
    echo "runner_image_id=$(img_id "$RUNNER_TAG")"
    echo "runner_repo_digests=$(img_rdg "$RUNNER_TAG")"
    echo "runner_boost_from_common_image_id=$COMMON_ID"
    echo "common_base=$COMMON_BASE"
    echo "runner_base=$RUNNER_BASE"
    echo "boost_version=$BOOST_VERSION"
    echo "protoc_version=$PROTOC_VERSION"
    echo "libprotobuf_a_sha256=$LIBPROTOBUF_A_SHA256"
} | tee "$OUT_DIR/toolchain-build-manifest.txt"

echo "[build-toolchain] 运行镜像内自检"
docker run --rm "$RUNNER_TAG" /usr/local/bin/verify-toolchain --in-image
echo "[build-toolchain] OK  manifest=$OUT_DIR/toolchain-build-manifest.txt"
