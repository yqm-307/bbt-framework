#!/usr/bin/env bash
# 双服务资源 runner 候选镜像构建入口（resource-job）。只认 docker/resource-runner.lock，
# 产出的镜像身份、资源前缀 manifest、ABI/版本/ldd 证据写入产物目录。
# 只构建，不部署、不 import、不切换 runner、不 publish registry（发布另行授权）。
#
# 用法：docker/build_resource_runner.sh
# 可配：
#   BBT_RESOURCE_TAG            候选 tag（默认 bbt-dual-resource:20261007-candidate）
#   BBT_RESOURCE_OUT            产物目录（默认 <repo>/build-resource，已被 .gitignore 忽略）
#   BBT_INFRA_SOURCE_DIR        离线可用的 bbtools-infra 检出（须 HEAD==锁定 commit）；
#                               未给则从 INFRA_REPO 只抓锁定 commit（需网络）
#   BBT_HTTP_PROXY              构建期代理（默认空 = noProxy；git clone 上游需要时显式给出）
#   BBT_RESOURCE_JOBS           并行度（默认取 lock，上限 3）
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCK="$REPO_DIR/docker/resource-runner.lock"
OUT_DIR="${BBT_RESOURCE_OUT:-$REPO_DIR/build-resource}"
TAG="${BBT_RESOURCE_TAG:-bbt-dual-resource:20261007-candidate}"

[ -f "$LOCK" ] || { echo "[build-resource] FATAL: 缺 $LOCK" >&2; exit 2; }

# 解析锁：不 source（禁止执行任意内容）；重复键 fail-closed。
dup="$(awk -F= '/^[A-Za-z_][A-Za-z0-9_]*=/ {c[$1]++} END {for (k in c) if (c[k]>1) print k}' "$LOCK" | sort | paste -sd, -)"
[ -z "$dup" ] || { echo "[build-resource] FATAL: lock 重复键: $dup" >&2; exit 2; }
lock_get() { sed -n "s/^$1=//p" "$LOCK" | head -n1; }

BASE_IMAGE_ID="$(lock_get RESOURCE_RUNNER_BASE_IMAGE_ID)"
BASE_REGISTRY_REF="$(lock_get RESOURCE_RUNNER_BASE_REGISTRY_REF)"
BASE_TAG_HINT="$(lock_get RESOURCE_RUNNER_BASE_TAG_HINT)"
INFRA_REPO="$(lock_get INFRA_REPO)"
INFRA_COMMIT="$(lock_get INFRA_COMMIT)"
INFRA_RECIPE_PATH="$(lock_get INFRA_RECIPE_PATH)"
INFRA_VERIFY_PATH="$(lock_get INFRA_VERIFY_PATH)"
PREFIX_ROOT="$(lock_get RESOURCE_PREFIX_ROOT)"
LOCK_JOBS="$(lock_get RESOURCE_JOBS)"
HIREDIS_COMMIT="$(lock_get HIREDIS_COMMIT)"
MONGOC_COMMIT="$(lock_get MONGOC_COMMIT)"
MONGOCXX_COMMIT="$(lock_get MONGOCXX_COMMIT)"
CACHE_KEY="$(lock_get RESOURCE_DEPS_CACHE_KEY)"
PROTOC_VERSION="$(lock_get PROTOC_VERSION)"
LIBPROTOBUF_A_SHA256="$(lock_get LIBPROTOBUF_A_SHA256)"

# 必填键各自非空校验（拼接串非空不算过）。
for kv in \
    "RESOURCE_RUNNER_BASE_IMAGE_ID=$BASE_IMAGE_ID" \
    "INFRA_REPO=$INFRA_REPO" \
    "INFRA_COMMIT=$INFRA_COMMIT" \
    "INFRA_RECIPE_PATH=$INFRA_RECIPE_PATH" \
    "INFRA_VERIFY_PATH=$INFRA_VERIFY_PATH" \
    "RESOURCE_PREFIX_ROOT=$PREFIX_ROOT" \
    "HIREDIS_COMMIT=$HIREDIS_COMMIT" \
    "MONGOC_COMMIT=$MONGOC_COMMIT" \
    "MONGOCXX_COMMIT=$MONGOCXX_COMMIT" \
    "RESOURCE_DEPS_CACHE_KEY=$CACHE_KEY" \
    "PROTOC_VERSION=$PROTOC_VERSION" \
    "LIBPROTOBUF_A_SHA256=$LIBPROTOBUF_A_SHA256"; do
    if [ -z "${kv#*=}" ]; then
        echo "[build-resource] FATAL: lock 缺 ${kv%%=*}（或为空）" >&2
        exit 2
    fi
done

JOBS="${BBT_RESOURCE_JOBS:-${LOCK_JOBS:-3}}"
case "$JOBS" in ''|*[!0-9]*) echo "[build-resource] FATAL: JOBS 必须为正整数" >&2; exit 2 ;; esac
[ "$JOBS" -ge 1 ] || { echo "[build-resource] FATAL: JOBS 必须为正整数" >&2; exit 2; }
[ "$JOBS" -le 3 ] || { echo "[build-resource] FATAL: JOBS 上限 3（避免宿主压力），得到 $JOBS" >&2; exit 2; }

# ---- 基座身份核对：daemon 实读 imageID，不用 tag/env 自报 ----------------------
actual_id="$(docker image inspect "$BASE_IMAGE_ID" --format '{{.Id}}' 2>/dev/null || true)"
[ "$actual_id" = "$BASE_IMAGE_ID" ] || {
    echo "[build-resource] FATAL: 基座 imageID 不符：期望 $BASE_IMAGE_ID 实读 '$actual_id'" >&2
    exit 2
}
if [ -n "$BASE_TAG_HINT" ]; then
    hint_id="$(docker image inspect "$BASE_TAG_HINT" --format '{{.Id}}' 2>/dev/null || true)"
    [ "$hint_id" = "$BASE_IMAGE_ID" ] || {
        echo "[build-resource] FATAL: tag 提示 $BASE_TAG_HINT 未解析到锁定 imageID（实读 '$hint_id'）" >&2
        exit 2
    }
fi

# FROM 输入：正式区分 imageID / registry ref。registry ref 缺失时用已核对的 imageID
# 作本地构建输入（本地构建器可用）；禁止拿 imageID 冒充 registry ref。
case "$BASE_REGISTRY_REF" in
    ''|unavailable)
        FROM_BASE="$BASE_IMAGE_ID"
        BASE_INPUT_KIND=imageID
        ;;
    *)
        FROM_BASE="$BASE_REGISTRY_REF"
        BASE_INPUT_KIND=registry-ref
        ;;
esac

mkdir -p "$OUT_DIR"
CTX="$OUT_DIR/context"
BUILD_LOG="$OUT_DIR/resource-runner-build.log"
rm -rf "$CTX"
mkdir -p "$CTX/infra"

# ---- 落盘 bbtools-infra 固定 commit 的 recipe + 消费者自检 --------------------
INFRA_SRC=""
CLONED=""
if [ -n "${BBT_INFRA_SOURCE_DIR:-}" ]; then
    INFRA_SRC="$BBT_INFRA_SOURCE_DIR"
    head="$(git -C "$INFRA_SRC" rev-parse HEAD 2>/dev/null || true)"
    [ "$head" = "$INFRA_COMMIT" ] || {
        echo "[build-resource] FATAL: BBT_INFRA_SOURCE_DIR HEAD=$head != 锁定 $INFRA_COMMIT" >&2
        exit 2
    }
else
    INFRA_SRC="$OUT_DIR/infra-src"
    rm -rf "$INFRA_SRC"
    mkdir -p "$INFRA_SRC"
    echo "[build-resource] 从 $INFRA_REPO 抓锁定 commit $INFRA_COMMIT"
    git -C "$INFRA_SRC" init -q
    git -C "$INFRA_SRC" remote add origin "$INFRA_REPO"
    GIT_TERMINAL_PROMPT=0 git -C "$INFRA_SRC" fetch --depth 1 origin "$INFRA_COMMIT"
    CLONED=1
fi
git -C "$INFRA_SRC" rev-parse --verify --quiet "${INFRA_COMMIT}^{commit}" >/dev/null || {
    echo "[build-resource] FATAL: $INFRA_SRC 不含锁定 commit $INFRA_COMMIT" >&2
    exit 2
}
# 用 git archive 精确取该 commit 的内容（不复制本仓外的下载/构建逻辑）。
git -C "$INFRA_SRC" archive "$INFRA_COMMIT" "$INFRA_RECIPE_PATH" "$INFRA_VERIFY_PATH" \
    | tar -x -C "$CTX/infra"
[ -f "$CTX/infra/$INFRA_RECIPE_PATH" ] || { echo "[build-resource] FATAL: recipe 未落盘" >&2; exit 2; }
[ -d "$CTX/infra/$INFRA_VERIFY_PATH" ] || { echo "[build-resource] FATAL: verifier 未落盘" >&2; exit 2; }

# ---- 交叉核对 recipe 派生 cache_key（防止 lock 与 recipe 漂移） ----------------
recipe_key="$(bash "$CTX/infra/$INFRA_RECIPE_PATH" --print-cache-key)"
[ "$recipe_key" = "$CACHE_KEY" ] || {
    echo "[build-resource] FATAL: recipe cache_key=$recipe_key != lock $CACHE_KEY" >&2
    exit 2
}

PROXY_ARGS=()
if [ -n "${BBT_HTTP_PROXY:-}" ]; then
    PROXY_ARGS=(--build-arg "HTTP_PROXY=$BBT_HTTP_PROXY")
fi

echo "[build-resource] build <- base=$FROM_BASE ($BASE_INPUT_KIND)  jobs=$JOBS  tag=$TAG"
set +e
docker build --network=host --memory 4g \
    -f "$REPO_DIR/docker/bbtools-resource-runner/Dockerfile" \
    --build-arg "RUNNER_BASE=$FROM_BASE" \
    --build-arg "RESOURCE_JOBS=$JOBS" \
    --build-arg "PROTOC_VERSION=$PROTOC_VERSION" \
    --build-arg "LIBPROTOBUF_A_SHA256=$LIBPROTOBUF_A_SHA256" \
    ${PROXY_ARGS[@]+"${PROXY_ARGS[@]}"} \
    -t "$TAG" "$CTX" 2>&1 | tee "$BUILD_LOG"
rc=${PIPESTATUS[0]}
set -e
[ "$rc" -eq 0 ] || { echo "[build-resource] FATAL: docker build rc=$rc（见 $BUILD_LOG）" >&2; exit "$rc"; }

# ---- 产物身份与证据（daemon 实读，不用环境自报） ------------------------------
img_id()  { docker image inspect "$1" --format '{{.Id}}'; }
img_rdg() { docker image inspect "$1" --format '{{join .RepoDigests ","}}' 2>/dev/null || true; }

IMAGE_ID="$(img_id "$TAG")"
REPO_DIGESTS="$(img_rdg "$TAG")"
case "$REPO_DIGESTS" in
    *"@sha256:"*)
        # 只有形如 repo@sha256:<manifest-digest> 且 **不等于 config imageID** 才是
        # 真实 registry manifest digest；本地构建产物无此值。
        rd_digest="${REPO_DIGESTS##*@}"
        if [ "$rd_digest" = "$IMAGE_ID" ]; then
            RDG_STATUS="local-synthetic(=imageID, 非 registry manifest digest)"
        else
            RDG_STATUS="present"
        fi
        ;;
    *) RDG_STATUS="absent(本地构建，未 publish registry)" ;;
esac

# 前缀 manifest 与运行时证据（容器默认以基座 User=runner 跑，等价 job 运行态）。
docker run --rm --entrypoint bash "$TAG" -lc '
    set -e
    echo "== id =="; id -un; echo "== prefix manifest =="
    cat '"$PREFIX_ROOT"'/resource-deps.manifest.json
    echo "== prefix libs (sha256) =="
    find '"$PREFIX_ROOT"' -path "*/lib/*.so*" -type f | sort | while read -r f; do
        printf "%s  %s\n" "$(sha256sum "$f" | awk "{print \$1}")" "$f"
    done
    echo "== RUNPATH (hiredis .so) =="
    objdump -x '"$PREFIX_ROOT"'/hiredis/lib/libhiredis.so | grep -E "RUNPATH|RPATH" || true
    echo "== ldd prefix libs =="
    find '"$PREFIX_ROOT"' -path "*/lib/*.so*" -type f | sort | while read -r f; do
        echo "-- $f"; ldd "$f" 2>&1 | grep -E "not found|=> /" || true
    done
    echo "== protobuf =="
    /opt/protobuf-'"$PROTOC_VERSION"'/usr/bin/protoc --version
    sha256sum /opt/protobuf-'"$PROTOC_VERSION"'/usr/lib/x86_64-linux-gnu/libprotobuf.a
' > "$OUT_DIR/final-runtime-evidence.txt" 2>&1

# 基座适用的工具链身份断言（只取对本基座成立的两三项；不代表 v2 lock 整体满足）。
set +e
bash "$REPO_DIR/scripts/verify_toolchain.sh" --image "$TAG" \
    > "$OUT_DIR/verify-toolchain-applicable.txt" 2>&1
vt_rc=$?
set -e

[ "$vt_rc" -eq 0 ] || {
    echo "[build-resource] FATAL: verify-toolchain rc=$vt_rc（见 $OUT_DIR/verify-toolchain-applicable.txt）" >&2
    exit "$vt_rc"
}

{
    echo "# 资源 runner 构建 manifest  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "tag=$TAG"
    echo "image_id=$IMAGE_ID"
    echo "repo_digests=$REPO_DIGESTS"
    echo "registry_manifest_digest=$RDG_STATUS"
    echo "base_image_id=$BASE_IMAGE_ID"
    echo "base_registry_ref=$BASE_REGISTRY_REF"
    echo "base_input_kind=$BASE_INPUT_KIND"
    echo "base_tag_hint=$BASE_TAG_HINT"
    echo "infra_repo=$INFRA_REPO"
    echo "infra_commit=$INFRA_COMMIT"
    echo "resource_prefix_root=$PREFIX_ROOT"
    echo "resource_jobs=$JOBS"
    echo "resource_deps_cache_key=$CACHE_KEY"
    echo "hiredis_commit=$HIREDIS_COMMIT"
    echo "mongoc_commit=$MONGOC_COMMIT"
    echo "mongocxx_commit=$MONGOCXX_COMMIT"
    echo "protoc_version=$PROTOC_VERSION"
    echo "libprotobuf_a_sha256=$LIBPROTOBUF_A_SHA256"
    echo "verify_toolchain_applicable_rc=$vt_rc"
} | tee "$OUT_DIR/resource-runner-build-manifest.txt"

[ "$CLONED" = "1" ] && rm -rf "$INFRA_SRC"
echo "[build-resource] OK  manifest=$OUT_DIR/resource-runner-build-manifest.txt"
echo "[build-resource] 证据：$OUT_DIR/{resource-runner-build.log,final-runtime-evidence.txt,verify-toolchain-applicable.txt}"
