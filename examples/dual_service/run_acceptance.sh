#!/usr/bin/env bash
# examples/dual_service/run_acceptance.sh — 双服务示例的资源验收入口。
#
# 这是本示例**唯一**的临时容器生命周期管理者：拉起 control-plane 之外的
# 真实 Redis / MongoDB 临时容器（仅 loopback 动态端口），跑 run_demo.sh 全场景，
# 再删除自己创建的容器与网络并复核已删除。不引入测试框架、不碰共享后端、
# 不写全局依赖、不改生产状态。
#
# 用法: run_acceptance.sh [build_dir]
#   build_dir 默认 <repo>/build-deps/project-build（传给 run_demo.sh）。
#
# 可覆盖变量（默认值即本地实测通过版本）：
#   BBT_DEMO_REDIS_IMAGE   默认 redis:7-alpine
#   BBT_DEMO_MONGO_IMAGE   默认 mongo:8.0
#   BBT_DEMO_NETWORK       默认 <本轮唯一名>；仅本脚本创建的私有 bridge
#   BBT_DEMO_ACCEPT_LOG_DIR 默认 <build_dir>/run-demo-logs
#   BBT_DEMO_EXPECT_CACHE_KEY  可选：把 recipe cache_key 钉到调用方给定的确切值
#                          （固定 recipe 由派生方这样钉版）。不填时仍然校验
#                          cache_key 与各组件 commit 互绑，脚本不硬编码 SHA。
#
# 资源身份校验（configure 记录的前缀 → recipe manifest → 实际产物）：
#   1) CMakeCache.txt 记录的 hiredis/mongoc/mongocxx 前缀必须同属一个 recipe 根；
#   2) <根>/resource-deps.manifest.json：schema、install_root、cmake_prefix_paths
#      必须与上面三者一致；components 恰为三组件且各有 40 位 commit/tag/upstream，
#      install_subdir 即组件名；cache_key 必须精确等于由三个 commit 前 8 位拼出的
#      bbt-resource-deps-hiredis-<c8>-mongoc-<c8>-mongocxx-<c8>（源身份与 recipe
#      互绑，脚本不自带构建配方）；每个已记录 .so 的实际 sha256 必须等于记录值；
#   3) svc_a 的 ldd：hiredis / libmongoc2 / libbson2 / libmongocxx1 / libbsoncxx1
#      逐个必须解析到对应私有前缀下，且输出不得含 not found。
#   读不到文件、命令失败或校验不过一律非零退出（fail closed），不降级成
#   「看起来通过」；不复制构建配方，只消费 manifest 记录的身份与摘要。
#
# 退出码：0 全场景 + 清理复核通过；非零表示任一环节失败（不吞错）。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="${1:-$REPO_DIR/build-deps/project-build}"

REDIS_IMAGE="${BBT_DEMO_REDIS_IMAGE-redis:7-alpine}"
MONGO_IMAGE="${BBT_DEMO_MONGO_IMAGE-mongo:8.0}"
RUN_TAG="$(date +%s)-$$"
NET="${BBT_DEMO_NETWORK-bbt-ds-accept-$RUN_TAG}"
REDIS_NAME="bbt-ds-redis-$RUN_TAG"
MONGO_NAME="bbt-ds-mongo-$RUN_TAG"
LOG_DIR="${BBT_DEMO_ACCEPT_LOG_DIR-$BUILD_DIR/run-demo-logs}"
mkdir -p "$LOG_DIR"
ACC_LOG="$LOG_DIR/acceptance-$RUN_TAG.log"

[ -x "$SCRIPT_DIR/run_demo.sh" ] || { echo "[accept] FATAL: run_demo.sh 不可执行" >&2; exit 2; }
command -v docker >/dev/null 2>&1 || { echo "[accept] FATAL: 需要 docker" >&2; exit 2; }

# 验收必须消费 infra recipe 产出的前缀，不能把旧的临时开发前缀误报成资源验收。
CACHE_FILE="$BUILD_DIR/CMakeCache.txt"
[ -f "$CACHE_FILE" ] || { echo "[accept] FATAL: 构建目录缺少 CMakeCache.txt: $CACHE_FILE" >&2; exit 2; }
cache_path() {
    sed -n "s#^$1:[^=]*=##p" "$CACHE_FILE" | head -1
}
HIREDIS_PREFIX="$(cache_path BBT_HIREDIS_PREFIX)"
MONGOC_PREFIX="$(cache_path BBT_MONGOC_PREFIX)"
MONGOCXX_PREFIX="$(cache_path BBT_MONGOCXX_PREFIX)"
[ -n "$HIREDIS_PREFIX" ] && [ -n "$MONGOC_PREFIX" ] && [ -n "$MONGOCXX_PREFIX" ] || {
    echo "[accept] FATAL: 构建未记录完整资源前缀（hiredis/mongoc/mongocxx）" >&2
    exit 2
}
RESOURCE_ROOT="$(dirname "$HIREDIS_PREFIX")"
[ "$MONGOC_PREFIX" = "$RESOURCE_ROOT/mongoc" ] &&
[ "$MONGOCXX_PREFIX" = "$RESOURCE_ROOT/mongocxx" ] || {
    echo "[accept] FATAL: 三个资源前缀不属于同一 recipe 根: $RESOURCE_ROOT" >&2
    exit 2
}
MANIFEST="$RESOURCE_ROOT/resource-deps.manifest.json"
[ -f "$MANIFEST" ] || {
    echo "[accept] FATAL: 缺少 recipe manifest: $MANIFEST" >&2
    exit 2
}
SVC_A="$BUILD_DIR/examples/dual_service/svc_a"
[ -x "$SVC_A" ] || { echo "[accept] FATAL: svc_a 不存在: $SVC_A" >&2; exit 2; }

# 源身份/摘要/实际装配三方一致才放行（见文件头第 2、3 条）。
if ! python3 - "$MANIFEST" "$RESOURCE_ROOT" "$HIREDIS_PREFIX" "$MONGOC_PREFIX" \
        "$MONGOCXX_PREFIX" "$SVC_A" "${BBT_DEMO_EXPECT_CACHE_KEY:-}" <<'PY'
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

manifest_path, root, hiredis, mongoc, mongocxx, svc_a, expect_key = sys.argv[1:8]

fail = []
def chk(cond, msg):
    if not cond:
        fail.append(msg)

def real(p):
    return os.path.realpath(p)

try:
    manifest = json.loads(Path(manifest_path).read_text())
except Exception as exc:                                  # noqa: BLE001
    print(f"[accept] FATAL: manifest 不可读或非法 JSON: {manifest_path}: {exc}",
          file=sys.stderr)
    sys.exit(2)

# ── 1) manifest 与 configure 记录的前缀必须指向同一 recipe 根 ──────────────
chk(manifest.get("schema") == "bbt-resource-deps/v1",
    "schema != bbt-resource-deps/v1")
chk(real(manifest.get("install_root", "")) == real(root),
    f"install_root 与 CMakeCache 前缀根不一致: {manifest.get('install_root')!r}")
chk([real(p) for p in manifest.get("cmake_prefix_paths", [])] ==
    [real(hiredis), real(mongoc), real(mongocxx)],
    "cmake_prefix_paths 与 CMakeCache 记录的三个前缀不一致")
abi = manifest.get("abi_requirements", {})
chk(abi.get("glibc") not in (None, "unknown"), "abi_requirements.glibc 缺失")
chk(abi.get("glibcxx") not in (None, "unknown"), "abi_requirements.glibcxx 缺失")

# ── 2) 组件身份从 manifest 读，但与 recipe cache_key 精确互绑 ───────────────
names = ("hiredis", "mongoc", "mongocxx")
by_name = {c.get("name"): c for c in manifest.get("components", [])}
chk(set(by_name) == set(names),
    "components 必须恰为 hiredis/mongoc/mongocxx，实际: "
    f"{sorted(k for k in by_name if k)}")
commit8 = {}
for name in names:
    comp = by_name.get(name, {})
    commit = comp.get("commit", "") or ""
    chk(re.fullmatch(r"[0-9a-f]{40}", commit) is not None,
        f"{name}.commit 不是 40 位小写 hex: {commit!r}")
    chk(bool(comp.get("tag")), f"{name} 缺少 tag")
    chk(bool(comp.get("upstream")), f"{name} 缺少 upstream")
    chk(comp.get("install_subdir") == name,
        f"{name}.install_subdir != {name}: {comp.get('install_subdir')!r}")
    commit8[name] = commit[:8]
expected_key = (
    "bbt-resource-deps-hiredis-{hiredis}-mongoc-{mongoc}-mongocxx-{mongocxx}"
    .format(**commit8))
cache_key = manifest.get("cache_key", "")
chk(cache_key == expected_key,
    f"cache_key 与组件 commit 不互绑: {cache_key!r} != {expected_key!r}")
if expect_key:
    chk(cache_key == expect_key,
        f"cache_key != BBT_DEMO_EXPECT_CACHE_KEY ({expect_key!r})")

# ── 3) 每个已记录 .so 的实际 sha256 必须等于记录值（读不到即失败） ──────────
libs_checked = 0
for name in names:
    for lib in by_name.get(name, {}).get("libs", []):
        lname = lib.get("name", "")
        digest = lib.get("sha256", "") or ""
        chk(re.fullmatch(r"[0-9a-f]{64}", digest) is not None,
            f"{name}/{lname} 记录的 sha256 非法: {digest!r}")
        path = Path(root) / name / "lib" / lname
        if not path.exists():
            fail.append(f"manifest 记录的库文件不存在: {path}")
            continue
        try:
            actual = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError as exc:
            fail.append(f"读取 {path} 失败: {exc}")
            continue
        libs_checked += 1
        chk(actual == digest, f"{path} sha256 与 manifest 记录不符")
chk(libs_checked > 0, "manifest 未记录任何库文件")

# ── 4) svc_a 实际加载：必需库逐个来自对应私有前缀，且无 not found ───────────
try:
    ldd = subprocess.run(["ldd", svc_a], capture_output=True, text=True,
                         timeout=60)
except OSError as exc:
    print(f"[accept] FATAL: 无法执行 ldd {svc_a}: {exc}", file=sys.stderr)
    sys.exit(2)
if ldd.returncode != 0:
    print(f"[accept] FATAL: ldd 失败(rc={ldd.returncode}): {ldd.stderr.strip()}",
          file=sys.stderr)
    sys.exit(2)
ldd_text = ldd.stdout + ldd.stderr
if "not found" in ldd_text:
    fail.append("ldd 报告存在 not found 依赖")
resolved = {}
for line in ldd_text.splitlines():
    m = re.match(r"\s*(\S+)\s+=>\s+(\S+)", line)
    if m:
        resolved.setdefault(m.group(1), []).append(m.group(2))
required = {
    "libhiredis.so": real(hiredis),
    "libmongoc2.so": real(mongoc),
    "libbson2.so": real(mongoc),
    "libmongocxx1.so": real(mongocxx),
    "libbsoncxx1.so": real(mongocxx),
}
for stem, prefix in required.items():
    paths = [p for lib, ps in resolved.items() if lib.startswith(stem) for p in ps]
    from_prefix = [p for p in paths if real(p).startswith(prefix + os.sep)]
    chk(bool(from_prefix),
        f"svc_a 未从 {prefix} 加载 {stem}*（实际: {paths or '未链接'}）")

if fail:
    for msg in fail:
        print(f"[accept] FATAL: 资源身份校验失败: {msg}", file=sys.stderr)
    sys.exit(2)
print(f"[accept] resource identity verified: cache_key={cache_key} "
      f"libs_sha256={libs_checked} svc_a_ldd=5/5 prefix-exact")
PY
then
    echo "[accept] FATAL: 资源身份校验失败（manifest/产物与 configure 记录不一致）" >&2
    exit 2
fi

echo "[accept] resource recipe root=$RESOURCE_ROOT manifest=$MANIFEST"

CREATED_NET=""; CREATED_REDIS=""; CREATED_MONGO=""

# 只清/只复核本脚本实际创建的对象（未创建过的不查不删，不误伤他人资源）。
# docker 删除或查询命令失败 = 无法证明已清理 = 失败：空输出/查询失败都不得当成
# 「没有残留」；复核失败时让退出码非零（teardown 的非零返回值会成为脚本退出码）。
teardown() {
    local rc=$? bad="" leftover="" names="" nets=""
    if [ -n "$CREATED_MONGO" ]; then
        if ! docker rm -f "$CREATED_MONGO" >/dev/null 2>&1; then
            bad="$bad rm-failed(container:$CREATED_MONGO)"
        fi
    fi
    if [ -n "$CREATED_REDIS" ]; then
        if ! docker rm -f "$CREATED_REDIS" >/dev/null 2>&1; then
            bad="$bad rm-failed(container:$CREATED_REDIS)"
        fi
    fi
    if [ -n "$CREATED_NET" ]; then
        if ! docker network rm "$CREATED_NET" >/dev/null 2>&1; then
            bad="$bad rm-failed(network:$CREATED_NET)"
        fi
    fi
    if [ -n "$CREATED_MONGO$CREATED_REDIS" ]; then
        if names="$(docker ps -a --format '{{.Names}}' 2>/dev/null)"; then
            for n in "$CREATED_MONGO" "$CREATED_REDIS"; do
                if [ -n "$n" ] && printf '%s\n' "$names" | grep -qx -- "$n"; then
                    leftover="$leftover container:$n"
                fi
            done
        else
            bad="$bad docker-ps-failed(无法复核自有容器)"
        fi
    fi
    if [ -n "$CREATED_NET" ]; then
        if nets="$(docker network ls --format '{{.Name}}' 2>/dev/null)"; then
            if printf '%s\n' "$nets" | grep -qx -- "$CREATED_NET"; then
                leftover="$leftover network:$CREATED_NET"
            fi
        else
            bad="$bad docker-network-ls-failed(无法复核自有网络)"
        fi
    fi
    if [ -n "$bad" ]; then
        echo "[accept] FATAL: teardown 未能证明清理完成:$bad" >&2
        rc=1
    fi
    if [ -n "$leftover" ]; then
        echo "[accept] FATAL: 自有对象未被清除:$leftover" >&2
        rc=1
    fi
    if [ -z "$bad$leftover" ]; then
        echo "[accept] teardown verified: 本轮自建容器/网络已全部移除且复核命令成功"
    fi
    return $rc
}
trap teardown EXIT

echo "[accept] redis image=$REDIS_IMAGE mongo image=$MONGO_IMAGE"
echo "[accept] log=$ACC_LOG"

# 私有网络 + 动态 loopback 端口（主机端口由 docker 分配，仅绑 127.0.0.1）。
docker network create "$NET" >/dev/null
CREATED_NET="$NET"

docker run -d --name "$REDIS_NAME" --network "$NET" \
    -p 127.0.0.1::6379 "$REDIS_IMAGE" >/dev/null
CREATED_REDIS="$REDIS_NAME"
docker run -d --name "$MONGO_NAME" --network "$NET" \
    -p 127.0.0.1::27017 "$MONGO_IMAGE" >/dev/null
CREATED_MONGO="$MONGO_NAME"

REDIS_PORT=""; MONGO_PORT=""
redis_port_out="$(docker port "$REDIS_NAME" 6379/tcp 2>/dev/null)" || {
    echo "[accept] FATAL: docker port($REDIS_NAME) 失败" >&2; exit 3; }
mongo_port_out="$(docker port "$MONGO_NAME" 27017/tcp 2>/dev/null)" || {
    echo "[accept] FATAL: docker port($MONGO_NAME) 失败" >&2; exit 3; }
REDIS_PORT="$(printf '%s\n' "$redis_port_out" | head -1 | sed 's/.*://')"
MONGO_PORT="$(printf '%s\n' "$mongo_port_out" | head -1 | sed 's/.*://')"
[ -n "$REDIS_PORT" ] && [ -n "$MONGO_PORT" ] || {
    echo "[accept] FATAL: 未取到动态端口（redis=$REDIS_PORT mongo=$MONGO_PORT）" >&2
    exit 3
}
echo "[accept] redis=127.0.0.1:$REDIS_PORT mongo=127.0.0.1:$MONGO_PORT"

# 就绪判据：真实命令往返（不是 sleep）。
for _ in $(seq 1 60); do
    docker exec "$REDIS_NAME" redis-cli ping >/dev/null 2>&1 && break
    sleep 0.5
done
docker exec "$REDIS_NAME" redis-cli ping >/dev/null 2>&1 || {
    echo "[accept] FATAL: redis 未就绪" >&2; exit 3; }
for _ in $(seq 1 90); do
    docker exec "$MONGO_NAME" mongosh --quiet --eval 'db.runCommand({ping:1}).ok' \
        >/dev/null 2>&1 && break
    sleep 1
done
docker exec "$MONGO_NAME" mongosh --quiet --eval 'db.runCommand({ping:1}).ok' \
    >/dev/null 2>&1 || { echo "[accept] FATAL: mongo 未就绪" >&2; exit 3; }
echo "[accept] backends ready"

# 走 run_demo.sh，同时取回真实退出码与 tee 退出码：日志写失败（tee 非零）不得
# 被当成「过程已记录」的通过。PIPESTATUS 必须在管道后的第一条命令里整份取出。
set +e
BBT_DEMO_REDIS_ADDR="127.0.0.1:$REDIS_PORT" \
BBT_DEMO_MONGO_URI="mongodb://127.0.0.1:$MONGO_PORT/" \
BBT_DEMO_REDIS_CONTAINER="$REDIS_NAME" \
BBT_DEMO_MONGO_CONTAINER="$MONGO_NAME" \
    "$SCRIPT_DIR/run_demo.sh" "$BUILD_DIR" 2>&1 | tee "$ACC_LOG"
pipe_rc=("${PIPESTATUS[@]}")
set -e
ACC_RC="${pipe_rc[0]}"
ACC_TEE_RC="${pipe_rc[1]:-0}"

if [ "$ACC_TEE_RC" -ne 0 ]; then
    echo "[accept] FATAL: 验收日志写入失败（tee rc=$ACC_TEE_RC，日志 $ACC_LOG）" >&2
    exit 3
fi
if [ "$ACC_RC" -ne 0 ]; then
    echo "[accept] FAIL: run_demo.sh rc=$ACC_RC（日志 $ACC_LOG）" >&2
    exit "$ACC_RC"
fi
echo "[accept] PASS: demo 全场景通过（run_demo rc=0 tee rc=0），后端残留已复核"
