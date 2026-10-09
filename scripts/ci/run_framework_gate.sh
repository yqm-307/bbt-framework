#!/usr/bin/env bash
#
# run_framework_gate.sh —— bbt-framework hosted-only 普通 CI 影子门禁（Issue #50）的
# 构建/封包/接收侧校验配方。本脚本只被新增的 .github/workflows/ci-shadow-v1.yml 调用；
# 不改现役 .github/workflows/ci.yml、scripts/ 既有配方、产品代码或依赖锁。
#
# 与现役 ci.yml 的判据**等价**（同一 scripts/fetch_deps.sh、scripts/prepare_protobuf.sh、
# scripts/local_build.sh → build_stack.sh 配方、同一 dual-service 负向门禁、同一链接来源
# 证据），只是运行在 hosted ubuntu-24.04、无缓存，且 build 与 test 分处两个独立 runner，
# 构建树经 artifact（含锁定 Boost runtime）传递。测试侧 ctest/ldd/exec 位的命令与 ci.yml
# 逐项一致，由 scripts/ci/test_shadow_ci.py 的耦合测试对照，防漂移。
#
# 子命令：
#   build           配置+构建（scripts/local_build.sh，BBT_SKIP_CTEST=1）→ dual-service
#                   负向门禁 → 链接来源证据 → 经已发布 shared cli.py 产出 provenance
#                   （producer/source/run/attempt 审计）→ 打 tar（闭集成员）→ 输出真实
#                   archive sha256 与 provenance digest。
#   verify-archive  接收侧：按 build 侧输出 sha256 校验 → 闭集成员/路径穿越/symlink/设备
#                   成员 fail-closed → 安全解包到 dest（绝不越界写 host）。
#
# 用法：
#   run_framework_gate.sh build --work-dir D --deps-dir D --protobuf-prefix D \
#       --boost-prefix D --archive F --pack-root D --automation-sha 40hex [--jobs N] \
#       [--repo-dir D] [--job-id build] [--source-repo O/N] [--source-sha 40hex] \
#       [--automation-repo O/N] [--run-id N] [--run-attempt N]
#   run_framework_gate.sh verify-archive --archive F --expected-sha256 HEX --dest D \
#       --allow-prefix P [--allow-prefix P ...]
set -euo pipefail

log() { printf '[framework-gate] %s\n' "$*" >&2; }
die() { printf '[framework-gate] FATAL: %s\n' "$*" >&2; exit 2; }

usage() { sed -n '3,32p' "$0"; }

# ---------------- verify-archive（接收侧，纯 bash + python） ----------------
cmd_verify_archive() {
    local archive="" expected="" dest="" prefixes=()
    while [ $# -gt 0 ]; do
        case "$1" in
            --archive) archive="${2:?--archive 需要文件}"; shift 2 ;;
            --expected-sha256) expected="${2:?--expected-sha256 需要值}"; shift 2 ;;
            --dest) dest="${2:?--dest 需要目录}"; shift 2 ;;
            --allow-prefix) prefixes+=("${2:?--allow-prefix 需要值}"); shift 2 ;;
            -h|--help) usage; exit 0 ;;
            *) die "verify-archive 未知参数: $1" ;;
        esac
    done
    [ -n "$archive" ] || die "verify-archive 缺 --archive"
    [ -f "$archive" ] || die "verify-archive 归档不存在: $archive"
    [ -n "$dest" ] || die "verify-archive 缺 --dest"
    [ "${#prefixes[@]}" -ge 1 ] || die "verify-archive 至少需要一个 --allow-prefix"

    # fail-closed：期望摘要必须是 build 侧输出的 64 hex；缺失/非法一律拒绝，
    # 绝不退化为「无校验解包」。
    if ! printf '%s' "$expected" | grep -Eq '^[0-9a-f]{64}$'; then
        die "缺少或非法的 --expected-sha256（build 侧未传有效 archive 摘要）"
    fi
    printf '%s  %s\n' "$expected" "$archive" | sha256sum --check --strict - \
        || die "归档 sha256 与 build 侧输出不一致，拒绝解包"

    mkdir -p "$dest"
    local csv
    csv="$(IFS=,; printf '%s' "${prefixes[*]}")"
    python3 - "$archive" "$csv" "$dest" <<'PY'
import os
import posixpath
import shutil
import sys
import tarfile

archive, prefixes_csv, dest = sys.argv[1], sys.argv[2], sys.argv[3]
prefixes = [p for p in prefixes_csv.split(",") if p]


def normalize(name):
    """归一化成员名；绝对路径/反斜杠/`..`/空段一律返回 None（拒绝）。"""
    if not name or name.startswith("/") or "\\" in name:
        return None
    parts = name.rstrip("/").split("/")
    if any(part in ("", ".", "..") for part in parts):
        return None
    return "/".join(parts)


norm_prefixes = []
for p in prefixes:
    n = normalize(p)
    if n is None:
        sys.exit(f"[framework-gate] FATAL: 非法 --allow-prefix: {p!r}")
    norm_prefixes.append(n)


def under(rel):
    return any(rel == p or rel.startswith(p + "/") for p in norm_prefixes)


dest_real = os.path.realpath(dest)
with tarfile.open(archive, "r:gz") as tf:
    members = tf.getmembers()
    if not members:
        sys.exit("[framework-gate] FATAL: 归档为空")
    for m in members:
        name = normalize(m.name)
        if name is None:
            sys.exit(f"[framework-gate] FATAL: 非法成员名（绝对/穿越/空段）: {m.name!r}")
        if not under(name):
            sys.exit(f"[framework-gate] FATAL: 成员超出闭集: {m.name!r}")
        if m.isdir() or m.isreg():
            continue
        if m.issym():
            link = m.linkname
            if link.startswith("/") or "\\" in link:
                sys.exit(f"[framework-gate] FATAL: symlink 目标非法: {m.name} -> {link}")
            resolved = posixpath.normpath(
                posixpath.join(posixpath.dirname(name), link)
            )
            if resolved == ".." or resolved.startswith("../") or not under(resolved):
                sys.exit(f"[framework-gate] FATAL: symlink 越界: {m.name} -> {link}")
            continue
        # 设备/命名管道/硬链接等非闭集成员一律拒绝。
        sys.exit(f"[framework-gate] FATAL: 不支持/非闭集成员类型: {m.name!r}")

    for m in members:
        name = normalize(m.name)
        target = os.path.join(dest, name)
        real = os.path.realpath(target)
        if real != dest_real and not real.startswith(dest_real + os.sep):
            sys.exit(f"[framework-gate] FATAL: 解包越界写 host: {name!r}")
        if m.isdir():
            os.makedirs(target, exist_ok=True)
        elif m.isreg():
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with tf.extractfile(m) as src, open(target, "wb") as out:
                shutil.copyfileobj(src, out)
            os.chmod(target, m.mode & 0o777)
        elif m.issym():
            os.makedirs(os.path.dirname(target), exist_ok=True)
            if os.path.lexists(target):
                os.remove(target)
            os.symlink(m.linkname, target)
    print(f"[framework-gate] 归档校验通过并解包成员数={len(members)} -> {dest}", file=sys.stderr)
PY
}

# ---------------- build（构建侧） ----------------
cmd_build() {
    local repo_dir="" work_dir="" deps_dir="" protobuf_prefix="" boost_prefix=""
    local archive="" pack_root="" jobs="3" job_id="build"
    local source_repo="${GITHUB_REPOSITORY:-}" source_sha="${GITHUB_SHA:-}"
    local automation_repo="${GITHUB_REPOSITORY:-}" automation_sha=""
    local run_id="${GITHUB_RUN_ID:-}" run_attempt="${GITHUB_RUN_ATTEMPT:-}"
    while [ $# -gt 0 ]; do
        case "$1" in
            --repo-dir) repo_dir="${2:?}"; shift 2 ;;
            --work-dir) work_dir="${2:?}"; shift 2 ;;
            --deps-dir) deps_dir="${2:?}"; shift 2 ;;
            --protobuf-prefix) protobuf_prefix="${2:?}"; shift 2 ;;
            --boost-prefix) boost_prefix="${2:?}"; shift 2 ;;
            --archive) archive="${2:?}"; shift 2 ;;
            --pack-root) pack_root="${2:?}"; shift 2 ;;
            --jobs) jobs="${2:?}"; shift 2 ;;
            --job-id) job_id="${2:?}"; shift 2 ;;
            --source-repo) source_repo="${2:?}"; shift 2 ;;
            --source-sha) source_sha="${2:?}"; shift 2 ;;
            --automation-repo) automation_repo="${2:?}"; shift 2 ;;
            --automation-sha) automation_sha="${2:?}"; shift 2 ;;
            --run-id) run_id="${2:?}"; shift 2 ;;
            --run-attempt) run_attempt="${2:?}"; shift 2 ;;
            -h|--help) usage; exit 0 ;;
            *) die "build 未知参数: $1" ;;
        esac
    done
    [ -n "$repo_dir" ] || repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
    for kv in "work-dir=$work_dir" "deps-dir=$deps_dir" "protobuf-prefix=$protobuf_prefix" \
              "boost-prefix=$boost_prefix" "archive=$archive" "pack-root=$pack_root" \
              "automation-sha=$automation_sha"; do
        [ -n "${kv#*=}" ] || die "build 缺参数 --${kv%%=*}"
    done
    case "$jobs" in ''|*[!0-9]*) die "--jobs 必须为正整数" ;; esac
    [ -d "$protobuf_prefix" ] || die "protobuf 前缀不存在: $protobuf_prefix"
    [ -d "$boost_prefix" ] || die "Boost 前缀不存在: $boost_prefix"
    [ -d "$deps_dir/coroutine-main" ] || die "缺依赖源码树: $deps_dir/coroutine-main"
    [ -d "$deps_dir/infra-main" ] || die "缺依赖源码树: $deps_dir/infra-main"
    local project_build="$work_dir/project-build"
    mkdir -p "$work_dir" "$(dirname "$archive")"

    # 与现役 local_build.sh/build_stack.sh 同一配方：NEED_TEST=ON 编译本仓 tests+examples，
    # BBT_SKIP_CTEST=1 表示 ctest 由独立 test job 承担（与 ci.yml 的 build job 一致）。
    export BBT_NEED_TEST=ON
    export BBT_SKIP_CTEST=1
    export BBT_JOBS="$jobs"
    export BBT_DEPS_DIR="$deps_dir"
    export BBT_COROUTINE_SOURCE_DIR="$deps_dir/coroutine-main"
    export BBT_INFRA_SOURCE_DIR="$deps_dir/infra-main"
    export BBT_WORK_DIR="$work_dir"
    export BBT_BUILD_DIR="$project_build"
    export BOOST_ROOT="$boost_prefix"
    export CMAKE_PREFIX_PATH="$boost_prefix"
    # BBT_CMAKE_ARGS 经 build_stack.sh 透传给 cmake 配置（锁定 protobuf 前缀，
    # 使 bbt_infra_rpc 注册、examples/tests 的强制验收用例生效）。
    export BBT_CMAKE_ARGS="-DBBT_PROTOBUF_PREFIX=$protobuf_prefix ${BBT_CMAKE_ARGS:-}"

    log "构建（scripts/local_build.sh，JOBS=$jobs，protobuf=$protobuf_prefix，boost=$boost_prefix）"
    bash "$repo_dir/scripts/local_build.sh"

    # ---- 链接来源证据（与 ci.yml build 侧同口径；诊断走 stderr，stdout 只留 key=value）----
    test -f "$work_dir/deps-manifest.txt"
    test -f "$work_dir/link-sources.txt"
    cat "$work_dir/deps-manifest.txt" >&2
    cat "$work_dir/link-sources.txt" >&2
    if grep -q /usr/local/ "$work_dir/link-sources.txt"; then
        echo "[framework-gate] FATAL: 链接到 /usr/local" >&2
        exit 7
    fi

    # ---- dual-service 开关负向门禁（Issue #5，与 ci.yml 逐字一致）----
    grep -q '^BBT_ENABLE_DUAL_SERVICE_EXAMPLE:BOOL=OFF$' "$project_build/CMakeCache.txt" || {
        echo "[framework-gate] FATAL: 默认构建树未保持 BBT_ENABLE_DUAL_SERVICE_EXAMPLE=OFF" >&2
        exit 1
    }
    if [ -e "$project_build/examples/dual_service" ]; then
        echo "[framework-gate] FATAL: 默认构建生成了 examples/dual_service（应默认不构建）" >&2
        exit 1
    fi
    set +e
    cmake -S "$repo_dir" -B "$work_dir/ds-gate-negative" -G Ninja \
        -DNEED_TEST=ON \
        -DBBT_INFRA_SOURCE_DIR="$deps_dir/infra-main" \
        -DBBT_COROUTINE_SOURCE_DIR="$deps_dir/coroutine-main" \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DBBT_ENABLE_DUAL_SERVICE_EXAMPLE=ON \
        >"$work_dir/ds-gate-negative.log" 2>&1
    local rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
        echo "[framework-gate] FATAL: 缺前缀时 BBT_ENABLE_DUAL_SERVICE_EXAMPLE=ON 仍 configure 成功" >&2
        exit 1
    fi
    if ! grep -q '缺少前置条件' "$work_dir/ds-gate-negative.log" ||
       ! grep -q -- '-DBBT_HIREDIS_PREFIX' "$work_dir/ds-gate-negative.log"; then
        echo "[framework-gate] FATAL: configure 失败原因不是 dual_service 前置条件门禁" >&2
        tail -40 "$work_dir/ds-gate-negative.log" >&2
        exit 1
    fi
    log "dual-service gate: OFF 默认生效；缺前缀时 ON fail-closed (rc=$rc)"
    rm -rf "$work_dir/ds-gate-negative"

    # ---- provenance（复用已发布 shared artifact 契约，不新增第二 public API）----
    # 记录 producer/source/run/attempt/job 审计元数据；摘要由 shared 逻辑对真实写入字节计算，
    # 不接受外部传入摘要。随归档一起传递，接收侧用同一 shared cli.py 复核身份。
    local meta_dir="$work_dir/artifact-meta"
    local meta_in="$work_dir/.artifact-manifest-input.json"
    python3 - "$meta_in" "$source_repo" "$source_sha" "$automation_repo" \
        "$automation_sha" "$run_id" "$run_attempt" "$job_id" <<'PY'
import json
import sys

out, src_repo, src_sha, auto_repo, auto_sha, run_id, run_attempt, job = sys.argv[1:9]
for label, value in (("source_repo", src_repo), ("source_sha", src_sha),
                     ("automation_repo", auto_repo), ("automation_sha", auto_sha),
                     ("run_id", run_id), ("run_attempt", run_attempt), ("job", job)):
    if not str(value).strip():
        sys.exit(f"[framework-gate] FATAL: provenance 缺 {label}")
doc = {
    "manifest_version": 1,
    "source_repo": src_repo,
    "source_sha": src_sha,
    "source_head": src_sha,
    "automation_repo": auto_repo,
    "automation_sha": auto_sha,
    "run_id": int(run_id),
    "run_attempt": int(run_attempt),
    "job": job,
}
with open(out, "w", encoding="utf-8") as handle:
    json.dump(doc, handle, ensure_ascii=False, sort_keys=True)
PY
    local meta_out="$work_dir/.artifact-produce.out"
    python3 "$repo_dir/scripts/ci/shared/cli.py" produce \
        --file "$meta_in" --out-dir "$meta_dir" --github-output "$meta_out" >/dev/null
    local prov_digest
    prov_digest="$(grep '^artifact_digest=' "$meta_out" | head -1 | cut -d= -f2-)"
    rm -f "$meta_in" "$meta_out"
    printf '%s' "$prov_digest" | grep -Eq '^sha256:[0-9a-f]{64}$' \
        || die "provenance 摘要非法: $prov_digest"

    # ---- 打归档（闭集成员，路径相对 pack-root）----
    local rel_work rel_boost
    rel_work="$(python3 -c 'import os,sys;print(os.path.relpath(sys.argv[1],sys.argv[2]))' "$work_dir" "$pack_root")"
    rel_boost="$(python3 -c 'import os,sys;print(os.path.relpath(sys.argv[1],sys.argv[2]))' "$boost_prefix" "$pack_root")"
    case "$rel_work" in ../*|/*) die "work-dir 不在 pack-root 内: $rel_work" ;; esac
    case "$rel_boost" in ../*|/*) die "boost-prefix 不在 pack-root 内: $rel_boost" ;; esac
    [ -d "$boost_prefix/lib" ] || die "Boost 前缀缺 lib 目录: $boost_prefix/lib"
    log "打归档: $rel_work + $rel_boost/lib -> $archive"
    tar -czf "$archive" -C "$pack_root" "$rel_work" "$rel_boost/lib"

    local sha
    sha="$(sha256sum "$archive" | cut -d' ' -f1)"
    printf 'build_archive_sha256=%s\n' "$sha"
    printf 'provenance_digest=%s\n' "$prov_digest"
    printf 'producer_run_attempt=%s\n' "$run_attempt"
    printf 'archive_bytes=%s\n' "$(stat -c '%s' "$archive")"
    log "完成：archive=$archive sha256=$sha provenance=$prov_digest"
}

cmd="${1:-}"
[ -n "$cmd" ] || { usage >&2; exit 2; }
shift || true
case "$cmd" in
    build) cmd_build "$@" ;;
    verify-archive) cmd_verify_archive "$@" ;;
    -h|--help|help) usage; exit 0 ;;
    *) die "未知子命令: $cmd" ;;
esac
