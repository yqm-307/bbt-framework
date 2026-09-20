#!/usr/bin/env bash
# 按 deps.lock 准备 core/coroutine/infra 源码树。已在目标 SHA 上则跳过。
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCK="$REPO_DIR/deps.lock"
BBT_DEPS_DIR="${BBT_DEPS_DIR:-$REPO_DIR/../deps}"

if [ ! -f "$LOCK" ]; then
    echo "[fetch_deps] FATAL: 缺少 $LOCK" >&2
    exit 2
fi

mkdir -p "$BBT_DEPS_DIR"

# 末行无换行时 read 返回非零但变量已读到内容，|| [ -n ] 保证最后一条也处理。
while read -r name rest || [ -n "$name$rest" ]; do
    case "$name" in
        ''|'#'*) continue ;;
    esac
    repo=""; sha=""; dir=""
    for kv in $rest; do
        case "$kv" in
            repo=*) repo="${kv#repo=}" ;;
            sha=*)  sha="${kv#sha=}" ;;
            dir=*)  dir="${kv#dir=}" ;;
        esac
    done
    if [ -z "$name" ] || [ -z "$repo" ] || [ -z "$sha" ] || [ -z "$dir" ]; then
        echo "[fetch_deps] FATAL: 无法解析 lock 行: $name $rest" >&2
        exit 2
    fi
    dest="$BBT_DEPS_DIR/$dir"
    # 已是 git 工作树（clone 或 worktree，.git 可是目录或文件）→ fetch+checkout；
    # 否则（目录不存在）→ clone。worktree 的 .git 是文件，不能只看 -d。
    if git -C "$dest" rev-parse --git-dir >/dev/null 2>&1; then
        have="$(git -C "$dest" rev-parse HEAD 2>/dev/null || true)"
        if [ "$have" = "$sha" ]; then
            echo "[fetch_deps] $name 已在 $sha"
            continue
        fi
        echo "[fetch_deps] $name 更新 $have -> $sha"
        git -C "$dest" fetch --tags --force origin "$sha"
        git -C "$dest" checkout --detach "$sha"
    elif [ -e "$dest" ]; then
        echo "[fetch_deps] FATAL: $dest 已存在但不是 git 工作树，拒绝覆盖" >&2
        exit 4
    else
        echo "[fetch_deps] $name clone -> $dest"
        git clone --no-checkout "$repo" "$dest"
        git -C "$dest" fetch --tags --force origin "$sha"
        git -C "$dest" checkout --detach "$sha"
    fi
    have="$(git -C "$dest" rev-parse HEAD)"
    if [ "$have" != "$sha" ]; then
        echo "[fetch_deps] FATAL: $name HEAD=$have 期望 $sha" >&2
        exit 3
    fi
done < "$LOCK"

echo "[fetch_deps] 完成 BBT_DEPS_DIR=$BBT_DEPS_DIR"
