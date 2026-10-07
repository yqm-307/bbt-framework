#!/usr/bin/env bash
# examples/dual_service/run_demo.sh — Issue #5 阶段 2 一条路径验收：
# 真实启动两个服务进程 + 一个 driver 进程，覆盖正常/业务错误/服务端超时/
# 未路由/对端下线/优雅关闭六条路径，并独立校验后端残留，打印过程与退出码。
#
# svc_a 消费真实 Redis/Mongo 后端：地址经环境变量显式传入，缺失即失败——
# 禁止 KvStore 替身或静默跳过真实后端。空值同样失败（不被默认值掩盖）。
#   BBT_DEMO_REDIS_ADDR   默认 127.0.0.1:16379（设为空串 → 明确失败）
#   BBT_DEMO_MONGO_URI    默认 mongodb://127.0.0.1:27017/（设为空串 → 失败）
#
# 用法: run_demo.sh [build_dir]
#   build_dir 默认 <repo>/build-deps/project-build（scripts/local_build.sh
#   的产物位置）。脚本可重复执行；每轮只管理自己启动的进程。
#   BBT_DEMO_PORT_A/B      两进程监听端口，默认各自动态挑一个 loopback 空闲端口
#   BBT_DEMO_KEY_PREFIX    本轮 key 命名空间前缀；未设则脚本生成本轮唯一前缀。
#                          所有写入键带该前缀，driver 只删自己写入的键。
#   BBT_DEMO_LOG_DIR       日志目录；默认 <build_dir>/run-demo-logs/<本轮唯一名>，
#                          不落源码树、不落系统 /tmp。
#   BBT_DEMO_KEEP_LOGS=0   删除本轮**自己写出的那几个日志文件**（默认保留）；
#                          不递归删目录、不碰目录里其他人的文件。
#   BBT_DEMO_REDIS_CONTAINER / BBT_DEMO_MONGO_CONTAINER  可选：用于独立校验
#   本轮 key 已从后端清除（未提供则该项标 unverified，不冒充已验证）。
#   BBT_DEMO_STOP_GRACE    进程优雅关闭宽限秒数（非负整数，默认 10），超时升级
#                          SIGKILL；非法值按普通诊断退出（EX_USAGE=64）。
set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$EXAMPLE_DIR/../.." && pwd)"
BUILD_DIR="${1:-$REPO_DIR/build-deps/project-build}"

# 产物位置随 examples/dual_service/CMakeLists.txt 的 target 归属（默认
# BBT_BUILD_DIR=<repo>/build-deps/project-build）。
SVC_A="$BUILD_DIR/examples/dual_service/svc_a"
SVC_B="$BUILD_DIR/examples/dual_service/svc_b"
DRIVER="$BUILD_DIR/examples/dual_service/driver"
for b in "$SVC_A" "$SVC_B" "$DRIVER"; do
    [ -x "$b" ] || { echo "[demo] FATAL: $b 不存在；先按 examples/dual_service/README.md 以 -DBBT_ENABLE_DUAL_SERVICE_EXAMPLE=ON 构建" >&2; exit 2; }
done

# 优雅关闭宽限：必须是非负十进制整数——非法值不得进入算术比较（旧写法会让
# `[ "$waited" -ge "$STOP_GRACE" ]` 在运行期报语法错误甚至提前放行 KILL）。
STOP_GRACE="${BBT_DEMO_STOP_GRACE-10}"
case "$STOP_GRACE" in
    ''|*[!0-9]*)
        echo "[demo] FATAL: BBT_DEMO_STOP_GRACE 必须是非负整数，收到 \"$STOP_GRACE\"" >&2
        exit 64
        ;;
esac

# 真实后端地址（必需）：用 ${VAR-}（无冒号）而非 ${VAR:-}——显式设空串必须
# 触发下面的失败分支，不能被默认值悄悄掩盖成「看起来有地址」。
REDIS_ADDR="${BBT_DEMO_REDIS_ADDR-127.0.0.1:16379}"
MONGO_URI="${BBT_DEMO_MONGO_URI-mongodb://127.0.0.1:27017/}"
if [ -z "$REDIS_ADDR" ] || [ -z "$MONGO_URI" ]; then
    echo "[demo] FATAL: 真实后端地址为空（BBT_DEMO_REDIS_ADDR/BBT_DEMO_MONGO_URI 显式设空）" >&2
    exit 2
fi
echo "[demo] redis=$REDIS_ADDR mongo=$MONGO_URI"

# loopback 空闲端口：显式给就用，显式给空串即失败；未给则动态挑。
pick_free_port() {
    local p n
    for _ in $(seq 1 60); do
        n=$(( RANDOM % 20000 + 20000 ))
        if ! (exec 3<>"/dev/tcp/127.0.0.1/$n") 2>/dev/null; then
            echo "$n"; return 0
        fi
    done
    return 1
}
validate_port() {
    local name="$1" value="$2"
    case "$value" in
        ''|*[!0-9]*)
            echo "[demo] FATAL: $name 必须是 1..65535 的十进制端口" >&2
            return 2
            ;;
    esac
    if [ "$value" -lt 1 ] || [ "$value" -gt 65535 ]; then
        echo "[demo] FATAL: $name 必须是 1..65535 的十进制端口" >&2
        return 2
    fi
}
if [ -n "${BBT_DEMO_PORT_A+x}" ]; then
    PORT_A="${BBT_DEMO_PORT_A}"
    validate_port BBT_DEMO_PORT_A "$PORT_A" || exit $?
else
    PORT_A="$(pick_free_port)" || { echo "[demo] FATAL: 找不到空闲端口 A" >&2; exit 2; }
fi
if [ -n "${BBT_DEMO_PORT_B+x}" ]; then
    PORT_B="${BBT_DEMO_PORT_B}"
    validate_port BBT_DEMO_PORT_B "$PORT_B" || exit $?
else
    while :; do
        PORT_B="$(pick_free_port)" || { echo "[demo] FATAL: 找不到空闲端口 B" >&2; exit 2; }
        [ "$PORT_B" != "$PORT_A" ] && break
    done
fi

# 本轮 key 命名空间：每轮唯一，只碰自己的键，不覆盖共享后端的固定键。
KEY_PREFIX="${BBT_DEMO_KEY_PREFIX:-bbt-ds-$(date +%s)-$$-}"
export BBT_DEMO_KEY_PREFIX="$KEY_PREFIX"

# 日志持久化：默认写到构建目录下（不落源码树、不落系统 /tmp），便于独立回读。
# 文件名带本轮唯一 RUN_TAG：清理时只删本脚本写的这几个文件，不用 rm -rf（用户
# 给的 BBT_DEMO_LOG_DIR 里可能有别人的日志，递归删会误伤）。
RUN_TAG="$(date +%Y%m%d-%H%M%S)-$$"
if [ -n "${BBT_DEMO_LOG_DIR+x}" ]; then
    LOG_DIR="${BBT_DEMO_LOG_DIR}"
    [ -n "$LOG_DIR" ] || { echo "[demo] FATAL: BBT_DEMO_LOG_DIR 显式设空" >&2; exit 2; }
else
    LOG_DIR="$BUILD_DIR/run-demo-logs/$RUN_TAG"
fi
mkdir -p "$LOG_DIR"
LOG_SVC_A="$LOG_DIR/svc_a-$RUN_TAG.log"
LOG_SVC_B="$LOG_DIR/svc_b-$RUN_TAG.log"
LOG_DRIVER="$LOG_DIR/driver-$RUN_TAG.log"
LOG_DRIVER_DOWN="$LOG_DIR/driver-down-$RUN_TAG.log"
LOG_FILES=("$LOG_SVC_A" "$LOG_SVC_B" "$LOG_DRIVER" "$LOG_DRIVER_DOWN")
KEEP_LOGS="${BBT_DEMO_KEEP_LOGS-1}"

echo "[demo] binaries: $BUILD_DIR/examples/dual_service/{svc_a,svc_b,driver}"
echo "[demo] ports: A=$PORT_A B=$PORT_B  key_prefix=$KEY_PREFIX"
echo "[demo] logs: $LOG_DIR (files *-$RUN_TAG.log)"

PID_A=""; PID_B=""
STOP_RC=0    # stop_pid 记录被等待进程的真实退出码（供优雅关闭断言使用）

# 有界停止：先 TERM，最多等 STOP_GRACE 秒，仍在则 KILL。只作用于本脚本自己
# 起的 PID（不用 pkill/-f 模式匹配，避免误伤他人进程）。被等待进程的真实
# 退出码放在 STOP_RC：KILL 升级即 137，优雅关闭应为 0——断言据此判成败。
stop_pid() {
    local pid="$1" name="$2" waited_tenths=0 grace_tenths
    STOP_RC=0
    [ -n "$pid" ] || return 0
    if ! kill -0 "$pid" 2>/dev/null; then
        wait "$pid" 2>/dev/null || STOP_RC=$?
        return 0
    fi
    grace_tenths=$(( STOP_GRACE * 10 ))
    kill -TERM "$pid" 2>/dev/null || true
    while kill -0 "$pid" 2>/dev/null; do
        if [ "$waited_tenths" -ge "$grace_tenths" ]; then
            echo "[demo] WARN: $name(pid=$pid) ${STOP_GRACE}s 内未退出，升级 SIGKILL" >&2
            kill -KILL "$pid" 2>/dev/null || true
            break
        fi
        sleep 0.1; waited_tenths=$(( waited_tenths + 1 ))
    done
    wait "$pid" 2>/dev/null || STOP_RC=$?
}

cleanup() {
    local rc=$?
    stop_pid "$PID_A" svc_a
    stop_pid "$PID_B" svc_b
    if [ "$KEEP_LOGS" = "0" ]; then
        for f in "${LOG_FILES[@]}"; do
            rm -f -- "$f"
        done
    fi
    return $rc
}
trap cleanup EXIT

# ── 场景 0：两进程启动 ────────────────────────────────────────────────
echo
echo "== 0. start svc_a(storage :$PORT_A) + svc_b(gateway :$PORT_B) =="
"$SVC_A" "$PORT_A" "$REDIS_ADDR" "$MONGO_URI" \
    >"$LOG_SVC_A" 2>&1 &
PID_A=$!
"$SVC_B" "$PORT_B" "127.0.0.1:$PORT_A" >"$LOG_SVC_B" 2>&1 &
PID_B=$!

# 等监听就绪：轮询端口可连通（真实 TCP connect 作就绪判据）。就绪标志必须
# 由成功 connect 置位——循环耗尽仍未连通即 FATAL，不能默认宣称已就绪。
READY=0
for _ in $(seq 1 100); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$PORT_A") 2>/dev/null && \
       (exec 3<>"/dev/tcp/127.0.0.1/$PORT_B") 2>/dev/null; then
        READY=1
        break
    fi
    if ! kill -0 "$PID_A" 2>/dev/null || ! kill -0 "$PID_B" 2>/dev/null; then
        echo "[demo] FATAL: service exited before listen" >&2
        cat "$LOG_SVC_A" "$LOG_SVC_B" >&2
        exit 3
    fi
    sleep 0.05
done
if [ "$READY" -ne 1 ]; then
    echo "[demo] FATAL: 监听未就绪（5s 内两端口均未可连）" >&2
    cat "$LOG_SVC_A" "$LOG_SVC_B" >&2
    exit 3
fi
echo "[demo] both services listening"

# ── 场景 1..6：正常回路 / 业务错误 / 服务端超时 / 未路由 / 本轮 key 清理 ──
# 退出码取管道首段（driver 自己）的真实状态，tee 的退出码单独留证据：日志写不
# 下去时 driver 的 rc 会被 tee 的成功掩盖（旧写法 `cmd | tee || RC=$?` 取的是
# tee 的状态），这里两者都查、都不吞。
echo
echo "== 1..6. driver 正常+错误+服务端预算超时+未路由+清理 =="
DRIVER_RC=0; DRIVER_TEE_RC=0
set +e
"$DRIVER" "127.0.0.1:$PORT_B" | tee "$LOG_DRIVER"
driver_pipe_rc=("${PIPESTATUS[@]}")
set -e
DRIVER_RC="${driver_pipe_rc[0]}"
DRIVER_TEE_RC="${driver_pipe_rc[1]:-0}"
echo "[demo] driver rc=$DRIVER_RC tee rc=$DRIVER_TEE_RC"

# ── 场景 7：storage 下线 → gateway 调用失败路径 ──────────────────────
echo
echo "== 7. storage down -> gateway.fetch error path =="
# 先关停 storage，再调 fetch：连接拒绝应如实返回传输层错误而非悬挂。停机走
# 有界 stop_pid（TERM → STOP_GRACE 内未退即 KILL），不用无界 wait：否则
# svc_a 卡死会把这个脚本一起挂住，验收永远不落定。
stop_pid "$PID_A" svc_a
A_RC="$STOP_RC"; PID_A=""
echo "[demo] svc_a exit code: $A_RC"
DOWN_RC=0; DOWN_TEE_RC=0
set +e
"$DRIVER" "127.0.0.1:$PORT_B" --expect-storage-down | tee "$LOG_DRIVER_DOWN"
down_pipe_rc=("${PIPESTATUS[@]}")
set -e
DOWN_RC="${down_pipe_rc[0]}"
DOWN_TEE_RC="${down_pipe_rc[1]:-0}"
echo "[demo] driver(down) rc=$DOWN_RC tee rc=$DOWN_TEE_RC"

# ── 优雅关闭 gateway ─────────────────────────────────────────────────
echo
echo "== 8. SIGTERM svc_b -> graceful shutdown =="
stop_pid "$PID_B" svc_b
B_RC="$STOP_RC"; PID_B=""
echo "[demo] svc_b exit code: $B_RC"

# ── 独立校验：本轮 key 是否已从真实后端清除 ─────────────────────────
# driver 已在本轮注入的 del 场景只删自己的键；这里用调用方给的容器名独立复核
# 「后端确实没有残留」。工具/容器名不可得时如实标 unverified，不冒充已验证。
RESIDUE_NOTE="unverified (BBT_DEMO_REDIS_CONTAINER/BBT_DEMO_MONGO_CONTAINER 未设置或 docker 不可用)"
RESIDUE_OK=1
if command -v docker >/dev/null 2>&1 && \
   { [ -n "${BBT_DEMO_REDIS_CONTAINER:-}" ] || [ -n "${BBT_DEMO_MONGO_CONTAINER:-}" ]; }; then
    RESIDUE_NOTE=""
    if [ -n "${BBT_DEMO_REDIS_CONTAINER:-}" ]; then
        redis_keys=""
        if ! redis_keys="$(docker exec "$BBT_DEMO_REDIS_CONTAINER" redis-cli --scan \
                --pattern "${KEY_PREFIX}*" 2>/dev/null)"; then
            RESIDUE_NOTE="redis=unverified (独立扫描失败)"
            RESIDUE_OK=0
        else
            left=$(printf '%s\n' "$redis_keys" | sed '/^$/d' | wc -l | tr -d ' ')
            if [ "$left" = "0" ]; then
                RESIDUE_NOTE="redis=clean"
            else
                RESIDUE_NOTE="redis=LEFTOVER($left)"
                RESIDUE_OK=0
            fi
        fi
    fi
    if [ -n "${BBT_DEMO_MONGO_CONTAINER:-}" ]; then
        cnt=$(docker exec "$BBT_DEMO_MONGO_CONTAINER" mongosh --quiet \
                --eval "db.getSiblingDB('bbt_dual_service').kv.countDocuments({_id: {\$regex: '^${KEY_PREFIX}'}})" \
                2>/dev/null | tail -1 | tr -d ' ')
        if [ "$cnt" = "0" ]; then
            RESIDUE_NOTE="${RESIDUE_NOTE:+$RESIDUE_NOTE }mongo=clean"
        else
            RESIDUE_NOTE="${RESIDUE_NOTE:+$RESIDUE_NOTE }mongo=LEFTOVER($cnt)"
            RESIDUE_OK=0
        fi
    fi
    [ "$RESIDUE_OK" -eq 1 ] || echo "[demo] WARN: 后端存在本轮 key 残留：$RESIDUE_NOTE" >&2
fi

echo
echo "===== logs ====="
echo "--- svc_a ---"; cat "$LOG_SVC_A"
echo "--- svc_b ---"; cat "$LOG_SVC_B"

echo
echo "===== summary ====="
echo "driver rc=$DRIVER_RC (tee=$DRIVER_TEE_RC)  driver(down) rc=$DOWN_RC (tee=$DOWN_TEE_RC)"
echo "svc_a exit=$A_RC  svc_b exit=$B_RC"
echo "key_prefix=$KEY_PREFIX"
echo "backend residue check: $RESIDUE_NOTE"
echo "logs: $LOG_DIR (files *-$RUN_TAG.log)"

ok=1
[ "$DRIVER_RC" -eq 0 ] || { echo "driver reported failures"; ok=0; }
[ "$DOWN_RC"  -eq 0 ] || { echo "driver(down) reported failures"; ok=0; }
[ "$DRIVER_TEE_RC" -eq 0 ] || { echo "driver log tee failed"; ok=0; }
[ "$DOWN_TEE_RC" -eq 0 ] || { echo "driver(down) log tee failed"; ok=0; }
# kExitOk = 0：按期优雅关闭（HostLifecycle::Run 返回码，示例不引用
# internal 头故脚本按 0 判定）。
[ "$A_RC" -eq 0 ] || { echo "svc_a did not shut down cleanly"; ok=0; }
[ "$B_RC" -eq 0 ] || { echo "svc_b did not shut down cleanly"; ok=0; }
# 有容器名可查时，残留即失败（清理失败不得静默通过）。
[ "$RESIDUE_OK" -eq 1 ] || { echo "backend residue not clean"; ok=0; }

if [ "$ok" -eq 1 ]; then
    echo "[demo] PASS: all scenarios verified"
    # 只有真正验证过清理（或根本没有容器名可查且本轮无残留证据）才写 done；
    # 上面的 RESIDUE_OK 已挡住「清理失败仍宣称完成」。
    if [ "$RESIDUE_NOTE" = "unverified (BBT_DEMO_REDIS_CONTAINER/BBT_DEMO_MONGO_CONTAINER 未设置或 docker 不可用)" ]; then
        echo "[demo] backend cleanup: unverified（未提供容器名，未做独立复核）"
    else
        echo "[demo] backend cleanup: done ($RESIDUE_NOTE)"
    fi
    if [ "$KEEP_LOGS" = "0" ]; then
        echo "[demo] logs: 退出时只删本轮自己写出的 ${#LOG_FILES[@]} 个文件（目录与其他文件保留）"
    fi
else
    echo "[demo] FAIL" >&2
    exit 1
fi
