#!/usr/bin/env bash
# examples/dual_service/run_demo.sh — Issue #5 阶段 2 一条路径验收：
# 真实启动两个服务进程 + 一个 driver 进程，覆盖正常/业务错误/超时/
# 未路由/对端下线/优雅关闭六条路径，打印过程并校验退出码。
#
# svc_a 消费真实 Redis/Mongo 后端：地址经环境变量显式传入，缺失即失败——
# 禁止 KvStore 替身或静默跳过真实后端。
#   BBT_DEMO_REDIS_ADDR   默认 127.0.0.1:16379
#   BBT_DEMO_MONGO_URI    默认 mongodb://127.0.0.1:27017/
#   BBT_DEMO_KEEP_LOGS=1  保留日志目录（默认也保留到持久位置）
#
# 用法: run_demo.sh [build_dir]
#   build_dir 默认 <repo>/build-deps/project-build（scripts/local_build.sh
#   的产物位置）。脚本可重复执行；残留进程先清。
set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$EXAMPLE_DIR/../.." && pwd)"
BUILD_DIR="${1:-$REPO_DIR/build-deps/project-build}"

SVC_A="$BUILD_DIR/examples/svc_a"
SVC_B="$BUILD_DIR/examples/svc_b"
DRIVER="$BUILD_DIR/examples/driver"
for b in "$SVC_A" "$SVC_B" "$DRIVER"; do
    [ -x "$b" ] || { echo "[demo] FATAL: $b 不存在，先跑 scripts/local_build.sh" >&2; exit 2; }
done

# 真实后端地址（必需）：不复用 BBT_TEST_* 以免和 ctest live 门禁混淆；
# 默认指向本机已验证容器，可用环境变量覆盖。空值 → 明确失败而非替身。
REDIS_ADDR="${BBT_DEMO_REDIS_ADDR:-127.0.0.1:16379}"
MONGO_URI="${BBT_DEMO_MONGO_URI:-mongodb://127.0.0.1:27017/}"
if [ -z "$REDIS_ADDR" ] || [ -z "$MONGO_URI" ]; then
    echo "[demo] FATAL: 真实后端地址为空（BBT_DEMO_REDIS_ADDR/BBT_DEMO_MONGO_URI）" >&2
    exit 2
fi
echo "[demo] redis=$REDIS_ADDR mongo=$MONGO_URI"

PORT_A=38101
PORT_B=38102
# 日志持久化：默认写到构建目录旁的持久位置（不随 EXIT 删除），便于独立
# 回读；BBT_DEMO_KEEP_LOGS=0 才退到临时目录并自删。
if [ "${BBT_DEMO_KEEP_LOGS:-1}" = "0" ]; then
    LOG_DIR="$(mktemp -d /tmp/bbt-dual-service.XXXXXX)"
    KEEP_LOGS=0
else
    LOG_DIR="$REPO_DIR/build-deps/run-demo-logs/$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$LOG_DIR"
    KEEP_LOGS=1
fi
PID_A=""; PID_B=""
cleanup() {
    [ -n "$PID_A" ] && kill -TERM "$PID_A" 2>/dev/null || true
    [ -n "$PID_B" ] && kill -TERM "$PID_B" 2>/dev/null || true
    [ -n "$PID_A" ] && wait "$PID_A" 2>/dev/null || true
    [ -n "$PID_B" ] && wait "$PID_B" 2>/dev/null || true
    [ "$KEEP_LOGS" = "0" ] && rm -rf "$LOG_DIR" || true
}
trap cleanup EXIT

echo "[demo] binaries: $BUILD_DIR/examples/{svc_a,svc_b,driver}"
echo "[demo] logs: $LOG_DIR"

# ── 场景 0：两进程启动 ────────────────────────────────────────────────
echo
echo "== 0. start svc_a(storage :$PORT_A) + svc_b(gateway :$PORT_B) =="
"$SVC_A" "$PORT_A" "$REDIS_ADDR" "$MONGO_URI" \
    >"$LOG_DIR/svc_a.log" 2>&1 &
PID_A=$!
"$SVC_B" "$PORT_B" "127.0.0.1:$PORT_A" >"$LOG_DIR/svc_b.log" 2>&1 &
PID_B=$!

# 等监听就绪：轮询端口可连通（真实 TCP connect 作就绪判据）。
for i in $(seq 1 100); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$PORT_A") 2>/dev/null && \
       (exec 3<>"/dev/tcp/127.0.0.1/$PORT_B") 2>/dev/null; then
        break
    fi
    if ! kill -0 "$PID_A" 2>/dev/null || ! kill -0 "$PID_B" 2>/dev/null; then
        echo "[demo] FATAL: service exited before listen" >&2
        cat "$LOG_DIR/svc_a.log" "$LOG_DIR/svc_b.log" >&2
        exit 3
    fi
    sleep 0.05
done
echo "[demo] both services listening"

# ── 场景 1..5：正常回路 / 业务错误 / 超时 / 未路由 ───────────────────
echo
echo "== 1..5. driver 正常+错误+超时+未路由 =="
DRIVER_RC=0
"$DRIVER" "127.0.0.1:$PORT_B" | tee "$LOG_DIR/driver.log" || DRIVER_RC=$?
echo "[demo] driver rc=$DRIVER_RC"

# ── 场景 6：storage 下线 → gateway 调用失败路径 ──────────────────────
echo
echo "== 6. storage down -> gateway.fetch error path =="
# 先关停 storage，再调 fetch：连接拒绝应如实返回错误而非悬挂。
kill -TERM "$PID_A"
A_RC=0
wait "$PID_A" || A_RC=$?
PID_A=""
echo "[demo] svc_a exit code: $A_RC"
DOWN_RC=0
"$DRIVER" "127.0.0.1:$PORT_B" --expect-storage-down | \
    tee "$LOG_DIR/driver-down.log" || DOWN_RC=$?
echo "[demo] driver(down) rc=$DOWN_RC"

# ── 优雅关闭 gateway ─────────────────────────────────────────────────
echo
echo "== 7. SIGTERM svc_b -> graceful shutdown =="
kill -TERM "$PID_B"
B_RC=0
wait "$PID_B" || B_RC=$?
PID_B=""
echo "[demo] svc_b exit code: $B_RC"

# ── 后端清理：删除本 demo 写入的 key，避免污染共享容器 ───────────────
# 经 svc_a 同一批后端做最小清理（demo 只写 color/absent 探测键）。失败
# 不阻断验收（数据残留不影响已产生的进程证据）。
if command -v docker >/dev/null 2>&1; then
    docker exec bbt-f8-live-redis redis-cli DEL color absent >/dev/null 2>&1 || true
    docker exec bbt-f8-live-mongo mongosh --quiet \
        --eval 'db.getSiblingDB("bbt_dual_service").kv.deleteMany({})' \
        >/dev/null 2>&1 || true
fi

echo
echo "===== logs ====="
echo "--- svc_a ---"; cat "$LOG_DIR/svc_a.log"
echo "--- svc_b ---"; cat "$LOG_DIR/svc_b.log"

echo
echo "===== summary ====="
echo "driver rc=$DRIVER_RC  driver(down) rc=$DOWN_RC"
echo "svc_a exit=$A_RC  svc_b exit=$B_RC"
echo "logs persisted at: $LOG_DIR"

ok=1
[ "$DRIVER_RC" -eq 0 ] || { echo "driver reported failures"; ok=0; }
[ "$DOWN_RC"  -eq 0 ] || { echo "driver(down) reported failures"; ok=0; }
# kExitOk = 0：按期优雅关闭（HostLifecycle::Run 返回码，示例不引用
# internal 头故脚本按 0 判定）。
[ "$A_RC" -eq 0 ] || { echo "svc_a did not shut down cleanly"; ok=0; }
[ "$B_RC" -eq 0 ] || { echo "svc_b did not shut down cleanly"; ok=0; }

if [ "$ok" -eq 1 ]; then
    echo "[demo] PASS: all scenarios verified"
else
    echo "[demo] FAIL" >&2
    exit 1
fi
