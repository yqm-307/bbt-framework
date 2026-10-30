#!/usr/bin/env python3
"""bbt-framework 公开出站策略 fixture 验收驱动器。

由 ctest 调用：
  rpc_policy_run.py --bin <rpc_policy> --results <json>

每场景独立 spawn 一个 fixture 子进程（一 CoApp 一策略），经 getvalue 切片的 Python 标准
库 wire codec 真实 HTTP POST /rpc 打 caller 服务，读 fixture journal 断言：

  1. allow     —— 策略放行：调用成功、Preflight 恰一次、callee handler 恰一次；
  2. reject    —— 策略拒绝（Cancelled）：调用报错、Preflight 一次、callee 零 hit
                  （观测；「出站未发送」由 CoApp 在 Send 前返回的源码结构保证）；
  3. tighten   —— 收紧期限只缩不放：callee 可见剩余预算 ≤ 收紧值且 < 原预算；
  4. widen     —— 策略放大期限被框架忽略：剩余预算仍≈原预算（绝不放大到上限）；
  5. expired   —— 收紧到已过期：调用方 I/O 前 TimedOut、callee 零 hit；
  6. decorator —— 外层包内层：两个虚 Preflight 各一次、顺序固定（outer→inner）；
  7. noroute   —— 未配置静态路由：NotFound、Preflight 计数为 0（路由先于策略）；
  8. shutdown  —— stdin EOF → 收束 rc=0/state=Closed/owner 同步 Close，且 fixture
                  在 `app.run()` 返回后、进程退出前对自己 bound 端口发一次有界
                  探针（journal event=listener_probe），driver 核该事件为 refused。

关闭后断言的对象是**存活期探针事件**（B1）：进程回收后再 connect 必然
ECONNREFUSED，与框架是否关闭 listener 无关，那种断言恒真、不构成证据。本驱动
只认 result=refused 且 errno=ECONNREFUSED 的存活期探针；探针超时/其它 errno
（result=error）判失败。该探针只证明「存活期 listener 已关」，不证明关闭期零额外
I/O。

wire 编解码复用 examples/getvalue/getvalue_client.py（唯一手写 proto3 codec）。
所有结构化结果标注 auth_state=unauthenticated_loopback。
"""

import argparse
import errno
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(HERE, "..", "getvalue"))
try:
    import getvalue_client as gvc
except Exception as exc:  # noqa: BLE001
    print(f"driver: cannot import #4 wire codec: {exc}", file=sys.stderr)
    sys.exit(9)

AUTH_STATE = "unauthenticated_loopback"
CALLER_SERVICE = "bbt.example.v1.PolicyCaller"
REQUEST_SCHEMA = "bbt.example.v1.GetValueRequest"
RESPONSE_SCHEMA = "bbt.example.v1.GetValueResponse"

# RpcErrorCode（infra rpc_envelope.proto enum）
CANCELLED = 5
TIMED_OUT = 6
NOT_FOUND = 11

DECLARED_BUDGET_MS = 3000
MAX_BUDGET_MS = 30000
TIGHTEN_MS = 300

RESULTS = {}
_FAILURES = []


def check(cond, label, scenario):
    if not cond:
        _FAILURES.append(f"{scenario}: {label}")
        print(f"FAIL {scenario}: {label}", file=sys.stderr)


def finish(scenario, ok):
    rec = RESULTS.setdefault(scenario, {})
    rec["status"] = "pass" if ok else "fail"
    rec["auth_state"] = AUTH_STATE
    print(f"{'PASS' if ok else 'FAIL'} scenario={scenario} auth_state={AUTH_STATE}")


def call(port, request_id, method, key="alpha", budget_ms=DECLARED_BUDGET_MS):
    payload = gvc.enc_get_value_request(key)
    body = gvc.enc_envelope(
        request_id=request_id,
        service=CALLER_SERVICE,
        method=method,
        budget_ms=budget_ms,
        request_schema=REQUEST_SCHEMA,
        response_schema=RESPONSE_SCHEMA,
        payload=payload,
    )
    status, _ctype, data = gvc.post_raw(port, body)
    return status, gvc.dec_envelope(data)


def read_journal(path):
    entries = []
    if not os.path.exists(path):
        return entries
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = {}
            for kv in line.split(" "):
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    rec[k] = v
            entries.append(rec)
    return entries


def count_event(entries, event):
    return sum(1 for e in entries if e.get("event") == event)


def preflight_names(entries):
    return [e.get("name") for e in entries if e.get("event") == "preflight"]


def callee_hits(entries):
    return [e for e in entries if e.get("event") == "callee_handler_entered"]


def callee_deadline_ms(entries):
    """callee handler 经公开 RequestContext::deadline 观测到的剩余预算。"""
    hits = callee_hits(entries)
    if not hits:
        return None
    try:
        return int(hits[-1].get("deadline_ms", "-1"))
    except (TypeError, ValueError):
        return None


def wait_port(path, proc, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"fixture exited early rc={proc.returncode}")
        try:
            with open(path) as f:
                p = int(f.read().strip())
            if p > 0:
                return p
        except (OSError, ValueError):
            pass
        time.sleep(0.05)
    raise RuntimeError("fixture did not write port file")


def reap(name, proc, failures):
    if proc is None:
        return
    if proc.stdin:
        try:
            proc.stdin.close()
        except OSError:
            pass
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        failures.append(f"{name} pid={proc.pid} did not stop cleanly")
        return
    rc = proc.returncode
    if rc is None:
        return
    if rc < 0:
        try:
            sig = signal.Signals(-rc).name
        except (ValueError, AttributeError):
            sig = f"signal{-rc}"
        failures.append(f"{name} pid={proc.pid} signal={-rc} ({sig})")
    elif rc != 0:
        failures.append(f"{name} pid={proc.pid} rc={rc}")


def listener_probe(entries):
    """fixture 存活期（app.run 返回后、进程退出前）对自己 bound 端口的探针事件。

    B1：这是「关闭后 listener 已关」唯一可用的真实 oracle。进程回收后再探端口
    必然 ECONNREFUSED，那种断言恒真——不构成证据，故不再探、也不再认。
    """
    for e in entries:
        if e.get("event") == "listener_probe":
            return e
    return None


def listener_probe_ok(probe, port):
    """存活期探针的判绿条件（唯一真值口径；scratch 负例直接复用本函数）。

    必须同时满足：事件存在、phase 为 after_run_before_exit、探的端口就是本次
    服务过真实 RPC 的那个端口、result=refused、errno=ECONNREFUSED。超时或任何
    其它 errno（result=error）→ 不判绿。
    """
    return (
        probe is not None
        and probe.get("phase") == "after_run_before_exit"
        and probe.get("port") == str(port)
        and probe.get("result") == "refused"
        and probe.get("errno") == str(errno.ECONNREFUSED)
    )


def scenario(fixture, mode, work_root, calls, assert_fn, timeout):
    """spawn fixture（mode）→ 跑 calls 断言 → 关闭收束断言。

    calls: list of (request_id, method, key, budget_ms)。
    返回 (entries, shutdown_rec, port)。
    """
    work = os.path.join(work_root, f"{mode}-{os.getpid()}-{time.time_ns()}")
    os.makedirs(work, exist_ok=True)
    port_file = os.path.join(work, "port")
    meta_file = os.path.join(work, "meta")
    journal_file = os.path.join(work, "journal")

    proc = None
    entries, shutdown_rec = [], {}
    try:
        proc = subprocess.Popen(
            [fixture, mode, port_file, meta_file, journal_file],
            stdin=subprocess.PIPE,
            stdout=sys.stderr,
            stderr=sys.stderr,
        )
        port = wait_port(port_file, proc, timeout)
        responses = [
            call(port, rid, method, key=key, budget_ms=budget)
            for (rid, method, key, budget) in calls
        ]
        entries = read_journal(journal_file)
        assert_fn(port, responses, entries)

        # 关闭：stdin EOF → request_shutdown → run 收束。
        if proc.stdin:
            proc.stdin.close()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            check(False, "fixture did not stop after stdin EOF", mode)
        rc = proc.returncode
        entries = read_journal(journal_file)
        for e in entries:
            if e.get("event") == "shutdown":
                shutdown_rec = e
        ok = (
            rc == 0
            and shutdown_rec.get("rc") == "0"
            and shutdown_rec.get("state") == "3"  # ShutdownState::Closed
            and shutdown_rec.get("owner_closed") == "1"
            and count_event(entries, "resource_close") == 1
        )
        check(ok, f"rc={rc} shutdown={shutdown_rec}", f"{mode}.shutdown")
        finish(f"{mode}.shutdown", ok)
        # 关闭后 listener 状态（B1）：核 fixture 在 run 返回后、进程退出前的存活期
        # 探针事件（判绿口径见 listener_probe_ok）。
        probe = listener_probe(entries)
        ok_probe = listener_probe_ok(probe, port)
        check(
            ok_probe,
            f"listener_probe={probe} (expect refused/"
            f"errno={errno.ECONNREFUSED} port={port})",
            f"{mode}.after-close",
        )
        finish(f"{mode}.after-close", ok_probe)
        RESULTS[f"{mode}.after-close"] = {
            "listener_probe": (
                {k: probe.get(k) for k in ("phase", "port", "result", "errno")}
                if probe
                else None
            ),
            "auth_state": AUTH_STATE,
        }
        proc = None
        return entries, shutdown_rec, port
    finally:
        reap(f"fixture[{mode}]", proc, _FAILURES)
        shutil.rmtree(work, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--results")
    ap.add_argument("--timeout", type=float, default=30.0)
    args = ap.parse_args()

    work_root = os.path.join(
        os.path.dirname(os.path.abspath(args.bin)), f"rpc-policy-{os.getpid()}"
    )
    os.makedirs(work_root, exist_ok=True)

    try:
        # 1) allow：放行、Preflight 恰一次、callee 恰一次。
        def a_allow(port, resp, entries):
            st, env = resp[0]
            rem = env.get("remaining_budget_ms", 0)
            cd = callee_deadline_ms(entries)
            ok = (
                st == 200
                and env.get("success") is True
                and env.get("request_id") == "rp-allow"
                and env.get("response_schema") == RESPONSE_SCHEMA
                and gvc.dec_get_value_response(env.get("payload", b""))
                == (True, "alpha")
                and 1 <= rem <= MAX_BUDGET_MS
                and cd is not None
                and 1 <= cd <= MAX_BUDGET_MS
                and preflight_names(entries) == ["allow"]
                and len(callee_hits(entries)) == 1
            )
            check(
                ok,
                f"st={st} rem={rem} callee_deadline={cd} "
                f"pre={preflight_names(entries)} "
                f"hits={len(callee_hits(entries))}",
                "allow",
            )
            RESULTS["allow"] = {
                "remaining_budget_ms": rem,
                "callee_deadline_ms": cd,
                "auth_state": AUTH_STATE,
            }
            finish("allow", ok)

        scenario(
            args.bin,
            "allow",
            work_root,
            [("rp-allow", "Forward", "alpha", DECLARED_BUDGET_MS)],
            a_allow,
            args.timeout,
        )

        # 2) reject：拒绝、Preflight 一次、callee 零 hit（零 I/O）。
        def a_reject(port, resp, entries):
            st, env = resp[0]
            code = env.get("error", {}).get("code")
            msg = env.get("error", {}).get("message", "")
            ok = (
                env.get("success") is not True
                and code == CANCELLED
                and "rejected" in msg
                and preflight_names(entries) == ["reject"]
                and len(callee_hits(entries)) == 0
            )
            check(
                ok,
                f"st={st} env={env} pre={preflight_names(entries)} "
                f"hits={len(callee_hits(entries))}",
                "reject",
            )
            RESULTS["reject.error"] = {
                "code": code,
                "message": msg,
                "auth_state": AUTH_STATE,
            }
            finish("reject", ok)

        scenario(
            args.bin,
            "reject",
            work_root,
            [("rp-reject", "Forward", "alpha", DECLARED_BUDGET_MS)],
            a_reject,
            args.timeout,
        )

        # 3) tighten：只缩不放，callee 观测可见期限 ≤ 收紧值且 < 原预算。
        def a_tighten(port, resp, entries):
            st, env = resp[0]
            cd = callee_deadline_ms(entries)
            ok = (
                env.get("success") is True
                and cd is not None
                and 0 < cd <= TIGHTEN_MS + 50
                and cd < DECLARED_BUDGET_MS
                and preflight_names(entries) == ["tighten"]
                and len(callee_hits(entries)) == 1
            )
            check(
                ok,
                f"st={st} callee_deadline={cd} (tighten={TIGHTEN_MS}) "
                f"hits={len(callee_hits(entries))}",
                "tighten",
            )
            RESULTS["tighten"] = {
                "callee_deadline_ms": cd,
                "tighten_ms": TIGHTEN_MS,
                "declared_budget_ms": DECLARED_BUDGET_MS,
                "auth_state": AUTH_STATE,
            }
            finish("tighten", ok)

        scenario(
            args.bin,
            "tighten",
            work_root,
            [("rp-tighten", "Forward", "alpha", DECLARED_BUDGET_MS)],
            a_tighten,
            args.timeout,
        )

        # 4) widen：策略放大被忽略；callee 观测期限仍≈原预算而非放大值/上限。
        def a_widen(port, resp, entries):
            st, env = resp[0]
            cd = callee_deadline_ms(entries)
            ok = (
                env.get("success") is True
                and cd is not None
                and DECLARED_BUDGET_MS - 300 <= cd <= DECLARED_BUDGET_MS
                and preflight_names(entries) == ["widen"]
                and len(callee_hits(entries)) == 1
            )
            check(
                ok,
                f"st={st} callee_deadline={cd} "
                f"(amplification would give ~{MAX_BUDGET_MS})",
                "widen",
            )
            RESULTS["widen"] = {
                "callee_deadline_ms": cd,
                "declared_budget_ms": DECLARED_BUDGET_MS,
                "auth_state": AUTH_STATE,
            }
            finish("widen", ok)

        scenario(
            args.bin,
            "widen",
            work_root,
            [("rp-widen", "Forward", "alpha", DECLARED_BUDGET_MS)],
            a_widen,
            args.timeout,
        )

        # 5) expired：收紧到已过期 → 调用方 I/O 前 TimedOut、callee 零 hit。
        def a_expired(port, resp, entries):
            st, env = resp[0]
            code = env.get("error", {}).get("code")
            ok = (
                env.get("success") is not True
                and code == TIMED_OUT
                and preflight_names(entries) == ["expired"]
                and len(callee_hits(entries)) == 0
            )
            check(ok, f"st={st} env={env}", "expired")
            finish("expired", ok)

        scenario(
            args.bin,
            "expired",
            work_root,
            [("rp-expired", "Forward", "alpha", DECLARED_BUDGET_MS)],
            a_expired,
            args.timeout,
        )

        # 6) decorator：外层包内层，两个虚 Preflight 各一次、顺序 outer→inner。
        def a_decorator(port, resp, entries):
            st, env = resp[0]
            ok = (
                env.get("success") is True
                and preflight_names(entries) == ["outer", "inner"]
                and len(callee_hits(entries)) == 1
            )
            check(ok, f"st={st} pre={preflight_names(entries)}", "decorator")
            RESULTS["decorator.order"] = preflight_names(entries)
            finish("decorator", ok)

        scenario(
            args.bin,
            "decorator",
            work_root,
            [("rp-decor", "Forward", "alpha", DECLARED_BUDGET_MS)],
            a_decorator,
            args.timeout,
        )

        # 7) 路由先于策略：未配置静态路由 → NotFound、Preflight 计数为 0。
        def a_noroute(port, resp, entries):
            st, env = resp[0]
            code = env.get("error", {}).get("code")
            ok = (
                env.get("success") is not True
                and code == NOT_FOUND
                and preflight_names(entries) == []
                and len(callee_hits(entries)) == 0
            )
            check(ok, f"st={st} env={env} pre={preflight_names(entries)}", "noroute")
            finish("noroute", ok)

        scenario(
            args.bin,
            "allow",
            work_root,
            [("rp-noroute", "ForwardNoRoute", "alpha", DECLARED_BUDGET_MS)],
            a_noroute,
            args.timeout,
        )
    except Exception as exc:  # noqa: BLE001
        _FAILURES.append(f"driver-error {type(exc).__name__}: {exc}")
        print(f"DRIVER-ERROR {type(exc).__name__}: {exc}", file=sys.stderr)
        traceback.print_exc()
    finally:
        shutil.rmtree(work_root, ignore_errors=True)

    tail = "RPC-POLICY-ALL-PASS" if not _FAILURES else "RPC-POLICY-FAILURES"
    RESULTS["_summary"] = {
        "failures": list(_FAILURES),
        "tail": tail,
        "auth_state": AUTH_STATE,
    }
    print(f"GRAND {tail} auth_state={AUTH_STATE}")
    if args.results:
        with open(args.results, "w") as f:
            json.dump(RESULTS, f, indent=2, sort_keys=True, ensure_ascii=False)
    if _FAILURES:
        print(f"RPC-POLICY-DRIVER-FAIL {_FAILURES}", file=sys.stderr)
        sys.exit(1)
    print("RPC-POLICY-DRIVER-PASS auth_state=unauthenticated_loopback")


if __name__ == "__main__":
    main()
