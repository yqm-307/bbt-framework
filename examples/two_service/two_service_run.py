#!/usr/bin/env python3
"""bbt-framework Issue #5（EX-T2）：单进程双 Service fixture 验收驱动器。

由 ctest 调用：
  two_service_run.py --bin <two_service> --results <json>

职责（R1/S1，全部为真实运行结果）：
  1. spawn fixture 子进程（自有进程、动态 loopback 端口、就绪以 port 文件为准）；
  2. 经 #4 的 Python 标准库 wire codec 真实 HTTP POST /rpc 打 caller 服务，
     覆盖 正常请求 / 业务错误 / 有限 deadline / 未配置路由；
  3. 读 fixture journal：callee handler 只被真正到达的请求进入；关闭记录
     rc=0、state=Closed、owner 同步 Close 且**在最后一条 handler 进入之后**
     （即 handler 排空后同步 Close）；
  4. 收尾：关 stdin 触发 request_shutdown，有限等待；异常路径回收自有进程。

wire 编解码复用 examples/getvalue/getvalue_client.py（#4 唯一手写 proto3
codec），#5 不另造第二套 schema/codec。

所有结构化结果标注 auth_state=unauthenticated_loopback：本路径只证明公开
App/Service 消费面与生命周期，不证明身份可信或授权完成（认证见 #38）。
"""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
# 复用 #4 的 codec 但不让 import 在源码树里留下 __pycache__。
sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(HERE, "..", "getvalue"))
try:
    import getvalue_client as gvc
except Exception as exc:  # noqa: BLE001
    print(f"driver: cannot import #4 wire codec "
          f"(examples/getvalue/getvalue_client.py): {exc}", file=sys.stderr)
    sys.exit(9)

AUTH_STATE = "unauthenticated_loopback"
CALLER_SERVICE = "bbt.example.v1.GetValueCallerService"
REQUEST_SCHEMA = "bbt.example.v1.GetValueRequest"
RESPONSE_SCHEMA = "bbt.example.v1.GetValueResponse"

# RpcErrorCode（infra rpc_envelope.proto enum）
INVALID_ARGUMENT = 1
TIMED_OUT = 6
NOT_FOUND = 11

# 声明预算上限（与 fixture NetworkLimits.incoming_timeout 一致）：成功回复的
# remaining_budget_ms 只由正式 wire profile 回填，且必须 clamp 在该上限内。
MAX_BUDGET_MS = 30000

RESULTS = {}
_FAILURES = []


def check(cond, label, scenario):
    if not cond:
        _FAILURES.append(f"{scenario}: {label}")
        print(f"FAIL {scenario}: {label}", file=sys.stderr)


def finish(scenario, ok):
    RESULTS[scenario] = {"status": "pass" if ok else "fail",
                         "auth_state": AUTH_STATE}
    if ok:
        print(f"PASS scenario={scenario} auth_state={AUTH_STATE}")


def call(port, request_id, method, key="alpha", budget_ms=3000):
    """真实 HTTP POST /rpc → (status, decoded envelope)。"""
    payload = gvc.enc_get_value_request(key)
    body = gvc.enc_envelope(request_id=request_id, service=CALLER_SERVICE,
                            method=method, budget_ms=budget_ms,
                            request_schema=REQUEST_SCHEMA,
                            response_schema=RESPONSE_SCHEMA, payload=payload)
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


def event_index(entries, event, occurrence=1):
    """第 occurrence 次出现 event 的行号；未出现返回 -1。"""
    seen = 0
    for i, e in enumerate(entries):
        if e.get("event") == event:
            seen += 1
            if seen == occurrence:
                return i
    return -1


def wait_port(path, proc, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(
                f"fixture exited early rc={proc.returncode}")
        try:
            with open(path) as f:
                p = int(f.read().strip())
            if p > 0:
                return p
        except (OSError, ValueError):
            pass
        time.sleep(0.05)
    raise RuntimeError("fixture did not write port file")


def signal_name(num):
    try:
        return signal.Signals(num).name
    except (ValueError, AttributeError):
        return f"signal-{num}"


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
        failures.append(f"{name} pid={proc.pid} did not stop cleanly "
                        f"(killed after timeout)")
        return
    rc = proc.returncode
    if rc is None:
        return
    if rc < 0:
        failures.append(f"{name} pid={proc.pid} signal={-rc} "
                        f"({signal_name(-rc)})")
    elif rc != 0:
        failures.append(f"{name} pid={proc.pid} rc={rc}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--results")
    ap.add_argument("--timeout", type=float, default=30.0)
    args = ap.parse_args()

    work = os.path.join(os.path.dirname(os.path.abspath(args.bin)),
                        f"two-service-{os.getpid()}")
    os.makedirs(work, exist_ok=True)
    port_file = os.path.join(work, "port")
    meta_file = os.path.join(work, "meta")
    journal_file = os.path.join(work, "journal")
    for p in (port_file, meta_file, journal_file):
        try:
            os.unlink(p)
        except OSError:
            pass

    proc = None
    try:
        proc = subprocess.Popen(
            [args.bin, "fixture", port_file, meta_file, journal_file],
            stdin=subprocess.PIPE, stdout=sys.stderr, stderr=sys.stderr)
        port = wait_port(port_file, proc, args.timeout)
        print(f"fixture up port={port}", file=sys.stderr)

        # 1) 正常请求：caller → 正式 wire → 同进程 callee（静态路由自环）。
        st, env = call(port, "sc-known", "Forward", key="alpha")
        remaining = env.get("remaining_budget_ms", 0)
        ok = (st == 200 and env.get("success") is True
              and env.get("request_id") == "sc-known"
              and env.get("response_schema") == RESPONSE_SCHEMA
              and gvc.dec_get_value_response(env.get("payload", b"")) ==
              (True, "value-alpha")
              # remaining_budget_ms 只由正式 ProtoWireV1 body profile 回填，
              # 且被接收端 local-min 后 clamp 在本地上限内。
              and 1 <= remaining <= MAX_BUDGET_MS)
        check(ok, f"got status={st} env={env}", "known")
        finish("known", ok)
        RESULTS["known"]["remaining_budget_ms"] = remaining

        # 2) miss：正常业务响应 found=false（不是错误）。
        st, env = call(port, "sc-miss", "Forward", key="no-such-key")
        ok = (env.get("success") is True
              and gvc.dec_get_value_response(env.get("payload", b"")) ==
              (False, ""))
        check(ok, f"got {env}", "miss")
        finish("miss", ok)

        # 3) 业务错误：callee 的 InvalidArgument 经受保护的 this->call 原样回到
        #    调用方，再经正式 wire 信封回给驱动器（不被转成成功）。
        st, env = call(port, "sc-empty", "Forward", key="")
        code = env.get("error", {}).get("code")
        ok = ("error" in env and code == INVALID_ARGUMENT)
        check(ok, f"expected InvalidArgument via Service→Service, got {env}",
              "empty-key")
        finish("empty-key", ok)
        RESULTS["empty-key-error"] = {
            "code": code,
            "message": env.get("error", {}).get("message", ""),
            "auth_state": AUTH_STATE}

        n_after_business = len([
            e for e in read_journal(journal_file)
            if e.get("event") == "callee_handler_entered"])
        check(n_after_business == 3,
              f"callee handler count expected 3, got {n_after_business}",
              "business-journal")
        finish("business-journal", n_after_business == 3)

        # 4) 有限 deadline（接收端预算门）：声明 1ms 预算 → 进 handler 前
        #    TimedOut，callee 不得被进入。
        st, env = call(port, "sc-budget1", "Forward", key="alpha", budget_ms=1)
        code = env.get("error", {}).get("code")
        n_budget = len([e for e in read_journal(journal_file)
                        if e.get("event") == "callee_handler_entered"])
        ok = ("error" in env and code == TIMED_OUT
              and n_budget == n_after_business)
        check(ok, f"expected TimedOut + no extra callee handler, got "
                  f"code={code} handlers={n_budget}", "finite-deadline")
        finish("finite-deadline", ok)
        RESULTS["finite-deadline"] = {"error_code": code,
                                     "callee_handler_after": n_budget,
                                     "auth_state": AUTH_STATE}

        # 5) 有限 deadline（调用方发起前已过期）：外层预算充足，子调用显式
        #    已过期 deadline → 发送前 TimedOut，且 callee 无新 handler。
        st, env = call(port, "sc-expired", "ForwardExpired", key="alpha")
        code = env.get("error", {}).get("code")
        n_expired = len([e for e in read_journal(journal_file)
                         if e.get("event") == "callee_handler_entered"])
        ok = ("error" in env and code == TIMED_OUT
              and n_expired == n_after_business)
        check(ok, f"expected TimedOut + no callee IO, got code={code} "
                  f"handlers={n_expired}", "expired-deadline")
        finish("expired-deadline", ok)
        RESULTS["expired-deadline"] = {"error_code": code,
                                       "callee_handler_after": n_expired,
                                       "auth_state": AUTH_STATE}

        # 6) 未配置静态路由：find_route 门 NotFound，不做 I/O。
        st, env = call(port, "sc-noroute", "ForwardNoRoute", key="alpha")
        code = env.get("error", {}).get("code")
        n_noroute = len([e for e in read_journal(journal_file)
                         if e.get("event") == "callee_handler_entered"])
        ok = ("error" in env and code == NOT_FOUND
              and n_noroute == n_after_business)
        check(ok, f"expected NotFound + no IO, got code={code} "
                  f"handlers={n_noroute}", "no-route")
        finish("no-route", ok)

        # 7) 关闭：request_shutdown（stdin EOF）→ run 收束 → owner 同步 Close；
        #    Close 必须发生在最后一次 callee handler 进入之后。
        meta = {}
        with open(meta_file) as f:
            for line in f:
                if "=" in line:
                    k, v = line.strip().split("=", 1)
                    meta[k] = v
        RESULTS["meta"] = dict(meta, auth_state=AUTH_STATE)
        if proc.stdin:
            proc.stdin.close()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            check(False, "fixture did not stop cleanly after stdin EOF",
                  "shutdown")
        rc = proc.returncode
        entries = read_journal(journal_file)
        last_handler = max((i for i, e in enumerate(entries)
                            if e.get("event") == "callee_handler_entered"),
                           default=-1)
        i_close = event_index(entries, "resource_close")
        i_shutdown = event_index(entries, "shutdown")
        shutdown_rec = entries[i_shutdown] if i_shutdown >= 0 else {}

        auth_ok = all(e.get("auth_state") == AUTH_STATE for e in entries)
        peer_upgraded = [e.get("req") for e in entries
                         if e.get("peer_principal")]
        ok = (rc == 0
              and shutdown_rec.get("rc") == "0"
              and shutdown_rec.get("state") == "3"      # ShutdownState::Closed
              and shutdown_rec.get("owner_closed") == "1"
              and i_close >= 0 and last_handler >= 0 and last_handler < i_close
              and event_index(entries, "resource_close", 2) == -1
              and auth_ok and not peer_upgraded)
        check(ok, f"rc={rc} shutdown={shutdown_rec} last_handler={last_handler} "
                  f"resource_close_at={i_close} entries={len(entries)} "
                  f"peer_upgraded={peer_upgraded}", "shutdown-and-close")
        finish("shutdown-and-close", ok)
        RESULTS["shutdown"] = {
            "exit_code": rc,
            "shutdown_state": shutdown_rec.get("state"),
            "owner_closed": shutdown_rec.get("owner_closed"),
            "callee_handler_entered": last_handler + 1 if last_handler >= 0 else 0,
            "resource_close_calls": len([e for e in entries
                                         if e.get("event") == "resource_close"]),
            "resource_close_after_last_handler":
                bool(i_close >= 0 and last_handler >= 0 and last_handler < i_close),
            "auth_state": AUTH_STATE}
        proc = None   # 已回收，避免 finally 重复处理
    except Exception as exc:  # noqa: BLE001
        _FAILURES.append(f"driver-error {type(exc).__name__}: {exc}")
        print(f"DRIVER-ERROR {type(exc).__name__}: {exc}", file=sys.stderr)
        traceback.print_exc()
    finally:
        reap("fixture", proc, _FAILURES)
        # 失败清理：只删本轮自己创建的临时工作目录（journal/port/meta）。
        # 断言与结构化结果在此之前已落地，清理失败不影响结论。
        shutil.rmtree(work, ignore_errors=True)

    tail = "TWO-SERVICE-ALL-PASS" if not _FAILURES else "TWO-SERVICE-FAILURES"
    RESULTS["_summary"] = {"failures": list(_FAILURES), "tail": tail,
                           "auth_state": AUTH_STATE}
    print(f"GRAND {tail} auth_state={AUTH_STATE}")
    if args.results:
        with open(args.results, "w") as f:
            json.dump(RESULTS, f, indent=2, sort_keys=True,
                      ensure_ascii=False)
    if _FAILURES:
        print(f"TWO-SERVICE-DRIVER-FAIL {_FAILURES}", file=sys.stderr)
        sys.exit(1)
    print("TWO-SERVICE-DRIVER-PASS auth_state=unauthenticated_loopback")


if __name__ == "__main__":
    main()
