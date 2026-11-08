#!/usr/bin/env python3
"""bbt-framework Service→Service fixture 验收驱动器。

由 ctest 调用：
  two_service_run.py --bin <two_service> --results <json>

形态：**两个互异 identity 的真实 CoApp 进程**（V2 会话拒绝 self-connection，
且框架约束进程内同一时刻至多一个活跃 CoApp）：
  - callee 进程：local_identity=two-service-callee，宿主 GetValueService
  - caller 进程：local_identity=two-service-caller，宿主 GetValueCallerService；
    handler 经公开 ICoService::call<Req,Resp> 按显式静态路由向 callee 发起
    真实 V2 Call
驱动器是与两者互异的第三个 identity（two-service-driver）。

职责（R1/S1，全部为真实运行结果）：
  1. spawn callee/caller 两个自有进程（动态 loopback 端口，就绪以 port 文件为准）；
  2. 经唯一 V2 codec（tests/rpc_v2_wire.py）打 caller 服务，
     覆盖 正常请求 / 业务错误 / 有限 deadline / 未配置路由；
  3. 读共享 journal：callee handler 只被真正到达的请求进入；关闭记录
     rc=0、state=Closed、owner 同步 Close 且在最后一条 handler 进入之后；
  4. 收尾：关 stdin 触发 request_shutdown，有限等待；异常路径回收自有进程。

会话/分帧/握手/错误信封复用 tests/rpc_v2_wire.py；业务 payload 复用
examples/getvalue/getvalue_client.py——不另造第二套 schema/codec。

所有结构化结果标注 auth_state=unauthenticated_loopback：本路径只证明公开
App/Service 消费面与生命周期，不证明身份可信或授权完成（认证由后续范围承载）。
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
# 复用 getvalue 切片的 codec 但不让 import 在源码树里留下 __pycache__。
sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(HERE, "..", "getvalue"))
sys.path.insert(0, os.path.join(HERE, "..", "..", "tests"))
try:
    import rpc_v2_wire as v2
    import getvalue_client as gvc
except Exception as exc:  # noqa: BLE001
    print(f"driver: cannot import V2 codec (tests/rpc_v2_wire.py) or business "
          f"codec (examples/getvalue/getvalue_client.py): {exc}",
          file=sys.stderr)
    sys.exit(9)

AUTH_STATE = "unauthenticated_loopback"
CALLER_SERVICE = "bbt.example.v1.GetValueCallerService"
REQUEST_SCHEMA = "bbt.example.v1.GetValueRequest"
RESPONSE_SCHEMA = "bbt.example.v1.GetValueResponse"

# 会话身份：驱动器客户端与两个 CoApp 进程互异（V2 拒绝 self-connection）。
IDENTITY = "two-service-driver"

# RpcErrorCode（session_frame.proto enum，与 infra ErrorCode 同序）
INVALID_ARGUMENT = v2.ERR_INVALID_ARGUMENT
TIMED_OUT = v2.ERR_TIMED_OUT
NOT_FOUND = v2.ERR_NOT_FOUND

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


def call(client, request_id, method, key="alpha", budget_ms=3000):
    """一次真实 V2 Call；返回响应帧（error_of 取错误分支）。

    整个场景序列共用一条已握手会话（V2 一个 peer identity 对应一条活跃
    link；逐次重连会在旧 link 退役前被同一 identity 的 fencing 规则拒绝）。
    """
    return client.call(
        CALLER_SERVICE, method,
        payload=gvc.enc_get_value_request(key),
        request_id=request_id,
        request_schema=REQUEST_SCHEMA,
        response_schema=RESPONSE_SCHEMA,
        budget_ms=budget_ms)


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
                f"{proc.args[1] if len(proc.args) > 1 else 'process'} "
                f"exited early rc={proc.returncode}")
        try:
            with open(path) as f:
                p = int(f.read().strip())
            if p > 0:
                return p
        except (OSError, ValueError):
            pass
        time.sleep(0.05)
    raise RuntimeError("process did not write port file")


def signal_name(num):
    try:
        return signal.Signals(num).name
    except (ValueError, AttributeError):
        return f"signal-{num}"


def reap(name, proc, failures, close_stdin=True):
    if proc is None:
        return
    if close_stdin and proc.stdin:
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
    callee_port_file = os.path.join(work, "callee.port")
    callee_meta_file = os.path.join(work, "callee.meta")
    caller_port_file = os.path.join(work, "caller.port")
    caller_meta_file = os.path.join(work, "caller.meta")
    journal_file = os.path.join(work, "journal")
    for p in (callee_port_file, callee_meta_file, caller_port_file,
              caller_meta_file, journal_file):
        try:
            os.unlink(p)
        except OSError:
            pass

    callee = None
    caller = None
    driver = None
    caller_rc = None
    try:
        # 1) callee 进程就先绪，caller 的静态路由直接指向它的真实 endpoint。
        callee = subprocess.Popen(
            [args.bin, "callee", callee_port_file, callee_meta_file,
             journal_file],
            stdin=subprocess.PIPE, stdout=sys.stderr, stderr=sys.stderr)
        callee_port = wait_port(callee_port_file, callee, args.timeout)
        print(f"callee up port={callee_port}", file=sys.stderr)
        callee_ep = f"127.0.0.1:{callee_port}"

        caller = subprocess.Popen(
            [args.bin, "caller", caller_port_file, callee_ep, caller_meta_file,
             journal_file],
            stdin=subprocess.PIPE, stdout=sys.stderr, stderr=sys.stderr)
        caller_port = wait_port(caller_port_file, caller, args.timeout)
        print(f"caller up port={caller_port}", file=sys.stderr)

        # 整个场景序列共用一条真实 V2 会话。
        driver = v2.V2Client(caller_port, identity=IDENTITY)
        driver.handshake(expected_peer="two-service-caller")

        # 2) 正常请求：caller → 正式 V2 会话 → callee 进程（静态路由）。
        env = call(driver, "sc-known", "Forward", key="alpha")
        ok = (v2.error_of(env) is None
              and env.get("request_id") == "sc-known"
              and env.get("response_schema") == RESPONSE_SCHEMA
              and gvc.dec_get_value_response(env.get("payload", b"")) ==
              (True, "value-alpha"))
        check(ok, f"got env={env}", "known")
        finish("known", ok)

        # 3) miss：正常业务响应 found=false（不是错误）。
        env = call(driver, "sc-miss", "Forward", key="no-such-key")
        ok = (v2.error_of(env) is None
              and gvc.dec_get_value_response(env.get("payload", b"")) ==
              (False, ""))
        check(ok, f"got {env}", "miss")
        finish("miss", ok)

        # 4) 业务错误：callee 的 InvalidArgument 经受保护的 this->call 原样回到
        #    调用方，再经正式 wire 信封回给驱动器（不被转成成功）。
        env = call(driver, "sc-empty", "Forward", key="")
        code = (v2.error_of(env) or {}).get("code")
        ok = (v2.error_of(env) is not None and code == INVALID_ARGUMENT)
        check(ok, f"expected InvalidArgument via Service→Service, got {env}",
              "empty-key")
        finish("empty-key", ok)
        RESULTS["empty-key-error"] = {
            "code": code,
            "message": (v2.error_of(env) or {}).get("message", ""),
            "auth_state": AUTH_STATE}

        n_after_business = len([
            e for e in read_journal(journal_file)
            if e.get("event") == "callee_handler_entered"])
        check(n_after_business == 3,
              f"callee handler count expected 3, got {n_after_business}",
              "business-journal")
        finish("business-journal", n_after_business == 3)

        # 5) 有限 deadline（接收端预算门）：声明 1ms 预算 → 进 handler 前
        #    TimedOut，callee 不得被进入。
        env = call(driver, "sc-budget1", "Forward", key="alpha", budget_ms=1)
        code = (v2.error_of(env) or {}).get("code")
        n_budget = len([e for e in read_journal(journal_file)
                        if e.get("event") == "callee_handler_entered"])
        ok = (v2.error_of(env) is not None and code == TIMED_OUT
              and n_budget == n_after_business)
        check(ok, f"expected TimedOut + no extra callee handler, got "
                  f"code={code} handlers={n_budget}", "finite-deadline")
        finish("finite-deadline", ok)
        RESULTS["finite-deadline"] = {"error_code": code,
                                      "callee_handler_after": n_budget,
                                      "auth_state": AUTH_STATE}

        # 6) 有限 deadline（调用方发起前已过期）：外层预算充足，子调用显式
        #    已过期 deadline → 发送前 TimedOut，且 callee 无新 handler。
        env = call(driver, "sc-expired", "ForwardExpired", key="alpha")
        code = (v2.error_of(env) or {}).get("code")
        n_expired = len([e for e in read_journal(journal_file)
                         if e.get("event") == "callee_handler_entered"])
        ok = (v2.error_of(env) is not None and code == TIMED_OUT
              and n_expired == n_after_business)
        check(ok, f"expected TimedOut + no callee IO, got code={code} "
                  f"handlers={n_expired}", "expired-deadline")
        finish("expired-deadline", ok)
        RESULTS["expired-deadline"] = {"error_code": code,
                                       "callee_handler_after": n_expired,
                                       "auth_state": AUTH_STATE}

        # 7) 未配置静态路由：find_route 门 NotFound，不做 I/O。
        env = call(driver, "sc-noroute", "ForwardNoRoute", key="alpha")
        code = (v2.error_of(env) or {}).get("code")
        n_noroute = len([e for e in read_journal(journal_file)
                         if e.get("event") == "callee_handler_entered"])
        ok = (v2.error_of(env) is not None and code == NOT_FOUND
              and n_noroute == n_after_business)
        check(ok, f"expected NotFound + no IO, got code={code} "
                  f"handlers={n_noroute}", "no-route")
        finish("no-route", ok)

        # 8) 关闭：caller 先失效（其 handler 已排空），callee 随后；
        #    caller 的 CloseJournal 必须发生在最后一次 callee handler 进入之后。
        meta = {}
        with open(caller_meta_file) as f:
            for line in f:
                if "=" in line:
                    k, v = line.strip().split("=", 1)
                    meta[k] = v
        RESULTS["meta"] = dict(meta, auth_state=AUTH_STATE)

        driver.close()
        driver = None
        caller_rc = None
        if caller.stdin:
            caller.stdin.close()
        try:
            caller_rc = caller.wait(timeout=15)
        except subprocess.TimeoutExpired:
            caller.kill()
            caller.wait()
            caller_rc = caller.returncode
            check(False, "caller did not stop cleanly after stdin EOF",
                  "shutdown")
        callee_exit = None
        if callee.stdin:
            callee.stdin.close()
        try:
            callee_exit = callee.wait(timeout=15)
        except subprocess.TimeoutExpired:
            callee.kill()
            callee.wait()
            callee_exit = callee.returncode
            check(False, "callee did not stop cleanly after stdin EOF",
                  "shutdown")

        entries = read_journal(journal_file)
        last_handler = max((i for i, e in enumerate(entries)
                            if e.get("event") == "callee_handler_entered"),
                           default=-1)
        i_close = event_index(entries, "resource_close")
        i_shutdown = event_index(entries, "shutdown")
        shutdown_rec = entries[i_shutdown] if i_shutdown >= 0 else {}
        callee_shutdown = [
            e for e in entries if e.get("event") == "callee_shutdown"]

        auth_ok = all(e.get("auth_state") == AUTH_STATE for e in entries)
        peer_upgraded = [e.get("req") for e in entries
                         if e.get("peer_principal")]
        ok = (caller_rc == 0 and callee_exit == 0
              and shutdown_rec.get("rc") == "0"
              and shutdown_rec.get("state") == "3"      # ShutdownState::Closed
              and shutdown_rec.get("owner_closed") == "1"
              and len(callee_shutdown) == 1
              and callee_shutdown[0].get("rc") == "0"
              and i_close >= 0 and last_handler >= 0 and last_handler < i_close
              and event_index(entries, "resource_close", 2) == -1
              and auth_ok and not peer_upgraded)
        check(ok, f"caller_rc={caller_rc} callee_rc={callee_exit} "
                  f"shutdown={shutdown_rec} last_handler={last_handler} "
                  f"resource_close_at={i_close} entries={len(entries)} "
                  f"peer_upgraded={peer_upgraded}", "shutdown-and-close")
        finish("shutdown-and-close", ok)
        RESULTS["shutdown"] = {
            "exit_code": caller_rc,
            "callee_exit_code": callee_exit,
            "shutdown_state": shutdown_rec.get("state"),
            "owner_closed": shutdown_rec.get("owner_closed"),
            "callee_handler_entered": last_handler + 1 if last_handler >= 0 else 0,
            "resource_close_calls": len([e for e in entries
                                         if e.get("event") == "resource_close"]),
            "resource_close_after_last_handler":
                bool(i_close >= 0 and last_handler >= 0 and last_handler < i_close),
            "auth_state": AUTH_STATE}
        caller = None   # 已回收，避免 finally 重复处理
        callee = None
    except Exception as exc:  # noqa: BLE001
        _FAILURES.append(f"driver-error {type(exc).__name__}: {exc}")
        print(f"DRIVER-ERROR {type(exc).__name__}: {exc}", file=sys.stderr)
        traceback.print_exc()
    finally:
        if driver is not None:
            try:
                driver.close()
            except OSError:
                pass
        reap("caller", caller, _FAILURES)
        reap("callee", callee, _FAILURES)
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
