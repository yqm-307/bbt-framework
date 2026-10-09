#!/usr/bin/env python3
"""bbt-framework Issue #4（P0-A）：正式 Service→Service 出站跨进程验收驱动器。

两个真实 framework CoApp，各自独立进程：
  - callee 进程（getvalue_server）：宿主 GetValueService（正式 ProtoWireV1 入站）
  - caller 进程（getvalue_caller）：宿主 GetValueCallerService，其 handler 内
    经 IWorkerService::call<Req,Resp>（typed seam）向 callee 发起真实正式 body
    出站（HTTP loopback + application/x-protobuf）
外加一个「完整读下请求后断开（丢 reply）」的真实 TCP 端点作为黑障目标。

本驱动器只用 Python 标准库手写 proto3 编解码（不依赖 google.protobuf），
既做输入又做字节级断言。

断言（结构化结果均带 auth_state=unauthenticated_loopback）：
  - s2s-known / s2s-miss / s2s-empty：出站 typed 调用成功 / miss / 业务错误透传
  - s2s-expired：发送前已过期 → caller 侧 TimedOut，callee journal 无新 handler
  - s2s-noroute：find_route 门拒绝，callee journal 无新 handler
  - s2s-blackhole-unknown：请求已完整写出后对端丢 reply（读完整个请求再断开）
    → caller 得到 OutcomeUnknown(14)，非未提交传输错误；对端只被连接一次（无 retry），
    callee journal 无新 handler
  - s2s-badroute-{empty-service,empty-transport,empty-endpoint,duplicate}（#5 EX-T3）：
    caller 以公开入口载入四类非法静态路由 → run 启动任何组件前拒绝
    （rc=1=kExitRejected），lifecycle_failures 恰 1 条且为对应原因，bound_endpoint
    为空，资源工厂计数 0（拒绝断言由驱动器独立核对进程观测，不信任自报结论）

结果完整性（本轮修复）：总体结论（tail）只在自有 caller/callee/blackhole 全部
清理、退出码检查完成后生成；异常/崩溃/非 0/超时强杀一律记 fail，且 exit 1，
结构化 status/tail/failures 互相一致，不再先打印 ALL-PASS 再报 fail。信号名由
真实 returncode 经 signal.Signals 映射，未采集到本次真实调用栈时 stack=unknown。

用法：
  getvalue_s2s_run.py --callee-bin <getvalue_server> --caller-bin <getvalue_caller>
                      --workdir <dir> [--results <json>] [--timeout 30]
"""

import argparse
import http.client
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback

RPC_PATH = "/rpc"
CONTENT_TYPE = "application/x-protobuf"
AUTH_STATE = "unauthenticated_loopback"

CALLER_SERVICE = "bbt.example.v1.GetValueCallerService"
REQUEST_SCHEMA = "bbt.example.v1.GetValueRequest"
RESPONSE_SCHEMA = "bbt.example.v1.GetValueResponse"

# RpcErrorCode（infra rpc_envelope.proto enum）
INVALID_ARGUMENT = 1
TIMED_OUT = 6
NOT_FOUND = 11
OUTCOME_UNKNOWN = 14   # RpcErrorCode.RPC_ERROR_CODE_OUTCOME_UNKNOWN

# #5 EX-T3：四类非法静态路由 → 公开入口 run 前拒绝。期望的 lifecycle_failures
# 单条子串来自 CoApp::_ValidateConfig 的对应校验分支。
BADROUTE_CASES = [
    ("empty-service", "empty service_name"),
    ("empty-transport", "has empty transport/endpoint"),
    ("empty-endpoint", "has empty transport/endpoint"),
    ("duplicate", "duplicate static route service_name"),
]

WIRE_VARINT = 0
WIRE_LEN = 2
RESULTS = {}


# --- proto3 wire format 最小编解码（与 getvalue_client.py 同源写法） -----------

def _varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _tag(field, wire):
    return _varint((field << 3) | wire)


def _f_varint(field, n):
    return _tag(field, WIRE_VARINT) + _varint(n)


def _f_len(field, data):
    if isinstance(data, str):
        data = data.encode("utf-8")
    return _tag(field, WIRE_LEN) + _varint(len(data)) + data


def _read_varint(buf, pos):
    shift = 0
    result = 0
    while True:
        if pos >= len(buf):
            raise ValueError("truncated varint")
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7
        if shift >= 70:
            raise ValueError("varint overflow")


def _parse_fields(buf):
    pos = 0
    out = []
    while pos < len(buf):
        key, pos = _read_varint(buf, pos)
        field = key >> 3
        wire = key & 7
        if field == 0:
            raise ValueError("field 0 invalid")
        if wire == WIRE_VARINT:
            v, pos = _read_varint(buf, pos)
            out.append((field, wire, v))
        elif wire == WIRE_LEN:
            n, pos = _read_varint(buf, pos)
            if pos + n > len(buf):
                raise ValueError("truncated LEN field")
            out.append((field, wire, buf[pos:pos + n]))
            pos += n
        else:
            raise ValueError(f"unsupported wire type {wire}")
    return out


def enc_get_value_request(key):
    return _f_len(1, key)


def dec_get_value_response(buf):
    found = False
    value = ""
    for f, w, v in _parse_fields(buf):
        if w == WIRE_VARINT and f == 1:
            found = bool(v)
        elif w == WIRE_LEN and f == 2:
            value = v.decode("utf-8")
    return found, value


def enc_envelope(*, request_id, service, method, budget_ms, request_schema,
                 response_schema, payload):
    out = _f_varint(1, 1)
    out += _f_len(2, request_id)
    out += _f_len(3, service)
    out += _f_len(4, method)
    out += _f_varint(5, budget_ms)
    out += _f_len(6, request_schema)
    out += _f_len(7, response_schema)
    out += _f_len(8, payload)
    return out


def dec_error(buf):
    err = {"details": []}
    for f, w, v in _parse_fields(buf):
        if w == WIRE_VARINT and f == 1:
            err["code"] = v
        elif w == WIRE_LEN and f == 2:
            err["domain"] = v.decode("utf-8")
        elif w == WIRE_LEN and f == 3:
            err["domain_code"] = v.decode("utf-8")
        elif w == WIRE_LEN and f == 4:
            err["message"] = v.decode("utf-8")
    return err


def dec_envelope(buf):
    env = {"metadata": []}
    for f, w, v in _parse_fields(buf):
        if w == WIRE_VARINT:
            if f == 1:
                env["profile_version"] = v
            elif f == 5:
                env["remaining_budget_ms"] = v
            elif f == 10:
                env["success"] = bool(v)
        elif w == WIRE_LEN:
            if f == 2:
                env["request_id"] = v.decode("utf-8")
            elif f == 3:
                env["service"] = v.decode("utf-8")
            elif f == 4:
                env["method"] = v.decode("utf-8")
            elif f == 6:
                env["request_schema"] = v.decode("utf-8")
            elif f == 7:
                env["response_schema"] = v.decode("utf-8")
            elif f == 8:
                env["payload"] = bytes(v)
            elif f == 11:
                env["error"] = dec_error(v)
    return env


def post_raw(port, body):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request("POST", RPC_PATH, body=body,
                 headers={"Content-Type": CONTENT_TYPE})
    resp = conn.getresponse()
    data = resp.read()
    status = resp.status
    conn.close()
    return status, dec_envelope(data)


def call(port, *, request_id, method, key, budget_ms=3000):
    payload = enc_get_value_request(key)
    body = enc_envelope(request_id=request_id, service=CALLER_SERVICE,
                        method=method, budget_ms=budget_ms,
                        request_schema=REQUEST_SCHEMA,
                        response_schema=RESPONSE_SCHEMA, payload=payload)
    return post_raw(port, body)


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


def signal_name(num):
    """真实信号号 → 精确名称；未知号退化为 signal-N，绝不冒充 SIGSEGV。"""
    try:
        return signal.Signals(num).name
    except (ValueError, AttributeError):
        return f"signal-{num}"


def summarize(failures):
    """总体结论只由清理后的最终 failures 决定，tail/failures/final_failures 一致。"""
    tail = "S2S-ALL-PASS" if not failures else "S2S-FAILURES"
    return {"failures": list(failures), "final_failures": list(failures),
            "tail": tail, "auth_state": AUTH_STATE}


def read_journal(path):
    entries = []
    if not os.path.exists(path):
        return entries
    with open(path) as f:
        for line in f:
            line = line.strip()
            if "event=handler_entered" in line:
                rec = {}
                for kv in line.split(" "):
                    if "=" in kv:
                        k, v = kv.split("=", 1)
                        rec[k] = v
                entries.append(rec)
    return entries


def wait_port(path, proc, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"process exited early rc={proc.returncode}")
        try:
            with open(path) as f:
                p = int(f.read().strip())
            if p > 0:
                return p
        except (OSError, ValueError):
            pass
        time.sleep(0.05)
    raise RuntimeError("did not write port file")


class Blackhole:
    """真实 TCP「完整读完请求后不回复直接断开」端点（丢 reply）。

    与「accept 后立即 close」不同：本端点先完整收下 HTTP 请求（头 + Content-Length
    body）再断开，使客户端请求字节已完整写出（RequestCommitted）后才失去可信回复，
    得到 OutcomeUnknown（wire 错误码 14），而不是未提交的确定传输错误。记录被连接
    次数，供「一次终态、无 retry」断言。
    """

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(8)
        self.port = self.sock.getsockname()[1]
        self._stop = False
        self._accepts = 0
        self._lock = threading.Lock()
        self._t = threading.Thread(target=self._serve, daemon=True)
        self._t.start()

    @property
    def accepts(self):
        with self._lock:
            return self._accepts

    def _serve(self):
        self.sock.settimeout(0.2)
        while not self._stop:
            try:
                conn, _ = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with self._lock:
                self._accepts += 1
            try:
                self._read_full_request(conn)
            finally:
                try:
                    conn.close()   # 已完整收下请求 → 不回复直接断开
                except OSError:
                    pass

    @staticmethod
    def _read_full_request(conn):
        """读到完整 HTTP 请求（头 + Content-Length body）为止；超时/中断即返回。"""
        conn.settimeout(5)
        buf = bytearray()
        header_end = -1
        content_length = -1
        while True:
            if header_end < 0:
                header_end = buf.find(b"\r\n\r\n")
                if header_end >= 0:
                    head = bytes(buf[:header_end]).lower()
                    cl = -1
                    for line in head.split(b"\r\n"):
                        if line.startswith(b"content-length:"):
                            cl = int(line.split(b":", 1)[1].strip())
                    content_length = cl if cl >= 0 else 0
            if header_end >= 0 and content_length >= 0:
                if len(buf) - (header_end + 4) >= content_length:
                    return
            try:
                chunk = conn.recv(4096)
            except (socket.timeout, OSError):
                return
            if not chunk:
                return
            buf.extend(chunk)

    def close(self):
        self._stop = True
        try:
            self.sock.close()
        except OSError:
            pass


def _reap(name, proc, failures):
    """有限等待并回收一个自有子进程；非 0 / 信号退出记 fail，不猜测调用栈。"""
    if proc is None:
        return
    if proc.stdin:
        try:
            proc.stdin.close()
        except OSError:
            pass
    killed = False
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        killed = True
        failures.append(
            f"{name} pid={proc.pid} did not stop cleanly (killed after timeout)")
    if killed:
        return
    rc = proc.returncode
    if rc is None:
        return
    if rc < 0:
        sig = -rc
        sname = signal_name(sig)
        failures.append(
            f"teardown-crash {name} pid={proc.pid} signal={sig} ({sname})")
        RESULTS["s2s-teardown-crash"] = {
            "process": name, "pid": proc.pid,
            "signal": sig, "signal_name": sname,
            "stack": "unknown",
            "status": "fail", "auth_state": AUTH_STATE}
    elif rc != 0:
        failures.append(f"{name} pid={proc.pid} rc={rc}")


def run_badroute(caller_bin, wd, kind, expect, timeout):
    """#5 EX-T3：独立进程经公开入口验证非法静态路由 run 前拒绝。

    只读该进程自己写的机器可读观测（rc/lifecycle_failures/bound_endpoint/
    resource_factory_calls），断言由驱动器独立做出，不信任进程自报结论；
    超时/异常/崩溃一律记 fail 并回收自有进程。"""
    scenario = f"s2s-badroute-{kind}"
    result_f = os.path.join(wd, f"badroute-{kind}.json")
    rc = None
    obs = {}
    try:
        try:
            os.unlink(result_f)
        except OSError:
            pass
        proc = subprocess.Popen(
            [caller_bin, "badroute", kind, result_f],
            stdin=subprocess.DEVNULL, stdout=sys.stderr, stderr=sys.stderr)
        try:
            rc = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            _FAILURES.append(
                f"{scenario}: process did not exit within {timeout}s (killed)")
        if os.path.exists(result_f):
            with open(result_f) as fh:
                obs = json.load(fh)
    except Exception as exc:  # noqa: BLE001 - 任何驱动异常都要落到结构化结果
        _FAILURES.append(f"{scenario}: driver-error {type(exc).__name__}: {exc}")

    failures = obs.get("lifecycle_failures")
    endpoint = obs.get("bound_endpoint")
    calls = obs.get("resource_factory_calls")
    ok = (
        rc == 1                        # kExitRejected：校验失败，未启动组件
        and obs.get("rc") == 1         # 进程内观测与退出码一致
        and obs.get("kind") == kind
        and isinstance(failures, list) and len(failures) == 1
        and expect in failures[0]      # 精确对应本次非法输入的原因
        and endpoint == ""             # 未绑定任何 ready 端口
        and calls == 0                 # 资源工厂未被调用（无组件启动）
        and obs.get("auth_state") == AUTH_STATE
    )
    check(ok, f"rc={rc} obs={obs}", scenario)
    finish(scenario, ok)
    # 指标在 finish() 之后写入（finish 会重置该场景条目），保留 status/auth_state。
    RESULTS[scenario].update({
        "rc": rc,
        "kind": kind,
        "expect_failure_substring": expect,
        "lifecycle_failures": failures,
        "bound_endpoint": endpoint,
        "resource_factory_calls": calls,
    })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--callee-bin", required=True)
    ap.add_argument("--caller-bin", required=True)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--results")
    ap.add_argument("--timeout", type=float, default=30.0)
    args = ap.parse_args()

    wd = args.workdir
    callee_port_f = os.path.join(wd, "callee.port")
    callee_meta_f = os.path.join(wd, "callee.meta")
    callee_jrnl_f = os.path.join(wd, "callee.journal")
    caller_port_f = os.path.join(wd, "caller.port")
    caller_meta_f = os.path.join(wd, "caller.meta")

    blackhole = None
    callee = None
    caller = None
    try:
        os.makedirs(wd, exist_ok=True)
        for p in (callee_port_f, callee_meta_f, callee_jrnl_f, caller_port_f,
                  caller_meta_f):
            try:
                os.unlink(p)
            except OSError:
                pass

        # 0) #5 EX-T3：四类非法静态路由 → 公开入口 run 前拒绝（四个独立进程）。
        for kind, expect in BADROUTE_CASES:
            run_badroute(args.caller_bin, wd, kind, expect, args.timeout)

        blackhole = Blackhole()

        # 1) callee 进程（GetValueService）。
        callee = subprocess.Popen(
            [args.callee_bin, "server", callee_port_f, callee_meta_f,
             callee_jrnl_f],
            stdin=subprocess.PIPE, stdout=sys.stderr, stderr=sys.stderr)
        callee_port = wait_port(callee_port_f, callee, args.timeout)
        print(f"callee up port={callee_port}", file=sys.stderr)

        # 2) caller 进程（GetValueCallerService），指向 callee 与 blackhole。
        callee_ep = f"127.0.0.1:{callee_port}"
        black_ep = f"127.0.0.1:{blackhole.port}"
        caller = subprocess.Popen(
            [args.caller_bin, "caller", caller_port_f, callee_ep, black_ep,
             caller_meta_f],
            stdin=subprocess.PIPE, stdout=sys.stderr, stderr=sys.stderr)
        caller_port = wait_port(caller_port_f, caller, args.timeout)
        print(f"caller up port={caller_port} blackhole={black_ep}",
              file=sys.stderr)

        # 3) 出站 typed 调用场景：经 caller 进程转发到 callee（真实 S2S）。
        st, env = call(caller_port, request_id="sc-known", method="Forward",
                       key="alpha")
        ok = (st == 200 and env.get("success") is True
              and env.get("request_id") == "sc-known"
              and env.get("response_schema") == RESPONSE_SCHEMA
              and dec_get_value_response(env.get("payload", b"")) ==
              (True, "value-alpha"))
        check(ok, f"got {env}", "s2s-known")
        finish("s2s-known", ok)

        st, env = call(caller_port, request_id="sc-miss", method="Forward",
                       key="no-such-key")
        ok = (env.get("success") is True
              and dec_get_value_response(env.get("payload", b"")) == (False, ""))
        check(ok, f"got {env}", "s2s-miss")
        finish("s2s-miss", ok)

        st, env = call(caller_port, request_id="sc-empty", method="Forward",
                       key="")
        code = env.get("error", {}).get("code")
        # proto3 省略默认 false：错误用例以 error 字段存在判定，不读 success。
        ok = ("error" in env and code == INVALID_ARGUMENT)
        check(ok, f"expected InvalidArgument via S2S, got {env}", "s2s-empty")
        RESULTS["s2s-empty-error"] = {"code": code,
                                      "message": env.get("error", {}).get(
                                          "message", ""),
                                      "auth_state": AUTH_STATE}
        finish("s2s-empty", ok)

        n_after_business = len(read_journal(callee_jrnl_f))
        check(n_after_business == 3,
              f"callee handler count expected 3, got {n_after_business}",
              "s2s-business-journal")
        RESULTS["s2s-business-journal"] = {"handler_entered": n_after_business,
                                           "auth_state": AUTH_STATE}

        # 4) 发送前已过期：caller 侧 TimedOut，且 callee 不得收到任何请求。
        st, env = call(caller_port, request_id="sc-expired",
                       method="ForwardExpired", key="alpha")
        code = env.get("error", {}).get("code")
        n_expired = len(read_journal(callee_jrnl_f))
        ok = ("error" in env and code == TIMED_OUT
              and n_expired == n_after_business)
        check(ok, f"expected TimedOut + no callee IO, got code={code} "
                  f"callee_handler={n_expired}", "s2s-expired")
        RESULTS["s2s-expired"] = {"error_code": code,
                                  "callee_handler_after": n_expired,
                                  "auth_state": AUTH_STATE}
        finish("s2s-expired", ok)

        # 5) 无静态路由：find_route 门拒绝，不做 I/O。
        st, env = call(caller_port, request_id="sc-noroute",
                       method="ForwardNoRoute", key="alpha")
        code = env.get("error", {}).get("code")
        n_noroute = len(read_journal(callee_jrnl_f))
        ok = ("error" in env and code == NOT_FOUND
              and n_noroute == n_after_business)
        check(ok, f"expected NotFound + no IO, got code={code} "
                  f"callee_handler={n_noroute}", "s2s-noroute")
        RESULTS["s2s-noroute"] = {"error_code": code,
                                  "callee_handler_after": n_noroute,
                                  "auth_state": AUTH_STATE}
        finish("s2s-noroute", ok)

        # 6) 请求已完整写出后丢 reply（真 OutcomeUnknown）：对端读完整个请求后
        #    断开，客户端请求已提交；一次终态、无 retry（对端只被连接一次）。
        accepts_before = blackhole.accepts
        st, env = call(caller_port, request_id="sc-blackhole",
                       method="ForwardBlackhole", key="alpha")
        code = env.get("error", {}).get("code")
        n_black = len(read_journal(callee_jrnl_f))
        black_accepts = blackhole.accepts - accepts_before
        ok = ("error" in env and code == OUTCOME_UNKNOWN
              and n_black == n_after_business and black_accepts == 1)
        check(ok, f"expected OutcomeUnknown(14) after committed reply loss + "
                  f"exactly one connection, got code={code} "
                  f"blackhole_accepts={black_accepts} env={env}",
              "s2s-blackhole-unknown")
        finish("s2s-blackhole-unknown", ok)
        # 指标在 finish() 之后写入（finish 会重置该场景条目），保留 status/auth_state。
        RESULTS["s2s-blackhole-unknown"].update({
            "error_code": code,
            "error_domain": env.get("error", {}).get("domain", ""),
            "error_message": env.get("error", {}).get("message", ""),
            "blackhole_accepts": black_accepts,
            "callee_handler_after": n_black})

        # 7) callee journal 终态：仅 3 条业务处理，principal 未被升级。
        entries = read_journal(callee_jrnl_f)
        auth_ok = all(e.get("auth_state") == AUTH_STATE for e in entries)
        peer_upgraded = [e.get("req") for e in entries
                         if e.get("peer_principal")]
        ok = (len(entries) == 3 and auth_ok and not peer_upgraded)
        check(ok, f"journal terminal entries={len(entries)} "
                  f"peer_upgraded={peer_upgraded}", "s2s-journal-terminal")
        RESULTS["s2s-journal-terminal"] = {
            "handler_entered": len(entries),
            "auth_state_ok": auth_ok,
            "peer_upgraded": peer_upgraded,
            "auth_state": AUTH_STATE}
        finish("s2s-journal-terminal", ok)
    except Exception as exc:  # noqa: BLE001 - 任何 setup/断言异常都要落到结构化结果
        _FAILURES.append(f"driver-error {type(exc).__name__}: {exc}")
        print(f"DRIVER-ERROR {type(exc).__name__}: {exc}", file=sys.stderr)
        traceback.print_exc()
    finally:
        # 清理所有自有的 caller/callee/blackhole，退出检查先于总体结论。
        _reap("caller", caller, _FAILURES)
        _reap("callee", callee, _FAILURES)
        if blackhole is not None:
            blackhole.close()

    # 总体结论：只在清理与退出检查完成后生成，tail/failures 与最终一致。
    RESULTS["_summary"] = summarize(_FAILURES)
    print(f"GRAND {RESULTS['_summary']['tail']} auth_state={AUTH_STATE}")

    if args.results:
        with open(args.results, "w") as f:
            json.dump(RESULTS, f, indent=2, sort_keys=True, ensure_ascii=False)

    if _FAILURES:
        print(f"S2S-DRIVER-FAIL {_FAILURES}", file=sys.stderr)
        sys.exit(1)
    print("S2S-DRIVER-PASS auth_state=unauthenticated_loopback")


if __name__ == "__main__":
    main()
