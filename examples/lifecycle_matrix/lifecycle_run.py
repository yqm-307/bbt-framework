#!/usr/bin/env python3
"""bbt-framework 容量 / 并发上下文 / 关闭生命周期验收驱动器。

由 ctest 调用：
  lifecycle_run.py --bin <lifecycle_fixture> [--results <json>]

职责（R3–R5 的真实运行证据，全部为真实公开入口 + 自有进程 + 动态 loopback）：
  1. spawn 自有 fixture 进程（真实公开 CoApp + real infra V2 会话），
     就绪以 port 文件为准；
  2. 经唯一 V2 codec（tests/rpc_v2_wire.py）打真实 Call；业务 payload 复用
     getvalue 切片的位置参数 codec；
  3. 场景：
     - S3 并发挂起两项，放行后上下文/协程不串（req_id/co_id 跨挂起一致）；
     - S3 客户端读超时后 work 仍物理在途占容量（新请求精确 Overloaded）；
     - R4 服务容量耗尽 → Overloaded（不无界排队）+ 字节上限维度；
     - R5 正常拒新 / drain / 同步 Close / release；
     - R5 ShutdownIncomplete 保留强持有、迟到完成、晚到无 UAF；
     - R5 外部 supervisor 硬杀只留「未完成」证据、不称优雅完成。
  4. 收尾：stdin 触发关闭、有限等待；异常路径回收自有进程。

会话/分帧/握手/错误信封复用 tests/rpc_v2_wire.py，业务 payload 复用
examples/getvalue/getvalue_client.py——不另造第二套 schema/codec。全部结构化
结果带 auth_state=unauthenticated_loopback。
"""

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(HERE, "..", "getvalue"))
sys.path.insert(0, os.path.join(HERE, "..", "..", "tests"))
try:
    import rpc_v2_wire as v2
    import getvalue_client as gvc
except Exception as exc:  # noqa: BLE001
    print(f"driver: cannot import V2 codec (tests/rpc_v2_wire.py) or "
          f"business codec (examples/getvalue/getvalue_client.py): {exc}",
          file=sys.stderr)
    sys.exit(9)

AUTH_STATE = "unauthenticated_loopback"
SERVICE = "bbt.example.v1.LifecycleGateService"
REQUEST_SCHEMA = "bbt.example.v1.GetValueRequest"
RESPONSE_SCHEMA = "bbt.example.v1.GetValueResponse"
# 会话身份：驱动器客户端与 fixture 互异（V2 拒绝 self-connection）。
IDENTITY = "lifecycle-driver"

# RpcErrorCode（session_frame.proto enum，与 infra ErrorCode 同序）
OVERLOADED = v2.ERR_OVERLOADED

RESULTS = {}
_FAILURES = []


def check(cond, label, scenario):
    if not cond:
        _FAILURES.append(f"{scenario}: {label}")
        print(f"FAIL {scenario}: {label}", file=sys.stderr)


def finish(scenario, ok, extra=None):
    rec = {"status": "pass" if ok else "fail", "auth_state": AUTH_STATE}
    if extra:
        rec.update(extra)
    RESULTS[scenario] = rec
    if ok:
        print(f"PASS scenario={scenario} auth_state={AUTH_STATE}")


# ── journal ────────────────────────────────────────────────────────────────

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


def wait_journal(path, pred, timeout, proc=None):
    """有界等待直到某条目满足 pred（观察即成事件，非固定 sleep 猜时序）。"""
    end = time.monotonic() + timeout
    last = []
    while True:
        last = read_journal(path)
        if pred(last):
            return last
        if time.monotonic() >= end:
            return last
        if proc is not None and proc.poll() is not None:
            return read_journal(path)
        time.sleep(0.02)


def event_index(entries, sign, occurrence=1):
    seen = 0
    for i, e in enumerate(entries):
        if e.get("event") == sign:
            seen += 1
            if seen == occurrence:
                return i
    return -1


# ── V2 over raw socket（复用唯一 codec；可控制超时/延迟读取）───────────

def open_and_send(port, *, request_id, key="hold", method="Hold",
                  budget_ms=30000, timeout=10, identity=None):
    """连接 + Hello/HelloAck 握手 + 发一帧 CALL；返回 (socket, FrameStream)。

    调用方自行控制读取时机/超时——这是「客户端读超时但 work 仍物理在途」
    与「迟到回复」两组断言的构造基础。identity 默认是本驱动器的身份；需要
    同时保持多条活跃会话时，每条会话必须用互异 identity（V2 同一 peer
    identity 只保留一条活跃 link，重复 link 按 fencing 规则退役）。
    """
    ident = identity or IDENTITY
    s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    s.settimeout(timeout)
    stream = v2.FrameStream(s)
    epoch = v2.client_handshake(stream, ident, int(time.time() * 1000),
                                v2.nonce_for(ident))
    stream.send({
        "kind": v2.KIND_CALL,
        "traffic": v2.TRAFFIC_SERVICE,
        "session_epoch": epoch,
        "request_id": request_id,
        "service": SERVICE,
        "method": method,
        "reply": v2.REPLY_REQUIRED,
        "remaining_budget_ms": budget_ms,
        "request_schema": REQUEST_SCHEMA,
        "response_schema": RESPONSE_SCHEMA,
        "payload": gvc.enc_get_value_request(key),
        "metadata": [],
    })
    return s, stream


def rpc_call(port, request_id, method="Fast", key="k", budget_ms=30000,
             timeout=10):
    """单次同步调用 → 响应帧。"""
    s, stream = open_and_send(port, request_id=request_id, key=key,
                              method=method, budget_ms=budget_ms,
                              timeout=timeout)
    try:
        return stream.read()
    finally:
        s.close()


def wait_port(path, proc, timeout):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
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


# ── fixture 进程封装 ────────────────────────────────────────────────────────

class Fixture:
    def __init__(self, binpath, workdir, tag, max_inflight, step_budget_ms):
        self.tag = tag
        self.dir = os.path.join(workdir, tag)
        os.makedirs(self.dir, exist_ok=True)
        self.port_file = os.path.join(self.dir, "port")
        self.meta_file = os.path.join(self.dir, "meta")
        self.journal = os.path.join(self.dir, "journal")
        for p in (self.port_file, self.meta_file, self.journal):
            try:
                os.unlink(p)
            except OSError:
                pass
        self.proc = subprocess.Popen(
            [binpath, "fixture", self.port_file, self.meta_file, self.journal,
             str(max_inflight), str(step_budget_ms)],
            stdin=subprocess.PIPE, stdout=sys.stderr, stderr=sys.stderr)
        self.port = None
        self.expect_rc = 0            # 预期退出码（graceful/正常）
        self.expect_signal = None     # 预期信号（supervisor 硬杀）

    def start(self, timeout):
        self.port = wait_port(self.port_file, self.proc, timeout)
        return self.port

    def cmd(self, line):
        self.proc.stdin.write((line + "\n").encode())
        self.proc.stdin.flush()

    def quit(self):
        """放行 fixture：stdin 读到 quit/EOF 后收尾（否则主线程停在 getline）。"""
        try:
            self.cmd("quit")
        except (OSError, ValueError):
            pass

    def entries(self):
        return read_journal(self.journal)

    def meta(self):
        m = {}
        try:
            with open(self.meta_file) as f:
                for line in f:
                    if "=" in line:
                        k, v = line.strip().split("=", 1)
                        m[k] = v
        except OSError:
            pass
        return m

    def wait_exit(self, timeout):
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return False
        return True

    def kill_sigkill(self):
        try:
            self.proc.send_signal(signal.SIGKILL)
        except OSError:
            pass
        self.proc.wait()

    def alive(self):
        return self.proc.poll() is None

    def reap(self, failures):
        p = self.proc
        if p is None:
            return
        if p.poll() is None:
            if p.stdin:
                try:
                    p.stdin.close()
                except OSError:
                    pass
            try:
                p.wait(timeout=15)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
                failures.append(f"{self.tag} pid={p.pid} did not stop cleanly "
                                f"(killed after timeout)")
                self.proc = None
                return
        rc = p.returncode
        if rc is None:
            return
        if self.expect_signal is not None:
            want = -abs(int(self.expect_signal))
            if rc != want:
                failures.append(f"{self.tag} pid={p.pid} rc={rc} "
                                f"(expected signal {self.expect_signal})")
        elif rc != self.expect_rc:
            failures.append(f"{self.tag} pid={p.pid} rc={rc} "
                            f"(expected rc={self.expect_rc})")
        self.proc = None


# ── 场景 ────────────────────────────────────────────────────────────────────

def scenario_r3_r4(fx):
    """R3 并发挂起/上下文不串 + 客户端读超时仍占容量 + R4 Overloaded 不排队 + 恢复。"""
    def both_entered(es):
        entered = {e.get("req") for e in es
                   if e.get("event") == "handler_entered"}
        return {"r3-a", "r3-b"} <= entered

    s_a, f_a = open_and_send(fx.port, request_id="r3-a", key="hold",
                             timeout=60, identity=IDENTITY + "-r3a")
    s_b, f_b = open_and_send(fx.port, request_id="r3-b", key="hold",
                             timeout=60, identity=IDENTITY + "-r3b")
    es = wait_journal(fx.journal, both_entered, 12, fx.proc)
    check(both_entered(es), "two Hold handlers not both entered", "r3-concurrent")

    # 客户端读超时（连接保持打开，不主动断连）：两项都取不到回复。
    read_timeout = {}
    for name, sock, stream in (("r3-a", s_a, f_a), ("r3-b", s_b, f_b)):
        sock.settimeout(0.8)
        try:
            stream.read()
            read_timeout[name] = "unexpected-reply"
        except socket.timeout:
            read_timeout[name] = "timeout"
        except Exception as exc:  # noqa: BLE001
            read_timeout[name] = f"err:{type(exc).__name__}"
    check(read_timeout == {"r3-a": "timeout", "r3-b": "timeout"},
          f"expected both client reads to time out while held, got {read_timeout}",
          "r3-read-timeout-still-inflight")

    # 此刻两项仍物理在途（无 resumed）→ 容量满 → 新请求精确 Overloaded（不排队）。
    held_entries = fx.entries()
    check(not [e for e in held_entries if e.get("event") == "handler_resumed"],
          "a held handler resumed before open (unexpected)", "r4-overloaded")
    env = rpc_call(fx.port, "r3-probe", "Fast", "kprobe")
    code = (v2.error_of(env) or {}).get("code")
    # proto3 省略 false：错误信封以 error 字段携带 code，success 字段缺省。
    probe_entered = any(e.get("event") == "handler_entered"
                        and e.get("req") == "r3-probe"
                        for e in fx.entries())
    ok_over = (v2.error_of(env) is not None and code == OVERLOADED
               and not probe_entered)
    check(ok_over, f"expected Overloaded (7) before handler for probe, got "
                   f"env={env} probe_entered={probe_entered}",
          "r4-overloaded")
    finish("r4-overloaded", ok_over,
           {"probe_error_code": code,
            "held_read_timeout": read_timeout})
    finish("r3-read-timeout-still-inflight",
           read_timeout == {"r3-a": "timeout", "r3-b": "timeout"},
           {"note": "client read deadline elapsed; connection kept open; "
                    "work still counted inflight (probe Overloaded)"})

    # 放行 → 两项迟到完成；上下文/协程跨挂起不串。
    fx.cmd("open")
    es = wait_journal(
        fx.journal,
        lambda es: len([e for e in es if e.get("event") == "handler_resumed"]) >= 2,
        12, fx.proc)
    resumed = [e for e in es if e.get("event") == "handler_resumed"]
    check(len(resumed) == 2, f"expected 2 handler_resumed, got {len(resumed)}",
          "r3-concurrent")
    for e in resumed:
        check(e.get("same_ctx") == "1" and e.get("ctx_id") == e.get("req"),
              f"context crossed across suspend/resume: {e}", "r3-concurrent")
        check(e.get("same_co") == "1",
              f"coroutine id drifted across suspend/resume: {e}", "r3-concurrent")
    check({e.get("req") for e in resumed} == {"r3-a", "r3-b"},
          f"resumed request ids unexpected: {resumed}", "r3-concurrent")

    # 迟到回复仍可读出（工作真实完成、未悬空）。
    replies = {}
    for name, sock, stream in (("r3-a", s_a, f_a), ("r3-b", s_b, f_b)):
        sock.settimeout(10)
        try:
            env = stream.read()
            replies[name] = (v2.error_of(env) is None,
                             env.get("request_id"),
                             gvc.dec_get_value_response(env.get("payload", b"")))
        except Exception as exc:  # noqa: BLE001
            replies[name] = (None, None, f"err:{type(exc).__name__}")
    for name in ("r3-a", "r3-b"):
        succ, rid, gv = replies[name]
        check(succ is True and rid == name and gv == (True, f"held:{name}"),
              f"late reply for {name} wrong: {replies[name]}", "r3-concurrent")
    finish("r3-concurrent",
           not [f for f in _FAILURES if f.startswith("r3-concurrent")],
           {"resumed": [{k: e.get(k) for k in ("req", "ctx_id", "same_ctx",
                                               "same_co", "status")}
                        for e in resumed],
            "late_replies": {k: {"success": v[0], "request_id": v[1],
                                 "payload": str(v[2])}
                             for k, v in replies.items()}})
    for s in (s_a, s_b):
        try:
            s.close()
        except OSError:
            pass

    # 容量释放后可继续接纳（非原调用 retry 的独立探测）。
    env = rpc_call(fx.port, "r3-recover", "Fast", "krecover")
    ok = (v2.error_of(env) is None
          and gvc.dec_get_value_response(env.get("payload", b"")) ==
          (True, "fast:krecover"))
    check(ok, f"recovery probe failed: {env}", "r4-recovered")
    finish("r4-recovered", ok)


def scenario_r4_byte_limit(fx):
    """R4 字节上限维度：超过 NetworkLimits.max_body_bytes 的帧被拒（功能检查）。

    V2 的 frame 前缀上限 = limits.max_body_bytes：超限帧在解码边界被拒。非法
    帧的拒绝形态是 SESSION_ERROR(ProtocolError) 帧、随后 link 退役；若退役先
    于读端完成，读端也可能直接看到 EOF/reset。两者都算真实拒绝，但读超时或
    任何非预期异常一律判失败，不放宽为「被拒」。本场景断言「请求被真实拒绝
    且未触达 handler、服务器健康」。"""
    big_key = "x" * 8000   # 帧 body 远超 max_body_bytes=4096
    outcome = None
    rejected = None
    sock = None
    try:
        sock, stream = open_and_send(fx.port, request_id="r4-big", key=big_key,
                                     method="Fast", timeout=5)
        try:
            env = stream.read()
            err = v2.error_of(env)
            outcome = {"reply": True,
                       "kind": v2.KIND_NAMES.get(env.get("kind")),
                       "error": err}
            # V2 在解码边界拒绝超限帧：先尽力回一帧
            # SESSION_ERROR(ProtocolError)，随后退役 link。该帧本身即真实
            # 拒绝证据；其它回复（如正常 RESPONSE）才是漏检。
            rejected = (env.get("kind") == v2.KIND_SESSION_ERROR
                        and (err or {}).get("code") == v2.ERR_PROTOCOL_ERROR)
        finally:
            sock.close()
    except (socket.timeout, TimeoutError) as exc:
        # 读超时不是拒绝证据：服务端可能挂起或在途，必须判失败。
        outcome = {"status": None, "exception": type(exc).__name__}
        rejected = False
    except (ConnectionResetError, ConnectionAbortedError,
            BrokenPipeError) as exc:
        # 预期断连形态：服务端不回复直接断开连接。
        outcome = {"status": None, "exception": type(exc).__name__}
        rejected = True
    except v2.ProtocolRejected as exc:
        # 超限帧被拒后对端退役 link：读端看到 EOF（无回复）即预期拒绝。
        outcome = {"status": None, "exception": type(exc).__name__,
                   "detail": str(exc)}
        rejected = "peer closed" in str(exc)
    except Exception as exc:  # noqa: BLE001
        outcome = {"status": None, "exception": type(exc).__name__}
        rejected = False
    check(rejected is True,
          f"oversized frame was not rejected (expected SESSION_ERROR/"
          f"ProtocolError or EOF/reset): {outcome}",
          "r4-byte-limit")
    # 超大请求未触达 handler 的独立证据：journal 没有 r4-big 的进入记录。
    big_entered = any(e.get("event") == "handler_entered"
                      and e.get("req") == "r4-big" for e in fx.entries())
    check(not big_entered,
          "oversized request reached handler (r4-big handler_entered)",
          "r4-byte-limit")
    # 服务器仍健康：正常请求成功。
    env = rpc_call(fx.port, "r4-after", "Fast", "kafter")
    healthy = (v2.error_of(env) is None)
    check(healthy, f"server unhealthy after oversized request: {env}",
          "r4-byte-limit")
    finish("r4-byte-limit", bool(rejected) and not big_entered and healthy,
           {"max_body_bytes": 4096, "oversized_key_bytes": len(big_key),
            "outcome": outcome,
            "reached_handler": big_entered,
            "server_healthy_after": healthy})


def scenario_r5_graceful(fx):
    """R5 正常：拒新 → drain → 同步 Close → release；无未完成项。"""
    env = rpc_call(fx.port, "r5-grace", "Fast", "kg")
    check(v2.error_of(env) is None, f"pre-shutdown request failed: {env}",
          "r5-graceful")
    fx.cmd("shutdown")
    fx.quit()
    stopped = fx.wait_exit(15)
    check(stopped, "fixture did not exit after shutdown", "r5-graceful")
    rc = fx.proc.returncode
    es = fx.entries()
    saw_closing = any(e.get("state") == "1" for e in es
                      if e.get("event") == "shutdown_state")
    shutdowns = [e for e in es if e.get("event") == "shutdown"]
    rec = shutdowns[-1] if shutdowns else {}
    closes = [e for e in es if e.get("event") == "resource_close"]
    last_handler = max((i for i, e in enumerate(es)
                        if e.get("event") in ("handler_entered",
                                              "handler_resumed")), default=-1)
    i_close = event_index(es, "resource_close")
    ok = (rc == 0
          and rec.get("rc") == "0" and rec.get("state") == "3"
          and rec.get("owner_closed") == "1" and rec.get("pending") == "0"
          and len(closes) == 1 and i_close > last_handler)
    check(ok, f"rc={rc} shutdown={rec} closes={len(closes)} "
              f"i_close={i_close} last_handler={last_handler}", "r5-graceful")
    finish("r5-graceful", ok,
           {"exit_code": rc, "shutdown": rec,
            "resource_close_calls": len(closes),
            "resource_close_after_last_handler": i_close > last_handler,
            "observed_closing_state": saw_closing})
    return rc


def scenario_r5_shutdown_incomplete(binpath, workdir):
    """R5 ShutdownIncomplete：在途关闭 → 保留强持有 → 迟到完成 → rc=3；拒新；晚到无 UAF。"""
    fx = Fixture(binpath, workdir, "b-incomplete", 4, 300)
    try:
        fx.start(20)
        s, f_stream = open_and_send(fx.port, request_id="s5-held", key="hold",
                                    timeout=60)
        es = wait_journal(
            fx.journal,
            lambda es: any(e.get("event") == "handler_entered"
                           and e.get("req") == "s5-held" for e in es),
            12, fx.proc)
        entered = any(e.get("event") == "handler_entered"
                      and e.get("req") == "s5-held" for e in es)
        check(entered, "held handler did not enter", "r5-shutdown-incomplete")
        if not entered:
            finish("r5-shutdown-incomplete", False)
            return

        fx.cmd("shutdown")
        es = wait_journal(
            fx.journal,
            lambda es: any(e.get("event") == "shutdown_state"
                           and e.get("state") == "2" for e in es),
            8, fx.proc)
        inc = [e for e in es if e.get("event") == "shutdown_state"
               and e.get("state") == "2"]
        alive = fx.alive()
        no_close_yet = not any(e.get("event") == "resource_close" for e in es)
        check(bool(inc), "ShutdownIncomplete not observed", "r5-shutdown-incomplete")
        check(alive, "fixture exited during ShutdownIncomplete",
              "r5-shutdown-incomplete")
        check(no_close_yet, "resource Close called while ShutdownIncomplete",
              "r5-shutdown-incomplete")
        pending = inc[0].get("pending") if inc else None
        check(pending not in (None, "0"),
              f"pending_cleanup empty during ShutdownIncomplete: {inc}",
              "r5-shutdown-incomplete")

        # 拒新：StopAccepting 后新连接被拒（服务仍存活但不再接纳）。
        refused = None
        try:
            c = socket.create_connection(("127.0.0.1", fx.port), timeout=1.5)
            refused = "connected"
            try:
                c.settimeout(1.5)
                c_stream = v2.FrameStream(c)
                epoch = v2.client_handshake(c_stream, IDENTITY + "-refuse",
                                            int(time.time() * 1000),
                                            v2.nonce_for(IDENTITY + "-refuse"))
                c_stream.send({
                    "kind": v2.KIND_CALL,
                    "traffic": v2.TRAFFIC_SERVICE,
                    "session_epoch": epoch,
                    "request_id": "r5-refuse",
                    "service": SERVICE,
                    "method": "Fast",
                    "reply": v2.REPLY_REQUIRED,
                    "remaining_budget_ms": 30000,
                    "request_schema": REQUEST_SCHEMA,
                    "response_schema": RESPONSE_SCHEMA,
                    "payload": gvc.enc_get_value_request("k"),
                    "metadata": [],
                })
                env = c_stream.read()
                refused = "served" if v2.error_of(env) is None else "refused-error"
            except Exception as exc:  # noqa: BLE001
                refused = f"connected-then-{type(exc).__name__}"
            finally:
                c.close()
        except ConnectionRefusedError:
            refused = "connection-refused"
        except Exception as exc:  # noqa: BLE001
            refused = f"connect-{type(exc).__name__}"
        check(refused != "served", f"new request was served after "
                                   f"StopAccepting: {refused}",
              "r5-shutdown-incomplete")

        fx.cmd("open")
        fx.quit()
        stopped = fx.wait_exit(20)
        check(stopped, "fixture did not exit after late completion",
              "r5-shutdown-incomplete")
        rc = fx.proc.returncode
        fx.expect_rc = 3   # kExitShutdownLate：迟到收尾已全部完成
        es = fx.entries()
        shutdowns = [e for e in es if e.get("event") == "shutdown"]
        rec = shutdowns[-1] if shutdowns else {}
        i_resumed = event_index(es, "handler_resumed")
        i_close = event_index(es, "resource_close")
        closes = len([e for e in es if e.get("event") == "resource_close"])
        resumed_rec = es[i_resumed] if i_resumed >= 0 else {}
        # 迟到完成无 UAF：进程如期以 rc=3(kExitShutdownLate) 退出（非崩溃信号），
        # handler 恢复记录在案，且资源 Close 发生在迟到完成之后。
        ok = (rc == 3
              and rec.get("rc") == "3" and rec.get("state") == "3"
              and rec.get("owner_closed") == "1" and rec.get("pending") == "0"
              and i_resumed >= 0 and i_close >= 0 and i_resumed < i_close
              and closes == 1
              and resumed_rec.get("same_ctx") == "1")
        check(ok, f"rc={rc} shutdown={rec} i_resumed={i_resumed} "
                  f"i_close={i_close} closes={closes}", "r5-shutdown-incomplete")

        # 迟到回复是否送达：关闭序列在 handler 返回后即同步收口网络，故
        # 「迟到回复送达」不是承诺（关闭不等于底层 I/O 完成）；此处只如实记录，
        # 不作硬断言。无 UAF 的正面证据是上面的 rc=3 + handler 恢复 + 无崩溃。
        late = None
        try:
            s.settimeout(10)
            env = f_stream.read()
            late = {"delivered": True,
                    "success": v2.error_of(env) is None,
                    "request_id": env.get("request_id")}
        except Exception as exc:  # noqa: BLE001
            late = {"delivered": False, "exception": type(exc).__name__}

        finish("r5-shutdown-incomplete",
               not [f for f in _FAILURES
                    if f.startswith("r5-shutdown-incomplete")],
               {"exit_code": rc, "shutdown": rec, "refuse_new": refused,
                "pending_during_incomplete": pending,
                "resource_close_after_late_completion": i_resumed < i_close,
                "late_completion_resumed": resumed_rec,
                "late_reply_delivery": late,
                "note": "shutdown closes network after handler returns; late "
                        "reply delivery is not a framework promise"})
        try:
            s.close()
        except OSError:
            pass
    finally:
        fx.reap(_FAILURES)


def scenario_r5_hard_kill(binpath, workdir):
    """R5 外部 supervisor 硬杀：journal 只留「未完成」证据，不称优雅完成。"""
    fx = Fixture(binpath, workdir, "c-hardkill", 4, 3000)
    try:
        fx.start(20)
        s, _ = open_and_send(fx.port, request_id="s5-kill", key="hold",
                             timeout=60)
        es = wait_journal(
            fx.journal,
            lambda es: any(e.get("event") == "handler_entered"
                           and e.get("req") == "s5-kill" for e in es),
            12, fx.proc)
        entered = any(e.get("event") == "handler_entered"
                      and e.get("req") == "s5-kill" for e in es)
        check(entered, "held handler did not enter before hard kill",
              "r5-hard-kill")
        alive = fx.alive()
        check(alive, "fixture died before hard kill", "r5-hard-kill")

        fx.kill_sigkill()
        fx.expect_signal = signal.SIGKILL
        killed_rc = fx.proc.returncode
        check(killed_rc == -signal.SIGKILL,
              f"expected SIGKILL, got rc={killed_rc}", "r5-hard-kill")

        es = fx.entries()
        has_entered = any(e.get("event") == "handler_entered"
                          and e.get("req") == "s5-kill" for e in es)
        has_shutdown = any(e.get("event") == "shutdown" for e in es)
        has_graceful = any(e.get("event") == "shutdown" and e.get("rc") == "0"
                           for e in es)
        has_close = any(e.get("event") == "resource_close" for e in es)
        has_closed_state = any(e.get("event") == "shutdown_state"
                               and e.get("state") == "3" for e in es)
        not_graceful = (not has_shutdown and not has_graceful
                        and not has_close and not has_closed_state)
        check(has_entered, "journal missing incomplete marker (handler_entered)",
              "r5-hard-kill")
        check(not_graceful, "hard-kill journal claims graceful completion: "
                            f"{es}", "r5-hard-kill")
        finish("r5-hard-kill", has_entered and not_graceful and
               killed_rc == -signal.SIGKILL,
               {"killed_rc": killed_rc, "supervisor_signal": "SIGKILL",
                "journal_has_handler_entered": has_entered,
                "journal_has_graceful_shutdown": has_shutdown,
                "journal_has_resource_close": has_close,
                "marked": "incomplete/unknown (no graceful record)"})
        try:
            s.close()
        except OSError:
            pass
    finally:
        fx.reap(_FAILURES)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--results")
    ap.add_argument("--timeout", type=float, default=30.0)
    args = ap.parse_args()

    work = os.path.join(os.path.dirname(os.path.abspath(args.bin)),
                        f"lifecycle-{os.getpid()}")
    os.makedirs(work, exist_ok=True)

    # 主 fixture：max_inflight=2（容量/Voverloaded/字节/正常关闭）。
    fx = None
    try:
        fx = Fixture(args.bin, work, "a-capacity", 2, 3000)
        try:
            fx.start(args.timeout)
            meta = fx.meta()
            RESULTS["meta"] = dict(meta, auth_state=AUTH_STATE)
            check(meta.get("max_inflight") == "2",
                  f"fixture meta max_inflight unexpected: {meta}", "meta")
            check(meta.get("auth_state") == AUTH_STATE,
                  f"fixture meta auth_state unexpected: {meta}", "meta")
            check(all(e.get("auth_state") == AUTH_STATE for e in fx.entries()),
                  "journal line missing auth_state", "meta")
            scenario_r3_r4(fx)
            scenario_r4_byte_limit(fx)
            scenario_r5_graceful(fx)
        except Exception as exc:  # noqa: BLE001
            _FAILURES.append(f"driver-error[a-capacity] {type(exc).__name__}: {exc}")
            traceback.print_exc()
    finally:
        if fx is not None:
            fx.reap(_FAILURES)

    for fn in (scenario_r5_shutdown_incomplete, scenario_r5_hard_kill):
        try:
            fn(args.bin, work)
        except Exception as exc:  # noqa: BLE001
            _FAILURES.append(f"driver-error[{fn.__name__}] "
                             f"{type(exc).__name__}: {exc}")
            traceback.print_exc()

    # 全部结构化结果带 auth_state 标记。
    for rec in RESULTS.values():
        rec.setdefault("auth_state", AUTH_STATE)

    tail = "LIFECYCLE-ALL-PASS" if not _FAILURES else "LIFECYCLE-FAILURES"
    RESULTS["_summary"] = {"failures": list(_FAILURES), "tail": tail,
                           "auth_state": AUTH_STATE}
    print(f"GRAND {tail} auth_state={AUTH_STATE}")
    if args.results:
        with open(args.results, "w") as f:
            json.dump(RESULTS, f, indent=2, sort_keys=True, ensure_ascii=False)
    shutil.rmtree(work, ignore_errors=True)
    if _FAILURES:
        print(f"LIFECYCLE-DRIVER-FAIL {_FAILURES}", file=sys.stderr)
        sys.exit(1)
    print("LIFECYCLE-DRIVER-PASS auth_state=unauthenticated_loopback")


if __name__ == "__main__":
    main()
