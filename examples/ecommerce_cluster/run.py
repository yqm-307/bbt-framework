#!/usr/bin/env python3
"""Issue #70 第五项验收驱动器：真实 Consul + 多进程电商集群的动态发现。

只使用 Python 标准库，不 mock 注册中心、不代替 C++ adapter：
  - 启动本次任务独占的 Consul agent（loopback、动态端口、独立 data-dir/日志）；
  - 以独立进程启动 catalog ×2 → order → gateway，全部动态 loopback 端口；
  - 用真实 ProtoWireV1 请求打 gateway，按响应里的实例身份断言实际落点；
  - 断言：双实例命中、扩容后三实例命中、实例退出/重启后落点自动更新、
    视图无变化时健康超过 5s 仍可用、目标全退时有限失败、注册中心不可用超过
    5s 后显式失败并能恢复；
  - finally 收口本次全部子进程（只碰自己 Popen 的句柄）。

用法：
  run.py --binary <ecommerce_cluster> --consul <consul 可执行文件>
         --output <result.json> [--workdir <dir>] [--timeout 170]

退出码：0 全部断言通过；1 有断言失败；2 环境/启动失败；3 超时。
"""

import argparse
import http.client
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback

RPC_PATH = "/rpc"
CONTENT_TYPE = "application/x-protobuf"

SERVICE = "bbt.example.ecommerce.v1.GatewayService"
METHOD = "Checkout"
REQUEST_SCHEMA = "bbt.example.ecommerce.v1.CheckoutRequest"
RESPONSE_SCHEMA = "bbt.example.ecommerce.v1.CheckoutResponse"

# RpcErrorCode（infra rpc_envelope.proto v1）
INVALID_ARGUMENT = 1
RUNTIME_UNAVAILABLE = 3
TIMED_OUT = 6
NOT_FOUND = 11

WIRE_VARINT = 0
WIRE_LEN = 2

SKU = "sku-book"
UNIT_CENTS = 1299

# 动态路由表的陈旧边界（框架约定 5s）：本驱动器用它断言 last-known-good 窗口。
STALE_WINDOW_S = 5.0


# --- proto3 wire format 最小编解码 ------------------------------------------


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
            value, pos = _read_varint(buf, pos)
            out.append((field, wire, value))
        elif wire == WIRE_LEN:
            size, pos = _read_varint(buf, pos)
            if pos + size > len(buf):
                raise ValueError("truncated LEN field")
            out.append((field, wire, buf[pos:pos + size]))
            pos += size
        else:
            raise ValueError("unsupported wire type %d" % wire)
    return out


def enc_checkout_request(sku, qty):
    return _f_len(1, sku) + _f_varint(2, qty)


def dec_checkout_response(buf):
    out = {"sku": "", "qty": 0, "total_cents": 0, "catalog_instance": "",
           "order_instance": "", "gateway_instance": ""}
    names = {1: "sku", 2: "qty", 3: "total_cents", 4: "catalog_instance",
             5: "order_instance", 6: "gateway_instance"}
    for field, wire, value in _parse_fields(buf):
        name = names.get(field)
        if name is None:
            continue
        if wire == WIRE_LEN:
            out[name] = value.decode("utf-8")
        elif wire == WIRE_VARINT:
            out[name] = value
    return out


def enc_envelope(request_id, budget_ms, payload):
    out = _f_varint(1, 1)                       # profile_version = 1
    out += _f_len(2, request_id)
    out += _f_len(3, SERVICE)
    out += _f_len(4, METHOD)
    out += _f_varint(5, budget_ms)
    out += _f_len(6, REQUEST_SCHEMA)
    out += _f_len(7, RESPONSE_SCHEMA)
    out += _f_len(8, payload)
    return out


def dec_error(buf):
    err = {}
    for field, wire, value in _parse_fields(buf):
        if wire == WIRE_VARINT and field == 1:
            err["code"] = value
        elif wire == WIRE_LEN and field == 4:
            err["message"] = value.decode("utf-8", "replace")
    return err


def dec_envelope(buf):
    env = {"success": False}
    for field, wire, value in _parse_fields(buf):
        if wire == WIRE_VARINT and field == 10:
            env["success"] = bool(value)
        elif wire == WIRE_LEN and field == 2:
            env["request_id"] = value.decode("utf-8", "replace")
        elif wire == WIRE_LEN and field == 8:
            env["payload"] = bytes(value)
        elif wire == WIRE_LEN and field == 11:
            env["error"] = dec_error(value)
    return env


# --- 断言与结果收集 ---------------------------------------------------------


class Report:
    def __init__(self):
        self.checks = []
        self.metrics = {}

    def check(self, name, ok, detail="none"):
        self.checks.append({"name": name, "ok": bool(ok), "detail": detail})
        print("%s %-42s %s" % ("PASS" if ok else "FAIL", name, detail))
        return ok

    def metric(self, key, value):
        self.metrics[key] = value

    @property
    def failures(self):
        return [c for c in self.checks if not c["ok"]]


class Deadline:
    def __init__(self, seconds):
        self.start = time.monotonic()
        self.limit = seconds

    def remaining(self):
        return self.limit - (time.monotonic() - self.start)

    def expired(self):
        return self.remaining() <= 0


# --- 自有 Consul agent ------------------------------------------------------


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ConsulAgent:
    def __init__(self, binary, workdir):
        self.binary = binary
        self.data_dir = os.path.join(workdir, "consul-data")
        self.log_path = os.path.join(workdir, "consul.log")
        self.http_port = free_port()
        self.pid = None
        self.log = None
        self._paused = False

    def start(self):
        os.makedirs(self.data_dir, exist_ok=True)
        args = [
            self.binary, "agent", "-dev",
            "-node=bbt-ecommerce-cluster",
            "-bind=127.0.0.1", "-advertise=127.0.0.1", "-client=127.0.0.1",
            "-data-dir=" + self.data_dir,
            "-http-port=%d" % self.http_port,
            "-serf-lan-port=%d" % free_port(),
            "-serf-wan-port=%d" % free_port(),
            "-server-port=%d" % free_port(),
            "-dns-port=0", "-grpc-port=0",
            "-log-level=warn", "-disable-keyring-file", "-ui=false",
        ]
        self.log = open(self.log_path, "w")
        self.proc = subprocess.Popen(args, stdout=self.log, stderr=subprocess.STDOUT)
        self.pid = self.proc.pid

    def url(self):
        return "http://127.0.0.1:%d" % self.http_port

    def ready(self, timeout=25.0):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            try:
                conn = http.client.HTTPConnection("127.0.0.1", self.http_port, timeout=2)
                conn.request("GET", "/v1/status/leader")
                resp = conn.getresponse()
                body = resp.read().decode("utf-8", "replace")
                conn.close()
                if resp.status == 200 and body.strip() not in ("", '""'):
                    return True
            except OSError:
                pass
            time.sleep(0.1)
        return False

    def pause(self):
        os.kill(self.pid, signal.SIGSTOP)
        self._paused = True

    def resume(self):
        if self._paused:
            os.kill(self.pid, signal.SIGCONT)
            self._paused = False

    def alive(self):
        return self.proc.poll() is None

    def stop(self):
        self.resume()
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        if self.log is not None:
            self.log.close()
            self.log = None


# --- 业务节点进程 -----------------------------------------------------------


class Node:
    """一个自有业务进程：读 stdout 的 event= 行，按需优雅或强杀。"""

    def __init__(self, binary, mode, consul_url, log_path):
        self.mode = mode
        self.log_path = log_path
        self.lines = []
        self.ready_info = None
        self._cv = threading.Condition()
        self._log = open(log_path, "w")
        self.proc = subprocess.Popen(
            [binary, mode, "--consul", consul_url],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, bufsize=1, text=True)
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()

    def _read(self):
        for line in self.proc.stdout:
            line = line.rstrip("\n")
            with self._cv:
                self.lines.append(line)
                if line.startswith("event=ready") and self.ready_info is None:
                    self.ready_info = dict(
                        token.split("=", 1)
                        for token in line.split(" ") if "=" in token)
                self._cv.notify_all()
            self._log.write(line + "\n")
            self._log.flush()

    def wait_ready(self, timeout=20.0):
        end = time.monotonic() + timeout
        with self._cv:
            while self.ready_info is None:
                left = end - time.monotonic()
                if left <= 0 or self.proc.poll() is not None:
                    return None
                self._cv.wait(left)
            return dict(self.ready_info)

    def wait_event(self, prefix, timeout=10.0):
        end = time.monotonic() + timeout
        with self._cv:
            while True:
                for line in self.lines:
                    if line.startswith(prefix):
                        return line
                left = end - time.monotonic()
                if left <= 0:
                    return None
                self._cv.wait(left)

    def events(self):
        with self._cv:
            return list(self.lines)

    def kill(self):
        """强杀：不注销，只能靠 Consul 健康检查把它从 passing 视图里去掉。"""
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)

    def stop(self, timeout=20.0):
        """优雅退出：关 stdin → 进程注销并收束；返回 (rc, stderr tail)。"""
        if self.proc.poll() is None:
            try:
                self.proc.stdin.close()
            except OSError:
                pass
            try:
                return self.proc.wait(timeout=timeout), ""
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
                return None, "graceful stop timed out"
        return self.proc.returncode, ""

    def cleanup(self):
        if self.proc.poll() is None:
            self.proc.kill()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        try:
            if self.proc.stdin and not self.proc.stdin.closed:
                self.proc.stdin.close()
        except OSError:
            pass
        self._reader.join(timeout=5)
        self._log.close()


# --- 真实 RPC 调用 ----------------------------------------------------------


def call_gateway(port, request_id, sku=SKU, qty=1, budget_ms=3000, timeout=8.0):
    payload = enc_checkout_request(sku, qty)
    body = enc_envelope(request_id, budget_ms, payload)
    started = time.monotonic()
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
        conn.request("POST", RPC_PATH, body=body,
                     headers={"Content-Type": CONTENT_TYPE})
        resp = conn.getresponse()
        data = resp.read()
        status = resp.status
        ctype = resp.getheader("Content-Type")
        conn.close()
    except OSError as exc:
        return {"ok": False, "transport_error": str(exc),
                "elapsed": time.monotonic() - started}
    elapsed = time.monotonic() - started
    try:
        env = dec_envelope(data)
    except ValueError as exc:
        return {"ok": False, "decode_error": str(exc), "status": status,
                "elapsed": elapsed}
    result = {"status": status, "content_type": ctype,
              "elapsed": elapsed, "request_id": env.get("request_id")}
    if "error" in env:
        result.update({"ok": False, "error_code": env["error"].get("code", -1),
                       "error_message": env["error"].get("message", "")})
        return result
    if not env.get("success"):
        result.update({"ok": False, "error_code": -1,
                       "error_message": "envelope success=false"})
        return result
    result.update(dec_checkout_response(env.get("payload", b"")))
    result["ok"] = True
    return result


def call_n(port, count, prefix, **kwargs):
    calls = []
    for i in range(count):
        calls.append(call_gateway(port, "%s-%d" % (prefix, i), **kwargs))
    return calls


def poll_until(fn, timeout, interval=0.2):
    """有限轮询：返回 (是否满足, 最后一次结果)。轮询只等视图收敛，不冒充同步。"""
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        last = fn()
        if last.get("done"):
            return True, last
        time.sleep(interval)
    return False, last


# --- 主流程 -----------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--consul", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--workdir", default="")
    parser.add_argument("--timeout", type=float, default=170.0)
    args = parser.parse_args()

    # A result belongs to this run; do not leave a prior pass visible on startup failure.
    output_path = os.path.abspath(args.output)
    if os.path.exists(output_path):
        os.remove(output_path)

    if not os.access(args.binary, os.X_OK):
        print("binary not executable: %s" % args.binary, file=sys.stderr)
        return 2
    if not os.access(args.consul, os.X_OK):
        print("consul binary not executable: %s" % args.consul, file=sys.stderr)
        return 2

    # 默认把 Consul data-dir、各进程日志与结果放在 --output 同级目录下，避免把
    # 运行产物写到系统临时目录或仓库跟踪路径之外。
    workdir = args.workdir or os.path.join(
        os.path.dirname(os.path.abspath(args.output)), "cluster-work")
    os.makedirs(workdir, exist_ok=True)

    report = Report()
    consul = ConsulAgent(args.consul, workdir)
    nodes = {}
    deadline = Deadline(args.timeout)
    started_at = time.time()
    rc = 2

    def procs():
        return [consul.proc] + [n.proc for n in nodes.values()]

    def watchdog():
        while not deadline.expired():
            time.sleep(0.5)
        print("hard timeout after %.0fs" % deadline.limit, file=sys.stderr)
        for proc in procs():
            try:
                proc.kill()
            except OSError:
                pass
        os._exit(3)

    def start_node(name, mode):
        node = Node(args.binary, mode, consul.url(),
                    os.path.join(workdir, "%s-%d.log" % (name, len(nodes))))
        nodes[name] = node
        info = node.wait_ready(timeout=25.0)
        if info is None:
            raise RuntimeError("%s did not become ready (rc=%s)"
                               % (name, node.proc.poll()))
        report.metric(name + "_instance", info.get("instance"))
        report.metric(name + "_pid", int(info.get("pid", 0)))
        return node, info

    try:
        consul.start()
        threading.Thread(target=watchdog, daemon=True).start()
        if not report.check("consul_ready", consul.ready(), consul.url()):
            raise RuntimeError("Consul agent did not become ready")
        report.metric("consul_pid", consul.pid)
        report.metric("consul_http_port", consul.http_port)

        # 先起 gateway、再起 order/catalog：gateway 的首次权威查询必然看到
        # 「合法空健康列表」，用来证明空初态不阻塞启动、继续 watch 后收敛。
        gw, gw_info = start_node("gateway", "gateway")
        _, order = start_node("order", "order")
        _, cat_a = start_node("catalog_a", "catalog")
        _, cat_b = start_node("catalog_b", "catalog")
        gateway_port = int(gw_info["endpoint"].rsplit(":", 1)[1])
        gateway_pid = int(gw_info["pid"])
        report.metric("gateway_endpoint", gw_info["endpoint"])

        # S0 视图收敛：新登记的实例要等 Consul 的 TCP check 首次 passing 才会
        # 进 passing 视图（check interval 1s）。有限轮询等收敛，不用固定 sleep
        # 冒充同步；这也是「合法空健康列表」的初态证据。
        # 收敛条件是「两个 catalog 实例都进 passing 视图」，不是「有一次调用成功」：
        # 单个实例先 passing 时视图只有 1 个 endpoint，此时调用会成功但只命中一个。
        converged_at = time.monotonic()
        done, last = poll_until(
            lambda: _distinct_probe(gateway_port,
                                    {cat_a["instance"], cat_b["instance"]}),
            timeout=25.0)
        report.check("s0_views_converged", done,
                     "after=%.2fs distinct=%s"
                     % (time.monotonic() - converged_at, (last or {}).get("distinct")))
        report.metric("s0_convergence_s", round(time.monotonic() - converged_at, 2))

        # 初态证据：gateway 自己的日志必须出现 endpoints=0 的权威视图，且随后
        # 同一服务收敛到非空——空健康列表是合法初态，watcher 继续 watch。
        empty_seen, non_empty_seen = _view_sequence(
            gw.events(), "bbt.example.ecommerce.v1.OrderService")
        report.check("s0_initial_empty_view_then_converged",
                     empty_seen and non_empty_seen,
                     "empty=%s non_empty=%s" % (empty_seen, non_empty_seen))

        # S1 双实例都被命中（round-robin），且响应里的下游身份真实。
        calls = call_n(gateway_port, 8, "s1")
        good = [c for c in calls if c.get("ok")]
        report.check("s1_all_calls_ok", len(good) == len(calls),
                     "ok=%d/%d codes=%s"
                     % (len(good), len(calls),
                        sorted({c.get("error_code") for c in calls if not c.get("ok")})))
        hits = sorted({c.get("catalog_instance") for c in good})
        report.check("s1_both_catalog_instances_hit",
                     bool(good) and set(hits) == {cat_a["instance"], cat_b["instance"]},
                     "hits=%s" % hits)
        report.check("s1_chain_identity",
                     bool(good) and all(c.get("order_instance") == order["instance"] and
                                        c.get("gateway_instance") == gw_info["instance"]
                                        for c in good),
                     "order=%s gateway=%s" % (order["instance"], gw_info["instance"]))
        report.check("s1_total_cents",
                     bool(good) and all(c.get("total_cents") == UNIT_CENTS for c in good),
                     "expected=%d" % UNIT_CENTS)
        report.metric("s1_catalog_hits", hits)

        # S2 扩容：新增第三个 catalog 实例，gateway PID 不变。
        _, cat_c = start_node("catalog_c", "catalog")
        done, last = poll_until(
            lambda: _distinct_probe(gateway_port, {cat_a["instance"], cat_b["instance"],
                                                   cat_c["instance"]}),
            timeout=20.0)
        report.check("s2_scale_out_third_instance_hit", done,
                     "distinct=%s" % (last or {}).get("distinct"))
        report.check("s2_gateway_pid_stable", gw.proc.poll() is None and
                     gw.proc.pid == gateway_pid, "pid=%d" % gateway_pid)

        # S3 退出（强杀，不注销）：只能靠真实健康检查从视图移除。
        cat_a_node = nodes["catalog_a"]
        cat_a_node.kill()
        done, last = poll_until(
            lambda: _absence_probe(gateway_port, cat_a["instance"], 3), timeout=25.0)
        report.check("s3_killed_instance_leaves_view", done,
                     "distinct=%s" % (last or {}).get("distinct"))
        report.metric("s3_view_after_kill", (last or {}).get("distinct"))

        # S4 重启（新进程/新 incarnation）：重新命中，且不是旧身份。
        _, cat_a2 = start_node("catalog_a2", "catalog")
        done, last = poll_until(
            lambda: _distinct_probe(gateway_port, {cat_b["instance"], cat_c["instance"],
                                                   cat_a2["instance"]}),
            timeout=20.0)
        report.check("s4_restarted_instance_hit_again", done,
                     "distinct=%s" % (last or {}).get("distinct"))
        report.check("s4_old_incarnation_absent",
                     cat_a["instance"] not in (last or {}).get("distinct", []),
                     "old=%s" % cat_a["instance"])

        # S5 视图无变化时健康 >5s 仍可用：成功查询必须刷新新鲜度。
        ok, worst = _steady_probe(gateway_port, 6.5)
        report.check("s5_stable_view_beyond_stale_window", ok,
                     "worst_elapsed=%.3fs" % worst)

        # S6 业务错误仍沿真实链路回传（不被发现层吞掉）。
        missing = call_gateway(gateway_port, "s6-missing", sku="sku-nope")
        report.check("s6_unknown_sku_not_found",
                     missing.get("error_code") == NOT_FOUND,
                     "code=%s" % missing.get("error_code"))
        bad_qty = call_gateway(gateway_port, "s6-qty", qty=0)
        report.check("s6_bad_qty_invalid_argument",
                     bad_qty.get("error_code") == INVALID_ARGUMENT,
                     "code=%s" % bad_qty.get("error_code"))

        # S7 目标全退：有限时间内明确失败（空健康视图 → NotFound），不挂住。
        for name in ("catalog_b", "catalog_c", "catalog_a2"):
            nodes[name].kill()
        # 强杀后视图先短暂保留旧 endpoint（健康检查还没跑到），这些调用会
        # TransportError；等 passing 视图清空后必须变成 NotFound。两种都是
        # 有限失败，断言的是「有限时间内进入明确终态」而不是固定某个码。
        attempts = []

        def _drain_probe():
            call = call_gateway(gateway_port, "all-down-%d" % len(attempts))
            attempts.append({"code": call.get("error_code"),
                             "elapsed": call.get("elapsed", 99.0),
                             "ok": bool(call.get("ok"))})
            return {"done": not call.get("ok") and
                            call.get("error_code") == NOT_FOUND,
                    "code": call.get("error_code"),
                    "elapsed": call.get("elapsed", 99.0)}

        done, last = poll_until(_drain_probe, timeout=25.0)
        report.check("s7_all_targets_down_bounded_failure", done,
                     "final_code=%s attempts=%d codes=%s"
                     % ((last or {}).get("code"), len(attempts),
                        sorted({a["code"] for a in attempts})))
        report.check("s7_failure_is_not_found_not_timeout",
                     (last or {}).get("code") == NOT_FOUND,
                     "code=%s" % (last or {}).get("code"))
        report.check("s7_no_call_hangs",
                     all(a["elapsed"] < 5.0 for a in attempts),
                     "worst_elapsed=%.3fs"
                     % max([a["elapsed"] for a in attempts] or [0.0]))
        report.metric("s7_failure_elapsed", (last or {}).get("elapsed"))
        report.metric("s7_transition_codes", sorted({a["code"] for a in attempts}))

        # 恢复一个 catalog，确认失败后能重新可用。
        _, cat_d = start_node("catalog_d", "catalog")
        done, _ = poll_until(lambda: _distinct_probe(gateway_port, {cat_d["instance"]}),
                             timeout=20.0)
        report.check("s7b_recovery_after_restart", done, "instance=%s" % cat_d["instance"])

        # S8 注册中心不可用：5s 内 last-known-good 仍可用，超过后显式失败。
        consul.pause()
        paused_at = time.monotonic()
        early = call_gateway(gateway_port, "s8-early")
        report.check("s8_last_known_good_within_window", early.get("ok"),
                     "elapsed=%.3fs" % early.get("elapsed", -1.0))
        time.sleep(max(0.0, 2.0 - (time.monotonic() - paused_at)))
        still = call_gateway(gateway_port, "s8-still")
        report.check("s8_still_serving_at_2s", still.get("ok"),
                     "elapsed=%.3fs" % still.get("elapsed", -1.0))

        time.sleep(max(0.0, 7.0 - (time.monotonic() - paused_at)))
        stale = call_gateway(gateway_port, "s8-stale")
        report.check("s8_stale_view_fails_after_window",
                     stale.get("error_code") == RUNTIME_UNAVAILABLE,
                     "code=%s elapsed=%.3fs"
                     % (stale.get("error_code"), stale.get("elapsed", -1.0)))
        report.check("s8_stale_failure_is_bounded",
                     stale.get("elapsed", 99.0) < 5.0,
                     "elapsed=%.3fs" % stale.get("elapsed", -1.0))
        report.metric("s8_outage_seconds", round(time.monotonic() - paused_at, 3))

        consul.resume()
        done, _ = poll_until(
            lambda: _recovered_probe(gateway_port, cat_d["instance"]), timeout=20.0)
        report.check("s8b_recovery_after_consul_returns", done, "consul resumed")

        # S9 收尾：gateway PID 仍稳定，优雅退出都返回 0。
        report.check("s9_gateway_pid_stable_end",
                     gw.proc.poll() is None and gw.proc.pid == gateway_pid,
                     "pid=%d" % gateway_pid)
        for name in ("gateway", "order"):
            code, note = nodes[name].stop()
            report.check("s9_%s_graceful_stop" % name, code == 0,
                         "rc=%s %s" % (code, note))
        code, note = nodes["catalog_d"].stop()
        report.check("s9_catalog_graceful_stop", code == 0,
                     "rc=%s %s" % (code, note))

        rc = 0 if not report.failures else 1
    except Exception:
        traceback.print_exc()
        rc = 2
    finally:
        consul.resume()
        for node in nodes.values():
            node.cleanup()
        consul.stop()

    report.metric("duration_s", round(time.time() - started_at, 2))
    result = {
        "status": "pass" if rc == 0 else "fail",
        "rc": rc,
        "checks": report.checks,
        "metrics": report.metrics,
        "failures": [c["name"] for c in report.failures],
        "consul_binary": args.consul,
        "binary": args.binary,
        "workdir": workdir,
        "auth_state": "unauthenticated_loopback",
    }
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as handle:
        json.dump(result, handle, indent=2, sort_keys=True)
    print("GRAND %s checks=%d failures=%d duration=%.1fs"
          % ("ALL-CHECKS-PASS" if rc == 0 else "CHECK-FAILURES",
             len(report.checks), len(report.failures),
             report.metrics["duration_s"]))
    return rc


def _view_sequence(lines, service):
    """从进程日志里读 event=view 序列：先出现空视图、后出现非空视图。"""
    empty_seen = False
    non_empty_seen = False
    for line in lines:
        if not line.startswith("event=view "):
            continue
        fields = dict(token.split("=", 1) for token in line.split(" ") if "=" in token)
        if fields.get("service") != service:
            continue
        if fields.get("endpoints") == "0":
            empty_seen = True
        elif int(fields.get("endpoints", "0")) > 0 and empty_seen:
            non_empty_seen = True
    return empty_seen, non_empty_seen


def _distinct_probe(port, expected):
    calls = call_n(port, 6, "probe")
    distinct = sorted({c.get("catalog_instance") for c in calls if c.get("ok")})
    ok = all(c.get("ok") for c in calls) and set(distinct) == set(expected)
    return {"done": ok, "distinct": distinct,
            "ok": sum(1 for c in calls if c.get("ok")), "total": len(calls)}


def _absence_probe(port, gone, streak):
    calls = call_n(port, streak, "absent")
    distinct = {c.get("catalog_instance") for c in calls if c.get("ok")}
    ok = (all(c.get("ok") for c in calls) and gone not in distinct)
    return {"done": ok, "distinct": sorted(distinct)}


def _steady_probe(port, seconds):
    """视图不变期间连续调用：任何一次失败都说明新鲜度没有刷新。"""
    end = time.monotonic() + seconds
    worst = 0.0
    while time.monotonic() < end:
        call = call_gateway(port, "steady-%d" % int(time.monotonic() * 1000))
        worst = max(worst, call.get("elapsed", 99.0))
        if not call.get("ok"):
            return False, worst
        time.sleep(0.5)
    return True, worst


def _recovered_probe(port, expected_instance):
    call = call_gateway(port, "recovered")
    return {"done": bool(call.get("ok")) and
                    call.get("catalog_instance") == expected_instance,
            "code": call.get("error_code")}


if __name__ == "__main__":
    sys.exit(main())
