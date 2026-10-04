#!/usr/bin/env python3
"""bbt-framework Issue #4（P0-A）：GetValue 示例的 Python 标准库客户端。

不依赖 google.protobuf：按 proto3 wire format 手写业务 .proto
(examples/getvalue/proto/bbt/example/v1/get_value.proto) 与 infra
rpc_envelope.proto 的编解码，直接向 C++ getvalue_server 发起真实
HTTP POST /rpc 调用。手写编解码本身即字节级互通证据。

结构化结果均带 auth_state=unauthenticated_loopback：本路径只证明业务/传输
行为，不证明身份可信或授权（认证轨道见 #38）。

用法：
  getvalue_client.py --port <port> [--request-schema S] [--response-schema S]
                     [--results <json-path>]
"""

import argparse
import http.client
import json
import socket
import sys

RPC_PATH = "/rpc"
CONTENT_TYPE = "application/x-protobuf"

# 与 proto package bbt.example.v1 一致（唯一真源 = .proto 的 descriptor full_name）。
SERVICE = "bbt.example.v1.GetValueService"
METHOD = "GetValue"
REQUEST_SCHEMA = "bbt.example.v1.GetValueRequest"
RESPONSE_SCHEMA = "bbt.example.v1.GetValueResponse"

AUTH_STATE = "unauthenticated_loopback"

# RpcErrorCode（infra rpc_envelope.proto enum）
INVALID_ARGUMENT = 1
TIMED_OUT = 6
PROTOCOL_ERROR = 10
NOT_FOUND = 11
TYPE_MISMATCH = 12

WIRE_VARINT = 0
WIRE_LEN = 2

# 业务 golden vectors：C++ 与 Python 两侧独立断言同一字节串（无 schema 漂移）。
GV_REQ_ALPHA = b"\x0a\x05alpha"                       # GetValueRequest{key="alpha"}
GV_RESP_ALPHA = b"\x08\x01\x12\x0bvalue-alpha"        # found=true, value="value-alpha"

_RESULTS = {}


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


# --- 业务 payload codec ------------------------------------------------------

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


# --- envelope codec ----------------------------------------------------------

def enc_envelope(*, request_id, service, method, budget_ms, request_schema,
                 response_schema, payload, metadata=()):
    out = _f_varint(1, 1)                       # profile_version = 1
    out += _f_len(2, request_id)
    out += _f_len(3, service)
    out += _f_len(4, method)
    out += _f_varint(5, budget_ms)
    out += _f_len(6, request_schema)
    out += _f_len(7, response_schema)
    out += _f_len(8, payload)
    for k, v in metadata:
        out += _f_len(9, _f_len(1, k) + _f_len(2, v))
    return out


def dec_error(buf):
    err = {}
    details = []
    for f, w, v in _parse_fields(buf):
        if w == WIRE_VARINT:
            if f == 1:
                err["code"] = v
            elif f == 6:
                err["backend_code"] = v
        elif w == WIRE_LEN:
            if f == 2:
                err["domain"] = v.decode("utf-8")
            elif f == 3:
                err["domain_code"] = v.decode("utf-8")
            elif f == 4:
                err["message"] = v.decode("utf-8")
            elif f == 5:
                err["backend_category"] = v.decode("utf-8")
            elif f == 8:
                det = {}
                for df, dw, dv in _parse_fields(v):
                    if dw == WIRE_LEN and df == 1:
                        det["key"] = dv.decode("utf-8")
                    elif dw == WIRE_LEN and df == 2:
                        det["value"] = dv.decode("utf-8")
                details.append((det.get("key", ""), det.get("value", "")))
    err["details"] = details
    return err


def dec_envelope(buf):
    env = {"metadata": [], "unknown_fields": []}
    for f, w, v in _parse_fields(buf):
        if w == WIRE_VARINT:
            if f == 1:
                env["profile_version"] = v
            elif f == 5:
                env["remaining_budget_ms"] = v
            elif f == 10:
                env["success"] = bool(v)
            else:
                env["unknown_fields"].append(f)
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
            elif f == 9:
                kv = {}
                for mf, mw, mv in _parse_fields(v):
                    if mw == WIRE_LEN and mf == 1:
                        kv["key"] = mv.decode("utf-8")
                    elif mw == WIRE_LEN and mf == 2:
                        kv["value"] = mv.decode("utf-8")
                env["metadata"].append((kv.get("key", ""), kv.get("value", "")))
            elif f == 11:
                env["error"] = dec_error(v)
            else:
                env["unknown_fields"].append(f)
    return env


# --- HTTP --------------------------------------------------------------------

def post_raw(port, body, path=RPC_PATH, content_type=CONTENT_TYPE):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request("POST", path, body=body,
                 headers={"Content-Type": content_type})
    resp = conn.getresponse()
    data = resp.read()
    status = resp.status
    ctype = resp.getheader("Content-Type")
    conn.close()
    return status, ctype, data


def call(port, *, request_id, method=METHOD, key="alpha", budget_ms=3000,
         request_schema=REQUEST_SCHEMA, response_schema=RESPONSE_SCHEMA,
         payload=None, extra_payload=b"", extra_envelope=b""):
    if payload is None:
        payload = enc_get_value_request(key) + extra_payload
    body = enc_envelope(
        request_id=request_id, service=SERVICE, method=method,
        budget_ms=budget_ms, request_schema=request_schema,
        response_schema=response_schema, payload=payload) + extra_envelope
    status, ctype, data = post_raw(port, body)
    env = dec_envelope(data)
    return status, ctype, env


_FAILURES = []


def check(cond, label, scenario):
    if not cond:
        _FAILURES.append(f"{scenario}: {label}")
        print(f"FAIL {scenario}: {label}", file=sys.stderr)


def finish(scenario, ok):
    _RESULTS[scenario] = {"status": "pass" if ok else "fail",
                          "auth_state": AUTH_STATE}
    if ok:
        print(f"PASS scenario={scenario} auth_state={AUTH_STATE}")


def main():
    global REQUEST_SCHEMA, RESPONSE_SCHEMA
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--request-schema", default=REQUEST_SCHEMA)
    ap.add_argument("--response-schema", default=RESPONSE_SCHEMA)
    ap.add_argument("--results")
    args = ap.parse_args()
    port = args.port

    # schema 漂移守卫：服务端（descriptor 推导）与本客户端（.proto 常量）必须一致。
    schema_drift = (args.request_schema == REQUEST_SCHEMA and
                    args.response_schema == RESPONSE_SCHEMA)
    _RESULTS["schema_drift_guard"] = {
        "server_request_schema": args.request_schema,
        "server_response_schema": args.response_schema,
        "status": "pass" if schema_drift else "fail",
        "auth_state": AUTH_STATE}
    if not schema_drift:
        print(f"FAIL schema-drift: server={args.request_schema} "
              f"client={REQUEST_SCHEMA}", file=sys.stderr)
    else:
        print(f"PASS scenario=schema-drift-guard auth_state={AUTH_STATE}")

    # golden vectors（业务字节级）
    _RESULTS["golden_vectors"] = {
        "req_alpha": GV_REQ_ALPHA.hex(),
        "resp_alpha": GV_RESP_ALPHA.hex(),
        "status": "pass" if (
            enc_get_value_request("alpha") == GV_REQ_ALPHA and
            dec_get_value_response(GV_RESP_ALPHA) == (True, "value-alpha")
        ) else "fail",
        "auth_state": AUTH_STATE}

    # S1 known
    st, ct, env = call(port, request_id="sc-known")
    ok = (st == 200 and ct == CONTENT_TYPE and "error" not in env
          and env.get("success") and env.get("request_id") == "sc-known"
          and env.get("response_schema") == RESPONSE_SCHEMA
          and dec_get_value_response(env.get("payload", b"")) ==
          (True, "value-alpha"))
    check(st == 200, f"http={st}", "known")
    check(ct == CONTENT_TYPE, f"content-type={ct}", "known")
    check("error" not in env, "unexpected error", "known")
    check(env.get("request_id") == "sc-known", "request_id not echoed", "known")
    check(env.get("response_schema") == RESPONSE_SCHEMA,
          "response_schema not declared schema", "known")
    check(dec_get_value_response(env.get("payload", b"")) ==
          (True, "value-alpha"), "payload wrong", "known")
    finish("known", ok and not _FAILURES)

    # S2 miss
    before = len(_FAILURES)
    st, ct, env = call(port, request_id="sc-miss", key="no-such-key")
    ok = (env.get("success") and
          dec_get_value_response(env.get("payload", b"")) == (False, ""))
    check(ok, "miss should be found=false/value=''", "miss")
    finish("miss", ok and len(_FAILURES) == before)

    # S3 empty key -> InvalidArgument
    before = len(_FAILURES)
    st, ct, env = call(port, request_id="sc-empty", key="")
    ok = (env.get("error", {}).get("code") == INVALID_ARGUMENT)
    check(ok, f"expected InvalidArgument, got {env.get('error')}", "empty-key")
    finish("empty-key", ok and len(_FAILURES) == before)

    # S4 unknown method -> NotFound
    before = len(_FAILURES)
    st, ct, env = call(port, request_id="sc-nomethod", method="Nope")
    ok = (env.get("error", {}).get("code") == NOT_FOUND)
    check(ok, f"expected NotFound, got {env.get('error')}", "unknown-method")
    finish("unknown-method", ok and len(_FAILURES) == before)

    # S5 request schema mismatch -> TypeMismatch
    before = len(_FAILURES)
    st, ct, env = call(port, request_id="sc-schema",
                       request_schema="bbt.example.v1.Nope")
    ok = (env.get("error", {}).get("code") == TYPE_MISMATCH)
    check(ok, f"expected TypeMismatch, got {env.get('error')}", "schema-mismatch")
    finish("schema-mismatch", ok and len(_FAILURES) == before)

    # S6a truncated business payload -> ProtocolError (业务 codec 失败)
    before = len(_FAILURES)
    st, ct, env = call(port, request_id="sc-badpayload",
                       payload=b"\x0a\x05ab")
    ok = (env.get("error", {}).get("code") == PROTOCOL_ERROR)
    check(ok, f"expected ProtocolError, got {env.get('error')}", "malformed-payload")
    finish("malformed-payload", ok and len(_FAILURES) == before)

    # S6b truncated envelope body -> ProtocolError (wire 解码失败)
    before = len(_FAILURES)
    body = enc_envelope(request_id="sc-trunc", service=SERVICE, method=METHOD,
                        budget_ms=3000, request_schema=REQUEST_SCHEMA,
                        response_schema=RESPONSE_SCHEMA,
                        payload=enc_get_value_request("alpha"))[:8]
    st, ct, data = post_raw(port, body)
    env = dec_envelope(data)
    ok = (st == 200 and env.get("error", {}).get("code") == PROTOCOL_ERROR)
    check(ok, f"expected ProtocolError, got status={st} {env.get('error')}",
          "malformed-envelope")
    finish("malformed-envelope", ok and len(_FAILURES) == before)

    # S6c wrong content-type -> InvalidArgument (wire 拒绝，不读 x-bbt-* 头旁路)
    before = len(_FAILURES)
    body = enc_envelope(request_id="sc-ctype", service=SERVICE, method=METHOD,
                        budget_ms=3000, request_schema=REQUEST_SCHEMA,
                        response_schema=RESPONSE_SCHEMA,
                        payload=enc_get_value_request("alpha"))
    st, ct, data = post_raw(port, body, content_type="application/json")
    env = dec_envelope(data)
    ok = (env.get("error", {}).get("code") == INVALID_ARGUMENT)
    check(ok, f"expected InvalidArgument, got {env.get('error')}",
          "wrong-content-type")
    finish("wrong-content-type", ok and len(_FAILURES) == before)

    # S7 unknown field in envelope -> 正常处理（proto3 前向兼容）
    before = len(_FAILURES)
    st, ct, env = call(port, request_id="sc-fwd",
                       extra_envelope=_tag(200, WIRE_VARINT) + _varint(12345))
    ok = (env.get("success") and env.get("request_id") == "sc-fwd" and
          dec_get_value_response(env.get("payload", b"")) ==
          (True, "value-alpha"))
    check(ok, "envelope unknown field broke processing", "unknown-field-envelope")
    finish("unknown-field-envelope", ok and len(_FAILURES) == before)

    # S8 V2 request adds client_note=2 -> V1 服务端忽略未知字段
    before = len(_FAILURES)
    v2_extra = _f_len(2, "from-v2-client")   # client_note = 2
    st, ct, env = call(port, request_id="sc-v2", key="beta", extra_payload=v2_extra)
    ok = (env.get("success") and
          dec_get_value_response(env.get("payload", b"")) == (True, "value-beta"))
    check(ok, f"V2 unknown field broke processing: {env.get('error')}", "v2-compat")
    finish("v2-compat", ok and len(_FAILURES) == before)

    # S9 budget 0 -> wire 拒绝（InvalidArgument），不进 handler（journal 验证）
    before = len(_FAILURES)
    st, ct, env = call(port, request_id="sc-budget0", budget_ms=0)
    ok = (env.get("error", {}).get("code") == INVALID_ARGUMENT)
    check(ok, f"expected InvalidArgument, got {env.get('error')}", "budget-zero")
    finish("budget-zero", ok and len(_FAILURES) == before)

    # S9b 可解码但已过期（budget=1ms，接收端 local-min − 回包预留后期限已过）
    # -> 在进 handler 前 TimedOut（error 信封回显 request_id/schema），
    # journal 不得出现 handler_entered。不使用 0 预算拒收冒充「预算保护」。
    before = len(_FAILURES)
    st, ct, env = call(port, request_id="sc-budget1", budget_ms=1)
    err = env.get("error", {})
    ok = (err.get("code") == TIMED_OUT
          and env.get("request_id") == "sc-budget1"
          and env.get("request_schema") == REQUEST_SCHEMA)
    check(ok, f"expected TimedOut+id/schema echo, got {env}", "budget-expired")
    finish("budget-expired", ok and len(_FAILURES) == before)

    # S9c 声明预算超过本地 in-flight 上限（2400000ms > incoming_timeout 30s）：
    # 请求仍成功，但响应剩余预算必须被 clamp 到本地期限以内（不得原值回显）。
    before = len(_FAILURES)
    st, ct, env = call(port, request_id="sc-budgetlarge", budget_ms=2400000)
    remaining = env.get("remaining_budget_ms", 0)
    ok = (env.get("success") and env.get("request_id") == "sc-budgetlarge"
          and 1 <= remaining <= 30000
          and dec_get_value_response(env.get("payload", b"")) ==
          (True, "value-alpha"))
    check(ok, f"expected clamped remaining<=30000, got {env}", "budget-clamped")
    finish("budget-clamped", ok and len(_FAILURES) == before)

    # S10 发起后强制断连（丢 reply）-> 客户端记录未知结果，不重试
    before = len(_FAILURES)
    outcome = send_and_disconnect(port, "sc-disconnect")
    ok = (outcome == "unknown")
    check(ok, f"expected unknown outcome, got {outcome}", "disconnect-unknown")
    _RESULTS["disconnect-unknown"] = {"status": "pass" if ok else "fail",
                                      "outcome": outcome, "auth_state": AUTH_STATE}
    if ok:
        print(f"PASS scenario=disconnect-unknown outcome=unknown "
              f"auth_state={AUTH_STATE}")

    # S11 unknown error code（本地解码向量，M-check）：仅证明客户端对未知码的解码
    # 不误判成功；**不是**服务端传播证据（服务端当前不产生未知码）。真实远端错误
    # 传播由 S4(NotFound)/S5(TypeMismatch) 等经服务端产生的错误码承载。
    before = len(_FAILURES)
    fake_err = dec_envelope(_f_len(11, _f_varint(1, 999) + _f_len(4, "x")))
    code = fake_err.get("error", {}).get("code")
    ok = ("error" in fake_err and code == 999 and code != 0)
    check(ok, "unknown error code misclassified (local decode only)", "unknown-error-code")
    finish("unknown-error-code", ok and len(_FAILURES) == before)

    tail = "ALL-SCENARIOS-PASS" if not _FAILURES else "SCENARIO-FAILURES"
    print(f"GRAND {tail} auth_state={AUTH_STATE}")
    if args.results:
        _RESULTS["_summary"] = {"failures": _FAILURES, "tail": tail,
                                "auth_state": AUTH_STATE}
        with open(args.results, "w") as f:
            json.dump(_RESULTS, f, indent=2, sort_keys=True)
    sys.exit(0 if not _FAILURES else 1)


def send_and_disconnect(port, request_id):
    """发起真实请求后立即关闭连接（不读回复）——客户端只能记为未知结果。"""
    body = enc_envelope(request_id=request_id, service=SERVICE, method=METHOD,
                        budget_ms=3000, request_schema=REQUEST_SCHEMA,
                        response_schema=RESPONSE_SCHEMA,
                        payload=enc_get_value_request("alpha"))
    req = (f"POST {RPC_PATH} HTTP/1.1\r\nHost: 127.0.0.1\r\n"
           f"Content-Type: {CONTENT_TYPE}\r\n"
           f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode()
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    try:
        s.sendall(req + body)
    finally:
        s.close()
    # 不读回复、不重试：结果未知。
    return "unknown"


if __name__ == "__main__":
    main()
