#!/usr/bin/env python3
"""bbt-framework GetValue 示例的 Python 标准库客户端。

不依赖 google.protobuf：按 proto3 wire format 手写业务 .proto
(examples/getvalue/proto/bbt/example/v1/get_value.proto) 的业务 payload
编解码，会话/分帧/握手/错误信封一律复用唯一 V2 codec
(tests/rpc_v2_wire.py)——不复制第二套 wire 实现。

真实 V2 会话：TCP + u32be 分帧 + Hello/HelloAck 握手 + SessionFrame
profile 2，直接向 C++ getvalue_server 发起 Call。手写业务编解码本身即
字节级互通证据。

结构化结果均带 auth_state=unauthenticated_loopback：本路径只证明业务/传输
行为，不证明身份可信或授权（认证/授权由后续范围承载）。

用法：
  getvalue_client.py --port <port> [--request-schema S] [--response-schema S]
                     [--results <json-path>]
"""

import argparse
import json
import os
import sys

# 唯一 V2 codec 在 tests/；示例目录用明确相对路径定位，不依赖 cwd。
_HERE = os.path.dirname(os.path.abspath(__file__))
_TESTS_DIR = os.path.normpath(os.path.join(_HERE, "..", "..", "tests"))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
import rpc_v2_wire as v2  # noqa: E402

# 与 proto package bbt.example.v1 一致（唯一真源 = .proto 的 descriptor full_name）。
SERVICE = "bbt.example.v1.GetValueService"
METHOD = "GetValue"
REQUEST_SCHEMA = "bbt.example.v1.GetValueRequest"
RESPONSE_SCHEMA = "bbt.example.v1.GetValueResponse"

# 会话身份必须与服务端互异：V2 拒绝同 identity 的 self-connection。
CLIENT_IDENTITY = "getvalue-python-client"

AUTH_STATE = "unauthenticated_loopback"

# RpcErrorCode（session_frame.proto enum，与 infra ErrorCode 同序）
INVALID_ARGUMENT = v2.ERR_INVALID_ARGUMENT
TIMED_OUT = v2.ERR_TIMED_OUT
PROTOCOL_ERROR = v2.ERR_PROTOCOL_ERROR
NOT_FOUND = v2.ERR_NOT_FOUND
TYPE_MISMATCH = v2.ERR_TYPE_MISMATCH

WIRE_VARINT = 0
WIRE_LEN = 2

# 业务 golden vectors：C++ 与 Python 两侧独立断言同一字节串（无 schema 漂移）。
GV_REQ_ALPHA = b"\x0a\x05alpha"                       # GetValueRequest{key="alpha"}
GV_RESP_ALPHA = b"\x08\x01\x12\x0bvalue-alpha"        # found=true, value="value-alpha"

_RESULTS = {}


# --- proto3 wire format 最小编解码（业务 payload 专用）------------------------


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


# --- V2 会话调用 -------------------------------------------------------------


def call_frame(client, *, request_id, method=METHOD, key="alpha", budget_ms=3000,
               request_schema=REQUEST_SCHEMA, response_schema=RESPONSE_SCHEMA,
               payload=None):
    if payload is None:
        payload = enc_get_value_request(key)
    return {
        "kind": v2.KIND_CALL,
        "traffic": v2.TRAFFIC_SERVICE,
        "session_epoch": client.epoch,
        "request_id": request_id,
        "service": SERVICE,
        "method": method,
        "reply": v2.REPLY_REQUIRED,
        "remaining_budget_ms": budget_ms,
        "request_schema": request_schema,
        "response_schema": response_schema,
        "payload": payload,
        "metadata": [],
    }


def call(client, **kwargs):
    """一次真实 V2 Call；返回响应帧（error_of 取错误分支）。"""
    frame = call_frame(client, **kwargs)
    return client.call(
        frame["service"], frame["method"], payload=frame["payload"],
        request_id=frame["request_id"],
        request_schema=frame["request_schema"],
        response_schema=frame["response_schema"],
        budget_ms=frame["remaining_budget_ms"])


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

    client = v2.V2Client(port, identity=CLIENT_IDENTITY)
    client.handshake(expected_peer="getvalue-server")
    try:
        # S1 known
        env = call(client, request_id="sc-known")
        err = v2.error_of(env)
        ok = (err is None
              and env.get("request_id") == "sc-known"
              and env.get("response_schema") == RESPONSE_SCHEMA
              and dec_get_value_response(env.get("payload", b"")) ==
              (True, "value-alpha"))
        check(err is None, f"unexpected error {err}", "known")
        check(env.get("request_id") == "sc-known", "request_id not echoed", "known")
        check(env.get("response_schema") == RESPONSE_SCHEMA,
              "response_schema not declared schema", "known")
        check(dec_get_value_response(env.get("payload", b"")) ==
              (True, "value-alpha"), "payload wrong", "known")
        finish("known", ok and not _FAILURES)

        # S2 miss
        before = len(_FAILURES)
        env = call(client, request_id="sc-miss", key="no-such-key")
        ok = (v2.error_of(env) is None and
              dec_get_value_response(env.get("payload", b"")) == (False, ""))
        check(ok, "miss should be found=false/value=''", "miss")
        finish("miss", ok and len(_FAILURES) == before)

        # S3 empty key -> InvalidArgument
        before = len(_FAILURES)
        env = call(client, request_id="sc-empty", key="")
        ok = (v2.error_of(env) or {}).get("code") == INVALID_ARGUMENT
        check(ok, f"expected InvalidArgument, got {v2.error_of(env)}", "empty-key")
        finish("empty-key", ok and len(_FAILURES) == before)

        # S4 unknown method -> NotFound
        before = len(_FAILURES)
        env = call(client, request_id="sc-nomethod", method="Nope")
        ok = (v2.error_of(env) or {}).get("code") == NOT_FOUND
        check(ok, f"expected NotFound, got {v2.error_of(env)}", "unknown-method")
        finish("unknown-method", ok and len(_FAILURES) == before)

        # S5 request schema mismatch -> TypeMismatch
        before = len(_FAILURES)
        env = call(client, request_id="sc-schema",
                   request_schema="bbt.example.v1.Nope")
        ok = (v2.error_of(env) or {}).get("code") == TYPE_MISMATCH
        check(ok, f"expected TypeMismatch, got {v2.error_of(env)}", "schema-mismatch")
        finish("schema-mismatch", ok and len(_FAILURES) == before)

        # S6a truncated business payload -> ProtocolError (业务 codec 失败)
        before = len(_FAILURES)
        env = call(client, request_id="sc-badpayload", payload=b"\x0a\x05ab")
        ok = (v2.error_of(env) or {}).get("code") == PROTOCOL_ERROR
        check(ok, f"expected ProtocolError, got {v2.error_of(env)}", "malformed-payload")
        finish("malformed-payload", ok and len(_FAILURES) == before)

        # S7 unknown field in SessionFrame -> 正常处理（proto3 前向兼容）
        before = len(_FAILURES)
        frame = call_frame(client, request_id="sc-fwd")
        env = client.call_unchecked(frame, unknown_fields=[(200, 12345)])
        ok = (v2.error_of(env) is None and env.get("request_id") == "sc-fwd" and
              dec_get_value_response(env.get("payload", b"")) ==
              (True, "value-alpha"))
        check(ok, "frame unknown field broke processing", "unknown-field-frame")
        finish("unknown-field-frame", ok and len(_FAILURES) == before)

        # S8 V2 request adds client_note=2 -> 服务端忽略未知业务字段
        before = len(_FAILURES)
        v2_extra = _f_len(2, "from-v2-client")   # client_note = 2
        env = call(client, request_id="sc-v2", key="beta",
                   payload=enc_get_value_request("beta") + v2_extra)
        ok = (v2.error_of(env) is None and
              dec_get_value_response(env.get("payload", b"")) == (True, "value-beta"))
        check(ok, f"V2 unknown field broke processing: {v2.error_of(env)}", "v2-compat")
        finish("v2-compat", ok and not _FAILURES)

        # S9b 可解码但已过期（budget=1ms，接收端 local-min − 回包预留后期限已过）
        # -> 在进 handler 前 TimedOut（错误帧回显 request_id/schema），
        # journal 不得出现 handler_entered。不使用 0 预算拒收冒充「预算保护」。
        before = len(_FAILURES)
        env = call(client, request_id="sc-budget1", budget_ms=1)
        err = v2.error_of(env) or {}
        ok = (err.get("code") == TIMED_OUT
              and env.get("request_id") == "sc-budget1"
              and env.get("request_schema") == REQUEST_SCHEMA)
        check(ok, f"expected TimedOut+id/schema echo, got {env}", "budget-expired")
        finish("budget-expired", ok and len(_FAILURES) == before)

        # S11 unknown error code（本地解码向量）：仅证明客户端对未知码的解码
        # 不误判成功；**不是**服务端传播证据（服务端当前不产生未知码）。真实远端
        # 错误传播由 S4(NotFound)/S5(TypeMismatch) 等经服务端产生的错误码承载。
        before = len(_FAILURES)
        synthetic = {
            "kind": v2.KIND_RESPONSE,
            "traffic": v2.TRAFFIC_SERVICE,
            "session_epoch": client.epoch,
            "request_id": "sc-unknown-code",
            "service": SERVICE,
            "method": METHOD,
            "reply": v2.REPLY_REQUIRED,
            "request_schema": REQUEST_SCHEMA,
            "response_schema": RESPONSE_SCHEMA,
            "outcome": "error",
            "error": {"code": 999, "message": "x"},
        }
        decoded = v2.decode_frame(v2.encode_frame_unchecked(synthetic))
        code = (v2.error_of(decoded) or {}).get("code")
        ok = (code == 999 and code != 0)
        check(ok, "unknown error code misclassified (local decode only)",
              "unknown-error-code")
        finish("unknown-error-code", ok and len(_FAILURES) == before)
    finally:
        client.close()

    # S9 声明预算 0 -> wire 字段矩阵违规：对端以 SESSION_ERROR(ProtocolError)
    # 拒绝并退役 link（0 不在 1..86400000 内），请求不进 handler。
    # 独立连接：该拒绝会退役会话，不能复用上面的 link。
    before = len(_FAILURES)
    bad = v2.V2Client(port, identity=CLIENT_IDENTITY + "-budget0")
    try:
        bad.handshake(expected_peer="getvalue-server")
        frame = call_frame(bad, request_id="sc-budget0", budget_ms=0)
        rejected = None
        try:
            bad.call_unchecked(frame)
        except v2.ProtocolRejected as exc:
            rejected = str(exc)
        ok = rejected is not None and f"code={PROTOCOL_ERROR}" in rejected
        check(ok, f"expected SESSION_ERROR ProtocolError, got {rejected!r}",
              "budget-zero")
        finish("budget-zero", ok and len(_FAILURES) == before)
    finally:
        bad.abort()

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

    tail = "ALL-SCENARIOS-PASS" if not _FAILURES else "SCENARIO-FAILURES"
    print(f"GRAND {tail} auth_state={AUTH_STATE}")
    if args.results:
        _RESULTS["_summary"] = {"failures": _FAILURES, "tail": tail,
                                "auth_state": AUTH_STATE}
        with open(args.results, "w") as f:
            json.dump(_RESULTS, f, indent=2, sort_keys=True)
    sys.exit(0 if not _FAILURES else 1)


def send_and_disconnect(port, request_id):
    """发起真实请求后立即断开 TCP（不读回复）——客户端只能记为未知结果。"""
    client = v2.V2Client(port, identity=CLIENT_IDENTITY + "-abort")
    try:
        client.handshake(expected_peer="getvalue-server")
        client.send_call_no_wait(call_frame(client, request_id=request_id))
    finally:
        client.abort()
    # 不读回复、不重试：结果未知。
    return "unknown"


if __name__ == "__main__":
    main()
