#!/usr/bin/env python3
"""framework #4：G5 RpcEnvelope wire profile 的 Python 客户端（零依赖）。

不依赖 google.protobuf：按 proto3 wire format 手写
proto/bbt/infra/rpc/v1/rpc_envelope.proto 的编解码，直接向 C++
rpc_xlang_server 发起真实 HTTP POST /rpc 调用。手写 wire 编解码本身是
更强的互通证据——双方只在字节流层面共享同一 schema。

覆盖场景：
  1. round-trip：正常请求/响应 envelope 字段无损往返；
  2. 业务错误 envelope（InvalidArgument + details）；
  3. domain/domain_code/backend_* 错误字段透传；
  4. 未知路由 → RemoteError；
  5. profile_version 不兼容 → ProtocolError envelope；
  6. 截断 body → ProtocolError；
  7. 空 body → ProtocolError；
  8. 未知字段（proto3 前向兼容）被忽略。

用法：python3 rpc_xlang_client.py --port <port>
"""

import argparse
import http.client
import sys

RPC_PATH = "/rpc"
CONTENT_TYPE = "application/x-protobuf"

# RpcErrorCode（与 rpc_envelope.proto enum 一致）
INVALID_ARGUMENT = 1
UNAVAILABLE = 8
PROTOCOL_ERROR = 10
REMOTE_ERROR = 15

WIRE_VARINT = 0
WIRE_LEN = 2


# --- proto3 wire format 最小编解码 --------------------------------------------

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
    """产出 (field_no, wire_type, value)；LEN 字段 value=bytes。"""
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
        elif wire == 5:  # fixed32，本 schema 不用但按 proto3 跳过
            if pos + 4 > len(buf):
                raise ValueError("truncated fixed32")
            out.append((field, wire, buf[pos:pos + 4]))
            pos += 4
        elif wire == 1:  # fixed64
            if pos + 8 > len(buf):
                raise ValueError("truncated fixed64")
            out.append((field, wire, buf[pos:pos + 8]))
            pos += 8
        else:
            raise ValueError(f"unsupported wire type {wire}")
    return out


def encode_metadata_entry(key, value):
    return _f_len(1, key) + _f_len(2, value)


def encode_envelope(env):
    """env: dict 表示 RpcEnvelopeMsg。"""
    out = _f_varint(1, env["profile_version"])
    out += _f_len(2, env["request_id"])
    out += _f_len(3, env["service"])
    out += _f_len(4, env["method"])
    if env.get("remaining_budget_ms"):
        out += _f_varint(5, env["remaining_budget_ms"])
    if env.get("request_schema"):
        out += _f_len(6, env["request_schema"])
    if env.get("response_schema"):
        out += _f_len(7, env["response_schema"])
    if env.get("payload"):
        out += _f_len(8, env["payload"])
    for k, v in env.get("metadata", []):
        out += _f_len(9, encode_metadata_entry(k, v))
    if env.get("success"):
        out += _f_varint(10, 1)
    return out


def decode_error(buf):
    err = {}
    details = []
    for f, w, v in _parse_fields(buf):
        if w == WIRE_VARINT:
            if f == 1:
                err["code"] = v
            elif f == 6:
                err["backend_code"] = v
            elif f == 7:
                err["transferred_bytes"] = v
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


def decode_envelope(buf):
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
                env["error"] = decode_error(v)
            else:
                env["unknown_fields"].append(f)
    return env


def make_request(**over):
    env = {
        "profile_version": 1,
        "request_id": "py-req-1",
        "service": "svc.echo",
        "method": "Echo",
        "remaining_budget_ms": 3000,
        "request_schema": "bbt.echo.EchoReq/v1",
        "response_schema": "bbt.echo.EchoResp/v1",
        "payload": b"ping-from-python",
        "metadata": [("route.zone", "z1"), ("trace.id", "t-py")],
    }
    env.update(over)
    return env


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


def fail(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def expect_error_envelope(port, req_bytes, expect_code, label):
    status, ctype, body = post_raw(port, req_bytes)
    if status != 200:
        fail(f"{label}: http status={status}（错误也应以 envelope 返回）")
    if ctype != CONTENT_TYPE:
        fail(f"{label}: content-type={ctype!r}")
    try:
        env = decode_envelope(body)
    except ValueError as e:
        fail(f"{label}: response not decodable envelope: {e}")
    if "error" not in env:
        fail(f"{label}: response missing error outcome")
    if env["error"].get("code") != expect_code:
        fail(f"{label}: error.code={env['error'].get('code')} expect={expect_code}")
    return env


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    args = ap.parse_args()
    port = args.port

    # ---- 场景 1：round-trip ------------------------------------------------
    req = make_request()
    status, ctype, body = post_raw(port, encode_envelope(req))
    if status != 200 or ctype != CONTENT_TYPE:
        fail(f"round-trip: status={status} ctype={ctype}")
    resp = decode_envelope(body)
    if "error" in resp:
        fail(f"round-trip: error envelope code={resp['error'].get('code')}")
    if not resp.get("success"):
        fail("round-trip: missing success outcome")
    for field in ("request_id", "service", "method",
                  "request_schema", "response_schema"):
        if resp.get(field) != req[field]:
            fail(f"round-trip: {field} mismatch "
                 f"{resp.get(field)!r} != {req[field]!r}")
    if resp.get("payload") != req["payload"]:
        fail("round-trip: payload mismatch")
    if sorted(resp.get("metadata", [])) != sorted(req["metadata"]):
        fail(f"round-trip: metadata mismatch {resp.get('metadata')}")
    print("PASS scenario=round-trip")

    # ---- 场景 2：业务错误 envelope（InvalidArgument + details） -------------
    env = expect_error_envelope(
        port,
        encode_envelope(make_request(method="Fail", request_id="py-fail-1")),
        INVALID_ARGUMENT, "error-envelope")
    if env["error"].get("domain") != "framework.test":
        fail(f"error-envelope: domain={env['error'].get('domain')!r}")
    if "svc.echo/Fail" not in env["error"].get("message", ""):
        fail(f"error-envelope: message={env['error'].get('message')!r}")
    if ("reason", "explicit-fail") not in env["error"]["details"]:
        fail(f"error-envelope: details={env['error']['details']}")
    if env.get("request_id") != "py-fail-1":
        fail("error-envelope: request_id not echoed")
    print("PASS scenario=error-envelope")

    # ---- 场景 3：domain_code/backend_* 错误字段透传 --------------------------
    env = expect_error_envelope(
        port,
        encode_envelope(make_request(method="FailDomain",
                                     request_id="py-faildom-1")),
        UNAVAILABLE, "error-envelope-domain")
    e = env["error"]
    if e.get("domain") != "framework.test.backend":
        fail(f"fail-domain: domain={e.get('domain')!r}")
    if e.get("domain_code") != "E_BACKEND_DOWN":
        fail(f"fail-domain: domain_code={e.get('domain_code')!r}")
    if e.get("backend_category") != "tcp" or e.get("backend_code") != 111:
        fail(f"fail-domain: backend_* mismatch {e}")
    if ("backend", "127.0.0.1:9999") not in e["details"]:
        fail(f"fail-domain: details={e['details']}")
    print("PASS scenario=error-envelope-domain")

    # ---- 场景 4：未知路由 → RemoteError -------------------------------------
    env = expect_error_envelope(
        port,
        encode_envelope(make_request(method="NoSuch",
                                     request_id="py-nosuch-1")),
        REMOTE_ERROR, "unknown-route")
    print("PASS scenario=unknown-route")

    # ---- 场景 5：profile_version 不兼容 → ProtocolError envelope ------------
    badver = make_request(request_id="py-badver-1")
    badver["profile_version"] = 99
    env = expect_error_envelope(port, encode_envelope(badver),
                                PROTOCOL_ERROR, "version-incompatible")
    print("PASS scenario=version-incompatible")

    # ---- 场景 6：截断 body → ProtocolError ----------------------------------
    truncated = encode_envelope(req)[:8]
    env = expect_error_envelope(port, truncated, PROTOCOL_ERROR,
                                "truncated-body")
    print("PASS scenario=truncated-body")

    # ---- 场景 7：空 body → ProtocolError ------------------------------------
    env = expect_error_envelope(port, b"", PROTOCOL_ERROR, "empty-body")
    print("PASS scenario=empty-body")

    # ---- 场景 8：未知字段前向兼容（proto3 忽略未知字段） ----------------------
    # 手工在合法 envelope 尾部追加一个未知字段（field=200 varint）。
    fwd = encode_envelope(make_request(request_id="py-fwd-1"))
    fwd += _tag(200, WIRE_VARINT) + _varint(12345)
    status, ctype, body = post_raw(port, fwd)
    if status != 200:
        fail(f"unknown-field-fwd: status={status}")
    resp = decode_envelope(body)
    if "error" in resp:
        fail(f"unknown-field-fwd: rejected with "
             f"code={resp['error'].get('code')}")
    if resp.get("request_id") != "py-fwd-1" or not resp.get("success"):
        fail("unknown-field-fwd: envelope not processed normally")
    print("PASS scenario=unknown-field-forward-compat")

    print("ALL-SCENARIOS-PASS")


if __name__ == "__main__":
    main()
