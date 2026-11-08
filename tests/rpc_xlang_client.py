#!/usr/bin/env python3
"""V2 SessionFrame wire profile 的 Python 客户端（零依赖）。

不依赖 google.protobuf：编解码、u32be 分帧与 Hello/HelloAck 握手全部来自
同目录 tests/rpc_v2_wire.py（唯一手写 codec，跨语言测试与示例共用）。
客户端向 C++ rpc_xlang_server 发起真实 TCP loopback 调用——双方只在字节流
层面共享同一 schema，手写 wire 本身就是互通证据。

覆盖场景：
  1. round-trip：Call/Response 五关联字段、payload、metadata 无损往返；
  2. 业务错误分支（InvalidArgument + details + domain）；
  3. domain/domain_code/backend_* 错误字段透传；
  4. 未知路由 → RemoteError；
  5. 字段矩阵违规（Call 缺 request_id）→ 对端 SESSION_ERROR(ProtocolError)；
  6. profile_version 不兼容 → 对端退役 link（无响应、连接关闭）；
  7. 帧前缀为 0 → 对端退役 link；
  8. 未知字段（proto3 前向兼容）被忽略，请求照常处理；
  9. 单向 Send + SendAccepted（V2 能力，V1 无此面）；
 10. self-connection（Hello 身份与服务端相同）→ 对端拒绝并关闭。

用法：python3 rpc_xlang_client.py --port <port> [--server-identity <id>]
"""

import argparse
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import rpc_v2_wire as wire  # noqa: E402

SERVER_IDENTITY = "cpp-xlang-server"
CLIENT_IDENTITY = "py-xlang-client"

ECHO_REQ = "bbt.echo.EchoReq/v1"
ECHO_RESP = "bbt.echo.EchoResp/v1"


def fail(msg):
    print("FAIL: %s" % msg, file=sys.stderr)
    sys.exit(1)


def expect_error(frame, code, label):
    error = wire.error_of(frame)
    if error is None:
        fail("%s: response missing error outcome (kind=%s)"
             % (label, wire.frame_kind_name(frame)))
    if error.get("code") != code:
        fail("%s: error.code=%r expect=%r (message=%r)"
             % (label, error.get("code"), code, error.get("message")))
    return error


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--server-identity", default=SERVER_IDENTITY)
    args = ap.parse_args()
    port = args.port
    server_identity = args.server_identity

    client = wire.V2Client(port, CLIENT_IDENTITY)
    epoch = client.handshake(expected_peer=server_identity)
    if epoch == 0:
        fail("handshake: selected epoch is zero")
    print("PASS scenario=handshake epoch=%d" % epoch)

    # ---- 场景 1：round-trip ------------------------------------------------
    request_id = "py-req-1"
    metadata = [("route.zone", "z1"), ("trace.id", "t-py")]
    payload = b"ping-from-python"
    resp = client.call("svc.echo", "Echo", payload=payload,
                       request_id=request_id, request_schema=ECHO_REQ,
                       response_schema=ECHO_RESP, metadata=metadata)
    if resp.get("kind") != wire.KIND_RESPONSE:
        fail("round-trip: unexpected frame kind")
    if resp.get("outcome") != "success":
        fail("round-trip: missing success outcome")
    for field, want in (("request_id", request_id),
                        ("service", "svc.echo"),
                        ("method", "Echo"),
                        ("request_schema", ECHO_REQ),
                        ("response_schema", ECHO_RESP)):
        if resp.get(field) != want:
            fail("round-trip: %s mismatch %r != %r"
                 % (field, resp.get(field), want))
    if resp.get("payload") != payload:
        fail("round-trip: payload mismatch %r" % (resp.get("payload"),))
    if sorted(resp.get("metadata", [])) != sorted(metadata):
        fail("round-trip: metadata mismatch %r" % (resp.get("metadata"),))
    if resp.get("session_epoch") != epoch:
        fail("round-trip: response epoch mismatch")
    print("PASS scenario=round-trip")

    # ---- 场景 2：业务错误分支（InvalidArgument + details + domain） ---------
    err = expect_error(
        client.call("svc.echo", "Fail", payload=b"x", request_id="py-fail-1",
                    request_schema=ECHO_REQ, response_schema=ECHO_RESP),
        wire.ERR_INVALID_ARGUMENT, "error-branch")
    if err.get("domain") != "framework.test":
        fail("error-branch: domain=%r" % (err.get("domain"),))
    if "svc.echo/Fail" not in err.get("message", ""):
        fail("error-branch: message=%r" % (err.get("message"),))
    if ("reason", "explicit-fail") not in err["details"]:
        fail("error-branch: details=%r" % (err["details"],))
    print("PASS scenario=error-branch")

    # ---- 场景 3：domain_code/backend_* 错误字段透传 --------------------------
    err = expect_error(
        client.call("svc.echo", "FailDomain", request_id="py-faildom-1",
                    request_schema=ECHO_REQ, response_schema=ECHO_RESP),
        wire.ERR_UNAVAILABLE, "error-branch-domain")
    if err.get("domain") != "framework.test.backend":
        fail("error-branch-domain: domain=%r" % (err.get("domain"),))
    if err.get("domain_code") != "E_BACKEND_DOWN":
        fail("error-branch-domain: domain_code=%r" % (err.get("domain_code"),))
    if err.get("backend_category") != "tcp" or err.get("backend_code") != 111:
        fail("error-branch-domain: backend_* mismatch %r" % (err,))
    if ("backend", "127.0.0.1:9999") not in err["details"]:
        fail("error-branch-domain: details=%r" % (err["details"],))
    print("PASS scenario=error-branch-domain")

    # ---- 场景 4：未知路由 → RemoteError --------------------------------
    expect_error(
        client.call("svc.echo", "NoSuch", request_id="py-nosuch-1",
                    request_schema=ECHO_REQ, response_schema=ECHO_RESP),
        wire.ERR_REMOTE_ERROR, "unknown-route")
    print("PASS scenario=unknown-route")

    # ---- 场景 9：Send + SendAccepted（V2 单向面） ---------------------------
    accepted = client.send("svc.echo", "Echo", payload=b"one-way",
                           message_id="py-msg-1", request_schema=ECHO_REQ,
                           response_schema=ECHO_RESP,
                           require_acceptance=True)
    if accepted.get("kind") != wire.KIND_SEND_ACCEPTED:
        fail("send-accepted: unexpected kind")
    if accepted.get("outcome") != "success":
        fail("send-accepted: missing success outcome")
    if accepted.get("message_id") != "py-msg-1":
        fail("send-accepted: message_id=%r" % (accepted.get("message_id"),))
    print("PASS scenario=send-accepted")

    # ---- 场景 8：未知字段前向兼容 ------------------------------------------
    fwd_frame = {
        "kind": wire.KIND_CALL,
        "traffic": wire.TRAFFIC_SERVICE,
        "session_epoch": epoch,
        "request_id": "py-fwd-1",
        "service": "svc.echo",
        "method": "Echo",
        "reply": wire.REPLY_REQUIRED,
        "remaining_budget_ms": 3000,
        "request_schema": ECHO_REQ,
        "response_schema": ECHO_RESP,
        "payload": b"fwd",
    }
    # 在合法 body 尾部追加一个未知字段（field=200 varint 12345）。
    body = wire.append_unknown_varint(wire.encode_frame(fwd_frame), 200, 12345)
    client._stream.send_raw(struct.pack(">I", len(body)) + body)
    fwd = client._await_response()
    if fwd.get("outcome") != "success" or fwd.get("request_id") != "py-fwd-1":
        fail("unknown-field-forward-compat: frame not processed normally")
    print("PASS scenario=unknown-field-forward-compat")

    # ---- 场景 5：字段矩阵违规 → SESSION_ERROR(ProtocolError) ----------------
    bad = dict(fwd_frame)
    bad["request_id"] = ""  # Call 必须有 request_id
    try:
        client._stream.send_unchecked(bad)
        client._await_response()
        fail("field-matrix: peer accepted an invalid Call frame")
    except wire.ProtocolRejected as exc:
        if "10" not in str(exc):
            fail("field-matrix: expected ProtocolError(10), got %s" % exc)
    print("PASS scenario=field-matrix-reject")
    client.close()

    # ---- 场景 6：profile_version 不兼容 → link 退役 --------------------------
    bad_ver = wire.V2Client(port, "py-badver-client")
    bad_ver.handshake(expected_peer=server_identity)
    frame = dict(fwd_frame)
    frame["session_epoch"] = bad_ver.epoch
    body = bytearray(wire.encode_frame(frame))
    body[0:2] = bytes([0x08, 99])  # field 1 varint = 99
    bad_ver._stream.send_raw(struct.pack(">I", len(body)) + bytes(body))
    try:
        # 协议拒绝可以是 SESSION_ERROR(ProtocolError) 帧，也可以是直接退役
        # link；两者都不允许出现正常 Response/SendAccepted。
        frame = bad_ver._stream.read()
        if frame is not None and frame.get("kind") != wire.KIND_SESSION_ERROR:
            fail("version-incompatible: peer answered an unsupported profile")
    except wire.ProtocolRejected:
        pass
    bad_ver.close()
    print("PASS scenario=version-incompatible")

    # ---- 场景 7：帧前缀为 0 → link 退役 -------------------------------------
    zero = wire.V2Client(port, "py-zero-prefix")
    zero.handshake(expected_peer=server_identity)
    zero._stream.send_raw(struct.pack(">I", 0))
    try:
        # 同 version-incompatible：拒绝可以是 SESSION_ERROR 或 link 退役。
        frame = zero._stream.read()
        if frame is not None and frame.get("kind") != wire.KIND_SESSION_ERROR:
            fail("zero-prefix: peer accepted payload_length 0")
    except wire.ProtocolRejected:
        pass
    zero.close()
    print("PASS scenario=zero-prefix-reject")

    # ---- 场景 10：self-connection 被拒 --------------------------------------
    self_conn = wire.V2Client(port, "py-self-client")
    try:
        # Hello 自称服务端身份 → 服务端按 self-connection 拒绝。
        self_conn._stream.send(wire.hello_frame(server_identity,
                                                self_conn.epoch,
                                                wire.nonce_for("self")))
        self_conn._stream.read()
        fail("self-connection: peer did not reject duplicate identity")
    except (wire.ProtocolRejected, wire.WireError):
        pass
    self_conn.close()
    print("PASS scenario=self-connection-rejected")

    print("ALL-SCENARIOS-PASS")


if __name__ == "__main__":
    main()
