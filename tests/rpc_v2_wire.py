#!/usr/bin/env python3
"""V2 AppSession wire profile 的唯一零依赖实现（bbt.infra.rpc.v2）。

真源：bbtools-infra `proto/bbt/infra/rpc/v2/session_frame.proto`（profile 2）。
本模块按 proto3 wire format 手写 SessionFrame 编解码、u32be 分帧与
Hello/HelloAck 握手，供跨语言测试与示例共用——两份手写 codec 会漂移，
所以只允许存在这一份（tests/rpc_xlang_client.py 与
examples/getvalue/getvalue_client.py 都从这里导入）。

分帧（ADR 0014 §5.2）：u32be payload_length | payload[payload_length]；
payload_length==0 一律拒绝；Hello/HelloAck ≤ min(max_frame, 64KiB)。

握手（infra Session.cc OutboundHandshake/InboundHandshake）：两端各写一次
Hello、读对端 Hello；identity 字典序小者为 epoch owner，其 proposal 即
selected epoch，并写 HelloAck(epoch, 对端 nonce)；另一侧读 HelloAck 校验
peer_identity/session_epoch/connection_nonce 后原样 echo 一次。identity 相同
即 self-connection，对端拒绝。

用法：
    client = V2Client(port, identity="py-xlang-client")
    client.handshake()
    frame = client.call("svc.echo", "Echo", payload=b"ping")
    client.close()
"""

import socket
import struct
import time

PROFILE_VERSION = 2

# FrameKind
KIND_HELLO = 1
KIND_HELLO_ACK = 2
KIND_CALL = 3
KIND_RESPONSE = 4
KIND_SEND = 5
KIND_SEND_ACCEPTED = 6
KIND_SESSION_ERROR = 7
KIND_CLOSE = 8

KIND_NAMES = {
    KIND_HELLO: "HELLO",
    KIND_HELLO_ACK: "HELLO_ACK",
    KIND_CALL: "CALL",
    KIND_RESPONSE: "RESPONSE",
    KIND_SEND: "SEND",
    KIND_SEND_ACCEPTED: "SEND_ACCEPTED",
    KIND_SESSION_ERROR: "SESSION_ERROR",
    KIND_CLOSE: "CLOSE",
}

# TrafficClass
TRAFFIC_SYSTEM = 0
TRAFFIC_SERVICE = 1
TRAFFIC_ACTOR = 2

# ReplyMode
REPLY_NONE = 0
REPLY_REQUIRED = 1

# RpcErrorCode（与 session_frame.proto / bbt::infra::ErrorCode 同序）
ERR_INVALID_ARGUMENT = 1
ERR_INVALID_CONTEXT = 2
ERR_RUNTIME_UNAVAILABLE = 3
ERR_CLOSED = 4
ERR_CANCELLED = 5
ERR_TIMED_OUT = 6
ERR_OVERLOADED = 7
ERR_UNAVAILABLE = 8
ERR_TRANSPORT_ERROR = 9
ERR_PROTOCOL_ERROR = 10
ERR_NOT_FOUND = 11
ERR_TYPE_MISMATCH = 12
ERR_UNSUPPORTED_ROUTE = 13
ERR_OUTCOME_UNKNOWN = 14
ERR_REMOTE_ERROR = 15
ERR_INTERNAL_ERROR = 16

# 字段上限（SessionCodec.hpp）
MAX_FIELD_BYTES = 256
MAX_MESSAGE_ID_BYTES = 128
MAX_PEER_IDENTITY_BYTES = 256
MAX_ORDERING_KEY_BYTES = 256
MAX_METADATA_ITEMS = 32
MAX_METADATA_KEY_BYTES = 64
MAX_METADATA_VALUE_BYTES = 256
MAX_REMAINING_BUDGET_MS = 24 * 60 * 60 * 1000  # kNetworkLimitsMaxIncomingTimeout
DEFAULT_MAX_FRAME_BYTES = 8 * 1024 * 1024
HELLO_MAX_FRAME_BYTES = 64 * 1024

_WIRE_VARINT = 0
_WIRE_LEN = 2
_WIRE_FIXED32 = 5
_WIRE_FIXED64 = 1


class WireError(Exception):
    """本地编解码/握手/协议错误。"""


class ProtocolRejected(WireError):
    """对端以 SESSION_ERROR 拒绝，或对端在协议层退役了连接。"""


# --- proto3 wire format 最小编解码 --------------------------------------------


def _varint(n):
    if n < 0:
        raise WireError("varint must be non-negative")
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
    return _tag(field, _WIRE_VARINT) + _varint(n)


def _int32_varint(n):
    """protobuf int32 字段值 → varint：负值按 two's-complement 编码为
    64 位无符号 varint（10 字节）；非负值走普通 varint。"""
    if not isinstance(n, int) or isinstance(n, bool):
        raise WireError("int32 must be int")
    if n < -(1 << 31) or n > (1 << 31) - 1:
        raise WireError("int32 out of range: %r" % n)
    return _varint(n & 0xFFFFFFFFFFFFFFFF if n < 0 else n)


def _decode_int32(value):
    """无符号 varint → int32：取低 32 位按 two's-complement 解释（protobuf
    int32 的负值以 64 位形式上线，解码端丢弃高 32 位）。"""
    value &= 0xFFFFFFFF
    return value - (1 << 32) if value >= (1 << 31) else value


def _f_len(field, data):
    if isinstance(data, str):
        data = data.encode("utf-8")
    return _tag(field, _WIRE_LEN) + _varint(len(data)) + data


def _read_varint(buf, pos):
    shift = 0
    result = 0
    while True:
        if pos >= len(buf):
            raise WireError("truncated varint")
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7
        if shift >= 70:
            raise WireError("varint overflow")


def parse_fields(buf):
    """产出 (field_no, wire_type, value)；LEN 字段 value=bytes。"""
    pos = 0
    out = []
    while pos < len(buf):
        key, pos = _read_varint(buf, pos)
        field = key >> 3
        wire = key & 7
        if field == 0:
            raise WireError("field 0 invalid")
        if wire == _WIRE_VARINT:
            value, pos = _read_varint(buf, pos)
            out.append((field, wire, value))
        elif wire == _WIRE_LEN:
            n, pos = _read_varint(buf, pos)
            if pos + n > len(buf):
                raise WireError("truncated LEN field")
            out.append((field, wire, buf[pos:pos + n]))
            pos += n
        elif wire == _WIRE_FIXED32:
            if pos + 4 > len(buf):
                raise WireError("truncated fixed32")
            out.append((field, wire, buf[pos:pos + 4]))
            pos += 4
        elif wire == _WIRE_FIXED64:
            if pos + 8 > len(buf):
                raise WireError("truncated fixed64")
            out.append((field, wire, buf[pos:pos + 8]))
            pos += 8
        else:
            raise WireError("unsupported wire type %d" % wire)
    return out


# --- 编码 ---------------------------------------------------------------------


def _check_utf8(name, value, max_bytes, required=False):
    if not isinstance(value, str):
        raise WireError("%s must be str" % name)
    if required and not value:
        raise WireError("%s must be non-empty" % name)
    encoded = value.encode("utf-8")
    if len(encoded) > max_bytes:
        raise WireError("%s exceeds %d bytes" % (name, max_bytes))
    return encoded


def _encode_metadata(metadata):
    if len(metadata) > MAX_METADATA_ITEMS:
        raise WireError("metadata exceeds %d items" % MAX_METADATA_ITEMS)
    seen = set()
    out = b""
    for key, value in metadata:
        key_b = _check_utf8("metadata key", key, MAX_METADATA_KEY_BYTES,
                            required=True)
        value_b = _check_utf8("metadata value", value,
                              MAX_METADATA_VALUE_BYTES)
        if not (key_b.startswith(b"fw.") or key_b.startswith(b"route.")
                or key_b.startswith(b"trace.")):
            raise WireError("metadata key uses unregistered prefix: %r" % key)
        if key_b in seen:
            raise WireError("metadata duplicate key: %r" % key)
        seen.add(key_b)
        out += _f_len(1, key_b) + _f_len(2, value_b)
    return out


def _encode_error(error):
    out = b""
    if error.get("code"):
        out += _f_varint(1, error["code"])
    if error.get("domain"):
        out += _f_len(2, error["domain"])
    if error.get("domain_code"):
        out += _f_len(3, error["domain_code"])
    if error.get("message"):
        out += _f_len(4, error["message"])
    if error.get("backend_category"):
        out += _f_len(5, error["backend_category"])
    if error.get("backend_code"):
        # proto int32：负值必须按 two's-complement 编码（_varint 只接非负）。
        out += _tag(6, _WIRE_VARINT) + _int32_varint(error["backend_code"])
    if error.get("transferred_bytes"):
        out += _f_varint(7, error["transferred_bytes"])
    for key, value in error.get("details", ()):
        out += _f_len(8, _f_len(1, key) + _f_len(2, value))
    if error.get("request_phase") is not None:
        out += _f_varint(9, error["request_phase"])
    return out


def validate_frame(frame):
    """按 kind 的字段存在性矩阵做本地校验（编码前，失败即抛）。"""
    kind = frame["kind"]
    if kind not in KIND_NAMES:
        raise WireError("frame_kind is not a valid local kind: %r" % kind)
    traffic = frame.get("traffic", TRAFFIC_SYSTEM)
    if traffic not in (TRAFFIC_SYSTEM, TRAFFIC_SERVICE, TRAFFIC_ACTOR):
        raise WireError("traffic_class invalid")
    reply = frame.get("reply", REPLY_NONE)
    if reply not in (REPLY_NONE, REPLY_REQUIRED):
        raise WireError("reply_mode invalid")

    handshake = kind in (KIND_HELLO, KIND_HELLO_ACK)
    data = kind in (KIND_CALL, KIND_SEND, KIND_RESPONSE)
    outcome_kind = kind in (KIND_RESPONSE, KIND_SEND_ACCEPTED,
                            KIND_SESSION_ERROR)

    if data and traffic == TRAFFIC_SYSTEM:
        raise WireError("data frame must not use TRAFFIC_SYSTEM")
    if not data and traffic != TRAFFIC_SYSTEM:
        raise WireError("control frame must use TRAFFIC_SYSTEM")

    identity = frame.get("peer_identity", "")
    if handshake:
        _check_utf8("peer_identity", identity, MAX_PEER_IDENTITY_BYTES,
                    required=True)
    elif identity:
        raise WireError("peer_identity must be absent on non-handshake frame")

    nonce = frame.get("connection_nonce", 0)
    if handshake:
        if nonce == 0:
            raise WireError("connection_nonce must be non-zero on handshake")
    elif nonce != 0:
        raise WireError("connection_nonce must be absent on non-handshake")

    epoch = frame.get("session_epoch", 0)
    if (handshake or data or kind == KIND_SEND_ACCEPTED) and epoch == 0:
        raise WireError("session_epoch must be non-zero")

    message_id = frame.get("message_id", "")
    if kind in (KIND_SEND, KIND_SEND_ACCEPTED):
        _check_utf8("message_id", message_id, MAX_MESSAGE_ID_BYTES,
                    required=True)
    elif message_id:
        raise WireError("message_id must be absent on non-Send frame")

    request_id = frame.get("request_id", "")
    if kind in (KIND_CALL, KIND_RESPONSE) and not request_id:
        raise WireError("request_id required on Call/Response")
    if request_id:
        _check_utf8("request_id", request_id, MAX_FIELD_BYTES)

    require_route = kind in (KIND_CALL, KIND_RESPONSE)
    _check_utf8("service", frame.get("service", ""), MAX_FIELD_BYTES,
                required=require_route)
    _check_utf8("method", frame.get("method", ""), MAX_FIELD_BYTES,
                required=require_route)

    if kind in (KIND_CALL, KIND_RESPONSE) and reply != REPLY_REQUIRED:
        raise WireError("reply_mode must be Required on Call/Response")
    if kind not in (KIND_CALL, KIND_RESPONSE) and reply != REPLY_NONE:
        raise WireError("reply_mode must be None on non-Call/Response")

    sequence = frame.get("sequence", 0)
    ordering_key = frame.get("ordering_key", "")
    if sequence != 0:
        if not ordering_key or len(ordering_key) > MAX_ORDERING_KEY_BYTES:
            raise WireError("ordering_key required ≤256 when sequence enabled")
    elif ordering_key:
        raise WireError("ordering_key requires non-zero sequence")

    budget = frame.get("remaining_budget_ms", 0)
    if kind in (KIND_CALL, KIND_SEND):
        if budget == 0 or budget > MAX_REMAINING_BUDGET_MS:
            raise WireError("remaining_budget_ms must be in 1..max on Call/Send")
    elif kind in (KIND_RESPONSE, KIND_SEND_ACCEPTED):
        if budget > MAX_REMAINING_BUDGET_MS:
            raise WireError("remaining_budget_ms exceeds max")
    elif budget != 0:
        raise WireError("remaining_budget_ms must be absent on control frame")

    _check_utf8("request_schema", frame.get("request_schema", ""),
                MAX_FIELD_BYTES, required=require_route)
    _check_utf8("response_schema", frame.get("response_schema", ""),
                MAX_FIELD_BYTES, required=require_route)

    if "acceptance_requested" in frame and kind != KIND_SEND:
        raise WireError("acceptance_requested allowed only on Send")

    outcome = frame.get("outcome")
    if (outcome is not None) != outcome_kind:
        raise WireError("outcome presence mismatch for frame kind")
    if outcome == "error":
        _encode_error(frame.get("error", {}))

    _encode_metadata(frame.get("metadata", ()))


def encode_frame(frame):
    """编码一帧 protobuf body（不含 u32be 前缀）；先过本地字段矩阵。"""
    validate_frame(frame)
    return encode_frame_unchecked(frame)


def encode_frame_unchecked(frame):
    """跳过本地字段矩阵直接编码——只给负向测试构造「对端必拒」的帧用。"""
    out = _f_varint(1, PROFILE_VERSION)
    out += _f_varint(2, frame["kind"])
    out += _f_varint(3, frame.get("traffic", TRAFFIC_SYSTEM))
    if frame.get("peer_identity"):
        out += _f_len(4, frame["peer_identity"])
    out += _f_varint(5, frame.get("session_epoch", 0))
    if frame.get("message_id"):
        out += _f_len(6, frame["message_id"])
    if frame.get("request_id"):
        out += _f_len(7, frame["request_id"])
    if frame.get("service"):
        out += _f_len(8, frame["service"])
    if frame.get("method"):
        out += _f_len(9, frame["method"])
    out += _f_varint(10, frame.get("reply", REPLY_NONE))
    out += _f_varint(11, frame.get("sequence", 0))
    if frame.get("ordering_key"):
        out += _f_len(12, frame["ordering_key"])
    out += _f_varint(13, frame.get("remaining_budget_ms", 0))
    if frame.get("request_schema"):
        out += _f_len(14, frame["request_schema"])
    if frame.get("response_schema"):
        out += _f_len(15, frame["response_schema"])
    if frame.get("payload"):
        out += _f_len(16, frame["payload"])
    metadata = frame.get("metadata", ())
    if metadata:
        for entry in _split_metadata(metadata):
            out += _f_len(17, entry)
    if frame.get("outcome") == "success":
        out += _f_varint(18, 1)
    elif frame.get("outcome") == "error":
        out += _f_len(19, _encode_error(frame.get("error", {})))
    if frame.get("connection_nonce"):
        out += _f_varint(20, frame["connection_nonce"])
    if "acceptance_requested" in frame:
        out += _f_varint(21, 1 if frame["acceptance_requested"] else 0)
    return out


def _split_metadata(metadata):
    """把 (key,value) 列表编成独立 MetadataEntry body 列表。"""
    entries = []
    for key, value in metadata:
        entries.append(_f_len(1, key) + _f_len(2, value))
    return entries


def append_unknown_varint(body, field, value):
    """前向兼容测试用：在合法 body 尾部追加一个未知 varint 字段。"""
    return body + _tag(field, _WIRE_VARINT) + _varint(value)


def encode_framed(frame, max_frame_bytes=DEFAULT_MAX_FRAME_BYTES,
                  unchecked=False):
    body = encode_frame_unchecked(frame) if unchecked else encode_frame(frame)
    handshake = frame["kind"] in (KIND_HELLO, KIND_HELLO_ACK)
    limit = min(max_frame_bytes, HELLO_MAX_FRAME_BYTES) if handshake \
        else max_frame_bytes
    if not body:
        raise WireError("empty frame bytes")
    if len(body) > limit:
        raise WireError("frame body exceeds kind byte limit")
    return struct.pack(">I", len(body)) + body


# --- 解码 ---------------------------------------------------------------------


def _decode_error(buf):
    error = {"details": []}
    for field, wire, value in parse_fields(buf):
        if wire == _WIRE_VARINT:
            if field == 1:
                error["code"] = value
            elif field == 6:
                error["backend_code"] = _decode_int32(value)
            elif field == 7:
                error["transferred_bytes"] = value
            elif field == 9:
                error["request_phase"] = value
        elif wire == _WIRE_LEN:
            if field == 2:
                error["domain"] = value.decode("utf-8")
            elif field == 3:
                error["domain_code"] = value.decode("utf-8")
            elif field == 4:
                error["message"] = value.decode("utf-8")
            elif field == 5:
                error["backend_category"] = value.decode("utf-8")
            elif field == 8:
                entry = {}
                for df, dw, dv in parse_fields(value):
                    if dw == _WIRE_LEN and df == 1:
                        entry["key"] = dv.decode("utf-8")
                    elif dw == _WIRE_LEN and df == 2:
                        entry["value"] = dv.decode("utf-8")
                error["details"].append(
                    (entry.get("key", ""), entry.get("value", "")))
    return error


def decode_frame(body):
    """解码一帧 body；未知字段按 proto3 前向兼容忽略。"""
    if not body:
        raise WireError("empty frame bytes")
    frame = {"metadata": [], "unknown_fields": []}  # type: dict
    for field, wire, value in parse_fields(body):
        if wire == _WIRE_VARINT:
            if field == 1:
                frame["profile_version"] = value
            elif field == 2:
                frame["kind"] = value
            elif field == 3:
                frame["traffic"] = value
            elif field == 5:
                frame["session_epoch"] = value
            elif field == 10:
                frame["reply"] = value
            elif field == 11:
                frame["sequence"] = value
            elif field == 13:
                frame["remaining_budget_ms"] = value
            elif field == 18:
                if value:
                    frame["outcome"] = "success"
            elif field == 20:
                frame["connection_nonce"] = value
            elif field == 21:
                frame["acceptance_requested"] = bool(value)
            else:
                frame["unknown_fields"].append(field)
        elif wire == _WIRE_LEN:
            if field == 4:
                frame["peer_identity"] = value.decode("utf-8")
            elif field == 6:
                frame["message_id"] = value.decode("utf-8")
            elif field == 7:
                frame["request_id"] = value.decode("utf-8")
            elif field == 8:
                frame["service"] = value.decode("utf-8")
            elif field == 9:
                frame["method"] = value.decode("utf-8")
            elif field == 12:
                frame["ordering_key"] = value
            elif field == 14:
                frame["request_schema"] = value.decode("utf-8")
            elif field == 15:
                frame["response_schema"] = value.decode("utf-8")
            elif field == 16:
                frame["payload"] = bytes(value)
            elif field == 17:
                entry = {}
                for mf, mw, mv in parse_fields(value):
                    if mw == _WIRE_LEN and mf == 1:
                        entry["key"] = mv.decode("utf-8")
                    elif mw == _WIRE_LEN and mf == 2:
                        entry["value"] = mv.decode("utf-8")
                frame["metadata"].append(
                    (entry.get("key", ""), entry.get("value", "")))
            elif field == 19:
                frame["outcome"] = "error"
                frame["error"] = _decode_error(value)
            else:
                frame["unknown_fields"].append(field)
        else:
            frame["unknown_fields"].append(field)
    if frame.get("profile_version") != PROFILE_VERSION:
        raise WireError("unsupported profile_version: %r"
                        % frame.get("profile_version"))
    return frame


def frame_kind_name(frame):
    return KIND_NAMES.get(frame.get("kind"), "UNKNOWN(%r)" % frame.get("kind"))


# --- 流式分帧 + 握手 -----------------------------------------------------------


class FrameStream:
    """在已连接的 TCP socket 上按 u32be 分帧收发 SessionFrame。"""

    def __init__(self, sock, max_frame_bytes=DEFAULT_MAX_FRAME_BYTES):
        self._sock = sock
        self._max_frame_bytes = max_frame_bytes
        self._buf = bytearray()

    def send(self, frame):
        self._sock.sendall(encode_framed(frame, self._max_frame_bytes))

    def send_raw(self, data):
        self._sock.sendall(data)

    def send_unchecked(self, frame):
        """负向测试专用：跳过本地校验发一帧对端必拒的 wire。"""
        self._sock.sendall(encode_framed(frame, self._max_frame_bytes,
                                         unchecked=True))

    def _fill(self, n):
        while len(self._buf) < n:
            chunk = self._sock.recv(65536)
            if not chunk:
                return False
            self._buf.extend(chunk)
        return True

    def read(self, allow_eof=False):
        """读一帧；对端正常关闭返回 None（allow_eof）或抛 ProtocolRejected。"""
        if not self._fill(4):
            if allow_eof:
                return None
            raise ProtocolRejected("peer closed before frame prefix")
        length = struct.unpack(">I", bytes(self._buf[:4]))[0]
        del self._buf[:4]
        if length == 0:
            raise WireError("frame payload_length zero is rejected")
        if length > self._max_frame_bytes:
            raise WireError("frame payload_length exceeds phase limit")
        if not self._fill(length):
            if allow_eof:
                return None
            raise ProtocolRejected("peer closed mid-frame")
        body = bytes(self._buf[:length])
        del self._buf[:length]
        return decode_frame(body)


def hello_frame(identity, epoch, nonce):
    return {
        "kind": KIND_HELLO,
        "traffic": TRAFFIC_SYSTEM,
        "peer_identity": identity,
        "session_epoch": epoch,
        "connection_nonce": nonce,
    }


def hello_ack_frame(identity, epoch, peer_nonce):
    return {
        "kind": KIND_HELLO_ACK,
        "traffic": TRAFFIC_SYSTEM,
        "peer_identity": identity,
        "session_epoch": epoch,
        "connection_nonce": peer_nonce,
    }


def client_handshake(stream, identity, proposal, nonce,
                     expected_peer=None):
    """拨号侧握手；返回 selected epoch。"""
    stream.send(hello_frame(identity, proposal, nonce))
    remote = stream.read()
    if remote is None or remote.get("kind") != KIND_HELLO:
        raise ProtocolRejected(
            "expected Hello from peer, got %s" % frame_kind_name(remote or {}))
    peer_identity = remote.get("peer_identity", "")
    if expected_peer is not None and peer_identity != expected_peer:
        raise ProtocolRejected("peer identity mismatch: %r != %r"
                               % (peer_identity, expected_peer))
    if peer_identity == identity:
        raise ProtocolRejected("self-connection rejected by peer")

    own_epoch = identity < peer_identity
    epoch = 0
    if own_epoch:
        epoch = proposal
        stream.send(hello_ack_frame(identity, epoch,
                                    remote.get("connection_nonce", 0)))
    ack = stream.read()
    if ack is None or ack.get("kind") != KIND_HELLO_ACK:
        raise ProtocolRejected(
            "expected HelloAck from peer, got %s" % frame_kind_name(ack or {}))
    if ack.get("peer_identity") != peer_identity:
        raise ProtocolRejected("HelloAck identity mismatch")
    if ack.get("connection_nonce") != nonce:
        raise ProtocolRejected("HelloAck echo mismatch (nonce)")
    if own_epoch:
        if ack.get("session_epoch") != epoch:
            raise ProtocolRejected("HelloAck echo mismatch (epoch)")
        return epoch
    epoch = ack.get("session_epoch", 0)
    stream.send(hello_ack_frame(identity, epoch,
                                remote.get("connection_nonce", 0)))
    return epoch


def server_handshake(stream, identity, proposal, nonce, expected_peer=None):
    """接受侧握手；返回 selected epoch。

    与 client_handshake 对称：两端各写一次 Hello、读对端 Hello；identity
    字典序小者为 epoch owner（其 proposal 即 selected），owner 写
    HelloAck(epoch, 对端 nonce)；另一侧读 HelloAck 校验后原样 echo 一次。
    """
    remote = stream.read()
    if remote is None or remote.get("kind") != KIND_HELLO:
        raise ProtocolRejected(
            "expected Hello from peer, got %s" % frame_kind_name(remote or {}))
    peer_identity = remote.get("peer_identity", "")
    if expected_peer is not None and peer_identity != expected_peer:
        raise ProtocolRejected("peer identity mismatch: %r != %r"
                               % (peer_identity, expected_peer))
    if peer_identity == identity:
        raise ProtocolRejected("self-connection rejected by peer")

    stream.send(hello_frame(identity, proposal, nonce))
    own_epoch = identity < peer_identity
    if own_epoch:
        epoch = proposal
        stream.send(hello_ack_frame(identity, epoch,
                                    remote.get("connection_nonce", 0)))
        ack = stream.read()
        if ack is None or ack.get("kind") != KIND_HELLO_ACK:
            raise ProtocolRejected(
                "expected HelloAck echo from peer, got %s"
                % frame_kind_name(ack or {}))
        if ack.get("peer_identity") != peer_identity:
            raise ProtocolRejected("HelloAck echo identity mismatch")
        if ack.get("connection_nonce") != nonce:
            raise ProtocolRejected("HelloAck echo mismatch (nonce)")
        if ack.get("session_epoch") != epoch:
            raise ProtocolRejected("HelloAck echo mismatch (epoch)")
        return epoch
    ack = stream.read()
    if ack is None or ack.get("kind") != KIND_HELLO_ACK:
        raise ProtocolRejected(
            "expected HelloAck from peer, got %s" % frame_kind_name(ack or {}))
    epoch = ack.get("session_epoch", 0)
    if ack.get("peer_identity") != peer_identity:
        raise ProtocolRejected("HelloAck identity mismatch")
    if ack.get("connection_nonce") != nonce:
        raise ProtocolRejected("HelloAck echo mismatch (nonce)")
    stream.send(hello_ack_frame(identity, epoch,
                                remote.get("connection_nonce", 0)))
    return epoch


class V2Client:
    """真实 TCP 的 V2 会话客户端（同步、零依赖）。"""

    def __init__(self, port, identity, host="127.0.0.1", timeout=10.0,
                 max_frame_bytes=DEFAULT_MAX_FRAME_BYTES, epoch=None):
        self.identity = identity
        self.port = port
        self.timeout = timeout
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._stream = FrameStream(self._sock, max_frame_bytes)
        self.epoch = epoch if epoch is not None else int(time.time() * 1000)
        self._next_request = 0

    def handshake(self, expected_peer=None):
        self.epoch = client_handshake(self._stream, self.identity, self.epoch,
                                      nonce_for(self.identity),
                                      expected_peer=expected_peer)
        return self.epoch

    def _new_request_id(self, prefix):
        self._next_request += 1
        return "%s-%d" % (prefix, self._next_request)

    def call(self, service, method, payload=b"", request_id=None,
             request_schema="bbt.echo.EchoReq/v1",
             response_schema="bbt.echo.EchoResp/v1",
             budget_ms=3000, metadata=(), traffic=TRAFFIC_SERVICE):
        if request_id is None:
            request_id = self._new_request_id("py-req")
        frame = {
            "kind": KIND_CALL,
            "traffic": traffic,
            "session_epoch": self.epoch,
            "request_id": request_id,
            "service": service,
            "method": method,
            "reply": REPLY_REQUIRED,
            "remaining_budget_ms": budget_ms,
            "request_schema": request_schema,
            "response_schema": response_schema,
            "payload": payload,
            "metadata": list(metadata),
        }
        self._stream.send(frame)
        return self._await_response()

    def call_unchecked(self, frame, unknown_fields=()):
        """负向/前向兼容测试入口：跳过本地字段矩阵编码并发送一帧 CALL，
        等待 RESPONSE；unknown_fields 为 (field, varint) 追加项。"""
        body = encode_frame_unchecked(frame)
        for field, value in unknown_fields:
            body = append_unknown_varint(body, field, value)
        self._stream.send_raw(struct.pack(">I", len(body)) + body)
        return self._await_response()

    def send_call_no_wait(self, frame):
        """发送一帧 CALL 后立即返回（不读回复）——用于「发起后断连」用例。"""
        self._stream.send(frame)

    def _await_response(self):
        while True:
            frame = self._stream.read()
            if frame is None:
                raise ProtocolRejected("peer closed before response")
            kind = frame.get("kind")
            if kind == KIND_RESPONSE:
                return frame
            if kind == KIND_SESSION_ERROR:
                raise ProtocolRejected(
                    "session error code=%r message=%r"
                    % (frame.get("error", {}).get("code"),
                       frame.get("error", {}).get("message")))
            if kind == KIND_SEND_ACCEPTED:
                continue  # 与 Call 无关的控制帧，继续等
            raise WireError("unexpected frame kind while awaiting response: %s"
                            % frame_kind_name(frame))

    def send(self, service, method, payload=b"", message_id=None,
             request_schema="bbt.echo.EchoReq/v1",
             response_schema="bbt.echo.EchoResp/v1",
             budget_ms=3000, metadata=(), require_acceptance=True,
             traffic=TRAFFIC_SERVICE):
        if message_id is None:
            message_id = self._new_request_id("py-msg")
        frame = {
            "kind": KIND_SEND,
            "traffic": traffic,
            "session_epoch": self.epoch,
            "message_id": message_id,
            "service": service,
            "method": method,
            "remaining_budget_ms": budget_ms,
            "request_schema": request_schema,
            "response_schema": response_schema,
            "payload": payload,
            "metadata": list(metadata),
            "acceptance_requested": bool(require_acceptance),
        }
        self._stream.send(frame)
        if not require_acceptance:
            return None
        accepted = self._stream.read()
        if accepted is None:
            raise ProtocolRejected("peer closed before SendAccepted")
        if accepted.get("kind") != KIND_SEND_ACCEPTED:
            raise WireError("expected SendAccepted, got %s"
                            % frame_kind_name(accepted))
        return accepted

    def close(self):
        try:
            self._stream.send({
                "kind": KIND_CLOSE,
                "traffic": TRAFFIC_SYSTEM,
                "session_epoch": self.epoch,
            })
        except (OSError, WireError):
            pass
        self._sock.close()

    def abort(self):
        """不发送 CLOSE，直接断开 TCP——用于「发起后强制断连」用例。"""
        self._sock.close()


def nonce_for(identity):
    """稳定的非零 nonce（不用随机数，便于复现）。"""
    digest = 0
    for ch in identity.encode("utf-8"):
        digest = (digest * 131 + ch) & 0xFFFFFFFFFFFF
    return digest or 1


def error_of(frame):
    """取 error 分支；成功帧返回 None。"""
    if frame.get("outcome") == "error":
        return frame.get("error", {})
    return None


def _selfcheck():
    """零依赖自检：python3 rpc_v2_wire.py 直接运行，断言关键 wire 事实。

    负 int32 backend_code 是跨语言必须一致的编码面——proto3 int32 的负值
    以 64 位 two's-complement varint 上线（与 C++ 一致的字节级真源）。
    """
    for value in (-1, -7, -(1 << 31), 0, 1, 111, (1 << 31) - 1):
        error = _decode_error(
            _tag(6, _WIRE_VARINT) + _int32_varint(value))
        assert error["backend_code"] == value, (
            "backend_code round-trip %r -> %r" % (value, error["backend_code"]))
    # 字节级：-1 与 INT32_MIN 的 10 字节编码与 C++ protobuf 一致。
    assert _int32_varint(-1) == b"\xff" * 9 + b"\x01"
    assert _int32_varint(-(1 << 31)) == \
        b"\x80\x80\x80\x80\xf8\xff\xff\xff\xff\x01"
    # 完整 error 分支编解码路径也走同一 int32 编解码。
    error = _decode_error(_encode_error({"backend_code": -7}))
    assert error["backend_code"] == -7
    try:
        _int32_varint(1 << 31)
        raise AssertionError("out-of-range int32 not rejected")
    except WireError:
        pass
    print("rpc_v2_wire selfcheck ok")


if __name__ == "__main__":
    _selfcheck()
