"""Sans-io WebSocket (RFC 6455) client core: no asyncio, no sockets.

Two pull-parsers in the house style (:meth:`receive_data` + :meth:`next_event`,
returning :data:`WS_NEED_DATA` when the buffer does not yet hold a full unit):

* :class:`WSHandshake` — builds the HTTP/1.1 ``Upgrade`` request and parses the
  ``101 Switching Protocols`` response, validating ``Sec-WebSocket-Accept``.
* :class:`WSFrameDecoder` — decodes server frames (unmasked) into typed events,
  reassembling nothing at the message level: each data frame's payload is handed
  up immediately as opaque NATS bytes (the NATS parser does its own framing).

Plus stateless client frame encoders (FIN=1, always client-masked per RFC).

Compression stance: we NEVER offer ``permessage-deflate``. A deliberate v1
simplicity choice — the reserved (RSV) bits are consequently a hard protocol
violation on decode, since no extension was negotiated. nats.go offers it
optionally; declining keeps the whole codec allocation-light and branch-free.

Framing violations are fatal, exactly like the NATS parser: a WebSocket stream
cannot be resynchronized mid-frame, so a :class:`WebsocketError` tears the
transport down and the decoder refuses further use.
"""

import base64
import hashlib
import os
from dataclasses import dataclass
from enum import Enum
from typing import Final, Literal

from natsio.errors import WebsocketError

__all__ = [
    "WS_NEED_DATA",
    "WSClose",
    "WSData",
    "WSFrameDecoder",
    "WSFrameEvent",
    "WSHandshake",
    "WSHandshakeAccepted",
    "WSPing",
    "WSPong",
    "encode_binary_frame",
    "encode_close",
    "encode_ping",
    "encode_pong",
]

# From https://www.rfc-editor.org/rfc/rfc6455#section-1.3
_GUID: Final = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

# Opcodes (RFC 6455 section 5.2).
_OP_CONT: Final = 0x0
_OP_TEXT: Final = 0x1
_OP_BINARY: Final = 0x2
_OP_CLOSE: Final = 0x8
_OP_PING: Final = 0x9
_OP_PONG: Final = 0xA

_FIN_BIT: Final = 0x80
_RSV_BITS: Final = 0x70
_MASK_BIT: Final = 0x80
_LEN_MASK: Final = 0x7F

_MAX_CONTROL_PAYLOAD: Final = 125
# Ceiling on a single declared frame length (nats.go caps its reader at 64MB).
# Frames the server would legitimately send are bounded by max_payload plus
# protocol overhead — far below this; anything bigger is hostile or broken.
MAX_FRAME_SIZE: Final = 64 * 1024 * 1024
# Guard against an unbounded HTTP header block before the 101 line completes.
_MAX_HANDSHAKE_BYTES: Final = 64 * 1024
# Consumed-prefix length after which the decoder compacts its receive buffer.
_COMPACT_THRESHOLD: Final = 64 * 1024

_CLOSE_NORMAL: Final = 1000
_CLOSE_NO_STATUS: Final = 1005


class _WSNeedData(Enum):
    """Sentinel returned when the buffer does not yet hold a complete unit."""

    WS_NEED_DATA = "WS_NEED_DATA"

    def __repr__(self) -> str:
        return "WS_NEED_DATA"


WS_NEED_DATA: Final = _WSNeedData.WS_NEED_DATA


# -- frame events -----------------------------------------------------------


@dataclass(frozen=True, slots=True)
class WSData:
    """Opaque application bytes from a binary/text/continuation data frame."""

    payload: bytes


@dataclass(frozen=True, slots=True)
class WSPing:
    payload: bytes


@dataclass(frozen=True, slots=True)
class WSPong:
    payload: bytes


@dataclass(frozen=True, slots=True)
class WSClose:
    code: int
    reason: str


type WSFrameEvent = WSData | WSPing | WSPong | WSClose
type _FrameOutput = WSFrameEvent | Literal[_WSNeedData.WS_NEED_DATA]


@dataclass(frozen=True, slots=True)
class WSHandshakeAccepted:
    """The 101 response was valid. ``leftover`` is the first frame bytes that
    arrived glued to the header block and MUST be fed to the frame decoder."""

    leftover: bytes


type _HandshakeOutput = WSHandshakeAccepted | Literal[_WSNeedData.WS_NEED_DATA]


# -- masking ----------------------------------------------------------------


def _apply_mask(data: bytes, key: bytes) -> bytes:
    """XOR ``data`` with the repeating 4-byte ``key`` (RFC 6455 section 5.3).

    Whole-buffer big-integer XOR: two allocations regardless of length, no
    per-byte Python loop. The key is tiled to the payload length, then both
    sides become one big int and are XOR'd in C.
    """
    n = len(data)
    if n == 0:
        return b""
    tiled = (key * (n // 4 + 1))[:n]
    return (int.from_bytes(data, "big") ^ int.from_bytes(tiled, "big")).to_bytes(n, "big")


def _new_mask() -> bytes:
    return os.urandom(4)


# -- client frame encoders --------------------------------------------------


def _frame_header(opcode: int, length: int, mask: bytes) -> bytes:
    b0 = _FIN_BIT | opcode
    if length <= 125:
        return bytes((b0, _MASK_BIT | length)) + mask
    if length < 65536:
        return bytes((b0, _MASK_BIT | 126)) + length.to_bytes(2, "big") + mask
    return bytes((b0, _MASK_BIT | 127)) + length.to_bytes(8, "big") + mask


def _encode(opcode: int, payload: bytes, mask: bytes | None) -> bytes:
    key = mask if mask is not None else _new_mask()
    return _frame_header(opcode, len(payload), key) + _apply_mask(payload, key)


def encode_binary_frame(payload: bytes, *, mask: bytes | None = None) -> bytes:
    """One masked, FIN binary frame carrying ``payload`` (opaque NATS bytes).

    Coalescing is the caller's job: pass already-joined bytes to get exactly one
    frame — this never splits. ``mask`` is injectable for deterministic tests;
    production always draws a fresh random key.
    """
    return _encode(_OP_BINARY, payload, mask)


def encode_ping(payload: bytes = b"", *, mask: bytes | None = None) -> bytes:
    return _encode(_OP_PING, payload, mask)


def encode_pong(payload: bytes = b"", *, mask: bytes | None = None) -> bytes:
    return _encode(_OP_PONG, payload, mask)


def encode_close(code: int = _CLOSE_NORMAL, reason: bytes = b"", *, mask: bytes | None = None) -> bytes:
    body = code.to_bytes(2, "big") + reason if code else b""
    return _encode(_OP_CLOSE, body, mask)


# -- handshake --------------------------------------------------------------


def _accept_key(challenge: bytes) -> str:
    """The expected ``Sec-WebSocket-Accept`` for a base64 challenge key."""
    return base64.b64encode(hashlib.sha1(challenge + _GUID).digest()).decode("ascii")


class WSHandshake:
    """Client opening handshake: request builder + 101-response pull-parser."""

    __slots__ = ("_buf", "_expected_accept", "request")

    def __init__(self, host: str, path: str, *, key: bytes | None = None) -> None:
        challenge = key if key is not None else base64.b64encode(os.urandom(16))
        self._expected_accept = _accept_key(challenge)
        # NO Sec-WebSocket-Extensions header: permessage-deflate is declined.
        self.request: bytes = (
            "\r\n".join(
                (
                    f"GET {path} HTTP/1.1",
                    f"Host: {host}",
                    "Upgrade: websocket",
                    "Connection: Upgrade",
                    f"Sec-WebSocket-Key: {challenge.decode('ascii')}",
                    "Sec-WebSocket-Version: 13",
                    "",
                    "",
                )
            )
        ).encode("ascii")
        self._buf = bytearray()

    def receive_data(self, data: bytes | bytearray | memoryview) -> None:
        if data:
            self._buf.extend(data)

    def next_event(self) -> _HandshakeOutput:
        idx = self._buf.find(b"\r\n\r\n")
        if idx < 0:
            if len(self._buf) > _MAX_HANDSHAKE_BYTES:
                raise WebsocketError("WebSocket handshake response headers too large")
            return WS_NEED_DATA
        head = bytes(self._buf[:idx])
        leftover = bytes(self._buf[idx + 4 :])
        self._validate(head)
        return WSHandshakeAccepted(leftover)

    def _validate(self, head: bytes) -> None:
        lines = head.split(b"\r\n")
        status = lines[0].split(b" ", 2)
        if len(status) < 2 or status[1] != b"101":
            raise WebsocketError(f"WebSocket handshake rejected: {lines[0][:128]!r}")
        headers: dict[str, str] = {}
        for raw in lines[1:]:
            name, sep, value = raw.partition(b":")
            if not sep:
                continue
            # Duplicate header names: last one wins (adequate for the fields we check).
            headers[name.strip().lower().decode("latin-1")] = value.strip().decode("latin-1")
        if headers.get("upgrade", "").lower() != "websocket":
            raise WebsocketError("WebSocket handshake missing 'Upgrade: websocket'")
        if "upgrade" not in headers.get("connection", "").lower():
            raise WebsocketError("WebSocket handshake missing 'Connection: Upgrade'")
        if headers.get("sec-websocket-accept") != self._expected_accept:
            raise WebsocketError("WebSocket handshake Sec-WebSocket-Accept mismatch")


# -- frame decoder ----------------------------------------------------------


class WSFrameDecoder:
    """Pull-decoder for server frames. Chunk-boundary safe: a frame is consumed
    only once fully buffered, so the event sequence is invariant under any split
    of the byte stream. Server frames are unmasked (a masked one is a violation)."""

    __slots__ = ("_buf", "_error", "_expect_cont", "_start")

    def __init__(self) -> None:
        self._buf = bytearray()
        self._start = 0
        # True while a fragmented data message is in progress (the previous data
        # frame had FIN=0), so the next data frame MUST be a continuation.
        self._expect_cont = False
        self._error: WebsocketError | None = None

    def receive_data(self, data: bytes | bytearray | memoryview) -> None:
        if self._error is not None:
            raise self._error
        if data:
            self._buf.extend(data)

    def next_event(self) -> _FrameOutput:
        if self._error is not None:
            raise self._error
        try:
            return self._decode()
        except WebsocketError as exc:
            self._error = exc
            raise

    def _decode(self) -> _FrameOutput:
        buf = self._buf
        start = self._start
        avail = len(buf) - start
        if avail < 2:
            return WS_NEED_DATA

        b0 = buf[start]
        b1 = buf[start + 1]
        fin = bool(b0 & _FIN_BIT)
        if b0 & _RSV_BITS:
            raise WebsocketError("reserved bit set in WebSocket frame (no extension negotiated)")
        opcode = b0 & 0x0F
        if b1 & _MASK_BIT:
            raise WebsocketError("server WebSocket frame must not be masked")

        is_control = opcode >= _OP_CLOSE
        len7 = b1 & _LEN_MASK
        if is_control:
            if len7 > _MAX_CONTROL_PAYLOAD:
                raise WebsocketError("control frame payload exceeds 125 bytes")
            if not fin:
                raise WebsocketError("fragmented control frame")

        if len7 <= 125:
            length = len7
            hlen = 2
        elif len7 == 126:
            if avail < 4:
                return WS_NEED_DATA
            length = int.from_bytes(buf[start + 2 : start + 4], "big")
            hlen = 4
        else:  # 127
            if avail < 10:
                return WS_NEED_DATA
            length = int.from_bytes(buf[start + 2 : start + 10], "big")
            if length & (1 << 63):
                raise WebsocketError("invalid 64-bit length: most-significant bit set")
            hlen = 10
        if length > MAX_FRAME_SIZE:
            # A hostile server declaring a huge frame must be a typed teardown,
            # not unbounded buffering (the NATS parser's max-payload guard sits
            # downstream and never sees the bytes). nats.go caps at 64MB too.
            raise WebsocketError(f"websocket frame too large: {length} > {MAX_FRAME_SIZE}")

        total = hlen + length
        if avail < total:
            return WS_NEED_DATA

        payload = bytes(buf[start + hlen : start + total])
        self._start = start + total
        self._compact()
        return self._dispatch(opcode, fin, payload)

    def _dispatch(self, opcode: int, fin: bool, payload: bytes) -> WSFrameEvent:
        if opcode == _OP_CONT:
            if not self._expect_cont:
                raise WebsocketError("continuation frame with no message to continue")
            self._expect_cont = not fin
            return WSData(payload)
        if opcode in (_OP_TEXT, _OP_BINARY):
            if self._expect_cont:
                raise WebsocketError("new data frame started before previous message completed")
            self._expect_cont = not fin
            return WSData(payload)
        if opcode == _OP_CLOSE:
            return _parse_close(payload)
        if opcode == _OP_PING:
            return WSPing(payload)
        if opcode == _OP_PONG:
            return WSPong(payload)
        raise WebsocketError(f"unknown WebSocket opcode {opcode:#x}")

    def _compact(self) -> None:
        if self._start >= _COMPACT_THRESHOLD:
            del self._buf[: self._start]
            self._start = 0


def _parse_close(payload: bytes) -> WSClose:
    if len(payload) < 2:
        return WSClose(_CLOSE_NO_STATUS, "")
    code = int.from_bytes(payload[:2], "big")
    reason = payload[2:].decode("utf-8", "replace")
    return WSClose(code, reason)
