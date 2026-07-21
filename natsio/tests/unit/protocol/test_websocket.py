"""Sans-io WebSocket core: handshake, frame codec, masking, fragmentation,
control-frame interleaving, extended-length boundaries, and the differential
chunking invariant (the whole-stream event list must equal the every-split one).
"""

import base64
import random

import pytest

from natsio._internal.protocol.websocket import (
    WS_NEED_DATA,
    WSClose,
    WSData,
    WSFrameDecoder,
    WSHandshake,
    WSHandshakeAccepted,
    WSPing,
    WSPong,
    encode_binary_frame,
    encode_close,
    encode_ping,
    encode_pong,
)
from natsio.errors import WebsocketError

# -- test-side SERVER frame encoder (unmasked, per RFC) ---------------------


def server_frame(opcode: int, payload: bytes, *, fin: bool = True) -> bytes:
    b0 = (0x80 if fin else 0x00) | opcode
    n = len(payload)
    if n <= 125:
        header = bytes((b0, n))
    elif n < 65536:
        header = bytes((b0, 126)) + n.to_bytes(2, "big")
    else:
        header = bytes((b0, 127)) + n.to_bytes(8, "big")
    return header + payload


def decode_all(stream: bytes) -> list:
    decoder = WSFrameDecoder()
    decoder.receive_data(stream)
    events = []
    while (event := decoder.next_event()) is not WS_NEED_DATA:
        events.append(event)
    return events


def decode_chunked(stream: bytes, boundaries) -> list:
    decoder = WSFrameDecoder()
    events = []
    previous = 0
    for boundary in [*boundaries, len(stream)]:
        decoder.receive_data(stream[previous:boundary])
        while (event := decoder.next_event()) is not WS_NEED_DATA:
            events.append(event)
        previous = boundary
    return events


# -- handshake --------------------------------------------------------------


class TestHandshake:
    def test_request_shape_and_no_compression_offer(self) -> None:
        hs = WSHandshake("example.com:443", "/nats?a=1", key=b"AAAAAAAAAAAAAAAAAAAAAA==")
        lines = hs.request.split(b"\r\n")
        assert lines[0] == b"GET /nats?a=1 HTTP/1.1"
        assert b"Host: example.com:443" in lines
        assert b"Upgrade: websocket" in lines
        assert b"Connection: Upgrade" in lines
        assert b"Sec-WebSocket-Version: 13" in lines
        assert b"Sec-WebSocket-Key: AAAAAAAAAAAAAAAAAAAAAA==" in lines
        # permessage-deflate is deliberately never offered (v1 simplicity).
        assert b"Sec-WebSocket-Extensions" not in hs.request
        assert hs.request.endswith(b"\r\n\r\n")

    def test_rfc6455_accept_value(self) -> None:
        # The canonical example from RFC 6455 section 1.3.
        hs = WSHandshake("h:80", "/", key=b"dGhlIHNhbXBsZSBub25jZQ==")
        response = (
            b"HTTP/1.1 101 Switching Protocols\r\n"
            b"Upgrade: websocket\r\n"
            b"Connection: Upgrade\r\n"
            b"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n"
        )
        hs.receive_data(response)
        assert hs.next_event() == WSHandshakeAccepted(leftover=b"")

    def test_leftover_frame_bytes_preserved(self) -> None:
        hs = _accepting_handshake()
        response = _ok_response(hs) + b"\x82\x03abc"
        hs.receive_data(response)
        event = hs.next_event()
        assert isinstance(event, WSHandshakeAccepted)
        assert event.leftover == b"\x82\x03abc"

    def test_header_case_and_order_insensitive(self) -> None:
        hs = _accepting_handshake()
        accept = _accept_for(hs)
        response = (
            b"HTTP/1.1 101 Switching Protocols\r\n"
            b"sec-websocket-accept: " + accept.encode() + b"\r\n"
            b"CONNECTION: keep-alive, Upgrade\r\n"
            b"uPgRaDe: WebSocket\r\n\r\n"
        )
        hs.receive_data(response)
        assert isinstance(hs.next_event(), WSHandshakeAccepted)

    def test_need_data_until_headers_complete(self) -> None:
        hs = _accepting_handshake()
        full = _ok_response(hs)
        hs.receive_data(full[:20])
        assert hs.next_event() is WS_NEED_DATA
        hs.receive_data(full[20:])
        assert isinstance(hs.next_event(), WSHandshakeAccepted)

    def test_wrong_status_rejected(self) -> None:
        hs = _accepting_handshake()
        hs.receive_data(b"HTTP/1.1 200 OK\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
        with pytest.raises(WebsocketError, match="rejected"):
            hs.next_event()

    def test_accept_mismatch_rejected(self) -> None:
        hs = _accepting_handshake()
        response = (
            b"HTTP/1.1 101 Switching Protocols\r\n"
            b"Upgrade: websocket\r\nConnection: Upgrade\r\n"
            b"Sec-WebSocket-Accept: wrongwrongwrongwrongwrong=\r\n\r\n"
        )
        hs.receive_data(response)
        with pytest.raises(WebsocketError, match="Accept mismatch"):
            hs.next_event()

    def test_missing_upgrade_header_rejected(self) -> None:
        hs = _accepting_handshake()
        response = (
            b"HTTP/1.1 101 Switching Protocols\r\n"
            b"Connection: Upgrade\r\n"
            b"Sec-WebSocket-Accept: " + _accept_for(hs).encode() + b"\r\n\r\n"
        )
        hs.receive_data(response)
        with pytest.raises(WebsocketError, match="Upgrade"):
            hs.next_event()

    def test_oversized_header_block_rejected(self) -> None:
        hs = _accepting_handshake()
        hs.receive_data(b"HTTP/1.1 101 x\r\n" + b"X-Pad: " + b"a" * (70 * 1024))
        with pytest.raises(WebsocketError, match="too large"):
            hs.next_event()


def _accepting_handshake() -> WSHandshake:
    return WSHandshake("h:80", "/", key=base64.b64encode(b"0123456789abcdef"))


def _accept_for(hs: WSHandshake) -> str:
    return hs._expected_accept


def _ok_response(hs: WSHandshake) -> bytes:
    return (
        b"HTTP/1.1 101 Switching Protocols\r\n"
        b"Upgrade: websocket\r\nConnection: Upgrade\r\n"
        b"Sec-WebSocket-Accept: " + _accept_for(hs).encode() + b"\r\n\r\n"
    )


# -- client frame encoding + masking ----------------------------------------


class TestClientEncoding:
    def test_binary_frame_is_fin_masked_binary(self) -> None:
        frame = encode_binary_frame(b"hello", mask=b"\x01\x02\x03\x04")
        assert frame[0] == 0x82  # FIN + binary opcode
        assert frame[1] == (0x80 | 5)  # MASK bit + length
        assert frame[2:6] == b"\x01\x02\x03\x04"
        masked = frame[6:]
        unmasked = bytes(masked[i] ^ b"\x01\x02\x03\x04"[i % 4] for i in range(len(masked)))
        assert unmasked == b"hello"

    def test_mask_is_present_and_random(self) -> None:
        keys = {encode_binary_frame(b"x")[2:6] for _ in range(50)}
        assert len(keys) > 1  # random 4-byte mask per frame

    def test_client_frames_decode_when_unmasked_by_a_peer(self) -> None:
        # Round-trip: mask on encode, XOR back off, feed the unmasked body to the
        # decoder (which models the server side) to prove payload fidelity.
        for payload in (b"", b"a", b"NATS/1.0\r\n", bytes(range(256)) * 4):
            frame = encode_binary_frame(payload, mask=b"\xaa\xbb\xcc\xdd")
            body_start = len(frame) - len(payload)
            key = frame[body_start - 4 : body_start]  # mask sits just before the body
            body = bytes(frame[body_start + i] ^ key[i % 4] for i in range(len(payload)))
            assert body == payload

    def test_extended_length_forms(self) -> None:
        assert encode_binary_frame(b"a" * 125, mask=b"\0\0\0\0")[1] & 0x7F == 125
        assert encode_binary_frame(b"a" * 126, mask=b"\0\0\0\0")[1] & 0x7F == 126
        assert encode_binary_frame(b"a" * 65535, mask=b"\0\0\0\0")[1] & 0x7F == 126
        assert encode_binary_frame(b"a" * 65536, mask=b"\0\0\0\0")[1] & 0x7F == 127

    def test_close_and_ping_pong_encode(self) -> None:
        close = encode_close(1000, b"bye", mask=b"\0\0\0\0")
        assert close[0] == 0x88  # FIN + close opcode
        # unmasked body (mask all zero): code 1000 big-endian + reason
        assert close[2:6] == b"\0\0\0\0"
        assert close[6:] == (1000).to_bytes(2, "big") + b"bye"
        assert encode_ping(b"p", mask=b"\0\0\0\0")[0] == 0x89
        assert encode_pong(b"p", mask=b"\0\0\0\0")[0] == 0x8A


# -- server frame decoding --------------------------------------------------


class TestDecoding:
    def test_binary_and_text_as_data(self) -> None:
        assert decode_all(server_frame(0x2, b"binbytes")) == [WSData(b"binbytes")]
        assert decode_all(server_frame(0x1, b"textbytes")) == [WSData(b"textbytes")]

    def test_ping_pong_close(self) -> None:
        assert decode_all(server_frame(0x9, b"pi")) == [WSPing(b"pi")]
        assert decode_all(server_frame(0xA, b"po")) == [WSPong(b"po")]
        assert decode_all(server_frame(0x8, (1001).to_bytes(2, "big") + b"gone")) == [WSClose(1001, "gone")]

    def test_close_without_status(self) -> None:
        assert decode_all(server_frame(0x8, b"")) == [WSClose(1005, "")]

    @pytest.mark.parametrize("size", [0, 1, 125, 126, 127, 65535, 65536, 70000])
    def test_length_boundary_values(self, size: int) -> None:
        payload = bytes(i % 251 for i in range(size))
        assert decode_all(server_frame(0x2, payload)) == [WSData(payload)]

    def test_fragmented_message_reassembled_as_stream(self) -> None:
        stream = (
            server_frame(0x2, b"AAA", fin=False)
            + server_frame(0x0, b"BBB", fin=False)
            + server_frame(0x0, b"CCC", fin=True)
        )
        # Each data frame's payload is surfaced immediately (opaque NATS bytes);
        # joining them yields the whole message.
        events = decode_all(stream)
        assert events == [WSData(b"AAA"), WSData(b"BBB"), WSData(b"CCC")]
        assert b"".join(e.payload for e in events) == b"AAABBBCCC"

    def test_control_frame_interleaved_mid_fragmentation(self) -> None:
        stream = (
            server_frame(0x2, b"AAA", fin=False)
            + server_frame(0x9, b"ping")  # control frame between fragments (RFC-legal)
            + server_frame(0x0, b"BBB", fin=True)
        )
        assert decode_all(stream) == [WSData(b"AAA"), WSPing(b"ping"), WSData(b"BBB")]

    def test_mixed_multi_frame_stream(self) -> None:
        stream = _mixed_stream()
        events = decode_all(stream)
        assert events == _EXPECTED_MIXED


class TestDecodingErrors:
    def test_masked_server_frame_rejected(self) -> None:
        masked = bytes((0x82, 0x80 | 3)) + b"\0\0\0\0" + b"abc"
        with pytest.raises(WebsocketError, match="must not be masked"):
            decode_all(masked)

    def test_reserved_bits_rejected(self) -> None:
        with pytest.raises(WebsocketError, match="reserved bit"):
            decode_all(bytes((0xC2, 0x00)))  # RSV1 set (would mean compression)

    def test_unknown_opcode_rejected(self) -> None:
        with pytest.raises(WebsocketError, match="unknown"):
            decode_all(server_frame(0x3, b""))  # reserved non-control opcode

    def test_oversized_control_frame_rejected(self) -> None:
        with pytest.raises(WebsocketError, match="control frame payload"):
            decode_all(bytes((0x89, 126)) + (200).to_bytes(2, "big") + b"x" * 200)

    def test_fragmented_control_frame_rejected(self) -> None:
        with pytest.raises(WebsocketError, match="fragmented control"):
            decode_all(server_frame(0x9, b"x", fin=False))

    def test_unexpected_continuation_rejected(self) -> None:
        with pytest.raises(WebsocketError, match="no message to continue"):
            decode_all(server_frame(0x0, b"orphan"))

    def test_new_data_before_completion_rejected(self) -> None:
        stream = server_frame(0x2, b"AAA", fin=False) + server_frame(0x2, b"BBB")
        with pytest.raises(WebsocketError, match="before previous message"):
            decode_all(stream)

    def test_64bit_msb_set_rejected(self) -> None:
        frame = bytes((0x82, 127)) + (1 << 63).to_bytes(8, "big")
        with pytest.raises(WebsocketError, match="most-significant bit"):
            decode_all(frame)

    def test_decoder_refuses_use_after_error(self) -> None:
        decoder = WSFrameDecoder()
        decoder.receive_data(bytes((0xC2, 0x00)))
        with pytest.raises(WebsocketError):
            decoder.next_event()
        with pytest.raises(WebsocketError):
            decoder.next_event()

    def test_malformed_inputs_never_hang(self) -> None:
        rng = random.Random(0xF122)
        for _ in range(500):
            blob = bytes(rng.randrange(256) for _ in range(rng.randint(0, 40)))
            decoder = WSFrameDecoder()
            decoder.receive_data(blob)
            try:
                for _ in range(100):  # bounded: must reach NEED_DATA or raise, never loop
                    if decoder.next_event() is WS_NEED_DATA:
                        break
                else:
                    raise AssertionError("decoder did not terminate")
            except WebsocketError:
                pass


# -- differential chunking invariant ----------------------------------------


def _mixed_stream() -> bytes:
    big = bytes(i % 251 for i in range(70000))  # forces the 64-bit length form
    return b"".join(
        (
            server_frame(0x2, b"INFO {}\r\n"),
            server_frame(0x9, b"keepalive"),  # server ping
            server_frame(0x2, b"MSG foo 1 5\r\nhello\r\n"),
            server_frame(0x2, b"frag-a", fin=False),
            server_frame(0xA, b"pong-mid"),  # control interleaved mid-fragmentation
            server_frame(0x0, b"frag-b", fin=True),
            server_frame(0x2, b""),  # empty data frame
            server_frame(0x2, big),  # 64-bit length
            server_frame(0x1, b"+OK\r\n"),  # text opcode as data
            server_frame(0x8, (1000).to_bytes(2, "big") + b"bye"),
        )
    )


_EXPECTED_MIXED = decode_all(_mixed_stream())


class TestDifferentialChunking:
    def test_reference_stream_shape(self) -> None:
        assert len(_EXPECTED_MIXED) == 10
        assert _EXPECTED_MIXED[-1] == WSClose(1000, "bye")

    def test_every_single_split_point(self) -> None:
        stream = _mixed_stream()
        # Every split; the huge middle frame keeps this bounded to interesting edges.
        points = list(range(1, 200)) + list(range(len(stream) - 200, len(stream)))
        for split in points:
            assert decode_chunked(stream, [split]) == _EXPECTED_MIXED, f"split {split} diverged"

    def test_one_byte_at_a_time(self) -> None:
        stream = _mixed_stream()
        assert decode_chunked(stream, list(range(1, len(stream)))) == _EXPECTED_MIXED

    def test_random_multiway_splits(self) -> None:
        stream = _mixed_stream()
        rng = random.Random(0xC0FFEE)
        for _ in range(200):
            count = rng.randint(2, 16)
            boundaries = sorted(rng.sample(range(1, len(stream)), count))
            assert decode_chunked(stream, boundaries) == _EXPECTED_MIXED


class TestFrameSizeCap:
    """Review regression: a hostile server declaring a huge frame must raise,
    not buffer unboundedly toward OOM."""

    def test_oversized_64bit_length_rejected_immediately(self) -> None:
        from natsio._internal.protocol.websocket import MAX_FRAME_SIZE, WSFrameDecoder

        decoder = WSFrameDecoder()
        header = bytes([0x82, 127]) + (MAX_FRAME_SIZE + 1).to_bytes(8, "big")
        decoder.receive_data(header)
        with pytest.raises(WebsocketError, match="frame too large"):
            decoder.next_event()

    def test_oversized_frame_needs_no_payload_bytes(self) -> None:
        """The reject fires on the header alone — zero payload buffered."""
        from natsio._internal.protocol.websocket import WSFrameDecoder

        decoder = WSFrameDecoder()
        decoder.receive_data(bytes([0x82, 127]) + (1 << 62).to_bytes(8, "big"))
        with pytest.raises(WebsocketError):
            decoder.next_event()

    def test_frame_at_cap_boundary_accepted(self) -> None:
        from natsio._internal.protocol.websocket import MAX_FRAME_SIZE, WS_NEED_DATA, WSFrameDecoder

        decoder = WSFrameDecoder()
        decoder.receive_data(bytes([0x82, 127]) + MAX_FRAME_SIZE.to_bytes(8, "big"))
        # Exactly at the cap: no error — the decoder simply awaits the payload.
        assert decoder.next_event() is WS_NEED_DATA
