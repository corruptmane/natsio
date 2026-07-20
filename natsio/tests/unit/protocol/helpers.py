"""Test-side encoders of *server* frames and parser-driving utilities."""

from collections.abc import Iterable, Sequence

from natsio._internal.protocol import NEED_DATA, Parser, ServerEvent


def msg_frame(subject: str, sid: int, payload: bytes, reply: str | None = None) -> bytes:
    reply_part = f" {reply}" if reply is not None else ""
    return f"MSG {subject} {sid}{reply_part} {len(payload)}\r\n".encode() + payload + b"\r\n"


def hmsg_frame(
    subject: str,
    sid: int,
    header_block: bytes,
    payload: bytes,
    reply: str | None = None,
) -> bytes:
    reply_part = f" {reply}" if reply is not None else ""
    head = (f"HMSG {subject} {sid}{reply_part} {len(header_block)} {len(header_block) + len(payload)}\r\n").encode()
    return head + header_block + payload + b"\r\n"


def header_block(*lines: str, status: str | None = None) -> bytes:
    version = f"NATS/1.0 {status}" if status else "NATS/1.0"
    return "\r\n".join([version, *lines, "", ""]).encode()


def info_frame(raw_json: bytes) -> bytes:
    return b"INFO " + raw_json + b"\r\n"


def err_frame(message: str) -> bytes:
    return f"-ERR '{message}'\r\n".encode()


def drain_events(parser: Parser) -> list[ServerEvent]:
    """Pull every currently-complete event out of the parser."""
    events: list[ServerEvent] = []
    while (event := parser.next_event()) is not NEED_DATA:
        events.append(event)
    return events


def parse_whole(stream: bytes) -> list[ServerEvent]:
    parser = Parser()
    parser.receive_data(stream)
    return drain_events(parser)


def parse_chunked(stream: bytes, boundaries: Sequence[int]) -> list[ServerEvent]:
    """Parse ``stream`` fed in pieces split at ``boundaries``, draining between feeds."""
    parser = Parser()
    events: list[ServerEvent] = []
    previous = 0
    for boundary in [*boundaries, len(stream)]:
        parser.receive_data(stream[previous:boundary])
        events.extend(drain_events(parser))
        previous = boundary
    return events


def concat(frames: Iterable[bytes]) -> bytes:
    return b"".join(frames)
