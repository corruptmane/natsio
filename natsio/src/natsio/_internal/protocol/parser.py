"""Sans-io NATS protocol parser.

An h11-style pull parser: :meth:`Parser.receive_data` appends raw bytes to an
internal buffer, :meth:`Parser.next_event` returns exactly one complete typed
event or :data:`NEED_DATA`. The parser never performs I/O and never awaits —
chunk-boundary handling is therefore a pure function of the buffer, testable
for every possible split of a byte stream.

Framing errors are fatal by design: a byte stream cannot be resynchronized
mid-frame, so the only correct recovery is tearing down the transport. After
a :class:`ParserError` the parser instance refuses further use.
"""

from enum import Enum

from natsio.errors import BadHeadersError, MaxControlLineExceededError, ParserError

from .const import (
    BUFFER_COMPACT_THRESHOLD,
    CRLF,
    DEFAULT_MAX_CONTROL_LINE,
    DEFAULT_MAX_PAYLOAD,
)
from .events import (
    NEED_DATA,
    OK_EVENT,
    PING_EVENT,
    PONG_EVENT,
    ErrEvent,
    HMsgEvent,
    InfoEvent,
    MsgEvent,
    ParserOutput,
)
from .headers import parse_header_block

__all__ = ["Parser"]


class _State(Enum):
    CONTROL = "CONTROL"
    PAYLOAD = "PAYLOAD"
    FAILED = "FAILED"


class Parser:
    __slots__ = (
        "_buf",
        "_error",
        "_max_control_line",
        "_max_payload",
        "_pending",
        "_scan",
        "_start",
        "_state",
    )

    def __init__(
        self,
        *,
        max_control_line: int = DEFAULT_MAX_CONTROL_LINE,
        max_payload: int = DEFAULT_MAX_PAYLOAD,
    ) -> None:
        self._buf = bytearray()
        self._start = 0  # offset of the first unconsumed byte
        self._scan = 0  # offset up to which we already searched for CRLF
        self._state = _State.CONTROL
        # (subject, sid, reply_to, header_size, total_size) of the frame
        # whose payload we are awaiting; header_size < 0 means plain MSG.
        self._pending: tuple[str, int, str | None, int, int] | None = None
        self._max_control_line = max_control_line
        self._max_payload = max_payload
        self._error: ParserError | None = None

    @property
    def buffered(self) -> int:
        """Number of unconsumed bytes currently held."""
        return len(self._buf) - self._start

    def set_max_payload(self, limit: int) -> None:
        """Adjust the payload ceiling (e.g. from the server's INFO)."""
        self._max_payload = limit

    def receive_data(self, data: bytes | bytearray | memoryview) -> None:
        if self._error is not None:
            raise self._error
        if data:
            self._buf.extend(data)

    def next_event(self) -> ParserOutput:
        if self._error is not None:
            raise self._error
        try:
            if self._state is _State.CONTROL:
                return self._next_from_control()
            return self._next_from_payload()
        except ParserError as exc:
            self._state = _State.FAILED
            self._error = exc
            raise

    # -- control lines ------------------------------------------------------

    def _next_from_control(self) -> ParserOutput:
        idx = self._buf.find(CRLF, self._scan)
        if idx < 0:
            pending = len(self._buf) - self._start
            if pending > self._max_control_line + len(CRLF):
                raise MaxControlLineExceededError(
                    f"no CRLF within {pending} bytes (max control line {self._max_control_line})"
                )
            # Re-scan the final byte next time: it may be the CR of a split CRLF.
            self._scan = max(self._start, len(self._buf) - 1)
            self._compact()
            return NEED_DATA

        if idx - self._start > self._max_control_line:
            raise MaxControlLineExceededError(
                f"control line of {idx - self._start} bytes exceeds {self._max_control_line}"
            )

        line = bytes(memoryview(self._buf)[self._start : idx])
        self._start = idx + len(CRLF)
        self._scan = self._start
        event = self._dispatch_control(line)
        if event is NEED_DATA:  # MSG/HMSG: payload follows
            return self._next_from_payload()
        self._compact()
        return event

    def _dispatch_control(self, line: bytes) -> ParserOutput:
        # nats.go's OP_PING/OP_PONG/OP_PLUS_OK states accept any bytes between
        # the token and the terminating \n, so "PING  ", "PONG x", "+OKay" are
        # all valid. Match on the leading token, not exact equality.
        if line.startswith(b"PING"):
            return PING_EVENT
        if line.startswith(b"PONG"):
            return PONG_EVENT
        if line.startswith(b"+OK"):
            return OK_EVENT
        if line.startswith(b"MSG "):
            return self._parse_msg_args(line)
        if line.startswith(b"HMSG "):
            return self._parse_hmsg_args(line)
        if line.startswith(b"INFO "):
            return InfoEvent(raw=line[5:].strip())
        if line.startswith(b"-ERR"):
            # normalizeErr: trim surrounding whitespace, then strip any leading
            # and trailing single quotes independently (charset strip).
            message = line[4:].strip().strip(b"'")
            return ErrEvent(message=message.decode("utf-8", "replace"))
        # The protocol is case-insensitive in principle; real servers send
        # uppercase. Normalize ONLY the operation token — arguments (subjects,
        # INFO JSON, -ERR text) are case-significant and must pass through
        # untouched.
        op, sep, rest = line.partition(b" ")
        op_upper = op.upper()
        if op_upper != op:
            return self._dispatch_control(op_upper + sep + rest)
        raise ParserError(f"unknown protocol operation: {line[:32]!r}")

    def _parse_msg_args(self, line: bytes) -> ParserOutput:
        tokens = line.split()
        if len(tokens) == 4:
            _, subject_b, sid_b, size_b = tokens
            reply_b = None
        elif len(tokens) == 5:
            _, subject_b, sid_b, reply_b, size_b = tokens
        else:
            raise ParserError(f"malformed MSG control line: {line!r}")
        size = self._parse_size(size_b, line)
        self._pending = (
            self._decode_subject(subject_b, line),
            self._parse_sid(sid_b, line),
            self._decode_subject(reply_b, line) if reply_b is not None else None,
            -1,
            size,
        )
        self._state = _State.PAYLOAD
        return NEED_DATA

    def _parse_hmsg_args(self, line: bytes) -> ParserOutput:
        tokens = line.split()
        if len(tokens) == 5:
            _, subject_b, sid_b, hsize_b, tsize_b = tokens
            reply_b = None
        elif len(tokens) == 6:
            _, subject_b, sid_b, reply_b, hsize_b, tsize_b = tokens
        else:
            raise ParserError(f"malformed HMSG control line: {line!r}")
        header_size = self._parse_size(hsize_b, line)
        total_size = self._parse_size(tsize_b, line)
        if header_size > total_size:
            raise ParserError(f"HMSG header size exceeds total size: {line!r}")
        self._pending = (
            self._decode_subject(subject_b, line),
            self._parse_sid(sid_b, line),
            self._decode_subject(reply_b, line) if reply_b is not None else None,
            header_size,
            total_size,
        )
        self._state = _State.PAYLOAD
        return NEED_DATA

    # Generous bound on numeric tokens: a 20-digit run already exceeds any
    # legitimate size or sid, and bounding *before* int() keeps CPython's
    # integer-string conversion limit (ValueError past 4300 digits) from
    # escaping the ParserError contract on huge max_control_line configs.
    _MAX_NUMERIC_DIGITS = 20

    def _parse_size(self, token: bytes, line: bytes) -> int:
        if not token.isdigit() or len(token) > self._MAX_NUMERIC_DIGITS:
            raise ParserError(f"invalid size in control line: {line[:64]!r}")
        size = int(token)
        if size > self._max_payload:
            raise ParserError(f"announced payload of {size} bytes exceeds max {self._max_payload}")
        return size

    @classmethod
    def _parse_sid(cls, token: bytes, line: bytes) -> int:
        if not token.isdigit() or len(token) > cls._MAX_NUMERIC_DIGITS:
            raise ParserError(f"invalid sid in control line: {line[:64]!r}")
        return int(token)

    @staticmethod
    def _decode_subject(token: bytes, line: bytes) -> str:
        try:
            return token.decode("ascii")
        except UnicodeDecodeError:
            raise ParserError(f"non-ASCII subject in control line: {line!r}") from None

    # -- payloads -----------------------------------------------------------

    def _next_from_payload(self) -> ParserOutput:
        assert self._pending is not None
        subject, sid, reply_to, header_size, total_size = self._pending

        needed = total_size + len(CRLF)
        if len(self._buf) - self._start < needed:
            self._compact()
            return NEED_DATA

        start = self._start
        end = start + total_size
        if self._buf[end : end + len(CRLF)] != CRLF:
            raise ParserError(f"message payload for subject {subject!r} is not terminated by CRLF")

        with memoryview(self._buf) as view:
            if header_size < 0:
                event: MsgEvent | HMsgEvent = MsgEvent(
                    subject=subject,
                    sid=sid,
                    reply_to=reply_to,
                    payload=bytes(view[start:end]),
                )
            else:
                block = bytes(view[start : start + header_size])
                payload = bytes(view[start + header_size : end])
                try:
                    headers, status = parse_header_block(block)
                    headers_error = None
                except BadHeadersError as exc:
                    # The block is length-delimited, so a corrupt block is not a
                    # framing hazard — deliver the message, surface the reason.
                    headers, status, headers_error = None, None, str(exc)
                event = HMsgEvent(
                    subject=subject,
                    sid=sid,
                    reply_to=reply_to,
                    headers=headers,
                    status=status,
                    payload=payload,
                    headers_error=headers_error,
                )

        self._pending = None
        self._state = _State.CONTROL
        self._start = end + len(CRLF)
        self._scan = self._start
        self._compact()
        return event

    # -- buffer management --------------------------------------------------

    def _compact(self) -> None:
        if self._start >= BUFFER_COMPACT_THRESHOLD:
            del self._buf[: self._start]
            self._scan -= self._start
            self._start = 0
