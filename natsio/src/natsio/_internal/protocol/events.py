"""Typed events emitted by the sans-io protocol parser.

One event corresponds to one complete server operation. Events are plain data;
nothing here knows about sockets or asyncio. The two hot-path deliveries
(``MsgEvent``/``HMsgEvent``) are not frozen — they are treated as read-only, but
skip the immutability enforcement to keep per-message construction cheap.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Final, Literal

from .headers import Headers, InlineStatus


class NeedData(Enum):
    """Sentinel returned by ``Parser.next_event()`` when more bytes are required."""

    NEED_DATA = "NEED_DATA"

    def __repr__(self) -> str:
        return "NEED_DATA"


NEED_DATA: Final = NeedData.NEED_DATA


@dataclass(slots=True)
class MsgEvent:
    subject: str
    sid: int
    reply_to: str | None
    payload: bytes


@dataclass(slots=True)
class HMsgEvent:
    subject: str
    sid: int
    reply_to: str | None
    headers: Headers | None
    status: InlineStatus | None
    payload: bytes
    # Set when the length-delimited header block was corrupt: the message is
    # still delivered (framing was never at risk), the reason is surfaced.
    headers_error: str | None = None


@dataclass(frozen=True, slots=True)
class InfoEvent:
    """Raw INFO JSON payload; deserialization is the connection layer's job."""

    raw: bytes


@dataclass(frozen=True, slots=True)
class ErrEvent:
    """Server ``-ERR`` with the surrounding single quotes stripped."""

    message: str


@dataclass(frozen=True, slots=True)
class PingEvent:
    pass


@dataclass(frozen=True, slots=True)
class PongEvent:
    pass


@dataclass(frozen=True, slots=True)
class OkEvent:
    pass


PING_EVENT: Final = PingEvent()
PONG_EVENT: Final = PongEvent()
OK_EVENT: Final = OkEvent()

type ServerEvent = MsgEvent | HMsgEvent | InfoEvent | ErrEvent | PingEvent | PongEvent | OkEvent
type ParserOutput = ServerEvent | Literal[NeedData.NEED_DATA]
