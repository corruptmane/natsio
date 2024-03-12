from dataclasses import dataclass
from typing import Final

from natsio.const import CRLF

from .base import BaseProtocolClientMessage, BaseProtocolServerMessage

PING_OP: Final[bytes] = b"PING"
PONG_OP: Final[bytes] = b"PONG"


@dataclass
class Ping(BaseProtocolClientMessage, BaseProtocolServerMessage):
    def build(self) -> bytes:
        return PING_OP + CRLF


@dataclass
class Pong(BaseProtocolClientMessage, BaseProtocolServerMessage):
    def build(self) -> bytes:
        return PONG_OP + CRLF


PING = Ping().build()
PONG = Pong().build()


__all__ = (
    "PING_OP",
    "PONG_OP",
    "Ping",
    "Pong",
    "PING",
    "PONG",
)
