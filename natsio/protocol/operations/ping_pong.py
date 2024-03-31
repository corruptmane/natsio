from dataclasses import dataclass
from typing import Final

from natsio.abc.protocol import ClientMessageProto, ServerMessageProto
from natsio.const import CRLF

PING_OP: Final[bytes] = b"PING"
PONG_OP: Final[bytes] = b"PONG"


@dataclass
class Ping(ClientMessageProto, ServerMessageProto):
    def build(self) -> bytes:
        return PING_OP + CRLF


@dataclass
class Pong(ClientMessageProto, ServerMessageProto):
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
