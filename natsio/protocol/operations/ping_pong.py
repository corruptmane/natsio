from dataclasses import dataclass
from typing import Final
from .base import BaseProtocolClientMessage, BaseProtocolServerMessage

PING_OP: Final[bytes] = b"PING"
PONG_OP: Final[bytes] = b"PONG"


@dataclass
class Ping(BaseProtocolClientMessage, BaseProtocolServerMessage):
    @classmethod
    def from_bytes(cls, data: bytes) -> 'Ping':
        return cls()

    def build(self) -> bytes:
        return 'PING\r\n'.encode()


@dataclass
class Pong(BaseProtocolClientMessage, BaseProtocolServerMessage):
    @classmethod
    def from_bytes(cls, data: bytes) -> 'Pong':
        return cls()

    def build(self) -> bytes:
        return 'PONG\r\n'.encode()


__all__ = (
    "PING_OP", "Ping",
    "PONG_OP", "Pong",
)
