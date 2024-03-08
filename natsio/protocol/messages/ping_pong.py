from dataclasses import dataclass
from .base import BaseProtocolClientMessage, BaseProtocolServerMessage


@dataclass(slots=True)
class Ping(BaseProtocolClientMessage, BaseProtocolServerMessage):
    @classmethod
    def from_bytes(cls, data: bytes) -> 'Ping':
        return cls()

    def build(self) -> bytes:
        return 'PING\r\n'.encode()


@dataclass(slots=True)
class Pong(BaseProtocolClientMessage, BaseProtocolServerMessage):
    @classmethod
    def from_bytes(cls, data: bytes) -> 'Pong':
        return cls()

    def build(self) -> bytes:
        return 'PONG\r\n'.encode()


__all__ = (
    "Ping", "Pong",
)
