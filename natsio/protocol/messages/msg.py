from dataclasses import dataclass
from typing import Optional

from .base import BaseProtocolServerMessage


@dataclass(slots=True)
class Msg(BaseProtocolServerMessage):
    subject: str
    sid: str
    payload_size: int
    reply_to: Optional[str] = None
    payload: Optional[bytes] = None

    @classmethod
    def from_bytes(cls, data: bytes) -> 'Msg':
        return cls()


__all__ = (
    "Msg",
)
