from dataclasses import dataclass
from typing import Optional

from .base import BaseProtocolServerMessage


@dataclass(slots=True)
class HMsg(BaseProtocolServerMessage):
    subject: str
    sid: str
    headers_size: int
    total_size: int
    reply_to: Optional[str] = None
    headers: Optional[bytes] = None
    payload: Optional[bytes] = None

    @classmethod
    def from_bytes(cls, data: bytes) -> 'HMsg':
        return cls()


__all__ = (
    "HMsg",
)
