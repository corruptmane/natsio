from dataclasses import dataclass
from typing import Final, Mapping, Optional

from .base import BaseProtocolServerMessage

HMSG_OP: Final[bytes] = b"HMSG"


@dataclass
class HMsg(BaseProtocolServerMessage):
    subject: str
    sid: str
    headers_size: int
    total_size: int
    reply_to: Optional[str] = None
    headers: Optional[Mapping[str, str]] = None
    payload: Optional[bytes] = None


__all__ = (
    "HMSG_OP",
    "HMsg",
)
