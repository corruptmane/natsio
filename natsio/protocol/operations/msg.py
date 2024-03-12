from dataclasses import dataclass
from typing import Final, Optional

from .base import BaseProtocolServerMessage

MSG_OP: Final[bytes] = b"MSG"


@dataclass
class Msg(BaseProtocolServerMessage):
    subject: str
    sid: str
    payload_size: int
    reply_to: Optional[str] = None
    payload: Optional[bytes] = None


__all__ = (
    "MSG_OP",
    "Msg",
)
