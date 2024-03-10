from dataclasses import dataclass
from typing import Final, Optional

from .base import BaseProtocolClientMessage

HPUB_OP: Final[bytes] = b"HPUB"


@dataclass
class HPub(BaseProtocolClientMessage):
    subject: str
    # total_size: int
    # headers_size: int
    reply_to: Optional[str] = None
    headers: Optional[bytes] = None
    payload: Optional[bytes] = None

    def build(self) -> bytes:
        msg = f"HPUB {{}}\r\n"
        return msg.encode()


__all__ = (
    "HPUB_OP", "HPub",
)
