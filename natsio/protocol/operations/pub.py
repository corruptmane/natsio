from dataclasses import dataclass
from typing import Final, Optional

from .base import BaseProtocolClientMessage

PUB_OP: Final[bytes] = b"PUB"


@dataclass
class Pub(BaseProtocolClientMessage):
    subject: str
    # payload_size: int
    reply_to: Optional[str] = None
    payload: Optional[bytes] = None

    def build(self) -> bytes:
        msg = f"PUB {{}}\r\n"
        return msg.encode()


__all__ = (
    "PUB_OP", "Pub",
)
