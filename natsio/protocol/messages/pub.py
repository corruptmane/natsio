from dataclasses import dataclass
from typing import Optional

from .base import BaseProtocolClientMessage


@dataclass(slots=True)
class Pub(BaseProtocolClientMessage):
    subject: str
    # payload_size: int
    reply_to: Optional[str] = None
    payload: Optional[bytes] = None

    def build(self) -> bytes:
        msg = f"PUB {{}}\r\n"
        return msg.encode()


__all__ = (
    "Pub",
)
