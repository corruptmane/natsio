from dataclasses import dataclass
from typing import Final, Optional

from natsio.const import CRLF

from .base import BaseProtocolClientMessage

UNSUB_OP: Final[bytes] = b"UNSUB"


@dataclass
class Unsub(BaseProtocolClientMessage):
    sid: str
    max_msgs: Optional[int] = None

    def build(self) -> bytes:
        msg = f"SUB {self.sid}"
        if self.max_msgs:
            msg += f" {self.max_msgs}"
        return msg.encode() + CRLF


__all__ = (
    "UNSUB_OP", "Unsub",
)
