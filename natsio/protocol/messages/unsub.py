from dataclasses import dataclass
from typing import Optional

from .base import BaseProtocolClientMessage


@dataclass(slots=True)
class Unsub(BaseProtocolClientMessage):
    sid: str
    max_msgs: Optional[int] = None

    def build(self) -> bytes:
        msg = f"SUB {self.sid}"
        if self.max_msgs:
            msg += f" {self.max_msgs}"
        msg += f"\r\n"
        return msg.encode()


__all__ = (
    "Unsub",
)
