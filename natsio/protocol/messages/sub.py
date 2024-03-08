from dataclasses import dataclass
from typing import Optional

from .base import BaseProtocolClientMessage


@dataclass(slots=True)
class Sub(BaseProtocolClientMessage):
    subject: str
    sid: str
    queue: Optional[str] = None

    def build(self) -> bytes:
        msg = f"SUB {self.subject}"
        if self.queue:
            msg += f" {self.queue}"
        msg += f" {self.sid}\r\n"
        return msg.encode()


__all__ = (
    "Sub",
)
