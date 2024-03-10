from dataclasses import dataclass
from typing import Final, Optional

from .base import BaseProtocolClientMessage

SUB_OP: Final[bytes] = b"SUB"


@dataclass
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
    "SUB_OP", "Sub",
)
