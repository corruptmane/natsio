from dataclasses import dataclass
from typing import Final, Optional

from natsio.const import CRLF

from .base import BaseProtocolClientMessage

UNSUB_OP: Final[bytes] = b"UNSUB"


@dataclass
class Unsub(BaseProtocolClientMessage):
    sid: str
    max_msgs: Optional[int] = None

    def _build_payload(self) -> bytes:
        payload = f"{self.sid}"
        if self.max_msgs is not None:
            payload += f" {self.max_msgs}"
        return payload.encode()

    def build(self) -> bytes:
        msg = f"SUB {self.sid}"
        if self.max_msgs:
            msg += f" {self.max_msgs}"
        return UNSUB_OP + b" " + self._build_payload() + CRLF


__all__ = (
    "UNSUB_OP",
    "Unsub",
)
