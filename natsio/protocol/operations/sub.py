from dataclasses import dataclass
from typing import Final, Optional

from natsio.abc.protocol import ClientMessageProto
from natsio.const import CRLF

SUB_OP: Final[bytes] = b"SUB"


@dataclass
class Sub(ClientMessageProto):
    subject: str
    sid: str
    queue: Optional[str] = None

    def _build_payload(self) -> bytes:
        payload = f"{self.subject}"
        if self.queue is not None:
            payload += f" {self.queue}"
        payload += f" {self.sid}"
        return payload.encode()

    def build(self) -> bytes:
        return SUB_OP + b" " + self._build_payload() + CRLF


__all__ = (
    "SUB_OP",
    "Sub",
)
