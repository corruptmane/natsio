from dataclasses import dataclass
from typing import Final, Optional

from natsio.abc.protocol import ClientMessageProto
from natsio.const import CRLF

PUB_OP: Final[bytes] = b"PUB"


@dataclass
class Pub(ClientMessageProto):
    subject: str
    reply_to: Optional[str] = None
    payload: Optional[bytes] = None

    def _build_payload(self) -> bytes:
        payload = self.subject.encode()
        if self.reply_to:
            payload += b" " + self.reply_to.encode()
        payload_size = 0 if not self.payload else len(self.payload)
        payload += b" " + str(payload_size).encode() + CRLF
        payload += self.payload if self.payload else b""
        return payload

    def build(self) -> bytes:
        return PUB_OP + b" " + self._build_payload() + CRLF


__all__ = (
    "PUB_OP",
    "Pub",
)
