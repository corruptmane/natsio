from dataclasses import dataclass
from typing import Final, MutableSequence, Optional

from natsio.abc.protocol import ClientMessageProto
from natsio.const import CRLF

PUB_OP: Final[bytes] = b"PUB"


@dataclass
class Pub(ClientMessageProto):
    subject: str
    reply_to: Optional[str] = None
    payload: Optional[bytes] = None

    def _build_payload(self) -> bytes:
        parts: MutableSequence[bytes] = [self.subject.encode()]
        if self.reply_to:
            parts.append(self.reply_to.encode())

        payload_size = 0 if not self.payload else len(self.payload)
        parts.append(str(payload_size).encode())
        return b" ".join(parts) + CRLF + (b"" if not self.payload else self.payload)

    def build(self) -> bytes:
        return PUB_OP + b" " + self._build_payload() + CRLF


__all__ = (
    "PUB_OP",
    "Pub",
)
