from dataclasses import dataclass
from typing import Final, MutableSequence, Optional

from natsio.abc.protocol import ClientMessageProto
from natsio.const import CRLF

SUB_OP: Final[bytes] = b"SUB"


@dataclass
class Sub(ClientMessageProto):
    subject: str
    sid: str
    queue: Optional[str] = None

    def _build_payload(self) -> bytes:
        parts: MutableSequence[bytes] = [self.subject.encode()]
        if self.queue is not None:
            parts.append(self.queue.encode())
        parts.append(self.sid.encode())
        return b" ".join(parts)

    def build(self) -> bytes:
        return SUB_OP + b" " + self._build_payload() + CRLF


__all__ = (
    "SUB_OP",
    "Sub",
)
