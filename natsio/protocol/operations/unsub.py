from dataclasses import dataclass
from typing import Final, MutableSequence, Optional

from natsio.abc.protocol import ClientMessageProto
from natsio.const import CRLF

UNSUB_OP: Final[bytes] = b"UNSUB"


@dataclass
class Unsub(ClientMessageProto):
    sid: str
    max_msgs: Optional[int] = None

    def _build_payload(self) -> bytes:
        parts: MutableSequence[bytes] = [self.sid.encode()]
        if self.max_msgs is not None:
            parts.append(str(self.max_msgs).encode())
        return b" ".join(parts)

    def build(self) -> bytes:
        return UNSUB_OP + b" " + self._build_payload() + CRLF


__all__ = (
    "UNSUB_OP",
    "Unsub",
)
