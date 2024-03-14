from dataclasses import dataclass
from typing import Final, Optional

from natsio.abc.protocol import ClientMessageProto
from natsio.const import CRLF

PUB_OP: Final[bytes] = b"PUB"


@dataclass
class Pub(ClientMessageProto):
    subject: str
    payload_size: int
    reply_to: Optional[str] = None
    payload: Optional[bytes] = None

    def _build_payload(self) -> bytes:
        # TODO
        return b""

    def build(self) -> bytes:
        # TODO
        return PUB_OP + b" " + self._build_payload() + CRLF


__all__ = (
    "PUB_OP",
    "Pub",
)
