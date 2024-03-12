from dataclasses import dataclass
from typing import Final, Optional

from natsio.const import CRLF

from .base import BaseProtocolClientMessage

PUB_OP: Final[bytes] = b"PUB"


@dataclass
class Pub(BaseProtocolClientMessage):
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
