from dataclasses import dataclass
from typing import Final, Optional

from natsio.abc.protocol import ClientMessageProto
from natsio.const import CRLF

HPUB_OP: Final[bytes] = b"HPUB"


@dataclass
class HPub(ClientMessageProto):
    subject: str
    total_size: int
    headers_size: int
    reply_to: Optional[str] = None
    headers: Optional[bytes] = None
    payload: Optional[bytes] = None

    def _build_payload(self) -> bytes:
        # TODO
        return b""

    def build(self) -> bytes:
        # TODO
        return HPUB_OP + b" " + self._build_payload() + CRLF


__all__ = (
    "HPUB_OP",
    "HPub",
)
