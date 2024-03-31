from dataclasses import dataclass
from typing import Final, Mapping, MutableSequence, Optional

from natsio.abc.protocol import ClientMessageProto
from natsio.const import CRLF

HPUB_OP: Final[bytes] = b"HPUB"


@dataclass
class HPub(ClientMessageProto):
    subject: str
    headers: Mapping[str, str]
    reply_to: Optional[str] = None
    payload: Optional[bytes] = None

    def _build_headers(self) -> bytes:
        headers = b"NATS/1.0" + CRLF
        headers += CRLF.join(f"{k}: {v}".encode() for k, v in self.headers.items())
        headers += 2 * CRLF
        return headers

    def _build_payload(self) -> bytes:
        parts: MutableSequence[bytes] = [self.subject.encode()]
        if self.reply_to:
            parts.append(self.reply_to.encode())

        headers = self._build_headers()
        headers_size = len(headers)
        parts.append(str(headers_size).encode())

        total_size = headers_size + (0 if not self.payload else len(self.payload))
        parts.append(str(total_size).encode())

        return (
            b" ".join(parts)
            + CRLF
            + headers
            + (b"" if not self.payload else self.payload)
        )

    def build(self) -> bytes:
        return HPUB_OP + b" " + self._build_payload() + CRLF


__all__ = (
    "HPUB_OP",
    "HPub",
)
