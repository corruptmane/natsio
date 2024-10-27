from dataclasses import dataclass
from typing import Final, Mapping

from natsio.abc.protocol import ServerMessageProto

HMSG_OP: Final[bytes] = b"HMSG"


@dataclass
class HMsg(ServerMessageProto):
    subject: str
    sid: str
    headers_size: int
    total_size: int
    reply_to: str | None = None
    headers: Mapping[str, str] | None = None
    payload: bytes | None = None

    def is_request_inbox(self, inbox_prefix: str) -> bool:
        return self.subject.startswith(inbox_prefix)

    def inbox_id(self, inbox_prefix: str) -> str:
        return self.subject.lstrip(inbox_prefix + ".")


__all__ = (
    "HMSG_OP",
    "HMsg",
)
