from dataclasses import dataclass
from typing import Final, Mapping, Optional

from natsio.abc.protocol import ServerMessageProto

HMSG_OP: Final[bytes] = b"HMSG"


@dataclass
class HMsg(ServerMessageProto):
    subject: str
    sid: str
    headers_size: int
    total_size: int
    reply_to: Optional[str] = None
    headers: Optional[Mapping[str, str]] = None
    payload: Optional[bytes] = None

    def is_request_inbox(self, inbox_prefix: str) -> bool:
        return self.subject.startswith(inbox_prefix)

    def inbox_id(self, inbox_prefix: str) -> str:
        return self.subject.rstrip(inbox_prefix + ".")


__all__ = (
    "HMSG_OP",
    "HMsg",
)
