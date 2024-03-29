from dataclasses import dataclass
from typing import Final, Optional

from natsio.abc.protocol import ServerMessageProto

MSG_OP: Final[bytes] = b"MSG"


@dataclass
class Msg(ServerMessageProto):
    subject: str
    sid: str
    payload_size: int
    reply_to: Optional[str] = None
    payload: Optional[bytes] = None

    def is_request_inbox(self, inbox_prefix: str) -> bool:
        return self.subject.startswith(inbox_prefix)

    def inbox_id(self, inbox_prefix: str) -> str:
        return self.subject.rstrip(inbox_prefix + ".")


__all__ = (
    "MSG_OP",
    "Msg",
)
