from dataclasses import dataclass
from functools import lru_cache
from typing import Final, Optional

from natsio.abc.protocol import ServerMessageProto

MSG_OP: Final[bytes] = b"MSG"


@dataclass(eq=False)
class Msg(ServerMessageProto):
    subject: str
    sid: str
    payload_size: int
    reply_to: Optional[str] = None
    payload: Optional[bytes] = None

    @property
    @lru_cache
    def is_request_inbox(self) -> bool:
        return self.subject.startswith("_REQ_INBOX.")

    @property
    @lru_cache
    def inbox_id(self) -> str:
        return self.subject.split(".", maxsplit=1)[1]


__all__ = (
    "MSG_OP",
    "Msg",
)
