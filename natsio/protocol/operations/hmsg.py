from dataclasses import dataclass
from functools import lru_cache
from typing import Final, Mapping, Optional

from natsio.abc.protocol import ServerMessageProto

HMSG_OP: Final[bytes] = b"HMSG"


@dataclass(eq=False)
class HMsg(ServerMessageProto):
    subject: str
    sid: str
    headers_size: int
    total_size: int
    reply_to: Optional[str] = None
    headers: Optional[Mapping[str, str]] = None
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
    "HMSG_OP",
    "HMsg",
)
