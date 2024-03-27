from dataclasses import dataclass
from functools import cached_property
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

    @cached_property
    def is_request_inbox(self) -> bool:
        return self.subject.startswith("_REQ_INBOX.")

    @cached_property
    def inbox_id(self) -> str:
        return self.subject.split(".", maxsplit=1)[1]


__all__ = (
    "HMSG_OP",
    "HMsg",
)
