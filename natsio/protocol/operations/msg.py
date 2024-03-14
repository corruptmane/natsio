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


__all__ = (
    "MSG_OP",
    "Msg",
)
