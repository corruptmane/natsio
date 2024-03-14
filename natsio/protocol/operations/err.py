from dataclasses import dataclass
from typing import Final

from natsio.abc.protocol import ServerMessageProto

ERR_OP: Final[bytes] = b"-ERR"


@dataclass
class Err(ServerMessageProto):
    message: str


__all__ = (
    "ERR_OP",
    "Err",
)
