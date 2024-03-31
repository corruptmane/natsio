from dataclasses import dataclass
from typing import Final

from natsio.abc.protocol import ServerMessageProto

OK_OP: Final[bytes] = b"+OK"


@dataclass
class Ok(ServerMessageProto):
    pass


__all__ = (
    "OK_OP",
    "Ok",
)
