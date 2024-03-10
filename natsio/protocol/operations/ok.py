from dataclasses import dataclass
from typing import Final

from .base import BaseProtocolServerMessage

OK_OP: Final[bytes] = b"+OK"


@dataclass
class Ok(BaseProtocolServerMessage):
    @classmethod
    def from_bytes(cls, data: bytes) -> 'Ok':
        return cls()


__all__ = (
    "OK_OP", "Ok",
)
