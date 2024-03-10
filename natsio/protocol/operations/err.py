from dataclasses import dataclass
from typing import Final

from .base import BaseProtocolServerMessage

ERR_OP: Final[bytes] = b"-ERR"


@dataclass
class Err(BaseProtocolServerMessage):
    message: str

    @classmethod
    def from_bytes(cls, data: bytes) -> "Err":
        return cls(message=data.decode())


__all__ = (
    "ERR_OP",
    "Err",
)
