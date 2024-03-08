from dataclasses import dataclass

from .base import BaseProtocolServerMessage


@dataclass(slots=True)
class Ok(BaseProtocolServerMessage)
    @classmethod
    def from_bytes(cls, data: bytes) -> 'Ok':
        return cls()


__all__ = (
    "Ok",
)
