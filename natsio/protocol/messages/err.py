from dataclasses import dataclass

from .base import BaseProtocolServerMessage


@dataclass(slots=True)
class Err(BaseProtocolServerMessage)
    message: str

    @classmethod
    def from_bytes(cls, data: bytes) -> 'Err':
        return cls()


__all__ = (
    "Err",
)
