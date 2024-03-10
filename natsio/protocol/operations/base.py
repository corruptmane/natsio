from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class BaseProtocolClientMessage(ABC):
    @abstractmethod
    def build(self) -> bytes:
        pass


@dataclass
class BaseProtocolServerMessage(ABC):
    @classmethod
    @abstractmethod
    def from_bytes(cls, data: bytes) -> "BaseProtocolServerMessage":
        pass


__all__ = (
    "BaseProtocolClientMessage",
    "BaseProtocolServerMessage",
)
