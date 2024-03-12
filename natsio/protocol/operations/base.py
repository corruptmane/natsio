from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class BaseProtocolClientMessage(ABC):
    @abstractmethod
    def build(self) -> bytes:
        pass


@dataclass
class BaseProtocolServerMessage(ABC):
    pass


__all__ = (
    "BaseProtocolClientMessage",
    "BaseProtocolServerMessage",
)
