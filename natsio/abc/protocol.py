from typing import Protocol


class ClientMessageProto(Protocol):
    def build(self) -> bytes:
        raise NotImplementedError


class ServerMessageProto(Protocol):
    pass
