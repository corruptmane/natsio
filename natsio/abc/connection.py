from typing import Protocol

from .protocol import ClientMessageProto


class StreamProto(Protocol):
    @property
    def is_closed(self) -> bool:
        raise NotImplementedError

    async def read(self, max_bytes: int) -> bytes:
        raise NotImplementedError

    async def read_exactly(self, size: int) -> bytes:
        raise NotImplementedError

    async def read_until(self, separator: bytes) -> bytes:
        raise NotImplementedError

    async def write(self, data: bytes) -> None:
        raise NotImplementedError

    async def send_eof(self) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError


class ConnectionProto(Protocol):
    @property
    def is_closed(self) -> bool:
        raise NotImplementedError

    @property
    def outstanding_pings(self) -> int:
        raise NotImplementedError

    @classmethod
    async def connect(cls, host: str, port: int, timeout: float = 5) -> "ConnectionProto":
        raise NotImplementedError

    async def send_command(self, cmd: ClientMessageProto, force_flush: bool = False) -> None:
        raise NotImplementedError

    async def flush(self) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def process_info(self, payload: bytes) -> None:
        raise NotImplementedError

    async def process_ping(self) -> None:
        raise NotImplementedError

    async def process_pong(self) -> None:
        raise NotImplementedError

    async def process_msg(self, payload: bytes) -> None:
        raise NotImplementedError

    async def process_hmsg(self, payload: bytes) -> None:
        raise NotImplementedError

    async def process_error(self, payload: bytes) -> None:
        raise NotImplementedError
