import asyncio
from ssl import SSLContext
from typing import TYPE_CHECKING, Optional, Protocol

from natsio.connection.status import ConnectionStatus
from natsio.protocol.operations.connect import Connect

from .client import ErrorCallback
from .dispatcher import DispatcherProto
from .protocol import ClientMessageProto

if TYPE_CHECKING:
    from natsio.client.config import ServerInfo


class StreamProto(Protocol):
    @property
    def is_closed(self) -> bool:
        raise NotImplementedError

    def upgraded_to_tls(self, transport: asyncio.Transport) -> None:
        raise NotImplementedError

    async def read(self, max_bytes: int) -> bytes:
        raise NotImplementedError

    async def read_exactly(self, size: int) -> bytes:
        raise NotImplementedError

    async def read_until(self, separator: bytes) -> bytes:
        raise NotImplementedError

    async def write(self, data: bytes) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError


class ConnectionProto(Protocol):
    @property
    def is_disconnected(self) -> bool:
        raise NotImplementedError

    @property
    def is_connecting(self) -> bool:
        raise NotImplementedError

    @property
    def is_connected(self) -> bool:
        raise NotImplementedError

    @property
    def is_draining(self) -> bool:
        raise NotImplementedError

    @property
    def is_closed(self) -> bool:
        raise NotImplementedError

    @property
    def status(self) -> ConnectionStatus:
        raise NotImplementedError

    @property
    def outstanding_pings(self) -> int:
        raise NotImplementedError

    @property
    def server_info(self) -> "ServerInfo":
        raise NotImplementedError

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        dispatcher: DispatcherProto,
        disconnect_event: asyncio.Event,
        connect_operation: Connect,
        ping_interval: int,
        max_outstanding_pings: int,
        flusher_queue_size: int,
        max_pending_size: int,
        force_flush_timeout: int,
        error_callback: ErrorCallback,
        timeout: float,
        ssl: Optional[SSLContext],
        ssl_hostname: Optional[str],
        handshake_first: Optional[bool],
    ) -> "ConnectionProto":
        raise NotImplementedError

    async def send_command(
        self, cmd: ClientMessageProto, force_flush: bool = False
    ) -> None:
        raise NotImplementedError

    async def flush(self) -> None:
        raise NotImplementedError

    async def close(self, flush: bool = True, close_dispatcher: bool = True) -> None:
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
