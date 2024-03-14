import asyncio
from typing import Optional, Tuple, cast
from uuid import uuid4

from natsio.abc.connection import ConnectionProto, StreamProto
from natsio.abc.protocol import ClientMessageProto
from natsio.connection.protocol import StreamProtocol
from natsio.connection.stream import Stream
from natsio.connection.tcp import TCPConnection
from natsio.protocol.operations.ping_pong import PING
from natsio.protocol.operations.sub import Sub
from natsio.protocol.operations.unsub import Unsub
from natsio.protocol.parser import ProtocolParser


class NATSCore:
    def __init__(self, host: str = "localhost", port: int = 4222, connection_timeout: float = 5) -> None:
        self.connection_timeout = connection_timeout
        self._host = host
        self._port = port
        self._connection: Optional[ConnectionProto] = None

    async def _connect(self) -> None:
        self._connection = await TCPConnection.connect(self._host, self._port, self.connection_timeout)

    async def connect(self) -> None:
        await self._connect()

    async def close(self) -> None:
        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()

    async def flush(self) -> None:
        if self._connection is None:
            raise ValueError("Connection is not established")
        await self._connection.flush()

    async def _send_command(self, cmd: ClientMessageProto) -> None:
        if self._connection is None:
            raise ValueError("Connection is not established")
        await self._connection.send_command(cmd)

    async def subscribe(self, subject: str, queue: Optional[str] = None) -> None:
        sid = str(uuid4())
        await self._send_command(Sub(sid=sid, subject=subject, queue=queue))
