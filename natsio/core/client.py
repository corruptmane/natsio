import asyncio
from typing import Optional, Tuple, cast
from uuid import uuid4

from natsio.connection.listener import NATSListener
from natsio.connection.protocol import StreamProtocol
from natsio.connection.stream import Stream
from natsio.protocol.operations.ping_pong import PING
from natsio.protocol.operations.sub import Sub
from natsio.protocol.parser import ProtocolParser


class NATSCore:
    def __init__(self, host: str, port: int, timeout: float = 5) -> None:
        self.timeout = timeout
        self._host = host
        self._port = port
        self._listener: Optional[NATSListener] = None
        self._listener_task: Optional[asyncio.Task[None]] = None
        self._ping_task: Optional[asyncio.Task[None]] = None
        self._current_stream: Optional[Stream] = None

    async def _ping_loop(self) -> None:
        while True:
            if (
                self._current_stream is not None
                and not self._current_stream.is_closed
                and self._listener is not None
            ):
                await self._current_stream.write(PING)
                self._listener.outstanding_pings += 1
            await asyncio.sleep(10)

    async def _connect(self) -> None:
        loop = asyncio.get_running_loop()
        transport, protocol = cast(
            Tuple[asyncio.Transport, StreamProtocol],
            await asyncio.wait_for(
                loop.create_connection(
                    StreamProtocol,
                    self._host,
                    self._port,
                ),
                timeout=self.timeout,
            ),
        )
        transport.pause_reading()
        self._current_stream = Stream(transport, protocol)
        on_con_made = loop.create_future()
        self._listener = NATSListener(
            self._current_stream, ProtocolParser(), on_con_made
        )
        self._listener_task = loop.create_task(self._listener.listen())
        await on_con_made
        self._ping_task = loop.create_task(self._ping_loop())

    async def connect(self) -> None:
        await self._connect()

    async def close(self) -> None:
        if self._listener_task is not None and not self._listener_task.cancelled():
            self._listener_task.cancel()
        if self._current_stream is not None and not self._current_stream.is_closed:
            await self._current_stream.close()

    async def subscribe(self, subject: str) -> None:
        if self._current_stream is None:
            raise ValueError("Connection is not established")  # TODO
        sid = str(uuid4())
        await self._current_stream.write(Sub(sid=sid, subject=subject).build())
