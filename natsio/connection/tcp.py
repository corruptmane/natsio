import asyncio
import logging
from typing import Optional, Tuple, cast

from natsio.abc.connection import ConnectionProto, StreamProto
from natsio.abc.dispatcher import DispatcherProto
from natsio.abc.protocol import ClientMessageProto
from natsio.const import CRLF
from natsio.exceptions.protocol import UnknownProtocol
from natsio.exceptions.stream import EndOfStream
from natsio.exceptions.connection import TimeoutError
from natsio.protocol.operations.connect import Connect
from natsio.protocol.operations.err import ERR_OP
from natsio.protocol.operations.hmsg import HMSG_OP
from natsio.protocol.operations.info import INFO_OP
from natsio.protocol.operations.msg import MSG_OP
from natsio.protocol.operations.ok import OK_OP
from natsio.protocol.operations.ping_pong import PING_OP, PONG_OP, Ping, Pong
from natsio.protocol.parser import ProtocolParser
from natsio.utils.logger import connection_logger as log

from .protocol import StreamProtocol
from .stream import Stream
from .status import ConnectionStatus


class TCPConnection(ConnectionProto):
    def __init__(
        self,
        stream: StreamProto,
        dispatcher: DispatcherProto,
        status: ConnectionStatus = ConnectionStatus.DISCONNECTED,
    ) -> None:
        self._stream = stream
        self._parser = ProtocolParser()
        self._on_con_made: asyncio.Future[None] = asyncio.Future()
        self._listener_task: Optional[asyncio.Task[None]] = None
        self._pinger_task: Optional[asyncio.Task[None]] = None
        self._flusher_task: Optional[asyncio.Task[None]] = None
        self._flush_queue: asyncio.Queue[asyncio.Future[None]] = asyncio.Queue()
        self._pending: list[bytes] = []
        self._outstanding_pings = 0
        self._status = status
        self._dispatcher = dispatcher

    @property
    def outstanding_pings(self) -> int:
        return self._outstanding_pings

    @property
    def status(self) -> ConnectionStatus:
        return self._status

    @property
    def is_closed(self) -> bool:
        return self.status == ConnectionStatus.CLOSED

    @property
    def is_disconnected(self) -> bool:
        return self.status == ConnectionStatus.DISCONNECTED

    @property
    def is_connecting(self) -> bool:
        return self.status == ConnectionStatus.CONNECTING

    @property
    def is_connected(self) -> bool:
        return self.status == ConnectionStatus.CONNECTED

    @property
    def is_reconnecting(self) -> bool:
        return self.status == ConnectionStatus.RECONNECTING

    @property
    def is_draining(self) -> bool:
        return self.status == ConnectionStatus.DRAINING

    @classmethod
    async def connect(cls, host: str, port: int, dispatcher: DispatcherProto, timeout: float = 5) -> "TCPConnection":
        loop = asyncio.get_running_loop()
        transport, protocol = cast(
            Tuple[asyncio.Transport, StreamProtocol],
            await asyncio.wait_for(
                loop.create_connection(
                    StreamProtocol,
                    host,
                    port,
                ),
                timeout=timeout,
            ),
        )
        transport.pause_reading()
        self = cls(stream=Stream(transport, protocol), dispatcher=dispatcher, status=ConnectionStatus.CONNECTING)
        await self._setup_loops(loop)
        return self

    async def _process_operation(self, operation: bytes, payload: bytes) -> None:
        operation = operation.upper()
        try:
            if operation == MSG_OP:
                return await self.process_msg(payload)
            if operation == HMSG_OP:
                return await self.process_hmsg(payload)
            if operation == INFO_OP:
                return await self.process_info(payload)
            if operation == PING_OP:
                return await self.process_ping()
            if operation == PONG_OP:
                return await self.process_pong()
            if operation == OK_OP:
                return
            if operation == ERR_OP:
                return await self.process_error(payload)
        except Exception as exc:
            log.exception(exc)
        raise UnknownProtocol()

    async def _listen(self) -> None:
        while True:
            if self.is_closed or self.is_reconnecting:
                break

            try:
                data = await self._stream.read_until(CRLF)
            except EndOfStream:
                # TODO: handle EndOfStream
                continue
            except Exception as exc:
                # TODO: add error handling
                log.exception(exc)
                continue
            else:
                data = data.strip()

            try:
                operation, payload = data.split(maxsplit=1)
            except ValueError:
                operation = data
                payload = b""

            try:
                await self._process_operation(operation, payload)
            except UnknownProtocol:
                log.error("Unknown protocol")
                continue

    async def _listener(self) -> None:
        try:
            await self._listen()
        except asyncio.CancelledError:
            pass

    async def _flusher_loop(self, is_last_run: bool = False) -> None:
        while True:
            if is_last_run and self._flush_queue is not None and self._flush_queue.empty():
                break

            if not (self.is_connected or self.is_draining) or self.is_connecting:
                break

            if self._flush_queue is None:
                continue

            fut = await self._flush_queue.get()

            try:
                if len(self._pending) > 0:
                    await self._stream.write(b"".join(self._pending[:]))
                    self._pending = []
            except Exception as exc:
                # TODO: handle errors
                log.exception(exc)
            finally:
                fut.set_result(None)
                self._flush_queue.task_done()

    async def _flusher(self, is_last_run: bool = False) -> None:
        try:
            await self._flusher_loop(is_last_run)
        except asyncio.CancelledError:
            await self._flusher_loop(True)

    async def _ping_loop(self) -> None:
        while True:
            if not self.is_connected:
                continue

            await self.send_command(Ping())
            self._outstanding_pings += 1
            await asyncio.sleep(10)

    async def _pinger(self) -> None:
        try:
            await self._ping_loop()
        except asyncio.CancelledError:
            pass

    async def send_command(
        self, cmd: ClientMessageProto, force_flush: bool = False
    ) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        self._pending.append(cmd.build())
        await self._flush_queue.put(fut)
        if force_flush:
            await fut

    async def flush(self, timeout: float = 2) -> None:
        try:
            await asyncio.wait_for(self.send_command(Ping(), force_flush=True), timeout)
        except asyncio.TimeoutError:
            raise TimeoutError("Flush timeout")

    async def close(self) -> None:
        self._status = ConnectionStatus.DRAINING
        await self._dispatcher.close()
        if self._listener_task is not None and not self._listener_task.cancelled():
            self._listener_task.cancel()
        if self._pinger_task is not None and not self._pinger_task.cancelled():
            self._pinger_task.cancel()
        if self._flusher_task is not None and not self._flusher_task.cancelled():
            self._flusher_task.cancel()
        if self._stream is not None and not self._stream.is_closed:
            await self._stream.close()
        self._status = ConnectionStatus.CLOSED

    async def process_info(self, payload: bytes) -> None:
        await self._stream.write(
            Connect(
                verbose=False,
                pedantic=True,
                tls_required=False,
                lang="python/natsio",
                version="0.1.0",
                headers=True,
            ).build(),
        )
        self._on_con_made.set_result(None)

    async def process_ping(self) -> None:
        await self.send_command(Pong())

    async def process_pong(self) -> None:
        self._outstanding_pings = 0

    async def process_msg(self, payload: bytes) -> None:
        parsed = await self._parser.parse_msg(payload, self._stream)
        await self._dispatcher.dispatch_msg(parsed)

    async def process_hmsg(self, payload: bytes) -> None:
        parsed = await self._parser.parse_hmsg(payload, self._stream)
        await self._dispatcher.dispatch_hmsg(parsed)

    async def process_error(self, payload: bytes) -> None:
        log.warning("Received error from server: %s", payload.decode())

    async def _setup_loops(self, loop: asyncio.AbstractEventLoop) -> None:
        self._listener_task = loop.create_task(self._listener())
        await self._on_con_made
        self._status = ConnectionStatus.CONNECTED
        self._ping_task = loop.create_task(self._pinger())
        self._flush_task = loop.create_task(self._flusher())
