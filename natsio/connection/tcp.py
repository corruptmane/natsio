import asyncio
from ssl import SSLContext
from typing import TYPE_CHECKING, List, Optional, Tuple, cast

from natsio.abc.client import ErrorCallback
from natsio.abc.connection import ConnectionProto, StreamProto
from natsio.abc.dispatcher import DispatcherProto
from natsio.abc.protocol import ClientMessageProto
from natsio.const import CRLF
from natsio.exceptions.client import FlushTimeoutError, TLSError
from natsio.exceptions.connection import (
    ConnectionFailedError,
    ConnectionTimeoutError,
    NoConnectionError,
    OutboundBufferLimitError,
)
from natsio.exceptions.protocol import (
    ProtocolError,
    SlowConsumer,
    StaleConnection,
    UnknownProtocol,
)
from natsio.exceptions.stream import EndOfStream
from natsio.protocol.operations.connect import Connect
from natsio.protocol.operations.err import ERR_OP
from natsio.protocol.operations.hmsg import HMSG_OP
from natsio.protocol.operations.info import INFO_OP, Info
from natsio.protocol.operations.msg import MSG_OP
from natsio.protocol.operations.ok import OK_OP
from natsio.protocol.operations.ping_pong import PING_OP, PONG_OP, Ping, Pong
from natsio.protocol.parser import ProtocolParser
from natsio.utils.logger import connection_logger as log

from .protocol import StreamProtocol
from .status import ConnectionStatus
from .stream import Stream

if TYPE_CHECKING:
    from natsio.client.config import ServerInfo


class TCPConnection(ConnectionProto):
    def __init__(
        self,
        stream: StreamProto,
        dispatcher: DispatcherProto,
        disconnect_event: asyncio.Event,
        connect_operation: Connect,
        error_callback: ErrorCallback,
        ping_interval: int,
        max_outstanding_pings: int,
        flusher_queue_size: int,
        max_pending_size: int,
        force_flush_timeout: int,
    ) -> None:
        self._stream = stream
        self._connect_operation = connect_operation
        self._ping_interval = ping_interval
        self._max_outstanding_pings = max_outstanding_pings
        self._max_pending_size = max_pending_size
        self._force_flush_timeout = force_flush_timeout
        self._error_cb = error_callback
        self._parser = ProtocolParser()
        self._listener_task: Optional[asyncio.Task[None]] = None
        self._pinger_task: Optional[asyncio.Task[None]] = None
        self._flusher_task: Optional[asyncio.Task[None]] = None
        self._flush_queue: asyncio.Queue[asyncio.Future[None]] = asyncio.Queue(
            maxsize=flusher_queue_size
        )
        self._pending: List[bytes] = []
        self._pending_data_size: int = 0
        self._outstanding_pings = 0
        self._status = ConnectionStatus.CONNECTING
        self._dispatcher = dispatcher
        self._disconnect_event = disconnect_event
        self._server_info: Optional["Info"] = None

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
    def is_draining(self) -> bool:
        return self.status == ConnectionStatus.DRAINING

    @property
    def server_info(self) -> "ServerInfo":
        if self._server_info is None:
            raise NoConnectionError()
        return self._server_info.server_info

    @staticmethod
    async def _upgrade_tls(
        loop: asyncio.AbstractEventLoop,
        transport: asyncio.Transport,
        protocol: StreamProtocol,
        ssl: Optional[SSLContext],
        hostname: Optional[str],
    ) -> asyncio.Transport:
        if ssl is None:
            raise TLSError("SSLContext is required for TLS upgrade")
        new_transport = await loop.start_tls(
            transport, protocol, ssl, server_hostname=hostname
        )
        if new_transport is not None:
            protocol.patch_transport(new_transport)
            return new_transport
        return transport

    async def _confirm_connection(
        self,
        loop: asyncio.AbstractEventLoop,
        transport: asyncio.Transport,
        protocol: StreamProtocol,
        ssl: Optional[SSLContext],
        ssl_hostname: Optional[str],
        handshake_first: Optional[bool],
    ) -> None:
        if handshake_first is True:
            transport = await self._upgrade_tls(
                loop, transport, protocol, ssl, ssl_hostname
            )
            self._stream.upgraded_to_tls(transport)
        while True:
            operation, payload = await self._get_operation_and_payload()
            if operation != INFO_OP:
                continue
            if handshake_first is False:
                transport = await self._upgrade_tls(
                    loop, transport, protocol, ssl, ssl_hostname
                )
                self._stream.upgraded_to_tls(transport)
            await self.process_info(payload)
            break
        self._status = ConnectionStatus.CONNECTED

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
        ssl: Optional[SSLContext] = None,
        ssl_hostname: Optional[str] = None,
        handshake_first: Optional[bool] = None,
    ) -> "TCPConnection":
        loop = asyncio.get_running_loop()
        try:
            transport, protocol = cast(
                Tuple[asyncio.Transport, StreamProtocol],
                await asyncio.wait_for(
                    loop.create_connection(
                        lambda: StreamProtocol(disconnect_event=disconnect_event),
                        host,
                        port,
                    ),
                    timeout=timeout,
                ),
            )
        except asyncio.TimeoutError:
            raise ConnectionTimeoutError() from None
        except OSError as exc:
            cause: Optional[OSError] = exc
            if "Connect call failed" in str(cause):
                cause = None
            raise ConnectionFailedError() from cause
        self = cls(
            stream=Stream(transport, protocol),
            dispatcher=dispatcher,
            disconnect_event=disconnect_event,
            connect_operation=connect_operation,
            error_callback=error_callback,
            ping_interval=ping_interval,
            max_outstanding_pings=max_outstanding_pings,
            flusher_queue_size=flusher_queue_size,
            max_pending_size=max_pending_size,
            force_flush_timeout=force_flush_timeout,
        )
        await self._confirm_connection(
            loop, transport, protocol, ssl, ssl_hostname, handshake_first
        )
        await self._setup_loops(loop)
        return self

    async def _process_operation(self, operation: bytes, payload: bytes) -> None:
        operation = operation.upper()

        try:
            if operation == MSG_OP:
                return await self.process_msg(payload)
            if operation == HMSG_OP:
                return await self.process_hmsg(payload)
            if operation == PING_OP:
                return await self.process_ping()
            if operation == PONG_OP:
                return await self.process_pong()
            if operation == OK_OP:
                return
            if operation == ERR_OP:
                return await self.process_error(payload)
            raise UnknownProtocol(is_disconnected=False)
        except ProtocolError as exc:
            await self._error_cb(exc)
            await self._process_protocol_error(exc)
        except Exception as exc:
            await self._error_cb(exc)

    async def _get_operation_and_payload(self) -> Tuple[bytes, bytes]:
        data = await self._stream.read_until(CRLF)
        data = data.strip()

        try:
            operation, payload = data.split(maxsplit=1)
        except ValueError:
            operation = data
            payload = b""

        return operation, payload

    async def _listen(self) -> None:
        while True:
            log.debug("Listener loop start")
            if self.is_closed:
                break

            if self._disconnect_event.is_set():
                if self._status != ConnectionStatus.DISCONNECTED:
                    self._status = ConnectionStatus.DISCONNECTED
                break

            try:
                operation, payload = await self._get_operation_and_payload()
            except EndOfStream:
                # TODO: handle EndOfStream
                log.error("End of stream")
                continue
            except Exception as exc:
                # TODO: add error handling
                log.exception(exc)
                continue

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
            log.debug("Flusher loop start")
            if self._disconnect_event.is_set():
                if self._status != ConnectionStatus.DISCONNECTED:
                    self._status = ConnectionStatus.DISCONNECTED
                break
            if (
                is_last_run
                and self._flush_queue is not None
                and self._flush_queue.empty()
            ):
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
                    self._pending_data_size = 0
            except asyncio.CancelledError:
                break
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
            pass

    async def _ping_loop(self) -> None:
        while True:
            await asyncio.sleep(self._ping_interval)
            if self.is_closed:
                break
            if not self.is_connected:
                continue
            if self._disconnect_event.is_set():
                if self._status != ConnectionStatus.DISCONNECTED:
                    self._status = ConnectionStatus.DISCONNECTED
                break

            await self.send_command(Ping())
            self._outstanding_pings += 1
            if self._outstanding_pings > self._max_outstanding_pings:
                await self._process_protocol_error(StaleConnection())
                return

    async def _pinger(self) -> None:
        try:
            await self._ping_loop()
        except asyncio.CancelledError:
            pass

    async def send_command(
        self, cmd: ClientMessageProto, force_flush: bool = False
    ) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        msg = cmd.build()
        msg_size = len(msg)
        if self._max_pending_size > 0 and (
            msg_size + self._pending_data_size > self._max_pending_size
        ):
            raise OutboundBufferLimitError()
        self._pending.append(msg)
        self._pending_data_size += msg_size
        await self._flush_queue.put(fut)
        if force_flush or (
            self._max_pending_size > 0
            and self._pending_data_size > self._max_pending_size
        ):
            try:
                await asyncio.wait_for(fut, timeout=self._force_flush_timeout)
            except asyncio.TimeoutError:
                raise FlushTimeoutError()

    async def flush(self, timeout: float = 2) -> None:
        try:
            await asyncio.wait_for(self.send_command(Ping(), force_flush=True), timeout)
        except asyncio.TimeoutError:
            raise FlushTimeoutError() from None

    async def close(self, flush: bool = True, close_dispatcher: bool = True) -> None:
        self._status = ConnectionStatus.DRAINING
        if close_dispatcher:
            await self._dispatcher.close()
        if self._listener_task is not None and not self._listener_task.cancelled():
            self._listener_task.cancel()
        if self._pinger_task is not None and not self._pinger_task.cancelled():
            self._pinger_task.cancel()
        if self._flusher_task is not None and not self._flusher_task.cancelled():
            self._flusher_task.cancel()
        if flush:
            await self._flusher_loop(is_last_run=True)
        if self._stream is not None and not self._stream.is_closed:
            await self._stream.close()
        self._status = ConnectionStatus.CLOSED

    async def process_info(self, payload: bytes) -> None:
        self._server_info = self._parser.parse_info(payload)
        if self._server_info.headers:
            self._connect_operation.headers = True
            self._connect_operation.no_responders = (
                False  # TODO: support no_responders headers parsing
            )
        # TODO: sig/jwt/nkey support
        await self._stream.write(self._connect_operation.build())

    async def process_ping(self) -> None:
        await self.send_command(Pong())

    async def process_pong(self) -> None:
        self._outstanding_pings = 0

    async def process_msg(self, payload: bytes) -> None:
        parsed = await self._parser.parse_msg(payload, self._stream)
        try:
            await self._dispatcher.dispatch_msg(parsed)
        except asyncio.QueueFull:
            await self._error_cb(
                SlowConsumer(
                    subject=parsed.subject, sid=parsed.sid, is_disconnected=False
                )
            )

    async def process_hmsg(self, payload: bytes) -> None:
        parsed = await self._parser.parse_hmsg(payload, self._stream)
        try:
            await self._dispatcher.dispatch_hmsg(parsed)
        except asyncio.QueueFull:
            await self._error_cb(
                SlowConsumer(
                    subject=parsed.subject, sid=parsed.sid, is_disconnected=False
                )
            )

    async def process_error(self, payload: bytes) -> None:
        self._parser.parse_and_raise_error(payload)

    async def _process_protocol_error(self, error: ProtocolError) -> None:
        if error.is_disconnected:
            await self.close(flush=False)

    async def _setup_loops(self, loop: asyncio.AbstractEventLoop) -> None:
        self._listener_task = loop.create_task(self._listener())
        self._ping_task = loop.create_task(self._pinger())
        self._flush_task = loop.create_task(self._flusher())
