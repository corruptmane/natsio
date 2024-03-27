import asyncio
import time
from itertools import cycle
from types import TracebackType
from typing import Iterator, Mapping, MutableSequence, Optional, Tuple, Type

from natsio.abc.connection import ConnectionProto
from natsio.abc.dispatcher import DispatcherProto
from natsio.abc.protocol import ClientMessageProto
from natsio.connection.status import ConnectionStatus
from natsio.connection.tcp import TCPConnection
from natsio.exceptions.client import NoServersAvailable
from natsio.exceptions.connection import TimeoutError
from natsio.exceptions.stream import EndOfStream
from natsio.messages.core import CoreMsg
from natsio.messages.dispatcher import MessageDispatcher
from natsio.protocol.operations.hpub import HPub
from natsio.protocol.operations.pub import Pub
from natsio.protocol.operations.sub import Sub
from natsio.protocol.operations.unsub import Unsub
from natsio.subscriptions.core import (
    DEFAULT_SUB_PENDING_BYTES_LIMIT,
    DEFAULT_SUB_PENDING_MSGS_LIMIT,
    CoreCallback,
    Subscription,
)
from natsio.utils.logger import client_logger as log
from natsio.utils.uuid import get_uuid

from .config import ClientConfig, Server
from .status import ClientStatus


def get_now() -> int:
    return time.monotonic_ns()


class ServerPoolIterator:
    def __init__(
        self,
        pool: Tuple[Server, ...],
        max_reconnect_attempts: int,
        reconnect_time_wait: float,
    ):
        self._pool = pool
        self._pool_iter: Iterator[Server] = cycle(self._pool)
        self._current_server: Server = next(self._pool_iter)
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_time_wait = reconnect_time_wait
        self._reconnect_time_wait_in_ns = int(reconnect_time_wait * 1_000_000_000)

    @property
    def current_server(self) -> Server:
        return self._current_server

    @property
    def _next_server(self) -> Server:
        self._current_server = next(self._pool_iter)
        return self._current_server

    async def next_server(self) -> Server:
        if self._max_reconnect_attempts <= 0:
            return self._next_server
        while True:
            if all(
                server.reconnects >= self._max_reconnect_attempts
                for server in self._pool
            ):
                raise NoServersAvailable()
            server = self._next_server
            if server.reconnects >= self._max_reconnect_attempts:
                continue

            now = get_now()
            if (
                server.last_attempt != 0
                and now < server.last_attempt + self._reconnect_time_wait_in_ns
            ):
                await asyncio.sleep(self._reconnect_time_wait)

            return server


class NATSCore:
    __slots__ = (
        "_config",
        "_server_pool_iterator",
        "_current_server_index",
        "_connection",
        "_dispatcher",
        "_disconnect_event",
        "_on_disconnect_waiter",
        "_status",
    )

    def __init__(
        self,
        config: ClientConfig,
    ) -> None:
        self._config: ClientConfig = config
        self._server_pool_iterator = ServerPoolIterator(
            self._config.server_pool,
            max_reconnect_attempts=self._config.max_reconnect_attempts,
            reconnect_time_wait=self._config.reconnect_time_wait,
        )
        self._connection: Optional[ConnectionProto] = None
        self._dispatcher: DispatcherProto = MessageDispatcher(self)
        self._disconnect_event: asyncio.Event = asyncio.Event()
        self._on_disconnect_waiter: Optional[asyncio.Task[None]] = None
        self._status: ClientStatus = ClientStatus.DISCONNECTED

    @property
    def connection_status(self) -> ConnectionStatus:
        if self._connection is None:
            raise ValueError("Connection is not established")
        return self._connection.status

    @property
    def status(self) -> ClientStatus:
        return self._status

    @property
    def is_disconnected(self) -> bool:
        return self.status == ClientStatus.DISCONNECTED

    @property
    def is_connecting(self) -> bool:
        return self.status == ClientStatus.CONNECTING

    @property
    def is_connected(self) -> bool:
        return self.status == ClientStatus.CONNECTED

    @property
    def is_reconnecting(self) -> bool:
        return self.status == ClientStatus.RECONNECTING

    @property
    def is_draining(self) -> bool:
        return self.status == ClientStatus.DRAINING

    @property
    def is_closed(self) -> bool:
        return self.status == ClientStatus.CLOSED

    def _raise_if_closed(self) -> None:
        if self.is_closed:
            raise ValueError("Connection is closed")

    async def _connect(self, server: Server) -> None:
        if server.uri.hostname is None:
            raise ValueError("Invalid server hostname")
        if server.uri.port is None:
            raise ValueError("Invalid server port")

        ssl_context = None
        ssl_hostname = None
        handshake_first = None
        if self._config.tls is not None:
            ssl_context = self._config.tls.ssl
            ssl_hostname = self._config.tls.hostname
            handshake_first = self._config.tls.handshake_first

        server.last_attempt = get_now()
        self._connection = await TCPConnection.connect(
            host=server.uri.hostname,
            port=server.uri.port,
            dispatcher=self._dispatcher,
            disconnect_event=self._disconnect_event,
            ssl=ssl_context,
            ssl_hostname=ssl_hostname,
            handshake_first=handshake_first,
            timeout=self._config.connection_timeout,
        )
        self._status = ClientStatus.CONNECTED
        server.info = self._connection.server_info

    async def connect(self) -> None:
        server = self._server_pool_iterator.current_server
        await self._connect(server)
        self._on_disconnect_waiter = asyncio.create_task(self._on_disconnect())

    async def close(self, flush: bool = True, timeout: float = 5) -> None:
        if self.is_closed:
            return
        if (
            self._on_disconnect_waiter is not None
            and not self._on_disconnect_waiter.done()
        ):
            self._on_disconnect_waiter.cancel()
            self._on_disconnect_waiter = None
        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close(flush)

    async def flush(self) -> None:
        if self._connection is None:
            raise ValueError("Connection is not established")
        await self._connection.flush()

    async def _replay_subscriptions(self) -> None:
        to_remove: MutableSequence[Subscription] = []
        to_replay: MutableSequence[Subscription] = []
        for sub in self._dispatcher.all_subscriptions():
            if sub.is_ready_to_close:
                to_remove.append(sub)
                continue
            to_replay.append(sub)
        for sub in to_remove:
            self._dispatcher.remove_subscription(sub.sid)
        for sub in to_replay:
            await self._send_command(
                Sub(sid=sub.sid, subject=sub.subject, queue=sub.queue)
            )

    async def _reconnect(self) -> None:
        while True:
            log.debug("Reconnecting to NATS server")
            server = await self._server_pool_iterator.next_server()

            try:
                await self._connect(server)
            except EndOfStream:
                continue
            except TimeoutError:
                server.reconnects += 1
                log.error("Connection to server %s timed out", server.uri.netloc)
                continue
            except Exception as exc:
                server.reconnects += 1
                log.exception(
                    "Failed to reconnect to server %s: %s",
                    server.uri.netloc,
                    exc.__class__.__name__,
                )
                continue
            else:
                log.info("Reconnected to NATS server %s", server.uri.netloc)
                break

    async def _on_disconnect(self) -> None:
        while not self.is_closed:
            await self._disconnect_event.wait()
            if self.is_closed or self.is_draining:
                break
            if not self._config.allow_reconnect:
                await self.close(flush=False)
                break

            self._status = ClientStatus.RECONNECTING
            if self._connection is not None:
                await self._connection.close(flush=False, close_dispatcher=False)
                self._connection = None
            try:
                await self._reconnect()
            except NoServersAvailable:
                await self.close(flush=False)
                break
            except Exception:
                log.exception("Failed to reconnect")
            else:
                self._disconnect_event.clear()
                await self._replay_subscriptions()

    async def __aenter__(self) -> "NATSCore":
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.close()

    async def _send_command(self, cmd: ClientMessageProto) -> None:
        if self._connection is None:
            raise ValueError("Connection is not established")
        await self._connection.send_command(cmd)

    async def publish(
        self,
        subject: str,
        data: bytes,
        reply_to: Optional[str] = None,
        headers: Optional[Mapping[str, str]] = None,
    ) -> None:
        self._raise_if_closed()
        if not headers:
            await self._send_command(
                Pub(subject=subject, payload=data, reply_to=reply_to)
            )
        else:
            await self._send_command(
                HPub(subject=subject, payload=data, reply_to=reply_to, headers=headers)
            )

    async def subscribe(
        self,
        subject: str,
        queue: Optional[str] = None,
        callback: Optional[CoreCallback] = None,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ) -> Subscription:
        self._raise_if_closed()
        sub = Subscription(self, subject, queue, callback=callback)
        await self._send_command(Sub(sid=sub.sid, subject=subject, queue=queue))
        self._dispatcher.add_subscription(sub)
        await sub.start()
        return sub

    async def unsubscribe(self, sub: Subscription, max_msgs: int = 0) -> None:
        await self._send_command(Unsub(sid=sub.sid, max_msgs=max_msgs))
        if max_msgs <= 0:
            self._dispatcher.remove_subscription(sub.sid)
        else:
            self._dispatcher.remove_subscription_when_ready(sub.sid)

    async def request(
        self,
        subject: str,
        data: bytes,
        headers: Optional[Mapping[str, str]] = None,
        timeout: float = 1,
    ) -> CoreMsg:
        self._raise_if_closed()
        sid = get_uuid()
        inbox_id = get_uuid()
        reply_to = f"_REQ_INBOX.{inbox_id}"
        future: asyncio.Future[CoreMsg] = asyncio.Future()
        await self._send_command(Sub(subject=reply_to, sid=sid))
        self._dispatcher.add_request_inbox(inbox_id, future)

        await self.publish(subject, data, reply_to=reply_to, headers=headers)

        try:
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            future.cancel()
            raise TimeoutError("Request timeout")
        finally:
            await self._send_command(Unsub(sid=sid))
            self._dispatcher.remove_request_inbox(inbox_id)
