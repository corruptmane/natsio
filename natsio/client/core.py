import asyncio
from types import TracebackType
from typing import Mapping, Optional, Type

from natsio.abc.connection import ConnectionProto
from natsio.abc.protocol import ClientMessageProto
from natsio.connection.status import ConnectionStatus
from natsio.connection.tcp import TCPConnection
from natsio.exceptions.client import NoServersAvailable
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
from natsio.utils.uuid import get_uuid

from .config import ClientConfig, Server


class NATSCore:
    def __init__(
        self, config: ClientConfig,
    ) -> None:
        self._config = config
        self._current_server_index = -1
        self._connection: Optional[ConnectionProto] = None
        self._dispatcher = MessageDispatcher(self)
        self._do_reconnection_future: asyncio.Future[bool] = asyncio.Future()  # TODO
        self._reconnection_task: Optional[asyncio.Task[None]] = None

    @property
    def _current_server(self) -> Server:
        try:
            return self._config.server_pool.servers[self._current_server_index]
        except IndexError:
            raise NoServersAvailable()

    @property
    def _next_server(self) -> Server:
        self._current_server_index += 1
        return self._current_server

    @property
    def status(self) -> ConnectionStatus:
        if self._connection is None:
            raise ValueError("Connection is not established")
        return self._connection.status

    @property
    def is_closed(self) -> bool:
        return self.status == ConnectionStatus.CLOSED

    @property
    def is_connected(self) -> bool:
        return self.status == ConnectionStatus.CONNECTED

    @property
    def is_connecting(self) -> bool:
        return self.status == ConnectionStatus.CONNECTING

    @property
    def is_reconnecting(self) -> bool:
        return self.status == ConnectionStatus.RECONNECTING

    @property
    def is_disconnected(self) -> bool:
        return self.status == ConnectionStatus.DISCONNECTED

    @property
    def is_draining(self) -> bool:
        return self.status == ConnectionStatus.DRAINING

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

        self._connection = await TCPConnection.connect(
            host=server.uri.hostname,
            port=server.uri.port,
            dispatcher=self._dispatcher,
            do_reconnection_future=self._do_reconnection_future,
            ssl=ssl_context,
            ssl_hostname=ssl_hostname,
            handshake_first=handshake_first,
            timeout=self._config.connection_timeout,
        )

    async def _do_reconnect(self) -> None:
        self._do_reconnection_future = asyncio.Future()

    async def _reconnect(self) -> None:
        try:
            await self._do_reconnection_future
        except asyncio.CancelledError:
            print("Cancelled reconnection")
        else:
            await self._do_reconnect()

    async def connect(self) -> None:
        server = self._next_server
        await self._connect(server)
        # self._reconnection_task = asyncio.create_task(self._reconnect())

    async def close(self, timeout: float = 5) -> None:
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
        self._dispatcher.remove_subscription(sub.sid)

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
