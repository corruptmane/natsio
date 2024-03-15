import asyncio
from typing import Mapping, MutableMapping, Optional
from uuid import uuid4

from natsio.abc.connection import ConnectionProto
from natsio.abc.protocol import ClientMessageProto
from natsio.connection.status import ConnectionStatus
from natsio.connection.tcp import TCPConnection
from natsio.messages.core import CoreMsg
from natsio.messages.dispatcher import MessageDispatcher
from natsio.protocol.operations.hpub import HPub
from natsio.protocol.operations.pub import Pub
from natsio.protocol.operations.sub import Sub
from natsio.protocol.operations.unsub import Unsub
from natsio.subscriptions.core import DEFAULT_SUB_PENDING_BYTES_LIMIT, DEFAULT_SUB_PENDING_MSGS_LIMIT, CoreCallback, Subscription
from natsio.utils.uuid import get_uuid
from natsio.utils.logger import client_logger as log


class NATSCore:
    def __init__(
        self, host: str = "localhost", port: int = 4222, connection_timeout: float = 5
    ) -> None:
        self.connection_timeout = connection_timeout
        self._host = host
        self._port = port
        self._connection: Optional[ConnectionProto] = None
        self._dispatcher = MessageDispatcher(self)

    @property
    def status(self) -> ConnectionStatus:
        if self._connection is None:
            raise ValueError("Connection is not established")
        return self._connection.status

    @property
    def is_closed(self) -> bool:
        return self.status == ConnectionStatus.CLOSED

    async def _connect(self) -> None:
        self._connection = await TCPConnection.connect(
            self._host, self._port, self._dispatcher, self.connection_timeout
        )

    async def connect(self) -> None:
        await self._connect()

    async def close(self, timeout: float = 5) -> None:
        if self._connection is not None and not self._connection.is_closed:
            log.info("Flushing the connection")
            try:
                await self._connection.flush()
            except asyncio.TimeoutError:
                log.warning("Flush timed out")
            log.info("Closing the connection")
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
        if not headers:
            await self._send_command(Pub(subject=subject, payload=data, reply_to=reply_to))
        else:
            await self._send_command(HPub(subject=subject, payload=data, reply_to=reply_to, headers=headers))

    async def subscribe(
        self,
        subject: str,
        queue: Optional[str] = None,
        callback: Optional[CoreCallback] = None,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ) -> Subscription:
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
