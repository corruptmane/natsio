import asyncio
from typing import TYPE_CHECKING, Awaitable, Callable, Optional

from natsio.messages.core import CoreMsg
from natsio.utils.logger import subscription_logger as log
from natsio.utils.uuid import get_uuid

if TYPE_CHECKING:
    from natsio.client.core import NATSCore

CoreCallback = Callable[[CoreMsg], Awaitable[None]]

DEFAULT_SUB_PENDING_MSGS_LIMIT = 512 * 1024
DEFAULT_SUB_PENDING_BYTES_LIMIT = 128 * 1024 * 1024


class Subscription:
    def __init__(
        self,
        client: "NATSCore",
        subject: str,
        queue: Optional[str] = None,
        sid: Optional[str] = None,
        callback: Optional[CoreCallback] = None,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ) -> None:
        self.subject = subject
        self.queue = queue
        if sid is None:
            sid = get_uuid()
        self.sid = sid

        self._client = client
        self._msg_queue: asyncio.Queue[CoreMsg] = asyncio.Queue(
            maxsize=pending_msgs_limit
        )
        self._callback = callback
        self._pending_msgs_limit = pending_msgs_limit
        self._pending_bytes_limit = pending_bytes_limit
        self._pending_size = 0
        self._reader_task: Optional[asyncio.Task[None]] = None

    async def add_msg(self, msg: CoreMsg) -> None:
        await self._msg_queue.put(msg)
        self._pending_size += len(msg.payload)

    async def next_msg(self, timeout: Optional[float] = 1) -> CoreMsg:
        if self._callback is not None:
            raise ValueError("this method can not be used in async subscriptions")

        try:
            msg = await asyncio.wait_for(self._msg_queue.get(), timeout)
        except asyncio.TimeoutError:
            if self._client.is_closed:
                raise ValueError("client is closed")
            raise asyncio.TimeoutError("timeout waiting for message")
        else:
            self._pending_size -= len(msg.payload)
            self._msg_queue.task_done()
            return msg

    async def start(self) -> None:
        if self._callback is None:
            raise ValueError("callback is not set")
        if self._reader_task is not None:
            raise ValueError("reader task is already running")
        self._reader_task = asyncio.create_task(self._reader())

    async def _reader_loop(self) -> None:
        if self._callback is None:
            raise ValueError("callback is not set")
        while True:
            msg = await self._msg_queue.get()
            self._pending_size -= len(msg.payload)

            try:
                await self._callback(msg)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                # TODO: add error processing
                log.exception(exc)
            finally:
                self._msg_queue.task_done()

    async def _reader(self) -> None:
        try:
            await self._reader_loop()
        except asyncio.CancelledError:
            pass

    async def unsubscribe(self) -> None:
        if self._reader_task is not None and not self._reader_task.done():
            self._reader_task.cancel()
            self._reader_task = None

        await self._client.unsubscribe(self)
