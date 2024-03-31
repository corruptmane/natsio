import asyncio
from enum import Enum
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    Awaitable,
    Callable,
    MutableMapping,
    Optional,
)

from natsio.exceptions.client import ClientClosedError
from natsio.exceptions.subscription import (
    MessageRetrievalTimeoutError,
    SubscriptionAlreadyStartedError,
    SubscriptionClosedError,
    SubscriptionSetupError,
)
from natsio.messages.core import CoreMsg
from natsio.utils.logger import subscription_logger as log
from natsio.utils.uuid import get_uuid

if TYPE_CHECKING:
    from natsio.client.core import NATSCore

CoreCallback = Callable[[CoreMsg], Awaitable[None]]

DEFAULT_SUB_PENDING_MSGS_LIMIT = 512 * 1024
DEFAULT_SUB_PENDING_BYTES_LIMIT = 128 * 1024 * 1024


class SubscriptionStatus(Enum):
    INITIALISING = "INITIALISING"
    OPERATING = "OPERATING"
    DRAINING = "DRAINING"
    CLOSED = "CLOSED"


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
        self._pending_next_msg_calls: MutableMapping[str, asyncio.Future[CoreMsg]] = {}
        self._pending_bytes_limit = pending_bytes_limit
        self._pending_size = 0
        self._reader_task: Optional[asyncio.Task[None]] = None
        self._message_iterator: Optional[SubscriptionMessageIterator] = None
        self._status = SubscriptionStatus.INITIALISING
        self._received = 0
        self._max_msgs = 0
        self._close_after_task: Optional[asyncio.Task[None]] = None

    def _raise_if_slow_consumer(self, new_data_size: int) -> None:
        if self._pending_bytes_limit <= 0:
            return
        if self._pending_size + new_data_size >= self._pending_bytes_limit:
            raise asyncio.QueueFull()
        if self._msg_queue is not None and self._msg_queue.full():
            raise asyncio.QueueFull()

    def is_slow(self) -> bool:
        if self._pending_bytes_limit <= 0:
            return False
        return self._pending_size >= self._pending_bytes_limit

    def _raise_if_client_closed(self) -> None:
        if self._client.is_closed:
            raise ClientClosedError()

    async def add_msg(self, msg: CoreMsg) -> None:
        payload_size = len(msg.payload)
        self._raise_if_slow_consumer(payload_size)
        await self._msg_queue.put(msg)
        self._pending_size += payload_size
        if self._max_msgs > 0:
            self._received += 1

    async def next_msg(self, timeout: Optional[float] = 1) -> CoreMsg:
        if self._callback is not None:
            raise SubscriptionSetupError(
                "this method can not be used in async subscriptions"
            )
        if self._status is SubscriptionStatus.CLOSED:
            raise SubscriptionClosedError()

        task_id = get_uuid()
        try:
            fut = asyncio.create_task(asyncio.wait_for(self._msg_queue.get(), timeout))
            self._pending_next_msg_calls[task_id] = fut
            msg = await fut
        except asyncio.TimeoutError:
            self._raise_if_client_closed()
            raise MessageRetrievalTimeoutError()
        else:
            self._pending_size -= len(msg.payload)
            self._msg_queue.task_done()
            return msg
        finally:
            self._pending_next_msg_calls.pop(task_id, None)

    async def start(self) -> None:
        if self._status is SubscriptionStatus.OPERATING:
            raise SubscriptionAlreadyStartedError()
        if self._status is SubscriptionStatus.CLOSED:
            raise SubscriptionClosedError()
        if self._callback is not None:
            if self._reader_task is not None:
                raise SubscriptionAlreadyStartedError()
            self._reader_task = asyncio.create_task(self._reader())
        else:
            self._message_iterator = SubscriptionMessageIterator(self)
        self._status = SubscriptionStatus.OPERATING

    async def _reader_loop(self) -> None:
        if self._callback is None:
            raise SubscriptionSetupError("callback is not set")
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

    def _stop_processing(self) -> None:
        if self._reader_task is not None and not self._reader_task.done():
            self._reader_task.cancel()
            self._reader_task = None

        if self._message_iterator is not None:
            self._message_iterator.cancel()
            for fut in self._pending_next_msg_calls.values():
                if not fut.done():
                    fut.cancel()

    @property
    def is_ready_to_close(self) -> bool:
        if (
            self._max_msgs > 0
            and self._received >= self._max_msgs
            and self._msg_queue.empty()
        ):
            return True
        return False

    async def _close_after(self) -> None:
        while True:
            await asyncio.sleep(0)
            if self.is_ready_to_close:
                self._stop_processing()
                self._status = SubscriptionStatus.CLOSED
                break

    async def unsubscribe(self, max_msgs: int = 0) -> None:
        if (
            self._status is SubscriptionStatus.CLOSED
            or self._status is SubscriptionStatus.DRAINING
        ):
            return
        self._status = SubscriptionStatus.DRAINING
        await self._client.unsubscribe(self, max_msgs)
        if max_msgs <= 0:
            self._stop_processing()
            self._status = SubscriptionStatus.CLOSED
        else:
            self._max_msgs = max_msgs
            self._close_after_task = asyncio.create_task(self._close_after())

    @property
    def messages(self) -> AsyncIterator[CoreMsg]:
        if (
            self._status is not SubscriptionStatus.OPERATING
            or self._message_iterator is None
        ):
            raise SubscriptionSetupError("subscription is not started")
        return self._message_iterator


class SubscriptionMessageIterator:
    def __init__(self, subscription: Subscription) -> None:
        self._subscription = subscription
        self._stop_iteration_future: asyncio.Future[bool] = asyncio.Future()

    def cancel(self) -> None:
        if not self._stop_iteration_future.done():
            self._stop_iteration_future.set_result(True)

    def __aiter__(self) -> "SubscriptionMessageIterator":
        return self

    async def __anext__(self) -> CoreMsg:
        if self._stop_iteration_future.done():
            raise StopAsyncIteration
        return await self._subscription.next_msg(timeout=None)
