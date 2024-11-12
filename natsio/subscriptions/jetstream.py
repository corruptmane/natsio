import asyncio
import time
from typing import TYPE_CHECKING, AsyncIterator, MutableMapping, Self

from natsio.abc.subscription import JetStreamCallback, SubscriptionProto, SubscriptionStatus
from natsio.exceptions.client import ClientClosedError
from natsio.exceptions.jetstream import APIError
from natsio.exceptions.subscription import MessageRetrievalTimeoutError, SubscriptionAlreadyStartedError, SubscriptionClosedError, SubscriptionSetupError
from natsio.messages.core import CoreMsg
from natsio.messages.jetstream import JetStreamMsg
from natsio.protocol.headers import Header, StatusCode
from natsio.utils.logger import subscription_logger as log
from natsio.utils.time import to_nanoseconds
from natsio.utils.uuid import get_uuid

from .core import Subscription

if TYPE_CHECKING:
    from natsio.client.core import NATSCore
    from natsio.client.jetstream import JetStream

DEFAULT_SUB_PENDING_MSGS_LIMIT = 512 * 1024
DEFAULT_SUB_PENDING_BYTES_LIMIT = 256 * 1024 * 1024


class PushSubscription(SubscriptionProto):
    def __init__(
        self,
        client: "NATSCore",
        jetstream: "JetStream",
        stream_name: str,
        consumer_name: str,
        subject: str,
        queue: str | None = None,
        sid: str | None = None,
        callback: JetStreamCallback | None = None,
        is_flow_control: bool = False,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ) -> None:
        self.subject = subject
        self.queue = queue
        if sid is None:
            sid = get_uuid()
        self.sid = sid
        self._nc = client
        self._js = jetstream
        self._stream_name = stream_name
        self._consumer_name = consumer_name
        self._msg_queue: asyncio.Queue[JetStreamMsg] = asyncio.Queue(maxsize=pending_msgs_limit)
        self._callback = callback
        self._is_flow_control = is_flow_control
        self._pending_next_msg_calls: MutableMapping[str, asyncio.Future[JetStreamMsg]] = {}
        self._pending_bytes_limit = pending_bytes_limit
        self._pending_size = 0
        self._reader_task: asyncio.Task[None] | None = None
        self._message_iterator: PushSubscriptionMessageIterator | None = None
        self._status = SubscriptionStatus.INITIALISING
        self._received = 0
        self._max_msgs = 0
        self._close_after_task: asyncio.Task[None] | None = None

    @property
    def status(self) -> SubscriptionStatus:
        return self._status

    def is_empty(self) -> bool:
        return self._msg_queue.empty()

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
        if self._nc.is_closed:
            raise ClientClosedError()

    async def _process_if_flow_control(self, msg: CoreMsg) -> bool:
        if (
            not msg.payload
            and not msg.headers
            and msg.reply_to is not None
            and msg.reply_to.startswith(f"$JS.FC.{self._stream_name}.{self._consumer_name}")
        ):
            await msg.reply(b"")
            return True
        if (
            msg.headers
            and Header.STATUS in msg.headers
            and Header.DESCRIPTION in msg.headers
            and msg.headers[Header.STATUS] == StatusCode.CONTROL_MESSAGE
            and msg.headers[Header.DESCRIPTION] == "Idle Heartbeat"
        ):
            fc_reply = msg.headers.get(Header.CONSUMER_STALLED)
            if fc_reply:
                await self._nc.publish(fc_reply, b"")
            return True
        return False

    async def add_msg(self, msg: CoreMsg) -> None:
        # NOTE: need consulting on flow control flow
        if self._is_flow_control:
            if await self._process_if_flow_control(msg):
                return
        payload_size = len(msg.payload)
        self._raise_if_slow_consumer(payload_size)
        await self._msg_queue.put(JetStreamMsg(nats=self._nc, jetstream=self._js, msg=msg))
        self._pending_size += payload_size
        if self._max_msgs > 0:
            self._received += 1

    async def next_msg(self, timeout: float | None = 1) -> JetStreamMsg:
        if self._callback is not None:
            raise SubscriptionSetupError("this method can not be used in async subscriptions")
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
            self._message_iterator = PushSubscriptionMessageIterator(self)
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
        if self._max_msgs > 0 and self._received >= self._max_msgs and self._msg_queue.empty():
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
        await self._nc.unsubscribe(self, max_msgs)
        if max_msgs <= 0:
            self._stop_processing()
            self._status = SubscriptionStatus.CLOSED
        else:
            self._max_msgs = max_msgs
            self._close_after_task = asyncio.create_task(self._close_after())

    @property
    def messages(self) -> AsyncIterator[JetStreamMsg]:
        if self._status is not SubscriptionStatus.OPERATING or self._message_iterator is None:
            raise SubscriptionSetupError("subscription is not started")
        return self._message_iterator


class PushSubscriptionMessageIterator:
    def __init__(self, subscription: PushSubscription) -> None:
        self._subscription = subscription
        self._stop_iteration_future: asyncio.Future[bool] = asyncio.Future()

    def cancel(self) -> None:
        if not self._stop_iteration_future.done():
            self._stop_iteration_future.set_result(True)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> JetStreamMsg:
        if self._stop_iteration_future.done():
            raise StopAsyncIteration
        return await self._subscription.next_msg(timeout=None)


class PullSubscription:
    def __init__(
        self,
        client: "NATSCore",
        jetstream: "JetStream",
        sub: Subscription,
        stream_name: str,
        consumer_name: str,
        prefix: str,
    ) -> None:
        self._nc = client
        self._js = jetstream
        self._sub = sub
        self._stream_name = stream_name
        self._consumer_name = consumer_name
        self._request_batch_subject = f"{prefix}.CONSUMER.MSG.NEXT.{stream_name}.{consumer_name}"

    @property
    def _deliver_to(self) -> str:
        return self._sub.subject

    async def _request_batch(self, batch_size: int, expires: float | int | None = None, no_wait: bool | None = None) -> None:
        if no_wait and expires is not None:
            raise ValueError("`no_wait` and `expires` should not be used together")

        payload: MutableMapping[str, int | bool] = dict(batch=batch_size)
        if expires is not None:
            payload["expires"] = to_nanoseconds(expires)

        await self._nc.publish(
            subject=self._request_batch_subject,
            data=self._nc.serializer.dump(payload),
            reply_to=self._deliver_to,
        )

    @staticmethod
    def _calculate_time_until(start_time: float, timeout: float | int | None = None) -> float | None:
        if timeout is None:
            return None
        return timeout - (time.monotonic() - start_time)

    async def _get_from_queue(self, msg_queue: asyncio.Queue[CoreMsg]) -> JetStreamMsg | None:
        await asyncio.sleep(0)
        try:
            msg = msg_queue.get_nowait()
        except Exception as exc:
            log.error("Unexpected error at message retrieval: %s", exc.__class__.__name__, exc_info=exc)
            return None
        self._sub._pending_size -= len(msg.payload)
        msg_queue.task_done()
        if msg.headers and Header.STATUS in msg.headers:
            return None
        return JetStreamMsg(nats=self._nc, jetstream=self._js, msg=msg)

    async def _fetch_one(self, expires: float | int | None = None) -> JetStreamMsg:
        start_time = time.monotonic()
        msg_queue = self._sub._msg_queue

        msg: CoreMsg | JetStreamMsg | None
        while not msg_queue.empty():
            msg = await self._get_from_queue(msg_queue)
            if not msg:
                continue
            return msg

        await self._request_batch(batch_size=1, expires=expires)
        await asyncio.sleep(0)

        deadline = self._calculate_time_until(start_time, expires)
        while True:
            msg = await self._sub.next_msg(timeout=deadline)
            if not msg.headers or Header.STATUS not in msg.headers:
                return JetStreamMsg(nats=self._nc, jetstream=self._js, msg=msg)
            if msg.headers[Header.STATUS] in (
                StatusCode.NO_MESSAGES, StatusCode.CONFLICT, StatusCode.REQUEST_TIMEOUT,
            ):
                raise MessageRetrievalTimeoutError()
            else:
                raise APIError.from_msg_headers(msg.headers)

    async def _fetch_many(self, batch_size: int, expires: float | int | None = None) -> list[JetStreamMsg]:
        start_time = time.monotonic()
        msg_queue = self._sub._msg_queue
        messages: list[JetStreamMsg] = []

        msg: CoreMsg | JetStreamMsg | None
        while not msg_queue.empty():
            if batch_size - len(messages) <= 0:
                return messages
            msg = await self._get_from_queue(msg_queue)
            if not msg:
                continue
            messages.append(msg)

        await self._request_batch(batch_size=batch_size - len(messages), expires=expires)
        await asyncio.sleep(0)

        while batch_size - len(messages) > 0:
            deadline = self._calculate_time_until(start_time, expires)
            if deadline is not None and deadline <= 0:
                break
            try:
                msg = await self._sub.next_msg(timeout=deadline)
            except MessageRetrievalTimeoutError:
                break
            if not msg.headers or Header.STATUS not in msg.headers:
                messages.append(JetStreamMsg(nats=self._nc, jetstream=self._js, msg=msg))
                continue
            if msg.headers[Header.STATUS] in (
                StatusCode.NO_MESSAGES, StatusCode.CONFLICT, StatusCode.REQUEST_TIMEOUT,
            ):
                break
            else:
                if not messages:
                    raise APIError.from_msg_headers(msg.headers)

        if not messages:
            raise MessageRetrievalTimeoutError()
        return messages

    async def fetch(self, batch_size: int = 1, timeout: float | int | None = None) -> list[JetStreamMsg]:
        if batch_size == 1:
            msg = await self._fetch_one(timeout)
            return [msg]
        return await self._fetch_many(batch_size, timeout)
