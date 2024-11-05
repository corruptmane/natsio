import asyncio
import time
from typing import TYPE_CHECKING, AsyncIterator, MutableMapping

from natsio.exceptions.jetstream import APIError
from natsio.exceptions.subscription import MessageRetrievalTimeoutError, SubscriptionSetupError
from natsio.messages.core import CoreMsg
from natsio.messages.jetstream import JetStreamMsg
from natsio.protocol.headers import Header, StatusCode
from natsio.utils.json import json_dumps
from natsio.utils.logger import subscription_logger as log
from natsio.utils.time import to_nanoseconds

from .core import Subscription

if TYPE_CHECKING:
    from natsio.client.core import NATSCore
    from natsio.client.jetstream import JetStream


class PushSubscription:
    def __init__(
        self,
        client: "NATSCore",
        jetstream: "JetStream",
        sub: Subscription,
        stream_name: str,
        consumer_name: str,
        has_callback: bool,
    ) -> None:
        self._nc = client
        self._js = jetstream
        self._sub = sub
        self._stream_name = stream_name
        self._consumer_name = consumer_name
        self._message_iterator = PushSubscriptionMessageIterator(self) if not has_callback else None

    async def next_msg(self, timeout: float | int | None = 1) -> JetStreamMsg:
        msg = await self._sub.next_msg(timeout)
        if msg.headers and Header.STATUS in msg.headers:
            pass
        return JetStreamMsg(nats=self._nc, jetstream=self._js, msg=msg)

    @property
    def messages(self) -> AsyncIterator[JetStreamMsg]:
        if self._message_iterator is None:
            raise SubscriptionSetupError("subscription does not have callback")
        return self._message_iterator

    async def unsubscribe(self, max_msgs: int = 0) -> None:
        await self._sub.unsubscribe(max_msgs)


class PushSubscriptionMessageIterator:
    def __init__(self, subscription: PushSubscription) -> None:
        self._sub = subscription

    def __aiter__(self) -> "PushSubscriptionMessageIterator":
        return self

    async def __anext__(self) -> JetStreamMsg:
        return await self._sub.next_msg(timeout=None)


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
            payload["expires"] = int(to_nanoseconds(expires))

        await self._nc.publish(
            subject=self._request_batch_subject,
            data=json_dumps(payload),
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
