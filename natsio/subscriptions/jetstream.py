from typing import TYPE_CHECKING, AsyncIterator

from natsio.exceptions.subscription import SubscriptionSetupError
from natsio.messages.jetstream import JetStreamMsg
from natsio.protocol.headers import Header

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
        return JetStreamMsg(self._js, msg)

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
