import asyncio
from typing import Protocol, Sequence

from natsio.abc.subscription import SubscriptionProto
from natsio.messages.core import CoreMsg
from natsio.protocol.operations.hmsg import HMsg
from natsio.protocol.operations.msg import Msg


class DispatcherProto(Protocol):
    def add_subscription(self, sub: SubscriptionProto) -> None:
        raise NotImplementedError

    def all_subscriptions(self) -> Sequence[SubscriptionProto]:
        raise NotImplementedError

    def remove_subscription(self, sid: str) -> None:
        raise NotImplementedError

    def remove_subscription_when_ready(self, sid: str) -> None:
        raise NotImplementedError

    def add_request_inbox(self, sid: str, future: asyncio.Future[CoreMsg]) -> None:
        raise NotImplementedError

    def remove_request_inbox(self, sid: str) -> None:
        raise NotImplementedError

    async def dispatch_msg(self, msg: Msg) -> None:
        raise NotImplementedError

    async def dispatch_hmsg(self, msg: HMsg) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError
