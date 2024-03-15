import asyncio
from typing import TYPE_CHECKING, MutableMapping, Union

from natsio.abc.dispatcher import DispatcherProto
from natsio.protocol.operations.hmsg import HMsg
from natsio.protocol.operations.msg import Msg
from natsio.subscriptions.core import Subscription

from .core import CoreMsg

if TYPE_CHECKING:
    from natsio.client.core import NATSCore


class MessageDispatcher(DispatcherProto):
    def __init__(self, client: "NATSCore") -> None:
        self._client = client
        self._subscriptions: MutableMapping[str, Subscription] = {}
        self._inboxes: MutableMapping[str, asyncio.Future[CoreMsg]] = {}

    def add_subscription(self, sub: Subscription) -> None:
        self._subscriptions[sub.sid] = sub

    def remove_subscription(self, sid: str) -> None:
        self._subscriptions.pop(sid, None)

    def add_request_inbox(self, sid: str, future: asyncio.Future[CoreMsg]) -> None:
        self._inboxes[sid] = future

    def remove_request_inbox(self, sid: str) -> None:
        self._inboxes.pop(sid, None)

    def _build_core_msg(self, msg: Union[Msg, HMsg]) -> CoreMsg:
        payload = msg.payload if msg.payload is not None else b""
        if isinstance(msg, Msg):
            return CoreMsg(self._client, msg.subject, payload, msg.reply_to)
        return CoreMsg(self._client, msg.subject, payload, msg.reply_to, msg.headers)

    async def _dispatch_to_inbox(self, msg: CoreMsg, inbox_id: str) -> None:
        future = self._inboxes.get(inbox_id)
        if future is None or future.done():
            return
        future.set_result(msg)
        await asyncio.sleep(0)

    async def dispatch_msg(self, msg: Msg) -> None:
        core_msg = self._build_core_msg(msg)
        if msg.is_request_inbox:
            return await self._dispatch_to_inbox(core_msg, msg.inbox_id)
        sub = self._subscriptions.get(msg.sid)
        if sub is None:
            return
        await sub.add_msg(core_msg)

    async def dispatch_hmsg(self, msg: HMsg) -> None:
        core_msg = self._build_core_msg(msg)
        if msg.is_request_inbox:
            return await self._dispatch_to_inbox(core_msg, msg.inbox_id)
        sub = self._subscriptions.get(msg.sid)
        if sub is None:
            return
        await sub.add_msg(core_msg)

    async def close(self) -> None:
        for sub in self._subscriptions.values():
            await sub.unsubscribe()
        for fut in self._inboxes.values():
            if not fut.done() or not fut.cancelled():
                fut.cancel()
        self._subscriptions.clear()
        self._inboxes.clear()
