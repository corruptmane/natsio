"""Adapter for nats-core (the official new beta — ``nats.client``).

nats-core 0.2.0 is a **core-only** client: it ships pub/sub, request/reply, and
flush, but no JetStream, KV, or Object-Store surface (JetStream appears only as
a server-capability flag). So this adapter declares just ``Capability.CORE`` and
every JetStream/KV/Object-Store scenario auto-skips for it — reported ``n/s``.

Delivery is driven idiomatically through the subscription's async iterator in a
reader task (matching the one queue-hop that natsio's and nats-py's callback
paths also incur), with pending limits disabled so a burst is never dropped.
"""

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING, ClassVar

import nats.client as natscore

from natsio_bench.adapters.base import Adapter, BenchSub, Capability, MsgCallback
from natsio_bench.adapters.util import maybe_await

if TYPE_CHECKING:
    from nats.client import Client, Subscription

__all__ = ["NatsCoreAdapter"]


class _NatsCoreSub(BenchSub):
    def __init__(self, sub: "Subscription", task: asyncio.Task[None]) -> None:
        self._sub = sub
        self._task = task

    async def unsubscribe(self) -> None:
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        await self._sub.unsubscribe()


class NatsCoreAdapter(Adapter):
    name: ClassVar[str] = "nats-core"
    capabilities: ClassVar[frozenset[Capability]] = frozenset({Capability.CORE})

    def __init__(self) -> None:
        self._nc: Client | None = None

    @property
    def client(self) -> "Client":
        assert self._nc is not None, "adapter is not connected"
        return self._nc

    async def connect(self, url: str) -> None:
        self._nc = await natscore.connect(url)

    async def publish(self, subject: str, payload: bytes) -> None:
        await self.client.publish(subject, payload)

    async def subscribe(self, subject: str, cb: MsgCallback) -> BenchSub:
        # None disables the pending-message / pending-byte caps (subscribe()
        # defaults are 65536 / 64MiB) so a fast delivery burst is never dropped.
        sub = await self.client.subscribe(subject, max_pending_messages=None, max_pending_bytes=None)

        async def reader() -> None:
            async for msg in sub:
                await maybe_await(cb(msg))

        task = asyncio.create_task(reader(), name=f"natscore-sub-{subject}")
        return _NatsCoreSub(sub, task)

    async def request(self, subject: str, payload: bytes, timeout: float) -> bytes:  # noqa: ASYNC109
        msg = await self.client.request(subject, payload, timeout=timeout)
        return msg.data

    async def flush(self) -> None:
        await self.client.flush()

    async def close(self) -> None:
        if self._nc is not None:
            await self._nc.close()
            self._nc = None
