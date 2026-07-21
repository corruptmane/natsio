"""Adapter for nats-py (the incumbent asyncio client, 2.x line).

Uses its intended fast paths: ``publish`` (which itself schedules a socket
flush when the flush queue is idle) plus an explicit ``flush`` PING/PONG at the
stop-clock, the semaphore-bounded ``publish_async`` window with
``publish_async_completed``, and ``pull_subscribe`` + ``fetch`` batches. KV is
created with ``direct=True`` so its reads use the same Direct-Get path natsio's
KV uses — an apples-to-apples get.
"""

from typing import TYPE_CHECKING, Any, ClassVar

import nats
from nats.js import api

from natsio_bench.adapters.base import Adapter, BenchSub, Capability, MsgCallback
from natsio_bench.adapters.util import maybe_await, unique

if TYPE_CHECKING:
    from nats.aio.client import Client
    from nats.aio.subscription import Subscription
    from nats.js import JetStreamContext
    from nats.js.kv import KeyValue
    from nats.js.object_store import ObjectStore

__all__ = ["NatsPyAdapter"]

_FETCH_BATCH = 256


class _NatsPySub(BenchSub):
    def __init__(self, sub: "Subscription") -> None:
        self._sub = sub

    async def unsubscribe(self) -> None:
        await self._sub.unsubscribe()


class NatsPyAdapter(Adapter):
    name: ClassVar[str] = "nats-py"
    capabilities: ClassVar[frozenset[Capability]] = frozenset(Capability)

    def __init__(self) -> None:
        self._nc: Client | None = None
        self._js: JetStreamContext | None = None
        self._kv: KeyValue | None = None
        self._obj: ObjectStore | None = None

    @property
    def client(self) -> "Client":
        assert self._nc is not None, "adapter is not connected"
        return self._nc

    def _jetstream(self) -> "JetStreamContext":
        if self._js is None:
            self._js = self.client.jetstream()
        return self._js

    # -- core ----------------------------------------------------------------

    async def connect(self, url: str) -> None:
        self._nc = await nats.connect(url)

    async def publish(self, subject: str, payload: bytes) -> None:
        await self.client.publish(subject, payload)

    async def subscribe(self, subject: str, cb: MsgCallback) -> BenchSub:
        # nats-py always invokes the subscription callback as a coroutine from a
        # reader task, so wrap to honour a possibly-sync MsgCallback. Limits of 0
        # make the pending queue unbounded and disable the byte-based slow-consumer
        # drop (client.py:2012/2021) — no dropped deliveries during a measured burst.
        async def handler(msg: Any) -> None:
            await maybe_await(cb(msg))

        sub = await self.client.subscribe(subject, cb=handler, pending_msgs_limit=0, pending_bytes_limit=0)
        return _NatsPySub(sub)

    async def request(self, subject: str, payload: bytes, timeout: float) -> bytes:  # noqa: ASYNC109
        msg = await self.client.request(subject, payload, timeout=timeout)
        return msg.data

    async def flush(self) -> None:
        await self.client.flush()

    async def close(self) -> None:
        if self._nc is not None:
            await self._nc.close()
            self._nc = None

    # -- jetstream -----------------------------------------------------------

    async def js_create_stream(self, name: str, subjects: list[str]) -> None:
        await self._jetstream().add_stream(name=name, subjects=subjects, storage=api.StorageType.FILE)

    async def js_publish(self, subject: str, payload: bytes) -> None:
        await self._jetstream().publish(subject, payload)

    async def js_publish_async(self, subject: str, payload: bytes) -> None:
        await self._jetstream().publish_async(subject, payload)

    async def js_publish_async_complete(self) -> None:
        await self._jetstream().publish_async_completed()

    async def js_consumer(self, stream: str, subject: str) -> "JetStreamContext.PullSubscription":
        return await self._jetstream().pull_subscribe(subject, durable=unique("bench"), stream=stream)

    async def js_fetch(self, consumer: "JetStreamContext.PullSubscription", n: int) -> int:
        consumed = 0
        while consumed < n:
            batch = await consumer.fetch(min(_FETCH_BATCH, n - consumed), timeout=5.0)
            if not batch:
                break
            for msg in batch:
                await msg.ack()
            consumed += len(batch)
        return consumed

    # -- key-value -----------------------------------------------------------

    async def kv_create(self, bucket: str) -> None:
        self._kv = await self._jetstream().create_key_value(
            config=api.KeyValueConfig(bucket=bucket, history=1, storage=api.StorageType.FILE, direct=True)
        )

    async def kv_put(self, key: str, value: bytes) -> None:
        assert self._kv is not None
        await self._kv.put(key, value)

    async def kv_get(self, key: str) -> bytes:
        assert self._kv is not None
        entry = await self._kv.get(key)
        assert entry.value is not None
        return entry.value

    # -- object store --------------------------------------------------------

    async def os_create(self, bucket: str) -> None:
        self._obj = await self._jetstream().create_object_store(
            bucket=bucket, config=api.ObjectStoreConfig(bucket=bucket, storage=api.StorageType.FILE)
        )

    async def os_put(self, name: str, data: bytes) -> None:
        assert self._obj is not None
        await self._obj.put(name, data)

    async def os_get(self, name: str) -> bytes:
        assert self._obj is not None
        result = await self._obj.get(name)
        assert result.data is not None
        return result.data
