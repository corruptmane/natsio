"""Adapter for natsio (the client under test).

Driven through its intended fast paths: buffered ``publish`` + PING/PONG
``flush``, the muxed-inbox ``request``, the ``publish_async`` ack window with
``publish_async_complete``, and pull-consumer ``fetch`` batches. natsio's
``subscribe`` is synchronous by design (the SUB frame is buffered and takes
effect immediately), so no message can slip between subscribe and consume.
"""

from typing import ClassVar

import natsio
from natsio import Client, Subscription
from natsio.jetstream import (
    Consumer,
    ConsumerConfig,
    JetStreamContext,
    StorageType,
    StreamConfig,
)
from natsio.kv import KeyValue, KeyValueConfig
from natsio.objectstore import ObjectStore, ObjectStoreConfig
from natsio_bench.adapters.base import Adapter, BenchSub, Capability, MsgCallback
from natsio_bench.adapters.util import unique

__all__ = ["NatsioAdapter"]

_FETCH_BATCH = 256


class _NatsioSub(BenchSub):
    def __init__(self, sub: Subscription) -> None:
        self._sub = sub

    async def unsubscribe(self) -> None:
        await self._sub.unsubscribe()


class NatsioAdapter(Adapter):
    name: ClassVar[str] = "natsio"
    capabilities: ClassVar[frozenset[Capability]] = frozenset(Capability)

    def __init__(self) -> None:
        self._nc: Client | None = None
        self._js: JetStreamContext | None = None
        self._kv: KeyValue | None = None
        self._obj: ObjectStore | None = None

    @property
    def client(self) -> Client:
        assert self._nc is not None, "adapter is not connected"
        return self._nc

    def _jetstream(self) -> JetStreamContext:
        if self._js is None:
            self._js = self.client.jetstream()
        return self._js

    # -- core ----------------------------------------------------------------

    async def connect(self, url: str) -> None:
        self._nc = await natsio.connect(url)

    async def publish(self, subject: str, payload: bytes) -> None:
        await self.client.publish(subject, payload)

    async def subscribe(self, subject: str, cb: MsgCallback) -> BenchSub:
        # natsio accepts a callback returning Awaitable|None and awaits it in a
        # per-subscription reader task — exactly our MsgCallback contract, so the
        # scenario callback is passed straight through. Generous pending limits
        # (0 == unlimited) so a fast delivery burst is never dropped mid-measure.
        sub = self.client.subscribe(subject, cb=cb, pending_msgs_limit=0, pending_bytes_limit=0)
        return _NatsioSub(sub)

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
        await self._jetstream().create_stream(StreamConfig(name=name, subjects=subjects, storage=StorageType.FILE))

    async def js_publish(self, subject: str, payload: bytes) -> None:
        await self._jetstream().publish(subject, payload)

    async def js_publish_async(self, subject: str, payload: bytes) -> None:
        await self._jetstream().publish_async(subject, payload)

    async def js_publish_async_complete(self) -> None:
        await self._jetstream().publish_async_complete()

    async def js_consumer(self, stream: str, subject: str) -> Consumer:
        handle = await self._jetstream().stream(stream)
        return await handle.create_consumer(ConsumerConfig(durable_name=unique("bench")))

    async def js_fetch(self, consumer: Consumer, n: int) -> int:
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
            KeyValueConfig(bucket=bucket, history=1, storage=StorageType.FILE)
        )

    async def kv_put(self, key: str, value: bytes) -> None:
        assert self._kv is not None
        await self._kv.put(key, value)

    async def kv_get(self, key: str) -> bytes:
        assert self._kv is not None
        entry = await self._kv.get(key)
        return entry.value

    # -- object store --------------------------------------------------------

    async def os_create(self, bucket: str) -> None:
        self._obj = await self._jetstream().create_object_store(
            ObjectStoreConfig(bucket=bucket, storage=StorageType.FILE)
        )

    async def os_put(self, name: str, data: bytes) -> None:
        assert self._obj is not None
        await self._obj.put(name, data)

    async def os_get(self, name: str) -> bytes:
        assert self._obj is not None
        return await self._obj.get_bytes(name)
