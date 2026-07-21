"""The client-adapter contract.

One adapter wraps one NATS client behind a uniform, *idiomatic* surface: every
method is implemented with the client's own intended fast path, not a
lowest-common-denominator shim. An adapter advertises the :class:`Capability`
set it can honour; the runner never invokes a method whose capability the
adapter did not declare, so unsupported operations are skipped, never faked.

The subscribe callback receives the client's **native** message object. All
three clients happen to expose ``.data`` (payload bytes) and ``.reply`` (reply
subject) on that object, so scenarios read those two attributes uniformly with
zero wrapper allocation per message — the fairest possible hot path.
"""

from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import Any, ClassVar

__all__ = ["Adapter", "BenchSub", "Capability", "MsgCallback"]

# A scenario callback: handed the native message, may be sync or async. The
# adapter awaits the return value when it is a coroutine, so a responder can
# `async def cb(msg): await adapter.publish(msg.reply, msg.data)` and a counter
# can stay synchronous for the lowest possible per-message cost.
type MsgCallback = Callable[[Any], Awaitable[None] | None]


class Capability(StrEnum):
    """A family of operations an adapter may support."""

    CORE = "core"
    JETSTREAM = "jetstream"
    KV = "kv"
    OBJECT_STORE = "object_store"


class BenchSub:
    """Opaque subscription handle: the one thing a scenario does with it is close it."""

    async def unsubscribe(self) -> None:  # pragma: no cover - overridden
        raise NotImplementedError


class Adapter:
    """One NATS client, driven idiomatically.

    Concrete adapters override the core methods (always) and whichever JetStream
    / KV / Object-Store methods their ``capabilities`` advertise. The unsupported
    defaults raise, but capability gating in the runner means they are never
    reached — they exist only to keep a single, ty-checkable interface.
    """

    name: ClassVar[str]
    capabilities: ClassVar[frozenset[Capability]]

    def supports(self, capability: Capability) -> bool:
        return capability in self.capabilities

    # -- core ----------------------------------------------------------------

    async def connect(self, url: str) -> None:
        raise NotImplementedError

    async def publish(self, subject: str, payload: bytes) -> None:
        raise NotImplementedError

    async def subscribe(self, subject: str, cb: MsgCallback) -> BenchSub:
        raise NotImplementedError

    async def request(self, subject: str, payload: bytes, timeout: float) -> bytes:  # noqa: ASYNC109
        raise NotImplementedError

    async def flush(self) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    # -- jetstream: publish / consume ----------------------------------------

    async def js_create_stream(self, name: str, subjects: list[str]) -> None:
        raise NotImplementedError

    async def js_publish(self, subject: str, payload: bytes) -> None:
        """Publish one message and await its PubAck (the durable, awaited path)."""
        raise NotImplementedError

    async def js_publish_async(self, subject: str, payload: bytes) -> None:
        """Enqueue a publish into the client's in-flight ack window (fire, don't wait)."""
        raise NotImplementedError

    async def js_publish_async_complete(self) -> None:
        """Block until every outstanding async publish has been acked."""
        raise NotImplementedError

    async def js_consumer(self, stream: str, subject: str) -> Any:
        """Create a durable pull consumer and return an opaque handle to fetch from.

        Split from :meth:`js_fetch` so a scenario can build the consumer (a
        control-plane round-trip) *outside* its timed window and time only the
        message draining."""
        raise NotImplementedError

    async def js_fetch(self, consumer: Any, n: int) -> int:
        """Fetch and ack up to ``n`` messages from a prepared consumer; return how many drained."""
        raise NotImplementedError

    # -- key-value -----------------------------------------------------------

    async def kv_create(self, bucket: str) -> None:
        raise NotImplementedError

    async def kv_put(self, key: str, value: bytes) -> None:
        raise NotImplementedError

    async def kv_get(self, key: str) -> bytes:
        raise NotImplementedError

    # -- object store --------------------------------------------------------

    async def os_create(self, bucket: str) -> None:
        raise NotImplementedError

    async def os_put(self, name: str, data: bytes) -> None:
        raise NotImplementedError

    async def os_get(self, name: str) -> bytes:
        raise NotImplementedError
