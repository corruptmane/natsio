"""Subscriptions: bounded delivery queues, backpressure policy, consumption modes.

A subscription is fed synchronously from the connection's read path, so
delivery must never block there. Everything that can block — user callbacks,
async iteration — happens in the consumer's own task, pulling from a bounded
queue. When the queue is full the configured :class:`PendingLimitPolicy`
decides what gives, and every policy is loud: drops are counted and reported
through the client's error callback.
"""

import asyncio
import builtins
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from enum import Enum
from types import TracebackType
from typing import TYPE_CHECKING, Self

from natsio._internal.dispatcher import SubscriptionEntry
from natsio.errors import SlowConsumerError, SubscriptionClosedError
from natsio.errors import TimeoutError as NATSTimeoutError
from natsio.message import Msg

if TYPE_CHECKING:
    from natsio.client import Client

__all__ = ["PendingLimitPolicy", "Subscription"]

type Callback = Callable[[Msg], Awaitable[None] | None]


class PendingLimitPolicy(Enum):
    """What to do when a subscription's pending queue is full."""

    DROP_NEW = "drop_new"
    """Discard the arriving message (default). Counted and reported."""

    DROP_OLD = "drop_old"
    """Discard the oldest queued message to make room (last-value-wins)."""

    BLOCK = "block"
    """Stop reading from the socket until the consumer catches up.

    Applies connection-wide backpressure — the server will eventually declare
    this client a slow consumer rather than any message being lost.
    """

    ERROR = "error"
    """Treat overflow as fatal for this subscription: stop delivering and
    raise :class:`~natsio.errors.SlowConsumerError` to the consumer."""


class Subscription:
    """An active subscription.

    Consume it as an async iterator (the default), or pass ``cb=`` to
    :meth:`Client.subscribe` to have messages handed to a callback instead.
    Usable as an async context manager, which unsubscribes on exit.
    """

    __slots__ = (
        "_callback",
        "_client",
        "_closed",
        "_dropped",
        "_entry",
        "_failure",
        "_pending_bytes",
        "_pending_bytes_limit",
        "_pending_msgs",
        "_policy",
        "_queue",
        "_reader",
        "_reading_paused",
    )

    def __init__(
        self,
        client: "Client",
        entry: SubscriptionEntry,
        *,
        callback: Callback | None = None,
        pending_msgs_limit: int,
        pending_bytes_limit: int,
        policy: PendingLimitPolicy,
    ) -> None:
        self._client = client
        self._entry = entry
        self._callback = callback
        self._policy = policy
        self._pending_bytes_limit = pending_bytes_limit
        self._pending_bytes = 0
        self._pending_msgs = 0
        self._dropped = 0
        self._closed = False
        self._failure: Exception | None = None
        self._reading_paused = False
        self._queue: asyncio.Queue[Msg | None] = asyncio.Queue(maxsize=pending_msgs_limit)
        self._reader: asyncio.Task[None] | None = None

    # -- identity / stats ----------------------------------------------------

    @property
    def sid(self) -> int:
        return self._entry.sid

    @property
    def subject(self) -> str:
        return self._entry.subject

    @property
    def queue_group(self) -> str | None:
        return self._entry.queue

    @property
    def delivered(self) -> int:
        """Messages handed to this subscription by the connection."""
        return self._entry.delivered

    @property
    def dropped(self) -> int:
        """Messages discarded because the pending limits were exceeded."""
        return self._dropped

    @property
    def pending_msgs(self) -> int:
        return self._pending_msgs

    @property
    def pending_bytes(self) -> int:
        return self._pending_bytes

    @property
    def is_closed(self) -> bool:
        return self._closed

    def __repr__(self) -> str:
        return (
            f"Subscription(sid={self.sid}, subject={self.subject!r}, "
            f"queue={self.queue_group!r}, pending={self.pending_msgs})"
        )

    # -- delivery (called synchronously from the read path) ------------------

    def _deliver(self, msg: Msg) -> None:
        if self._closed:
            return
        size = len(msg.payload)
        over_bytes = self._pending_bytes_limit > 0 and self._pending_bytes + size > self._pending_bytes_limit
        if over_bytes or self._queue.full():
            self._overflow(msg)
            return
        self._queue.put_nowait(msg)
        self._pending_bytes += size
        self._pending_msgs += 1
        if self._policy is PendingLimitPolicy.BLOCK and self._at_high_water():
            self._pause_reading()

    def _overflow(self, msg: Msg) -> None:
        match self._policy:
            case PendingLimitPolicy.DROP_NEW:
                self._record_drop(1)
            case PendingLimitPolicy.DROP_OLD:
                evicted = 0
                while True:
                    try:
                        old = self._queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    self._queue.task_done()
                    if old is not None:
                        self._pending_bytes -= len(old.payload)
                        self._pending_msgs -= 1
                    evicted += 1
                    if not self._queue.full() and not self._over_byte_limit(len(msg.payload)):
                        break
                self._record_drop(evicted)
                if not self._queue.full():
                    self._queue.put_nowait(msg)
                    self._pending_bytes += len(msg.payload)
                    self._pending_msgs += 1
            case PendingLimitPolicy.BLOCK:
                # The queue is full and reading is paused; the connection stops
                # draining the socket rather than dropping anything.
                self._pause_reading()
                self._record_drop(1)
            case PendingLimitPolicy.ERROR:
                self._record_drop(1)
                self._fail(
                    SlowConsumerError(
                        f"subscription on {self.subject!r} exceeded its pending limits",
                        subject=self.subject,
                        sid=self.sid,
                        dropped=self._dropped,
                    )
                )

    def _over_byte_limit(self, size: int) -> bool:
        return self._pending_bytes_limit > 0 and self._pending_bytes + size > self._pending_bytes_limit

    def _at_high_water(self) -> bool:
        if self._queue.maxsize and self._pending_msgs >= self._queue.maxsize:
            return True
        return self._over_byte_limit(0)

    def _record_drop(self, count: int) -> None:
        if count <= 0:
            return
        self._dropped += count
        self._client._on_slow_consumer(self, count)

    def _fail(self, error: Exception) -> None:
        if self._failure is None:
            self._failure = error
        self._wake_consumers()

    def _wake_consumers(self) -> None:
        with suppress(asyncio.QueueFull):
            self._queue.put_nowait(None)  # sentinel: stop iteration / reader

    def _pause_reading(self) -> None:
        if not self._reading_paused:
            self._reading_paused = True
            self._client._pause_reading()

    def _resume_reading_if_drained(self) -> None:
        if self._reading_paused and not self._at_high_water():
            self._reading_paused = False
            self._client._resume_reading()

    # -- consumption ---------------------------------------------------------

    def _start_callback_reader(self) -> None:
        self._reader = self._client._spawn(self._callback_loop(), name=f"natsio-sub-{self.sid}")

    async def _callback_loop(self) -> None:
        assert self._callback is not None
        while True:
            msg = await self._next_or_none()
            if msg is None:
                return
            try:
                result = self._callback(msg)
                if result is not None:
                    await result
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._client._on_callback_error(self, exc)

    async def _next_or_none(self) -> Msg | None:
        msg = await self._queue.get()
        self._queue.task_done()
        if msg is None:
            return None
        self._pending_bytes -= len(msg.payload)
        self._pending_msgs -= 1
        self._resume_reading_if_drained()
        return msg

    def __aiter__(self) -> AsyncIterator[Msg]:
        if self._callback is not None:
            raise SubscriptionClosedError("subscription is in callback mode; it cannot also be iterated")
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Msg]:
        while True:
            if self._failure is not None:
                raise self._failure
            if self._closed and self._queue.empty():
                return
            msg = await self._next_or_none()
            if msg is None:
                if self._failure is not None:
                    raise self._failure
                return
            yield msg

    async def next_msg(self, timeout: float | None = None) -> Msg:  # noqa: ASYNC109
        """Await the next message. Raises :class:`~natsio.errors.TimeoutError` on expiry."""
        if self._failure is not None:
            raise self._failure
        if self._closed and self._queue.empty():
            raise SubscriptionClosedError(f"subscription on {self.subject!r} is closed")
        try:
            async with asyncio.timeout(timeout):
                msg = await self._next_or_none()
        except builtins.TimeoutError:
            raise NATSTimeoutError(f"no message on {self.subject!r} within {timeout}s") from None
        if msg is None:
            if self._failure is not None:
                raise self._failure
            raise SubscriptionClosedError(f"subscription on {self.subject!r} is closed")
        return msg

    # -- teardown ------------------------------------------------------------

    async def unsubscribe(self) -> None:
        """Stop delivery immediately and discard anything still queued."""
        if self._closed:
            return
        self._closed = True
        self._client._remove_subscription(self, max_msgs=None)
        await self._shutdown()

    async def unsubscribe_after(self, max_msgs: int) -> None:
        """Ask the server to stop delivery after ``max_msgs`` total messages.

        The count is total deliveries since the subscription was created, which
        is what the server's ``UNSUB <sid> <max>`` contract means.
        """
        if self._closed:
            return
        if max_msgs <= 0:
            await self.unsubscribe()
            return
        self._client._remove_subscription(self, max_msgs=max_msgs)

    async def drain(self) -> None:
        """Stop new delivery, let queued messages be handled, then close."""
        if self._closed:
            return
        self._client._remove_subscription(self, max_msgs=None)
        self._closed = True
        await self._client.flush()  # make sure the UNSUB reached the server
        reader = self._reader
        if reader is not None:
            # Queue the stop sentinel BEHIND the messages already queued, then
            # wait for the reader to work through them and exit on its own.
            # Cancelling here instead would abandon the in-flight callback.
            self._reader = None
            await self._queue.put(None)
            with suppress(asyncio.CancelledError, Exception):
                await reader
        await self._shutdown()

    async def _shutdown(self) -> None:
        self._wake_consumers()
        if self._reading_paused:
            self._reading_paused = False
            self._client._resume_reading()
        reader = self._reader
        self._reader = None
        if reader is not None and not reader.done():
            reader.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await reader

    def _close_local(self) -> None:
        """Mark closed without talking to the server (auto-unsub completed, client closing)."""
        self._closed = True
        self._wake_consumers()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.unsubscribe()
