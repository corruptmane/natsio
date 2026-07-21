"""Subscriptions: bounded delivery queues, backpressure policy, consumption modes.

A subscription is fed synchronously from the connection's read path, so
delivery must never block there. Everything that can block — user callbacks,
async iteration — happens in the consumer's own task, pulling from the queue.
When the pending limits are exceeded the configured :class:`PendingLimitPolicy`
decides what gives, and every policy is loud: drops are counted and reported
through the client's error callback.

Termination design: closure is signalled by an :class:`asyncio.Event` latch,
never by an in-band queue sentinel. A sentinel can be dropped when the queue is
full, is consumed by only one of several waiters, and blocks the closer when
nobody is reading — an Event has none of those failure modes. The queue itself
is unbounded; the configured limits are enforced by explicit counters, which
lets the BLOCK policy admit the in-flight burst instead of dropping it.

Cancellation is never suppressed here. ``Client.drain()`` bounds the whole
drain with ``asyncio.timeout``, which works by cancelling this task — eating
that cancellation would make the deadline unenforceable.
"""

import asyncio
import builtins
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from enum import Enum
from types import TracebackType
from typing import TYPE_CHECKING, Self

from natsio._internal.dispatcher import SubscriptionEntry
from natsio._internal.protocol import StatusCode
from natsio.errors import NATSError, NoRespondersError, SlowConsumerError, SubscriptionClosedError
from natsio.errors import TimeoutError as NATSTimeoutError
from natsio.message import Msg

if TYPE_CHECKING:
    from natsio.client import Client

__all__ = ["PendingLimitPolicy", "Subscription"]

type Callback = Callable[[Msg], Awaitable[None] | None]


class PendingLimitPolicy(Enum):
    """What to do when a subscription's pending limits are exceeded."""

    DROP_NEW = "drop_new"
    """Discard the arriving message (default). Counted and reported."""

    DROP_OLD = "drop_old"
    """Discard the oldest queued messages to make room (last-value-wins)."""

    BLOCK = "block"
    """Stop reading from the socket until the consumer catches up.

    Nothing is dropped: messages already parsed from the in-flight segment are
    admitted beyond the limit (memory may briefly exceed it by one read burst),
    and the socket stays paused until the consumer drains below the limits.
    Under sustained pressure the *server* eventually declares this client a
    slow consumer instead of the client silently losing messages.
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
        "_callback_active",
        "_client",
        "_closed",
        "_closed_event",
        "_dropped",
        "_entry",
        "_failure",
        "_idle",
        "_last_drop_report",
        "_pending_bytes",
        "_pending_bytes_limit",
        "_pending_msgs",
        "_pending_msgs_limit",
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
        self._callback_active = False
        self._policy = policy
        self._pending_msgs_limit = pending_msgs_limit
        self._pending_bytes_limit = pending_bytes_limit
        self._pending_bytes = 0
        self._pending_msgs = 0
        self._dropped = 0
        self._last_drop_report: float | None = None
        self._closed = False
        self._failure: Exception | None = None
        self._reading_paused = False
        # Unbounded on purpose: limits are enforced by the counters above.
        self._queue: asyncio.Queue[Msg] = asyncio.Queue()
        self._closed_event = asyncio.Event()
        self._idle = asyncio.Event()  # set <=> queue empty and no callback in flight
        self._idle.set()
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
        if self._closed or self._failure is not None:
            return
        size = len(msg.payload)
        if self._policy is PendingLimitPolicy.BLOCK:
            # Admit unconditionally (the burst is already parsed), then pause
            # the socket so no further bytes arrive until we drain.
            self._enqueue(msg, size)
            if self._at_limit():
                self._pause_reading()
            return
        if self._would_exceed(size):
            self._overflow(msg, size)
            return
        self._enqueue(msg, size)

    def _enqueue(self, msg: Msg, size: int) -> None:
        self._queue.put_nowait(msg)
        self._pending_bytes += size
        self._pending_msgs += 1
        self._idle.clear()

    def _would_exceed(self, size: int) -> bool:
        if self._pending_msgs_limit > 0 and self._pending_msgs >= self._pending_msgs_limit:
            return True
        return self._pending_bytes_limit > 0 and self._pending_bytes + size > self._pending_bytes_limit

    def _at_limit(self) -> bool:
        if self._pending_msgs_limit > 0 and self._pending_msgs >= self._pending_msgs_limit:
            return True
        return self._pending_bytes_limit > 0 and self._pending_bytes >= self._pending_bytes_limit

    def _overflow(self, msg: Msg, size: int) -> None:
        match self._policy:
            case PendingLimitPolicy.DROP_NEW:
                self._record_drop(1)
            case PendingLimitPolicy.DROP_OLD:
                if self._pending_bytes_limit > 0 and size > self._pending_bytes_limit:
                    # The arriving message alone exceeds the byte budget:
                    # admitting it would leave the subscription permanently
                    # over limit. Drop it, keep what is queued.
                    self._record_drop(1)
                    return
                evicted = 0
                while self._would_exceed(size):
                    try:
                        old = self._queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    self._queue.task_done()
                    self._pending_bytes -= len(old.payload)
                    self._pending_msgs -= 1
                    evicted += 1
                self._record_drop(evicted)
                self._enqueue(msg, size)
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
            case PendingLimitPolicy.BLOCK:  # pragma: no cover - BLOCK never routes here
                self._enqueue(msg, size)

    def _record_drop(self, count: int) -> None:
        if count <= 0:
            return
        self._dropped += count
        self._client._on_slow_consumer(self, count)

    def _fail(self, error: Exception) -> None:
        if self._failure is None:
            self._failure = error
        self._closed_event.set()

    # -- backpressure --------------------------------------------------------

    def _pause_reading(self) -> None:
        if not self._reading_paused:
            self._reading_paused = True
            self._client._pause_reading(self.sid)

    def _resume_reading_if_drained(self) -> None:
        if self._reading_paused and not self._at_limit():
            self._reading_paused = False
            self._client._resume_reading(self.sid)

    def _release_pause(self) -> None:
        if self._reading_paused:
            self._reading_paused = False
            self._client._resume_reading(self.sid)

    # -- consumption ---------------------------------------------------------

    def _start_callback_reader(self) -> None:
        self._reader = self._client._spawn(self._callback_loop(), name=f"natsio-sub-{self.sid}")

    async def _callback_loop(self) -> None:
        assert self._callback is not None
        while True:
            msg = await self._next_or_none()
            if msg is None:
                return
            self._callback_active = True
            try:
                result = self._callback(msg)
                if result is not None:
                    await result
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._client._on_callback_error(self, exc)
            finally:
                self._callback_active = False
                self._maybe_idle()

    async def _next_or_none(self) -> Msg | None:
        """Next queued message, or None once the subscription is finished.

        Queued messages are always served first — closure only ends the stream
        after the backlog (e.g. the tail of an auto-unsubscribe) is consumed.
        Waiting is a race between the queue and the closed latch, so any number
        of concurrent consumers all wake on closure and none can be stranded.
        """
        while True:
            try:
                msg = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                if self._closed or self._failure is not None:
                    return None
                getter = asyncio.ensure_future(self._queue.get())
                closer = asyncio.ensure_future(self._closed_event.wait())
                try:
                    done, _ = await asyncio.wait((getter, closer), return_when=asyncio.FIRST_COMPLETED)
                finally:
                    getter.cancel()
                    closer.cancel()
                if getter in done and not getter.cancelled():
                    msg = getter.result()
                else:
                    continue  # woken by closure; re-check the queue for a backlog
            self._queue.task_done()
            self._pending_bytes -= len(msg.payload)
            self._pending_msgs -= 1
            if not self._callback_active:
                self._maybe_idle()
            self._resume_reading_if_drained()
            return msg

    def _maybe_idle(self) -> None:
        if self._pending_msgs == 0 and not self._callback_active:
            self._idle.set()

    def __aiter__(self) -> AsyncIterator[Msg]:
        if self._callback is not None:
            raise SubscriptionClosedError("subscription is in callback mode; it cannot also be iterated")
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Msg]:
        while True:
            if self._failure is not None:
                raise self._failure
            msg = await self._next_or_none()
            if msg is None:
                if self._failure is not None:
                    raise self._failure
                return
            yield msg

    async def next_msg(self, timeout: float | None = None) -> Msg:  # noqa: ASYNC109
        """Await the next message. Raises :class:`~natsio.errors.TimeoutError` on expiry."""
        if self._callback is not None:
            raise SubscriptionClosedError("subscription is in callback mode; use the callback, not next_msg()")
        if self._failure is not None:
            raise self._failure
        try:
            async with asyncio.timeout(timeout):
                msg = await self._next_or_none()
        except builtins.TimeoutError:
            raise NATSTimeoutError(f"no message on {self.subject!r} within {timeout}s") from None
        if msg is None:
            if self._failure is not None:
                raise self._failure
            raise SubscriptionClosedError(f"subscription on {self.subject!r} is closed")
        if msg.status is not None and msg.status.code == StatusCode.NO_RESPONDERS and not msg.payload:
            raise NoRespondersError(f"no responders listening on {self.subject!r}")
        return msg

    # -- teardown ------------------------------------------------------------

    async def unsubscribe(self) -> None:
        """Stop delivery immediately and discard anything still queued."""
        if self._closed:
            return
        self._closed = True
        self._client._remove_subscription(self, max_msgs=None)
        self._discard_backlog()
        self._closed_event.set()
        await self._finalize(cancel_reader=True)

    async def unsubscribe_after(self, max_msgs: int) -> None:
        """Ask the server to stop delivery after ``max_msgs`` total messages.

        The count is total deliveries since the subscription was created (the
        server's ``UNSUB <sid> <max>`` contract). When the limit is reached the
        subscription closes itself: iterators finish, ``next_msg`` raises
        :class:`~natsio.errors.SubscriptionClosedError`.
        """
        if self._closed:
            return
        if max_msgs <= 0:
            await self.unsubscribe()
            return
        self._client._remove_subscription(self, max_msgs=max_msgs)

    async def drain(self) -> None:
        """Stop new delivery, wait for the backlog to be handled, then close.

        Unbounded by itself — bound it with :meth:`Client.drain`'s
        ``drain_timeout`` or your own ``asyncio.timeout``; cancellation is
        honored, never swallowed.
        """
        if self._closed:
            return
        self._client._remove_subscription(self, max_msgs=None)
        # Suppressed: when not connected there is nothing in flight to wait for.
        with suppress(NATSError):
            await self._client.flush()  # make sure the UNSUB reached the server
        # Draining from inside the callback: the in-flight message IS the last
        # one, and _idle can never be set while this callback is running.
        if self._reader is not asyncio.current_task():
            await self._idle.wait()
        self._closed = True
        self._closed_event.set()
        await self._finalize(cancel_reader=False)

    async def _finalize(self, *, cancel_reader: bool) -> None:
        self._release_pause()
        reader = self._reader
        self._reader = None
        # Never cancel/join the reader from within itself (teardown invoked from
        # inside the callback): doing so would destroy the callback's own
        # continuation. The callback returns normally and _callback_loop exits
        # on its next iteration once _closed is observed.
        if reader is not None and reader is not asyncio.current_task() and not reader.done():
            if cancel_reader:
                reader.cancel()
            # asyncio.wait neither raises the reader's exception nor swallows
            # OUR cancellation — a drain deadline still cuts this short.
            await asyncio.wait((reader,))

    def _discard_backlog(self) -> None:
        while True:
            try:
                msg = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._queue.task_done()
            self._pending_bytes -= len(msg.payload)
            self._pending_msgs -= 1
        self._maybe_idle()

    def _close_local(self) -> None:
        """Mark closed without talking to the server (client shutting down)."""
        self._closed = True
        self._closed_event.set()
        self._release_pause()

    def _complete_local(self) -> None:
        """Auto-unsubscribe retired server-side: finish after the backlog drains."""
        self._closed = True
        self._closed_event.set()
        self._release_pause()
        self._client._subscriptions.pop(self.sid, None)

    def _fail_permanent(self, error: Exception) -> None:
        """Terminate with an error: parked consumers raise it, the sub is closed.

        Used when the server denies this subscription (permission violation) and
        ``permission_err_on_subscribe`` is set. Runs on the read path — must not
        block. Setting ``_failure`` makes ``next_msg`` / iteration raise ``error``
        (including on every subsequent call), and the ``_closed_event`` latch
        wakes any consumer already parked in ``_next_or_none``.
        """
        if self._failure is None:
            self._failure = error
        self._closed = True
        self._closed_event.set()
        self._release_pause()
        self._client._subscriptions.pop(self.sid, None)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.unsubscribe()
