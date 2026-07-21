"""Pull consumers: fetch(), next(), consume() — and the ordered consumer.

Every pull request gets its own reply token under a consumer-owned inbox, so
replies and status frames are always correlated with the exact request that
caused them — the structural fix for the "fetch stalls forever" family of
bugs. Statuses are classified per ADR-13/ADR-9: 100 heartbeats reset the
liveness timer, 404/408 end a pull quietly, 409 splits into benign
(request-limit, leadership change: re-pull) and fatal (consumer deleted).
"""

import asyncio
import builtins
import json
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from datetime import timedelta

from natsio._internal.dispatcher import SubscriptionEntry
from natsio._internal.nuid import next_nuid
from natsio._internal.protocol import HMsgEvent, MsgEvent
from natsio.errors import ConfigError, ConnectionClosedError
from natsio.message import Msg

if TYPE_CHECKING:
    from .stream import Stream

from .entities import ConsumerConfig, ConsumerInfo, DeliverPolicy
from .errors import ConsumerDeletedError, JetStreamError, NoMessagesError
from .headers import PIN_ID
from .message import JsMsg

__all__ = ["Consumer", "Consumption", "OrderedConsumer"]

_NANOSECOND: int = 1_000_000_000
_FETCH_GRACE = 1.0  # local slack past the server-side `expires`


class _RequestGoneError(Exception):
    """Internal: the current pull ended (quietly or benignly)."""


class Consumer:
    """A handle to a pull consumer. Spawns fetches and consume sessions."""

    __slots__ = ("_pin_ids", "_stream", "cached_info")

    def __init__(self, stream: "Stream", info: ConsumerInfo) -> None:
        self._stream = stream
        self.cached_info = info
        # Latest priority-group pin id (ADR-42 pinned_client), keyed by group.
        # The server pins one client per (consumer, group) and echoes the id on
        # delivered messages; we replay it on subsequent pulls so the server
        # keeps delivering to us, and drop it on a pin-mismatch (423) status.
        self._pin_ids: dict[str, str] = {}

    @property
    def name(self) -> str:
        return self.cached_info.name

    @property
    def stream(self) -> "Stream":
        return self._stream

    def __repr__(self) -> str:
        return f"Consumer(stream={self._stream.name!r}, name={self.name!r})"

    async def info(self) -> ConsumerInfo:
        self.cached_info = await self._stream.consumer_info(self.name)
        return self.cached_info

    async def delete(self) -> None:
        await self._stream.delete_consumer(self.name)

    async def unpin(self, group: str) -> None:
        """Release the pinned client for ``group`` (ADR-42 ``pinned_client``).

        The server drops the current pin; the next pull request from any client
        in the group becomes the new pinned client. Raises
        :class:`ConsumerNotFoundError` if the consumer no longer exists.
        """
        await self._stream._ctx._api_request(
            f"CONSUMER.UNPIN.{self._stream.name}.{self.name}",
            {"group": group},
        )
        self._forget_pin(group)

    def _pull_subject(self) -> str:
        ctx = self._stream._ctx
        return f"{ctx.api_prefix}.CONSUMER.MSG.NEXT.{self._stream.name}.{self.name}"

    # -- priority groups (ADR-42) --------------------------------------------

    def _validate_group(self, group: str | None) -> None:
        """Client-side priority-group check, mirroring nats.go's Consume/Messages.

        A consumer configured with priority groups requires every pull to name
        one of them; a plain consumer rejects a group it can't honour.
        """
        configured = self.cached_info.config.priority_groups
        if configured:
            if group is None:
                raise ConfigError("priority group is required for this consumer")
            if group not in configured:
                raise ConfigError(f"invalid priority group {group!r} (configured: {configured})")
        elif group is not None:
            raise ConfigError("priority group is not supported for this consumer")

    def _pin_id(self, group: str | None) -> str | None:
        return self._pin_ids.get(group) if group is not None else None

    def _remember_pin(self, group: str | None, msg: Msg) -> None:
        if group is None or msg.headers is None:
            return
        pin = msg.headers.get(PIN_ID)
        if pin:
            self._pin_ids[group] = pin

    def _forget_pin(self, group: str | None) -> None:
        if group is not None:
            self._pin_ids.pop(group, None)

    # -- fetch / next --------------------------------------------------------

    async def fetch(
        self,
        max_messages: int = 100,
        *,
        max_bytes: int | None = None,
        timeout: float = 5.0,  # noqa: ASYNC109
        idle_heartbeat: float | None = None,
        no_wait: bool = False,
        group: str | None = None,
        min_pending: int | None = None,
        min_ack_pending: int | None = None,
    ) -> list[JsMsg]:
        """One bounded pull: up to ``max_messages`` within ``timeout`` seconds.

        Returns what arrived when the batch fills, the server ends the request
        (no more messages / request expired), or the deadline passes — an empty
        list is a normal outcome. ``no_wait`` returns only immediately-available
        messages. Raises :class:`ConnectionClosedError` if the connection closes
        mid-fetch with the batch still incomplete.

        ``group`` selects an ADR-42 priority group (required, and validated, when
        the consumer is configured with priority groups). ``min_pending`` /
        ``min_ack_pending`` gate delivery on an ``overflow``-policy consumer:
        the server only serves this request when the consumer has at least that
        many pending messages / unacked messages (server-enforced).
        """
        if max_messages <= 0:
            raise ConfigError("fetch max_messages must be positive")
        if timeout < 0:
            raise ConfigError("fetch timeout must not be negative")
        self._validate_group(group)
        if min_pending is not None and min_pending < 1:
            raise ConfigError("fetch min_pending must be positive")
        if min_ack_pending is not None and min_ack_pending < 1:
            raise ConfigError("fetch min_ack_pending must be positive")
        client = self._stream._ctx.client
        conn = client._conn
        inbox = f"_INBOX.{next_nuid()}"
        queue: asyncio.Queue[Msg] = asyncio.Queue()
        closed = asyncio.Event()

        def handler(event: MsgEvent | HMsgEvent) -> None:
            queue.put_nowait(client._build_msg(event))

        from natsio._internal.lifecycle import Closed

        def on_event(event: object) -> None:
            if isinstance(event, Closed):
                closed.set()

        entry = conn.subscribe(inbox, None, handler)
        unsubscribe_bus = conn.bus.subscribe(on_event)
        try:
            request: dict[str, Any] = {"batch": max_messages}
            if no_wait:
                request["no_wait"] = True
            else:
                request["expires"] = int(timeout * _NANOSECOND)
            if max_bytes is not None:
                request["max_bytes"] = max_bytes
            if idle_heartbeat is not None and not no_wait:
                request["idle_heartbeat"] = int(idle_heartbeat * _NANOSECOND)
            if group is not None:
                request["group"] = group
                pin = self._pin_id(group)
                if pin:
                    request["id"] = pin  # wire field for the pin id is "id"
            if min_pending is not None:
                request["min_pending"] = min_pending
            if min_ack_pending is not None:
                request["min_ack_pending"] = min_ack_pending
            await client.publish(self._pull_subject(), json.dumps(request).encode(), reply=inbox)

            messages: list[JsMsg] = []
            loop = asyncio.get_running_loop()
            deadline = loop.time() + timeout + _FETCH_GRACE
            while len(messages) < max_messages:
                try:
                    msg = queue.get_nowait()
                except asyncio.QueueEmpty:
                    if closed.is_set():
                        # Drain buffered frames first (above); once empty, an
                        # incomplete batch cut short by close is a hard error.
                        raise ConnectionClosedError("connection closed during fetch") from None
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        break
                    wait = remaining if idle_heartbeat is None else min(remaining, idle_heartbeat * 2)
                    getter = asyncio.ensure_future(queue.get())
                    closer = asyncio.ensure_future(closed.wait())
                    try:
                        async with asyncio.timeout(wait):
                            done, _ = await asyncio.wait((getter, closer), return_when=asyncio.FIRST_COMPLETED)
                    except builtins.TimeoutError:
                        break  # deadline or two missed heartbeats: done with what we have
                    finally:
                        getter.cancel()
                        closer.cancel()
                    if getter in done and not getter.cancelled():
                        msg = getter.result()
                    else:
                        continue  # close fired: re-check the queue, then raise
                if msg.status is not None and msg.status.code == 423:
                    self._forget_pin(group)  # stale pin: next fetch re-pulls unpinned
                try:
                    if self._classify(msg):
                        continue  # heartbeat
                except _RequestGoneError:
                    break
                self._remember_pin(group, msg)
                messages.append(JsMsg(msg, client))
            return messages
        finally:
            unsubscribe_bus()
            with suppress(ConnectionClosedError):
                conn.unsubscribe(entry.sid)

    async def next(
        self,
        *,
        timeout: float = 5.0,  # noqa: ASYNC109
        group: str | None = None,
        min_pending: int | None = None,
        min_ack_pending: int | None = None,
    ) -> JsMsg:
        """The next available message, or :class:`NoMessagesError` on expiry.

        ``group`` / ``min_pending`` / ``min_ack_pending`` carry the same ADR-42
        priority-group semantics as :meth:`fetch`.
        """
        messages = await self.fetch(
            1,
            timeout=timeout,
            group=group,
            min_pending=min_pending,
            min_ack_pending=min_ack_pending,
        )
        if not messages:
            raise NoMessagesError(f"no message on consumer {self.name!r} within {timeout}s")
        return messages[0]

    def _classify(self, msg: Msg) -> bool:
        """True → heartbeat (skip). False → data. Raises for terminal statuses."""
        status = msg.status
        if status is None:
            return False
        if status.code == 100:
            return True
        if status.code in (404, 408):
            raise _RequestGoneError
        if status.code == 409:
            if "consumer deleted" in status.description.lower():
                raise ConsumerDeletedError(f"consumer {self.name!r} was deleted")
            if "consumer is push based" in status.description.lower():
                raise JetStreamError(f"consumer {self.name!r} is push based; pull it cannot be")
            raise _RequestGoneError  # request-limit / leadership-change family
        if status.code == 423:
            raise _RequestGoneError  # pin lost; caller re-pulls with a fresh pin
        # Anything else (400 Bad Request, future codes) means the pull itself
        # was rejected. Swallowing it as a heartbeat would make an empty batch
        # indistinguishable from a malformed request — surface it.
        raise JetStreamError(f"pull request on consumer {self.name!r} failed: {status.code} {status.description}")

    # -- consume -------------------------------------------------------------

    def consume(
        self,
        *,
        max_messages: int = 500,
        max_bytes: int | None = None,
        expires: float = 30.0,
        idle_heartbeat: float | None = None,
        threshold: float = 0.5,
        group: str | None = None,
        min_pending: int | None = None,
        min_ack_pending: int | None = None,
    ) -> "Consumption":
        """A continuous, self-refilling message stream (ADR-37 ``consume``).

        Keeps up to ``max_messages`` requested from the server, re-pulling when
        the outstanding count drops below ``threshold`` of the target so the
        buffer never drains to a stall. Use as an async context manager::

            async with consumer.consume() as messages:
                async for msg in messages:
                    await msg.ack()

        ``group`` / ``min_pending`` / ``min_ack_pending`` carry ADR-42 priority-
        group semantics (see :meth:`fetch`); the group, when the consumer has
        priority groups configured, is validated here before the session starts.
        """
        self._validate_group(group)
        if min_pending is not None and min_pending < 1:
            raise ConfigError("consume min_pending must be positive")
        if min_ack_pending is not None and min_ack_pending < 1:
            raise ConfigError("consume min_ack_pending must be positive")
        heartbeat = idle_heartbeat if idle_heartbeat is not None else min(expires / 2, 30.0)
        return Consumption(
            self,
            max_messages=max_messages,
            max_bytes=max_bytes,
            expires=expires,
            idle_heartbeat=heartbeat,
            threshold_msgs=max(1, int(max_messages * threshold)),
            group=group,
            min_pending=min_pending,
            min_ack_pending=min_ack_pending,
        )


class Consumption:
    """One active ``consume()`` session."""

    __slots__ = (
        "_closed",
        "_closed_event",
        "_consumer",
        "_entry",
        "_expires",
        "_failure",
        "_group",
        "_heartbeat",
        "_inbox_base",
        "_last_frame_at",
        "_max_bytes",
        "_max_messages",
        "_min_ack_pending",
        "_min_pending",
        "_pulls",
        "_queue",
        "_rapid_ends",
        "_threshold_msgs",
        "_total_pending",
        "_unsubscribe_bus",
        "_wake",
        "_worker",
    )

    def __init__(
        self,
        consumer: Consumer,
        *,
        max_messages: int,
        max_bytes: int | None,
        expires: float,
        idle_heartbeat: float,
        threshold_msgs: int,
        group: str | None = None,
        min_pending: int | None = None,
        min_ack_pending: int | None = None,
    ) -> None:
        self._consumer = consumer
        self._max_messages = max_messages
        self._max_bytes = max_bytes
        self._expires = expires
        self._heartbeat = idle_heartbeat
        self._threshold_msgs = threshold_msgs
        self._group = group
        self._min_pending = min_pending
        self._min_ack_pending = min_ack_pending
        self._inbox_base = f"_INBOX.{next_nuid()}"
        self._queue: asyncio.Queue[JsMsg] = asyncio.Queue()
        self._pulls: dict[str, tuple[int, float]] = {}  # token -> (still expected, issued at)
        self._total_pending = 0
        self._rapid_ends = 0  # consecutive short-lived, message-less pulls
        self._closed = False
        self._closed_event = asyncio.Event()
        self._failure: Exception | None = None
        self._wake = asyncio.Event()
        self._last_frame_at = 0.0
        self._entry: SubscriptionEntry | None = None
        self._worker: asyncio.Task[None] | None = None
        self._unsubscribe_bus = None

    # -- lifecycle -----------------------------------------------------------

    async def __aenter__(self) -> Self:
        self._start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    def _start(self) -> None:
        if self._entry is not None:
            return
        client = self._consumer._stream._ctx.client
        conn = client._conn
        self._entry = conn.subscribe(f"{self._inbox_base}.*", None, self._route)
        self._last_frame_at = asyncio.get_running_loop().time()
        self._worker = client._spawn(self._pump(), name=f"natsio-consume-{self._consumer.name}")

        from natsio._internal.lifecycle import Closed, Reconnected

        def on_event(event: object) -> None:
            if isinstance(event, Reconnected):
                # Server-side pull requests died with the old connection.
                self._pulls.clear()
                self._total_pending = 0
                self._wake.set()
            elif isinstance(event, Closed):
                # Emitted synchronously on client.close() and on reconnect
                # exhaustion — without this, parked iterators hang forever.
                self._fail(ConnectionClosedError("connection closed"))

        self._unsubscribe_bus = conn.bus.subscribe(on_event)

    async def stop(self) -> None:
        """End the session; queued messages remain readable via the iterator."""
        if self._closed:
            return
        self._closed = True
        self._closed_event.set()
        if self._unsubscribe_bus is not None:
            self._unsubscribe_bus()
            self._unsubscribe_bus = None
        client = self._consumer._stream._ctx.client
        if self._entry is not None:
            # Best-effort: a closed connection cannot carry the UNSUB, and the
            # subscription dies with it anyway.
            with suppress(ConnectionClosedError):
                client._conn.unsubscribe(self._entry.sid)
            self._entry = None
        worker = self._worker
        self._worker = None
        if worker is not None and not worker.done():
            worker.cancel()
            await asyncio.wait((worker,))

    # -- read path (sync) ----------------------------------------------------

    def _route(self, event: MsgEvent | HMsgEvent) -> None:
        client = self._consumer._stream._ctx.client
        self._last_frame_at = asyncio.get_running_loop().time()
        token = event.subject.rpartition(".")[2]
        msg = client._build_msg(event)
        status = msg.status
        if status is None:
            # Pinned-client delivery (ADR-42): the server stamps every message
            # with the current Nats-Pin-Id; remember the latest so subsequent
            # pulls replay it and stay pinned. No-op for non-priority sessions.
            self._consumer._remember_pin(self._group, msg)
            # Data deliveries carry the message's ORIGINAL subject — the reply
            # inbox (and its token) appears only on status frames. Attribute
            # the delivery to the oldest outstanding pull: the server serves
            # requests in FIFO order, and any drift self-corrects when a pull
            # ends via its (token-addressed) status.
            if self._pulls:
                oldest = next(iter(self._pulls))
                remaining, issued_at = self._pulls[oldest]
                if remaining <= 1:
                    self._pulls.pop(oldest, None)
                else:
                    self._pulls[oldest] = (remaining - 1, issued_at)
            self._total_pending = max(0, self._total_pending - 1)
            self._rapid_ends = 0  # real data: any rejection loop is over
            self._queue.put_nowait(JsMsg(msg, client))
            if self._total_pending <= self._threshold_msgs:
                self._wake.set()
            return
        if status.code == 100:
            return  # heartbeat: _last_frame_at already refreshed
        # Any other status ends that pull request.
        entry = self._pulls.pop(token, None)
        if entry is not None:
            remaining, issued_at = entry
            self._total_pending = max(0, self._total_pending - remaining)
            # A pull that dies quickly without delivering anything is the
            # signature of a rejection loop (max_bytes too small, heartbeat
            # invalid, sustained leadership churn). Pace the re-pull instead
            # of hammering the server — probed at 22k pulls/sec without this.
            now = asyncio.get_running_loop().time()
            if now - issued_at < 1.0:
                self._rapid_ends = min(self._rapid_ends + 1, 8)
            else:
                self._rapid_ends = 0
        description = status.description.lower()
        if status.code == 409 and "consumer deleted" in description:
            self._fail(ConsumerDeletedError(f"consumer {self._consumer.name!r} was deleted"))
            return
        if status.code == 409 and "consumer is push based" in description:
            self._fail(JetStreamError(f"consumer {self._consumer.name!r} is push based"))
            return
        if status.code == 409 and "message size exceeds maxbytes" in description:
            # Re-pulling with the same max_bytes can never succeed.
            self._fail(
                JetStreamError(
                    f"consume() max_bytes is smaller than the next message on "
                    f"consumer {self._consumer.name!r}: {status.description}"
                )
            )
            return
        if status.code not in (404, 408, 409, 423):
            # 400 Bad Request and unknown codes: the pull is malformed or the
            # server is telling us something new — re-pulling identically would
            # loop forever. Fail loudly.
            self._fail(
                JetStreamError(
                    f"pull request on consumer {self._consumer.name!r} failed: {status.code} {status.description}"
                )
            )
            return
        if status.code == 423:
            # Pin lost (ADR-42): our stored id is stale. Drop it so the re-pull
            # goes out unpinned and the server can re-pin (or hand us over).
            self._consumer._forget_pin(self._group)
        self._wake.set()  # benign end (404/408/limits/leadership/pin): re-pull

    def _fail(self, error: Exception) -> None:
        if self._failure is None:
            self._failure = error
        self._closed_event.set()

    # -- pull issuance -------------------------------------------------------

    async def _pump(self) -> None:
        try:
            await self._pump_loop()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # A dead pump with no recorded failure is a silent forever-hang for
            # the iterator (probe-confirmed): always convert to a failure.
            self._fail(exc)

    async def _pump_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._closed:
            if self._failure is not None:
                return
            if self._rapid_ends:
                # Rejection-loop pacing: capped exponential, cancelled early
                # only by session close.
                delay = min(0.1 * (2**self._rapid_ends), 5.0)
                with suppress(builtins.TimeoutError):
                    async with asyncio.timeout(delay):
                        await self._closed_event.wait()
                if self._closed or self._failure is not None:
                    return
            # Clear FIRST: a wake arriving while we issue the pull below stays
            # latched for the next loop instead of being lost.
            self._wake.clear()
            need = self._max_messages - self._total_pending - self._queue.qsize()
            if need > 0 and (self._total_pending <= self._threshold_msgs or not self._pulls):
                await self._issue_pull(need)
            stall_after = self._heartbeat * 2
            try:
                async with asyncio.timeout(stall_after):
                    await self._wake.wait()
            except builtins.TimeoutError:
                if self._pulls and loop.time() - self._last_frame_at > stall_after:
                    # No frames, no heartbeats: the requests are dead server-side.
                    self._pulls.clear()
                    self._total_pending = 0

    async def _issue_pull(self, batch: int) -> None:
        token = next_nuid()
        request: dict[str, Any] = {
            "batch": batch,
            "expires": int(self._expires * _NANOSECOND),
            "idle_heartbeat": int(self._heartbeat * _NANOSECOND),
        }
        if self._max_bytes is not None:
            request["max_bytes"] = self._max_bytes
        if self._group is not None:
            request["group"] = self._group
            pin = self._consumer._pin_id(self._group)
            if pin:
                request["id"] = pin  # wire field for the pin id is "id"
        if self._min_pending is not None:
            request["min_pending"] = self._min_pending
        if self._min_ack_pending is not None:
            request["min_ack_pending"] = self._min_ack_pending
        self._pulls[token] = (batch, asyncio.get_running_loop().time())
        self._total_pending += batch
        client = self._consumer._stream._ctx.client
        try:
            await client.publish(
                self._consumer._pull_subject(),
                json.dumps(request).encode(),
                reply=f"{self._inbox_base}.{token}",
            )
        except Exception:
            self._pulls.pop(token, None)
            self._total_pending = max(0, self._total_pending - batch)
            raise

    # -- consumption ---------------------------------------------------------

    def __aiter__(self) -> AsyncIterator[JsMsg]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[JsMsg]:
        while True:
            msg = await self._next_or_none()
            if msg is None:
                if self._failure is not None:
                    raise self._failure
                return
            yield msg

    async def next(self, *, timeout: float | None = None) -> JsMsg:  # noqa: ASYNC109
        try:
            async with asyncio.timeout(timeout):
                msg = await self._next_or_none()
        except builtins.TimeoutError:
            raise NoMessagesError(f"no message within {timeout}s") from None
        if msg is None:
            if self._failure is not None:
                raise self._failure
            raise NoMessagesError("consume session is closed")
        return msg

    async def _next_or_none(self) -> JsMsg | None:
        self._start()  # tolerate use without `async with` — hang-free by design
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
                    continue
            # Consuming from the buffer is what makes room for the next pull —
            # without this wake, delivery stalls for 2x idle_heartbeat every
            # time the buffer fills (probe: ~1.2s gaps every batch).
            if self._total_pending + self._queue.qsize() <= self._threshold_msgs:
                self._wake.set()
            return msg


class OrderedConsumer:
    """An always-in-order, self-healing stream view (ADR-17).

    Backed by an ephemeral pull consumer with ``ack_policy=none``. Order is
    judged by *consumer* sequence contiguity; on any gap, stall, or consumer
    loss it silently recreates itself starting at the next unseen stream
    sequence (``deliver_policy=by_start_sequence`` — always paired with
    ``opt_start_seq``).

    Prefer the context-manager form for deterministic teardown of the
    server-side ephemeral consumer::

        async with stream.ordered_consumer() as ordered:
            async for msg in ordered:
                ...

    A bare ``async for`` over the object also works; teardown then happens at
    generator finalization (or via :meth:`stop`).
    """

    __slots__ = (
        "_base",
        "_consumer",
        "_expected_cseq",
        "_last_sseq",
        "_recreate_failures",
        "_session",
        "_stream",
    )

    def __init__(self, stream: "Stream", base: ConsumerConfig) -> None:
        self._stream = stream
        self._base = base
        self._consumer: Consumer | None = None
        self._session: Consumption | None = None
        self._expected_cseq = 1
        self._last_sseq = 0
        self._recreate_failures = 0

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    async def start(self) -> ConsumerInfo:
        """Eagerly create the underlying ephemeral consumer.

        Iteration does this lazily; starting explicitly is useful when the
        caller needs the initial :class:`ConsumerInfo` (e.g. ``num_pending``
        to detect an initially-empty stream) before blocking on messages.
        """
        if self._session is None:
            await self._start_session(500, 30.0, None)
        assert self._consumer is not None
        return self._consumer.cached_info

    async def stop(self) -> None:
        """Deterministically stop the session and delete the ephemeral consumer."""
        await self._teardown()

    def messages(
        self,
        *,
        max_messages: int = 500,
        expires: float = 30.0,
        idle_heartbeat: float | None = None,
        idle_timeout: float | None = None,
    ) -> AsyncGenerator[JsMsg]:
        """The ordered message stream (an async generator — ``aclose()`` to stop).

        ``idle_timeout`` bounds the wait for each message: on expiry the stream
        raises :class:`NoMessagesError` instead of silently self-healing
        forever, letting callers distinguish a quiet stream from a dead one.
        """
        return self._iterate(
            max_messages=max_messages,
            expires=expires,
            idle_heartbeat=idle_heartbeat,
            idle_timeout=idle_timeout,
        )

    def __aiter__(self) -> AsyncIterator[JsMsg]:
        return self._iterate(max_messages=500, expires=30.0, idle_heartbeat=None)

    async def _iterate(
        self,
        *,
        max_messages: int,
        expires: float,
        idle_heartbeat: float | None,
        idle_timeout: float | None = None,
    ) -> AsyncGenerator[JsMsg]:
        try:
            while True:
                if self._session is None:
                    await self._recreate_backoff()
                    try:
                        await self._start_session(max_messages, expires, idle_heartbeat)
                    except Exception:
                        self._recreate_failures = min(self._recreate_failures + 1, 8)
                        raise
                assert self._session is not None
                try:
                    msg = await self._session.next(timeout=idle_timeout)
                except NoMessagesError:
                    if idle_timeout is not None:
                        raise  # the caller asked for a liveness bound
                    await self._reset()
                    continue
                except (ConsumerDeletedError, JetStreamError):
                    self._recreate_failures = min(self._recreate_failures + 1, 8)
                    await self._reset()
                    continue
                metadata = msg.metadata
                if metadata.consumer_seq != self._expected_cseq:
                    self._recreate_failures = min(self._recreate_failures + 1, 8)
                    await self._reset()  # gap: recreate from the last good point
                    continue
                self._recreate_failures = 0
                self._expected_cseq += 1
                self._last_sseq = metadata.stream_seq
                yield msg
        finally:
            await self._teardown()

    async def _recreate_backoff(self) -> None:
        if self._recreate_failures == 0:
            return
        # Recreate storms tend to coincide with cluster churn — back off.
        await asyncio.sleep(min(0.1 * (2**self._recreate_failures), 5.0))

    async def _start_session(self, max_messages: int, expires: float, idle_heartbeat: float | None) -> None:
        config = ConsumerConfig(
            **{
                field: getattr(self._base, field)
                for field in (
                    "deliver_policy",
                    "opt_start_seq",
                    "opt_start_time",
                    "ack_policy",
                    "replay_policy",
                    "filter_subjects",
                    "headers_only",
                    "num_replicas",
                    "mem_storage",
                )
            }
        )
        config.name = f"oc_{next_nuid()}"
        config.inactive_threshold = _ordered_inactive_threshold()
        if self._last_sseq > 0:
            # Both halves together, always: a start sequence without the policy
            # is rejected by the server.
            config.deliver_policy = DeliverPolicy.BY_START_SEQUENCE
            config.opt_start_seq = self._last_sseq + 1
        self._consumer = await self._stream.create_consumer(config)
        if self._last_sseq == 0 and config.deliver_policy is DeliverPolicy.NEW:
            # Anchor a NEW-policy consumer at creation time: if it is lost
            # before delivering anything, the recreate resumes from here
            # instead of re-anchoring NEW and silently skipping the gap.
            self._last_sseq = self._consumer.cached_info.delivered.stream_seq
        self._expected_cseq = 1
        heartbeat = idle_heartbeat if idle_heartbeat is not None else min(expires / 2, 30.0)
        self._session = self._consumer.consume(max_messages=max_messages, expires=expires, idle_heartbeat=heartbeat)
        self._session._start()

    async def _reset(self) -> None:
        await self._teardown()

    async def _teardown(self) -> None:
        session = self._session
        self._session = None
        if session is not None:
            await session.stop()
        consumer = self._consumer
        self._consumer = None
        if consumer is not None:
            # Best-effort: the inactive_threshold reaps it server-side anyway.
            with suppress(Exception):
                await consumer.delete()


def _ordered_inactive_threshold() -> "timedelta":
    from datetime import timedelta

    return timedelta(minutes=5)
