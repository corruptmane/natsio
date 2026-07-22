"""The public NATS client."""

import asyncio
import builtins
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self, Unpack

from natsio._internal.connection import Connection, TransportFactory
from natsio._internal.lifecycle import (
    Closed,
    ConnectionEvent,
    ConnectionState,
    Disconnected,
    ErrorOccurred,
    Reconnected,
)
from natsio._internal.nuid import next_nuid
from natsio._internal.protocol import (
    Headers,
    HeadersInput,
    HMsgEvent,
    MsgEvent,
    StatusCode,
    encode_header_block,
    encode_hpub,
    encode_pub,
)
from natsio._internal.validation import validate_queue_group, validate_subject
from natsio.errors import (
    ConnectionClosedError,
    DrainTimeoutError,
    MaxPayloadExceededError,
    NATSError,
    NoRespondersError,
    SlowConsumerError,
)
from natsio.errors import TimeoutError as NATSTimeoutError
from natsio.message import Msg
from natsio.options import ConnectKwargs, ConnectOptions
from natsio.subscription import Callback, PendingLimitPolicy, Subscription

if TYPE_CHECKING:
    from natsio.jetstream import JetStreamContext

__all__ = ["Client", "ClientStatistics", "connect"]

log = logging.getLogger("natsio.client")

type ErrorCallback = Callable[[NATSError], Awaitable[None] | None]


@dataclass(frozen=True, slots=True)
class ClientStatistics:
    """A point-in-time snapshot of client counters."""

    in_msgs: int = 0
    out_msgs: int = 0
    in_bytes: int = 0
    out_bytes: int = 0
    reconnects: int = 0
    errors: int = 0


class _RequestSink:
    """Where replies for one in-flight request token go."""

    __slots__ = ("future", "queue")

    def __init__(self, *, many: bool) -> None:
        self.future: asyncio.Future[Msg] | None = None
        self.queue: asyncio.Queue[Msg | None] | None = None
        if many:
            self.queue = asyncio.Queue()
        else:
            self.future = asyncio.get_running_loop().create_future()

    def deliver(self, msg: Msg) -> None:
        if self.queue is not None:
            self.queue.put_nowait(msg)
        elif self.future is not None and not self.future.done():
            self.future.set_result(msg)

    def close(self) -> None:
        if self.queue is not None:
            self.queue.put_nowait(None)
        elif self.future is not None and not self.future.done():
            self.future.set_exception(ConnectionClosedError("connection closed"))


class Client:
    """A connection to a NATS server or cluster.

    Prefer the `connect()` factory:

        async with await natsio.connect("nats://localhost:4222") as nc:
            await nc.publish("greet", b"hello")
    """

    def __init__(
        self,
        options: ConnectOptions | None = None,
        *,
        error_cb: ErrorCallback | None = None,
        _transport_factory: TransportFactory | None = None,
    ) -> None:
        self._options = options if options is not None else ConnectOptions()
        self._error_cb = error_cb
        self._conn = Connection(self._options, transport_factory=_transport_factory)
        self._subscriptions: dict[int, Subscription] = {}
        self._tasks: set[asyncio.Task[Any]] = set()
        self._event_streams: set[asyncio.Queue[ConnectionEvent | None]] = set()
        # sids currently applying BLOCK backpressure; the transport-wide pause
        # is held while this is non-empty (a plain bool would let one drained
        # subscription resume the socket out from under a still-full one).
        self._pausing_sids: set[int] = set()
        self._unsubscribe_bus: Callable[[], None] | None = None

        # Muxed request inbox: one wildcard subscription, replies routed by token.
        self._inbox_prefix = f"{self._options.inbox_prefix}.{next_nuid()}"
        self._mux_sid: int | None = None
        self._sinks: dict[str, _RequestSink] = {}

        self._stats = {"in_msgs": 0, "out_msgs": 0, "in_bytes": 0, "out_bytes": 0, "reconnects": 0, "errors": 0}

    # -- lifecycle -----------------------------------------------------------

    async def connect(self) -> Self:
        self._unsubscribe_bus = self._conn.bus.subscribe(self._on_connection_event)
        try:
            await self._conn.connect()
        except BaseException:
            self._detach_bus()
            raise
        return self

    async def close(self) -> None:
        """Close immediately: flush pending writes, drop anything still queued."""
        for sub in list(self._subscriptions.values()):
            sub._close_local()
        self._subscriptions.clear()
        for sink in list(self._sinks.values()):
            sink.close()
        self._sinks.clear()
        await self._conn.close(flush=True)
        await self._cancel_tasks()
        self._detach_bus()
        for stream in list(self._event_streams):
            stream.put_nowait(None)
        self._event_streams.clear()

    async def drain(self) -> None:
        """Unsubscribe everything, let queued messages be handled, then close.

        Bounded by ``drain_timeout``; the client is closed no matter what.
        """
        subs = list(self._subscriptions.values())
        try:
            async with asyncio.timeout(self._options.drain_timeout):
                for sub in subs:
                    try:
                        await sub.drain()
                    except NATSError as exc:
                        log.debug("draining %r failed: %s", sub.subject, exc)
        except builtins.TimeoutError:
            log.warning("drain timed out after %ss; closing anyway", self._options.drain_timeout)
            self._conn.background_error(
                DrainTimeoutError(f"drain did not complete within {self._options.drain_timeout}s")
            )
        finally:
            await self.close()

    async def force_reconnect(self) -> None:
        """Deliberately drop the current transport and reconnect immediately.

        Pending writes are flushed best-effort, then the session is torn down
        through the normal lost path — subscriptions replay and buffered
        publishes survive — but the first reconnect attempt bypasses the backoff
        and the drop is not counted as a server failure. ``Disconnected`` then
        ``Reconnected`` fire as usual. Non-blocking: it returns once the drop is
        scheduled, not once the connection is back.

        Raises `ConnectionClosedError` if the client is
        already closed or draining.
        """
        await self._conn.force_reconnect()

    async def __aenter__(self) -> Self:
        if self.status is ConnectionState.DISCONNECTED:
            await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    # -- introspection -------------------------------------------------------

    @property
    def status(self) -> ConnectionState:
        return self._conn.state

    @property
    def is_connected(self) -> bool:
        return self._conn.is_connected

    @property
    def connected_url(self) -> str | None:
        return self._conn.connected_url

    @property
    def server_info(self) -> dict[str, Any]:
        return self._conn.server_info

    @property
    def max_payload(self) -> int:
        return self._conn.max_payload

    @property
    def stats(self) -> ClientStatistics:
        return ClientStatistics(**self._stats)

    @property
    def inbox_prefix(self) -> str:
        return self._inbox_prefix

    def jetstream(
        self,
        *,
        domain: str | None = None,
        api_prefix: str | None = None,
        timeout: float = 5.0,
        publish_async_max_pending: int = 4000,
        publish_async_stall_wait: float = 0.2,
        publish_async_timeout: float | None = None,
    ) -> "JetStreamContext":
        """A JetStream context over this connection.

        ``domain`` routes the control plane through ``$JS.<domain>.API`` (leaf
        nodes); ``api_prefix`` overrides the prefix entirely (exports). The
        ``publish_async_*`` knobs tune the async publish window (max in-flight
        acks, stall wait when full, optional per-message ack timeout).
        """
        from natsio.jetstream import JetStreamContext

        return JetStreamContext(
            self,
            domain=domain,
            api_prefix=api_prefix,
            timeout=timeout,
            publish_async_max_pending=publish_async_max_pending,
            publish_async_stall_wait=publish_async_stall_wait,
            publish_async_timeout=publish_async_timeout,
        )

    async def events(self) -> AsyncIterator[ConnectionEvent]:
        """Stream connection lifecycle events (connected, disconnected, errors...)."""
        stream: asyncio.Queue[ConnectionEvent | None] = asyncio.Queue(maxsize=1024)
        self._event_streams.add(stream)
        try:
            while True:
                event = await stream.get()
                if event is None:
                    return
                yield event
        finally:
            self._event_streams.discard(stream)

    # -- publishing ----------------------------------------------------------

    async def publish(
        self,
        subject: str,
        payload: bytes | str = b"",
        *,
        reply: str | None = None,
        headers: HeadersInput | None = None,
        _validate_reply: bool = True,
    ) -> None:
        """Publish a message. Returns once the frame is buffered, not delivered.

        ``_validate_reply`` is a private escape hatch: internal callers that
        build their own reply inbox (``request``/``request_many``, JetStream
        async publish) pass ``False`` to skip re-validating a subject they just
        generated. User-supplied replies are always validated.
        """
        data = payload.encode() if isinstance(payload, str) else payload
        validate_subject(subject)
        if reply is not None and _validate_reply:
            validate_subject(reply, argument="reply subject")
        limit = self.max_payload
        if len(data) > limit:
            raise MaxPayloadExceededError(f"payload of {len(data)} bytes exceeds the server maximum of {limit}")
        frame = (
            encode_pub(subject, reply, data)
            if headers is None
            else encode_hpub(subject, reply, encode_header_block(headers), data)
        )
        await self._conn.publish_frame(frame)
        self._stats["out_msgs"] += 1
        self._stats["out_bytes"] += len(data)
        self._conn.instrumentation.on_message_published(subject, headers, len(data))

    async def flush(self, timeout: float | None = None) -> None:  # noqa: ASYNC109
        """Round-trip a PING and wait for the PONG."""
        await self._conn.flush(timeout)

    # -- subscribing ---------------------------------------------------------

    def subscribe(
        self,
        subject: str,
        *,
        queue: str | None = None,
        cb: Callback | None = None,
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        policy: PendingLimitPolicy = PendingLimitPolicy.DROP_NEW,
    ) -> Subscription:
        """Subscribe to ``subject``.

        Synchronous by design: the SUB frame is buffered on the write path and
        registration takes effect immediately, so no message can be missed
        between creating the subscription and starting to consume it.
        """
        validate_subject(subject, wildcards=True)
        if queue is not None:
            validate_queue_group(queue)

        subscription: Subscription | None = None

        def handler(event: MsgEvent | HMsgEvent) -> None:
            if subscription is not None:
                subscription._deliver(self._build_msg(event))

        entry = self._conn.subscribe(subject, queue, handler)
        subscription = Subscription(
            self,
            entry,
            callback=cb,
            pending_msgs_limit=(
                pending_msgs_limit if pending_msgs_limit is not None else self._options.pending_msgs_limit
            ),
            pending_bytes_limit=(
                pending_bytes_limit if pending_bytes_limit is not None else self._options.pending_bytes_limit
            ),
            policy=policy,
        )
        entry.on_complete = subscription._complete_local
        entry.on_fail = subscription._fail_permanent
        self._subscriptions[entry.sid] = subscription
        if cb is not None:
            subscription._start_callback_reader()
        return subscription

    # -- request / reply -----------------------------------------------------

    async def request(
        self,
        subject: str,
        payload: bytes | str = b"",
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        headers: HeadersInput | None = None,
    ) -> Msg:
        """Send a request and await a single reply."""
        deadline = timeout if timeout is not None else self._options.request_timeout
        self._ensure_mux()
        token = next_nuid()
        sink = _RequestSink(many=False)
        self._sinks[token] = sink
        try:
            # Outside the timeout-catch: a publish failure (e.g. the write
            # buffer's own TimeoutError) must surface as itself, not be
            # relabeled as "no reply within deadline".
            await self.publish(
                subject, payload, reply=f"{self._inbox_prefix}.{token}", headers=headers, _validate_reply=False
            )
            try:
                async with asyncio.timeout(deadline):
                    assert sink.future is not None
                    msg = await sink.future
            except builtins.TimeoutError:
                raise NATSTimeoutError(f"no reply to {subject!r} within {deadline}s") from None
        finally:
            self._sinks.pop(token, None)
        if msg.status is not None and msg.status.code == StatusCode.NO_RESPONDERS:
            raise NoRespondersError(f"no responders listening on {subject!r}")
        return msg

    async def request_many(
        self,
        subject: str,
        payload: bytes | str = b"",
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        max_msgs: int | None = None,
        stall: float | None = None,
        headers: HeadersInput | None = None,
    ) -> AsyncIterator[Msg]:
        """Send one request and yield every reply (ADR-47 "request many").

        Completion is whichever comes first: ``max_msgs`` replies, a gap of
        ``stall`` seconds between replies, or the overall ``timeout``. A
        no-responders status ends the stream without yielding.
        """
        overall = timeout if timeout is not None else self._options.request_timeout
        loop = asyncio.get_running_loop()
        deadline_at = loop.time() + overall
        self._ensure_mux()
        token = next_nuid()
        sink = _RequestSink(many=True)
        self._sinks[token] = sink
        assert sink.queue is not None
        received = 0
        try:
            await self.publish(
                subject, payload, reply=f"{self._inbox_prefix}.{token}", headers=headers, _validate_reply=False
            )
            while True:
                # The deadline is armed ONLY around the queue wait. Arming it
                # across the `yield` would tie the timer to the consumer's
                # task while it is outside this generator, so an expiry would
                # cancel arbitrary caller code instead of ending the stream.
                remaining = deadline_at - loop.time()
                if remaining <= 0:
                    return
                window = remaining if stall is None else min(stall, remaining)
                try:
                    async with asyncio.timeout(window):
                        msg = await sink.queue.get()
                except builtins.TimeoutError:
                    return  # stall gap or overall deadline: complete with what we have
                if msg is None:
                    return
                if msg.status is not None and msg.status.code == StatusCode.NO_RESPONDERS:
                    return
                yield msg
                received += 1
                if max_msgs is not None and received >= max_msgs:
                    return
        finally:
            self._sinks.pop(token, None)

    def _ensure_mux(self) -> None:
        """Create the single wildcard reply inbox on first use.

        Replies are routed straight to their per-token sink rather than through
        a Subscription queue: a dropped reply is a hung request, so this path
        has no pending limit and no drop policy. Synchronous, so concurrent
        callers cannot race into two subscriptions.
        """
        if self._mux_sid is not None:
            return

        def route(event: MsgEvent | HMsgEvent) -> None:
            token = event.subject.rpartition(".")[2]
            sink = self._sinks.get(token)
            if sink is not None:
                sink.deliver(self._build_msg(event))

        entry = self._conn.subscribe(f"{self._inbox_prefix}.*", None, route)
        self._mux_sid = entry.sid

    # -- internals used by Subscription --------------------------------------

    def _build_msg(self, event: MsgEvent | HMsgEvent) -> Msg:
        self._stats["in_msgs"] += 1
        self._stats["in_bytes"] += len(event.payload)
        headers: Headers | None = None
        status = None
        if isinstance(event, HMsgEvent):
            headers = event.headers
            status = event.status
        return Msg(
            subject=event.subject,
            payload=event.payload,
            reply=event.reply_to,
            headers=headers,
            status=status,
            sid=event.sid,
            _client=self,
        )

    def _remove_subscription(self, sub: Subscription, *, max_msgs: int | None) -> None:
        self._conn.unsubscribe(sub.sid, max_msgs)
        if max_msgs is None:
            self._subscriptions.pop(sub.sid, None)

    def _spawn(self, coro: Any, *, name: str) -> asyncio.Task[Any]:
        task = asyncio.get_running_loop().create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def _pause_reading(self, sid: int) -> None:
        if not self._pausing_sids:
            self._conn.pause_reading()
        self._pausing_sids.add(sid)

    def _resume_reading(self, sid: int) -> None:
        self._pausing_sids.discard(sid)
        if not self._pausing_sids:
            self._conn.resume_reading()

    def _on_slow_consumer(self, sub: Subscription, dropped: int) -> None:
        self._conn.instrumentation.on_slow_consumer(sub.subject, sub.sid)
        # Coalesced: this fires on the read path once per dropped message, and
        # each report spawns an error-callback task and fans out to every
        # events() stream — unthrottled, a message flood becomes a task flood.
        now = asyncio.get_running_loop().time()
        last = sub._last_drop_report
        if last is not None and now - last < 1.0:
            return
        sub._last_drop_report = now
        self._conn.background_error(
            SlowConsumerError(
                f"dropped {sub.dropped} message(s) so far on {sub.subject!r} (pending limits exceeded)",
                subject=sub.subject,
                sid=sub.sid,
                dropped=sub.dropped,
            )
        )

    def _on_callback_error(self, sub: Subscription, error: Exception) -> None:
        log.exception("subscription callback for %r failed", sub.subject, exc_info=error)
        self._conn.background_error(error)

    # -- event fan-out -------------------------------------------------------

    def _on_connection_event(self, event: ConnectionEvent) -> None:
        match event:
            case Reconnected():
                self._stats["reconnects"] += 1
                if self._pausing_sids:
                    # The new transport starts un-paused; re-assert the
                    # backpressure still owed by saturated BLOCK subscriptions.
                    self._conn.pause_reading()
            case ErrorOccurred(error=error):
                self._stats["errors"] += 1
                if self._error_cb is not None:
                    self._spawn(self._invoke_error_cb(error), name="natsio-error-cb")
            case Disconnected() | Closed():
                pass
        for stream in list(self._event_streams):
            self._offer_event(stream, event)
        if isinstance(event, Closed):
            for stream in list(self._event_streams):
                self._offer_event(stream, None)

    @staticmethod
    def _offer_event(stream: "asyncio.Queue[ConnectionEvent | None]", event: "ConnectionEvent | None") -> None:
        # Bounded with drop-oldest: a consumer slower than the event rate loses
        # the oldest events instead of growing the queue without bound.
        while True:
            try:
                stream.put_nowait(event)
                return
            except asyncio.QueueFull:
                with suppress(asyncio.QueueEmpty):  # pragma: no cover - single-threaded
                    stream.get_nowait()

    async def _invoke_error_cb(self, error: Exception) -> None:
        if self._error_cb is None:
            return
        try:
            wrapped = error if isinstance(error, NATSError) else NATSError(str(error))
            result = self._error_cb(wrapped)
            if result is not None:
                await result
        except Exception:
            log.exception("error callback itself failed")

    def _detach_bus(self) -> None:
        if self._unsubscribe_bus is not None:
            self._unsubscribe_bus()
            self._unsubscribe_bus = None

    async def _cancel_tasks(self) -> None:
        tasks = [t for t in self._tasks if not t.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            # asyncio.wait does not swallow OUR cancellation and does not raise
            # the children's exceptions; retrieve those explicitly below so no
            # "exception was never retrieved" warnings fire at GC time.
            await asyncio.wait(tasks)
        for task in tasks:
            if not task.cancelled() and task.exception() is not None:
                log.debug("background task %r failed during close", task.get_name(), exc_info=task.exception())
        self._tasks.clear()


async def connect(
    *servers: str,
    error_cb: ErrorCallback | None = None,
    options: ConnectOptions | None = None,
    _transport_factory: TransportFactory | None = None,
    **kwargs: Unpack[ConnectKwargs],
) -> Client:
    """Connect to NATS and return a ready `Client`.

    ``servers`` and any keyword arguments are folded into a
    `ConnectOptions`; pass ``options=`` to supply one
    directly (keyword arguments then override its fields).
    """
    base = options if options is not None else ConnectOptions()
    if servers:
        kwargs["servers"] = tuple(servers)
    resolved = base.replace(**kwargs) if kwargs else base
    client = Client(resolved, error_cb=error_cb, _transport_factory=_transport_factory)
    await client.connect()
    return client
