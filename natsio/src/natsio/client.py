"""The public NATS client."""

import asyncio
import builtins
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self

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
from natsio.errors import ConfigError, NATSError, NoRespondersError, SlowConsumerError
from natsio.errors import TimeoutError as NATSTimeoutError
from natsio.message import Msg
from natsio.options import ConnectOptions
from natsio.subscription import Callback, PendingLimitPolicy, Subscription

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


class Client:
    """A connection to a NATS server or cluster.

    Prefer the :func:`connect` factory::

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
        """Unsubscribe everything, let queued messages be handled, then close."""
        subs = list(self._subscriptions.values())
        try:
            async with asyncio.timeout(self._options.drain_timeout):
                for sub in subs:
                    await sub.drain()
        except builtins.TimeoutError:
            log.warning("drain timed out after %ss; closing anyway", self._options.drain_timeout)
        await self.close()

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
        value = self._conn.server_info.get("max_payload")
        return int(value) if value else 1024 * 1024

    @property
    def stats(self) -> ClientStatistics:
        return ClientStatistics(**self._stats)  # type: ignore[arg-type]

    @property
    def inbox_prefix(self) -> str:
        return self._inbox_prefix

    async def events(self) -> AsyncIterator[ConnectionEvent]:
        """Stream connection lifecycle events (connected, disconnected, errors...)."""
        stream: asyncio.Queue[ConnectionEvent | None] = asyncio.Queue()
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
    ) -> None:
        """Publish a message. Returns once the frame is buffered, not delivered."""
        data = payload.encode() if isinstance(payload, str) else payload
        validate_subject(subject)
        if reply is not None:
            validate_subject(reply, argument="reply subject")
        limit = self.max_payload
        if len(data) > limit:
            raise ConfigError(f"payload of {len(data)} bytes exceeds the server maximum of {limit}")
        frame = (
            encode_pub(subject, reply, data)
            if headers is None
            else encode_hpub(subject, reply, encode_header_block(headers), data)
        )
        await self._conn.publish_frame(frame)
        self._stats["out_msgs"] += 1
        self._stats["out_bytes"] += len(data)

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
            await self.publish(subject, payload, reply=f"{self._inbox_prefix}.{token}", headers=headers)
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
        deadline = timeout if timeout is not None else self._options.request_timeout
        self._ensure_mux()
        token = next_nuid()
        sink = _RequestSink(many=True)
        self._sinks[token] = sink
        assert sink.queue is not None
        received = 0
        try:
            await self.publish(subject, payload, reply=f"{self._inbox_prefix}.{token}", headers=headers)
            async with asyncio.timeout(deadline):
                while True:
                    try:
                        async with asyncio.timeout(stall):
                            msg = await sink.queue.get()
                    except builtins.TimeoutError:
                        return  # stall timer expired: treat as complete
                    if msg is None:
                        return
                    if msg.status is not None and msg.status.code == StatusCode.NO_RESPONDERS:
                        return
                    yield msg
                    received += 1
                    if max_msgs is not None and received >= max_msgs:
                        return
        except builtins.TimeoutError:
            return  # overall deadline: complete with what we produced
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

    def _pause_reading(self) -> None:
        self._conn.pause_reading()

    def _resume_reading(self) -> None:
        self._conn.resume_reading()

    def _on_slow_consumer(self, sub: Subscription, dropped: int) -> None:
        self._conn.background_error(
            SlowConsumerError(
                f"dropped {dropped} message(s) on {sub.subject!r} (pending limits exceeded)",
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
            case ErrorOccurred(error=error):
                self._stats["errors"] += 1
                if self._error_cb is not None:
                    self._spawn(self._invoke_error_cb(error), name="natsio-error-cb")
            case Disconnected() | Closed():
                pass
        for stream in list(self._event_streams):
            stream.put_nowait(event)
        if isinstance(event, Closed):
            for stream in list(self._event_streams):
                stream.put_nowait(None)

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
        for task in tasks:
            with suppress(asyncio.CancelledError, Exception):
                await task
        self._tasks.clear()


async def connect(
    *servers: str,
    error_cb: ErrorCallback | None = None,
    options: ConnectOptions | None = None,
    **kwargs: Any,
) -> Client:
    """Connect to NATS and return a ready :class:`Client`.

    ``servers`` and any keyword arguments are folded into a
    :class:`~natsio.options.ConnectOptions`; pass ``options=`` to supply one
    directly (keyword arguments then override its fields).
    """
    base = options if options is not None else ConnectOptions()
    if servers:
        kwargs["servers"] = tuple(servers)
    resolved = base.replace(**kwargs) if kwargs else base
    client = Client(resolved, error_cb=error_cb)
    await client.connect()
    return client
