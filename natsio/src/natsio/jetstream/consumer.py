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
from natsio.message import Msg

if TYPE_CHECKING:
    from .stream import Stream

from .entities import ConsumerConfig, ConsumerInfo, DeliverPolicy
from .errors import ConsumerDeletedError, JetStreamError, NoMessagesError
from .message import JsMsg

__all__ = ["Consumer", "Consumption", "OrderedConsumer"]

_NANOSECOND: int = 1_000_000_000
_FETCH_GRACE = 1.0  # local slack past the server-side `expires`

# 409 descriptions that end the *request* but not the consumer.
_BENIGN_409_MARKERS = (
    "exceeded maxrequest",
    "message size exceeds maxbytes",
    "exceeded maxwaiting",
    "leadership change",
    "server shutdown",
)


class _RequestGoneError(Exception):
    """Internal: the current pull ended (quietly or benignly)."""


class Consumer:
    """A handle to a pull consumer. Spawns fetches and consume sessions."""

    __slots__ = ("_stream", "cached_info")

    def __init__(self, stream: "Stream", info: ConsumerInfo) -> None:
        self._stream = stream
        self.cached_info = info

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

    def _pull_subject(self) -> str:
        ctx = self._stream._ctx
        return f"{ctx.api_prefix}.CONSUMER.MSG.NEXT.{self._stream.name}.{self.name}"

    # -- fetch / next --------------------------------------------------------

    async def fetch(
        self,
        max_messages: int = 100,
        *,
        max_bytes: int | None = None,
        timeout: float = 5.0,  # noqa: ASYNC109
        idle_heartbeat: float | None = None,
        no_wait: bool = False,
    ) -> list[JsMsg]:
        """One bounded pull: up to ``max_messages`` within ``timeout`` seconds.

        Returns what arrived when the batch fills, the server ends the request
        (no more messages / request expired), or the deadline passes — an empty
        list is a normal outcome. ``no_wait`` returns only immediately-available
        messages.
        """
        client = self._stream._ctx.client
        conn = client._conn
        inbox = f"_INBOX.{next_nuid()}"
        queue: asyncio.Queue[Msg] = asyncio.Queue()

        def handler(event: MsgEvent | HMsgEvent) -> None:
            queue.put_nowait(client._build_msg(event))

        entry = conn.subscribe(inbox, None, handler)
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
            await client.publish(self._pull_subject(), json.dumps(request).encode(), reply=inbox)

            messages: list[JsMsg] = []
            loop = asyncio.get_running_loop()
            deadline = loop.time() + timeout + _FETCH_GRACE
            while len(messages) < max_messages:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                wait = remaining if idle_heartbeat is None else min(remaining, idle_heartbeat * 2)
                try:
                    async with asyncio.timeout(wait):
                        msg = await queue.get()
                except builtins.TimeoutError:
                    break  # deadline or two missed heartbeats: done with what we have
                try:
                    if self._classify(msg):
                        continue  # heartbeat
                except _RequestGoneError:
                    break
                messages.append(JsMsg(msg, client))
            return messages
        finally:
            conn.unsubscribe(entry.sid)

    async def next(self, *, timeout: float = 5.0) -> JsMsg:  # noqa: ASYNC109
        """The next available message, or :class:`NoMessagesError` on expiry."""
        messages = await self.fetch(1, timeout=timeout)
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
        return True  # unknown status: skip, stay alive

    # -- consume -------------------------------------------------------------

    def consume(
        self,
        *,
        max_messages: int = 500,
        max_bytes: int | None = None,
        expires: float = 30.0,
        idle_heartbeat: float | None = None,
        threshold: float = 0.5,
    ) -> "Consumption":
        """A continuous, self-refilling message stream (ADR-37 ``consume``).

        Keeps up to ``max_messages`` requested from the server, re-pulling when
        the outstanding count drops below ``threshold`` of the target so the
        buffer never drains to a stall. Use as an async context manager::

            async with consumer.consume() as messages:
                async for msg in messages:
                    await msg.ack()
        """
        heartbeat = idle_heartbeat if idle_heartbeat is not None else min(expires / 2, 30.0)
        return Consumption(
            self,
            max_messages=max_messages,
            max_bytes=max_bytes,
            expires=expires,
            idle_heartbeat=heartbeat,
            threshold_msgs=max(1, int(max_messages * threshold)),
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
        "_heartbeat",
        "_inbox_base",
        "_last_frame_at",
        "_max_bytes",
        "_max_messages",
        "_pulls",
        "_queue",
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
    ) -> None:
        self._consumer = consumer
        self._max_messages = max_messages
        self._max_bytes = max_bytes
        self._expires = expires
        self._heartbeat = idle_heartbeat
        self._threshold_msgs = threshold_msgs
        self._inbox_base = f"_INBOX.{next_nuid()}"
        self._queue: asyncio.Queue[JsMsg] = asyncio.Queue()
        self._pulls: dict[str, int] = {}  # token -> messages still expected
        self._total_pending = 0
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

        from natsio._internal.lifecycle import Reconnected

        def on_event(event: object) -> None:
            if isinstance(event, Reconnected):
                # Server-side pull requests died with the old connection.
                self._pulls.clear()
                self._total_pending = 0
                self._wake.set()

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
            remaining = self._pulls.get(token)
            if remaining is not None:
                if remaining <= 1:
                    self._pulls.pop(token, None)
                else:
                    self._pulls[token] = remaining - 1
                self._total_pending = max(0, self._total_pending - 1)
            self._queue.put_nowait(JsMsg(msg, client))
            if self._total_pending <= self._threshold_msgs:
                self._wake.set()
            return
        if status.code == 100:
            return  # heartbeat: _last_frame_at already refreshed
        # Any other status ends that pull request.
        remaining = self._pulls.pop(token, None)
        if remaining is not None:
            self._total_pending = max(0, self._total_pending - remaining)
        description = status.description.lower()
        if status.code == 409 and "consumer deleted" in description:
            self._fail(ConsumerDeletedError(f"consumer {self._consumer.name!r} was deleted"))
            return
        if status.code == 409 and "consumer is push based" in description:
            self._fail(JetStreamError(f"consumer {self._consumer.name!r} is push based"))
            return
        self._wake.set()  # benign end (404/408/limits/leadership): re-pull

    def _fail(self, error: Exception) -> None:
        if self._failure is None:
            self._failure = error
        self._closed_event.set()

    # -- pull issuance -------------------------------------------------------

    async def _pump(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._closed:
            if self._failure is not None:
                return
            need = self._max_messages - self._total_pending - self._queue.qsize()
            if need > 0 and (self._total_pending <= self._threshold_msgs or not self._pulls):
                await self._issue_pull(need)
            self._wake.clear()
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
        self._pulls[token] = batch
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
        async with asyncio.timeout(timeout):
            msg = await self._next_or_none()
        if msg is None:
            if self._failure is not None:
                raise self._failure
            raise NoMessagesError("consume session is closed")
        return msg

    async def _next_or_none(self) -> JsMsg | None:
        while True:
            try:
                return self._queue.get_nowait()
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
                    return getter.result()


class OrderedConsumer:
    """An always-in-order, self-healing stream view (ADR-17).

    Backed by an ephemeral pull consumer with ``ack_policy=none``. Order is
    judged by *consumer* sequence contiguity; on any gap, stall, or consumer
    loss it silently recreates itself starting at the next unseen stream
    sequence (``deliver_policy=by_start_sequence`` — always paired with
    ``opt_start_seq``).
    """

    __slots__ = ("_base", "_consumer", "_expected_cseq", "_last_sseq", "_session", "_stream")

    def __init__(self, stream: "Stream", base: ConsumerConfig) -> None:
        self._stream = stream
        self._base = base
        self._consumer: Consumer | None = None
        self._session: Consumption | None = None
        self._expected_cseq = 1
        self._last_sseq = 0

    def messages(
        self,
        *,
        max_messages: int = 500,
        expires: float = 30.0,
        idle_heartbeat: float | None = None,
    ) -> AsyncGenerator[JsMsg]:
        """The ordered message stream (an async generator — ``aclose()`` to stop)."""
        return self._iterate(max_messages=max_messages, expires=expires, idle_heartbeat=idle_heartbeat)

    def __aiter__(self) -> AsyncIterator[JsMsg]:
        return self._iterate(max_messages=500, expires=30.0, idle_heartbeat=None)

    async def _iterate(
        self, *, max_messages: int, expires: float, idle_heartbeat: float | None
    ) -> AsyncGenerator[JsMsg]:
        try:
            while True:
                if self._session is None:
                    await self._start_session(max_messages, expires, idle_heartbeat)
                assert self._session is not None
                try:
                    msg = await self._session.next()
                except (ConsumerDeletedError, JetStreamError):
                    await self._reset()
                    continue
                metadata = msg.metadata
                if metadata.consumer_seq != self._expected_cseq:
                    await self._reset()  # gap: recreate from the last good point
                    continue
                self._expected_cseq += 1
                self._last_sseq = metadata.stream_seq
                yield msg
        finally:
            await self._teardown()

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
