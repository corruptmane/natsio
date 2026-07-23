"""The consume session: one joined member instance, supervised.

Both flavours share this machinery. A session owns three tasks:

- a **feeder** iterating the group's KV watcher into a queue,
- a **control** task — the only mutator of session state: it applies config
  changes, restarts the member's consumer, and owns teardown,
- a **worker** iterating the current consume session into the handler.

Termination is Event-latched (`_stopped` requests it, `_done` reports it), never
in-band sentinels; every parked await in the control loop is woken by
`_stopped`, by a config update, by the worker ending, or by the idle tick.
`CancelledError` is never swallowed, and `stop()` called from inside a handler
does not cancel the calling task — it latches and returns, letting the control
task finish the teardown.
"""

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Coroutine
from contextlib import suppress
from datetime import timedelta
from typing import Any, Final, Self

from natsio._internal.protocol import Headers
from natsio.errors import NATSError
from natsio.jetstream import (
    AckMetadata,
    Consumer,
    ConsumerConfig,
    ConsumerNotFoundError,
    Consumption,
    JetStreamContext,
    JsMsg,
    PriorityPolicy,
    Stream,
)
from natsio.kv import KeyValue, KvEntry, KvWatcher

from .entities import MIN_PULL_EXPIRY_PINNED_TTL, PRIORITY_GROUP, PULL_TIMEOUT_DIVIDER
from .errors import ConsumerGroupError
from .partitions import compose_key, strip_partition

__all__ = ["ConsumerGroupConsumeContext", "MessageHandler", "PartitionedMsg"]

log: Final = logging.getLogger("natsio.pcgroups")


class PartitionedMsg:
    """A group message, with the partition token stripped from its subject.

    `subject` reads exactly as it did before the stream was partitioned, so an
    existing handler works unchanged; `partitioned_subject` and `partition`
    expose what was stripped. Acknowledgement is delegated to the underlying
    `natsio.jetstream.JsMsg` unchanged.
    """

    __slots__ = ("_msg", "_subject")

    def __init__(self, msg: JsMsg) -> None:
        self._msg = msg
        self._subject = strip_partition(msg.subject)

    def __repr__(self) -> str:
        return f"PartitionedMsg(subject={self._subject!r}, partition={self.partition!r})"

    @property
    def subject(self) -> str:
        """The original (pre-partitioning) subject."""
        return self._subject

    @property
    def partitioned_subject(self) -> str:
        """The subject as stored, partition token included."""
        return self._msg.subject

    @property
    def partition(self) -> int | None:
        """The leading partition number, or ``None`` if there isn't one."""
        token = self._msg.subject.partition(".")[0]
        return int(token) if token.isdigit() else None

    @property
    def data(self) -> bytes:
        return self._msg.payload

    @property
    def payload(self) -> bytes:
        return self._msg.payload

    @property
    def headers(self) -> Headers | None:
        return self._msg.headers

    @property
    def reply(self) -> str | None:
        return self._msg.msg.reply

    @property
    def metadata(self) -> AckMetadata:
        return self._msg.metadata

    @property
    def message(self) -> JsMsg:
        """The wrapped `natsio.jetstream.JsMsg` — the escape hatch."""
        return self._msg

    async def ack(self) -> None:
        await self._msg.ack()

    async def ack_sync(self, timeout: float | None = 5.0) -> None:  # noqa: ASYNC109
        await self._msg.ack_sync(timeout)

    async def nak(self, *, delay: timedelta | float | None = None) -> None:
        await self._msg.nak(delay=delay)

    async def term(self, reason: str = "") -> None:
        await self._msg.term(reason)

    async def in_progress(self) -> None:
        await self._msg.in_progress()


type MessageHandler = Callable[[PartitionedMsg], Awaitable[None]]
"""An async callable invoked once per delivered message, serially."""


class ConsumerGroupConsumeContext[ConfigT]:
    """A joined consumer-group member instance.

    Returned by `natsio.pcgroups.static_consume` and
    `natsio.pcgroups.elastic_consume`; never constructed directly. Stop it with
    `stop()` (idempotent, deterministic) or by using it as an async context
    manager. `wait()` blocks until the session ends and re-raises whatever
    terminated it.
    """

    __slots__ = (
        "_config",
        "_consumer",
        "_consumption",
        "_control",
        "_delete_consumer_on_teardown",
        "_done",
        "_error",
        "_feeder",
        "_group",
        "_handler",
        "_js",
        "_key",
        "_kv",
        "_last_recovered_error",
        "_member",
        "_recovered",
        "_state_changed",
        "_stopped",
        "_stream_name",
        "_template",
        "_updates",
        "_watcher",
        "_worker",
        "_worker_ended",
        "_worker_error",
        "_worker_error_from_handler",
    )

    # Whether a deleted group config also deletes this member's JetStream
    # consumer on the way out (static does; elastic's stream goes with it).
    _deletes_consumer_on_group_delete: bool = False

    def __init__(
        self,
        js: JetStreamContext,
        kv: KeyValue,
        stream: str,
        group: str,
        member: str,
        handler: MessageHandler,
        template: ConsumerConfig,
        config: ConfigT,
    ) -> None:
        self._js = js
        self._kv = kv
        self._stream_name = stream
        self._group = group
        self._member = member
        self._handler = handler
        self._template = template
        self._config = config
        self._key = compose_key(stream, group)

        self._updates: asyncio.Queue[KvEntry] = asyncio.Queue()
        self._stopped = asyncio.Event()
        self._state_changed = asyncio.Event()
        self._done = asyncio.Event()
        self._worker_ended = asyncio.Event()
        self._error: Exception | None = None
        self._worker_error: Exception | None = None
        # Whether `_worker_error` came out of the user's handler (a poison
        # message) rather than the consume iteration / infra. Elastic groups
        # self-heal the latter but must NOT loop on the former.
        self._worker_error_from_handler = False
        self._last_recovered_error: Exception | None = None
        self._recovered = 0
        self._delete_consumer_on_teardown = False

        self._watcher: KvWatcher | None = None
        self._feeder: asyncio.Task[None] | None = None
        self._control: asyncio.Task[None] | None = None
        self._worker: asyncio.Task[None] | None = None
        self._consumer: Consumer | None = None
        self._consumption: Consumption | None = None

    # -- public surface ------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(stream={self._stream_name!r}, group={self._group!r}, "
            f"member={self._member!r}, consuming={self._consumer is not None})"
        )

    @property
    def stream(self) -> str:
        return self._stream_name

    @property
    def group(self) -> str:
        return self._group

    @property
    def member(self) -> str:
        return self._member

    @property
    def config(self) -> ConfigT:
        """The group config this instance last acted on."""
        return self._config

    @property
    def consuming(self) -> bool:
        """Whether this instance currently holds a consumer it pulls from.

        ``True`` does not mean this instance is the *pinned* one — every
        instance of a member pulls, and the server serves only the pinned one.
        """
        return self._consumer is not None

    @property
    def error(self) -> Exception | None:
        """What terminated the session, or ``None`` (running, or stopped cleanly)."""
        return self._error

    @property
    def recovered_errors(self) -> int:
        """Count of recoverable faults absorbed while (re)joining or consuming.

        Nothing is dropped silently: this counter and `last_recovered_error`
        are the report. A handful is normal during elastic membership churn (a
        work-queue stream refuses two consumers with overlapping filters while
        the members converge on the new membership).
        """
        return self._recovered

    @property
    def last_recovered_error(self) -> Exception | None:
        return self._last_recovered_error

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    async def stop(self) -> None:
        """End the session. Idempotent, and safe to call from inside a handler.

        Called from a handler it latches the stop and returns immediately
        rather than cancelling the calling task; the control task completes the
        teardown once the handler unwinds.
        """
        self._stopped.set()
        if asyncio.current_task() is self._worker:
            return
        await self._done.wait()

    async def wait_for_consuming(self, consuming: bool = True, *, timeout: float | None = None) -> None:  # noqa: ASYNC109
        """Block until this instance is (or stops being) attached to a consumer.

        The useful wait for an elastic member: `elastic_consume` returns before
        the member is in the membership, and this is how you find out it has
        been added (or dropped) without polling. Raises `ConsumerGroupError` if
        the session ends first — chained to whatever terminated it.
        """
        async with asyncio.timeout(timeout):
            while self.consuming is not consuming:
                if self._done.is_set():
                    raise ConsumerGroupError(
                        f"the consume session for member {self._member!r} ended before it was "
                        f"{'consuming' if consuming else 'idle'}"
                    ) from self._error
                self._state_changed.clear()
                if self.consuming is consuming or self._done.is_set():
                    continue
                await self._state_changed.wait()

    async def wait(self) -> None:
        """Block until the session ends; re-raise whatever terminated it.

        Returns normally when the session was stopped or its consumer group was
        deleted — the oracle's ``Done()`` channel semantics, with the error
        raised rather than returned.
        """
        await self._done.wait()
        if self._error is not None:
            raise self._error

    # -- startup -------------------------------------------------------------

    async def _start(self) -> None:
        """Watch the config, join, and hand over to the control task."""
        self._watcher = self._kv.watch(self._key)
        self._feeder = self._spawn(self._feed_updates(), "feeder")
        try:
            await self._initial_join()
        except BaseException:
            await self._stop_watcher()
            raise
        self._control = self._spawn(self._control_loop(), "control")

    def _spawn(self, coro: Coroutine[Any, Any, None], role: str) -> asyncio.Task[None]:
        # The client owns the task set, so client.close() cancels these too —
        # the wake path of last resort for anything parked in here.
        return self._js.client._spawn(coro, name=f"natsio-pcgroups-{role}-{self._group}-{self._member}")

    async def _initial_join(self) -> None:
        raise NotImplementedError

    # -- feeder --------------------------------------------------------------

    async def _feed_updates(self) -> None:
        watcher = self._watcher
        assert watcher is not None
        try:
            async for entry in watcher:
                if entry is not None:  # the initial-state marker carries nothing
                    self._updates.put_nowait(entry)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fail(exc)
            return
        if not self._stopped.is_set():
            self._fail(ConsumerGroupError(f"the config watcher for {self._key!r} closed unexpectedly"))

    # -- control loop --------------------------------------------------------

    @property
    def _idle_period(self) -> float | None:
        """Seconds between idle ticks, or ``None`` for a purely reactive loop."""
        return None

    async def _control_loop(self) -> None:
        try:
            while not self._stopped.is_set():
                entry = await self._wait_next()
                if self._stopped.is_set():
                    break
                if entry is not None:
                    await self._on_config_entry(entry)
                elif self._worker_ended.is_set():
                    await self._on_worker_ended()
                else:
                    await self._on_idle_tick()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fail(exc)
        finally:
            try:
                await self._teardown()
            finally:
                self._done.set()
                self._state_changed.set()  # wake anything parked in wait_for_consuming

    async def _wait_next(self) -> KvEntry | None:
        """The next config entry, or ``None`` when something else woke us."""
        getter = asyncio.ensure_future(self._updates.get())
        stopper = asyncio.ensure_future(self._stopped.wait())
        ended = asyncio.ensure_future(self._worker_ended.wait())
        try:
            done, _ = await asyncio.wait(
                (getter, stopper, ended),
                timeout=self._idle_period,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            # Cancelling a finished future is a no-op that keeps its result, and
            # a cancelled Queue.get() never swallows an item (it re-wakes the
            # next getter), so nothing is lost here.
            getter.cancel()
            stopper.cancel()
            ended.cancel()
        if getter in done and not getter.cancelled():
            return getter.result()
        return None

    async def _on_config_entry(self, entry: KvEntry) -> None:
        if entry.is_marker:
            # The group was deleted (or purged) out from under us: a normal end.
            self._delete_consumer_on_teardown = self._deletes_consumer_on_group_delete
            self._stopped.set()
            return
        try:
            config = self._parse_config(json.loads(entry.value))
        except Exception as exc:
            self._fail(ConsumerGroupError(f"consumer group {self._key!r} has an unusable config: {exc}"))
            return
        await self._apply_config(config)

    def _parse_config(self, payload: dict[str, Any]) -> ConfigT:
        raise NotImplementedError

    async def _apply_config(self, config: ConfigT) -> None:
        raise NotImplementedError

    async def _on_idle_tick(self) -> None:
        """Periodic self-correction. Static groups have nothing to do here."""

    async def _on_worker_ended(self) -> None:
        raise NotImplementedError

    # -- consuming -----------------------------------------------------------

    @property
    def _ack_wait(self) -> timedelta:
        ack_wait = self._template.ack_wait
        assert ack_wait is not None  # defaulted by the consume factory
        return ack_wait

    @property
    def _pinned_ttl(self) -> timedelta:
        return max(self._ack_wait, MIN_PULL_EXPIRY_PINNED_TTL)

    @property
    def _pull_expiry(self) -> float:
        return max(self._ack_wait / PULL_TIMEOUT_DIVIDER, MIN_PULL_EXPIRY_PINNED_TTL).total_seconds()

    def _member_consumer_config(self, name: str, filters: list[str], *, durable: bool) -> ConsumerConfig:
        """The user's template with the group-owned fields overwritten.

        Names, filters and the whole ADR-42 priority-group block belong to the
        library; everything else (ack policy, max ack pending, deliver policy,
        …) stays the caller's.
        """
        config = ConsumerConfig.from_wire(self._template.to_wire())  # never mutate the caller's
        if durable:
            config.durable_name = name
            config.name = None
        else:
            config.durable_name = None
            config.name = name
        config.filter_subject = None
        config.filter_subjects = filters
        config.priority_groups = [PRIORITY_GROUP]
        config.priority_policy = PriorityPolicy.PINNED_CLIENT
        # The pinned TTL's wire field is `priority_timeout` (nats.go calls the
        # Go field PinnedTTL and tags it priority_timeout).
        config.priority_timeout = self._pinned_ttl
        return config

    async def _create_member_consumer(self, stream: Stream, name: str, config: ConsumerConfig) -> Consumer:
        """Create the member's consumer, replacing one whose filters have moved.

        natsio's ``create_consumer`` is create-or-update, so a filter change
        would be applied in place — which silently keeps the consumer's
        position. Newly assigned partitions can need an EARLIER position, so a
        filter difference is a delete-and-recreate, exactly what the oracle gets
        out of its create/ErrConsumerExists path.
        """
        try:
            info = await stream.consumer_info(name)
        except ConsumerNotFoundError:
            info = None
        if info is not None and list(info.config.filter_subjects or ()) != config.filter_subjects:
            await stream.delete_consumer(name)
        return await stream.create_consumer(config)

    def _start_worker(self, consumer: Consumer) -> None:
        self._consumer = consumer
        self._consumption = consumer.consume(expires=self._pull_expiry, group=PRIORITY_GROUP)
        self._worker = self._spawn(self._run_worker(self._consumption), "worker")
        self._state_changed.set()

    async def _run_worker(self, consumption: Consumption) -> None:
        error: Exception | None = None
        from_handler = False
        try:
            if self._consumption is not consumption:
                # Stopped between being spawned and first running. Entering the
                # session now would install a subscription that the already-
                # completed stop() would never take down. (No suspension point
                # between this check and `_start()` inside __aenter__.)
                return
            async with consumption:
                async for msg in consumption:
                    if self._stopped.is_set():
                        break
                    try:
                        await self._handler(PartitionedMsg(msg))
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        # A handler fault is the user's, not the transport's:
                        # tag it so elastic fails fatally instead of redelivering
                        # a poison message forever (static fails on any error).
                        error = exc
                        from_handler = True
                        raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if error is None:
                error = exc  # a consumption/infra error, not the handler
        finally:
            # Latched for the control task, the only place that decides whether
            # this was fatal. Cleared again by _stop_worker.
            self._worker_error = error
            self._worker_error_from_handler = from_handler
            self._worker_ended.set()

    async def _stop_worker(self) -> None:
        """Stop consuming. Graceful first: a handler mid-message gets to finish.

        Only ever called from the control task, so it needs no re-entrancy
        guard — and ``_worker`` is deliberately cleared LAST, so a handler that
        calls `stop()` while this is in flight still recognizes itself and
        latches instead of parking on a teardown that is waiting for it.
        """
        consumption, self._consumption = self._consumption, None
        worker = self._worker
        self._consumer = None
        if consumption is not None:
            with suppress(NATSError):
                await consumption.stop()
        if worker is not None:
            if not worker.done():
                # Ending the consume session ends the iteration; only a handler
                # still running holds it open. Bound that by the ack wait —
                # past it the message is redelivered anyway.
                finished, _ = await asyncio.wait((worker,), timeout=self._ack_wait.total_seconds())
                if not finished:
                    worker.cancel()
            await asyncio.wait((worker,))
        self._worker = None
        self._worker_error = None
        self._worker_error_from_handler = False
        self._worker_ended.clear()
        self._state_changed.set()

    def _note_recovered(self, exc: Exception, what: str) -> None:
        self._recovered += 1
        self._last_recovered_error = exc
        log.warning("pcgroups %s/%s member %s: %s: %r", self._stream_name, self._group, self._member, what, exc)

    def _fail(self, exc: Exception) -> None:
        if self._error is None:
            self._error = exc
        self._stopped.set()

    # -- teardown ------------------------------------------------------------

    async def _teardown(self) -> None:
        await self._stop_worker()
        await self._stop_watcher()
        if self._delete_consumer_on_teardown:
            with suppress(NATSError):
                await self._delete_member_consumer()

    async def _stop_watcher(self) -> None:
        # Cancel the feeder BEFORE stopping the watcher: stopping it while its
        # ordered consumer is parked mid-iteration makes the consumer recreate
        # itself instead of ending.
        feeder, self._feeder = self._feeder, None
        if feeder is not None:
            if not feeder.done():
                feeder.cancel()
            await asyncio.wait((feeder,))
        watcher, self._watcher = self._watcher, None
        if watcher is not None:
            with suppress(NATSError):
                await watcher.stop()

    async def _delete_member_consumer(self) -> None:
        raise NotImplementedError
