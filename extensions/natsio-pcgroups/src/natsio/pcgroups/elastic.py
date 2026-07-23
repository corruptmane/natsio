"""Elastic consumer groups: a sourced work-queue stream, changeable membership.

The origin stream needs no partition token. Creating the group creates a
**work-queue stream** named ``"<stream>-<group>"`` that sources from the origin
and inserts the partition number as the first subject token with a
``{{Partition(...)}}`` subject transform. Members then consume from *that*
stream, and their partition assignment can be changed administratively at any
time — every running member instance watches the config and re-joins.

Faithful port of `orbit.go/pcgroups/elastic_stream_consumer_group.go`.
"""

import asyncio
import json
import random
from contextlib import suppress
from typing import Any

from natsio.errors import NATSError
from natsio.jetstream import (
    AckPolicy,
    APIError,
    Consumer,
    ConsumerConfig,
    ConsumerNotFoundError,
    DiscardPolicy,
    JetStreamContext,
    RetentionPolicy,
    StorageType,
    Stream,
    StreamConfig,
    StreamSource,
    SubjectTransform,
)
from natsio.jetstream.errors import WrongLastSequenceError
from natsio.kv import KeyExistsError, KeyNotFoundError, KeyValue

from .buckets import open_bucket, open_or_create_bucket, require_names
from .entities import (
    CONSUMER_IDLE_TIMEOUT_FACTOR,
    DEFAULT_ACK_WAIT,
    ELASTIC_BUCKET,
    PRIORITY_GROUP,
    ElasticConsumerGroupConfig,
    MemberMapping,
    PartitioningFilter,
    validate_elastic_config,
)
from .errors import (
    ConsumerGroupConfigError,
    ConsumerGroupError,
    ConsumerGroupExistsError,
    ConsumerGroupNotFoundError,
    GroupConfigChangedError,
)
from .partitions import (
    compose_group_stream_name,
    compose_key,
    elastic_get_partition_filters,
    partitioning_transform_destination,
)
from .session import ConsumerGroupConsumeContext, MessageHandler

__all__ = [
    "ElasticConsumeContext",
    "add_members",
    "create_elastic",
    "delete_elastic",
    "delete_member_mappings",
    "delete_members",
    "elastic_consume",
    "elastic_get_partition_filters",
    "elastic_is_in_membership_and_active",
    "elastic_member_step_down",
    "get_elastic_config",
    "list_elastic_active_members",
    "list_elastic_groups",
    "set_member_mappings",
]

# Server error: "filtered consumer not unique on workqueue stream". Expected
# (and transient) while members converge on a new membership, since two members
# briefly hold overlapping filters. Oracle: elastic_stream_consumer_group.go
# checks the same code and stays quiet about it.
_WQ_CONSUMER_NOT_UNIQUE: int = 10100


def _decode(payload: bytes) -> ElasticConsumerGroupConfig:
    try:
        data: dict[str, Any] = json.loads(payload)
    except ValueError as exc:
        raise ConsumerGroupError(f"the elastic consumer group config is not valid JSON: {exc}") from exc
    config = ElasticConsumerGroupConfig.from_wire(data)
    validate_elastic_config(config)
    return config


async def _fetch(kv: KeyValue, stream: str, group: str) -> tuple[ElasticConsumerGroupConfig, int]:
    """The stored config and its KV revision (elastic updates are CAS-gated)."""
    require_names(stream, group)
    try:
        entry = await kv.get(compose_key(stream, group))
    except KeyNotFoundError as exc:
        raise ConsumerGroupNotFoundError(f"no elastic consumer group {group!r} on stream {stream!r}") from exc
    return _decode(entry.value), entry.revision


async def _store(kv: KeyValue, stream: str, group: str, config: ElasticConsumerGroupConfig, revision: int) -> None:
    """Compare-and-set the config back.

    The oracle uses an unguarded put for the mapping calls and a CAS for the
    membership ones; this is CAS throughout, so a concurrent administrator
    loses loudly (`WrongLastSequenceError`) instead of silently clobbering.
    """
    await kv.update(compose_key(stream, group), json.dumps(config.to_wire()).encode(), last=revision)


def _group_stream_config(
    stream: str,
    group: str,
    config: ElasticConsumerGroupConfig,
    *,
    replicas: int,
    storage: StorageType,
) -> StreamConfig:
    """The sourced work-queue stream that carries the partitioned messages.

    One subject transform per partitioning filter (or a single ``>`` transform
    when there are none), each prefixing the computed partition number. Discard
    policy is ``new`` so a full buffer pauses sourcing instead of dropping
    messages — the whole point of the buffering limits.
    """
    filters = config.partitioning_filters or [PartitioningFilter(filter=">", partitioning_wildcards=[])]
    transforms = [
        SubjectTransform(
            src=pfilter.filter,
            dest=partitioning_transform_destination(pfilter.filter, pfilter.partitioning_wildcards, config.max_members),
        )
        for pfilter in filters
    ]
    return StreamConfig(
        name=compose_group_stream_name(stream, group),
        retention=RetentionPolicy.WORK_QUEUE,
        num_replicas=replicas,
        storage=storage,
        max_msgs=config.max_buffered_msg if config.max_buffered_msg is not None else 0,
        max_bytes=config.max_buffered_bytes if config.max_buffered_bytes is not None else 0,
        discard=DiscardPolicy.NEW,
        sources=[StreamSource(name=stream, subject_transforms=transforms)],
        allow_direct=True,
    )


async def create_elastic(
    js: JetStreamContext,
    stream: str,
    group: str,
    max_members: int,
    partitioning_filters: list[PartitioningFilter] | None = None,
    max_buffered_msgs: int = -1,
    max_buffered_bytes: int = -1,
) -> ElasticConsumerGroupConfig:
    """Create an elastic consumer group and its sourced work-queue stream.

    ``max_members`` is the number of partitions and the ceiling on membership.
    ``partitioning_filters`` select which subjects the group covers and which
    of their ``*`` wildcards key the partitioning; with none, the group covers
    the whole stream and partitions on the entire subject.

    The group starts with **no members** — add them with `add_members` (or
    assign partitions explicitly with `set_member_mappings`). Member instances
    may join and wait before that happens.

    ``max_buffered_msgs`` / ``max_buffered_bytes`` bound the work-queue stream
    (-1 = unlimited). When it fills, sourcing pauses for at least a second, so
    size it above a second's worth of consumption.

    Idempotent for an identical config; a different one raises
    `ConsumerGroupExistsError`.
    """
    require_names(stream, group)
    config = ElasticConsumerGroupConfig(
        max_members=max_members,
        partitioning_filters=partitioning_filters,
        max_buffered_msg=max_buffered_msgs,
        max_buffered_bytes=max_buffered_bytes,
    )
    validate_elastic_config(config)

    source = await js.stream(stream)  # must exist: StreamNotFoundError otherwise
    stream_config = source.cached_info.config
    replicas, storage = stream_config.num_replicas, stream_config.storage

    kv = await open_or_create_bucket(js, ELASTIC_BUCKET, replicas)
    key = compose_key(stream, group)
    stored = config
    try:
        await kv.create(key, json.dumps(config.to_wire()).encode())
    except (KeyExistsError, WrongLastSequenceError):
        stored = _decode((await kv.get(key)).value)
        if (
            stored.max_members != config.max_members
            or stored.partitioning_filters != config.partitioning_filters
            or stored.max_buffered_msg != config.max_buffered_msg
            or stored.max_buffered_bytes != config.max_buffered_bytes
        ):
            raise ConsumerGroupExistsError(
                f"elastic consumer group {group!r} on stream {stream!r} already exists with a different config; "
                "delete it and create a new one"
            ) from None

    await js.create_stream(_group_stream_config(stream, group, stored, replicas=replicas, storage=storage))
    return stored


async def get_elastic_config(js: JetStreamContext, stream: str, group: str) -> ElasticConsumerGroupConfig:
    """The stored config of an elastic consumer group."""
    kv = await open_bucket(js, ELASTIC_BUCKET)
    config, _ = await _fetch(kv, stream, group)
    return config


async def delete_elastic(js: JetStreamContext, stream: str, group: str) -> None:
    """Delete an elastic consumer group: its config, then its work-queue stream.

    Deleting the config stops every running member; deleting the stream takes
    their consumers (and anything still buffered) with it.
    """
    require_names(stream, group)
    kv = await open_bucket(js, ELASTIC_BUCKET)
    with suppress(KeyNotFoundError):
        await kv.delete(compose_key(stream, group))
    await js.delete_stream(compose_group_stream_name(stream, group))


async def list_elastic_groups(js: JetStreamContext, stream: str) -> list[str]:
    """The names of every elastic consumer group defined on ``stream``."""
    kv = await open_bucket(js, ELASTIC_BUCKET)
    groups: list[str] = []
    for key in await kv.keys():
        head, dot, rest = key.partition(".")
        if dot and head == stream and rest:
            groups.append(rest)
    return groups


async def add_members(js: JetStreamContext, stream: str, group: str, members: list[str]) -> list[str]:
    """Add members to an elastic group; returns the new membership.

    Rejected for a group using explicit member mappings — change those with
    `set_member_mappings`. The membership is stored sorted and deduplicated
    (the oracle stores it in map order, which is random; the partition
    assignment sorts it anyway, so this only makes the stored bytes stable).
    """
    require_names(stream, group)
    if not members:
        raise ConsumerGroupError("no member names to add")
    kv = await open_bucket(js, ELASTIC_BUCKET)
    config, revision = await _fetch(kv, stream, group)
    if config.member_mappings:
        raise ConsumerGroupConfigError("cannot add members to an elastic consumer group that uses member mappings")
    config.members = sorted({*(config.members or ()), *(name for name in members if name)})
    await _store(kv, stream, group, config, revision)
    return config.members


async def delete_members(js: JetStreamContext, stream: str, group: str, members: list[str]) -> list[str]:
    """Drop members from an elastic group; returns the remaining membership."""
    require_names(stream, group)
    if not members:
        raise ConsumerGroupError("no member names to drop")
    kv = await open_bucket(js, ELASTIC_BUCKET)
    config, revision = await _fetch(kv, stream, group)
    if config.member_mappings:
        raise ConsumerGroupConfigError("cannot drop members from an elastic consumer group that uses member mappings")
    dropping = set(members)
    remaining = [name for name in config.members or () if name not in dropping]
    config.members = remaining or None
    await _store(kv, stream, group, config, revision)
    return remaining


async def set_member_mappings(
    js: JetStreamContext, stream: str, group: str, member_mappings: list[MemberMapping]
) -> None:
    """Replace the balanced membership with explicit partition assignments.

    Every partition in ``[0, max_members)`` must be claimed exactly once; any
    existing ``members`` list is cleared.
    """
    require_names(stream, group)
    if not member_mappings:
        raise ConsumerGroupError("no member mappings given")
    kv = await open_bucket(js, ELASTIC_BUCKET)
    config, revision = await _fetch(kv, stream, group)
    config.members = None
    config.member_mappings = member_mappings
    validate_elastic_config(config)
    await _store(kv, stream, group, config, revision)


async def delete_member_mappings(js: JetStreamContext, stream: str, group: str) -> None:
    """Drop the explicit partition assignments.

    Mirrors the oracle: the members list is *not* restored, so the group is
    left with no membership at all and nothing is consumed until members are
    added back.
    """
    require_names(stream, group)
    kv = await open_bucket(js, ELASTIC_BUCKET)
    config, revision = await _fetch(kv, stream, group)
    config.member_mappings = None
    await _store(kv, stream, group, config, revision)


async def _member_consumer_names(js: JetStreamContext, stream: str, group: str) -> set[str]:
    cg_stream = await js.stream(compose_group_stream_name(stream, group))
    return {name async for name in cg_stream.consumer_names()}


async def list_elastic_active_members(js: JetStreamContext, stream: str, group: str) -> list[str]:
    """Members of an elastic group that currently have a consumer on its stream."""
    kv = await open_bucket(js, ELASTIC_BUCKET)
    config, _ = await _fetch(kv, stream, group)
    members = config.members or [mapping.member for mapping in config.member_mappings or ()]
    if not members:
        return []
    names = await _member_consumer_names(js, stream, group)
    return [member for member in members if member in names]


async def elastic_is_in_membership_and_active(
    js: JetStreamContext, stream: str, group: str, member: str
) -> tuple[bool, bool]:
    """``(in_membership, is_active)`` for one member of an elastic group.

    Note: the oracle's version of this check answers "is *any* member active",
    not "is *this* member active" (it never compares against the member name).
    This port answers the documented question.
    """
    kv = await open_bucket(js, ELASTIC_BUCKET)
    config, _ = await _fetch(kv, stream, group)
    names = await _member_consumer_names(js, stream, group)
    return config.is_in_membership(member), member in names


async def elastic_member_step_down(js: JetStreamContext, stream: str, group: str, member: str) -> None:
    """Force the currently pinned instance of ``member`` to step down (ADR-42 unpin)."""
    cg_stream = await js.stream(compose_group_stream_name(stream, group))
    consumer = await cg_stream.consumer(member)
    await consumer.unpin(PRIORITY_GROUP)


class ElasticConsumeContext(ConsumerGroupConsumeContext[ElasticConsumerGroupConfig]):
    """The consume context `elastic_consume` returns.

    Watches the group config: membership changes start or stop this instance's
    consumption, while a change to the immutable part (max members, buffering
    limits, partitioning filters) ends the session with
    `GroupConfigChangedError`.
    """

    __slots__ = ("_cg_stream",)

    def __init__(
        self,
        js: JetStreamContext,
        kv: KeyValue,
        stream: str,
        group: str,
        member: str,
        handler: MessageHandler,
        template: ConsumerConfig,
        config: ElasticConsumerGroupConfig,
        cg_stream: Stream,
    ) -> None:
        super().__init__(js, kv, stream, group, member, handler, template, config)
        self._cg_stream = cg_stream

    @property
    def _idle_period(self) -> float | None:
        # The self-correcting tick. A member that should be consuming but isn't
        # (its consumer was deleted and re-created by another member's view of
        # the membership, or a create lost a race) retries here; the period
        # trails the consumer's inactive threshold so a stale consumer has been
        # reaped by the time we try again.
        return self._ack_wait.total_seconds() * CONSUMER_IDLE_TIMEOUT_FACTOR + 0.5

    async def _initial_join(self) -> None:
        # Never fatal: joining before being added to the membership is the
        # documented way to use an elastic group.
        if self._config.is_in_membership(self._member):
            await self._join()

    def _parse_config(self, payload: dict[str, Any]) -> ElasticConsumerGroupConfig:
        config = ElasticConsumerGroupConfig.from_wire(payload)
        validate_elastic_config(config)
        return config

    async def _apply_config(self, config: ElasticConsumerGroupConfig) -> None:
        current = self._config
        if (
            config.max_members != current.max_members
            or config.max_buffered_msg != current.max_buffered_msg
            or config.max_buffered_bytes != current.max_buffered_bytes
            or config.partitioning_filters != current.partitioning_filters
        ):
            self._fail(
                GroupConfigChangedError(
                    f"the immutable config of elastic consumer group {self._group!r} on stream "
                    f"{self._stream_name!r} changed (max members, buffering limits or partitioning filters); "
                    "delete and re-create the group instead"
                )
            )
            return
        unchanged = config.members == current.members and config.member_mappings == current.member_mappings
        if unchanged and self._consumer is not None:
            return
        current.members = config.members
        current.member_mappings = config.member_mappings
        await self._process_membership_change()

    async def _on_idle_tick(self) -> None:
        if self._consumer is None and self._config.is_in_membership(self._member):
            await self._join()

    async def _on_worker_ended(self) -> None:
        error = self._worker_error
        from_handler = self._worker_error_from_handler
        await self._stop_worker()  # clears both fields — read them first
        if error is None:
            return
        if from_handler:
            # A handler fault is the user's bug, not membership churn. Treating
            # it as recoverable would redeliver the same poison message forever
            # (max_ack_pending=1 head-of-line blocks the whole partition). Fail
            # fatally, exactly as the static path does.
            self._fail(error)
            return
        # A consume-iteration/infra error IS recoverable here: another member's
        # view of the membership legitimately deletes and re-creates our
        # consumer. The idle tick re-joins.
        self._note_recovered(error, "consume session ended")

    async def _process_membership_change(self) -> None:
        """Re-join with the new partition assignment.

        The consumer must be **deleted** rather than updated: partitions moving
        to this member can need an earlier stream position than the consumer
        currently holds. Only the pinned instance deletes it — the others back
        off briefly so the pinned one wins the race and the members don't
        thrash a work-queue stream that refuses overlapping filters.
        """
        pinned = False
        if self._consumer is not None:
            pinned = await self._is_pinned(self._consumer)
            await self._stop_worker()
        if pinned:
            try:
                await self._cg_stream.delete_consumer(self._member)
            except ConsumerNotFoundError:
                pass
            except NATSError as exc:
                self._note_recovered(exc, "could not delete the member consumer")
        else:
            delay = random.uniform(0.4, 0.5)
            with suppress(TimeoutError):
                async with asyncio.timeout(delay):
                    await self._stopped.wait()  # the backoff's wake path
            if self._stopped.is_set():
                return
        await self._join()

    async def _is_pinned(self, consumer: Consumer) -> bool:
        """Whether THIS instance is the pinned client of the member's consumer.

        Both halves need natsio internals: the pin id the client is replaying
        lives in a private dict, and `ConsumerInfo` does not model ADR-42
        priority-group state, so it arrives in ``extra``.
        """
        pin = consumer._pin_ids.get(PRIORITY_GROUP)
        if not pin:
            return False
        try:
            info = await self._cg_stream.consumer_info(self._member)
        except NATSError:
            return False  # the consumer may already be gone; treat as not pinned
        states: Any = info.extra.get("priority_groups") or ()
        return any(state.get("group") == PRIORITY_GROUP and state.get("pinned_client_id") == pin for state in states)

    async def _join(self) -> None:
        filters = elastic_get_partition_filters(self._config, self._member)
        if not filters:
            return  # not in the membership (or mapped to no partitions)
        config = self._member_consumer_config(self._member, filters, durable=False)
        # Ephemeral-ish: a member instance that stops pulling has its consumer
        # reaped, which is what lets the group self-heal after a membership
        # change nobody was around to apply.
        config.inactive_threshold = self._ack_wait * CONSUMER_IDLE_TIMEOUT_FACTOR
        try:
            consumer = await self._create_member_consumer(self._cg_stream, self._member, config)
        except APIError as exc:
            if exc.err_code == _WQ_CONSUMER_NOT_UNIQUE:
                # Expected while members converge: counted, not logged.
                self._recovered += 1
                self._last_recovered_error = exc
            else:
                self._note_recovered(exc, "could not create the member consumer")
            return
        except NATSError as exc:
            self._note_recovered(exc, "could not create the member consumer")
            return
        self._start_worker(consumer)

    async def _delete_member_consumer(self) -> None:
        with suppress(ConsumerNotFoundError):
            await self._cg_stream.delete_consumer(self._member)


async def elastic_consume(
    js: JetStreamContext,
    stream: str,
    group: str,
    member: str,
    handler: MessageHandler,
    config: ConsumerConfig | None = None,
) -> ElasticConsumeContext:
    """Join an elastic consumer group as ``member`` and consume while in it.

    Unlike `natsio.pcgroups.static_consume`, the member does **not** have to be
    in the membership yet: the session waits, and starts consuming as soon as
    it is added (and stops again when dropped).

    ``handler`` is an async callable invoked once per message, serially, with
    the partition token already stripped from `PartitionedMsg.subject`. It
    **must** acknowledge every message — an elastic group consumes from a
    work-queue stream, so ``ack_policy`` must be ``explicit`` (the default).

    ``config`` is a template; the group owns the consumer's name, filters,
    priority-group block and inactive threshold. ``ack_wait`` (default 5s)
    drives the failover and self-correction timers.

    Returns a context whose `stop()` ends the session. Consumption also ends on
    its own when the group is deleted (normally) or its immutable config
    changes (`GroupConfigChangedError`).
    """
    template = ConsumerConfig.from_wire(config.to_wire()) if config is not None else ConsumerConfig()
    if template.ack_policy is not AckPolicy.EXPLICIT:
        raise ConsumerGroupConfigError(
            "the ack policy for an elastic consumer group must be explicit "
            f"(got {template.ack_policy.value!r}): its stream is a work queue"
        )
    if template.ack_wait is None or template.ack_wait.total_seconds() <= 0:
        template.ack_wait = DEFAULT_ACK_WAIT

    cg_stream = await js.stream(compose_group_stream_name(stream, group))
    kv = await open_bucket(js, ELASTIC_BUCKET)
    group_config, _ = await _fetch(kv, stream, group)
    session = ElasticConsumeContext(js, kv, stream, group, member, handler, template, group_config, cg_stream)
    await session._start()
    return session
