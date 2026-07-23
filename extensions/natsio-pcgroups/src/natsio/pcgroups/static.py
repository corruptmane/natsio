"""Static consumer groups: one JetStream consumer per member, fixed membership.

The stream must already carry the partition number as the first subject token
(a stream-level subject transform does that on ingest). Creating the group
writes its config to the ``static-consumer-groups`` KV bucket; each member
instance then creates the member's own durable consumer with the partition
filters that config assigns to it.

Faithful port of `orbit.go/pcgroups/static_stream_consumer_group.go`.
"""

import json
from contextlib import suppress
from typing import Any

from natsio.errors import NATSError
from natsio.jetstream import ConsumerConfig, ConsumerNotFoundError, JetStreamContext, Stream
from natsio.jetstream.errors import WrongLastSequenceError
from natsio.kv import KeyExistsError, KeyNotFoundError, KeyValue

from .buckets import open_bucket, open_or_create_bucket, require_names
from .entities import (
    DEFAULT_ACK_WAIT,
    PRIORITY_GROUP,
    STATIC_BUCKET,
    MemberMapping,
    StaticConsumerGroupConfig,
    validate_static_config,
)
from .errors import (
    ConsumerGroupError,
    ConsumerGroupExistsError,
    ConsumerGroupNotFoundError,
    GroupConfigChangedError,
    MemberNotInGroupError,
)
from .partitions import compose_key, compose_static_consumer_name, static_get_partition_filters
from .session import ConsumerGroupConsumeContext, MessageHandler

__all__ = [
    "StaticConsumeContext",
    "create_static",
    "delete_static",
    "get_static_config",
    "list_static_active_members",
    "list_static_groups",
    "static_consume",
    "static_member_step_down",
]


def _decode(payload: bytes) -> StaticConsumerGroupConfig:
    try:
        data: dict[str, Any] = json.loads(payload)
    except ValueError as exc:
        raise ConsumerGroupError(f"the static consumer group config is not valid JSON: {exc}") from exc
    config = StaticConsumerGroupConfig.from_wire(data)
    validate_static_config(config)
    return config


async def _fetch(kv: KeyValue, stream: str, group: str) -> StaticConsumerGroupConfig:
    require_names(stream, group)
    try:
        entry = await kv.get(compose_key(stream, group))
    except KeyNotFoundError as exc:
        raise ConsumerGroupNotFoundError(f"no static consumer group {group!r} on stream {stream!r}") from exc
    return _decode(entry.value)


async def create_static(
    js: JetStreamContext,
    stream: str,
    group: str,
    max_members: int,
    filters: list[str] | None = None,
    members: list[str] | None = None,
    member_mappings: list[MemberMapping] | None = None,
) -> StaticConsumerGroupConfig:
    """Create a static consumer group and return its stored config.

    ``max_members`` is also the number of partitions. Pass either ``members``
    (balanced distribution) or ``member_mappings`` (explicit assignment), never
    both. ``filters`` are the *unpartitioned* subject filters the group covers
    (default: the whole stream); each is prefixed with a partition number per
    member.

    Idempotent for an identical config; a different one raises
    `ConsumerGroupExistsError` — a static group's membership is baked into its
    consumers, so changing it means delete and re-create.
    """
    require_names(stream, group)
    config = StaticConsumerGroupConfig(
        max_members=max_members,
        filters=filters,
        members=members,
        member_mappings=member_mappings,
    )
    validate_static_config(config)

    source = await js.stream(stream)  # must exist: StreamNotFoundError otherwise
    kv = await open_or_create_bucket(js, STATIC_BUCKET, source.cached_info.config.num_replicas)
    key = compose_key(stream, group)
    try:
        # create() rather than the oracle's put(): a concurrent creator with a
        # DIFFERENT config must lose loudly instead of silently overwriting.
        await kv.create(key, json.dumps(config.to_wire()).encode())
    except (KeyExistsError, WrongLastSequenceError):
        existing = _decode((await kv.get(key)).value)
        if existing != config:
            raise ConsumerGroupExistsError(
                f"static consumer group {group!r} on stream {stream!r} already exists with a different config"
            ) from None
        return existing
    return config


async def get_static_config(js: JetStreamContext, stream: str, group: str) -> StaticConsumerGroupConfig:
    """The stored config of a static consumer group."""
    kv = await open_bucket(js, STATIC_BUCKET)
    return await _fetch(kv, stream, group)


async def delete_static(js: JetStreamContext, stream: str, group: str) -> None:
    """Delete a static consumer group: its config, then every member consumer.

    Deleting the config is what stops the running members (their watchers see
    the deletion and shut down); the sweep here removes the consumers they
    leave behind. Failures on individual consumers are collected into an
    `ExceptionGroup` rather than hidden.
    """
    require_names(stream, group)
    source = await js.stream(stream)
    kv = await open_bucket(js, STATIC_BUCKET)
    with suppress(KeyNotFoundError):
        await kv.delete(compose_key(stream, group))

    prefix = f"{group}-"
    failures: list[Exception] = []
    async for name in source.consumer_names():
        if not name.startswith(prefix):
            continue
        try:
            await source.delete_consumer(name)
        except ConsumerNotFoundError:
            continue
        except NATSError as exc:
            failures.append(exc)
    if failures:
        raise ExceptionGroup(f"could not delete every consumer of static group {group!r}", failures)


async def list_static_groups(js: JetStreamContext, stream: str) -> list[str]:
    """The names of every static consumer group defined on ``stream``."""
    kv = await open_bucket(js, STATIC_BUCKET)
    groups: list[str] = []
    for key in await kv.keys():
        head, dot, rest = key.partition(".")
        if dot and head == stream and rest:
            groups.append(rest)
    return groups


async def list_static_active_members(js: JetStreamContext, stream: str, group: str) -> list[str]:
    """Members whose consumer currently has at least one waiting pull request.

    "Active" means some instance of that member is polling — what a live member
    looks like from the server's side.
    """
    kv = await open_bucket(js, STATIC_BUCKET)
    config = await _fetch(kv, stream, group)
    source = await js.stream(stream)
    members = config.members or [mapping.member for mapping in config.member_mappings or ()]
    active: list[str] = []
    for member in members:
        try:
            info = await source.consumer_info(compose_static_consumer_name(group, member))
        except ConsumerNotFoundError:
            continue
        if info.num_waiting != 0:
            active.append(member)
    return active


async def static_member_step_down(js: JetStreamContext, stream: str, group: str, member: str) -> None:
    """Force the currently pinned instance of ``member`` to step down (ADR-42 unpin).

    The next pull from any instance of that member — possibly the same one —
    becomes the new pinned client.
    """
    source = await js.stream(stream)
    consumer = await source.consumer(compose_static_consumer_name(group, member))
    await consumer.unpin(PRIORITY_GROUP)


class StaticConsumeContext(ConsumerGroupConsumeContext[StaticConsumerGroupConfig]):
    """The consume context `static_consume` returns.

    A static group's config never legitimately changes, so this session only
    ever reacts to the group being deleted (a normal end) or its config being
    modified (`GroupConfigChangedError`).
    """

    __slots__ = ("_source",)

    _deletes_consumer_on_group_delete = True

    def __init__(
        self,
        js: JetStreamContext,
        kv: KeyValue,
        stream: str,
        group: str,
        member: str,
        handler: MessageHandler,
        template: ConsumerConfig,
        config: StaticConsumerGroupConfig,
        source: Stream,
    ) -> None:
        super().__init__(js, kv, stream, group, member, handler, template, config)
        self._source = source

    @property
    def consumer_name(self) -> str:
        """This member's durable JetStream consumer: ``"<group>-<member>"``."""
        return compose_static_consumer_name(self._group, self._member)

    async def _initial_join(self) -> None:
        filters = static_get_partition_filters(self._config, self._member)
        if not filters:
            return  # in the membership with no partitions mapped: nothing to consume
        config = self._member_consumer_config(self.consumer_name, filters, durable=True)
        consumer = await self._create_member_consumer(self._source, self.consumer_name, config)
        self._start_worker(consumer)

    def _parse_config(self, payload: dict[str, Any]) -> StaticConsumerGroupConfig:
        config = StaticConsumerGroupConfig.from_wire(payload)
        validate_static_config(config)
        return config

    async def _apply_config(self, config: StaticConsumerGroupConfig) -> None:
        if config == self._config:
            return  # the watcher's initial delivery, or a rewrite of the same bytes
        self._delete_consumer_on_teardown = True
        self._fail(
            GroupConfigChangedError(
                f"the config of static consumer group {self._group!r} on stream {self._stream_name!r} changed; "
                "a static group's membership is immutable, so this member instance stops"
            )
        )

    async def _on_worker_ended(self) -> None:
        error = self._worker_error
        await self._stop_worker()
        if await self._group_was_deleted():
            # `delete_static` deletes the config and then sweeps the member
            # consumers; whichever reaches us first, the group is gone and this
            # is a normal end, not a fault.
            self._stopped.set()
            return
        self._fail(
            error
            if error is not None
            else ConsumerGroupError(f"the consume session for member {self._member!r} ended unexpectedly")
        )

    async def _group_was_deleted(self) -> bool:
        try:
            await self._kv.get(self._key)
        except KeyNotFoundError:  # includes KeyDeletedError
            return True
        except NATSError:
            return False  # can't tell (connection trouble): treat the fault as real
        return False

    async def _delete_member_consumer(self) -> None:
        with suppress(ConsumerNotFoundError):
            await self._source.delete_consumer(self.consumer_name)


async def static_consume(
    js: JetStreamContext,
    stream: str,
    group: str,
    member: str,
    handler: MessageHandler,
    config: ConsumerConfig | None = None,
) -> StaticConsumeContext:
    """Join a static consumer group as ``member`` and start consuming.

    ``handler`` is an async callable invoked once per message, serially, with
    the partition token already stripped from `PartitionedMsg.subject`. It must
    acknowledge according to the ack policy in ``config``.

    ``config`` is a template: the group owns the consumer's name, its filters
    and the whole ADR-42 priority-group block; everything else (``ack_policy``,
    ``max_ack_pending``, ``deliver_policy``, …) is yours. ``ack_wait`` sets the
    failover reactivity — the pinned TTL and the pull expiry derive from it —
    and defaults to 5 seconds.

    Run several instances with the same ``member`` for HA: the server pins
    exactly one of them and the rest idle as hot standbys.

    Returns a context whose `stop()` ends the session. Consumption also ends on
    its own when the group is deleted (normally) or its config changes at all
    (`GroupConfigChangedError`).
    """
    template = ConsumerConfig.from_wire(config.to_wire()) if config is not None else ConsumerConfig()
    if template.ack_wait is None or template.ack_wait.total_seconds() <= 0:
        template.ack_wait = DEFAULT_ACK_WAIT

    source = await js.stream(stream)
    kv = await open_bucket(js, STATIC_BUCKET)
    group_config = await _fetch(kv, stream, group)
    if not group_config.is_in_membership(member):
        raise MemberNotInGroupError(
            f"member {member!r} is not in the membership of static consumer group {group!r} on stream {stream!r}"
        )
    session = StaticConsumeContext(js, kv, stream, group, member, handler, template, group_config, source)
    await session._start()
    return session
