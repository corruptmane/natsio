"""Consumer group wire entities and the pinned wire contract.

Every constant and every JSON field name here is byte-pinned to
[`orbit.go/pcgroups`](https://github.com/synadia-io/orbit.go/tree/main/pcgroups)
so a group created by this library is administered and consumed by the Go one
and vice versa. The oracle files are named per symbol; the whole contract is
asserted in `tests/test_pcgroups_wire.py`.
"""

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Final

from natsio._internal.jsonmodel import JsonModel

from .errors import ConsumerGroupConfigError

__all__ = [
    "DEFAULT_ACK_WAIT",
    "ELASTIC_BUCKET",
    "MIN_PULL_EXPIRY_PINNED_TTL",
    "PRIORITY_GROUP",
    "STATIC_BUCKET",
    "ElasticConsumerGroupConfig",
    "MemberMapping",
    "PartitioningFilter",
    "StaticConsumerGroupConfig",
    "validate_elastic_config",
    "validate_static_config",
]

# -- pinned wire contract ----------------------------------------------------
# KV buckets holding the group configs, keyed "<stream>.<group>".
# Oracle: static_stream_consumer_group.go `kvStaticBucketName`,
# elastic_stream_consumer_group.go `kvElasticBucketName`.
STATIC_BUCKET: Final = "static-consumer-groups"
ELASTIC_BUCKET: Final = "elastic-consumer-groups"

# ADR-42 priority group every member consumer is created with, and the group
# named on every pull. Oracle: stream_consumer_group.go `priorityGroupName`.
PRIORITY_GROUP: Final = "PCG"

# Fault-reactivity timers are all derived from the caller's AckWait.
# Oracle: stream_consumer_group.go `defaultAckWait`, `minPullExpiryPinnedTTL`,
# `pullTimeoutDivider`, `consumerIdleTimeoutFactor`.
DEFAULT_ACK_WAIT: Final = timedelta(seconds=5)
MIN_PULL_EXPIRY_PINNED_TTL: Final = timedelta(seconds=1)
PULL_TIMEOUT_DIVIDER: Final = 2
CONSUMER_IDLE_TIMEOUT_FACTOR: Final = 1


@dataclass(slots=True, kw_only=True)
class MemberMapping(JsonModel):
    """An explicit partition assignment for one member.

    The alternative to a balanced membership list: every partition in
    ``[0, max_members)`` must be claimed exactly once across all mappings.
    """

    member: str = ""
    partitions: list[int] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class PartitioningFilter(JsonModel):
    """An elastic group's filter plus which of its wildcards key the partition.

    ``partitioning_wildcards`` holds 1-based indexes of the ``*`` tokens in
    ``filter`` (so ``"foo.*.*"`` with ``[1]`` partitions on the second subject
    token only). Empty means the whole subject is the partitioning key.
    """

    filter: str = ""
    partitioning_wildcards: list[int] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class StaticConsumerGroupConfig(JsonModel):
    """A static group's config, as stored in the ``static-consumer-groups`` KV.

    Immutable for the life of the group: any change stops every member
    instance (see `GroupConfigChangedError`). Either ``members`` (balanced
    distribution) or ``member_mappings`` (explicit), never both.
    """

    max_members: int = 0
    filters: list[str] | None = None
    members: list[str] | None = None
    member_mappings: list[MemberMapping] | None = None

    def __post_init__(self) -> None:
        # Go tags these three `omitempty`, which omits nil AND empty slices.
        # Normalizing empty to None keeps our JSON byte-identical, and that
        # matters: CreateStatic compares member_mappings with reflect.DeepEqual,
        # where an empty slice is NOT equal to a nil one — a stored "[]" would
        # make an otherwise-identical Go create fail with "doesn't match ours".
        self.filters = self.filters or None
        self.members = self.members or None
        self.member_mappings = self.member_mappings or None

    def is_in_membership(self, member: str) -> bool:
        """Whether ``member`` appears in the membership list or the mappings."""
        return member in (self.members or ()) or any(m.member == member for m in self.member_mappings or ())


@dataclass(slots=True, kw_only=True)
class ElasticConsumerGroupConfig(JsonModel):
    """An elastic group's config, as stored in the ``elastic-consumer-groups`` KV.

    ``max_members``, ``partitioning_filters`` and the two buffering limits are
    fixed at creation (they define the sourced work-queue stream); membership
    is administratively changeable at any time.

    ``max_buffered_msg`` is spelled singular because that is the pinned JSON
    key (orbit.go tags ``MaxBufferedMsgs`` as ``max_buffered_msg``); it is the
    max number of messages buffered in the group's work-queue stream.
    """

    max_members: int = 0
    partitioning_filters: list[PartitioningFilter] | None = None
    max_buffered_msg: int | None = None
    max_buffered_bytes: int | None = None
    members: list[str] | None = None
    member_mappings: list[MemberMapping] | None = None

    def __post_init__(self) -> None:
        # Same `omitempty` normalization as the static config. The buffering
        # limits are ints with `omitempty`, so Go omits 0 — None is our "unset"
        # and 0 collapses onto it so a round-trip through either client is
        # stable. `partitioning_filters` has NO omitempty in Go: an empty list
        # stays an emitted "[]" (what a Go client writes for an empty slice).
        # Go writes nil as `null`; our JsonModel omits a None field entirely.
        # Decode-safe on both sides (`null` and an absent key both read back as
        # None/nil, and Go's `reflect.DeepEqual(nil, nil)` idempotency still
        # holds) — but NOT byte-identical for the filterless case, so we don't
        # claim it is.
        self.members = self.members or None
        self.member_mappings = self.member_mappings or None
        if self.max_buffered_msg == 0:
            self.max_buffered_msg = None
        if self.max_buffered_bytes == 0:
            self.max_buffered_bytes = None

    def is_in_membership(self, member: str) -> bool:
        """Whether ``member`` appears in the membership list or the mappings."""
        return member in (self.members or ()) or any(m.member == member for m in self.member_mappings or ())


def _validate_membership(max_members: int, members: list[str] | None, mappings: list[MemberMapping] | None) -> None:
    """Shared members/member_mappings validation (identical in both flavours).

    Oracle: `validateStaticConfig` / `validateConfig`. Members are deliberately
    tolerant (deduplicated and capped when the filters are generated); mappings
    are strict — they must partition ``[0, max_members)`` exactly.
    """
    if max_members < 1:
        raise ConsumerGroupConfigError("the max number of members must be >= 1")
    if members and mappings:
        raise ConsumerGroupConfigError("either members or member mappings must be provided, not both")
    if not mappings:
        return
    if len(mappings) > max_members:
        raise ConsumerGroupConfigError("the number of member mappings must be between 1 and the max number of members")
    seen_members: set[str] = set()
    seen_partitions: set[int] = set()
    for mapping in mappings:
        if mapping.member in seen_members:
            raise ConsumerGroupConfigError("member names must be unique")
        seen_members.add(mapping.member)
        for partition in mapping.partitions:
            if partition in seen_partitions:
                raise ConsumerGroupConfigError("partition numbers must be used only once")
            seen_partitions.add(partition)
            if partition < 0 or partition >= max_members:
                raise ConsumerGroupConfigError(
                    "partition numbers must be between 0 and one less than the max number of members"
                )
    if len(seen_partitions) != max_members:
        raise ConsumerGroupConfigError(
            "the number of unique partition numbers specified in the mappings "
            "must be equal to the max number of members"
        )


def validate_static_config(config: StaticConsumerGroupConfig) -> None:
    """Validate a static group config, raising `ConsumerGroupConfigError`."""
    _validate_membership(config.max_members, config.members, config.member_mappings)


def validate_elastic_config(config: ElasticConsumerGroupConfig) -> None:
    """Validate an elastic group config, raising `ConsumerGroupConfigError`.

    Adds the partitioning-filter rules on top of the shared membership rules: a
    filter must be partitionable (at least one ``*``, or a trailing ``>``) and
    every partitioning wildcard index must name a distinct ``*`` in it.
    """
    # Checked here as well as in _validate_membership so the failure ORDER
    # matches the oracle's (max members, then filters, then membership).
    if config.max_members < 1:
        raise ConsumerGroupConfigError("the max number of members must be >= 1")
    for pfilter in config.partitioning_filters or ():
        if not pfilter.filter:
            raise ConsumerGroupConfigError("partitioning filters must have a non-empty filter")
        tokens = pfilter.filter.split(".")
        wildcards = tokens.count("*")
        if wildcards == 0 and tokens[-1] != ">":
            raise ConsumerGroupConfigError("partitioning filters must have at least one * wildcard or end with >")
        if len(pfilter.partitioning_wildcards) > wildcards:
            raise ConsumerGroupConfigError(
                "the number of partitioning wildcards must not be larger than "
                "the total number of * wildcards in the filter"
            )
        seen: set[int] = set()
        for index in pfilter.partitioning_wildcards:
            if index in seen:
                raise ConsumerGroupConfigError("partitioning wildcard indexes must be unique")
            seen.add(index)
            if index < 1 or index > wildcards:
                raise ConsumerGroupConfigError(
                    "partitioning wildcard indexes must be between 1 and the number of * wildcards in the filter"
                )
    _validate_membership(config.max_members, config.members, config.member_mappings)
