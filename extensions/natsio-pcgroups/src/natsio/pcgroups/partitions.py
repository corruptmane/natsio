"""The partition assignment algorithm and the naming/subject conventions.

Pure logic, no I/O: everything here is a faithful port of
`orbit.go/pcgroups/stream_consumer_group.go` (`GeneratePartitionFilters`,
`composeKey`) and `elastic_stream_consumer_group.go`
(`getPartitioningTransformDest`, `composeCGSName`), differential-tested against
the oracle's own expectations in `tests/test_pcgroups_units.py`.
"""

from .entities import ElasticConsumerGroupConfig, MemberMapping, StaticConsumerGroupConfig

__all__ = [
    "compose_group_stream_name",
    "compose_key",
    "compose_static_consumer_name",
    "elastic_get_partition_filters",
    "generate_partition_filters",
    "partitioning_transform_destination",
    "static_get_partition_filters",
    "strip_partition",
]


def compose_key(stream: str, group: str) -> str:
    """The group's KV key: ``"<stream>.<group>"``."""
    return f"{stream}.{group}"


def compose_static_consumer_name(group: str, member: str) -> str:
    """A static group's per-member JetStream consumer name: ``"<group>-<member>"``.

    (Elastic groups name the consumer after the member alone — it lives on the
    group's own stream, so there is nothing to disambiguate.)
    """
    return f"{group}-{member}"


def compose_group_stream_name(stream: str, group: str) -> str:
    """An elastic group's sourced work-queue stream: ``"<stream>-<group>"``."""
    return f"{stream}-{group}"


def strip_partition(subject: str) -> str:
    """Drop the leading partition-number token from a delivered subject.

    A subject with no dot is returned unchanged — the oracle's behaviour, and
    the only sane one: there is no partition token to strip.
    """
    head, dot, rest = subject.partition(".")
    return rest if dot else head


def generate_partition_filters(
    members: list[str] | None,
    max_members: int,
    member_mappings: list[MemberMapping] | None,
    member: str,
    subject_filter: str,
) -> list[str]:
    """The partition filters one member owns, as ``"<partition>.<subject_filter>"``.

    ``members`` (a balanced distribution) wins over ``member_mappings`` (an
    explicit one) when both are somehow present. The membership list is
    deduplicated, sorted and capped to ``max_members`` first, so every member
    computes the same assignment from the same config without coordinating.

    The balanced distribution hands each member a contiguous run of
    ``max_members // len(members)`` partitions and then deals the remainder
    round-robin from the front — chosen so that adding or removing a member
    moves as few partitions as possible.

    Note that the mapping branch ignores ``subject_filter`` and always emits
    ``"<partition>.>"``. That is the oracle's behaviour and it is load-bearing
    for interop, not an oversight of this port.
    """
    if members:
        chosen = sorted(dict.fromkeys(members))[:max_members]
        count = len(chosen)
        per = max_members // count  # >= 1: the list was capped to max_members
        balanced = count * per
        filters: list[str] = []
        for partition in range(max_members):
            # Contiguous runs first; the leftover partitions are dealt round
            # robin from the front of the (sorted) membership.
            owner = chosen[(partition // per) % count if partition < balanced else (partition - balanced) % count]
            if owner == member:
                filters.append(f"{partition}.{subject_filter}")
        return filters
    if member_mappings:
        return [
            f"{partition}.>"
            for mapping in member_mappings
            if mapping.member == member
            for partition in mapping.partitions
        ]
    return []


def _combine(groups: list[list[str]]) -> list[str]:
    """Flatten per-filter partition filters, dropping exact duplicates.

    Deduplication is a **deliberate divergence** from the oracle. Its mapping
    branch ignores the subject filter and always emits ``"<partition>.>"``, so
    a group that combines member mappings with more than one filter asks the
    server for the same filter twice — which every server rejects with
    ``consumer subject filters cannot overlap`` (err_code 10138), leaving that
    combination permanently unable to create a consumer. Dropping the duplicate
    asks for exactly the same subjects, so nothing about the group's meaning
    changes; only the impossible request does.
    """
    return list(dict.fromkeys(item for group in groups for item in group))


def static_get_partition_filters(config: StaticConsumerGroupConfig, member: str) -> list[str]:
    """Every partition filter a member of a static group consumes.

    One filter per (partition, configured filter) pair; with no configured
    filters the group covers the whole stream (``">"``).
    """
    if config.filters:
        return _combine(
            [
                generate_partition_filters(
                    config.members, config.max_members, config.member_mappings, member, subject_filter
                )
                for subject_filter in config.filters
            ]
        )
    return generate_partition_filters(config.members, config.max_members, config.member_mappings, member, ">")


def elastic_get_partition_filters(config: ElasticConsumerGroupConfig, member: str) -> list[str]:
    """Every partition filter a member of an elastic group consumes.

    The filters address the group's *sourced* stream, where the partition
    number has already been prepended by the subject transform.
    """
    if config.partitioning_filters:
        return _combine(
            [
                generate_partition_filters(
                    config.members, config.max_members, config.member_mappings, member, pfilter.filter
                )
                for pfilter in config.partitioning_filters
            ]
        )
    return generate_partition_filters(config.members, config.max_members, config.member_mappings, member, ">")


def partitioning_transform_destination(
    subject_filter: str, partitioning_wildcards: list[int] | None, max_members: int
) -> str:
    """The subject-transform destination that prepends the partition number.

    ``"foo.*.*.>"`` with wildcards ``[1, 2]`` over 4 partitions becomes
    ``"{{Partition(4,1,2)}}.foo.{{Wildcard(1)}}.{{Wildcard(2)}}.>"``. With no
    partitioning wildcards the whole subject is the key, so the destination is
    ``"{{Partition(4)}}.<filter with its wildcards mapped>"``.
    """
    tokens = subject_filter.split(".")
    index = 1
    for position, token in enumerate(tokens):
        if token == "*":
            tokens[position] = f"{{{{Wildcard({index})}}}}"
            index += 1
    destination = ".".join(tokens)
    if not partitioning_wildcards:
        return f"{{{{Partition({max_members})}}}}.{destination}"
    keys = ",".join(str(wildcard) for wildcard in partitioning_wildcards)
    return f"{{{{Partition({max_members},{keys})}}}}.{destination}"
