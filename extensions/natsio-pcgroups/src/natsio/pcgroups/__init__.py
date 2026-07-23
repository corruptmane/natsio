"""Partitioned consumer groups over JetStream — static and elastic.

A consumer group splits a stream into ``max_members`` partitions keyed by
subject, and hands each *member* a fixed subset of them. Members consume in
parallel while messages that share a partitioning key are still processed one
at a time and in order — the throughput a single ``max_ack_pending=1`` consumer
cannot give you.

Two flavours, mirroring
[`orbit.go/pcgroups`](https://github.com/synadia-io/orbit.go/tree/main/pcgroups):

**Static** — the stream already carries the partition number as the first
subject token (put there by a stream subject transform on ingest). One durable
JetStream consumer per member, created straight on that stream. The membership
is fixed for the life of the group.

    await create_static(js, "ORDERS", "cg", 10, ["orders.*"], ["m1", "m2"])
    ctx = await static_consume(js, "ORDERS", "cg", "m1", handle)

**Elastic** — the stream needs no partition token. Creating the group creates a
sourced *work-queue* stream that inserts the partition token on the way in, and
the membership can change at any time; running members start and stop consuming
as they are added and dropped.

    await create_elastic(js, "ORDERS", "cg", 10, [PartitioningFilter(filter="orders.*", partitioning_wildcards=[1])])
    ctx = await elastic_consume(js, "ORDERS", "cg", "m1", handle)
    await add_members(js, "ORDERS", "cg", ["m1", "m2"])

In both cases the handler is invoked with the partition token already stripped
from the subject, and running several instances of the same member gives you
hot-standby HA: the server's ADR-42 pinned-client priority group serves exactly
one instance at a time.

    async def handle(msg: PartitionedMsg) -> None:
        print(msg.subject, msg.data)   # the subject as it was before partitioning
        await msg.ack()

Requires nats-server 2.11+ (priority groups, subject transforms).
"""

from natsio.pcgroups.elastic import (
    ElasticConsumeContext,
    add_members,
    create_elastic,
    delete_elastic,
    delete_member_mappings,
    delete_members,
    elastic_consume,
    elastic_is_in_membership_and_active,
    elastic_member_step_down,
    get_elastic_config,
    list_elastic_active_members,
    list_elastic_groups,
    set_member_mappings,
)
from natsio.pcgroups.entities import (
    DEFAULT_ACK_WAIT,
    ELASTIC_BUCKET,
    PRIORITY_GROUP,
    STATIC_BUCKET,
    ElasticConsumerGroupConfig,
    MemberMapping,
    PartitioningFilter,
    StaticConsumerGroupConfig,
    validate_elastic_config,
    validate_static_config,
)
from natsio.pcgroups.errors import (
    ConsumerGroupConfigError,
    ConsumerGroupError,
    ConsumerGroupExistsError,
    ConsumerGroupNotFoundError,
    GroupConfigChangedError,
    MemberNotInGroupError,
)
from natsio.pcgroups.partitions import (
    compose_group_stream_name,
    compose_key,
    compose_static_consumer_name,
    elastic_get_partition_filters,
    generate_partition_filters,
    partitioning_transform_destination,
    static_get_partition_filters,
    strip_partition,
)
from natsio.pcgroups.session import ConsumerGroupConsumeContext, MessageHandler, PartitionedMsg
from natsio.pcgroups.static import (
    StaticConsumeContext,
    create_static,
    delete_static,
    get_static_config,
    list_static_active_members,
    list_static_groups,
    static_consume,
    static_member_step_down,
)

__all__ = [
    "DEFAULT_ACK_WAIT",
    "ELASTIC_BUCKET",
    "PRIORITY_GROUP",
    "STATIC_BUCKET",
    "ConsumerGroupConfigError",
    "ConsumerGroupConsumeContext",
    "ConsumerGroupError",
    "ConsumerGroupExistsError",
    "ConsumerGroupNotFoundError",
    "ElasticConsumeContext",
    "ElasticConsumerGroupConfig",
    "GroupConfigChangedError",
    "MemberMapping",
    "MemberNotInGroupError",
    "MessageHandler",
    "PartitionedMsg",
    "PartitioningFilter",
    "StaticConsumeContext",
    "StaticConsumerGroupConfig",
    "add_members",
    "compose_group_stream_name",
    "compose_key",
    "compose_static_consumer_name",
    "create_elastic",
    "create_static",
    "delete_elastic",
    "delete_member_mappings",
    "delete_members",
    "delete_static",
    "elastic_consume",
    "elastic_get_partition_filters",
    "elastic_is_in_membership_and_active",
    "elastic_member_step_down",
    "generate_partition_filters",
    "get_elastic_config",
    "get_static_config",
    "list_elastic_active_members",
    "list_elastic_groups",
    "list_static_active_members",
    "list_static_groups",
    "partitioning_transform_destination",
    "set_member_mappings",
    "static_consume",
    "static_get_partition_filters",
    "static_member_step_down",
    "strip_partition",
    "validate_elastic_config",
    "validate_static_config",
]
