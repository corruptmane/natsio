"""Elastic consumer groups against a real nats-server (pinned 2.14, -js).

The membership walk reproduces the oracle's ``TestElastic``: a plain (not
pre-partitioned) stream, a group over two partitions, then m1 alone takes all
20 matching messages, m2 joins and takes its half of the next batch, m1 is
dropped and m2 takes everything.
"""

import asyncio
import json
from datetime import timedelta

import pytest
from natsio.pcgroups import (  # ty: ignore[unresolved-import]
    PRIORITY_GROUP,
    ConsumerGroupConfigError,
    ConsumerGroupExistsError,
    ConsumerGroupNotFoundError,
    GroupConfigChangedError,
    MemberMapping,
    PartitionedMsg,
    PartitioningFilter,
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

import natsio
from conftest import GROUP_TEMPLATE, Recorder, wait_for_any, wait_for_total  # ty: ignore[unresolved-import]
from natsio.jetstream import (
    AckPolicy,
    ConsumerConfig,
    DiscardPolicy,
    JetStreamContext,
    PriorityPolicy,
    RetentionPolicy,
    StreamConfig,
    StreamNotFoundError,
)

STREAM = "test"
GROUP = "group"
CG_STREAM = "test-group"
FILTERS = [
    PartitioningFilter(filter="foo.*", partitioning_wildcards=[1]),
    PartitioningFilter(filter="bar.*", partitioning_wildcards=[1]),
]
HA_TEMPLATE = ConsumerConfig(max_ack_pending=1, ack_wait=timedelta(seconds=3), ack_policy=AckPolicy.EXPLICIT)


@pytest.fixture
async def js(nc: natsio.Client) -> JetStreamContext:
    js = nc.jetstream()
    await js.create_stream(StreamConfig(name=STREAM, subjects=["foo.*", "bar.*", "bad.*"]))
    return js


async def publish_batch(js: JetStreamContext, count: int = 10) -> None:
    """10 each of foo.*, bad.* and bar.* — 20 of the 30 match the group."""
    for prefix in ("foo", "bad", "bar"):
        for index in range(count):
            await js.publish(f"{prefix}.{index}", b"payload")


class TestCreate:
    async def test_creates_the_sourced_work_queue_stream(self, js: JetStreamContext) -> None:
        config = await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)
        assert config.max_members == 2
        assert config.members is None  # a new group has no membership yet

        info = await js.stream_info(CG_STREAM)
        assert info.config.retention is RetentionPolicy.WORK_QUEUE
        assert info.config.discard is DiscardPolicy.NEW
        assert info.config.allow_direct is True
        assert info.config.max_msgs == -1
        assert info.config.max_bytes == -1
        sources = info.config.sources or []
        assert [source.name for source in sources] == [STREAM]
        assert [(t.src, t.dest) for t in sources[0].subject_transforms or []] == [
            ("foo.*", "{{Partition(2,1)}}.foo.{{Wildcard(1)}}"),
            ("bar.*", "{{Partition(2,1)}}.bar.{{Wildcard(1)}}"),
        ]

    async def test_without_filters_partitions_the_whole_subject(self, js: JetStreamContext) -> None:
        await create_elastic(js, STREAM, GROUP, 4)
        info = await js.stream_info(CG_STREAM)
        sources = info.config.sources or []
        assert [(t.src, t.dest) for t in sources[0].subject_transforms or []] == [(">", "{{Partition(4)}}.>")]

    async def test_buffering_limits_are_applied(self, js: JetStreamContext) -> None:
        await create_elastic(js, STREAM, GROUP, 2, FILTERS, 500, 1_000_000)
        info = await js.stream_info(CG_STREAM)
        assert (info.config.max_msgs, info.config.max_bytes) == (500, 1_000_000)
        stored = await get_elastic_config(js, STREAM, GROUP)
        assert (stored.max_buffered_msg, stored.max_buffered_bytes) == (500, 1_000_000)

    async def test_stored_bytes_match_the_oracle(self, js: JetStreamContext) -> None:
        await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)
        kv = await js.key_value("elastic-consumer-groups")
        entry = await kv.get(f"{STREAM}.{GROUP}")
        assert json.loads(entry.value) == {
            "max_members": 2,
            "partitioning_filters": [
                {"filter": "foo.*", "partitioning_wildcards": [1]},
                {"filter": "bar.*", "partitioning_wildcards": [1]},
            ],
            "max_buffered_msg": -1,
            "max_buffered_bytes": -1,
        }

    async def test_create_is_idempotent(self, js: JetStreamContext) -> None:
        first = await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)
        again = await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)
        assert first == again

    async def test_create_with_a_different_config_is_refused(self, js: JetStreamContext) -> None:
        await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)
        with pytest.raises(ConsumerGroupExistsError):
            await create_elastic(js, STREAM, GROUP, 4, FILTERS, -1, -1)

    async def test_membership_does_not_block_recreation(self, js: JetStreamContext) -> None:
        await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)
        await add_members(js, STREAM, GROUP, ["m1"])
        # Only the immutable half has to match; the membership is expected to move.
        again = await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)
        assert again.members == ["m1"]

    async def test_lists_groups(self, js: JetStreamContext) -> None:
        await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)
        assert await list_elastic_groups(js, STREAM) == [GROUP]

    async def test_missing_group_reads_raise(self, js: JetStreamContext) -> None:
        with pytest.raises(ConsumerGroupNotFoundError):
            await get_elastic_config(js, STREAM, GROUP)


class TestMembershipAdmin:
    @pytest.fixture(autouse=True)
    async def group(self, js: JetStreamContext) -> None:
        await create_elastic(js, STREAM, GROUP, 4, FILTERS, -1, -1)

    async def test_add_and_drop(self, js: JetStreamContext) -> None:
        assert await add_members(js, STREAM, GROUP, ["m2", "m1"]) == ["m1", "m2"]
        assert await add_members(js, STREAM, GROUP, ["m1", "m3"]) == ["m1", "m2", "m3"]
        assert await delete_members(js, STREAM, GROUP, ["m2"]) == ["m1", "m3"]
        assert (await get_elastic_config(js, STREAM, GROUP)).members == ["m1", "m3"]

    async def test_dropping_everyone_leaves_no_members(self, js: JetStreamContext) -> None:
        await add_members(js, STREAM, GROUP, ["m1"])
        assert await delete_members(js, STREAM, GROUP, ["m1"]) == []
        assert (await get_elastic_config(js, STREAM, GROUP)).members is None

    async def test_mappings_replace_the_membership_list(self, js: JetStreamContext) -> None:
        await add_members(js, STREAM, GROUP, ["m1"])
        await set_member_mappings(
            js,
            STREAM,
            GROUP,
            [MemberMapping(member="m1", partitions=[0, 1]), MemberMapping(member="m2", partitions=[2, 3])],
        )
        config = await get_elastic_config(js, STREAM, GROUP)
        assert config.members is None
        assert config.member_mappings is not None
        assert [m.member for m in config.member_mappings] == ["m1", "m2"]

    async def test_mappings_are_validated(self, js: JetStreamContext) -> None:
        with pytest.raises(ConsumerGroupConfigError):
            await set_member_mappings(js, STREAM, GROUP, [MemberMapping(member="m1", partitions=[0])])

    async def test_add_and_drop_refuse_a_mapped_group(self, js: JetStreamContext) -> None:
        await set_member_mappings(js, STREAM, GROUP, [MemberMapping(member="m1", partitions=[0, 1, 2, 3])])
        with pytest.raises(ConsumerGroupConfigError):
            await add_members(js, STREAM, GROUP, ["m2"])
        with pytest.raises(ConsumerGroupConfigError):
            await delete_members(js, STREAM, GROUP, ["m1"])

    async def test_delete_mappings_leaves_the_group_empty(self, js: JetStreamContext) -> None:
        await set_member_mappings(js, STREAM, GROUP, [MemberMapping(member="m1", partitions=[0, 1, 2, 3])])
        await delete_member_mappings(js, STREAM, GROUP)
        config = await get_elastic_config(js, STREAM, GROUP)
        # Mirrors the oracle: the members list is NOT restored.
        assert config.member_mappings is None
        assert config.members is None


class TestConsume:
    async def test_membership_walk(self, js: JetStreamContext) -> None:
        """The oracle's TestElastic, end to end."""
        await publish_batch(js)
        await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)

        m1, m2 = Recorder("m1"), Recorder("m2")
        async with (
            await elastic_consume(js, STREAM, GROUP, "m1", m1, GROUP_TEMPLATE) as c1,
            await elastic_consume(js, STREAM, GROUP, "m2", m2, GROUP_TEMPLATE) as c2,
        ):
            # Joining before being in the membership is legal and idle.
            assert not c1.consuming
            assert not c2.consuming

            # m1 alone owns both partitions and takes all 20 matching messages.
            await add_members(js, STREAM, GROUP, ["m1"])
            await m1.wait_for_count(20)
            assert m2.count == 0, "a member outside the membership must not consume"

            # m2 joins: it now owns partition 1. How the next batch splits
            # depends on how fast each member applies the change, so the
            # deterministic claims are the total and which partitions each one
            # is allowed to see.
            await add_members(js, STREAM, GROUP, ["m2"])
            await c2.wait_for_consuming(timeout=20)
            await publish_batch(js)
            await wait_for_total([m1, m2], 40)
            assert m2.count > 0, "an added member must start consuming"
            assert set(m2.partitions) == {1}, "a member must only ever see its own partitions"

            # m1 is dropped: m2 takes over partition 0 as well. It can only do
            # that once m1's consumer is gone — a work-queue stream refuses
            # overlapping filters — so seeing partition 0 here proves the
            # handover happened.
            await delete_members(js, STREAM, GROUP, ["m1"])
            await publish_batch(js)
            await wait_for_total([m1, m2], 60)
            assert 0 in m2.partitions, "the remaining member must take over the dropped one's partitions"

            # Converged: from here m1 must consume nothing at all.
            settled = m1.count
            target = m2.count + 20
            await publish_batch(js)
            await m2.wait_for_count(target)
            assert m1.count == settled, "a dropped member must stop consuming"
            assert c1.error is None

        # "bad.*" never matched a partitioning filter, so it was never sourced.
        assert not any(subject.startswith("bad.") for subject in m1.subjects + m2.subjects)
        assert m1.count + m2.count == 80

    async def test_subject_and_partition_are_exposed(self, js: JetStreamContext) -> None:
        await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)
        m1 = Recorder("m1")
        async with await elastic_consume(js, STREAM, GROUP, "m1", m1, GROUP_TEMPLATE):
            await add_members(js, STREAM, GROUP, ["m1"])
            await publish_batch(js, count=3)
            await m1.wait_for_count(6)
        assert all(subject.startswith(("foo.", "bar.")) for subject in m1.subjects)
        # The sole member owns every partition; which subject lands where is
        # the server's hash to decide.
        assert set(m1.partitions) <= {0, 1}

    async def test_consumer_is_group_owned(self, js: JetStreamContext) -> None:
        await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)
        async with (
            await elastic_consume(js, STREAM, GROUP, "m1", Recorder(), GROUP_TEMPLATE),
            await elastic_consume(js, STREAM, GROUP, "m2", Recorder(), GROUP_TEMPLATE) as c2,
        ):
            await add_members(js, STREAM, GROUP, ["m1", "m2"])
            await c2.wait_for_consuming(timeout=20)
            cg = await js.stream(CG_STREAM)
            info = await cg.consumer_info("m2")
        assert info.name == "m2"
        assert info.config.priority_groups == [PRIORITY_GROUP]
        assert info.config.priority_policy is PriorityPolicy.PINNED_CLIENT
        assert info.config.priority_timeout == timedelta(seconds=1)
        assert info.config.inactive_threshold == timedelta(seconds=1)
        assert info.config.filter_subjects == ["1.foo.*", "1.bar.*"]

    async def test_member_mappings_drive_consumption(self, js: JetStreamContext) -> None:
        await publish_batch(js, count=5)
        await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)
        m1, m2 = Recorder("m1"), Recorder("m2")
        async with (
            await elastic_consume(js, STREAM, GROUP, "m1", m1, GROUP_TEMPLATE),
            await elastic_consume(js, STREAM, GROUP, "m2", m2, GROUP_TEMPLATE),
        ):
            await set_member_mappings(
                js,
                STREAM,
                GROUP,
                [MemberMapping(member="m1", partitions=[0]), MemberMapping(member="m2", partitions=[1])],
            )
            await wait_for_total([m1, m2], 10)
        assert m1.count + m2.count == 10
        # Which subject hashes to which partition is the server's business; that
        # a member never sees another's partition is ours.
        assert set(m1.partitions) <= {0}
        assert set(m2.partitions) <= {1}

    async def test_recreates_a_consumer_deleted_underneath_it(self, js: JetStreamContext) -> None:
        """The self-correcting tick: elastic members re-join on their own.

        Membership churn legitimately deletes and re-creates member consumers,
        so a member that finds itself without one must recover without any
        further config change.
        """
        await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)
        recorder = Recorder("m1")
        async with await elastic_consume(js, STREAM, GROUP, "m1", recorder, GROUP_TEMPLATE) as context:
            await add_members(js, STREAM, GROUP, ["m1"])
            await context.wait_for_consuming(timeout=20)

            cg = await js.stream(CG_STREAM)
            await cg.delete_consumer("m1")

            # No config change follows: only the idle tick can bring it back.
            await publish_batch(js, count=2)
            await recorder.wait_for_count(4)
        assert context.error is None

    async def test_explicit_ack_policy_is_required(self, js: JetStreamContext) -> None:
        await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)
        with pytest.raises(ConsumerGroupConfigError, match="ack policy"):
            await elastic_consume(js, STREAM, GROUP, "m1", Recorder(), ConsumerConfig(ack_policy=AckPolicy.NONE))

    async def test_join_requires_the_group_to_exist(self, js: JetStreamContext) -> None:
        with pytest.raises(StreamNotFoundError):
            await elastic_consume(js, STREAM, "ghost", "m1", Recorder(), GROUP_TEMPLATE)

    async def test_active_members_and_membership_probe(self, js: JetStreamContext) -> None:
        await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)
        assert await list_elastic_active_members(js, STREAM, GROUP) == []
        assert await elastic_is_in_membership_and_active(js, STREAM, GROUP, "m1") == (False, False)

        m1 = Recorder("m1")
        async with await elastic_consume(js, STREAM, GROUP, "m1", m1, GROUP_TEMPLATE) as c1:
            await add_members(js, STREAM, GROUP, ["m1", "m2"])
            await c1.wait_for_consuming(timeout=20)
            assert await list_elastic_active_members(js, STREAM, GROUP) == ["m1"]
            assert await elastic_is_in_membership_and_active(js, STREAM, GROUP, "m1") == (True, True)
            # m2 is in the membership but nobody is running it.
            assert await elastic_is_in_membership_and_active(js, STREAM, GROUP, "m2") == (True, False)


class TestHighAvailability:
    async def test_step_down_hands_over_to_the_other_instance(self, js: JetStreamContext) -> None:
        await create_elastic(js, STREAM, GROUP, 1, FILTERS, -1, -1)
        first, second = Recorder("first"), Recorder("second")
        async with (
            await elastic_consume(js, STREAM, GROUP, "m1", first, HA_TEMPLATE),
            await elastic_consume(js, STREAM, GROUP, "m1", second, HA_TEMPLATE),
        ):
            await add_members(js, STREAM, GROUP, ["m1"])
            await js.publish("foo.0", b"payload")
            await wait_for_any(first.wait_for_count(1), second.wait_for_count(1))
            assert first.count + second.count == 1
            standby = second if first.count else first

            for attempt in range(8):
                await elastic_member_step_down(js, STREAM, GROUP, "m1")
                await js.publish(f"foo.{attempt + 1}", b"payload")
                try:
                    await standby.wait_for_count(1, timeout=6.0)
                    break
                except TimeoutError:
                    continue
            assert standby.count >= 1, "the standby instance never took over"


class TestTermination:
    async def test_delete_stops_members_and_removes_the_stream(self, js: JetStreamContext) -> None:
        await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)
        context = await elastic_consume(js, STREAM, GROUP, "m1", Recorder(), GROUP_TEMPLATE)
        await add_members(js, STREAM, GROUP, ["m1"])
        await delete_elastic(js, STREAM, GROUP)

        async with asyncio.timeout(20):
            await context.wait()
        assert context.error is None
        with pytest.raises(StreamNotFoundError):
            await js.stream_info(CG_STREAM)

    async def test_immutable_config_change_terminates_members(self, js: JetStreamContext) -> None:
        await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)
        context = await elastic_consume(js, STREAM, GROUP, "m1", Recorder(), GROUP_TEMPLATE)
        kv = await js.key_value("elastic-consumer-groups")
        await kv.put(
            f"{STREAM}.{GROUP}",
            json.dumps(
                {
                    "max_members": 4,  # was 2
                    "partitioning_filters": [{"filter": "foo.*", "partitioning_wildcards": [1]}],
                    "max_buffered_msg": -1,
                    "max_buffered_bytes": -1,
                }
            ).encode(),
        )
        with pytest.raises(GroupConfigChangedError):
            async with asyncio.timeout(20):
                await context.wait()

    async def test_stop_is_idempotent(self, js: JetStreamContext) -> None:
        await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)
        context = await elastic_consume(js, STREAM, GROUP, "m1", Recorder(), GROUP_TEMPLATE)
        await context.stop()
        await context.stop()
        await context.wait()
        assert context.error is None

    async def test_handler_exception_is_fatal_not_an_infinite_retry(self, js: JetStreamContext) -> None:
        """A poison message must terminate the member, not loop forever.

        Elastic self-heals when the *transport* deletes its consumer (see
        ``test_recreates_a_consumer_deleted_underneath_it``), so an early version
        folded a handler exception into that same recoverable path — and with
        ``max_ack_pending=1`` a single always-throwing handler head-of-line
        blocked the partition and redelivered indefinitely. A handler fault is
        the user's, and it is fatal, exactly as the static path already is.
        """
        await create_elastic(js, STREAM, GROUP, 2, FILTERS, -1, -1)

        calls = 0

        async def poison(msg: PartitionedMsg) -> None:
            nonlocal calls
            calls += 1
            raise RuntimeError("bad message")

        context = await elastic_consume(js, STREAM, GROUP, "m1", poison, GROUP_TEMPLATE)
        await add_members(js, STREAM, GROUP, ["m1"])
        await context.wait_for_consuming(timeout=20)
        await js.publish("foo.1", b"payload")

        with pytest.raises(RuntimeError, match="bad message"):
            async with asyncio.timeout(20):
                await context.wait()
        assert isinstance(context.error, RuntimeError)
        # Fatal on the first delivery — never re-driven as a recovered error.
        assert calls == 1
        assert context.recovered_errors == 0
