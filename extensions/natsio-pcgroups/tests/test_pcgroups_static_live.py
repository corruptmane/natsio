"""Static consumer groups against a real nats-server (pinned 2.14, -js).

The two-member split reproduces the oracle's ``TestStatic`` exactly: a stream
whose subject transform partitions ``*.*`` over two partitions, 30 published
messages of which 20 match the group's filters, and a 10/10 split between the
members.
"""

import asyncio
import json
from contextlib import suppress
from datetime import timedelta

import pytest
from natsio.pcgroups import (  # ty: ignore[unresolved-import]
    PRIORITY_GROUP,
    ConsumerGroupConfigError,
    ConsumerGroupError,
    ConsumerGroupExistsError,
    ConsumerGroupNotFoundError,
    GroupConfigChangedError,
    MemberMapping,
    MemberNotInGroupError,
    PartitionedMsg,
    StaticConsumeContext,
    create_static,
    delete_static,
    get_static_config,
    list_static_active_members,
    list_static_groups,
    static_consume,
    static_member_step_down,
)
from natsio.testing import NatsServerProcess  # ty: ignore[unresolved-import]

import natsio
from conftest import GROUP_TEMPLATE, Recorder, wait_for_any, wait_for_total  # ty: ignore[unresolved-import]
from natsio.errors import ConnectionClosedError
from natsio.jetstream import (
    AckPolicy,
    ConsumerConfig,
    ConsumerNotFoundError,
    JetStreamContext,
    PriorityPolicy,
    StreamConfig,
    SubjectTransform,
)

STREAM = "test"
GROUP = "group"

# Failover tests need ack_wait >= 2s: with a shorter one the pull expiry floor
# (1s) collides with the pinned TTL and the pin can flap between instances,
# which is exactly the caveat the oracle's README documents.
HA_TEMPLATE = ConsumerConfig(max_ack_pending=1, ack_wait=timedelta(seconds=3), ack_policy=AckPolicy.EXPLICIT)


@pytest.fixture
async def js(nc: natsio.Client) -> JetStreamContext:
    """A JetStream context with the oracle's partitioned test stream."""
    js = nc.jetstream()
    await js.create_stream(
        StreamConfig(
            name=STREAM,
            subjects=["foo.*", "bar.*", "bad.*"],
            # Partition on the second token, over 2 partitions, on ingest.
            subject_transform=SubjectTransform(src="*.*", dest="{{partition(2,2)}}.{{wildcard(1)}}.{{wildcard(2)}}"),
        )
    )
    return js


async def publish_batch(
    js: JetStreamContext, prefixes: tuple[str, ...] = ("foo", "bad", "bar"), count: int = 10
) -> None:
    for prefix in prefixes:
        for index in range(count):
            await js.publish(f"{prefix}.{index}", b"payload")


class TestAdmin:
    async def test_create_get_and_list(self, js: JetStreamContext) -> None:
        created = await create_static(js, STREAM, GROUP, 2, ["foo.*", "bar.*"], ["m1", "m2"], [])
        assert created.max_members == 2
        assert created.members == ["m1", "m2"]
        assert await get_static_config(js, STREAM, GROUP) == created
        assert await list_static_groups(js, STREAM) == [GROUP]
        assert await list_static_groups(js, "other") == []

    async def test_create_is_idempotent(self, js: JetStreamContext) -> None:
        first = await create_static(js, STREAM, GROUP, 2, ["foo.*"], ["m1", "m2"], [])
        again = await create_static(js, STREAM, GROUP, 2, ["foo.*"], ["m1", "m2"], [])
        assert first == again

    async def test_create_with_a_different_config_is_refused(self, js: JetStreamContext) -> None:
        await create_static(js, STREAM, GROUP, 2, ["foo.*"], ["m1", "m2"], [])
        with pytest.raises(ConsumerGroupExistsError):
            await create_static(js, STREAM, GROUP, 2, ["foo.*"], ["m1", "m3"], [])

    async def test_missing_group_reads_raise(self, js: JetStreamContext) -> None:
        with pytest.raises(ConsumerGroupNotFoundError):
            await get_static_config(js, STREAM, "nope")  # bucket does not exist yet
        await create_static(js, STREAM, GROUP, 2, ["foo.*"], ["m1"], [])
        with pytest.raises(ConsumerGroupNotFoundError):
            await get_static_config(js, STREAM, "nope")  # bucket exists, key does not

    async def test_stored_bytes_match_the_oracle(self, js: JetStreamContext) -> None:
        await create_static(js, STREAM, GROUP, 2, ["foo.*", "bar.*"], ["m1", "m2"], [])
        kv = await js.key_value("static-consumer-groups")
        entry = await kv.get(f"{STREAM}.{GROUP}")
        assert json.loads(entry.value) == {
            "max_members": 2,
            "filters": ["foo.*", "bar.*"],
            "members": ["m1", "m2"],
        }

    async def test_invalid_config_is_refused_before_any_write(self, js: JetStreamContext) -> None:
        with pytest.raises(ConsumerGroupConfigError, match="max number of members"):
            await create_static(js, STREAM, GROUP, 0, ["foo.*"], ["m1"], [])
        # Nothing was written: the bucket was never even created.
        with pytest.raises(ConsumerGroupNotFoundError):
            await list_static_groups(js, STREAM)


class TestConsume:
    async def test_two_members_split_the_partitions(self, js: JetStreamContext) -> None:
        """The oracle's TestStatic: 20 matching messages, 10 to each member."""
        await publish_batch(js)
        await create_static(js, STREAM, GROUP, 2, ["foo.*", "bar.*"], ["m1", "m2"], [])

        m1, m2 = Recorder("m1"), Recorder("m2")
        async with (
            await static_consume(js, STREAM, GROUP, "m1", m1, GROUP_TEMPLATE),
            await static_consume(js, STREAM, GROUP, "m2", m2, GROUP_TEMPLATE),
        ):
            await m1.wait_for_count(10)
            await m2.wait_for_count(10)

        assert (m1.count, m2.count) == (10, 10)
        # Subjects arrive as they were published: the partition token is gone.
        assert all(subject.startswith(("foo.", "bar.")) for subject in m1.subjects + m2.subjects)
        # ...and no member ever saw the other's partition.
        assert set(m1.partitions) == {0}
        assert set(m2.partitions) == {1}
        # "bad.*" is outside the group's filters and stays unconsumed.
        assert not any(subject.startswith("bad.") for subject in m1.subjects + m2.subjects)

    async def test_group_without_filters_covers_the_whole_stream(self, js: JetStreamContext) -> None:
        await publish_batch(js)
        await create_static(js, STREAM, GROUP, 2, None, ["m1", "m2"], [])
        m1, m2 = Recorder("m1"), Recorder("m2")
        async with (
            await static_consume(js, STREAM, GROUP, "m1", m1, GROUP_TEMPLATE),
            await static_consume(js, STREAM, GROUP, "m2", m2, GROUP_TEMPLATE),
        ):
            await m1.wait_for_count(15)
            await m2.wait_for_count(15)
        assert m1.count + m2.count == 30  # bad.* included this time

    async def test_member_mappings_assign_partitions_explicitly(self, js: JetStreamContext) -> None:
        await publish_batch(js, prefixes=("foo",))
        await create_static(
            js,
            STREAM,
            GROUP,
            2,
            ["foo.*"],
            None,
            [MemberMapping(member="m1", partitions=[0]), MemberMapping(member="m2", partitions=[1])],
        )
        m1, m2 = Recorder("m1"), Recorder("m2")
        async with (
            await static_consume(js, STREAM, GROUP, "m1", m1, GROUP_TEMPLATE),
            await static_consume(js, STREAM, GROUP, "m2", m2, GROUP_TEMPLATE),
        ):
            # The hash decides how the 10 subjects split, so only the total is
            # predictable — but every message must reach exactly one member.
            await wait_for_total([m1, m2], 10)
            stream = await js.stream(STREAM)
            info = await stream.consumer_info("group-m1")
            # Mappings always produce "<partition>.>" filters (oracle parity).
            assert info.config.filter_subjects == ["0.>"]
        assert m1.count + m2.count == 10
        assert set(m1.partitions) <= {0}
        assert set(m2.partitions) <= {1}

    async def test_member_mappings_with_several_filters(self, js: JetStreamContext) -> None:
        """Regression: mappings + >1 filter must not ask for a duplicate filter.

        The oracle emits "<partition>.>" once per configured filter, which the
        server rejects with err_code 10138 and leaves the member unable to
        create a consumer at all.
        """
        await publish_batch(js, prefixes=("foo", "bar"))
        await create_static(
            js,
            STREAM,
            GROUP,
            2,
            ["foo.*", "bar.*"],
            None,
            [MemberMapping(member="m1", partitions=[0]), MemberMapping(member="m2", partitions=[1])],
        )
        m1, m2 = Recorder("m1"), Recorder("m2")
        async with (
            await static_consume(js, STREAM, GROUP, "m1", m1, GROUP_TEMPLATE),
            await static_consume(js, STREAM, GROUP, "m2", m2, GROUP_TEMPLATE),
        ):
            await wait_for_total([m1, m2], 20)
            stream = await js.stream(STREAM)
            assert (await stream.consumer_info("group-m1")).config.filter_subjects == ["0.>"]
        assert m1.count + m2.count == 20

    async def test_consumer_config_is_group_owned(self, js: JetStreamContext) -> None:
        await create_static(js, STREAM, GROUP, 2, ["foo.*"], ["m1", "m2"], [])
        template = ConsumerConfig(
            max_ack_pending=1,
            ack_wait=timedelta(seconds=3),
            ack_policy=AckPolicy.EXPLICIT,
            description="mine",
            # Deliberately wrong: the group overwrites these.
            durable_name="ignored",
            filter_subject="ignored.>",
            priority_groups=["WRONG"],
        )
        async with await static_consume(js, STREAM, GROUP, "m1", Recorder(), template):
            stream = await js.stream(STREAM)
            info = await stream.consumer_info("group-m1")
        config = info.config
        assert info.name == "group-m1"
        assert config.filter_subjects == ["0.foo.*"]
        assert config.filter_subject is None
        assert config.priority_groups == [PRIORITY_GROUP]
        assert config.priority_policy is PriorityPolicy.PINNED_CLIENT
        # PinnedTTL == max(ack_wait, 1s), on the wire as `priority_timeout`.
        assert config.priority_timeout == timedelta(seconds=3)
        # ...and the caller's own fields survive untouched.
        assert config.max_ack_pending == 1
        assert config.ack_wait == timedelta(seconds=3)
        assert config.description == "mine"

    async def test_template_is_not_mutated(self, js: JetStreamContext) -> None:
        await create_static(js, STREAM, GROUP, 2, ["foo.*"], ["m1"], [])
        template = ConsumerConfig(ack_policy=AckPolicy.EXPLICIT)
        async with await static_consume(js, STREAM, GROUP, "m1", Recorder(), template):
            pass
        assert template.durable_name is None
        assert template.filter_subjects is None
        assert template.priority_groups is None
        assert template.ack_wait is None

    async def test_join_rejects_a_member_outside_the_membership(self, js: JetStreamContext) -> None:
        await create_static(js, STREAM, GROUP, 2, ["foo.*"], ["m1", "m2"], [])
        with pytest.raises(MemberNotInGroupError):
            await static_consume(js, STREAM, GROUP, "m3", Recorder(), GROUP_TEMPLATE)

    async def test_join_requires_the_group_to_exist(self, js: JetStreamContext) -> None:
        with pytest.raises(ConsumerGroupNotFoundError):
            await static_consume(js, STREAM, "ghost", "m1", Recorder(), GROUP_TEMPLATE)

    async def test_active_members(self, js: JetStreamContext) -> None:
        # One member owning both stream partitions, so the probe message is
        # guaranteed to reach it (which is what proves it is polling).
        await create_static(js, STREAM, GROUP, 2, ["foo.*"], ["m1"], [])
        assert await list_static_active_members(js, STREAM, GROUP) == []
        recorder = Recorder("m1")
        async with await static_consume(js, STREAM, GROUP, "m1", recorder, GROUP_TEMPLATE):
            await publish_batch(js, prefixes=("foo",), count=1)
            await recorder.wait_for_count(1)
            active = await list_static_active_members(js, STREAM, GROUP)
        assert active == ["m1"]


class TestHighAvailability:
    async def test_only_one_instance_of_a_member_is_served(self, js: JetStreamContext) -> None:
        await publish_batch(js, prefixes=("foo",))
        # max_members=2 so the single member owns BOTH stream partitions.
        await create_static(js, STREAM, GROUP, 2, ["foo.*"], ["m1"], [])
        first, second = Recorder("first"), Recorder("second")
        async with (
            await static_consume(js, STREAM, GROUP, "m1", first, HA_TEMPLATE),
            await static_consume(js, STREAM, GROUP, "m1", second, HA_TEMPLATE),
        ):
            await wait_for_any(first.wait_for_count(10), second.wait_for_count(10))
        # Everything went to the pinned instance; the other was a hot standby.
        assert {first.count, second.count} == {10, 0}

    async def test_step_down_hands_over_to_the_other_instance(self, js: JetStreamContext) -> None:
        # max_members=2 so the single member owns BOTH stream partitions.
        await create_static(js, STREAM, GROUP, 2, ["foo.*"], ["m1"], [])
        first, second = Recorder("first"), Recorder("second")
        stream = await js.stream(STREAM)

        async def pinned_client_id() -> str | None:
            info = await stream.consumer_info("group-m1")
            groups = info.extra.get("priority_groups") or []
            return groups[0].get("pinned_client_id") if groups else None

        async with (
            await static_consume(js, STREAM, GROUP, "m1", first, HA_TEMPLATE),
            await static_consume(js, STREAM, GROUP, "m1", second, HA_TEMPLATE),
        ):
            await js.publish("foo.0", b"payload")
            # Whoever gets it is the pinned instance; the other is standby.
            await wait_for_any(first.wait_for_count(1), second.wait_for_count(1))
            assert first.count + second.count == 1
            before = await pinned_client_id()
            standby = second if first.count else first

            # Unpinning frees the slot; the next pull re-pins, which may be the
            # same instance, so retry (bounded) until the standby is served.
            for attempt in range(8):
                await static_member_step_down(js, STREAM, GROUP, "m1")
                await js.publish(f"foo.{attempt + 1}", b"payload")
                try:
                    await standby.wait_for_count(1, timeout=6.0)
                    break
                except TimeoutError:
                    continue
            assert standby.count >= 1, "the standby instance never took over"
            assert await pinned_client_id() != before


class TestTermination:
    async def test_delete_stops_members_and_sweeps_consumers(self, js: JetStreamContext) -> None:
        await create_static(js, STREAM, GROUP, 2, ["foo.*"], ["m1", "m2"], [])
        context = await static_consume(js, STREAM, GROUP, "m1", Recorder(), GROUP_TEMPLATE)
        await delete_static(js, STREAM, GROUP)

        async with asyncio.timeout(20):
            await context.wait()  # returns cleanly: a deleted group is a normal end
        assert context.error is None

        stream = await js.stream(STREAM)
        with pytest.raises(ConsumerNotFoundError):
            await stream.consumer_info("group-m1")
        with pytest.raises(ConsumerGroupNotFoundError):
            await get_static_config(js, STREAM, GROUP)

    async def test_config_change_terminates_every_member(self, js: JetStreamContext) -> None:
        await create_static(js, STREAM, GROUP, 2, ["foo.*"], ["m1", "m2"], [])
        context = await static_consume(js, STREAM, GROUP, "m1", Recorder(), GROUP_TEMPLATE)

        kv = await js.key_value("static-consumer-groups")
        await kv.put(f"{STREAM}.{GROUP}", json.dumps({"max_members": 2, "members": ["m1", "m2", "m3"]}).encode())

        with pytest.raises(GroupConfigChangedError):
            async with asyncio.timeout(20):
                await context.wait()
        assert isinstance(context.error, GroupConfigChangedError)
        # The member cleans up after itself: a stale consumer would keep
        # holding partitions the new config no longer gives it.
        stream = await js.stream(STREAM)
        with pytest.raises(ConsumerNotFoundError):
            await stream.consumer_info("group-m1")

    async def test_unparseable_config_terminates_the_member(self, js: JetStreamContext) -> None:
        await create_static(js, STREAM, GROUP, 2, ["foo.*"], ["m1"], [])
        context = await static_consume(js, STREAM, GROUP, "m1", Recorder(), GROUP_TEMPLATE)
        kv = await js.key_value("static-consumer-groups")
        await kv.put(f"{STREAM}.{GROUP}", b"not json at all")
        with pytest.raises(ConsumerGroupError, match="unusable config"):
            async with asyncio.timeout(20):
                await context.wait()

    async def test_stop_is_idempotent_and_deterministic(self, js: JetStreamContext) -> None:
        await create_static(js, STREAM, GROUP, 2, ["foo.*"], ["m1"], [])
        context = await static_consume(js, STREAM, GROUP, "m1", Recorder(), GROUP_TEMPLATE)
        await context.stop()
        await context.stop()
        await context.wait()
        assert context.error is None
        # stop() leaves the durable consumer in place: this member may come back.
        stream = await js.stream(STREAM)
        assert (await stream.consumer_info("group-m1")).name == "group-m1"

    async def test_closing_the_client_ends_the_session(self, js: JetStreamContext, server: NatsServerProcess) -> None:
        """Closing the connection terminates the session — never a hang.

        The invariant is the termination one: every parked await has a wake path
        from the `Closed` lifecycle event. Whether the teardown reaches the
        session as a task cancellation (clean) or as a `ConnectionClosedError`
        surfaced from a mid-flight watcher pull (loud) is a race not worth
        pinning; what must always hold is that the session ENDS within the
        deadline and `stop()` stays safe over the dead connection.
        """
        await create_static(js, STREAM, GROUP, 2, ["foo.*"], ["m1"], [])
        other = await natsio.connect(server.url, connect_timeout=5.0, request_timeout=5.0)
        context = await static_consume(other.jetstream(), STREAM, GROUP, "m1", Recorder(), GROUP_TEMPLATE)
        await other.close()
        async with asyncio.timeout(15):
            with suppress(ConnectionClosedError):
                await context.wait()  # returns (clean) or raises ConnectionClosedError (loud)
            await context.stop()  # still idempotent over a dead connection
        assert context.error is None or isinstance(context.error, ConnectionClosedError)

    async def test_stop_from_inside_the_handler(self, js: JetStreamContext) -> None:
        """Self-teardown must not cancel the handler's own task."""
        await create_static(js, STREAM, GROUP, 2, ["foo.*"], ["m1"], [])
        holder: dict[str, StaticConsumeContext] = {}
        survived = asyncio.Event()

        async def handler(msg: PartitionedMsg) -> None:
            await msg.ack()
            await holder["context"].stop()
            await asyncio.sleep(0)  # still alive after stopping ourselves
            survived.set()

        context = await static_consume(js, STREAM, GROUP, "m1", handler, GROUP_TEMPLATE)
        holder["context"] = context
        await js.publish("foo.1", b"payload")

        async with asyncio.timeout(20):
            await survived.wait()
            await context.wait()
        assert context.error is None
