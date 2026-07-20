"""Regressions for the defects found reviewing the M3 public API.

Numbering follows the review report; every scenario here reproduced a hang,
a wrong exception, or silent loss before the corresponding fix.
"""

import asyncio

import pytest
from test_client import connected_client, deliver_msg, make_options

import natsio
from fake import FakeEnv, frames_written
from natsio import Client, PendingLimitPolicy
from natsio._internal.lifecycle import ConnectionState
from natsio.errors import (
    NATSError,
    SlowConsumerError,
    SubscriptionClosedError,
    TimeoutError,
)


class TestDrainDeadline:
    async def test_drain_timeout_is_enforced_with_stuck_callbacks(self) -> None:
        """Finding 1: suppressed CancelledError made drain_timeout a no-op.

        Three subscriptions whose callbacks never finish: drain must still
        return within (roughly) drain_timeout, and must close the client.
        """
        env = FakeEnv()
        client = await connected_client(env, drain_timeout=0.2)
        stuck = asyncio.Event()

        async def never_finishes(msg: natsio.Msg) -> None:
            await stuck.wait()

        subs = [client.subscribe(f"stuck.{i}", cb=never_finishes) for i in range(3)]
        for sub in subs:
            deliver_msg(env, sub.sid, sub.subject, b"x")
        await asyncio.sleep(0)  # let the callbacks start and block

        async with asyncio.timeout(2):  # far below 3 x unbounded
            await client.drain()
        assert client.status is ConnectionState.CLOSED
        stuck.set()

    async def test_drain_waits_for_iterator_backlog(self) -> None:
        """Finding 16: iterator-mode backlog must be handled before close."""
        env = FakeEnv()
        client = await connected_client(env, drain_timeout=5.0)
        sub = client.subscribe("work")
        for i in range(5):
            deliver_msg(env, sub.sid, "work", b"%d" % i)

        consumed: list[bytes] = []

        async def consume() -> None:
            async for msg in sub:
                await asyncio.sleep(0.01)
                consumed.append(msg.payload)

        consumer = asyncio.create_task(consume())
        await asyncio.sleep(0)
        await client.drain()
        await asyncio.wait_for(consumer, timeout=2)
        assert consumed == [b"0", b"1", b"2", b"3", b"4"]

    async def test_drain_while_disconnected_still_closes(self) -> None:
        """Finding 9: drain must not abort with ConnectionClosedError."""
        env = FakeEnv()
        client = await connected_client(env)
        client.subscribe("x")
        env.refuse_next(50)
        env.current.drop()
        await asyncio.sleep(0.02)  # now RECONNECTING
        async with asyncio.timeout(3):
            await client.drain()  # must not raise
        assert client.status is ConnectionState.CLOSED


class TestRequestManyDeadline:
    async def test_deadline_does_not_cancel_the_consumer_body(self) -> None:
        """Finding 2: a timeout spanning the yield cancelled arbitrary caller code."""
        env = FakeEnv()
        client = await connected_client(env)
        try:
            got: list[bytes] = []
            slow_work_completed = asyncio.Event()

            async def consume() -> None:
                async for msg in client.request_many("svc", b"x", timeout=0.1):
                    got.append(msg.payload)
                    await asyncio.sleep(0.4)  # outlives the overall deadline
                    slow_work_completed.set()

            task = asyncio.create_task(consume())
            await asyncio.sleep(0)
            await client.flush()
            written = frames_written(env.current)
            marker = client.inbox_prefix.encode()
            index = written.rindex(marker)
            reply_subject = written[index : written.index(b" ", index)].decode()
            assert client._mux_sid is not None
            deliver_msg(env, client._mux_sid, reply_subject, b"r1")

            # The generator must complete normally: no CancelledError into the
            # loop body, the slow work finishes, and the sink is cleaned up.
            await asyncio.wait_for(task, timeout=2)
            assert got == [b"r1"]
            assert slow_work_completed.is_set()
            assert client._sinks == {}
        finally:
            await client.close()

    async def test_publish_failure_is_not_relabeled_as_no_reply(self) -> None:
        """Finding 12: a write-buffer TimeoutError must surface as itself."""
        env = FakeEnv()
        client = await connected_client(env, max_pending_size=64, flush_timeout=0.05)
        try:
            env.current.block_writes()
            await client._conn.publish_frame(b"PUB fill 40\r\n" + b"x" * 40 + b"\r\n")
            with pytest.raises(TimeoutError, match="write buffer"):
                await client.request("svc", b"y" * 40, timeout=5)
            # request_many: the same failure must RAISE, not complete empty.
            with pytest.raises(TimeoutError, match="write buffer"):
                async for _ in client.request_many("svc", b"y" * 40, timeout=5):
                    pass
        finally:
            env.current.unblock_writes()
            await client.close()


class TestAutoUnsubTermination:
    async def test_iterator_finishes_after_auto_unsub_completes(self) -> None:
        """Finding 3: 'give me exactly N messages' must not hang forever."""
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("n.*")
            await sub.unsubscribe_after(2)
            for i in range(2):
                deliver_msg(env, sub.sid, f"n.{i}", b"%d" % i)

            async with asyncio.timeout(2):  # hung forever before the fix
                got: list[bytes] = [msg.payload async for msg in sub]
            assert got == [b"0", b"1"]
            assert sub.is_closed
            assert sub.sid not in client._subscriptions
        finally:
            await client.close()

    async def test_next_msg_raises_closed_after_auto_unsub(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("m")
            await sub.unsubscribe_after(1)
            deliver_msg(env, sub.sid, "m", b"only")
            assert (await sub.next_msg(timeout=1)).payload == b"only"
            with pytest.raises(SubscriptionClosedError):
                await sub.next_msg(timeout=1)
        finally:
            await client.close()


class TestTerminationLatch:
    async def test_error_policy_wakes_stuck_callback_reader(self) -> None:
        """Findings 4+5: the sentinel was lost exactly when the queue was full."""
        env = FakeEnv()
        client = await connected_client(env)
        try:
            release = asyncio.Event()

            async def slow_cb(msg: natsio.Msg) -> None:
                await release.wait()

            sub = client.subscribe("f", cb=slow_cb, pending_msgs_limit=2, policy=PendingLimitPolicy.ERROR)
            for i in range(5):
                deliver_msg(env, sub.sid, "f", b"%d" % i)
            await asyncio.sleep(0)
            release.set()
            reader = sub._reader
            assert reader is not None
            async with asyncio.timeout(2):  # reader hung forever before the fix
                await asyncio.wait((reader,))
            # drain() afterwards must not hang either (old Finding 4 shape).
            async with asyncio.timeout(2):
                await sub.drain()
        finally:
            await client.close()

    async def test_two_concurrent_consumers_both_finish_on_unsubscribe(self) -> None:
        """Finding 10: a single sentinel woke only one of two waiters."""
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("c")

            async def iterate() -> None:
                async for _ in sub:
                    pass

            async def take_one() -> None:
                with pytest.raises(SubscriptionClosedError):
                    await sub.next_msg(timeout=None)

            a = asyncio.create_task(iterate())
            b = asyncio.create_task(take_one())
            await asyncio.sleep(0.01)
            await sub.unsubscribe()
            async with asyncio.timeout(2):
                await asyncio.gather(a, b)
        finally:
            await client.close()


class TestBlockPolicy:
    async def test_block_never_drops(self) -> None:
        """Finding 7: BLOCK's contract is zero loss; the burst is admitted."""
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("b", pending_msgs_limit=2, policy=PendingLimitPolicy.BLOCK)
            for i in range(5):
                deliver_msg(env, sub.sid, "b", b"%d" % i)
            assert sub.dropped == 0
            assert sub.pending_msgs == 5
            assert env.current.reading_paused is True
            got = [await sub.next_msg(timeout=1) for _ in range(5)]
            assert [m.payload for m in got] == [b"0", b"1", b"2", b"3", b"4"]
            assert env.current.reading_paused is False
        finally:
            await client.close()

    async def test_pause_is_refcounted_across_subscriptions(self) -> None:
        """Finding 6: one drained subscription must not resume the socket while
        another is still saturated."""
        env = FakeEnv()
        client = await connected_client(env)
        try:
            a = client.subscribe("a", pending_msgs_limit=2, policy=PendingLimitPolicy.BLOCK)
            b = client.subscribe("bb", pending_msgs_limit=2, policy=PendingLimitPolicy.BLOCK)
            for i in range(2):
                deliver_msg(env, a.sid, "a", b"%d" % i)
                deliver_msg(env, b.sid, "bb", b"%d" % i)
            assert env.current.reading_paused is True

            await b.next_msg(timeout=1)  # b drains below its limit
            assert env.current.reading_paused is True  # a still holds the pause

            await a.next_msg(timeout=1)
            assert env.current.reading_paused is False
        finally:
            await client.close()

    async def test_block_byte_limit_pauses(self) -> None:
        """Finding 14: the byte high-water test was unreachable (> vs >=)."""
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe(
                "bytes", pending_bytes_limit=20, pending_msgs_limit=1000, policy=PendingLimitPolicy.BLOCK
            )
            deliver_msg(env, sub.sid, "bytes", b"x" * 20)
            assert env.current.reading_paused is True
            assert sub.dropped == 0
            await sub.next_msg(timeout=1)
            assert env.current.reading_paused is False
        finally:
            await client.close()

    async def test_pause_reasserted_after_reconnect(self) -> None:
        """Finding 8: the fresh transport starts un-paused; backpressure owed by
        a still-saturated subscription must be re-applied."""
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("r", pending_msgs_limit=2, policy=PendingLimitPolicy.BLOCK)
            for i in range(2):
                deliver_msg(env, sub.sid, "r", b"%d" % i)
            assert env.current.reading_paused is True

            first = env.current
            reconnected = asyncio.Event()
            client._conn.bus.subscribe(lambda e: reconnected.set() if isinstance(e, natsio.Reconnected) else None)
            first.drop()
            async with asyncio.timeout(2):
                await reconnected.wait()
            assert env.current is not first
            assert env.current.reading_paused is True  # re-asserted

            await sub.next_msg(timeout=1)
            await sub.next_msg(timeout=1)
            assert env.current.reading_paused is False
        finally:
            await client.close()

    async def test_close_releases_the_pause(self) -> None:
        """Finding 15: _close_local left the transport paused."""
        env = FakeEnv()
        client = await connected_client(env)
        sub = client.subscribe("p", pending_msgs_limit=1, policy=PendingLimitPolicy.BLOCK)
        deliver_msg(env, sub.sid, "p", b"x")
        assert env.current.reading_paused is True
        await client.close()
        assert client._pausing_sids == set()


class TestDropAccounting:
    async def test_drop_old_rejects_oversized_message(self) -> None:
        """Finding 13: an over-budget message must not blow past the byte limit."""
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("big", pending_bytes_limit=10, policy=PendingLimitPolicy.DROP_OLD)
            deliver_msg(env, sub.sid, "big", b"x" * 8)
            deliver_msg(env, sub.sid, "big", b"y" * 50)  # alone exceeds the budget
            assert sub.pending_bytes == 8  # the queued message survives
            assert (await sub.next_msg(timeout=1)).payload == b"x" * 8
            assert sub.dropped == 1
        finally:
            await client.close()

    async def test_slow_consumer_reports_are_coalesced(self) -> None:
        """Finding 11: one error-callback task per dropped message was a flood."""
        env = FakeEnv()
        errors: list[NATSError] = []
        client = Client(make_options(), error_cb=errors.append, _transport_factory=env.factory)
        await client.connect()
        try:
            sub = client.subscribe("flood", pending_msgs_limit=2)
            for i in range(500):
                deliver_msg(env, sub.sid, "flood", b"%d" % i)
            assert sub.dropped == 498
            await asyncio.sleep(0.05)  # let the (few) callback tasks run
            slow = [e for e in errors if isinstance(e, SlowConsumerError)]
            assert 1 <= len(slow) <= 3  # coalesced, not 498
            assert slow[-1].dropped >= 1
        finally:
            await client.close()


class TestSubscribeLifecycleEdges:
    async def test_subscribe_before_connect_is_replayed(self) -> None:
        """Finding 17: pre-connect subscribe must work, not leak a dead entry."""
        env = FakeEnv()
        client = Client(make_options(), _transport_factory=env.factory)
        sub = client.subscribe("early.>")
        await client.connect()
        try:
            await client.flush()
            assert f"SUB early.> {sub.sid}\r\n".encode() in frames_written(env.current)
            deliver_msg(env, sub.sid, "early.msg", b"hi")
            assert (await sub.next_msg(timeout=1)).payload == b"hi"
        finally:
            await client.close()

    async def test_subscribe_after_close_raises_without_leaking(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        await client.close()
        with pytest.raises(NATSError):
            client.subscribe("late")
        assert client._conn.dispatcher.entries() == []
