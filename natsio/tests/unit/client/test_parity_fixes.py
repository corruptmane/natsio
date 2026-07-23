"""Regressions for the nats.go parity audit findings in the client/subscription layer.

Every scenario here reproduced a hang, a wrong exception, or silent loss before
its fix. Named after the corresponding nats.go tests where one exists.
"""

import asyncio

import pytest
from test_client import _extract_reply, connected_client, deliver_msg

import natsio
from fake import FakeEnv, frames_written
from natsio._internal.lifecycle import ConnectionState
from natsio.errors import (
    ConnectionClosedError,
    DrainTimeoutError,
    NoRespondersError,
    SubscriptionClosedError,
)


class TestRequestClose:
    async def test_close_wakes_pending_request(self) -> None:
        """A request in flight when close() runs must fail at once, not hang the timeout."""
        env = FakeEnv()
        client = await connected_client(env, request_timeout=30.0)
        task = asyncio.create_task(client.request("svc", b"ping", timeout=30.0))
        await asyncio.sleep(0)  # let the sink register and the PUB buffer
        assert client._sinks  # request is armed
        await client.close()
        with pytest.raises(ConnectionClosedError):
            await asyncio.wait_for(task, timeout=1)


class TestSelfReferentialTeardown:
    async def test_unsubscribe_from_inside_callback(self) -> None:
        """Finding: unsubscribe() from the callback cancelled the running callback."""
        env = FakeEnv()
        client = await connected_client(env)
        received: list[bytes] = []
        after_await = asyncio.Event()
        holder: dict[str, natsio.Subscription] = {}

        async def handler(msg: natsio.Msg) -> None:
            received.append(msg.payload)
            if msg.payload == b"stop":
                await holder["sub"].unsubscribe()
                after_await.set()  # continuation after the await must run

        try:
            sub = client.subscribe("cb", cb=handler)
            holder["sub"] = sub
            deliver_msg(env, sub.sid, "cb", b"1")
            deliver_msg(env, sub.sid, "cb", b"stop")
            deliver_msg(env, sub.sid, "cb", b"late")  # queued behind stop; must be dropped
            async with asyncio.timeout(2):
                await after_await.wait()
            await asyncio.sleep(0.01)  # give any stray delivery a chance to (not) happen
            assert received == [b"1", b"stop"]
            assert sub.is_closed
        finally:
            await client.close()

    async def test_drain_from_inside_callback(self) -> None:
        """Finding: drain() from the callback deadlocked on _idle.wait()."""
        env = FakeEnv()
        client = await connected_client(env)
        received: list[bytes] = []
        done = asyncio.Event()
        holder: dict[str, natsio.Subscription] = {}

        async def handler(msg: natsio.Msg) -> None:
            received.append(msg.payload)
            await holder["sub"].drain()
            done.set()

        try:
            sub = client.subscribe("cb", cb=handler)
            holder["sub"] = sub
            deliver_msg(env, sub.sid, "cb", b"only")
            async with asyncio.timeout(2):  # deadlocked before the fix
                await done.wait()
            assert received == [b"only"]
            assert sub.is_closed
        finally:
            await client.close()


class TestNextMsgGuards:
    async def test_next_msg_rejected_in_callback_mode(self) -> None:
        """Finding: next_msg() on a callback-mode sub raced the callback loop."""
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("cb", cb=lambda msg: None)
            with pytest.raises(SubscriptionClosedError, match="callback mode"):
                await sub.next_msg(timeout=1)
        finally:
            await client.close()

    async def test_next_msg_converts_503_to_no_responders(self) -> None:
        """Finding: sync next_msg() must surface a payload-less 503 as NoRespondersError."""
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("_INBOX.manual")
            block = b"NATS/1.0 503\r\n\r\n"
            env.current.deliver(
                f"HMSG _INBOX.manual {sub.sid} {len(block)} {len(block)}\r\n".encode() + block + b"\r\n"
            )
            with pytest.raises(NoRespondersError):
                await sub.next_msg(timeout=1)
        finally:
            await client.close()

    async def test_iterator_yields_raw_503_status(self) -> None:
        """The async/iterator path must NOT convert the 503; it yields the raw status."""
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("_INBOX.iter")
            block = b"NATS/1.0 503\r\n\r\n"
            env.current.deliver(f"HMSG _INBOX.iter {sub.sid} {len(block)} {len(block)}\r\n".encode() + block + b"\r\n")
            got: list[natsio.Msg] = []
            async with asyncio.timeout(2):
                async for msg in sub:
                    got.append(msg)
                    break
            assert got[0].status is not None
            assert got[0].status.code == 503
        finally:
            await client.close()


class TestDrainTimeoutErrorReported:
    async def test_drain_timeout_fires_error_handler(self) -> None:
        """Finding: drain timeout only log.warning'd; it must also emit DrainTimeoutError."""
        env = FakeEnv()
        client = await connected_client(env, drain_timeout=0.1)
        errors: list[Exception] = []
        client._conn.bus.subscribe(
            lambda e: errors.append(e.error) if hasattr(e, "error") and e.error is not None else None
        )
        stuck = asyncio.Event()

        async def never_finishes(msg: natsio.Msg) -> None:
            await stuck.wait()

        sub = client.subscribe("stuck", cb=never_finishes)
        deliver_msg(env, sub.sid, "stuck", b"x")
        await asyncio.sleep(0)  # let the callback start and block

        async with asyncio.timeout(2):
            await client.drain()
        assert client.status is ConnectionState.CLOSED
        assert any(isinstance(e, DrainTimeoutError) for e in errors)
        stuck.set()


class TestOptionalAwait:
    """`await` on subscribe is optional and a no-op — nats-py muscle-memory
    (and LLM-generated code) support."""

    async def test_await_subscribe_returns_same_subscription(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = client.subscribe("opt.await")
            assert (await sub) is sub
            assert (await sub) is sub  # idempotent
        finally:
            await client.close()

    async def test_awaited_subscription_still_iterates(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            sub = await client.subscribe("opt.iter")
            await client.flush()
            deliver_msg(env, sub.sid, "opt.iter", b"hi")
            msg = await sub.next_msg(timeout=1.0)
            assert msg.payload == b"hi"
        finally:
            await client.close()

    async def test_await_composes_with_context_manager(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            async with await client.subscribe("opt.cm") as sub:
                assert not sub.is_closed
            assert sub.is_closed
        finally:
            await client.close()


class TestRequestManyFirstReplyStall:
    """`stall` bounds the gap BETWEEN replies — the first reply gets the full
    deadline (nats.go natsext `if !first && stall != 0`). Applying stall before
    the first reply turned a responder slower than `stall` into zero results."""

    async def test_slow_first_reply_is_not_cut_off_by_stall(self) -> None:
        env = FakeEnv()
        client = await connected_client(env)
        try:
            replies: list[bytes] = []

            async def collect() -> None:
                replies.extend([msg.payload async for msg in client.request_many("svc", b"x", timeout=2.0, stall=0.05)])

            task = asyncio.create_task(collect())
            await asyncio.sleep(0)
            await client.flush()
            reply_subject = _extract_reply(frames_written(env.current), client.inbox_prefix)
            mux_sid = client._mux_sid
            assert mux_sid is not None
            # Answer well after the 50ms stall but inside the 2s deadline.
            await asyncio.sleep(0.2)
            deliver_msg(env, mux_sid, reply_subject, b"slow-but-valid")
            await asyncio.wait_for(task, timeout=2)
            assert replies == [b"slow-but-valid"]
        finally:
            await client.close()


class TestRequestManyCloseIsNotCompletion:
    """Closing the connection mid-stream truncates the result; it is not the
    responders finishing. `request()` already raised here — `request_many()`
    returned quietly, so every caller (batch Direct Get, `$SYS` gathers) read a
    truncated prefix as a complete answer."""

    async def test_close_mid_stream_raises_instead_of_ending_the_stream(self) -> None:
        env = FakeEnv()
        client = await connected_client(env, request_timeout=30.0)
        seen: list[bytes] = []

        async def collect() -> None:
            # Append per-message on purpose: the point of this test is that the
            # partial result survives the mid-stream raise, and a comprehension
            # would discard everything collected when the exception propagates.
            async for msg in client.request_many("svc", b"x", timeout=30.0):
                seen.append(msg.payload)  # noqa: PERF401

        task = asyncio.create_task(collect())
        await asyncio.sleep(0)
        await client.flush()
        reply_subject = _extract_reply(frames_written(env.current), client.inbox_prefix)
        mux_sid = client._mux_sid
        assert mux_sid is not None
        deliver_msg(env, mux_sid, reply_subject, b"first")
        await asyncio.sleep(0)  # let the consumer take it before we close

        await client.close()

        with pytest.raises(ConnectionClosedError, match="truncated"):
            await asyncio.wait_for(task, timeout=1)
        # The partial data was really delivered — this is truncation, not a lost stream.
        assert seen == [b"first"]
