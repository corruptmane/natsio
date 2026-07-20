"""Regressions for the nats.go parity audit findings in the client/subscription layer.

Every scenario here reproduced a hang, a wrong exception, or silent loss before
its fix. Named after the corresponding nats.go tests where one exists.
"""

import asyncio

import pytest
from test_client import connected_client, deliver_msg

import natsio
from fake import FakeEnv
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
