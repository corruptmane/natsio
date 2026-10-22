"""Regression coverage for hard write-buffer admission and waiter lifetime."""

import asyncio
from collections import deque

import pytest

from fake import FakeEnv
from natsio._internal.connection import Connection
from natsio._internal.protocol import encode_header_block, encode_hpub, encode_pub
from natsio.errors import ConfigError, ConnectionClosedError, TimeoutError
from natsio.options import ConnectOptions


class ObservedWaiters(deque[asyncio.Future[None]]):
    """Expose admission registration as an event rather than polling tasks."""

    def __init__(self) -> None:
        super().__init__()
        self.registered: asyncio.Queue[asyncio.Future[None]] = asyncio.Queue()

    def append(self, waiter: asyncio.Future[None]) -> None:
        super().append(waiter)
        self.registered.put_nowait(waiter)


async def connected(*, limit: int = 64, admission_timeout: float = 1.0):
    env = FakeEnv()
    conn = Connection(
        ConnectOptions(max_pending_size=limit, flush_timeout=admission_timeout, allow_reconnect=False),
        transport_factory=env.factory,
    )
    await conn.connect()
    await conn.flush()
    session = conn._session
    assert session is not None
    waiters = ObservedWaiters()
    session._drain_waiters = waiters
    return conn, env, session, waiters


@pytest.mark.parametrize("headers", [False, True])
@pytest.mark.parametrize("extra_capacity", [-1, 0, 1])
async def test_encoded_frame_must_fit_including_header_and_protocol_overhead(headers, extra_capacity) -> None:
    frame = (
        encode_hpub("target", "reply", encode_header_block({"Trace": "value"}), b"payload")
        if headers
        else encode_pub("target", "reply", b"payload")
    )
    conn, env, session, waiters = await connected(limit=len(frame) + extra_capacity)
    try:
        env.current.block_writes()
        if extra_capacity < 0:
            # An impossible frame is rejected without registering a waiter.
            async with asyncio.timeout(0.2):
                with pytest.raises(ConfigError, match="max_pending_size"):
                    await conn.publish_frame(frame)
            assert not waiters
            assert not session.pending
        else:
            await conn.publish_frame(frame)
            assert session.pending == [frame]
            assert session.pending_size == len(frame)
    finally:
        await conn.close(flush=False)


async def test_admission_timeout_is_overall_despite_repeated_wakeups() -> None:
    conn, env, session, waiters = await connected(admission_timeout=0.03)
    frame = encode_pub("target", None, b"x" * 40)
    env.current.block_writes()
    await conn.publish_frame(frame)
    publish = asyncio.create_task(conn.publish_frame(frame))

    async def wake_repeatedly() -> None:
        while True:
            waiter = await waiters.registered.get()
            # Simulate contention waking this publisher without making room.
            # Each wake arrives before the individual timeout would expire.
            ready = asyncio.Event()
            timer = asyncio.get_running_loop().call_later(0.005, ready.set)
            try:
                await ready.wait()
            finally:
                timer.cancel()
            if not waiter.done():
                waiter.set_result(None)

    waker = asyncio.create_task(wake_repeatedly())
    try:
        async with asyncio.timeout(0.2):
            with pytest.raises(TimeoutError, match="write buffer"):
                await publish
        assert not session._drain_waiters
        assert session.pending == [frame]
    finally:
        waker.cancel()
        await asyncio.gather(waker, return_exceptions=True)
        await conn.close(flush=False)


@pytest.mark.parametrize("termination", ["cancel", "timeout", "disconnect", "close"])
async def test_parked_publishers_terminate_and_unregister(termination) -> None:
    conn, env, session, waiters = await connected(admission_timeout=0.03 if termination == "timeout" else 1.0)
    frame = encode_pub("target", None, b"x" * 40)
    try:
        env.current.block_writes()
        await conn.publish_frame(frame)
        publish = asyncio.create_task(conn.publish_frame(frame))
        async with asyncio.timeout(0.5):
            await waiters.registered.get()
            if termination == "cancel":
                publish.cancel()
                expected = asyncio.CancelledError
            elif termination == "timeout":
                expected = TimeoutError
            elif termination == "disconnect":
                env.current.drop()
                expected = ConnectionClosedError
            else:
                await conn.close(flush=False)
                expected = ConnectionClosedError
            with pytest.raises(expected):
                await publish
        assert not session._drain_waiters
    finally:
        await conn.close(flush=False)


async def test_concurrent_publishers_preserve_frames_and_the_hard_buffer_limit(monkeypatch) -> None:
    conn, env, session, waiters = await connected()
    transport = env.current
    frames = [encode_pub("target", None, f"{i:02d}".encode() + b"x" * 38) for i in range(21)]
    publishers: list[asyncio.Task[None]] = []
    peak = 0
    original_enqueue = type(session).enqueue

    def observed_enqueue(target, frame: bytes) -> None:
        nonlocal peak
        original_enqueue(target, frame)
        if target is session and frame.startswith((b"PUB ", b"HPUB ")):
            peak = max(peak, session.pending_size)

    monkeypatch.setattr(type(session), "enqueue", observed_enqueue)
    try:
        transport.block_writes()
        await conn.publish_frame(frames[0])
        publishers = [asyncio.create_task(conn.publish_frame(frame)) for frame in frames[1:]]
        async with asyncio.timeout(2):
            for _ in publishers:
                await waiters.registered.get()
            transport.unblock_writes()
            await asyncio.gather(*publishers)
            await conn.flush()
        assert peak <= conn.options.max_pending_size
        assert not waiters
        written = bytes(transport.written)
        for frame in frames:
            assert written.count(frame) == 1
    finally:
        for publisher in publishers:
            publisher.cancel()
        await asyncio.gather(*publishers, return_exceptions=True)
        await conn.close(flush=False)
