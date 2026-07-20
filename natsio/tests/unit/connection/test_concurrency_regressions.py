"""Regressions for the concurrency defects found reviewing M2.

Each test reproduces the reported interleaving; every one of them failed before
the corresponding fix.
"""

import asyncio

import pytest
from test_connection import connected_conn, make_options

from fake import EventRecorder, FakeEnv, frames_written
from natsio._internal.connection import Connection
from natsio._internal.lifecycle import Connected, ConnectionState, Disconnected, Reconnected
from natsio.errors import ConfigError, NATSError, StaleConnectionError


class TestFrameGranularity:
    async def test_buffered_frames_survive_a_second_disconnect_without_duplicating(self) -> None:
        """Finding 1: the reconnect buffer must stay a list of frames.

        Joined into one blob it would be classified by its first opcode, either
        carrying a SUB across (duplicate subscription on a live sid) or
        discarding a publish outright.
        """
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder)
        try:
            env.refuse_next(1)
            env.current.drop()
            await recorder.wait_for(Disconnected)

            # Both a user publish and a control frame, buffered while down.
            await conn.publish_frame(b"PUB first 1\r\na\r\n")
            entry = conn.subscribe("blob.sub", None, lambda event: None)

            await recorder.wait_for(Reconnected)
            await conn.flush()
            written = frames_written(env.current)
            assert b"PUB first 1\r\na\r\n" in written
            assert written.count(f"SUB blob.sub {entry.sid}\r\n".encode()) == 1
        finally:
            await conn.close(flush=False)

    async def test_publish_survives_when_the_write_itself_fails(self) -> None:
        """Finding 2: frames swapped out of pending must be restored if write raises."""
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder)
        try:
            transport = env.current
            transport.block_writes()
            await conn.publish_frame(b"PUB kept 4\r\ndata\r\n")

            def explode(_data: bytes) -> None:
                raise ConnectionResetError("peer reset")

            object.__setattr__(transport, "write", explode)
            transport.unblock_writes()

            await recorder.wait_for(Reconnected)
            await conn.flush()
            assert b"PUB kept 4\r\ndata\r\n" in frames_written(env.current)
        finally:
            await conn.close(flush=False)


class TestPingAccounting:
    async def test_concurrent_flushes_do_not_trip_the_stale_detector(self) -> None:
        """Finding 3: user flush() pings must not consume the liveness budget.

        Four flushes are left un-PONGed across a pinger tick. Counting them
        against max_outstanding_pings=2 (as the old code did) tore the healthy
        connection down on the very first tick.
        """
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder, ping_interval=0.05, max_outstanding_pings=2, flush_timeout=5.0)
        try:
            env.auto_pong = False
            flushes = [asyncio.create_task(conn.flush(timeout=5)) for _ in range(4)]
            await asyncio.sleep(0.07)  # at least one pinger tick has passed
            assert conn.state is ConnectionState.CONNECTED

            env.auto_pong = True
            for _ in range(8):
                env.current.deliver(b"PONG\r\n")
            await asyncio.gather(*flushes)
            assert conn.state is ConnectionState.CONNECTED
        finally:
            await conn.close(flush=False)

    async def test_missing_pongs_still_declare_stale(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder, ping_interval=0.02, max_outstanding_pings=1, allow_reconnect=False)
        env.auto_pong = False
        await recorder.wait_for(Disconnected, timeout=5.0)
        disconnected = next(e for e in recorder.events if isinstance(e, Disconnected))
        assert isinstance(disconnected.error, StaleConnectionError)
        assert conn.state is not ConnectionState.CONNECTED


class TestBackpressureLimits:
    async def test_woken_publishers_recheck_the_watermark(self) -> None:
        """Finding 5: releasing every waiter at once must not overshoot the cap."""
        env = FakeEnv()
        conn = await connected_conn(env, max_pending_size=1000, flush_timeout=5.0)
        peak = 0
        try:
            transport = env.current
            transport.block_writes()
            frame = b"PUB p 900\r\n" + b"x" * 900 + b"\r\n"
            await conn.publish_frame(frame)

            publishers = [asyncio.create_task(conn.publish_frame(frame)) for _ in range(20)]
            await asyncio.sleep(0)
            transport.unblock_writes()

            session = conn._session
            assert session is not None
            for _ in range(200):
                await asyncio.sleep(0)
                peak = max(peak, session.pending_size)
                if all(p.done() for p in publishers):
                    break
            await asyncio.gather(*publishers)
            # Before the fix this peaked at ~18x the configured maximum.
            assert peak <= 1000 + len(frame)
        finally:
            await conn.close(flush=False)

    async def test_publish_after_loss_fails_fast_instead_of_hanging(self) -> None:
        """Finding 6: a waiter queued after mark_lost would never be resolved."""
        env = FakeEnv()
        conn = await connected_conn(env, max_pending_size=200, flush_timeout=10.0)
        try:
            transport = env.current
            transport.block_writes()
            await conn.publish_frame(b"PUB a 163\r\n" + b"x" * 163 + b"\r\n")
            session = conn._session
            assert session is not None
            session.mark_lost(ConnectionResetError("gone"))

            async with asyncio.timeout(1):  # must not wait out flush_timeout
                with pytest.raises(NATSError):
                    await conn.publish_frame(b"PUB b 163\r\n" + b"y" * 163 + b"\r\n")
        finally:
            await conn.close(flush=False)


class TestStateMachine:
    async def test_disconnected_is_not_emitted_while_reporting_connected(self) -> None:
        """Finding 7: hooks must never see state=CONNECTED with no session."""
        env = FakeEnv()
        observed: list[tuple[ConnectionState, bool]] = []
        conn = Connection(make_options(), transport_factory=env.factory)

        def hook(event: object) -> None:
            if isinstance(event, Disconnected):
                observed.append((conn.state, conn._session is not None))

        conn.bus.subscribe(hook)
        await conn.connect()
        try:
            env.current.drop()
            for _ in range(200):
                await asyncio.sleep(0.005)
                if observed:
                    break
            assert observed
            state, has_session = observed[0]
            assert state is not ConnectionState.CONNECTED
            assert has_session is False
        finally:
            await conn.close(flush=False)

    async def test_close_interrupts_a_stalled_handshake_promptly(self) -> None:
        """Finding 4: close() must not wait out connect_timeout."""
        env = FakeEnv()
        env.auto_info = False  # accept the socket, never send INFO
        conn = Connection(make_options(connect_timeout=30.0), transport_factory=env.factory)
        task = asyncio.create_task(conn.connect())
        await asyncio.sleep(0.05)
        async with asyncio.timeout(2):  # would take 30s without the fix
            await conn.close(flush=False)
        with pytest.raises(NATSError):
            await asyncio.wait_for(task, timeout=2)
        assert conn.state is ConnectionState.CLOSED

    async def test_err_alongside_handshake_pong_is_not_reported_connected(self) -> None:
        """Finding 10: a corpse session must not be published as CONNECTED."""
        env = FakeEnv()
        env.auto_pong = False
        real_write = env.on_client_write

        def on_write(transport, data: bytes) -> None:
            if b"CONNECT" in data:
                asyncio.get_running_loop().call_soon(transport.deliver, b"PONG\r\n-ERR 'Stale Connection'\r\n")
            real_write(transport, data)

        env.on_client_write = on_write
        recorder = EventRecorder()
        conn = Connection(make_options(allow_reconnect=False), transport_factory=env.factory)
        conn.bus.subscribe(recorder.hook)
        with pytest.raises(NATSError):
            await conn.connect()
        assert recorder.count(Connected) == 0
        assert conn.state is ConnectionState.CLOSED

    async def test_connect_after_close_is_rejected_clearly(self) -> None:
        """Finding 11: the double-connect guard must survive a close()."""
        env = FakeEnv()
        conn = await connected_conn(env)
        await conn.close(flush=False)
        with pytest.raises(ConfigError, match="CLOSED"):
            await conn.connect()

    async def test_double_connect_is_rejected(self) -> None:
        env = FakeEnv()
        conn = await connected_conn(env)
        try:
            with pytest.raises(ConfigError):
                await conn.connect()
        finally:
            await conn.close(flush=False)


class TestReadPathIsolation:
    async def test_broken_instrumentation_cannot_kill_the_connection(self) -> None:
        """Finding 8: a failing metrics hook must not abort the socket."""

        class Exploding:
            def __getattr__(self, name: str):
                def hook(*args: object, **kwargs: object) -> None:
                    raise RuntimeError(f"metrics backend is down ({name})")

                return hook

        env = FakeEnv()
        conn = await connected_conn(env, instrumentation=Exploding())
        received: list[object] = []
        try:
            entry = conn.subscribe("m.>", None, received.append)
            await conn.flush()
            env.current.deliver(f"MSG m.x {entry.sid} 2\r\nhi\r\n".encode())
            assert len(received) == 1
            assert conn.state is ConnectionState.CONNECTED
        finally:
            await conn.close(flush=False)

    async def test_lame_duck_does_not_truncate_the_segment(self) -> None:
        """Finding 9: LDM is a graceful drain, not a mid-segment cut."""
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder)
        received: list[object] = []
        try:
            entry = conn.subscribe("d.>", None, received.append)
            await conn.flush()
            segment = b'INFO {"ldm":true}\r\n' + b"".join(
                f"MSG d.{i} {entry.sid} 1\r\nx\r\n".encode() for i in range(3)
            )
            env.current.deliver(segment)
            assert len(received) == 3
            await recorder.wait_for(Reconnected)
        finally:
            await conn.close(flush=False)
