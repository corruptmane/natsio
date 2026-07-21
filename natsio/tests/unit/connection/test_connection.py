import asyncio

import pytest

from fake import EventRecorder, FakeEnv, FakeTransport, connect_payload, frames_written
from natsio._internal.connection import Connection, _SafeInstrumentation
from natsio._internal.lifecycle import (
    Closed,
    Connected,
    ConnectionState,
    Disconnected,
    ErrorOccurred,
    LameDuck,
    Reconnected,
    ServersDiscovered,
)
from natsio._internal.protocol import HMsgEvent, MsgEvent
from natsio.errors import (
    AuthenticationExpiredError,
    AuthorizationViolationError,
    ConfigError,
    ConnectionClosedError,
    NoServersAvailableError,
    PermissionsViolationError,
    ReconnectBufExceededError,
    StaleConnectionError,
    TimeoutError,
)
from natsio.instrumentation import NoopInstrumentation
from natsio.options import ConnectOptions


def make_options(**overrides) -> ConnectOptions:
    defaults: dict = {
        "servers": ("nats://s1.example:4222",),
        "connect_timeout": 1.0,
        "reconnect_time_wait": 0.01,
        "reconnect_time_wait_max": 0.02,
        "reconnect_jitter": 0.001,
        "reconnect_jitter_tls": 0.001,
        "no_randomize": True,
        "ping_interval": 60.0,
        "flush_timeout": 1.0,
        "drain_timeout": 1.0,
    }
    defaults.update(overrides)
    return ConnectOptions(**defaults)


async def connected_conn(env: FakeEnv, recorder: EventRecorder | None = None, **overrides):
    conn = Connection(make_options(**overrides), transport_factory=env.factory)
    if recorder is not None:
        conn.bus.subscribe(recorder.hook)
    await conn.connect()
    return conn


class TestHandshake:
    async def test_connect_success(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder)
        try:
            assert conn.state is ConnectionState.CONNECTED
            assert recorder.count(Connected) == 1
            payload = connect_payload(env.current)
            assert payload["protocol"] == 1
            assert payload["headers"] is True
            assert payload["no_responders"] is True
            assert payload["lang"] == "natsio"
        finally:
            await conn.close(flush=False)

    async def test_connect_applies_server_max_payload(self) -> None:
        env = FakeEnv()
        env.info["max_payload"] = 512
        conn = await connected_conn(env)
        try:
            assert conn.server_info["max_payload"] == 512
        finally:
            await conn.close(flush=False)

    async def test_auth_error_during_handshake_surfaces_as_cause(self) -> None:
        env = FakeEnv()
        env.auto_pong = False
        env.auto_info = True

        real_on_write = env.on_client_write

        def on_write(transport, data):
            if b"CONNECT" in data:
                asyncio.get_running_loop().call_soon(transport.deliver, b"-ERR 'Authorization Violation'\r\n")
            real_on_write(transport, data)

        env.on_client_write = on_write
        conn = Connection(make_options(), transport_factory=env.factory)
        with pytest.raises(NoServersAvailableError) as excinfo:
            await conn.connect()
        assert isinstance(excinfo.value.__cause__, AuthorizationViolationError)
        assert conn.state is ConnectionState.CLOSED

    async def test_initial_connect_all_servers_down(self) -> None:
        env = FakeEnv()
        env.refuse_next(2)
        conn = Connection(
            make_options(servers=("nats://a:4222", "nats://b:4222")),
            transport_factory=env.factory,
        )
        with pytest.raises(NoServersAvailableError):
            await conn.connect()
        assert env.attempts == 2
        assert conn.state is ConnectionState.CLOSED

    async def test_url_userinfo_password_auth(self) -> None:
        env = FakeEnv()
        conn = await connected_conn(env, servers=("nats://alice:secret@s1:4222",))
        try:
            payload = connect_payload(env.current)
            assert payload["user"] == "alice"
            assert payload["pass"] == "secret"
        finally:
            await conn.close(flush=False)

    async def test_url_userinfo_token_auth(self) -> None:
        env = FakeEnv()
        conn = await connected_conn(env, servers=("nats://s3cr3t@s1:4222",))
        try:
            assert connect_payload(env.current)["auth_token"] == "s3cr3t"
        finally:
            await conn.close(flush=False)

    async def test_url_userpass_wins_over_option_credentials(self) -> None:
        env = FakeEnv()
        conn = await connected_conn(env, servers=("nats://alice:secret@s1:4222",), user="bob", password="other")
        try:
            payload = connect_payload(env.current)
            assert payload["user"] == "alice"
            assert payload["pass"] == "secret"
        finally:
            await conn.close(flush=False)

    async def test_url_token_wins_over_option_token(self) -> None:
        env = FakeEnv()
        conn = await connected_conn(env, servers=("nats://url-tok@s1:4222",), token="opt-tok")
        try:
            assert connect_payload(env.current)["auth_token"] == "url-tok"
        finally:
            await conn.close(flush=False)

    async def test_config_error_fails_fast_without_burning_other_servers(self) -> None:
        class _MissingFileAuth:
            async def authenticate(self, nonce: bytes | None) -> object:
                raise FileNotFoundError(2, "No such file or directory", "/nope.creds")

        env = FakeEnv()
        conn = Connection(
            make_options(servers=("nats://s1:4222", "nats://s2:4222"), authenticator=_MissingFileAuth()),
            transport_factory=env.factory,
        )
        with pytest.raises(FileNotFoundError):
            await conn.connect()
        # The permanent config error aborts on the first server instead of
        # trying (and masking the cause with) the second.
        assert env.attempts == 1
        assert conn.state is ConnectionState.CLOSED


class TestMessaging:
    async def test_subscribe_and_dispatch(self) -> None:
        env = FakeEnv()
        conn = await connected_conn(env)
        received: list[MsgEvent | HMsgEvent] = []
        try:
            entry = conn.subscribe("foo.*", None, received.append)
            await conn.flush()
            assert f"SUB foo.* {entry.sid}\r\n".encode() in frames_written(env.current)
            env.current.deliver(f"MSG foo.bar {entry.sid} 5\r\nhello\r\n".encode())
            assert len(received) == 1
            assert received[0].subject == "foo.bar"
            assert received[0].payload == b"hello"
        finally:
            await conn.close(flush=False)

    async def test_publish_frames_are_coalesced(self) -> None:
        env = FakeEnv()
        conn = await connected_conn(env)
        try:
            baseline = len(env.current.writes)
            for i in range(10):
                await conn.publish_frame(f"PUB t.{i} 2\r\nhi\r\n".encode())
            await conn.flush()
            # All 10 PUBs (plus the flush PING) must have left in far fewer writes.
            writes_used = len(env.current.writes) - baseline
            assert writes_used <= 2
            assert frames_written(env.current).count(b"PUB t.") == 10
        finally:
            await conn.close(flush=False)

    async def test_auto_unsubscribe_bookkeeping(self) -> None:
        env = FakeEnv()
        conn = await connected_conn(env)
        received: list[MsgEvent | HMsgEvent] = []
        try:
            entry = conn.subscribe("n.*", None, received.append)
            conn.unsubscribe(entry.sid, max_msgs=2)
            for i in range(4):
                env.current.deliver(f"MSG n.{i} {entry.sid} 1\r\nx\r\n".encode())
            assert len(received) == 2
            assert conn.dispatcher.get(entry.sid) is None
        finally:
            await conn.close(flush=False)

    async def test_server_ping_gets_ponged(self) -> None:
        env = FakeEnv()
        conn = await connected_conn(env)
        try:
            baseline = frames_written(env.current).count(b"PONG\r\n")
            env.current.deliver(b"PING\r\n")
            await conn.flush()
            assert frames_written(env.current).count(b"PONG\r\n") == baseline + 1
        finally:
            await conn.close(flush=False)

    async def test_backpressure_timeout(self) -> None:
        env = FakeEnv()
        conn = await connected_conn(env, max_pending_size=64, flush_timeout=0.05)
        try:
            env.current.block_writes()
            # Park a frame in the buffer so the next send exceeds the watermark.
            await conn.publish_frame(b"PUB a 40\r\n" + b"x" * 40 + b"\r\n")
            with pytest.raises(TimeoutError):
                await conn.publish_frame(b"PUB b 40\r\n" + b"y" * 40 + b"\r\n")
        finally:
            env.current.unblock_writes()
            await conn.close(flush=False)


class TestServerErrors:
    async def test_benign_err_keeps_connection(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder)
        try:
            env.current.deliver(b"-ERR 'Permissions Violation for Publish to \"x\"'\r\n")
            event = await recorder.wait_for(ErrorOccurred)
            assert isinstance(event, ErrorOccurred)
            assert isinstance(event.error, PermissionsViolationError)
            assert conn.state is ConnectionState.CONNECTED
        finally:
            await conn.close(flush=False)

    async def test_fatal_err_triggers_reconnect(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder)
        try:
            env.current.deliver(b"-ERR 'Stale Connection'\r\n")
            disconnected = await recorder.wait_for(Disconnected)
            assert isinstance(disconnected, Disconnected)
            assert isinstance(disconnected.error, StaleConnectionError)
            await recorder.wait_for(Reconnected)
            assert conn.state is ConnectionState.CONNECTED
            assert len(env.transports) == 2
        finally:
            await conn.close(flush=False)


class TestReconnect:
    async def test_drop_reconnects_and_replays_subscriptions(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder)
        received: list[MsgEvent | HMsgEvent] = []
        try:
            entry = conn.subscribe("orders.>", "workers", received.append)
            await conn.flush()
            env.current.drop(ConnectionResetError("boom"))
            await recorder.wait_for(Reconnected)
            await conn.flush()  # barrier: replayed frames precede the flush PING
            replayed = frames_written(env.current)
            assert f"SUB orders.> workers {entry.sid}\r\n".encode() in replayed
            env.current.deliver(f"MSG orders.1 {entry.sid} 2\r\nok\r\n".encode())
            assert len(received) == 1
        finally:
            await conn.close(flush=False)

    async def test_auto_unsub_remaining_replayed(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder)
        received: list[MsgEvent | HMsgEvent] = []
        try:
            entry = conn.subscribe("m.*", None, received.append)
            conn.unsubscribe(entry.sid, max_msgs=5)
            for i in range(2):
                env.current.deliver(f"MSG m.{i} {entry.sid} 1\r\nx\r\n".encode())
            env.current.drop()
            await recorder.wait_for(Reconnected)
            await conn.flush()  # barrier: replayed frames precede the flush PING
            assert f"UNSUB {entry.sid} 3\r\n".encode() in frames_written(env.current)
        finally:
            await conn.close(flush=False)

    async def test_publish_while_reconnecting_is_buffered(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder)
        try:
            env.refuse_next(1)  # force one failed attempt to widen the reconnect window
            env.current.drop()
            await recorder.wait_for(Disconnected)
            frame = b"PUB buffered 3\r\nyes\r\n"
            await conn.publish_frame(frame)
            await recorder.wait_for(Reconnected)
            await conn.flush()
            assert frame in frames_written(env.current)
        finally:
            await conn.close(flush=False)

    async def test_reconnect_disabled_closes(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder, allow_reconnect=False)
        env.current.drop()
        await recorder.wait_for(Closed)
        assert conn.state is ConnectionState.CLOSED
        with pytest.raises(ConnectionClosedError):
            await conn.publish_frame(b"PUB x 0\r\n\r\n")

    async def test_pool_exhaustion_closes(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder, max_reconnect_attempts=2)
        env.refuse_next(10)
        env.current.drop()
        await recorder.wait_for(Closed, timeout=5.0)
        assert conn.state is ConnectionState.CLOSED
        # 1 initial + 2 failed reconnect attempts against the single server.
        assert env.attempts == 3


class TestAsyncInfo:
    async def test_connect_urls_discovered(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder)
        try:
            env.current.deliver(b'INFO {"connect_urls":["10.0.0.7:4222","10.0.0.8:4223"]}\r\n')
            event = await recorder.wait_for(ServersDiscovered)
            assert isinstance(event, ServersDiscovered)
            assert len(event.urls) == 2
        finally:
            await conn.close(flush=False)

    async def test_lame_duck_triggers_migration(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder)
        try:
            env.current.deliver(b'INFO {"ldm":true}\r\n')
            await recorder.wait_for(LameDuck)
            await recorder.wait_for(Reconnected)
            assert len(env.transports) == 2
        finally:
            await conn.close(flush=False)


class TestLiveness:
    async def test_stale_connection_on_missed_pongs(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder, ping_interval=0.02, max_outstanding_pings=1, allow_reconnect=False)
        env.auto_pong = False
        await recorder.wait_for(Closed, timeout=5.0)
        assert conn.state is ConnectionState.CLOSED
        disconnected = next(e for e in recorder.events if isinstance(e, Disconnected))
        assert isinstance(disconnected.error, StaleConnectionError)

    async def test_flush_timeout_without_pong(self) -> None:
        env = FakeEnv()
        conn = await connected_conn(env)
        try:
            env.auto_pong = False
            with pytest.raises(TimeoutError):
                await conn.flush(timeout=0.05)
        finally:
            await conn.close(flush=False)


class TestClose:
    async def test_close_is_idempotent_and_emits_closed_once(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder)
        await conn.close()
        await conn.close()
        assert recorder.count(Closed) == 1
        assert conn.state is ConnectionState.CLOSED

    async def test_close_flushes_pending_writes(self) -> None:
        env = FakeEnv()
        conn = await connected_conn(env)
        frame = b"PUB last.words 3\r\nbye\r\n"
        await conn.publish_frame(frame)
        await conn.close(flush=True)
        assert frame in frames_written(env.current)

    async def test_subscribe_after_close_raises(self) -> None:
        env = FakeEnv()
        conn = await connected_conn(env)
        await conn.close(flush=False)
        with pytest.raises(ConnectionClosedError):
            conn.subscribe("x", None, lambda event: None)


class TestPendingCarryover:
    async def test_unflushed_publishes_survive_reconnect(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder)
        try:
            first = env.current
            first.block_writes()  # flusher stalls; frames stay in session.pending
            frame = b"PUB carried 4\r\nover\r\n"
            await conn.publish_frame(frame)
            assert frame not in frames_written(first)
            first.drop()
            await recorder.wait_for(Reconnected)
            await conn.flush()
            assert frame in frames_written(env.current)
        finally:
            await conn.close(flush=False)

    async def test_control_frames_do_not_carry_over(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder)
        try:
            first = env.current
            first.block_writes()
            entry = conn.subscribe("dup.check", None, lambda e: None)
            first.drop()
            await recorder.wait_for(Reconnected)
            await conn.flush()
            # Replay sends the SUB exactly once; the unflushed original must not duplicate it.
            assert frames_written(env.current).count(f"SUB dup.check {entry.sid}\r\n".encode()) == 1
        finally:
            await conn.close(flush=False)


class TestCloseRaces:
    async def test_close_during_stalled_initial_connect(self) -> None:
        env = FakeEnv()
        env.auto_info = False  # handshake stalls awaiting INFO
        conn = Connection(make_options(connect_timeout=0.3), transport_factory=env.factory)
        connect_task = asyncio.create_task(conn.connect())
        await asyncio.sleep(0.05)  # let the handshake start and stall
        await asyncio.wait_for(conn.close(flush=False), timeout=2)
        with pytest.raises((NoServersAvailableError, ConnectionClosedError)):
            await asyncio.wait_for(connect_task, timeout=2)
        assert conn.state is ConnectionState.CLOSED


class TestReconnectPolicy:
    async def test_flapping_server_is_rate_limited(self) -> None:
        """A server that completes the handshake then immediately drops must not
        be retried in a tight loop: the failure counter resets on handshake, so
        the floor between attempts to the same server is what bounds it."""
        env = FakeEnv()
        conn = await connected_conn(env, reconnect_time_wait=0.25)
        try:
            for _ in range(3):
                env.current.drop()
                await asyncio.sleep(0.05)
            # Without the per-server floor this loops at ~200 attempts/second.
            assert env.attempts <= 2
        finally:
            await conn.close(flush=False)

    async def test_initial_info_seeds_cluster_topology(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        env.info["connect_urls"] = ["10.0.0.9:4222", "10.0.0.10:4222"]
        conn = await connected_conn(env, recorder)
        try:
            event = await recorder.wait_for(ServersDiscovered)
            assert isinstance(event, ServersDiscovered)
            assert len(event.urls) == 2
            # The seed plus both discovered peers are now failover candidates.
            assert len(conn._pool.candidates()) == 3
        finally:
            await conn.close(flush=False)


def _reject_auth_after_connect(env: FakeEnv, budget: dict[str, int], message: bytes) -> None:
    """Make the fake server reject CONNECT with ``message`` for the next
    ``budget['n']`` handshakes, auto-ponging normally otherwise."""

    def on_write(transport: FakeTransport, data: bytes) -> None:
        if budget["n"] > 0 and b"CONNECT" in data:
            budget["n"] -= 1
            asyncio.get_running_loop().call_soon(transport.deliver, message)
            return
        env._default_on_client_write(transport, data)

    env.on_client_write = on_write


class TestAuthReconnect:
    async def test_repeated_auth_error_during_reconnect_closes(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        budget = {"n": 0}
        _reject_auth_after_connect(env, budget, b"-ERR 'Authorization Violation'\r\n")
        conn = Connection(make_options(max_reconnect_attempts=-1), transport_factory=env.factory)
        conn.bus.subscribe(recorder.hook)
        await conn.connect()
        budget["n"] = 1000  # reject every reconnect handshake from now on
        env.current.drop()
        # Two identical auth rejections abort the whole loop despite unlimited retries.
        await recorder.wait_for(Closed, timeout=5.0)
        assert conn.state is ConnectionState.CLOSED

    async def test_ignore_auth_error_abort_keeps_retrying(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        budget = {"n": 1000}
        _reject_auth_after_connect(env, budget, b"-ERR 'Authorization Violation'\r\n")
        conn = Connection(
            make_options(max_reconnect_attempts=5, ignore_auth_error_abort=True),
            transport_factory=env.factory,
        )
        conn.bus.subscribe(recorder.hook)
        # Initial handshake must succeed, so do not reject until connected.
        budget["n"] = 0
        await conn.connect()
        budget["n"] = 1000
        env.current.drop()
        await recorder.wait_for(Closed, timeout=5.0)
        # Not aborted at strike two: exhausts the full retry budget instead.
        assert env.attempts == 1 + 5

    async def test_auth_error_then_success_clears_memory(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        budget = {"n": 0}
        _reject_auth_after_connect(env, budget, b"-ERR 'Authorization Violation'\r\n")
        conn = Connection(make_options(max_reconnect_attempts=-1), transport_factory=env.factory)
        conn.bus.subscribe(recorder.hook)
        await conn.connect()
        budget["n"] = 1  # exactly one reconnect handshake is rejected
        env.current.drop()
        await recorder.wait_for(Reconnected, timeout=5.0)
        assert conn.state is ConnectionState.CONNECTED
        assert all(s.last_auth_error is None for s in conn._pool.servers)
        await conn.close(flush=False)

    async def test_live_auth_error_reaches_error_handler_and_reconnects(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder)
        try:
            env.current.deliver(b"-ERR 'User Authentication Expired'\r\n")
            event = await recorder.wait_for(ErrorOccurred)
            assert isinstance(event, ErrorOccurred)
            assert isinstance(event.error, AuthenticationExpiredError)
            await recorder.wait_for(Reconnected)
            assert conn.state is ConnectionState.CONNECTED
        finally:
            await conn.close(flush=False)


class TestOptionsValidation:
    def test_max_reconnect_attempts_zero_rejected(self) -> None:
        with pytest.raises(ConfigError, match="allow_reconnect=False"):
            make_options(max_reconnect_attempts=0)

    def test_unlimited_and_positive_accepted(self) -> None:
        assert make_options(max_reconnect_attempts=-1).max_reconnect_attempts == -1
        assert make_options(max_reconnect_attempts=1).max_reconnect_attempts == 1


class TestForceReconnect:
    async def test_force_reconnect_on_healthy_connection(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        # A long backoff proves the first attempt bypasses it: without the bypass
        # the Reconnected wait (2s default) would expire before the 10s delay.
        conn = await connected_conn(env, recorder, reconnect_time_wait=10.0, reconnect_time_wait_max=10.0)
        received: list[MsgEvent | HMsgEvent] = []
        try:
            entry = conn.subscribe("foo", None, received.append)
            await conn.flush()
            await conn.force_reconnect()
            disconnected = await recorder.wait_for(Disconnected)
            assert isinstance(disconnected, Disconnected)
            assert disconnected.error is None  # a deliberate drop, not a failure
            await recorder.wait_for(Reconnected)
            assert conn.state is ConnectionState.CONNECTED
            assert len(env.transports) == 2
            # The drop was not counted as a server failure.
            assert all(s.consecutive_failures == 0 for s in conn._pool.servers)
            # Subscription replays on the fresh transport and still delivers.
            await conn.flush()
            assert f"SUB foo {entry.sid}\r\n".encode() in frames_written(env.current)
            env.current.deliver(f"MSG foo {entry.sid} 2\r\nhi\r\n".encode())
            assert len(received) == 1
        finally:
            await conn.close(flush=False)

    async def test_force_reconnect_raises_when_closed(self) -> None:
        env = FakeEnv()
        conn = await connected_conn(env)
        await conn.close(flush=False)
        with pytest.raises(ConnectionClosedError):
            await conn.force_reconnect()

    async def test_repeated_force_reconnect_collapses_to_one_cycle(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder, reconnect_time_wait=10.0, reconnect_time_wait_max=10.0)
        try:
            for _ in range(5):
                await conn.force_reconnect()
            await recorder.wait_for(Reconnected)
            assert conn.state is ConnectionState.CONNECTED
            # The five rapid calls tore down exactly one session, not five.
            assert len(env.transports) == 2
            assert conn._reconnect_count == 1
        finally:
            await conn.close(flush=False)

    async def test_stale_force_flag_does_not_skip_a_later_real_backoff(self) -> None:
        """Review regression: a force issued while no session existed (e.g.
        during the retry_on_failed_connect phase) must not survive the next
        successful connect and silently skip a future real backoff."""
        env = FakeEnv()
        recorder = EventRecorder()
        env.refuse_next(1)
        conn = Connection(
            make_options(
                retry_on_failed_connect=True,
                reconnect_time_wait=0.3,
                reconnect_time_wait_max=0.3,
            ),
            transport_factory=env.factory,
        )
        conn.bus.subscribe(recorder.hook)
        await conn.connect()  # returns while RECONNECTING
        try:
            await conn.force_reconnect()  # no session to drop: only arms the flag
            await recorder.wait_for(Connected, timeout=5.0)
            assert conn._force_reconnect is False  # honored by the connect
            loop = asyncio.get_running_loop()
            dropped_at = loop.time()
            env.current.drop()
            await recorder.wait_for(Reconnected, timeout=5.0)
            # The real disconnect respected the configured backoff.
            assert loop.time() - dropped_at >= 0.25
        finally:
            await conn.close(flush=False)


class TestRetryOnFailedConnect:
    async def test_initial_failure_enters_reconnecting_then_connects(self) -> None:
        env = FakeEnv()
        env.refuse_next(1)  # the initial connect finds no server
        recorder = EventRecorder()
        conn = Connection(
            make_options(retry_on_failed_connect=True, max_reconnect_attempts=-1),
            transport_factory=env.factory,
        )
        conn.bus.subscribe(recorder.hook)
        received: list[MsgEvent | HMsgEvent] = []
        try:
            await conn.connect()  # returns instead of raising
            assert conn.state is ConnectionState.RECONNECTING
            # Registered and published while still waiting for the first connect.
            entry = conn.subscribe("pending.sub", None, received.append)
            await conn.publish_frame(b"PUB buffered 3\r\nyes\r\n")
            # The next attempt succeeds; the first success fires Connected, not Reconnected.
            await recorder.wait_for(Connected)
            assert conn.state is ConnectionState.CONNECTED
            assert recorder.count(Reconnected) == 0
            await conn.flush()
            written = frames_written(env.current)
            assert f"SUB pending.sub {entry.sid}\r\n".encode() in written
            assert b"PUB buffered 3\r\nyes\r\n" in written
            env.current.deliver(f"MSG pending.sub {entry.sid} 2\r\nok\r\n".encode())
            assert len(received) == 1
        finally:
            await conn.close(flush=False)

    async def test_without_option_initial_failure_raises(self) -> None:
        env = FakeEnv()
        env.refuse_next(1)
        conn = Connection(make_options(), transport_factory=env.factory)
        with pytest.raises(NoServersAvailableError):
            await conn.connect()
        assert conn.state is ConnectionState.CLOSED

    async def test_repeated_auth_error_aborts_and_closes(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        budget = {"n": 1000}  # every handshake is rejected with the same auth error
        _reject_auth_after_connect(env, budget, b"-ERR 'Authorization Violation'\r\n")
        conn = Connection(
            make_options(retry_on_failed_connect=True, max_reconnect_attempts=-1),
            transport_factory=env.factory,
        )
        conn.bus.subscribe(recorder.hook)
        await conn.connect()  # returns (RECONNECTING) despite the rejection
        # Two identical auth rejections during retry abort the loop and close.
        await recorder.wait_for(Closed, timeout=5.0)
        assert conn.state is ConnectionState.CLOSED
        assert recorder.count(Connected) == 0


class TestReconnectBufSize:
    async def test_overflow_raises_and_connection_survives(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        frame = b"PUB x 1\r\na\r\n"
        # Cap == one frame: the first publish fills it, the second overflows.
        conn = await connected_conn(env, recorder, reconnect_buf_size=len(frame))
        try:
            env.refuse_next(1)  # widen the reconnect window
            env.current.drop()
            await recorder.wait_for(Disconnected)
            await conn.publish_frame(frame)  # buffered == cap now
            with pytest.raises(ReconnectBufExceededError):
                await conn.publish_frame(frame)
            # The overflow is not fatal: the connection still reconnects.
            await recorder.wait_for(Reconnected)
            assert conn.state is ConnectionState.CONNECTED
        finally:
            await conn.close(flush=False)

    async def test_minus_one_disables_buffering(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder, reconnect_buf_size=-1)
        try:
            # While connected, publishing is unaffected.
            await conn.publish_frame(b"PUB ok 1\r\na\r\n")
            env.refuse_next(1)
            env.current.drop()
            await recorder.wait_for(Disconnected)
            # While disconnected, every publish fails immediately (nothing buffers).
            with pytest.raises(ReconnectBufExceededError):
                await conn.publish_frame(b"PUB down 1\r\na\r\n")
            assert conn._reconnect_buffer_size == 0
            await recorder.wait_for(Reconnected)
            assert conn.state is ConnectionState.CONNECTED
        finally:
            await conn.close(flush=False)

    async def test_minus_one_drop_in_lost_window_is_loud(self) -> None:
        """Review regression: a publish that races the CONNECTED->lost window
        lands on the dead session; with buffering disabled it is lost by
        contract, but the loss must be REPORTED, never silent."""
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder, reconnect_buf_size=-1)
        try:
            env.refuse_next(1)
            frame = b"PUB win 3\r\nabc\r\n"
            # A fatal -ERR marks the session lost synchronously; the supervisor
            # has not flipped the state yet, so the publish takes the CONNECTED
            # path onto the dead session.
            env.current.deliver(b"-ERR 'Stale Connection'\r\n")
            assert conn.state is ConnectionState.CONNECTED
            await conn.publish_frame(frame)
            await recorder.wait_for(Reconnected, timeout=3.0)
            drops = [
                event
                for event in recorder.events
                if isinstance(event, ErrorOccurred) and isinstance(event.error, ReconnectBufExceededError)
            ]
            assert len(drops) == 1
            await conn.flush()
            assert frame not in frames_written(env.current)  # lost by contract, loudly
        finally:
            await conn.close(flush=False)

    async def test_buffered_bytes_flush_on_reconnect(self) -> None:
        env = FakeEnv()
        recorder = EventRecorder()
        conn = await connected_conn(env, recorder, reconnect_buf_size=1024)
        try:
            env.refuse_next(1)
            env.current.drop()
            await recorder.wait_for(Disconnected)
            frame = b"PUB survive 3\r\nyes\r\n"
            await conn.publish_frame(frame)
            await recorder.wait_for(Reconnected)
            await conn.flush()
            assert frame in frames_written(env.current)
        finally:
            await conn.close(flush=False)


class TestInstrumentationWiring:
    """The default Noop is stored bare; user backends get pre-bound guards."""

    def test_default_instrumentation_is_bare_noop(self) -> None:
        # No wrapper for the default: NoopInstrumentation's hooks cannot raise,
        # so the guard would only add per-call overhead on the read path.
        conn = Connection(make_options(), transport_factory=FakeEnv().factory)
        assert type(conn.instrumentation) is NoopInstrumentation

    def test_supplied_instrumentation_is_wrapped(self) -> None:
        conn = Connection(make_options(instrumentation=NoopInstrumentation()), transport_factory=FakeEnv().factory)
        assert isinstance(conn.instrumentation, _SafeInstrumentation)

    def test_guards_are_prebound_once(self) -> None:
        # With a per-__getattr__ closure the two reads would differ; pre-binding
        # returns the identical bound wrapper from its slot every time.
        safe = _SafeInstrumentation(NoopInstrumentation())
        assert safe.on_bytes_received is safe.on_bytes_received
        assert safe.on_message_delivered is safe.on_message_delivered

    def test_raising_backend_is_swallowed(self) -> None:
        class RaisingBackend(NoopInstrumentation):
            def on_error(self, error: Exception) -> None:
                raise RuntimeError("metrics backend is down")

            def on_bytes_received(self, count: int) -> None:
                raise RuntimeError("metrics backend is down")

            def on_message_delivered(self, subject: str, payload_size: int) -> None:
                raise RuntimeError("metrics backend is down")

        safe = _SafeInstrumentation(RaisingBackend())
        # Every guarded hook swallows the failure instead of propagating it.
        safe.on_error(RuntimeError("boom"))
        safe.on_bytes_received(42)
        safe.on_message_delivered("subj", 3)
