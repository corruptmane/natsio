"""Managed NATS connection: handshake, write path, liveness, reconnect.

Layering: one :class:`Connection` (lives as long as the client) supervises a
sequence of :class:`_Session` objects (one per physical transport connection).
A session owns the parser, the coalescing flusher, and ping liveness; the
connection owns the server pool, the subscription registry, reconnect policy,
and the event bus.

Concurrency model: all inbound dispatch happens synchronously inside the
transport's ``data_received`` callback — there is no reader task, and nothing
on that path may block. The only per-session tasks are the flusher and the
pinger, supervised by a TaskGroup that collapses when the session is lost.
"""

import asyncio
import builtins
import json
import logging
import random
from collections import deque
from collections.abc import Callable
from typing import Any

from natsio.errors import (
    ConfigError,
    ConnectionClosedError,
    NATSError,
    NoServersAvailableError,
    ParserError,
    ServerError,
    StaleConnectionError,
    TimeoutError,
)
from natsio.instrumentation import Instrumentation, NoopInstrumentation
from natsio.options import ConnectOptions, TLSConfig

from .auth import Authenticator, AuthResult, TokenAuth, UserPasswordAuth
from .dispatcher import Dispatcher, MessageHandler, SubscriptionEntry
from .lifecycle import (
    Closed,
    Connected,
    ConnectionEvent,
    ConnectionState,
    Disconnected,
    ErrorOccurred,
    EventBus,
    LameDuck,
    Reconnected,
    ServersDiscovered,
)
from .pool import ParsedServer, ServerPool
from .protocol import (
    NEED_DATA,
    PING_FRAME,
    PONG_FRAME,
    ErrEvent,
    HMsgEvent,
    InfoEvent,
    MsgEvent,
    OkEvent,
    Parser,
    PingEvent,
    PongEvent,
    build_connect_payload,
    classify_server_error,
    encode_connect,
    encode_sub,
    encode_unsub,
)
from .transport import TCPTransport, Transport

__all__ = ["Connection", "TransportFactory"]

log = logging.getLogger("natsio.connection")

type TransportFactory = Callable[..., Transport]


class _SafeInstrumentation:
    """Wraps user instrumentation so a broken metrics backend cannot kill the
    connection. Hooks fire on the read path, where an escaping exception would
    reach ``data_received`` and make asyncio abort the socket."""

    __slots__ = ("_inner",)

    def __init__(self, inner: Instrumentation) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Callable[..., None]:
        hook = getattr(self._inner, name)

        def guarded(*args: Any, **kwargs: Any) -> None:
            try:
                hook(*args, **kwargs)
            except Exception:
                log.exception("instrumentation hook %r failed", name)

        return guarded


class _SessionLostError(Exception):
    """Internal: collapses the session TaskGroup. Carries the loss reason."""

    def __init__(self, error: Exception | None) -> None:
        super().__init__(str(error) if error else "session lost")
        self.error = error


class _ClosingError(Exception):
    """Internal: the user requested close while we were between sessions."""


class _Session:
    """One physical connection: parser state, write buffer, liveness."""

    __slots__ = (
        "_conn",
        "_drain_waiters",
        "_flush_event",
        "handshake_pong",
        "info_future",
        "lost_future",
        "outstanding_pings",
        "parser",
        "pending",
        "pending_size",
        "ping_waiters",
        "running",
        "server",
        "server_info",
        "transport",
    )

    def __init__(self, conn: "Connection", server: ParsedServer) -> None:
        self._conn = conn
        self.server = server
        self.parser = Parser(max_control_line=conn.options.max_control_line)
        self.transport: Transport | None = None
        self.pending: list[bytes] = []
        self.pending_size = 0
        self._flush_event = asyncio.Event()
        self._drain_waiters: deque[asyncio.Future[None]] = deque()
        # One entry per PING written, in order; None for liveness pings.
        self.ping_waiters: deque[asyncio.Future[None] | None] = deque()
        self.outstanding_pings = 0
        self.running = False  # False during handshake
        self.server_info: dict[str, Any] = {}
        loop = asyncio.get_running_loop()
        self.info_future: asyncio.Future[bytes] = loop.create_future()
        self.handshake_pong: asyncio.Future[None] = loop.create_future()
        self.lost_future: asyncio.Future[Exception | None] = loop.create_future()

    # -- inbound path (sync, called from transport callbacks) ---------------

    def feed(self, data: bytes) -> None:
        self._conn.instrumentation.on_bytes_received(len(data))
        try:
            self.parser.receive_data(data)
            while True:
                if self.lost_future.done():
                    # Real transports still deliver buffered data after close();
                    # dispatching from an abandoned session would duplicate
                    # messages that the next session is about to replay.
                    return
                event = self.parser.next_event()
                if event is NEED_DATA:
                    return
                self._handle(event)
        except ParserError as exc:
            self.mark_lost(exc)

    def _handle(self, event: object) -> None:
        match event:
            case MsgEvent() | HMsgEvent():
                self._conn.instrumentation.on_message_delivered(event.subject, len(event.payload))
                self._conn.dispatcher.dispatch(event)
            case PingEvent():
                self.enqueue(PONG_FRAME)
            case PongEvent():
                self.outstanding_pings = 0
                if not self.handshake_pong.done():
                    self.handshake_pong.set_result(None)
                    return
                if self.ping_waiters:
                    waiter = self.ping_waiters.popleft()
                    if waiter is not None and not waiter.done():
                        waiter.set_result(None)
            case InfoEvent():
                if not self.info_future.done():
                    self.info_future.set_result(event.raw)
                else:
                    self._conn.handle_async_info(self, event.raw)
            case ErrEvent():
                self._handle_err(event.message)
            case OkEvent():
                pass

    def _handle_err(self, message: str) -> None:
        error = classify_server_error(message)
        if not error.fatal:
            self._conn.background_error(error)
            return
        if not self.running:
            # Handshake failure (auth, TLS required, ...) — fail the awaiting futures.
            for future in (self.info_future, self.handshake_pong):
                if not future.done():
                    future.set_exception(error)
        self.mark_lost(error)

    # -- outbound path ------------------------------------------------------

    def enqueue(self, frame: bytes) -> None:
        """Non-blocking append (internal/control frames bypass the watermark)."""
        self.pending.append(frame)
        self.pending_size += len(frame)
        self._flush_event.set()

    def enqueue_many(self, frames: list[bytes]) -> None:
        """Append frames individually.

        They must NOT be concatenated first: ``take_unsent_user_frames``
        classifies each element by its leading opcode, and a joined blob would
        be judged by whichever frame happens to be first — carrying control
        frames across a reconnect (duplicate SUBs) or discarding publishes.
        """
        if not frames:
            return
        self.pending.extend(frames)
        self.pending_size += sum(len(f) for f in frames)
        self._flush_event.set()

    async def send(self, frame: bytes) -> None:
        """User-path append with high-water-mark backpressure."""
        max_pending = self._conn.options.max_pending_size
        # Loop rather than test-once: _wake_drain_waiters releases every waiter
        # at once, and if each appended unconditionally the buffer would
        # overshoot the limit by roughly the number of blocked publishers.
        while self.pending_size + len(frame) > max_pending:
            if self.lost_future.done():
                # The flusher is gone; nobody would ever resolve a new waiter.
                raise ConnectionClosedError("connection lost while waiting for the write buffer")
            waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._drain_waiters.append(waiter)
            self._flush_event.set()
            try:
                async with asyncio.timeout(self._conn.options.flush_timeout):
                    await waiter
            except builtins.TimeoutError:
                raise TimeoutError("write buffer full: flusher could not drain in time") from None
        self.enqueue(frame)

    def send_ping(self, waiter: asyncio.Future[None] | None) -> None:
        # Deliberately does NOT touch outstanding_pings: that counter is the
        # pinger's liveness budget, and counting user flush() pings against it
        # would let a few concurrent flushes declare a healthy connection stale.
        self.ping_waiters.append(waiter)
        self.enqueue(PING_FRAME)

    async def flusher_loop(self) -> None:
        transport = self.transport
        assert transport is not None
        while True:
            await self._flush_event.wait()
            self._flush_event.clear()
            if not self.pending:
                self._wake_drain_waiters()
                continue
            await transport.wait_writable()
            if self.lost_future.done():
                return
            # Swap-then-write with no await in between: frames enqueued during
            # the synchronous write land in the fresh list, never dropped.
            frames = self.pending
            self.pending = []
            self.pending_size = 0
            data = b"".join(frames)
            try:
                transport.write(data)
            except Exception as exc:
                # Put them back before tearing down: _carry_over_unsent runs
                # against `pending`, so frames dropped here would be lost on
                # exactly the path that exists to preserve them.
                self.pending[:0] = frames
                self.pending_size += len(data)
                self.mark_lost(exc if isinstance(exc, NATSError) else ConnectionClosedError(str(exc)))
                return
            self._conn.instrumentation.on_bytes_sent(len(data))
            self._wake_drain_waiters()

    def _wake_drain_waiters(self) -> None:
        if self.pending_size <= self._conn.options.max_pending_size // 2:
            while self._drain_waiters:
                waiter = self._drain_waiters.popleft()
                if not waiter.done():
                    waiter.set_result(None)

    async def pinger_loop(self) -> None:
        options = self._conn.options
        while True:
            await asyncio.sleep(options.ping_interval)
            if self.outstanding_pings >= options.max_outstanding_pings:
                self.mark_lost(StaleConnectionError("no PONG for outstanding PINGs"))
                return
            self.send_ping(None)
            self.outstanding_pings += 1

    async def final_flush(self) -> None:
        """Best-effort write of whatever is pending (used during graceful close)."""
        transport = self.transport
        if transport is None or transport.is_closing or not self.pending:
            return
        frames = self.pending
        self.pending = []
        self.pending_size = 0
        try:
            transport.write(b"".join(frames))
        except Exception:
            log.debug("final flush failed", exc_info=True)

    # -- teardown -----------------------------------------------------------

    def mark_lost(self, error: Exception | None) -> None:
        """Idempotent, callable from any (sync) context. The single loss entrypoint."""
        if self.lost_future.done():
            return
        self.lost_future.set_result(error)
        failure = error if error is not None else ConnectionClosedError("connection lost")
        for future in (self.info_future, self.handshake_pong):
            if not future.done():
                future.set_exception(failure)
        while self.ping_waiters:
            waiter = self.ping_waiters.popleft()
            if waiter is not None and not waiter.done():
                waiter.set_exception(failure)
        while self._drain_waiters:
            waiter = self._drain_waiters.popleft()
            if not waiter.done():
                waiter.set_exception(failure)
        # Consume the exceptions of futures nobody awaits (handshake already done).
        for future in (self.info_future, self.handshake_pong):
            if future.done() and not future.cancelled():
                future.exception()
        if self.transport is not None and not self.transport.is_closing:
            self.transport.close()

    def take_unsent_user_frames(self) -> list[bytes]:
        """Unflushed PUB/HPUB frames at loss time; control/SUB frames are excluded
        (replay and liveness regenerate those)."""
        frames = [f for f in self.pending if f.startswith((b"PUB ", b"HPUB "))]
        self.pending = []
        self.pending_size = 0
        return frames

    async def wait_lost_then_collapse(self) -> None:
        error = await self.lost_future
        raise _SessionLostError(error)


class Connection:
    """The long-lived connection manager."""

    def __init__(
        self,
        options: ConnectOptions,
        *,
        transport_factory: TransportFactory | None = None,
    ) -> None:
        self.options = options
        self.instrumentation: Any = _SafeInstrumentation(options.instrumentation or NoopInstrumentation())
        self.dispatcher = Dispatcher()
        self.bus = EventBus()
        self._authenticator = options.resolve_authenticator()
        self._transport_factory: TransportFactory = transport_factory or TCPTransport
        self._pool = ServerPool(
            options.servers,
            randomize=not options.no_randomize,
            max_consecutive_failures=options.max_reconnect_attempts,
            accept_discovered=not options.ignore_discovered_servers,
        )
        self._state = ConnectionState.DISCONNECTED
        self._session: _Session | None = None
        # Published while _establish runs, so close() can interrupt a handshake
        # that is parked waiting for INFO instead of blocking for connect_timeout.
        self._establishing: _Session | None = None
        self._supervisor: asyncio.Task[None] | None = None
        self._closing = False
        self._closed_event = asyncio.Event()
        self._first_connect: asyncio.Future[None] | None = None
        self._reconnect_buffer: list[bytes] = []
        self._reconnect_buffer_size = 0
        self._reconnect_count = 0

    # -- public surface ------------------------------------------------------

    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def is_connected(self) -> bool:
        return self._state is ConnectionState.CONNECTED

    @property
    def server_info(self) -> dict[str, Any]:
        return dict(self._session.server_info) if self._session else {}

    @property
    def connected_url(self) -> str | None:
        if self._session is not None and self.is_connected:
            return self._session.server.url
        return None

    async def connect(self) -> None:
        # Guarding on the supervisor alone is not enough: _supervise clears it
        # on exit, so a post-close() connect() would slip through.
        if self._state is not ConnectionState.DISCONNECTED:
            raise ConfigError(f"connect() is not valid while {self._state.name}")
        self._state = ConnectionState.CONNECTING
        loop = asyncio.get_running_loop()
        self._first_connect = loop.create_future()
        self._supervisor = loop.create_task(self._supervise(), name="natsio-supervisor")
        try:
            await self._first_connect
        except BaseException:
            await self.close(flush=False)
            raise

    async def close(self, *, flush: bool = True) -> None:
        if self._state is ConnectionState.CLOSED and self._supervisor is None:
            return
        self._closing = True
        self._closed_event.set()
        # Unblock a handshake parked on INFO/PONG so we do not wait out
        # connect_timeout before the supervisor can notice we are closing.
        establishing = self._establishing
        if establishing is not None:
            establishing.mark_lost(None)
        session = self._session
        if session is not None and flush and self.is_connected:
            try:
                async with asyncio.timeout(self.options.drain_timeout):
                    await self.flush()
            except Exception:
                log.debug("flush during close failed", exc_info=True)
        if self._state is not ConnectionState.CLOSED:
            self._state = ConnectionState.DRAINING
        if session is not None:
            await session.final_flush()
            session.mark_lost(None)
        supervisor = self._supervisor
        if supervisor is not None and asyncio.current_task() is not supervisor:
            try:
                await supervisor
            except asyncio.CancelledError:
                # The supervisor was cancelled, not our caller — do not let that
                # surface as if close() itself had been cancelled.
                log.debug("supervisor was cancelled during close")
            except Exception:
                log.debug("supervisor ended with error during close", exc_info=True)
        self._finalize_closed()

    async def drain(self) -> None:
        """Graceful shutdown. (Subscription draining arrives with the client layer.)"""
        await self.close(flush=True)

    async def flush(self, timeout: float | None = None) -> None:  # noqa: ASYNC109
        session = self._require_session()
        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        session.send_ping(waiter)
        try:
            async with asyncio.timeout(timeout if timeout is not None else self.options.flush_timeout):
                await waiter
        except builtins.TimeoutError:
            raise TimeoutError("flush timed out awaiting PONG") from None

    async def publish_frame(self, frame: bytes) -> None:
        """Send an already-encoded frame, honoring state and backpressure."""
        if self._state is ConnectionState.CONNECTED:
            session = self._session
            assert session is not None
            await session.send(frame)
            return
        if self._state in (ConnectionState.CONNECTING, ConnectionState.RECONNECTING):
            self._buffer_for_reconnect(frame)
            return
        raise ConnectionClosedError(f"cannot send while {self._state.name}")

    def pause_reading(self) -> None:
        """Stop draining the socket (connection-wide backpressure)."""
        session = self._session
        if session is not None and session.transport is not None:
            session.transport.pause_reading()

    def resume_reading(self) -> None:
        session = self._session
        if session is not None and session.transport is not None:
            session.transport.resume_reading()

    def subscribe(self, subject: str, queue: str | None, handler: MessageHandler) -> SubscriptionEntry:
        if self._state in (ConnectionState.CLOSED, ConnectionState.DRAINING):
            raise ConnectionClosedError("connection is closed")
        entry = self.dispatcher.add(subject, queue, handler)
        self._send_control(encode_sub(subject, entry.sid, queue))
        return entry

    def unsubscribe(self, sid: int, max_msgs: int | None = None) -> None:
        if self.dispatcher.get(sid) is None:
            return
        if max_msgs is None or max_msgs <= 0:
            self.dispatcher.remove(sid)
            self._send_control(encode_unsub(sid))
        else:
            self.dispatcher.arm_auto_unsub(sid, max_msgs)
            self._send_control(encode_unsub(sid, max_msgs))

    # -- supervisor ----------------------------------------------------------

    async def _supervise(self) -> None:
        initial = True
        last_error: Exception | None = None
        try:
            while not self._closing:
                try:
                    session = await self._establish_any(initial=initial)
                except _ClosingError:
                    break
                except Exception as exc:
                    last_error = exc
                    break
                if self._closing:
                    session.mark_lost(None)
                    break
                self._session = session
                self._state = ConnectionState.CONNECTED
                if initial:
                    initial = False
                    assert self._first_connect is not None
                    if not self._first_connect.done():
                        self._first_connect.set_result(None)
                    # Anything registered or published while still CONNECTING.
                    self._replay_subscriptions(session)
                    self._flush_reconnect_buffer(session)
                    self.instrumentation.on_connect(session.server.url)
                    self._emit(Connected(session.server.url))
                else:
                    self._reconnect_count += 1
                    self._replay_subscriptions(session)
                    self._flush_reconnect_buffer(session)
                    self.instrumentation.on_reconnect(session.server.url, self._reconnect_count)
                    self._emit(Reconnected(session.server.url))
                error = await self._run_session(session)
                self._session = None
                if self._closing:
                    break
                self._carry_over_unsent(session)
                # Move out of CONNECTED *before* notifying: a hook that calls
                # back in must not observe state=CONNECTED with no session.
                self._state = (
                    ConnectionState.RECONNECTING if self.options.allow_reconnect else ConnectionState.DISCONNECTED
                )
                self.instrumentation.on_disconnect(error)
                self._emit(Disconnected(error))
                if not self.options.allow_reconnect:
                    last_error = error
                    break
        finally:
            if self._first_connect is not None and not self._first_connect.done():
                self._first_connect.set_exception(
                    last_error or NoServersAvailableError("could not connect to any server")
                )
            elif last_error is not None and not self._closing:
                self.background_error(last_error)
            self._supervisor = None
            self._finalize_closed()

    async def _run_session(self, session: _Session) -> Exception | None:
        # NOTE: `return` is a SyntaxError inside `except*` blocks — collect into
        # a variable instead. If both a routine loss and a real bug are present,
        # the real bug wins so it is not masked as a plain disconnect.
        result: Exception | None = None
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(session.flusher_loop(), name="natsio-flusher")
                tg.create_task(session.pinger_loop(), name="natsio-pinger")
                tg.create_task(session.wait_lost_then_collapse(), name="natsio-lost-watch")
        except* _SessionLostError as group:
            lost = group.exceptions[0]
            assert isinstance(lost, _SessionLostError)
            result = lost.error
        except* Exception as group:  # real bug in flusher/pinger — loud, then reconnect
            exc = group.exceptions[0]
            log.exception("internal session task crashed", exc_info=exc)
            session.mark_lost(None)
            actual = exc if isinstance(exc, Exception) else ConnectionClosedError(str(exc))
            self.background_error(actual)
            result = actual
        return result

    async def _establish_any(self, *, initial: bool) -> _Session:
        last_error: Exception | None = None
        while True:
            candidates = self._pool.candidates()
            if not candidates:
                raise NoServersAvailableError(str(last_error) if last_error else "no servers available")
            for server in candidates:
                if self._closing:
                    raise _ClosingError
                if not initial:
                    await self._backoff(server)
                    if self._closing:
                        raise _ClosingError
                try:
                    session = await self._establish(server)
                except Exception as exc:
                    last_error = exc
                    self._pool.mark_failure(server)
                    log.info("connect to %s failed: %s", server.url, exc)
                    continue
                self._pool.mark_success(server)
                return session
            if initial:
                raise NoServersAvailableError(f"initial connect failed: {last_error}") from last_error

    async def _backoff(self, server: ParsedServer) -> None:
        options = self.options
        uses_tls = server.tls_required or options.tls is not None
        jitter_cap = options.reconnect_jitter_tls if uses_tls else options.reconnect_jitter
        delay = random.uniform(0, jitter_cap)
        if server.consecutive_failures > 0:
            backoff = options.reconnect_time_wait * (2 ** (server.consecutive_failures - 1))
            delay += min(backoff, options.reconnect_time_wait_max)
        # A server that completes the handshake and then drops resets its failure
        # counter, so the exponential term alone would let a flapping server be
        # retried in a tight loop. Enforce reconnect_time_wait between successive
        # attempts to the SAME server regardless of how the previous one ended.
        elapsed = asyncio.get_running_loop().time() - server.last_attempt
        delay = max(delay, options.reconnect_time_wait - elapsed)
        if delay <= 0:
            return
        try:
            async with asyncio.timeout(delay):
                await self._closed_event.wait()
        except builtins.TimeoutError:
            return

    async def _establish(self, server: ParsedServer) -> _Session:
        options = self.options
        server.last_attempt = asyncio.get_running_loop().time()
        session = _Session(self, server)
        self._establishing = session
        transport = self._transport_factory(on_bytes=session.feed, on_close=session.mark_lost)
        session.transport = transport

        tls_config = options.tls
        wants_tls = server.tls_required or tls_config is not None
        handshake_first = tls_config is not None and tls_config.handshake_first
        tls_hostname = (tls_config.hostname if tls_config else None) or server.host

        async with asyncio.timeout(options.connect_timeout):
            await transport.connect(
                server.host,
                server.port,
                tls=(tls_config or TLSConfig()).resolve_context() if handshake_first else None,
                tls_hostname=tls_hostname if handshake_first else None,
            )
            try:
                raw_info = await session.info_future
                info: dict[str, Any] = json.loads(raw_info)
                session.server_info = info
                if info.get("max_payload"):
                    session.parser.set_max_payload(int(info["max_payload"]))
                # A real server advertises the whole cluster in the very first
                # INFO; async INFO frames only follow on membership changes.
                self._merge_connect_urls(info)

                if (wants_tls or info.get("tls_required")) and not handshake_first:
                    context = (tls_config or TLSConfig()).resolve_context()
                    await transport.upgrade_tls(context, tls_hostname)

                auth = await self._authenticate(server, info)
                payload = build_connect_payload(
                    version=_client_version(),
                    verbose=options.verbose,
                    pedantic=options.pedantic,
                    tls_required=wants_tls or handshake_first,
                    echo=options.echo,
                    name=options.name,
                    user=auth.user,
                    password=auth.password,
                    auth_token=auth.auth_token,
                    jwt=auth.jwt,
                    nkey=auth.nkey,
                    signature=auth.signature,
                )
                connect_json = json.dumps(payload, separators=(",", ":")).encode()
                transport.write(encode_connect(connect_json) + PING_FRAME)
                await session.handshake_pong
            except BaseException:
                session.mark_lost(None)
                transport.abort()
                raise
            finally:
                self._establishing = None
        # Finding 10: a fatal -ERR arriving in the same segment as the handshake
        # PONG resolves handshake_pong first and only marks the session lost, so
        # without this check we would publish a corpse as CONNECTED.
        if session.lost_future.done():
            error = session.lost_future.result()
            raise error if error is not None else ConnectionClosedError("connection lost during handshake")
        session.running = True
        return session

    async def _authenticate(self, server: ParsedServer, info: dict[str, Any]) -> AuthResult:
        nonce = info["nonce"].encode() if isinstance(info.get("nonce"), str) else None
        authenticator: Authenticator | None = self._authenticator
        if authenticator is None and server.username is not None:
            # URL userinfo fallback: nats://user:pass@host or nats://token@host
            if server.password is not None:
                authenticator = UserPasswordAuth(user=server.username, password=server.password)
            else:
                authenticator = TokenAuth(token=server.username)
        if authenticator is None:
            return AuthResult()
        return await authenticator.authenticate(nonce)

    # -- reconnect plumbing --------------------------------------------------

    def _replay_subscriptions(self, session: _Session) -> None:
        frames: list[bytes] = []
        for entry in self.dispatcher.entries():
            frames.append(encode_sub(entry.subject, entry.sid, entry.queue))
            remaining = entry.remaining
            if remaining is not None:
                frames.append(encode_unsub(entry.sid, remaining))
        session.enqueue_many(frames)

    def _flush_reconnect_buffer(self, session: _Session) -> None:
        if not self._reconnect_buffer:
            return
        session.enqueue_many(list(self._reconnect_buffer))
        self._reconnect_buffer.clear()
        self._reconnect_buffer_size = 0

    def _buffer_for_reconnect(self, frame: bytes) -> None:
        if not self.options.allow_reconnect:
            raise ConnectionClosedError("disconnected and reconnect is disabled")
        if self._reconnect_buffer_size + len(frame) > self.options.max_pending_size:
            raise ConnectionClosedError("reconnect buffer is full")
        self._reconnect_buffer.append(frame)
        self._reconnect_buffer_size += len(frame)

    def _carry_over_unsent(self, session: _Session) -> None:
        """Preserve unflushed user publishes across the reconnect (bounded)."""
        if not self.options.allow_reconnect:
            return
        for position, frame in enumerate(session.take_unsent_user_frames()):
            if self._reconnect_buffer_size + len(frame) > self.options.max_pending_size:
                self.background_error(ConnectionClosedError("reconnect buffer full: dropping unflushed publishes"))
                break
            self._reconnect_buffer.insert(position, frame)
            self._reconnect_buffer_size += len(frame)

    def _send_control(self, frame: bytes) -> None:
        """Send a SUB/UNSUB now, or rely on replay if we are not connected.

        Control frames are deliberately NOT buffered: the dispatcher is the
        source of truth for subscription state and _replay_subscriptions emits
        it on every (re)connect. Buffering as well would send the same SUB
        twice, and a duplicate sid means every matching message is delivered
        twice for the life of the session.
        """
        if self._state is ConnectionState.CONNECTED and self._session is not None:
            self._session.enqueue(frame)
        elif self._state in (ConnectionState.CLOSED, ConnectionState.DRAINING):
            raise ConnectionClosedError(f"cannot send while {self._state.name}")

    # -- inbound hooks -------------------------------------------------------

    def handle_async_info(self, session: _Session, raw: bytes) -> None:
        try:
            info: dict[str, Any] = json.loads(raw)
        except ValueError:
            log.warning("ignoring malformed async INFO")
            return
        session.server_info.update(info)
        self._merge_connect_urls(info)
        if info.get("ldm"):
            self._emit(LameDuck(session.server.url))
            if self.options.allow_reconnect and not self._closing:
                # Migrate before the server evicts us — but on the NEXT loop
                # iteration, so messages the server packed into this same
                # segment after the INFO are still delivered. Lame duck is a
                # graceful drain; truncating it mid-segment loses data.
                asyncio.get_running_loop().call_soon(session.mark_lost, None)

    def _merge_connect_urls(self, info: dict[str, Any]) -> None:
        urls = info.get("connect_urls")
        if not isinstance(urls, list) or not urls:
            return
        added = self._pool.merge_discovered([u for u in urls if isinstance(u, str)])
        if added:
            self._emit(ServersDiscovered(tuple(s.url for s in added)))

    def background_error(self, error: Exception) -> None:
        self.instrumentation.on_error(error)
        if isinstance(error, ServerError) and not error.fatal:
            log.warning("server error: %s", error)
        self._emit(ErrorOccurred(error))

    # -- misc ----------------------------------------------------------------

    def _emit(self, event: ConnectionEvent) -> None:
        self.bus.emit(event)

    def _require_session(self) -> _Session:
        if self._session is None or not self.is_connected:
            raise ConnectionClosedError("not connected")
        return self._session

    def _finalize_closed(self) -> None:
        if self._state is ConnectionState.CLOSED:
            return
        self._state = ConnectionState.CLOSED
        self._closing = True
        self._closed_event.set()
        self.instrumentation.on_close()
        self._emit(Closed())


def _client_version() -> str:
    from natsio import __version__

    return __version__
