"""Managed NATS connection: handshake, write path, liveness, reconnect.

Layering: one `Connection` (lives as long as the client) supervises a
sequence of `_Session` objects (one per physical transport connection).
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
import re
from collections import deque
from collections.abc import Callable
from typing import Any

from natsio.errors import (
    AuthenticationExpiredError,
    AuthorizationViolationError,
    ConfigError,
    ConnectionClosedError,
    NATSError,
    NoServersAvailableError,
    ParserError,
    PermissionsViolationError,
    ReconnectBufExceededError,
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
from .transport import TCPTransport, Transport, WSTransport

__all__ = ["Connection", "TransportFactory"]

log = logging.getLogger("natsio.connection")

type TransportFactory = Callable[..., Transport]

# nats.go DefaultReconnectBufSize (8MB); reconnect_buf_size=0 resolves to this.
_DEFAULT_RECONNECT_BUF_SIZE = 8 * 1024 * 1024

# Subject/queue tokens inside a "Permissions Violation for Subscription to
# "<subject>" [using queue "<queue>"]" -ERR (parity with nats.go permissionsRe /
# permissionsQueueRe). The publish variant carries no "Subscription to" token.
_PERM_SUBJECT_RE = re.compile(r'Subscription to "(\S+)"')
_PERM_QUEUE_RE = re.compile(r'using queue "(\S+)"')


# Every hook on the Instrumentation protocol, pre-bound once per _SafeInstrumentation.
_HOOK_NAMES = (
    "on_connect",
    "on_disconnect",
    "on_reconnect",
    "on_close",
    "on_bytes_sent",
    "on_bytes_received",
    "on_message_published",
    "on_message_delivered",
    "on_slow_consumer",
    "on_error",
)


class _SafeInstrumentation:
    """Wraps user instrumentation so a broken metrics backend cannot kill the
    connection. Hooks fire on the read path, where an escaping exception would
    reach ``data_received`` and make asyncio abort the socket.

    Each guarded wrapper is bound ONCE in ``__init__`` and stored in its own
    slot, so hot-path hook access is a plain attribute read — no per-call
    closure allocation and no ``__getattr__`` round-trip."""

    __slots__ = _HOOK_NAMES

    on_connect: Callable[..., None]
    on_disconnect: Callable[..., None]
    on_reconnect: Callable[..., None]
    on_close: Callable[..., None]
    on_bytes_sent: Callable[..., None]
    on_bytes_received: Callable[..., None]
    on_message_published: Callable[..., None]
    on_message_delivered: Callable[..., None]
    on_slow_consumer: Callable[..., None]
    on_error: Callable[..., None]

    def __init__(self, inner: Instrumentation) -> None:
        for name in _HOOK_NAMES:
            setattr(self, name, self._guard(name, getattr(inner, name)))

    @staticmethod
    def _guard(name: str, hook: Callable[..., None]) -> Callable[..., None]:
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


class _ConfigClassError(Exception):
    """Internal: a non-retryable auth/config failure (missing creds file, bad
    seed, no Ed25519 backend). Trying the next server cannot fix it, so it must
    fast-fail instead of being masked by a later transient error. Carries the
    original so it can be re-raised unwrapped."""

    def __init__(self, error: Exception) -> None:
        super().__init__(str(error))
        self.error = error


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
            case MsgEvent():
                # Split from HMsgEvent so the headerless case passes None with
                # no per-message getattr — the delivery path is hot.
                self._conn.instrumentation.on_message_delivered(event.subject, None, len(event.payload))
                self._conn.dispatcher.dispatch(event)
            case HMsgEvent():
                self._conn.instrumentation.on_message_delivered(event.subject, event.headers, len(event.payload))
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
            if isinstance(error, PermissionsViolationError):
                # A subscription-permission -ERR additionally terminates the
                # denied subscription(s) when permission_err_on_subscribe is set.
                self._conn.route_permission_error(message)
            return
        if self.running and isinstance(error, AuthorizationViolationError | AuthenticationExpiredError):
            # Parity with nats.go processAuthError: a fatal auth error on a live
            # session reaches the async error handler as well as the disconnect
            # path (unlike stale/other fatal errors, which only disconnect).
            self._conn.background_error(error)
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
        # The default Noop is stored bare: its hooks provably cannot raise, so
        # wrapping it in the guard would only add per-call overhead on the hot
        # read path. User-supplied backends get the guarded wrappers.
        supplied = options.instrumentation
        self.instrumentation: Any = _SafeInstrumentation(supplied) if supplied is not None else NoopInstrumentation()
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
        # Server-advertised publish ceiling, cached as a plain int off the hot
        # path. Defaults to the NATS server default (1 MiB) so pre-connect reads
        # match what Client.max_payload historically returned; refreshed from the
        # handshake INFO and from any async INFO that carries a new max_payload.
        self._max_payload: int = 1024 * 1024
        # Published while _establish runs, so close() can interrupt a handshake
        # that is parked waiting for INFO instead of blocking for connect_timeout.
        self._establishing: _Session | None = None
        self._supervisor: asyncio.Task[None] | None = None
        self._closing = False
        self._closed_event = asyncio.Event()
        self._first_connect: asyncio.Future[None] | None = None
        self._reconnect_buffer: list[bytes] = []
        self._reconnect_buffer_size = 0
        # Dedicated cap for bytes buffered while disconnected (feature: dedicated
        # reconnect buffer). 0 means "use the default"; -1 disables buffering.
        self._reconnect_buf_limit = (
            options.reconnect_buf_size if options.reconnect_buf_size != 0 else _DEFAULT_RECONNECT_BUF_SIZE
        )
        self._reconnect_count = 0
        # Set by force_reconnect(): consumed by the first _backoff to skip it, so
        # a forced reconnect attempts immediately without counting a failure.
        self._force_reconnect = False
        # Wakes an in-progress reconnect backoff sleep on force_reconnect().
        self._force_wake = asyncio.Event()

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
    def max_payload(self) -> int:
        """The server's advertised publish ceiling (cached int)."""
        return self._max_payload

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

    async def force_reconnect(self) -> None:
        """Drop the current transport and reconnect immediately.

        Flushes pending writes best-effort, then tears the session down through
        the normal lost path (subscriptions replay, buffered publishes survive,
        Disconnected+Reconnected fire) but bypassing the backoff for the first
        attempt and without counting a server failure. Non-blocking. Raises
        while the client is closed or draining.
        """
        if self._closing or self._state in (ConnectionState.CLOSED, ConnectionState.DRAINING):
            raise ConnectionClosedError(f"cannot force_reconnect while {self._state.name}")
        # Break any in-progress reconnect backoff and skip the next one.
        self._force_reconnect = True
        self._force_wake.set()
        session = self._session
        if self.is_connected and session is not None and not session.lost_future.done():
            # Flush to the doomed socket first (nats.go bw.flush before Close),
            # then tear down with error=None so the drop counts as deliberate.
            await session.final_flush()
            session.mark_lost(None)
        # Otherwise we are already between sessions (CONNECTING/RECONNECTING):
        # the flag + wake above are enough to hurry the pending attempt along.

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
        # `first_connect_pending` mirrors nats.go initc: it controls Connected vs
        # Reconnected and stays true across retry_on_failed_connect attempts, so
        # the first success still fires Connected. `use_backoff` is decoupled from
        # it: the very first attempt is immediate; every attempt after enters via
        # the backoff (parity with doReconnect vs the initial connect loop).
        first_connect_pending = True
        use_backoff = False
        retrying_initial = False
        last_error: Exception | None = None
        try:
            while not self._closing:
                try:
                    session = await self._establish_any(initial=not use_backoff)
                except _ClosingError:
                    break
                except NoServersAvailableError as exc:
                    if (
                        first_connect_pending
                        and not retrying_initial
                        and self.options.retry_on_failed_connect
                        and self.options.allow_reconnect
                    ):
                        # Initial pool exhausted, but retry_on_failed_connect is
                        # set: go RECONNECTING, let connect() return, and keep
                        # retrying with backoff. The first success fires Connected.
                        retrying_initial = True
                        use_backoff = True
                        last_error = exc
                        self._enter_retry_on_failed_connect()
                        continue
                    last_error = exc
                    break
                except Exception as exc:
                    last_error = exc
                    break
                if self._closing:
                    session.mark_lost(None)
                    break
                self._session = session
                self._state = ConnectionState.CONNECTED
                # Any pending force has been honored by reaching a fresh
                # connect; a residual flag would skip a future real backoff.
                self._force_reconnect = False
                self._force_wake.clear()
                use_backoff = True
                if first_connect_pending:
                    first_connect_pending = False
                    assert self._first_connect is not None
                    if not self._first_connect.done():
                        self._first_connect.set_result(None)
                    # Anything registered or published while still (re)connecting.
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
                # A forced drop reconnects even when allow_reconnect is off (the
                # force flag is consumed by the first _backoff below).
                reconnect_now = self.options.allow_reconnect or self._force_reconnect
                self._carry_over_unsent(session)
                # Move out of CONNECTED *before* notifying: a hook that calls
                # back in must not observe state=CONNECTED with no session.
                self._state = ConnectionState.RECONNECTING if reconnect_now else ConnectionState.DISCONNECTED
                self.instrumentation.on_disconnect(error)
                self._emit(Disconnected(error))
                if not reconnect_now:
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

    def _enter_retry_on_failed_connect(self) -> None:
        """Initial connect exhausted the pool but retry_on_failed_connect is set.

        Move to RECONNECTING and resolve first_connect so connect() returns a
        client that keeps retrying in the background. The first success still
        fires Connected (parity with nats.go initc / ConnectedCB)."""
        self._state = ConnectionState.RECONNECTING
        assert self._first_connect is not None
        if not self._first_connect.done():
            self._first_connect.set_result(None)

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
                except _ConfigClassError as exc:
                    # Permanent config/auth failure: fast-fail rather than let a
                    # later transient error from another server mask the cause.
                    raise exc.error from exc.error.__cause__
                except Exception as exc:
                    if not initial and self._is_repeated_auth_error(server, exc):
                        # Same auth rejection twice from this server during
                        # reconnect: abort the loop so the supervisor finalizes
                        # Closed, regardless of the reconnect budget.
                        raise
                    last_error = exc
                    self._pool.mark_failure(server)
                    log.info("connect to %s failed: %s", server.url, exc)
                    continue
                self._pool.mark_success(server)
                return session
            if initial:
                raise NoServersAvailableError(f"initial connect failed: {last_error}") from last_error

    def _is_repeated_auth_error(self, server: ParsedServer, exc: Exception) -> bool:
        """Whether ``exc`` repeats this server's previous auth rejection.

        Mirrors nats.go processAuthError: the per-server memory (identical type +
        message) lets a revoked credential abort the reconnect loop the second
        time it is seen, instead of hammering the server forever."""
        if not isinstance(exc, AuthorizationViolationError | AuthenticationExpiredError):
            return False
        previous = server.last_auth_error
        server.last_auth_error = exc
        if self.options.ignore_auth_error_abort:
            return False
        return previous is not None and type(previous) is type(exc) and str(previous) == str(exc)

    def _consume_force(self) -> bool:
        """True (and clears the flag) if a force_reconnect is pending."""
        if self._force_reconnect:
            self._force_reconnect = False
            return True
        return False

    async def _backoff(self, server: ParsedServer) -> None:
        # A pending force_reconnect skips the backoff for this attempt entirely.
        if self._consume_force():
            return
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
        self._force_wake.clear()
        closed = asyncio.ensure_future(self._closed_event.wait())
        forced = asyncio.ensure_future(self._force_wake.wait())
        try:
            async with asyncio.timeout(delay):
                await asyncio.wait({closed, forced}, return_when=asyncio.FIRST_COMPLETED)
        except builtins.TimeoutError:
            pass
        finally:
            closed.cancel()
            forced.cancel()
        # Woken early by force_reconnect(): consume so this attempt is immediate.
        self._consume_force()

    async def _establish(self, server: ParsedServer) -> _Session:
        options = self.options
        server.last_attempt = asyncio.get_running_loop().time()
        session = _Session(self, server)
        self._establishing = session
        # WebSocket servers always use the WS transport; the injectable factory
        # (default TCP, or a test double) drives plain nats/tls servers.
        if server.websocket:
            transport: Transport = WSTransport(on_bytes=session.feed, on_close=session.mark_lost, path=server.ws_path)
        else:
            transport = self._transport_factory(on_bytes=session.feed, on_close=session.mark_lost)
        session.transport = transport

        tls_config = options.tls
        wants_tls = server.tls_required or tls_config is not None
        handshake_first = tls_config is not None and tls_config.handshake_first
        tls_hostname = (tls_config.hostname if tls_config else None) or server.host
        # WebSocket wraps TLS around the socket BEFORE the HTTP Upgrade, so wss is
        # always TLS-first and plain ws never upgrades in-band even when INFO says
        # tls_required (parity with nats.go wsInitHandshake, which decides TLS
        # purely from scheme/options and ignores the INFO for the ws path).
        tls_first = wants_tls if server.websocket else handshake_first

        async with asyncio.timeout(options.connect_timeout):
            await transport.connect(
                server.host,
                server.port,
                tls=(tls_config or TLSConfig()).resolve_context() if tls_first else None,
                tls_hostname=tls_hostname if tls_first else None,
            )
            try:
                raw_info = await session.info_future
                info: dict[str, Any] = json.loads(raw_info)
                session.server_info = info
                if info.get("max_payload"):
                    self._max_payload = int(info["max_payload"])
                    session.parser.set_max_payload(self._max_payload)
                # A real server advertises the whole cluster in the very first
                # INFO; async INFO frames only follow on membership changes.
                self._merge_connect_urls(info, server)

                if not server.websocket and (wants_tls or info.get("tls_required")) and not handshake_first:
                    context = (tls_config or TLSConfig()).resolve_context()
                    await transport.upgrade_tls(context, tls_hostname)

                try:
                    auth = await self._authenticate(server, info)
                except (FileNotFoundError, PermissionError, IsADirectoryError, ConfigError) as exc:
                    # Config-class failures are permanent: tag them so
                    # _establish_any fast-fails instead of trying the next server.
                    raise _ConfigClassError(exc) from exc
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
        authenticator: Authenticator | None
        if server.username is not None:
            # URL userinfo wins over option-derived auth (parity with nats.go,
            # whose connectProto reads URL.User ahead of the Options credentials):
            # nats://user:pass@host or nats://token@host.
            if server.password is not None:
                authenticator = UserPasswordAuth(user=server.username, password=server.password)
            else:
                authenticator = TokenAuth(token=server.username)
        else:
            authenticator = self._authenticator
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
        # reconnect_buf_size governs while disconnected (max_pending_size only
        # applies to the live write path). A limit of -1 disables buffering, so a
        # publish while down fails at once; otherwise reject once the buffer has
        # reached the cap (parity with nats.go atLimitIfUsingPending, checked
        # before the frame is appended).
        if self._reconnect_buffer_size >= self._reconnect_buf_limit:
            raise ReconnectBufExceededError("reconnect buffer limit exceeded")
        self._reconnect_buffer.append(frame)
        self._reconnect_buffer_size += len(frame)

    def _carry_over_unsent(self, session: _Session) -> None:
        """Preserve unflushed user publishes across the reconnect (bounded)."""
        if not self.options.allow_reconnect:
            return
        if self._reconnect_buf_limit < 0:
            # Buffering disabled: frames that raced the CONNECTED->lost window
            # landed on the dead session. Losing them is the -1 contract, but
            # losing them SILENTLY is not — report the drop.
            dropped = session.take_unsent_user_frames()
            if dropped:
                self.background_error(
                    ReconnectBufExceededError(f"buffering disabled: dropped {len(dropped)} unflushed publish(es)")
                )
            return
        for position, frame in enumerate(session.take_unsent_user_frames()):
            if self._reconnect_buffer_size + len(frame) > self._reconnect_buf_limit:
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
        if info.get("max_payload"):
            # A membership/config change can carry a new ceiling; keep the cached
            # publish limit and the inbound parser ceiling in step with it.
            self._max_payload = int(info["max_payload"])
            session.parser.set_max_payload(self._max_payload)
        self._merge_connect_urls(info, session.server)
        if info.get("ldm"):
            self._emit(LameDuck(session.server.url))
            if self.options.allow_reconnect and not self._closing:
                # Migrate before the server evicts us — but on the NEXT loop
                # iteration, so messages the server packed into this same
                # segment after the INFO are still delivered. Lame duck is a
                # graceful drain; truncating it mid-segment loses data.
                asyncio.get_running_loop().call_soon(session.mark_lost, None)

    def _merge_connect_urls(self, info: dict[str, Any], current: ParsedServer) -> None:
        urls = info.get("connect_urls")
        if not isinstance(urls, list) or not urls:
            return
        entries = [u for u in urls if isinstance(u, str)]
        if current.websocket:
            # connect_urls are bare host:port. Over a WebSocket connection the
            # server still gossips them bare, so we re-scheme them to match the
            # current connection (ws/wss) — mirroring nats.go connScheme(), which
            # prefixes discovered URLs with the active scheme. This keeps the pool
            # single-scheme and reachable (bare -> nats:// would be unusable here).
            scheme = current.scheme  # "ws" or "wss"
            # Unconditional re-scheme (nats.go connScheme()): even a gossiped
            # url that arrives WITH a scheme is forced onto the connection's —
            # anything else could smuggle a TCP server into a ws-only pool,
            # which the construction-time mixing guard cannot catch here.
            entries = [f"{scheme}://{u.partition('://')[2]}" if "://" in u else f"{scheme}://{u}" for u in entries]
        added = self._pool.merge_discovered(entries, keep_key=current.key)
        if added:
            self._emit(ServersDiscovered(tuple(s.url for s in added)))

    def background_error(self, error: Exception) -> None:
        self.instrumentation.on_error(error)
        if isinstance(error, ServerError) and not error.fatal:
            log.warning("server error: %s", error)
        self._emit(ErrorOccurred(error))

    def route_permission_error(self, message: str) -> None:
        """Terminate subscriptions denied a SUB permission (opt-in).

        Only active under permission_err_on_subscribe. The subject (and optional
        queue) token is extracted the way nats.go processTransientError does; the
        publish-permission variant carries no "Subscription to" token and so
        matches nothing here — it reaches the user only via the error callback."""
        if not self.options.permission_err_on_subscribe:
            return
        match = _PERM_SUBJECT_RE.search(message)
        if match is None:
            return
        subject = match.group(1)
        queue_match = _PERM_QUEUE_RE.search(message)
        queue = queue_match.group(1) if queue_match is not None else None
        self.dispatcher.fail_by_subject(subject, queue, PermissionsViolationError(message))

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
