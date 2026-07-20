"""In-process scripted transport for connection-lifecycle tests.

`FakeEnv` plays the server side: it accepts (or refuses) each successive
transport connect, delivers INFO, answers PING with PONG, and records every
frame the client writes — all on the same event loop, no sockets.
"""

import asyncio
import json
from collections import deque
from collections.abc import Callable
from typing import Any

from natsio._internal.lifecycle import ConnectionEvent

DEFAULT_INFO: dict[str, Any] = {
    "server_id": "FAKE",
    "server_name": "fake",
    "version": "2.14.3",
    "host": "127.0.0.1",
    "port": 4222,
    "headers": True,
    "proto": 1,
    "max_payload": 1048576,
}


class FakeTransport:
    def __init__(self, env: "FakeEnv", *, on_bytes: Callable[[bytes], None], on_close) -> None:
        self.env = env
        self.on_bytes = on_bytes
        self.on_close = on_close
        self.writes: list[bytes] = []
        self.written = bytearray()
        self.closed = False
        self._close_reported = False
        self._writable = asyncio.Event()
        self._writable.set()
        self.reading_paused = False

    # -- Transport protocol --

    async def connect(self, host: str, port: int, *, tls=None, tls_hostname=None) -> None:
        outcome = self.env.next_connect_outcome()
        if outcome is not None:
            raise outcome
        self.env.transports.append(self)
        if self.env.auto_info:
            info = dict(self.env.info)
            asyncio.get_running_loop().call_soon(self.deliver_info, info)

    async def upgrade_tls(self, context, hostname) -> None:  # pragma: no cover - not used in unit tests
        raise AssertionError("TLS upgrade not scripted in FakeTransport")

    def write(self, data: bytes) -> None:
        if self.closed:
            raise ConnectionError("write to closed fake transport")
        self.writes.append(bytes(data))
        self.written += data
        self.env.on_client_write(self, bytes(data))

    async def wait_writable(self) -> None:
        await self._writable.wait()

    def pause_reading(self) -> None:
        self.reading_paused = True

    def resume_reading(self) -> None:
        self.reading_paused = False

    @property
    def is_closing(self) -> bool:
        return self.closed

    def close(self) -> None:
        self._finish(None)

    def abort(self) -> None:
        self._finish(None)

    # -- server-side controls --

    def _finish(self, exc: Exception | None) -> None:
        if self.closed:
            return
        self.closed = True
        if not self._close_reported:
            self._close_reported = True
            asyncio.get_running_loop().call_soon(self.on_close, exc)

    def deliver(self, data: bytes) -> None:
        """Bytes from 'server' to client."""
        if not self.closed:
            self.on_bytes(data)

    def deliver_info(self, info: dict[str, Any]) -> None:
        self.deliver(b"INFO " + json.dumps(info).encode() + b"\r\n")

    def drop(self, exc: Exception | None = None) -> None:
        """Server-side connection loss."""
        self._finish(exc)

    def block_writes(self) -> None:
        self._writable.clear()

    def unblock_writes(self) -> None:
        self._writable.set()


class FakeEnv:
    """Factory + scripted behavior across successive connection attempts."""

    def __init__(self) -> None:
        self.transports: list[FakeTransport] = []
        self.attempts = 0
        self.connect_outcomes: deque[Exception | None] = deque()
        self.info: dict[str, Any] = dict(DEFAULT_INFO)
        self.auto_info = True
        self.auto_pong = True
        # Overridable per-test to script custom reactions to client writes.
        self.on_client_write: Callable[[FakeTransport, bytes], None] = self._default_on_client_write

    def factory(self, *, on_bytes, on_close) -> FakeTransport:
        self.attempts += 1
        return FakeTransport(self, on_bytes=on_bytes, on_close=on_close)

    def next_connect_outcome(self) -> Exception | None:
        if self.connect_outcomes:
            return self.connect_outcomes.popleft()
        return None

    def refuse_next(self, count: int = 1) -> None:
        for _ in range(count):
            self.connect_outcomes.append(ConnectionRefusedError("refused"))

    def _default_on_client_write(self, transport: FakeTransport, data: bytes) -> None:
        if self.auto_pong and b"PING\r\n" in data:
            for _ in range(data.count(b"PING\r\n")):
                asyncio.get_running_loop().call_soon(transport.deliver, b"PONG\r\n")

    @property
    def current(self) -> FakeTransport:
        assert self.transports, "no transport connected yet"
        return self.transports[-1]


class EventRecorder:
    """Bus subscriber that records events and lets tests await specific types."""

    def __init__(self) -> None:
        self.events: list[ConnectionEvent] = []
        self._waiters: list[tuple[type, asyncio.Future[ConnectionEvent]]] = []

    def hook(self, event: ConnectionEvent) -> None:
        self.events.append(event)
        for pair in list(self._waiters):
            event_type, future = pair
            if isinstance(event, event_type) and not future.done():
                future.set_result(event)
                self._waiters.remove(pair)

    async def wait_for(self, event_type: type, timeout: float = 2.0) -> ConnectionEvent:  # noqa: ASYNC109
        for event in self.events:
            if isinstance(event, event_type):
                return event
        future: asyncio.Future[ConnectionEvent] = asyncio.get_running_loop().create_future()
        self._waiters.append((event_type, future))
        async with asyncio.timeout(timeout):
            return await future

    def count(self, event_type: type) -> int:
        return sum(1 for e in self.events if isinstance(e, event_type))


def frames_written(transport: FakeTransport) -> bytes:
    return bytes(transport.written)


def connect_payload(transport: FakeTransport) -> dict[str, Any]:
    """Extract and decode the CONNECT payload the client sent on this transport."""
    data = frames_written(transport)
    start = data.index(b"CONNECT ") + len(b"CONNECT ")
    end = data.index(b"\r\n", start)
    return json.loads(data[start:end])
