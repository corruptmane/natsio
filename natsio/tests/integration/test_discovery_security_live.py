"""Localhost wire regressions for discovery trust and TLS failover."""

import asyncio
import base64
import hashlib
import json
import ssl
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import pytest

from fake import EventRecorder
from natsio._internal.connection import Connection
from natsio._internal.lifecycle import Closed, Reconnected, ServersDiscovered
from natsio.options import ConnectOptions
from server import generate_self_signed_cert, openssl_available

_INFO = {"server_id": "DISCOVERY", "version": "2.14.3", "proto": 1, "headers": True, "max_payload": 1048576}
_TOKEN = "LOCALHOST_DISCOVERY_DUMMY_TOKEN"
type Handler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]


def info_frame(**values) -> bytes:
    return b"INFO " + json.dumps(_INFO | values).encode() + b"\r\n"


@asynccontextmanager
async def listener(
    handler: Handler, *, tls: ssl.SSLContext | None = None
) -> AsyncIterator[tuple[str, set[asyncio.StreamWriter]]]:
    writers: set[asyncio.StreamWriter] = set()
    tasks: set[asyncio.Task] = set()

    async def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        tasks.add(task)
        writers.add(writer)
        try:
            await handler(reader, writer)
        except (ConnectionError, OSError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            with suppress(ConnectionError, OSError):
                await writer.wait_closed()
            writers.discard(writer)
            tasks.discard(task)

    server = await asyncio.start_server(accept, "127.0.0.1", 0, ssl=tls)
    address = f"127.0.0.1:{server.sockets[0].getsockname()[1]}"
    try:
        yield address, writers
    finally:
        server.close()
        await server.wait_closed()
        for writer in tuple(writers):
            writer.close()
        for task in tuple(tasks):
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


def options(url: str, **overrides) -> ConnectOptions:
    values: dict = {
        "servers": (url,),
        "token": _TOKEN,
        "retry_on_failed_connect": True,
        "max_reconnect_attempts": 1,
        "no_randomize": True,
        "reconnect_time_wait": 0.01,
        "reconnect_time_wait_max": 0.02,
        "reconnect_jitter": 0,
        "reconnect_jitter_tls": 0,
        "connect_timeout": 1.0,
    }
    values.update(overrides)
    return ConnectOptions(**values)


@pytest.fixture
def tls_contexts(tmp_path: Path) -> tuple[ssl.SSLContext, ssl.SSLContext]:
    if not openssl_available():
        pytest.skip("openssl CLI not available")
    cert, key = generate_self_signed_cert(tmp_path)
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(cert, key)
    return server, ssl.create_default_context(cafile=str(cert))


async def answer_pings(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    captures: list[dict],
    *,
    connect_urls: list[str] | None = None,
) -> None:
    while line := await reader.readline():
        if line.startswith(b"CONNECT "):
            captures.append(json.loads(line[8:]))
        elif line == b"PING\r\n":
            writer.write(b"PONG\r\n")
            if connect_urls is not None:
                writer.write(info_frame(connect_urls=connect_urls))
                connect_urls = None
            await writer.drain()


@pytest.mark.parametrize("later_info", [False, True])
async def test_unverified_tls_seed_cannot_send_token_to_plaintext_discovery(later_info: bool) -> None:
    stolen = asyncio.Event()
    captures: list[dict] = []

    async def attacker(reader, writer) -> None:
        writer.write(info_frame())
        await writer.drain()
        line = await reader.readline()
        if line.startswith(b"CONNECT "):
            captures.append(json.loads(line[8:]))
            stolen.set()
        await answer_pings(reader, writer, captures)

    async with listener(attacker) as (peer, _):

        async def seed(reader, writer) -> None:
            first = info_frame() if later_info else info_frame(connect_urls=[peer])
            second = info_frame(connect_urls=[peer]) if later_info else b""
            writer.write(first + second)
            await writer.drain()
            # EOF before TLS authenticates this peer.

        async with listener(seed) as (seed_address, _):
            conn = Connection(options(f"tls://{seed_address}"))
            recorder = EventRecorder()
            conn.bus.subscribe(recorder.hook)
            try:
                await conn.connect()
                closed = asyncio.create_task(recorder.wait_for(Closed))
                leaked = asyncio.create_task(stolen.wait())
                try:
                    async with asyncio.timeout(3):
                        await asyncio.wait({closed, leaked}, return_when=asyncio.FIRST_COMPLETED)
                    assert not captures, "credentials escaped over the discovered plaintext socket"
                    assert recorder.count(ServersDiscovered) == 0
                    assert len(conn._pool.servers) == 1
                    await closed
                finally:
                    for task in (closed, leaked):
                        task.cancel()
                    await asyncio.gather(closed, leaked, return_exceptions=True)
            finally:
                await conn.close(flush=False)


async def test_untrusted_certificate_cannot_commit_discovery(tls_contexts) -> None:
    server_context, _ = tls_contexts
    captures: list[dict] = []

    async def seed(reader, writer) -> None:
        writer.write(info_frame(connect_urls=["127.0.0.1:1"]))
        await writer.drain()
        await writer.start_tls(server_context)
        await answer_pings(reader, writer, captures)

    async with listener(seed) as (address, _):
        conn = Connection(options(f"tls://{address}"))
        recorder = EventRecorder()
        conn.bus.subscribe(recorder.hook)
        try:
            await conn.connect()
            await recorder.wait_for(Closed)
            assert not captures
            assert len(conn._pool.servers) == 1
            assert recorder.count(ServersDiscovered) == 0
        finally:
            await conn.close(flush=False)


@pytest.mark.parametrize("source_scheme", ["tls", "nats"])
async def test_successful_tls_discovery_keeps_tls_on_reconnect(tls_contexts, monkeypatch, source_scheme: str) -> None:
    server_context, client_context = tls_contexts
    # Use scheme/INFO TLS requirements with ordinary certificate validation,
    # without global TLSConfig hiding a discovery downgrade.
    monkeypatch.setattr(ssl, "create_default_context", lambda: client_context)
    captures: list[dict] = []
    secure_peers: list[bool] = []
    seed_done = False

    async def peer(reader, writer) -> None:
        writer.write(info_frame(tls_required=True))
        await writer.drain()
        await writer.start_tls(server_context)
        secure_peers.append(writer.get_extra_info("ssl_object") is not None)
        await answer_pings(reader, writer, captures)

    async with listener(peer) as (peer_address, _):

        async def seed(reader, writer) -> None:
            if seed_done:
                return
            writer.write(info_frame(tls_required=True, connect_urls=[peer_address]))
            await writer.drain()
            await writer.start_tls(server_context)
            await answer_pings(reader, writer, [], connect_urls=[peer_address])

        async with listener(seed) as (seed_address, seed_writers):
            conn = Connection(options(f"{source_scheme}://{seed_address}"))
            recorder = EventRecorder()
            conn.bus.subscribe(recorder.hook)
            try:
                await conn.connect()
                await recorder.wait_for(ServersDiscovered)
                discovered = [server for server in conn._pool.servers if server.discovered]
                assert len(discovered) == 1
                assert discovered[0].tls_required
                assert discovered[0].scheme == "tls"
                seed_done = True
                for writer in tuple(seed_writers):
                    writer.close()
                await recorder.wait_for(Reconnected)
                await conn.flush()
                assert secure_peers == [True]
                assert len(captures) == 1
                assert captures[0]["auth_token"] == _TOKEN
            finally:
                await conn.close(flush=False)


@pytest.mark.parametrize("late_requirement", [False, True])
async def test_pre_tls_topology_is_discarded_even_after_successful_handshake(
    tls_contexts, monkeypatch, late_requirement
):
    server_context, client_context = tls_contexts
    monkeypatch.setattr(ssl, "create_default_context", lambda: client_context)
    auth_entered = asyncio.Event()
    resolve_auth = asyncio.Event()
    refresh = asyncio.Event()
    handle_info = Connection.handle_async_info

    def observe_info(self, session, raw):
        handle_info(self, session, raw)
        if session.server_info.get("tls_required"):
            resolve_auth.set()

    monkeypatch.setattr(Connection, "handle_async_info", observe_info)

    async def token():
        auth_entered.set()
        await resolve_auth.wait()
        return _TOKEN

    async def seed(reader, writer):
        writer.write(info_frame(connect_urls=["127.0.0.1:1"]))
        await writer.drain()
        if late_requirement:
            await auth_entered.wait()
            writer.write(info_frame(tls_required=True, connect_urls=["127.0.0.1:2"]))
            await writer.drain()
        await writer.start_tls(server_context)
        while line := await reader.readline():
            if line == b"PING\r\n":
                writer.write(b"PONG\r\n")
                await writer.drain()
                await refresh.wait()
                writer.write(info_frame(connect_urls=["127.0.0.1:3"]))
                await writer.drain()

    if not late_requirement:
        resolve_auth.set()
    async with listener(seed) as (address, _):
        scheme = "nats" if late_requirement else "tls"
        conn = Connection(options(f"{scheme}://{address}", token=token))
        recorder = EventRecorder()
        conn.bus.subscribe(recorder.hook)
        try:
            async with asyncio.timeout(5):
                await conn.connect()
                assert len(conn._pool.servers) == 1
                assert recorder.count(ServersDiscovered) == 0
                refresh.set()
                await recorder.wait_for(ServersDiscovered)
                discovered = [server for server in conn._pool.servers if server.discovered]
                assert [server.port for server in discovered] == [3]
                assert all(server.tls_required for server in discovered)
        finally:
            resolve_auth.set()
            refresh.set()
            await conn.close(flush=False)


async def test_failed_tls_reconnect_cannot_poison_verified_pool(tls_contexts, monkeypatch) -> None:
    server_context, client_context = tls_contexts
    monkeypatch.setattr(ssl, "create_default_context", lambda: client_context)
    reconnect = False

    async def seed(reader, writer) -> None:
        if reconnect:
            writer.write(info_frame(connect_urls=["127.0.0.1:1"]))
            await writer.drain()
            return
        writer.write(info_frame(tls_required=True))
        await writer.drain()
        await writer.start_tls(server_context)
        await answer_pings(reader, writer, [])

    async with listener(seed) as (address, writers):
        conn = Connection(options(f"tls://{address}"))
        recorder = EventRecorder()
        conn.bus.subscribe(recorder.hook)
        try:
            await conn.connect()
            reconnect = True
            for writer in tuple(writers):
                writer.close()
            await recorder.wait_for(Closed)
            assert len(conn._pool.servers) == 1
            assert recorder.count(ServersDiscovered) == 0
        finally:
            await conn.close(flush=False)


@pytest.mark.parametrize("later_info", [False, True])
async def test_tls_auth_rejection_cannot_commit_any_discovery(tls_contexts, monkeypatch, later_info: bool) -> None:
    server_context, client_context = tls_contexts
    monkeypatch.setattr(ssl, "create_default_context", lambda: client_context)

    async def seed(reader, writer) -> None:
        writer.write(info_frame() if later_info else info_frame(connect_urls=["127.0.0.1:1"]))
        await writer.drain()
        await writer.start_tls(server_context)
        assert (await reader.readline()).startswith(b"CONNECT ")
        if later_info:
            writer.write(info_frame(connect_urls=["127.0.0.1:1"]))
        writer.write(b"-ERR 'Authorization Violation'\r\n")
        await writer.drain()

    async with listener(seed) as (address, _):
        conn = Connection(options(f"tls://{address}"))
        recorder = EventRecorder()
        conn.bus.subscribe(recorder.hook)
        try:
            await conn.connect()
            await recorder.wait_for(Closed)
            assert len(conn._pool.servers) == 1
            assert recorder.count(ServersDiscovered) == 0
        finally:
            await conn.close(flush=False)


async def test_explicit_plaintext_failover_remains_opted_in_for_mixed_pool() -> None:
    captures: list[dict] = []

    async def explicit_plain(reader, writer) -> None:
        writer.write(info_frame())
        await writer.drain()
        await answer_pings(reader, writer, captures)

    async def broken_tls(reader, writer) -> None:
        writer.write(info_frame(connect_urls=["127.0.0.1:1"]))
        await writer.drain()

    async with listener(explicit_plain) as (plain, _), listener(broken_tls) as (secure, _):
        urls = (f"tls://{secure}", f"nats://{plain}")
        conn = Connection(options(urls[0], servers=urls))
        try:
            await conn.connect()
            await conn.flush()
            assert conn.connected_url == urls[1]
            assert len(conn._pool.servers) == 2
            assert captures[0]["auth_token"] == _TOKEN
        finally:
            await conn.close(flush=False)


def websocket_frame(payload: bytes) -> bytes:
    if len(payload) < 126:
        return bytes((0x82, len(payload))) + payload
    return b"\x82\x7e" + len(payload).to_bytes(2, "big") + payload


async def websocket_upgrade(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    request = await reader.readuntil(b"\r\n\r\n")
    headers = dict(line.split(b":", 1) for line in request.split(b"\r\n")[1:] if b":" in line)
    key = headers[b"Sec-WebSocket-Key"].strip()
    accept = base64.b64encode(hashlib.sha1(key + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest())
    writer.write(
        b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "
        + accept
        + b"\r\n\r\n"
    )
    await writer.drain()


async def websocket_answer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, captures: list[dict]) -> None:
    while True:
        header = await reader.readexactly(2)
        if header[0] & 0xF == 8:
            return
        size = header[1] & 0x7F
        if size == 126:
            size = int.from_bytes(await reader.readexactly(2), "big")
        elif size == 127:
            size = int.from_bytes(await reader.readexactly(8), "big")
        mask = await reader.readexactly(4)
        payload = await reader.readexactly(size)
        data = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
        for line in data.split(b"\r\n"):
            if line.startswith(b"CONNECT "):
                captures.append(json.loads(line[8:]))
            elif line == b"PING":
                writer.write(websocket_frame(b"PONG\r\n"))
                await writer.drain()


@pytest.mark.parametrize("scheme", ["ws", "wss"])
async def test_websocket_discovery_preserves_transport_and_security_on_failover(
    tls_contexts, monkeypatch, scheme: str
) -> None:
    server_context, client_context = tls_contexts
    monkeypatch.setattr(ssl, "create_default_context", lambda: client_context)
    tls = server_context if scheme == "wss" else None
    captures: list[dict] = []
    secure_connections: list[bool] = []
    seed_done = False

    async def peer(reader, writer) -> None:
        await websocket_upgrade(reader, writer)
        secure_connections.append(writer.get_extra_info("ssl_object") is not None)
        writer.write(websocket_frame(info_frame()))
        await writer.drain()
        await websocket_answer(reader, writer, captures)

    async with listener(peer, tls=tls) as (peer_address, _):

        async def seed(reader, writer) -> None:
            if seed_done:
                return
            await websocket_upgrade(reader, writer)
            writer.write(websocket_frame(info_frame(connect_urls=[f"nats://{peer_address}"])))
            await writer.drain()
            await websocket_answer(reader, writer, [])

        async with listener(seed, tls=tls) as (seed_address, writers):
            conn = Connection(options(f"{scheme}://{seed_address}"))
            recorder = EventRecorder()
            conn.bus.subscribe(recorder.hook)
            try:
                await conn.connect()
                discovered = [server for server in conn._pool.servers if server.discovered]
                assert len(discovered) == 1
                assert discovered[0].scheme == scheme
                assert discovered[0].websocket
                seed_done = True
                for writer in tuple(writers):
                    writer.close()
                await recorder.wait_for(Reconnected)
                await conn.flush()
                assert secure_connections == [scheme == "wss"]
                assert len(captures) == 1
                assert captures[0]["auth_token"] == _TOKEN
            finally:
                await conn.close(flush=False)


async def test_verified_info_tls_requirement_cannot_disappear_on_reconnect(tls_contexts, monkeypatch) -> None:
    server_context, client_context = tls_contexts
    monkeypatch.setattr(ssl, "create_default_context", lambda: client_context)
    reconnect = False
    observed = asyncio.Event()
    reconnect_bytes: list[bytes] = []

    async def seed(reader, writer) -> None:
        if reconnect:
            writer.write(info_frame())
            await writer.drain()
            reconnect_bytes.append(await reader.read(4096))
            observed.set()
            return
        writer.write(info_frame(tls_required=True))
        await writer.drain()
        await writer.start_tls(server_context)
        await answer_pings(reader, writer, [])

    async with listener(seed) as (address, writers):
        # The initial INFO upgrades this explicit nats:// endpoint to TLS.
        # That verified endpoint may not silently downgrade on a later attempt.
        conn = Connection(options(f"nats://{address}"))
        recorder = EventRecorder()
        conn.bus.subscribe(recorder.hook)
        try:
            await conn.connect()
            reconnect = True
            for writer in tuple(writers):
                writer.close()
            async with asyncio.timeout(3):
                await observed.wait()
            assert b"CONNECT " not in reconnect_bytes[0]
            assert _TOKEN.encode() not in reconnect_bytes[0]
            await recorder.wait_for(Closed)
            assert conn._pool.servers[0].tls_required
        finally:
            await conn.close(flush=False)


async def test_late_tls_info_during_auth_cannot_emit_plaintext_credentials() -> None:
    from natsio.auth import AuthResult
    from natsio.instrumentation import NoopInstrumentation

    entered = asyncio.Event()
    late_info_received = asyncio.Event()
    observed = asyncio.Event()
    first = info_frame()
    second = info_frame(tls_required=True)
    received: list[bytes] = []

    class ObserveIncoming(NoopInstrumentation):
        def __init__(self) -> None:
            self.total = 0

        def on_bytes_received(self, count: int) -> None:
            self.total += count
            if self.total >= len(first) + len(second):
                late_info_received.set()

    class DelayedAuth:
        async def authenticate(self, nonce) -> AuthResult:
            entered.set()
            await late_info_received.wait()
            return AuthResult(auth_token=_TOKEN)

    async def seed(reader, writer) -> None:
        writer.write(first)
        await writer.drain()
        await entered.wait()
        writer.write(second)
        await writer.drain()
        received.append(await reader.read(4096))
        observed.set()

    async with listener(seed) as (address, _):
        conn = Connection(
            options(f"nats://{address}", token=None, authenticator=DelayedAuth(), instrumentation=ObserveIncoming())
        )
        try:
            await conn.connect()
            async with asyncio.timeout(3):
                await observed.wait()
            assert b"CONNECT " not in received[0]
            assert _TOKEN.encode() not in received[0]
        finally:
            await conn.close(flush=False)
