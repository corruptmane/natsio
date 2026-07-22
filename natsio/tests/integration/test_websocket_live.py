"""End-to-end WebSocket transport tests against a real nats-server 2.14.

The server is started with a ``websocket { ... }`` config block on its own free
port; the client connects over ``ws://`` (and ``wss://`` when TLS wiring is
cheap). Covers the full stack: handshake, pub/sub, request/reply, >64 KiB frames
(forcing the 64-bit length form), reconnect across a restart, a JetStream smoke
test, and a clean close.
"""

import asyncio
import ssl

import pytest

import natsio
from natsio._internal.connection import Connection
from natsio._internal.lifecycle import ConnectionState, Reconnected
from natsio._internal.protocol import HMsgEvent, MsgEvent, encode_pub
from natsio.jetstream import ConsumerConfig, StorageType, StreamConfig
from natsio.options import ConnectOptions, TLSConfig
from server import (
    NatsServerProcess,
    free_port,
    generate_self_signed_cert,
    openssl_available,
    require_server_binary,
)


def ws_url(host: str, port: int, *, tls: bool = False) -> str:
    return f"{'wss' if tls else 'ws'}://{host}:{port}"


def ws_options(url: str, **overrides) -> ConnectOptions:
    defaults: dict = {
        "servers": (url,),
        "connect_timeout": 5.0,
        "reconnect_time_wait": 0.05,
        "reconnect_time_wait_max": 0.2,
        "reconnect_jitter": 0.01,
        "reconnect_jitter_tls": 0.01,
        "max_reconnect_attempts": 60,
    }
    defaults.update(overrides)
    return ConnectOptions(**defaults)


@pytest.fixture
async def ws_server():
    """A nats-server whose websocket listener runs (plaintext) on its own port."""
    binary = require_server_binary()
    wsport = free_port()
    config = f'websocket {{ host: "127.0.0.1", port: {wsport}, no_tls: true }}\n'
    process = NatsServerProcess(binary, config=config)
    await process.start()
    process.ws_port = wsport
    yield process
    await process.stop()


class TestWebSocketHandshake:
    async def test_connect_info_and_close(self, ws_server: NatsServerProcess) -> None:
        url = ws_url("127.0.0.1", ws_server.ws_port)
        conn = Connection(ws_options(url))
        await conn.connect()
        try:
            assert conn.state is ConnectionState.CONNECTED
            major, minor = (int(p) for p in conn.server_info["version"].split(".", 2)[:2])
            assert (major, minor) >= (2, 14)  # natsio's supported server floor
            assert conn.connected_url == url
            await conn.flush()
        finally:
            await conn.close()
        assert conn.state is ConnectionState.CLOSED

    async def test_pub_sub_roundtrip(self, ws_server: NatsServerProcess) -> None:
        url = ws_url("127.0.0.1", ws_server.ws_port)
        conn = Connection(ws_options(url))
        await conn.connect()
        received: asyncio.Queue[MsgEvent | HMsgEvent] = asyncio.Queue()
        try:
            conn.subscribe("ws.>", None, received.put_nowait)
            await conn.flush()
            await conn.publish_frame(encode_pub("ws.test", None, b"hello over websocket"))
            event = await asyncio.wait_for(received.get(), timeout=5)
            assert event.subject == "ws.test"
            assert event.payload == b"hello over websocket"
        finally:
            await conn.close()

    async def test_request_reply(self, ws_server: NatsServerProcess) -> None:
        url = ws_url("127.0.0.1", ws_server.ws_port)
        nc = await natsio.connect(url, connect_timeout=5.0, request_timeout=5.0)

        async def responder(msg) -> None:
            await msg.respond(b"pong:" + msg.payload)

        try:
            nc.subscribe("ws.echo", cb=responder)
            await nc.flush()
            reply = await nc.request("ws.echo", b"ping", timeout=5)
            assert reply.payload == b"pong:ping"
        finally:
            await nc.close()

    async def test_large_payload_forces_64bit_frame(self, ws_server: NatsServerProcess) -> None:
        url = ws_url("127.0.0.1", ws_server.ws_port)
        conn = Connection(ws_options(url))
        await conn.connect()
        received: asyncio.Queue[MsgEvent | HMsgEvent] = asyncio.Queue()
        # >64 KiB payload -> the outbound WS frame uses the 64-bit length form and
        # the inbound MSG spans multiple TCP reads / possibly multiple WS frames.
        payload = bytes(i % 251 for i in range(200_000))
        try:
            conn.subscribe("ws.big", None, received.put_nowait)
            await conn.flush()
            await conn.publish_frame(encode_pub("ws.big", None, payload))
            event = await asyncio.wait_for(received.get(), timeout=10)
            assert event.payload == payload
        finally:
            await conn.close()

    async def test_clean_close_leaves_no_tasks(self, ws_server: NatsServerProcess) -> None:
        url = ws_url("127.0.0.1", ws_server.ws_port)
        conn = Connection(ws_options(url))
        await conn.connect()
        await conn.flush()
        await conn.close()
        assert conn.state is ConnectionState.CLOSED
        # A second close is a no-op, never an error.
        await conn.close()


class TestWebSocketReconnect:
    async def test_server_restart_reconnects_and_replays(self) -> None:
        binary = require_server_binary()
        port = free_port()
        wsport = free_port()
        config = f'websocket {{ host: "127.0.0.1", port: {wsport}, no_tls: true }}\n'
        process = await NatsServerProcess(binary, port=port, config=config).start()
        url = ws_url("127.0.0.1", wsport)
        conn = Connection(ws_options(url))
        reconnected = asyncio.Event()
        conn.bus.subscribe(lambda e: reconnected.set() if isinstance(e, Reconnected) else None)
        received: asyncio.Queue[MsgEvent | HMsgEvent] = asyncio.Queue()
        try:
            await conn.connect()
            conn.subscribe("ws.resub.>", None, received.put_nowait)
            await conn.flush()

            process.kill()
            process = await NatsServerProcess(binary, port=port, config=config).start()

            async with asyncio.timeout(15):
                await reconnected.wait()

            await conn.flush()
            await conn.publish_frame(encode_pub("ws.resub.after", None, b"survived"))
            event = await asyncio.wait_for(received.get(), timeout=5)
            assert event.payload == b"survived"
        finally:
            await conn.close()
            await process.stop()


class TestWebSocketJetStream:
    async def test_publish_and_fetch_over_ws(self) -> None:
        binary = require_server_binary()
        wsport = free_port()
        config = f'websocket {{ host: "127.0.0.1", port: {wsport}, no_tls: true }}\n'
        process = NatsServerProcess(binary, jetstream=True, config=config)
        await process.start()
        url = ws_url("127.0.0.1", wsport)
        nc = await natsio.connect(url, connect_timeout=5.0, request_timeout=5.0)
        try:
            js = nc.jetstream()
            stream = await js.create_stream(StreamConfig(name="WSJS", subjects=["wsjs.>"], storage=StorageType.MEMORY))
            for i in range(5):
                await js.publish(f"wsjs.{i}", b"payload-%d" % i)
            consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))
            batch = await consumer.fetch(5, timeout=3)
            assert [m.payload for m in batch] == [b"payload-%d" % i for i in range(5)]
            for msg in batch:
                await msg.ack()
        finally:
            await nc.close()
            await process.stop()


class TestWebSocketTLS:
    async def test_wss_connect_and_roundtrip(self, tmp_path) -> None:
        if not openssl_available():
            pytest.skip("openssl CLI not available")
        binary = require_server_binary()
        cert, key = generate_self_signed_cert(tmp_path)
        wsport = free_port()
        config = (
            f"websocket {{\n"
            f'  host: "127.0.0.1"\n'
            f"  port: {wsport}\n"
            f'  tls {{ cert_file: "{cert}", key_file: "{key}" }}\n'
            f"}}\n"
        )
        process = await NatsServerProcess(binary, config=config).start()
        url = ws_url("127.0.0.1", wsport, tls=True)
        context = ssl.create_default_context(cafile=str(cert))
        conn = Connection(ws_options(url, tls=TLSConfig(context=context, hostname="localhost")))
        received: asyncio.Queue[MsgEvent | HMsgEvent] = asyncio.Queue()
        try:
            await conn.connect()
            assert conn.state is ConnectionState.CONNECTED
            conn.subscribe("wss.>", None, received.put_nowait)
            await conn.flush()
            await conn.publish_frame(encode_pub("wss.test", None, b"secure hello"))
            event = await asyncio.wait_for(received.get(), timeout=5)
            assert event.payload == b"secure hello"
        finally:
            await conn.close()
            await process.stop()
