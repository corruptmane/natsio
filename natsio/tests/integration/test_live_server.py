"""End-to-end smoke tests against a real nats-server 2.14 binary."""

import asyncio
import ssl

import pytest

from natsio._internal.auth import NKeyAuth, nkeys
from natsio._internal.connection import Connection
from natsio._internal.lifecycle import ConnectionState, Reconnected
from natsio._internal.protocol import HMsgEvent, MsgEvent, encode_pub
from natsio.errors import AuthorizationViolationError, NoServersAvailableError
from natsio.options import ConnectOptions, TLSConfig
from server import (
    NatsServerProcess,
    free_port,
    generate_self_signed_cert,
    openssl_available,
    require_server_binary,
)


def options_for(server: NatsServerProcess, **overrides) -> ConnectOptions:
    defaults: dict = {
        "servers": (server.url,),
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
async def server():
    binary = require_server_binary()
    process = NatsServerProcess(binary)
    await process.start()
    yield process
    await process.stop()


class TestHandshake:
    async def test_connect_flush_close(self, server: NatsServerProcess) -> None:
        conn = Connection(options_for(server))
        await conn.connect()
        try:
            assert conn.state is ConnectionState.CONNECTED
            major, minor = (int(p) for p in conn.server_info["version"].split(".", 2)[:2])
            assert (major, minor) >= (2, 14)  # natsio's supported server floor
            assert conn.server_info["headers"] is True
            await conn.flush()
        finally:
            await conn.close()
        assert conn.state is ConnectionState.CLOSED

    async def test_pub_sub_roundtrip(self, server: NatsServerProcess) -> None:
        conn = Connection(options_for(server))
        await conn.connect()
        received: asyncio.Queue[MsgEvent | HMsgEvent] = asyncio.Queue()
        try:
            conn.subscribe("smoke.>", None, received.put_nowait)
            await conn.flush()
            await conn.publish_frame(encode_pub("smoke.test", None, b"hello real server"))
            event = await asyncio.wait_for(received.get(), timeout=5)
            assert event.subject == "smoke.test"
            assert event.payload == b"hello real server"
        finally:
            await conn.close()

    async def test_echo_and_many_messages(self, server: NatsServerProcess) -> None:
        conn = Connection(options_for(server))
        await conn.connect()
        received: list[bytes] = []
        done = asyncio.Event()

        def handler(event: MsgEvent | HMsgEvent) -> None:
            received.append(event.payload)
            if len(received) == 500:
                done.set()

        try:
            conn.subscribe("burst", None, handler)
            await conn.flush()
            for i in range(500):
                await conn.publish_frame(encode_pub("burst", None, b"%d" % i))
            await asyncio.wait_for(done.wait(), timeout=10)
            assert received == [b"%d" % i for i in range(500)]
        finally:
            await conn.close()


class TestAuth:
    async def test_userpass_rejected_then_accepted(self) -> None:
        binary = require_server_binary()
        config = 'authorization { user: "alice", password: "wonder" }\n'
        process = await NatsServerProcess(binary, config=config).start()
        try:
            bad = Connection(options_for(process, allow_reconnect=False))
            with pytest.raises(NoServersAvailableError) as excinfo:
                await bad.connect()
            assert isinstance(excinfo.value.__cause__, AuthorizationViolationError)

            good = Connection(options_for(process, user="alice", password="wonder"))
            await good.connect()
            try:
                await good.flush()
            finally:
                await good.close()
        finally:
            await process.stop()

    async def test_nkey_auth_end_to_end(self) -> None:
        """The server verifies a nonce signature produced through our signer seam."""
        binary = require_server_binary()
        seed = nkeys.encode_seed(nkeys.Role.USER, bytes(range(32)))
        public = nkeys.from_seed(seed).public_key
        config = f'authorization {{ users = [ {{ nkey: "{public}" }} ] }}\n'
        process = await NatsServerProcess(binary, config=config).start()
        try:
            conn = Connection(options_for(process, authenticator=NKeyAuth(seed=seed)))
            await conn.connect()
            try:
                await conn.flush()
                assert conn.state is ConnectionState.CONNECTED
            finally:
                await conn.close()
        finally:
            await process.stop()


class TestReconnect:
    async def test_server_restart_reconnects_and_replays(self) -> None:
        binary = require_server_binary()
        port = free_port()
        process = await NatsServerProcess(binary, port=port).start()
        conn = Connection(options_for(process))
        reconnected = asyncio.Event()
        conn.bus.subscribe(lambda e: reconnected.set() if isinstance(e, Reconnected) else None)
        received: asyncio.Queue[MsgEvent | HMsgEvent] = asyncio.Queue()
        try:
            await conn.connect()
            conn.subscribe("resub.>", None, received.put_nowait)
            await conn.flush()

            process.kill()
            process = await NatsServerProcess(binary, port=port).start()

            async with asyncio.timeout(10):
                await reconnected.wait()

            await conn.flush()
            await conn.publish_frame(encode_pub("resub.after", None, b"survived"))
            event = await asyncio.wait_for(received.get(), timeout=5)
            assert event.payload == b"survived"
        finally:
            await conn.close()
            await process.stop()


class TestTLS:
    async def test_tls_upgrade(self, tmp_path) -> None:
        if not openssl_available():
            pytest.skip("openssl CLI not available")
        binary = require_server_binary()
        cert, key = generate_self_signed_cert(tmp_path)
        config = f'tls {{ cert_file: "{cert}", key_file: "{key}" }}\n'
        process = await NatsServerProcess(binary, config=config).start()
        try:
            context = ssl.create_default_context(cafile=str(cert))
            conn = Connection(options_for(process, tls=TLSConfig(context=context, hostname="localhost")))
            await conn.connect()
            try:
                await conn.flush()
                assert conn.state is ConnectionState.CONNECTED
            finally:
                await conn.close()
        finally:
            await process.stop()

    async def test_tls_handshake_first(self, tmp_path) -> None:
        if not openssl_available():
            pytest.skip("openssl CLI not available")
        binary = require_server_binary()
        cert, key = generate_self_signed_cert(tmp_path)
        config = f'tls {{ cert_file: "{cert}", key_file: "{key}", handshake_first: true }}\n'
        process = await NatsServerProcess(binary, config=config).start()
        try:
            context = ssl.create_default_context(cafile=str(cert))
            conn = Connection(
                options_for(
                    process,
                    tls=TLSConfig(context=context, hostname="localhost", handshake_first=True),
                )
            )
            await conn.connect()
            try:
                await conn.flush()
            finally:
                await conn.close()
        finally:
            await process.stop()
