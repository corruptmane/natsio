"""Discovery is trusted only after a complete authenticated handshake."""

import asyncio
import ssl

import pytest

from fake import EventRecorder, FakeEnv, FakeTransport
from natsio._internal.connection import Connection
from natsio._internal.lifecycle import ServersDiscovered
from natsio._internal.pool import ServerPool, parse_server_url
from natsio.errors import NoServersAvailableError
from natsio.options import ConnectOptions


def options(**overrides) -> ConnectOptions:
    values: dict = {"servers": ("nats://seed:4222",), "no_randomize": True, "connect_timeout": 1.0}
    values.update(overrides)
    return ConnectOptions(**values)


@pytest.mark.parametrize("later_info", [False, True])
async def test_failed_auth_cannot_commit_any_handshake_discovery(later_info: bool) -> None:
    env = FakeEnv()
    if not later_info:
        env.info["connect_urls"] = ["attacker:4222"]

    def reject(transport: FakeTransport, data: bytes) -> None:
        if b"CONNECT " in data:
            if later_info:
                transport.deliver_info({"connect_urls": ["attacker:4222"]})
            transport.deliver(b"-ERR 'Authorization Violation'\r\n")

    env.on_client_write = reject
    conn = Connection(options(), transport_factory=env.factory)
    events = EventRecorder()
    conn.bus.subscribe(events.hook)
    with pytest.raises(NoServersAvailableError):
        await conn.connect()
    assert [server.host for server in conn._pool.servers] == ["seed"]
    assert events.count(ServersDiscovered) == 0


async def test_certificate_failure_cannot_commit_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    async def reject_certificate(self, context, hostname) -> None:
        raise ssl.SSLCertVerificationError("untrusted certificate")

    monkeypatch.setattr(FakeTransport, "upgrade_tls", reject_certificate)
    env = FakeEnv()
    env.info["connect_urls"] = ["attacker:4222"]
    conn = Connection(options(servers=("tls://seed:4222",)), transport_factory=env.factory)
    with pytest.raises(NoServersAvailableError):
        await conn.connect()
    assert [server.host for server in conn._pool.servers] == ["seed"]
    assert env.current.writes == []


async def test_discovery_is_not_visible_while_authentication_is_parked() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    class DelayedAuth:
        async def authenticate(self, nonce):
            from natsio.auth import AuthResult

            entered.set()
            await release.wait()
            return AuthResult(auth_token="dummy")

    env = FakeEnv()
    env.info["connect_urls"] = ["first:4222"]
    conn = Connection(options(authenticator=DelayedAuth()), transport_factory=env.factory)
    task = asyncio.create_task(conn.connect())
    try:
        async with asyncio.timeout(2):
            await entered.wait()
        env.current.deliver_info({"connect_urls": ["latest:4222"]})
        assert [server.host for server in conn._pool.servers] == ["seed"]
        release.set()
        await task
        assert {server.host for server in conn._pool.servers} == {"seed", "latest"}
    finally:
        release.set()
        await conn.close(flush=False)
        await task


@pytest.mark.parametrize("source_scheme, expected", [("tls", "tls"), ("nats+tls", "tls"), ("wss", "wss"), ("ws", "ws")])
async def test_discovered_servers_inherit_source_security_and_transport(source_scheme: str, expected: str) -> None:
    source = parse_server_url(f"{source_scheme}://seed:4222")
    conn = Connection(options(servers=(source.url,)))
    conn._merge_connect_urls({"connect_urls": ["peer:4223", "nats://other:4224"]}, source)
    discovered = [server for server in conn._pool.servers if server.discovered]
    assert len(discovered) == 2
    assert all(server.tls_required == source.tls_required for server in discovered)
    assert all(server.websocket == source.websocket for server in discovered)
    assert all(server.scheme == expected for server in discovered)


@pytest.mark.parametrize("urls", [("nats://plain:4222", "tls://secure:4223"), ("ws://plain:80", "wss://secure:443")])
def test_mixed_explicit_security_pools_keep_per_endpoint_policy(urls: tuple[str, str]) -> None:
    # natsio already permits explicit opt-in to both security levels. Discovery
    # hardening must not accidentally promote or remove the configured entry.
    pool = ServerPool(urls, randomize=False)
    assert [server.tls_required for server in pool.servers] == [False, True]
    assert [server.url for server in pool.servers] == list(urls)


@pytest.mark.parametrize(
    "advertisement",
    [
        "nats://attacker:secret@peer:4222",
        "peer:4222/path",
        "peer:4222?token=secret",
        "peer:4222#fragment",
        "peer:0",
        "peer:65536",
        "[malformed:4222",
        "ws://peer:4222",
    ],
)
async def test_tcp_discovery_rejects_invalid_or_foreign_transport_endpoints(advertisement: str) -> None:
    conn = Connection(options())
    conn._merge_connect_urls({"connect_urls": [advertisement]}, conn._pool.servers[0])
    assert len(conn._pool.servers) == 1


@pytest.mark.parametrize("later_false", [False, True])
async def test_tls_requirement_arriving_during_async_auth_precedes_connect(monkeypatch, later_false: bool) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    upgraded = False
    plaintext_connects: list[bytes] = []

    class DelayedAuth:
        async def authenticate(self, nonce):
            from natsio.auth import AuthResult

            entered.set()
            await release.wait()
            return AuthResult(auth_token="LATE_TLS_DUMMY_TOKEN")

    async def accept_tls(self, context, hostname) -> None:
        nonlocal upgraded
        upgraded = True

    monkeypatch.setattr(FakeTransport, "upgrade_tls", accept_tls)
    env = FakeEnv()
    default_on_write = env.on_client_write

    def record_connect(transport, data) -> None:
        if b"CONNECT " in data and not upgraded:
            plaintext_connects.append(data)
        default_on_write(transport, data)

    env.on_client_write = record_connect
    conn = Connection(options(authenticator=DelayedAuth()), transport_factory=env.factory)
    task = asyncio.create_task(conn.connect())
    try:
        async with asyncio.timeout(2):
            await entered.wait()
        env.current.deliver_info({"tls_required": True, "connect_urls": ["peer:4223"]})
        if later_false:
            env.current.deliver_info({"tls_required": False})
        release.set()
        await task
        assert not plaintext_connects
        assert upgraded
    finally:
        release.set()
        await conn.close(flush=False)
        await task
