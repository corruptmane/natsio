"""Verified discovery and failover against a real two-node TLS NATS cluster."""

import asyncio
import ssl

import pytest

from natsio import Client, ConnectOptions, TLSConfig
from natsio._internal.lifecycle import ConnectionEvent, Reconnected, ServersDiscovered
from natsio._internal.transport.tcp import TCPTransport
from server import NatsServerProcess, free_port, generate_self_signed_cert, openssl_available, require_server_binary


def assert_encrypted(client: Client) -> None:
    session = client._conn._session
    assert session is not None
    assert session.tls_required
    transport = session.transport
    assert isinstance(transport, TCPTransport)
    assert transport._transport is not None
    assert transport._transport.get_extra_info("ssl_object") is not None


@pytest.mark.parametrize("seed_scheme", ["tls", "nats"])
async def test_verified_cluster_discovery_survives_tls_seed_loss(tmp_path, seed_scheme: str) -> None:
    if not openssl_available():
        pytest.skip("openssl CLI not available")
    binary = require_server_binary()
    cert, key = generate_self_signed_cert(tmp_path)
    route_ports = [free_port(), free_port()]
    nodes = [NatsServerProcess(binary), NatsServerProcess(binary)]
    for index, node in enumerate(nodes):
        node.config = (
            f'server_name: "verified-{index}"\n'
            'authorization { token: "TLS_CLUSTER_DUMMY_TOKEN" }\n'
            f'tls {{ cert_file: "{cert}", key_file: "{key}" }}\n'
            "cluster {\n"
            '  name: "verified-discovery"\n'
            '  host: "127.0.0.1"\n'
            f"  port: {route_ports[index]}\n"
            f'  routes: ["nats-route://127.0.0.1:{route_ports[1 - index]}"]\n'
            "}\n"
        )

    discovered = asyncio.Event()
    reconnected = asyncio.Event()

    def observe(event: ConnectionEvent) -> None:
        if isinstance(event, ServersDiscovered):
            discovered.set()
        elif isinstance(event, Reconnected):
            reconnected.set()

    client = Client(
        ConnectOptions(
            servers=(f"{seed_scheme}://127.0.0.1:{nodes[0].port}",),
            token="TLS_CLUSTER_DUMMY_TOKEN",
            tls=TLSConfig(context=ssl.create_default_context(cafile=str(cert))),
            no_randomize=True,
            reconnect_time_wait=0.01,
            reconnect_time_wait_max=0.02,
            reconnect_jitter=0,
            reconnect_jitter_tls=0,
            max_reconnect_attempts=-1,
        )
    )
    detach = client._conn.bus.subscribe(observe)
    try:
        async with asyncio.timeout(10):
            for node in nodes:
                await node.start()
            await client.connect()
            await discovered.wait()
            candidates = [server for server in client._conn._pool.servers if server.port == nodes[1].port]
            assert len(candidates) == 1
            assert candidates[0].discovered
            assert candidates[0].scheme == "tls"
            assert candidates[0].tls_required
            assert_encrypted(client)

            async with client.subscribe("verified.cluster") as subscription:
                await client.flush()
                nodes[0].kill()
                await nodes[0].stop()
                await reconnected.wait()
                assert client.connected_url == f"tls://127.0.0.1:{nodes[1].port}"
                assert_encrypted(client)
                await client.publish("verified.cluster", b"authenticated TLS failover")
                assert (await subscription.next_msg(timeout=2)).payload == b"authenticated TLS failover"
    finally:
        detach()
        await client.close()
        for node in nodes:
            await node.stop()
