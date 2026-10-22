"""A healthy server must not turn an impossible local write into a hang."""

import asyncio

import pytest

import natsio
from natsio._internal.protocol import encode_header_block, encode_hpub, encode_pub
from natsio.errors import ConfigError
from server import NatsServerProcess, require_server_binary


@pytest.fixture
async def server():
    process = NatsServerProcess(require_server_binary())
    await process.start()
    try:
        yield process
    finally:
        await process.stop()


async def test_server_accepted_payload_with_impossible_local_frame_fails_promptly(server) -> None:
    async with await natsio.connect(server.url, max_pending_size=64, flush_timeout=0.02) as client:
        payload = b"x" * 100
        assert client.max_payload >= len(payload)
        async with asyncio.timeout(0.2):
            with pytest.raises(ConfigError, match="max_pending_size"):
                await client.publish("accepted.by.server", payload)
        await client.publish("still.works", b"ok")
        await client.flush()
        assert client.stats.out_msgs == 1


@pytest.mark.parametrize("headers", [None, {"Trace": "includes-header-overhead"}])
@pytest.mark.parametrize("extra_capacity", [-1, 0, 1])
async def test_live_encoded_frame_boundaries_and_delivery(server, headers, extra_capacity) -> None:
    payload = b"body"
    frame = (
        encode_pub("boundaries", None, payload)
        if headers is None
        else encode_hpub("boundaries", None, encode_header_block(headers), payload)
    )
    async with (
        await natsio.connect(server.url, max_pending_size=len(frame) + extra_capacity, flush_timeout=0.02) as client,
        client.subscribe("boundaries") as subscription,
    ):
        await client.flush()
        if extra_capacity < 0:
            async with asyncio.timeout(0.2):
                with pytest.raises(ConfigError, match="max_pending_size"):
                    await client.publish("boundaries", payload, headers=headers)
            assert client.stats.out_msgs == 0
        else:
            await client.publish("boundaries", payload, headers=headers)
            message = await subscription.next_msg(timeout=2)
            assert message.payload == payload
            if headers is not None:
                assert message.headers is not None
                assert message.headers["Trace"] == headers["Trace"]
