"""Bounded recovery/resource checks; scheduled runs increase the cycle count.

Resource samples cover live natsio tasks, dispatcher entries, request sinks,
and allocations attributed to shipped natsio files after a collection. Two
warm-up cycles exclude lazy initialization. The fixed 256 KiB retained-growth
budget tolerates allocator/cache noise without permitting a per-cycle budget.
It is a leak detector, not an RSS or total-process-memory guarantee.
"""

import asyncio
import gc
import json
import os
import platform
import sys
import tracemalloc
from collections import deque
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest

import natsio
from natsio._internal.lifecycle import ConnectionEvent, Reconnected
from natsio._internal.transport.tcp import TCPTransport
from natsio.errors import NATSError, SlowConsumerError
from natsio.jetstream import ConsumerConfig, StreamConfig
from natsio.kv import KeyValueConfig
from natsio.objectstore import ObjectMeta, ObjectStoreConfig
from server import NatsServerProcess, require_server_binary

CYCLES = max(5, int(os.environ.get("NATSIO_RELIABILITY_CYCLES", "5")))
RETAINED_GROWTH_LIMIT = 256 * 1024


class ResourceSamples:
    def __init__(self, name: str) -> None:
        self.name = name
        self.rows: list[dict] = []
        self.details: dict = {}
        self.started = asyncio.get_running_loop().time()

    def sample(self, client: natsio.Client) -> dict:
        gc.collect()
        snapshot = tracemalloc.take_snapshot()
        retained = sum(
            stat.size for stat in snapshot.statistics("filename") if "/natsio/src/natsio/" in stat.traceback[0].filename
        )
        tasks = sorted(task.get_name() for task in asyncio.all_tasks() if task.get_name().startswith("natsio-"))
        row = {
            "cycle": len(self.rows),
            "elapsed_seconds": asyncio.get_running_loop().time() - self.started,
            "tasks": tasks,
            "subscriptions": len(client._conn.dispatcher.entries()),
            "public_subscriptions": len(client._subscriptions),
            "owned_tasks": len(client._tasks),
            "sinks": len(client._sinks),
            "retained_natsio_bytes": retained,
            "server": client.server_info.get("version"),
        }
        self.rows.append(row)
        return row

    def assert_stable(self) -> None:
        baseline = self.rows[2]
        for row in self.rows[3:]:
            assert len(row["tasks"]) == len(baseline["tasks"]), self.rows
            assert row["subscriptions"] == baseline["subscriptions"], self.rows
            assert row["public_subscriptions"] == baseline["public_subscriptions"], self.rows
            assert row["owned_tasks"] == baseline["owned_tasks"], self.rows
            assert row["sinks"] == baseline["sinks"], self.rows
            assert row["retained_natsio_bytes"] <= baseline["retained_natsio_bytes"] + RETAINED_GROWTH_LIMIT, self.rows

    def save(self) -> None:
        destination = os.environ.get("NATSIO_RELIABILITY_REPORT")
        if not destination:
            return
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        report = json.loads(path.read_text()) if path.exists() else {}
        report[self.name] = {
            "python": sys.version,
            "platform": platform.platform(),
            "configured_cycles": CYCLES,
            "retained_growth_limit_bytes": RETAINED_GROWTH_LIMIT,
            "samples": self.rows,
            "details": self.details,
            "remaining_tasks": [
                {
                    "name": task.get_name(),
                    "stack": [f"{frame.f_code.co_filename}:{frame.f_lineno}" for frame in task.get_stack()],
                }
                for task in asyncio.all_tasks()
                if task.get_name().startswith("natsio-")
            ],
        }
        path.write_text(json.dumps(report, indent=2) + "\n")


@pytest.fixture
async def resources(request):
    tracemalloc.start()
    samples = ResourceSamples(request.node.name)
    try:
        yield samples
    finally:
        samples.save()
        tracemalloc.stop()


def reconnection_latch(client: natsio.Client):
    event = asyncio.Event()

    def observe(change: ConnectionEvent) -> None:
        if isinstance(change, Reconnected):
            event.set()

    return event, client._conn.bus.subscribe(observe)


async def test_repeated_two_node_failover_rotates_credentials_without_resource_growth(resources) -> None:
    """Two independent pool nodes exercise replay and fresh callback auth."""
    binary = require_server_binary()
    token = "reliability-token-0"
    nodes = [NatsServerProcess(binary), NatsServerProcess(binary)]
    nodes[0].config = f'authorization {{ token: "{token}" }}'
    await nodes[0].start()
    client = None
    try:
        client = await natsio.connect(
            *(node.url for node in nodes),
            token=lambda: token,
            no_randomize=True,
            reconnect_time_wait=0.01,
            reconnect_time_wait_max=0.02,
            reconnect_jitter=0,
            reconnect_jitter_tls=0,
            max_reconnect_attempts=-1,
        )
        incoming = client.subscribe("recovery.delivery")
        requested = asyncio.Event()

        def silent_handler(message: natsio.Msg) -> None:
            requested.set()

        client.subscribe("recovery.silent", cb=silent_handler)
        await client.flush()
        current = 0
        for cycle in range(CYCLES):
            async with asyncio.timeout(10):
                target = 1 - current
                next_token = f"reliability-token-{cycle + 1}"
                nodes[target].config = f'authorization {{ token: "{next_token}" }}'
                await nodes[target].start()
                token = next_token
                reconnected, detach = reconnection_latch(client)
                try:
                    nodes[current].kill()
                    await nodes[current].stop()
                    await reconnected.wait()
                finally:
                    detach()
                current = target
                assert client.connected_url == nodes[current].url
                body = f"cycle-{cycle}".encode()
                await client.publish("recovery.delivery", body)
                assert (await incoming.next_msg(timeout=2)).payload == body

                # Cancel a request only after the live responder received it.
                requested.clear()
                request = asyncio.create_task(client.request("recovery.silent", timeout=60))
                try:
                    await requested.wait()
                finally:
                    request.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await request
                assert not client._sinks
                await client.flush()
                resources.sample(client)
        resources.assert_stable()
    finally:
        if client is not None:
            await client.close()
        for node in nodes:
            await node.stop()


async def test_long_lived_jetstream_kv_object_handles_survive_repeated_reconnects(resources) -> None:
    server = NatsServerProcess(require_server_binary(), jetstream=True)
    await server.start()
    client = None
    try:
        client = await natsio.connect(server.url, reconnect_time_wait=0.01, reconnect_jitter=0)
        js = client.jetstream()
        stream = await js.create_stream(StreamConfig(name="RELIABILITY", subjects=["reliability.js"], max_msgs=10))
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="reader"))
        kv = await js.create_key_value(KeyValueConfig(bucket="RELIABILITY"))
        objects = await js.create_object_store(ObjectStoreConfig(bucket="RELIABILITY"))
        async with kv.watch(updates_only=True) as watcher:
            updates = aiter(watcher)
            async with asyncio.timeout(5):
                assert await anext(updates) is None
            for cycle in range(CYCLES):
                async with asyncio.timeout(15):
                    reconnected, detach = reconnection_latch(client)
                    try:
                        await client.force_reconnect()
                        await reconnected.wait()
                    finally:
                        detach()
                    body = f"verified-cycle-{cycle}".encode()
                    await js.publish("reliability.js", body)
                    message = await consumer.next(timeout=3)
                    assert message.payload == body
                    await message.ack_sync()
                    revision = await kv.put("current", body)
                    entry = await anext(updates)
                    assert entry is not None
                    assert (entry.value, entry.revision) == (body, revision)
                    assert (await kv.get("current")).value == body
                    # Several chunks, replaced under the same name each cycle;
                    # get_bytes verifies digest and total length on every read.
                    object_body = body * 4096
                    await objects.put(ObjectMeta(name="current", chunk_size=16 * 1024), object_body)
                    assert await objects.get_bytes("current") == object_body
                    await client.flush()
                    resources.sample(client)
        resources.assert_stable()
    finally:
        if client is not None:
            await client.close()
        await server.stop()


class AdmissionWaiters(deque[asyncio.Future[None]]):
    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()

    def append(self, future: asyncio.Future[None]) -> None:
        super().append(future)
        self.entered.set()


async def test_slow_consumers_and_cancelled_blocked_writes_recover_without_accumulation(resources) -> None:
    server = NatsServerProcess(require_server_binary())
    await server.start()
    client = None
    try:
        reported = asyncio.Event()

        def on_error(error: NATSError) -> None:
            if isinstance(error, SlowConsumerError):
                reported.set()

        client = await natsio.connect(server.url, max_pending_size=256, error_cb=on_error)
        session = client._conn._session
        assert session is not None
        transport = session.transport
        assert isinstance(transport, TCPTransport)
        protocol = transport._protocol
        assert protocol is not None
        delivered = client.subscribe("reliability.write")
        for cycle in range(CYCLES):
            async with asyncio.timeout(5):
                reported.clear()
                async with client.subscribe("reliability.slow", pending_msgs_limit=2) as slow:
                    await client.flush()
                    for _ in range(8):
                        await client.publish("reliability.slow", b"overflow")
                    await client.flush()
                    await reported.wait()
                    assert slow.dropped == 6
                await client.flush()

                # Exercise the actual TCP flow-control callback deterministically
                # rather than depending on platform-specific kernel buffer sizes.
                waiters = AdmissionWaiters()
                session._drain_waiters = waiters
                protocol.pause_writing()
                frame_body = f"{cycle:04d}".encode() + b"x" * 176
                blocked = None
                try:
                    await client.publish("reliability.write", frame_body)
                    blocked = asyncio.create_task(client.publish("reliability.write", b"cancelled" * 20))
                    await waiters.entered.wait()
                    blocked.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await blocked
                    assert not waiters
                finally:
                    if blocked is not None and not blocked.done():
                        blocked.cancel()
                        await asyncio.gather(blocked, return_exceptions=True)
                    protocol.resume_writing()
                await client.flush()
                assert (await delivered.next_msg(timeout=2)).payload == frame_body
                assert delivered.pending_msgs == 0
                resources.sample(client)
        resources.assert_stable()
    finally:
        if client is not None:
            await client.close()
        await server.stop()


async def test_request_many_abandoned_burst_releases_queued_replies(resources) -> None:
    """Measure the existing unbounded pause-at-yield queue, then prove cleanup.

    max_msgs is a consumer completion condition, not an ingress bound. Keep
    this bounded reproduction explicit until a separately designed capacity
    and overflow contract is introduced.
    """
    server = NatsServerProcess(require_server_binary())
    await server.start()
    client = None
    try:
        client = await natsio.connect(server.url)
        send_burst = asyncio.Event()
        delivered = asyncio.Event()
        burst_count = 512
        payload = b"r" * 1024

        async def responder(request: natsio.Msg) -> None:
            await request.respond(b"first")
            await send_burst.wait()
            for _ in range(burst_count):
                await request.respond(payload)
            await client.publish("reliability.burst.done")

        client.subscribe("reliability.fanout", cb=responder)
        client.subscribe("reliability.burst.done", cb=lambda message: delivered.set())
        await client.flush()
        # Warm up the mux without retaining an outstanding generator.
        client._ensure_mux()
        await client.flush()
        before = resources.sample(client)
        replies = client.request_many("reliability.fanout", max_msgs=1, timeout=1)
        assert isinstance(replies, AsyncGenerator)
        try:
            async with asyncio.timeout(5):
                first = await anext(replies)
                assert isinstance(first, natsio.Msg)
                assert first.payload == b"first"
                send_burst.set()
                await delivered.wait()
                sink = next(iter(client._sinks.values()))
                assert sink.queue is not None
                queued = sink.queue.qsize()
                peak = resources.sample(client)
                resources.details = {
                    "max_msgs": 1,
                    "queued_while_generator_suspended": queued,
                    "burst_payload_bytes": burst_count * len(payload),
                    "peak_retained_growth": peak["retained_natsio_bytes"] - before["retained_natsio_bytes"],
                }
                assert queued == burst_count
        finally:
            await replies.aclose()
        del sink
        # Establish quiescence after the last response batch (including parser
        # buffers and the flusher's last joined write) has advanced.
        await client.flush()
        after = resources.sample(client)
        assert not client._sinks
        assert after["subscriptions"] == before["subscriptions"]
        assert len(after["tasks"]) == len(before["tasks"])
        assert after["retained_natsio_bytes"] <= before["retained_natsio_bytes"] + RETAINED_GROWTH_LIMIT
    finally:
        if client is not None:
            await client.close()
        await server.stop()
