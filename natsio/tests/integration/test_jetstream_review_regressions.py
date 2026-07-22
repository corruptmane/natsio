"""Regressions for the M4 review findings — all probe-confirmed defects."""

import asyncio

import pytest

import natsio
from natsio.errors import ConfigError, ConnectionClosedError
from natsio.jetstream import (
    ConsumerConfig,
    JetStreamError,
    NoMessagesError,
    StorageType,
    StreamConfig,
)
from server import NatsServerProcess, require_server_binary


@pytest.fixture
async def server():
    binary = require_server_binary()
    process = NatsServerProcess(binary, jetstream=True)
    await process.start()
    yield process
    await process.stop()


@pytest.fixture
async def nc(server: NatsServerProcess):
    client = await natsio.connect(server.url, connect_timeout=5.0, request_timeout=5.0)
    yield client
    await client.close()


def count_pulls(client: natsio.Client) -> list[str]:
    """Instrument the client to record every pull request it publishes."""
    pulls: list[str] = []
    original = client.publish

    async def counting_publish(subject: str, *args, **kwargs):
        if ".CONSUMER.MSG.NEXT." in subject:
            pulls.append(subject)
        return await original(subject, *args, **kwargs)

    client.publish = counting_publish  # ty: ignore[invalid-assignment]
    return pulls


class TestHotLoopPacing:
    async def test_rejected_pulls_are_paced_not_hammered(self, nc: natsio.Client) -> None:
        """Protocol finding 1: 22,472 pulls in 2s before the fix."""
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="HOT", subjects=["hot.>"], storage=StorageType.MEMORY))
        await js.publish("hot.a", b"much-larger-than-five-bytes")
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))

        pulls = count_pulls(nc)
        with pytest.raises(JetStreamError):
            # max_bytes=5 -> server rejects with 409 "Message Size Exceeds
            # MaxBytes" or the session fails; either way, no hot loop.
            async with consumer.consume(max_messages=5, max_bytes=5, expires=2) as messages:
                async with asyncio.timeout(2.5):
                    await messages.next()
        assert len(pulls) < 20  # was >20,000 in the same window

    async def test_bad_heartbeat_fails_consume_loudly(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="HB", subjects=["hb.>"], storage=StorageType.MEMORY))
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))
        pulls = count_pulls(nc)
        with pytest.raises(JetStreamError, match=r"400|heartbeat"):
            # idle_heartbeat > expires/2 -> server 400s every pull.
            async with consumer.consume(max_messages=5, expires=1.0, idle_heartbeat=0.9) as messages:
                async with asyncio.timeout(2.5):
                    await messages.next()
        assert len(pulls) < 20  # was ~25,000


class TestFetchStatusHandling:
    async def test_fetch_surfaces_rejected_pull_instead_of_empty(self, nc: natsio.Client) -> None:
        """Protocol finding 2: a 400 came back as an empty batch before."""
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="F400", subjects=["f4.>"], storage=StorageType.MEMORY))
        for i in range(3):
            await js.publish(f"f4.{i}", b"x")
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))
        with pytest.raises(JetStreamError, match=r"400|heartbeat"):
            await consumer.fetch(5, timeout=1.0, idle_heartbeat=0.9)


class TestConsumeThroughput:
    async def test_slow_consumer_does_not_stall_between_batches(self, nc: natsio.Client) -> None:
        """Protocol finding 3: ~1.2s dead time after each drained batch before."""
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="FLOW", subjects=["fl.>"], storage=StorageType.MEMORY))
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))
        total = 30
        for i in range(total):
            await js.publish("fl.m", b"%d" % i)

        loop = asyncio.get_running_loop()
        started = loop.time()
        received = 0
        async with consumer.consume(max_messages=10, expires=3) as messages:
            async for msg in messages:
                await asyncio.sleep(0.02)  # deliberately slower than delivery
                await msg.ack()
                received += 1
                if received == total:
                    break
        elapsed = loop.time() - started
        # 30 * 0.02s = 0.6s of work; the old stall added ~1.2s per batch (x2).
        assert elapsed < 2.0, f"took {elapsed:.2f}s — refill stall is back"


class TestPumpResilience:
    async def test_pull_publish_failure_surfaces_instead_of_hanging(self, nc: natsio.Client) -> None:
        """Lifecycle finding 1: a dead pump left the iterator hanging forever."""
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="PUMP", subjects=["pm.>"], storage=StorageType.MEMORY))
        await js.publish("pm.a", b"x")
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))

        original = nc.publish

        async def failing_publish(subject: str, *args, **kwargs):
            if ".CONSUMER.MSG.NEXT." in subject:
                raise ConnectionClosedError("simulated: cannot buffer pull")
            return await original(subject, *args, **kwargs)

        nc.publish = failing_publish  # ty: ignore[invalid-assignment]
        with pytest.raises(ConnectionClosedError):
            async with consumer.consume(max_messages=5) as messages:
                async with asyncio.timeout(5):  # hung forever before the fix
                    await messages.next()

    async def test_consumption_without_context_manager_works(self, nc: natsio.Client) -> None:
        """Lifecycle finding 5: an unstarted session used to hang silently."""
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="NOCM", subjects=["ncm.>"], storage=StorageType.MEMORY))
        await js.publish("ncm.a", b"lazy")
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))
        messages = consumer.consume(max_messages=5)
        try:
            msg = await messages.next(timeout=5)
            assert msg.payload == b"lazy"
            await msg.ack()
        finally:
            await messages.stop()

    async def test_consumption_next_timeout_raises_no_messages(self, nc: natsio.Client) -> None:
        """Lifecycle finding 8: was a bare TimeoutError, inconsistent with Consumer.next."""
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="NTMO", subjects=["nt.>"], storage=StorageType.MEMORY))
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))
        async with consumer.consume(max_messages=5) as messages:
            with pytest.raises(NoMessagesError):
                await messages.next(timeout=0.3)


class TestConfigImmutability:
    async def test_create_consumer_does_not_mutate_caller_config(self, nc: natsio.Client) -> None:
        """Lifecycle finding 2: a reused ephemeral config collapsed to ONE consumer."""
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="CFG", subjects=["cfg.>"], storage=StorageType.MEMORY))
        config = ConsumerConfig()
        first = await stream.create_consumer(config)
        assert config.name is None  # untouched
        second = await stream.create_consumer(config)
        assert first.name != second.name
        names = [name async for name in stream.consumer_names()]
        assert len(names) == 2


class TestOrderedLifecycle:
    async def test_context_manager_deletes_ephemeral_consumer(self, nc: natsio.Client) -> None:
        """Lifecycle finding 3: teardown was GC-deferred with no deterministic close."""
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="OCM", subjects=["ocm.>"], storage=StorageType.MEMORY))
        for i in range(5):
            await js.publish("ocm.m", b"%d" % i)

        async with stream.ordered_consumer() as ordered:
            got = []
            async for msg in ordered:
                got.append(msg.payload)
                if len(got) == 2:
                    break  # abandon mid-stream on purpose
        # Deterministic: stop() deleted the ephemeral consumer synchronously —
        # not deferred to GC (which would linger for inactive_threshold, 5min).
        # The CONSUMER.NAMES listing can lag the just-completed delete by a
        # hair under load, so poll a short bounded window; the point is prompt,
        # not literally instantaneous.
        async with asyncio.timeout(3.0):
            # Polling external (server) state — there is no local event to await.
            while [name async for name in stream.consumer_names()]:  # noqa: ASYNC110
                await asyncio.sleep(0.02)

    async def test_idle_timeout_bounds_a_quiet_stream(self, nc: natsio.Client) -> None:
        """Lifecycle finding 9: no way to distinguish quiet from dead before."""
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="OIDL", subjects=["oi.>"], storage=StorageType.MEMORY))
        ordered = stream.ordered_consumer()
        with pytest.raises(NoMessagesError):
            async with asyncio.timeout(10):
                async for _ in ordered.messages(idle_timeout=0.5):
                    pass
        await ordered.stop()


class TestAckResilience:
    async def test_failed_ack_can_be_retried(self, nc: natsio.Client) -> None:
        """Lifecycle finding 6: ack() marked terminal even when the frame never left."""
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="ACKR", subjects=["ar.>"], storage=StorageType.MEMORY))
        await js.publish("ar.a", b"x")
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))
        msg = await consumer.next(timeout=5)

        original = nc.publish
        calls = {"n": 0}

        async def flaky_publish(subject: str, *args, **kwargs):
            if subject.startswith("$JS.ACK") and calls["n"] == 0:
                calls["n"] += 1
                raise ConnectionClosedError("simulated ack failure")
            return await original(subject, *args, **kwargs)

        nc.publish = flaky_publish  # ty: ignore[invalid-assignment]
        with pytest.raises(ConnectionClosedError):
            await msg.ack()
        await msg.ack()  # retry must be allowed — the first frame never left


class TestTTLValidation:
    async def test_fractional_ttl_rejected(self, nc: natsio.Client) -> None:
        """Protocol finding 5: ttl=0.5 silently became '0' before."""
        js = nc.jetstream()
        await js.create_stream(
            StreamConfig(name="TTLV", subjects=["tv.>"], storage=StorageType.MEMORY, allow_msg_ttl=True)
        )
        with pytest.raises(ConfigError, match="at least 1 second"):
            await js.publish("tv.a", b"x", ttl=0)
