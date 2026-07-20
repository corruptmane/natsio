"""End-to-end JetStream tests against a real nats-server with -js."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

import natsio
from natsio.jetstream import (
    AckPolicy,
    ConsumerConfig,
    DeliverPolicy,
    NoMessagesError,
    RetentionPolicy,
    StorageType,
    StreamConfig,
    StreamNotFoundError,
    WrongLastSequenceError,
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


class TestStreams:
    async def test_create_info_update_delete(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="CRUD", subjects=["crud.>"], storage=StorageType.MEMORY))
        assert stream.name == "CRUD"
        assert stream.cached_info.config.subjects == ["crud.>"]
        assert stream.cached_info.created is not None

        updated = await js.update_stream(
            StreamConfig(name="CRUD", subjects=["crud.>", "extra.>"], storage=StorageType.MEMORY)
        )
        assert updated.cached_info.config.subjects == ["crud.>", "extra.>"]

        names = [name async for name in js.stream_names()]
        assert "CRUD" in names

        await js.delete_stream("CRUD")
        with pytest.raises(StreamNotFoundError):
            await js.stream_info("CRUD")

    async def test_account_info_and_api_level(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        info = await js.account_info()
        assert info.limits.max_streams != 0  # -1 (unlimited) on a default server
        assert await js.api_level() >= 3  # 2.14 == level 3

    async def test_purge_and_get_msg(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="GETS", subjects=["gets.>"], storage=StorageType.MEMORY))
        for i in range(5):
            await js.publish(f"gets.{i}", b"payload-%d" % i)

        # Plain streams do NOT default to allow_direct (only KV-style streams
        # with max_msgs_per_subject do) — this exercises the MSG.GET API path.
        assert not stream.cached_info.config.allow_direct
        by_seq = await stream.get_msg(3)
        assert by_seq.seq == 3
        assert by_seq.payload == b"payload-2"
        assert by_seq.subject == "gets.2"
        assert by_seq.time is not None

        last = await stream.get_msg(subject="gets.4")
        assert last.payload == b"payload-4"

        purged = await stream.purge(subject="gets.1")
        assert purged == 1
        assert (await stream.info()).state.messages == 4

    async def test_direct_get_path(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(
            StreamConfig(name="DIRECT", subjects=["dg.>"], storage=StorageType.MEMORY, allow_direct=True)
        )
        for i in range(3):
            await js.publish(f"dg.{i}", b"payload-%d" % i)
        assert stream.cached_info.config.allow_direct is True

        by_seq = await stream.get_msg(2)
        assert by_seq.seq == 2
        assert by_seq.payload == b"payload-1"
        assert by_seq.subject == "dg.1"
        assert by_seq.time is not None

        last = await stream.get_msg(subject="dg.2")
        assert last.payload == b"payload-2"

        from natsio.jetstream import MessageNotFoundError

        with pytest.raises(MessageNotFoundError):
            await stream.get_msg(999)

    async def test_delete_msg(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="DEL", subjects=["del.>"], storage=StorageType.MEMORY))
        await js.publish("del.a", b"x")
        await stream.delete_msg(1)
        assert (await stream.info()).state.messages == 0


class TestPublish:
    async def test_pub_ack_and_dedup(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        await js.create_stream(
            StreamConfig(
                name="PUBS",
                subjects=["pubs.>"],
                storage=StorageType.MEMORY,
                duplicate_window=timedelta(seconds=30),
            )
        )
        first = await js.publish("pubs.a", b"one", msg_id="id-1")
        assert (first.stream, first.seq) == ("PUBS", 1)
        assert not first.duplicate

        again = await js.publish("pubs.a", b"one", msg_id="id-1")
        assert again.duplicate is True
        assert again.seq == 1

    async def test_expectations_enforced(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        await js.create_stream(StreamConfig(name="EXP", subjects=["exp.>"], storage=StorageType.MEMORY))
        ack = await js.publish("exp.a", b"1")
        await js.publish("exp.a", b"2", expected_last_seq=ack.seq)
        with pytest.raises(WrongLastSequenceError):
            await js.publish("exp.a", b"conflict", expected_last_seq=ack.seq)

    async def test_no_stream_bound(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        from natsio.jetstream import NoStreamResponseError

        with pytest.raises(NoStreamResponseError):
            await js.publish("nobody.owns.this", b"x", timeout=2)

    async def test_per_message_ttl(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(
            StreamConfig(name="TTL", subjects=["ttl.>"], storage=StorageType.MEMORY, allow_msg_ttl=True)
        )
        await js.publish("ttl.short", b"gone-soon", ttl=1)
        await js.publish("ttl.keep", b"stays", msg_id="keep-1")
        assert (await stream.info()).state.messages == 2
        await asyncio.sleep(2.0)
        assert (await stream.info()).state.messages == 1


class TestConsumers:
    async def test_consumer_crud(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="CCRUD", subjects=["c.>"], storage=StorageType.MEMORY))
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="worker"))
        assert consumer.name == "worker"
        assert consumer.cached_info.config.ack_policy is AckPolicy.EXPLICIT

        names = [name async for name in stream.consumer_names()]
        assert names == ["worker"]

        again = await stream.consumer("worker")
        assert again.cached_info.stream_name == "CCRUD"
        await stream.delete_consumer("worker")
        assert [name async for name in stream.consumer_names()] == []

    async def test_fetch_and_ack(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="FETCH", subjects=["f.>"], storage=StorageType.MEMORY))
        for i in range(5):
            await js.publish(f"f.{i}", b"%d" % i)
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))

        batch = await consumer.fetch(3, timeout=2)
        assert [m.payload for m in batch] == [b"0", b"1", b"2"]
        for msg in batch:
            meta = msg.metadata
            assert meta.stream == "FETCH"
            assert meta.consumer == "w"
            await msg.ack()

        rest = await consumer.fetch(10, timeout=1)
        assert [m.payload for m in rest] == [b"3", b"4"]
        # Unacked: redelivered after ack_wait — just term them for cleanliness.
        for msg in rest:
            await msg.term()

        info = await consumer.info()
        assert info.num_pending == 0

    async def test_fetch_empty_stream_returns_empty(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="EMPTY", subjects=["e.>"], storage=StorageType.MEMORY))
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))
        assert await consumer.fetch(5, timeout=0.5) == []

    async def test_next_raises_no_messages(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="NEXT", subjects=["n.>"], storage=StorageType.MEMORY))
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))
        with pytest.raises(NoMessagesError):
            await consumer.next(timeout=0.5)

    async def test_nak_redelivers(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="NAK", subjects=["nk.>"], storage=StorageType.MEMORY))
        await js.publish("nk.x", b"try-me")
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))

        first = await consumer.next(timeout=2)
        await first.nak()
        second = await consumer.next(timeout=2)
        assert second.payload == b"try-me"
        assert second.metadata.num_delivered == 2
        await second.ack_sync()

    async def test_ack_sync_double_ack_raises(self, nc: natsio.Client) -> None:
        from natsio.jetstream import MessageAlreadyAckedError

        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="DBL", subjects=["d.>"], storage=StorageType.MEMORY))
        await js.publish("d.x", b"once")
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))
        msg = await consumer.next(timeout=2)
        await msg.ack_sync()
        with pytest.raises(MessageAlreadyAckedError):
            await msg.ack()

    async def test_consumer_pause_resume(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="PAUSE", subjects=["p.>"], storage=StorageType.MEMORY))
        await stream.create_consumer(ConsumerConfig(durable_name="w"))
        await stream.pause_consumer("w", datetime.now(tz=UTC) + timedelta(hours=1))
        info = await stream.consumer_info("w")
        assert info.paused is True
        await stream.resume_consumer("w")
        info = await stream.consumer_info("w")
        assert not info.paused


class TestConsume:
    async def test_continuous_consume(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="CONS", subjects=["co.>"], storage=StorageType.MEMORY))
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))

        received: list[bytes] = []
        async with consumer.consume(max_messages=10, expires=5) as messages:
            for i in range(25):
                await js.publish("co.m", b"%d" % i)
            async for msg in messages:
                received.append(msg.payload)
                await msg.ack()
                if len(received) == 25:
                    break
        assert received == [b"%d" % i for i in range(25)]

    async def test_consume_refills_across_small_window(self, nc: natsio.Client) -> None:
        """max_messages far below the total forces several re-pulls."""
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="REFILL", subjects=["rf.>"], storage=StorageType.MEMORY))
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))
        total = 40
        for i in range(total):
            await js.publish("rf.m", b"%d" % i)

        received = 0
        async with consumer.consume(max_messages=4, expires=5) as messages:
            async for msg in messages:
                await msg.ack()
                received += 1
                if received == total:
                    break
        assert received == total

    async def test_consume_survives_reconnect(self, server: NatsServerProcess) -> None:
        import tempfile

        with tempfile.TemporaryDirectory(prefix="natsio-js-recon-") as store:
            binary = server.binary
            first = NatsServerProcess(binary, jetstream=True, store_dir=store)
            await first.start()
            nc = await natsio.connect(first.url, connect_timeout=5.0, request_timeout=5.0)
            try:
                js = nc.jetstream()
                stream = await js.create_stream(StreamConfig(name="RECON", subjects=["rc.>"]))  # FILE storage
                consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))
                await js.publish("rc.m", b"before")

                received: list[bytes] = []
                reconnected = asyncio.Event()
                nc._conn.bus.subscribe(lambda e: reconnected.set() if isinstance(e, natsio.Reconnected) else None)
                async with consumer.consume(max_messages=5, expires=3) as messages:
                    msg = await messages.next(timeout=5)
                    received.append(msg.payload)
                    await msg.ack_sync()

                    first.kill()
                    restarted = NatsServerProcess(binary, port=first.port, jetstream=True, store_dir=store)
                    await restarted.start()
                    try:
                        async with asyncio.timeout(15):
                            await reconnected.wait()
                        # The restarted server may still be electing its JS meta
                        # leader; publish() retries 503s briefly on its own.
                        await js.publish("rc.m", b"after", timeout=10)
                        msg = await messages.next(timeout=10)
                        received.append(msg.payload)
                        await msg.ack()
                    finally:
                        await restarted.stop()
                assert received == [b"before", b"after"]
            finally:
                await nc.close()
                await first.stop()


class TestOrderedConsumer:
    async def test_in_order_delivery(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="ORD", subjects=["o.>"], storage=StorageType.MEMORY))
        for i in range(30):
            await js.publish("o.m", b"%d" % i)

        received: list[bytes] = []
        async for msg in stream.ordered_consumer():
            received.append(msg.payload)
            if len(received) == 30:
                break
        assert received == [b"%d" % i for i in range(30)]

    async def test_deliver_policy_new_start(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="ORDNEW", subjects=["on.>"], storage=StorageType.MEMORY))
        await js.publish("on.m", b"old")

        ordered = stream.ordered_consumer(deliver_policy=DeliverPolicy.NEW)
        iterator = ordered.messages()
        collect = asyncio.ensure_future(anext(iterator))
        await asyncio.sleep(0.3)  # consumer created; "old" must not arrive
        await js.publish("on.m", b"new")
        msg = await asyncio.wait_for(collect, timeout=5)
        assert msg.payload == b"new"
        await iterator.aclose()

    async def test_recreates_after_consumer_deleted(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="ORDHEAL", subjects=["oh.>"], storage=StorageType.MEMORY))
        for i in range(3):
            await js.publish("oh.m", b"%d" % i)

        ordered = stream.ordered_consumer()
        iterator = ordered.messages(expires=2, idle_heartbeat=0.5)
        received = [(await anext(iterator)).payload]

        # Sabotage: delete the ephemeral consumer out from under it.
        assert ordered._consumer is not None
        await stream.delete_consumer(ordered._consumer.name)

        async with asyncio.timeout(15):
            while len(received) < 3:
                received.append((await anext(iterator)).payload)
        await iterator.aclose()
        # Self-healing must not skip or duplicate anything.
        assert received == [b"0", b"1", b"2"]


class TestWorkQueueSemantics:
    async def test_workqueue_retention_consumed_means_gone(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(
            StreamConfig(
                name="WQ",
                subjects=["wq.>"],
                retention=RetentionPolicy.WORK_QUEUE,
                storage=StorageType.MEMORY,
            )
        )
        for i in range(3):
            await js.publish("wq.job", b"%d" % i)
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))
        batch = await consumer.fetch(3, timeout=2)
        for msg in batch:
            await msg.ack_sync()
        assert (await stream.info()).state.messages == 0
