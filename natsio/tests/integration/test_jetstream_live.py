"""End-to-end JetStream tests against a real nats-server with -js."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

import natsio
from natsio.errors import ConnectionClosedError
from natsio.jetstream import (
    AckPolicy,
    ConsumerConfig,
    ConsumerNotFoundError,
    DeliverPolicy,
    JetStreamContext,
    JetStreamError,
    NoMessagesError,
    NoStreamResponseError,
    PriorityPolicy,
    RetentionPolicy,
    StorageType,
    Stream,
    StreamConfig,
    StreamNotFoundError,
    TooManyStalledMsgsError,
    WrongLastSequenceError,
)
from natsio.jetstream import headers as js_headers
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
        assert await js.api_level() >= 3  # nats-server 2.14.3 reports 4

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


class TestBatchGet:
    """`Stream.get_last_msgs_for` — batch Direct Get (multi_last, ADR-31)."""

    async def _seed(self, js: JetStreamContext, name: str, *, allow_direct: bool) -> Stream:
        stream = await js.create_stream(
            StreamConfig(name=name, subjects=[f"{name}.>"], storage=StorageType.MEMORY, allow_direct=allow_direct)
        )
        # Two writes per subject so the "last" message is unambiguous.
        await js.publish(f"{name}.a", b"a-old")
        await js.publish(f"{name}.a", b"a-new")
        await js.publish(f"{name}.b", b"b-old")
        await js.publish(f"{name}.b", b"b-new")
        await js.publish(f"{name}.c", b"c-only")
        return stream

    async def test_returns_last_per_subject(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await self._seed(js, "BATCH", allow_direct=True)
        found = {m.subject: m.payload async for m in stream.get_last_msgs_for(["BATCH.>"])}
        assert found == {"BATCH.a": b"a-new", "BATCH.b": b"b-new", "BATCH.c": b"c-only"}

    async def test_explicit_subject_list_with_a_miss(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await self._seed(js, "BATCHX", allow_direct=True)
        # BATCHX.nope has no stored message — it is simply omitted, not an error.
        found = {m.subject: m.payload async for m in stream.get_last_msgs_for(["BATCHX.a", "BATCHX.nope", "BATCHX.c"])}
        assert found == {"BATCHX.a": b"a-new", "BATCHX.c": b"c-only"}

    async def test_string_subject(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await self._seed(js, "BATCHS", allow_direct=True)
        out = [m async for m in stream.get_last_msgs_for("BATCHS.b")]
        assert [(m.subject, m.payload) for m in out] == [("BATCHS.b", b"b-new")]

    async def test_carries_metadata(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await self._seed(js, "BATCHM", allow_direct=True)
        out = {m.subject: m async for m in stream.get_last_msgs_for(["BATCHM.a"])}
        entry = out["BATCHM.a"]
        assert entry.seq > 0
        assert entry.time is not None

    async def test_up_to_seq(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await self._seed(js, "BATCHU", allow_direct=True)
        # Seqs: a-old=1 a-new=2 b-old=3 b-new=4 c-only=5. Cap at 3 -> a=a-new(2), b=b-old(3).
        found = {m.subject: m.payload async for m in stream.get_last_msgs_for(["BATCHU.>"], up_to_seq=3)}
        assert found == {"BATCHU.a": b"a-new", "BATCHU.b": b"b-old"}

    async def test_non_direct_stream_errors_clearly(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await self._seed(js, "BATCHND", allow_direct=False)
        from natsio.errors import ConfigError

        with pytest.raises(ConfigError):
            async for _ in stream.get_last_msgs_for(["BATCHND.a"]):
                pass

    async def test_empty_subjects_errors(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await self._seed(js, "BATCHE", allow_direct=True)
        from natsio.errors import ConfigError

        with pytest.raises(ConfigError):
            async for _ in stream.get_last_msgs_for([]):
                pass


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


class TestOptionalAwait:
    async def test_consume_and_ordered_tolerate_await(self, nc) -> None:
        """nats-py muscle memory: awaiting the session factories is a no-op."""
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="OPTAW", subjects=["oa.>"]))
        await js.publish("oa.m", b"x")
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="oa"))
        async with await consumer.consume(max_messages=5, expires=2) as session:
            msg = await session.next(timeout=5)
            await msg.ack()
            assert msg.payload == b"x"
        ordered = await stream.ordered_consumer()
        async with ordered:
            got = [m.payload async for m in ordered.messages(until_drained=True)]
        assert got == [b"x"]


class TestUntilDrained:
    """messages(until_drained=True): finite reads end normally, not by exception."""

    async def _stream_with(self, nc, name: str, count: int):
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name=name, subjects=[f"{name.lower()}.>"]))
        for i in range(count):
            await js.publish(f"{name.lower()}.m", f"{i}".encode())
        return js, stream

    async def test_finite_read_ends_normally(self, nc) -> None:
        _js, stream = await self._stream_with(nc, "DRAIN", 25)
        async with stream.ordered_consumer() as ordered:
            got = [msg.payload async for msg in ordered.messages(until_drained=True)]
        # No NoMessagesError, no timeout wait — exactly the stream contents.
        assert got == [f"{i}".encode() for i in range(25)]

    async def test_empty_stream_yields_nothing(self, nc) -> None:
        _js, stream = await self._stream_with(nc, "EMPTYD", 0)
        async with stream.ordered_consumer() as ordered:
            got = [msg async for msg in ordered.messages(until_drained=True)]
        assert got == []

    async def test_second_drain_resumes_from_position(self, nc) -> None:
        js, stream = await self._stream_with(nc, "RESUME", 3)
        async with stream.ordered_consumer() as ordered:
            first = [msg.payload async for msg in ordered.messages(until_drained=True)]
            assert first == [b"0", b"1", b"2"]
            for i in range(3, 5):
                await js.publish("resume.m", f"{i}".encode())
            second = [msg.payload async for msg in ordered.messages(until_drained=True)]
        # The ordered consumer keeps its position: only the new messages.
        assert second == [b"3", b"4"]

    async def test_purge_mid_drain_ends_instead_of_hanging(self, nc) -> None:
        js, stream = await self._stream_with(nc, "PURGED", 400)
        drained = 0
        async with stream.ordered_consumer() as ordered:
            async with asyncio.timeout(30.0):
                async for _msg in ordered.messages(until_drained=True):
                    drained += 1
                    if drained == 5:
                        await js.purge_stream("PURGED")
        # Ended normally (via the internal probe + pending recheck), possibly
        # after a partial read — never a hang, never NoMessagesError.
        assert drained >= 5


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


def _pull_signal(client: natsio.Client) -> asyncio.Event:
    """Fire an event the first time the client publishes a pull request.

    Lets tests wait — event-driven, no sleeps — until a consume/fetch session
    is actually parked on the server before they perturb the connection.
    """
    event = asyncio.Event()
    original = client.publish

    async def publish(subject: str, *args, **kwargs):
        result = await original(subject, *args, **kwargs)
        if ".CONSUMER.MSG.NEXT." in subject:
            event.set()
        return result

    client.publish = publish  # ty: ignore[invalid-assignment]
    return event


class TestConnectionCloseWakesConsumers:
    """A closing/exhausted connection must wake parked pull iterators, not hang.

    Mirrors nats.go TestPullConsumerConnectionClosed and
    TestPullConsumerMaxReconnectsExceeded.
    """

    async def test_client_close_wakes_parked_consume(self, server: NatsServerProcess) -> None:
        nc = await natsio.connect(server.url, connect_timeout=5.0, request_timeout=5.0)
        try:
            js = nc.jetstream()
            stream = await js.create_stream(StreamConfig(name="CLC", subjects=["clc.>"], storage=StorageType.MEMORY))
            consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))
            pulled = _pull_signal(nc)
            async with consumer.consume(max_messages=5) as messages:
                parked = asyncio.ensure_future(messages.next(timeout=30))
                async with asyncio.timeout(5):
                    await pulled.wait()  # a pull is outstanding: next() is parked
                loop = asyncio.get_running_loop()
                await nc.close()
                started = loop.time()
                with pytest.raises(ConnectionClosedError):
                    async with asyncio.timeout(5):
                        await parked
                assert loop.time() - started < 2.0  # woken promptly, not on a deadline
        finally:
            await nc.close()

    async def test_client_close_wakes_parked_ordered_messages(self, server: NatsServerProcess) -> None:
        nc = await natsio.connect(server.url, connect_timeout=5.0, request_timeout=5.0)
        try:
            js = nc.jetstream()
            stream = await js.create_stream(StreamConfig(name="OCLC", subjects=["oclc.>"], storage=StorageType.MEMORY))
            ordered = stream.ordered_consumer()
            pulled = _pull_signal(nc)
            iterator = ordered.messages()
            parked = asyncio.ensure_future(anext(iterator))
            try:
                async with asyncio.timeout(5):
                    await pulled.wait()
                await nc.close()
                with pytest.raises(ConnectionClosedError):
                    async with asyncio.timeout(5):
                        await parked
            finally:
                parked.cancel()
        finally:
            await nc.close()

    async def test_reconnect_exhaustion_wakes_parked_consume(self, server: NatsServerProcess) -> None:
        nc = await natsio.connect(
            server.url,
            connect_timeout=5.0,
            request_timeout=5.0,
            max_reconnect_attempts=2,
            reconnect_time_wait=0.2,
            reconnect_time_wait_max=0.2,
        )
        try:
            js = nc.jetstream()
            stream = await js.create_stream(StreamConfig(name="EXH", subjects=["exh.>"], storage=StorageType.MEMORY))
            consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))
            pulled = _pull_signal(nc)
            async with consumer.consume(max_messages=5) as messages:
                parked = asyncio.ensure_future(messages.next(timeout=30))
                async with asyncio.timeout(5):
                    await pulled.wait()
                server.kill()  # never restarted: reconnect budget will exhaust
                with pytest.raises(ConnectionClosedError):
                    async with asyncio.timeout(20):
                        await parked
        finally:
            await nc.close()

    async def test_fetch_raises_on_client_close(self, server: NatsServerProcess) -> None:
        """A parked fetch() must surface ConnectionClosedError, not return []."""
        nc = await natsio.connect(server.url, connect_timeout=5.0, request_timeout=5.0)
        try:
            js = nc.jetstream()
            stream = await js.create_stream(StreamConfig(name="FCL", subjects=["fcl.>"], storage=StorageType.MEMORY))
            consumer = await stream.create_consumer(ConsumerConfig(durable_name="w"))
            pulled = _pull_signal(nc)
            fetching = asyncio.ensure_future(consumer.fetch(5, timeout=30))
            try:
                async with asyncio.timeout(5):
                    await pulled.wait()
                await nc.close()
                with pytest.raises(ConnectionClosedError):
                    async with asyncio.timeout(5):
                        await fetching
            finally:
                fetching.cancel()
        finally:
            await nc.close()


class TestCreateOrUpdateStream:
    async def test_create_then_update_then_noop(self, nc) -> None:
        js = nc.jetstream()
        config = StreamConfig(name="COU", subjects=["cou.>"])
        created = await js.create_or_update_stream(config)
        assert created.name == "COU"

        config.description = "second pass"
        updated = await js.create_or_update_stream(config)
        assert updated.cached_info.config.description == "second pass"

        # Identical config: idempotent, never StreamNameInUseError.
        again = await js.create_or_update_stream(config)
        assert again.cached_info.config.description == "second pass"


class TestStreamListing:
    async def test_streams_and_names_filter_by_subject(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        await js.create_stream(StreamConfig(name="ALPHA", subjects=["alpha.>"], storage=StorageType.MEMORY))
        await js.create_stream(StreamConfig(name="BETA", subjects=["beta.>"], storage=StorageType.MEMORY))

        infos = [info.config.name async for info in js.streams(subject="alpha.1")]
        assert infos == ["ALPHA"]
        names = [name async for name in js.stream_names(subject="beta.9")]
        assert names == ["BETA"]

        # No filter yields both.
        assert {info.config.name async for info in js.streams()} == {"ALPHA", "BETA"}

    async def test_pagination_across_page_boundary(self, nc: natsio.Client) -> None:
        """The server pages STREAM.NAMES/LIST at 256 rows; 300 forces a second page."""
        js = nc.jetstream()
        total = 300

        async def make(i: int) -> None:
            await js.create_stream(StreamConfig(name=f"P{i:04d}", subjects=[f"p{i}.>"], storage=StorageType.MEMORY))

        for start in range(0, total, 50):
            await asyncio.gather(*(make(i) for i in range(start, min(start + 50, total))))

        names = {name async for name in js.stream_names()}
        assert len(names) == total
        infos = {info.config.name async for info in js.streams()}
        assert len(infos) == total
        assert names == infos


class TestNextBySubject:
    async def _seed(self, js: JetStreamContext, name: str, *, allow_direct: bool) -> Stream:
        stream = await js.create_stream(
            StreamConfig(
                name=name,
                subjects=[f"{name}.>"],
                storage=StorageType.MEMORY,
                allow_direct=allow_direct,
            )
        )
        await js.publish(f"{name}.a", b"1")  # seq 1
        await js.publish(f"{name}.b", b"2")  # seq 2
        await js.publish(f"{name}.a", b"3")  # seq 3
        await js.publish(f"{name}.b", b"4")  # seq 4
        return stream

    async def test_next_by_subj_direct_path(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await self._seed(js, "NBSD", allow_direct=True)
        assert stream.cached_info.config.allow_direct is True

        first = await stream.get_msg(sequence=1, subject="NBSD.a", next_for=True)
        assert (first.seq, first.payload) == (1, b"1")
        # next a-message at or after seq 2 is seq 3, skipping the b at seq 2.
        nxt = await stream.get_msg(sequence=2, subject="NBSD.a", next_for=True)
        assert (nxt.seq, nxt.payload) == (3, b"3")

    async def test_next_by_subj_api_path(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await self._seed(js, "NBSA", allow_direct=False)
        assert not stream.cached_info.config.allow_direct

        first = await stream.get_msg(sequence=1, subject="NBSA.a", next_for=True)
        assert (first.seq, first.payload) == (1, b"1")
        nxt = await stream.get_msg(sequence=2, subject="NBSA.a", next_for=True)
        assert (nxt.seq, nxt.payload) == (3, b"3")


class TestNameValidation:
    async def test_dotted_stream_name_fails_fast(self, nc: natsio.Client) -> None:
        import time

        from natsio.errors import ConfigError

        js = nc.jetstream()
        start = time.monotonic()
        with pytest.raises(ConfigError):
            await js.stream_info("foo.123")
        # Must reject client-side, not hang the full JS timeout.
        assert time.monotonic() - start < 1.0


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


class TestPriorityGroups:
    """ADR-42 priority groups: overflow gating, pinned client, unpin handover."""

    async def test_overflow_min_pending_gates_delivery(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="OVF", subjects=["ovf.>"], storage=StorageType.MEMORY))
        consumer = await stream.create_consumer(
            ConsumerConfig(
                durable_name="w",
                priority_policy=PriorityPolicy.OVERFLOW,
                priority_groups=["A"],
            )
        )
        # The policy round-trips as a first-class field, not via `extra`.
        assert consumer.cached_info.config.priority_policy is PriorityPolicy.OVERFLOW
        assert consumer.cached_info.config.priority_groups == ["A"]

        for i in range(100):
            await js.publish("ovf.x", b"%d" % i)
        # 100 pending < min_pending 110: the overflow gate stays shut.
        assert await consumer.fetch(10, min_pending=110, group="A", timeout=0.5) == []

        for i in range(100):
            await js.publish("ovf.x", b"%d" % i)
        # 200 pending >= 110: messages flow.
        batch = await consumer.fetch(10, min_pending=110, group="A", timeout=2)
        assert len(batch) == 10
        for msg in batch:
            await msg.ack_sync()

    async def test_overflow_min_ack_pending_gates_delivery(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="OVFACK", subjects=["ovfack.>"], storage=StorageType.MEMORY))
        consumer = await stream.create_consumer(
            ConsumerConfig(
                durable_name="w",
                priority_policy=PriorityPolicy.OVERFLOW,
                priority_groups=["A"],
            )
        )
        for i in range(50):
            await js.publish("ovfack.x", b"%d" % i)

        # Nothing is unacked yet: the min_ack_pending gate stays shut.
        assert await consumer.fetch(10, min_ack_pending=10, group="A", timeout=0.5) == []

        # Take 10 and leave them unacked -> num_ack_pending == 10.
        unacked = await consumer.fetch(10, group="A", timeout=2)
        assert len(unacked) == 10

        # Now the gate opens.
        gated = await consumer.fetch(10, min_ack_pending=10, group="A", timeout=2)
        assert len(gated) == 10
        for msg in (*unacked, *gated):
            await msg.ack_sync()

    async def test_min_pending_requires_overflow_policy(self, nc: natsio.Client) -> None:
        """min_* on a non-overflow consumer is a server-side rejection."""
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="NOOVF", subjects=["noovf.>"], storage=StorageType.MEMORY))
        consumer = await stream.create_consumer(
            ConsumerConfig(
                durable_name="cons",
                priority_policy=PriorityPolicy.PINNED_CLIENT,
                priority_groups=["A"],
                priority_timeout=timedelta(seconds=5),
            )
        )
        await js.publish("noovf.x", b"payload")
        with pytest.raises(JetStreamError, match="Overflow"):
            await consumer.fetch(5, min_pending=1, group="A", timeout=1)

    async def test_pinned_client_starves_second_session(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="PINS", subjects=["pins.>"], storage=StorageType.MEMORY))
        await stream.create_consumer(
            ConsumerConfig(
                durable_name="cons",
                priority_policy=PriorityPolicy.PINNED_CLIENT,
                priority_groups=["A"],
                priority_timeout=timedelta(seconds=30),
            )
        )
        total = 20
        for i in range(total):
            await js.publish("pins.x", b"%d" % i)

        # Two independent client handles compete for the single pin.
        first_handle = await stream.consumer("cons")
        second_handle = await stream.consumer("cons")
        drained: list[bytes] = []

        # Start (and pin) the first session before the second exists.
        async with first_handle.consume(max_messages=5, expires=2, group="A") as pinned:
            first = await pinned.next(timeout=3)
            # Pinned delivery is stamped with the pin id.
            assert first.headers is not None
            assert first.headers.get(js_headers.PIN_ID)
            drained.append(first.payload)
            await first.ack_sync()

            async with second_handle.consume(max_messages=5, expires=2, group="A") as passive:
                # The pinned session drains everything...
                while len(drained) < total:
                    msg = await pinned.next(timeout=3)
                    drained.append(msg.payload)
                    await msg.ack_sync()
                # ...and the passive session, never pinned, starved throughout.
                with pytest.raises(NoMessagesError):
                    await passive.next(timeout=0.5)

        assert len(drained) == total

    async def test_unpin_hands_over_to_second_session(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="UNPIN", subjects=["unpin.>"], storage=StorageType.MEMORY))
        consumer = await stream.create_consumer(
            ConsumerConfig(
                durable_name="cons",
                priority_policy=PriorityPolicy.PINNED_CLIENT,
                priority_groups=["A"],
                priority_timeout=timedelta(seconds=50),
            )
        )
        for i in range(20):
            await js.publish("unpin.x", b"%d" % i)

        first_handle = await stream.consumer("cons")
        second_handle = await stream.consumer("cons")

        async with first_handle.consume(max_messages=5, expires=2, group="A") as pinned:
            first = await pinned.next(timeout=3)
            assert first.headers is not None
            first_pin = first.headers.get(js_headers.PIN_ID)
            assert first_pin
            await first.ack_sync()

            async with second_handle.consume(max_messages=5, expires=2, group="A") as taking_over:
                # Passive while the first session holds the pin.
                with pytest.raises(NoMessagesError):
                    await taking_over.next(timeout=0.5)

                # Release the pin; stop the old session so the takeover is
                # unambiguous, then publish fresh work for the new pinned client.
                await consumer.unpin("A")
                await pinned.stop()
                for i in range(20, 30):
                    await js.publish("unpin.x", b"%d" % i)

                msg = await taking_over.next(timeout=3)
                new_pin = msg.headers.get(js_headers.PIN_ID) if msg.headers else None
                assert new_pin
                assert new_pin != first_pin
                await msg.ack_sync()

    async def test_unpin_unknown_consumer_raises(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="UNPINX", subjects=["unpinx.>"], storage=StorageType.MEMORY))
        consumer = await stream.create_consumer(
            ConsumerConfig(
                durable_name="gone",
                priority_policy=PriorityPolicy.PINNED_CLIENT,
                priority_groups=["A"],
                priority_timeout=timedelta(seconds=5),
            )
        )
        await stream.delete_consumer("gone")
        with pytest.raises(ConsumerNotFoundError):
            await consumer.unpin("A")


class TestPublishAsync:
    async def test_async_publishes_get_sequential_acks(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        await js.create_stream(StreamConfig(name="APUB", subjects=["apub.>"], storage=StorageType.MEMORY))
        futures = [await js.publish_async(f"apub.{i}", b"%d" % i) for i in range(10)]
        await js.publish_async_complete(timeout=5)
        acks = await asyncio.gather(*futures)
        assert [ack.seq for ack in acks] == list(range(1, 11))
        assert js.publish_async_pending == 0

    async def test_async_dedup_by_msg_id(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        await js.create_stream(
            StreamConfig(
                name="ADEDUP",
                subjects=["adedup.>"],
                storage=StorageType.MEMORY,
                duplicate_window=timedelta(seconds=30),
            )
        )
        first = await (await js.publish_async("adedup.a", b"one", msg_id="x"))
        second = await (await js.publish_async("adedup.a", b"one", msg_id="x"))
        assert not first.duplicate
        assert second.duplicate is True
        assert second.seq == first.seq

    async def test_async_wrong_seq_fails_only_that_future(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        await js.create_stream(StreamConfig(name="AEXP", subjects=["aexp.>"], storage=StorageType.MEMORY))
        ack = await (await js.publish_async("aexp.a", b"1"))
        ok_fut = await js.publish_async("aexp.a", b"2", expected_last_seq=ack.seq)
        bad_fut = await js.publish_async("aexp.a", b"3", expected_last_seq=999)
        assert (await ok_fut).seq == 2
        with pytest.raises(WrongLastSequenceError):
            await bad_fut

    async def test_async_complete_drains_a_burst(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        await js.create_stream(StreamConfig(name="ACOMP", subjects=["acomp.>"], storage=StorageType.MEMORY))
        for i in range(50):
            await js.publish_async(f"acomp.{i}")
        await js.publish_async_complete(timeout=5)
        assert js.publish_async_pending == 0

    async def test_window_full_raises_stall(self, server: NatsServerProcess) -> None:
        # A max-pending of 1 means a second concurrent publish can never find
        # room (it is itself the outstanding one), so it stalls out deterministically.
        nc = await natsio.connect(server.url, connect_timeout=5.0, request_timeout=5.0)
        try:
            js = JetStreamContext(nc, publish_async_max_pending=1, publish_async_stall_wait=0.05)
            await js.create_stream(StreamConfig(name="ASTALL", subjects=["astall.>"], storage=StorageType.MEMORY))
            await js.publish_async("astall.a")  # fills the window
            with pytest.raises(TooManyStalledMsgsError):
                await js.publish_async("astall.b")
            await js.publish_async_complete(timeout=5)
        finally:
            await nc.close()

    async def test_close_fails_inflight_future(self, server: NatsServerProcess) -> None:
        """A close mid-flight must fail the outstanding future, not hang it."""
        nc = await natsio.connect(server.url, connect_timeout=5.0, request_timeout=5.0)
        try:
            js = nc.jetstream()
            await js.create_stream(StreamConfig(name="ACLOSE", subjects=["aclose.>"], storage=StorageType.MEMORY))
            # No stream is bound to this subject: the 503 keeps the ack in flight
            # (retrying) so the future is guaranteed pending when we close.
            pending = await js.publish_async("nobody.home")
            assert js.publish_async_pending == 1
            await nc.close()
            with pytest.raises((ConnectionClosedError, NoStreamResponseError)):
                async with asyncio.timeout(5):
                    await pending
        finally:
            await nc.close()


class TestExpectLastSubjectSeqForSubject:
    """2.12 companion header: scope the subject-sequence check to another filter."""

    async def test_wildcard_scoped_check(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        await js.create_stream(StreamConfig(name="SUBJSEQ", subjects=["ss.>"], storage=StorageType.MEMORY))
        await js.publish("ss.a.1", b"1")  # seq 1
        ack2 = await js.publish("ss.a.2", b"2")  # seq 2 (last on filter ss.a.*)

        # Publish to ss.b.1 while asserting the last sequence on the ss.a.* filter.
        ack3 = await js.publish(
            "ss.b.1",
            b"3",
            expected_last_subject_seq=ack2.seq,
            expected_last_subject_seq_subject="ss.a.*",
        )
        assert ack3.seq == 3

        with pytest.raises(WrongLastSequenceError):
            await js.publish(
                "ss.b.2",
                b"4",
                expected_last_subject_seq=99,
                expected_last_subject_seq_subject="ss.a.*",
            )

    async def test_scoped_check_on_async_publish(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        await js.create_stream(StreamConfig(name="SUBJSEQA", subjects=["ssa.>"], storage=StorageType.MEMORY))
        await js.publish("ssa.a.1", b"1")
        ack2 = await js.publish("ssa.a.2", b"2")

        good = await js.publish_async(
            "ssa.b.1",
            b"3",
            expected_last_subject_seq=ack2.seq,
            expected_last_subject_seq_subject="ssa.a.*",
        )
        assert (await good).seq == 3

        bad = await js.publish_async(
            "ssa.b.2",
            b"4",
            expected_last_subject_seq=99,
            expected_last_subject_seq_subject="ssa.a.*",
        )
        with pytest.raises(WrongLastSequenceError):
            await bad


class TestMessageSchedules:
    """2.12 message schedules: publish a schedule-definition message, assert delivery."""

    async def test_scheduled_message_materializes(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(
            StreamConfig(
                name="SCHED",
                subjects=["schedule.>", "target.>"],
                storage=StorageType.MEMORY,
                allow_msg_schedules=True,
            )
        )
        assert stream.cached_info.config.allow_msg_schedules is True
        consumer = await stream.create_consumer(ConsumerConfig(durable_name="w", filter_subject="target.>"))

        sched_time = datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=1)
        schedule = f"@at {sched_time.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        ack = await js.publish(
            "schedule.at",
            b"hello",
            headers={
                js_headers.SCHEDULE: schedule,
                js_headers.SCHEDULE_TARGET: "target.at",
            },
        )

        # The stored schedule-definition message carries the client-set headers.
        stored = await stream.get_msg(sequence=ack.seq)
        assert stored.headers is not None
        assert stored.headers.get(js_headers.SCHEDULE) == schedule
        assert stored.headers.get(js_headers.SCHEDULE_TARGET) == "target.at"

        # The server delivers the generated message to the target subject.
        msg = await consumer.next(timeout=8)
        assert msg.payload == b"hello"
        assert msg.headers is not None
        assert msg.headers.get(js_headers.SCHEDULER)  # server stamps the schedule source
        await msg.ack()
