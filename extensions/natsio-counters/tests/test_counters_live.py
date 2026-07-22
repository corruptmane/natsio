"""End-to-end counter tests against a real nats-server (pinned 2.14) with -js."""

import asyncio

import pytest
from natsio.counters import (  # ty: ignore[unresolved-import]
    Counter,
    CounterConfig,
    CounterEntry,
    CounterNotEnabledError,
    CounterNotFoundError,
    CounterSubjectNotInitializedError,
    InvalidCounterValueError,
    counter_from_stream,
    create_counter,
    get_counter,
)

import natsio
from conftest import NatsServerProcess, free_port  # ty: ignore[unresolved-import]
from natsio.jetstream import StorageType, StreamConfig


@pytest.fixture
async def counter(nc: natsio.Client) -> Counter:
    js = nc.jetstream()
    return await create_counter(js, CounterConfig(name="COUNTS", subjects=["events.>"]))


class TestLifecycle:
    async def test_create_bind(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        created = await create_counter(js, CounterConfig(name="LIFE", subjects=["a.>"]))
        assert created.name == "LIFE"

        bound = await get_counter(js, "LIFE")
        assert bound.name == "LIFE"
        config = bound._stream.cached_info.config
        assert config.allow_msg_counter is True
        assert config.allow_direct is True

    async def test_get_missing_counter(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        with pytest.raises(CounterNotFoundError):
            await get_counter(js, "NOPE")

    async def test_bind_plain_stream_rejected(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        await js.create_stream(StreamConfig(name="PLAIN", subjects=["p.>"], allow_direct=True))
        with pytest.raises(CounterNotEnabledError):
            await get_counter(js, "PLAIN")

    async def test_counter_from_stream(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(
            StreamConfig(name="FROMSTREAM", subjects=["f.>"], allow_msg_counter=True, allow_direct=True)
        )
        counter = counter_from_stream(js, stream)
        assert await counter.add("f.one", 7) == 7


class TestAdd:
    async def test_add_returns_running_total(self, counter: Counter) -> None:
        assert await counter.add("events.orders", 1) == 1
        assert await counter.add("events.orders", 10) == 11
        assert await counter.add("events.orders", 100) == 111

    async def test_negative_delta(self, counter: Counter) -> None:
        assert await counter.add("events.stock", 50) == 50
        assert await counter.add("events.stock", -20) == 30
        assert await counter.add("events.stock", -30) == 0
        assert await counter.add("events.stock", -5) == -5

    async def test_zero_delta_initializes(self, counter: Counter) -> None:
        assert await counter.add("events.zero", 0) == 0
        assert await counter.load("events.zero") == 0

    async def test_arbitrary_precision(self, counter: Counter) -> None:
        big = 2**80 + 12345
        assert await counter.add("events.big", big) == big
        assert await counter.add("events.big", 1) == big + 1

    async def test_add_rejects_non_int(self, counter: Counter) -> None:
        with pytest.raises(InvalidCounterValueError):
            await counter.add("events.bad", "5")

    async def test_add_rejects_bool(self, counter: Counter) -> None:
        # bool is an int subclass; adding True silently would be a foot-gun.
        with pytest.raises(InvalidCounterValueError):
            await counter.add("events.bad", True)


class TestReads:
    async def test_load(self, counter: Counter) -> None:
        await counter.add("events.a", 5)
        await counter.add("events.a", 3)
        assert await counter.load("events.a") == 8

    async def test_load_uninitialized(self, counter: Counter) -> None:
        with pytest.raises(CounterSubjectNotInitializedError):
            await counter.load("events.never")

    async def test_get_entry(self, counter: Counter) -> None:
        await counter.add("events.g", 4)
        await counter.add("events.g", 6)
        entry = await counter.get("events.g")
        assert isinstance(entry, CounterEntry)
        assert entry.subject == "events.g"
        assert entry.value == 10
        assert entry.incr == 6  # most recent increment
        assert entry.sources is None  # single-stream counter, no aggregation

    async def test_get_uninitialized(self, counter: Counter) -> None:
        with pytest.raises(CounterSubjectNotInitializedError):
            await counter.get("events.never")


class TestCrossSubject:
    async def test_independent_counters_in_one_stream(self, counter: Counter) -> None:
        await counter.add("events.orders", 3)
        await counter.add("events.clicks", 7)
        await counter.add("events.orders", 4)
        assert await counter.load("events.orders") == 7
        assert await counter.load("events.clicks") == 7

    async def test_get_multiple_wildcard(self, counter: Counter) -> None:
        await counter.add("events.orders", 1)
        await counter.add("events.clicks", 2)
        await counter.add("events.views", 3)
        found = {e.subject: e.value async for e in counter.get_multiple(["events.>"])}
        assert found == {"events.orders": 1, "events.clicks": 2, "events.views": 3}

    async def test_get_multiple_explicit_list(self, counter: Counter) -> None:
        await counter.add("events.orders", 10)
        await counter.add("events.clicks", 20)
        await counter.add("events.views", 30)
        found = {e.subject: e.value async for e in counter.get_multiple(["events.orders", "events.views"])}
        assert found == {"events.orders": 10, "events.views": 30}

    async def test_get_multiple_empty(self, counter: Counter) -> None:
        assert [e async for e in counter.get_multiple([])] == []


class TestConcurrency:
    async def test_concurrent_adds_from_two_clients_sum(self, server: NatsServerProcess) -> None:
        client_a = await natsio.connect(server.url, request_timeout=5.0)
        client_b = await natsio.connect(server.url, request_timeout=5.0)
        try:
            js_a = client_a.jetstream()
            await create_counter(js_a, CounterConfig(name="CONC", subjects=["hits.>"]))
            counter_a = await get_counter(js_a, "CONC")
            counter_b = await get_counter(client_b.jetstream(), "CONC")

            per_client = 100

            async def hammer(c: Counter) -> None:
                for _ in range(per_client):
                    await c.add("hits.page", 1)

            await asyncio.gather(hammer(counter_a), hammer(counter_b))
            assert await counter_a.load("hits.page") == 2 * per_client
        finally:
            await client_a.close()
            await client_b.close()


class TestDurability:
    async def test_value_survives_server_restart(self, server_binary: str, tmp_path) -> None:
        port = free_port()
        store = tmp_path / "js"

        first = NatsServerProcess(server_binary, port=port, jetstream=True, store_dir=store)
        await first.start()
        client = await natsio.connect(first.url, request_timeout=5.0)
        counter = await create_counter(
            client.jetstream(),
            CounterConfig(name="DURABLE", subjects=["d.>"], storage=StorageType.FILE),
        )
        await counter.add("d.total", 41)
        assert await counter.add("d.total", 1) == 42
        await client.close()
        await first.stop()

        second = NatsServerProcess(server_binary, port=port, jetstream=True, store_dir=store)
        await second.start()
        try:
            client = await natsio.connect(second.url, request_timeout=5.0)
            counter = await get_counter(client.jetstream(), "DURABLE")
            assert await counter.load("d.total") == 42
            assert await counter.add("d.total", 8) == 50  # accumulation continues
            await client.close()
        finally:
            await second.stop()
