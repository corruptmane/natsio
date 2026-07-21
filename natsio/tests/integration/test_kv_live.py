"""End-to-end Key-Value tests against a real nats-server with -js."""

import asyncio
from datetime import timedelta

import pytest

import natsio
from natsio.jetstream import StorageType, StreamConfig, WrongLastSequenceError
from natsio.kv import (
    BucketNotFoundError,
    KeyDeletedError,
    KeyExistsError,
    KeyNotFoundError,
    KeyValueConfig,
    KeyValueStatus,
    KvEntry,
    Operation,
)
from natsio.objectstore import ObjectStoreConfig
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


@pytest.fixture
async def kv(nc: natsio.Client):
    js = nc.jetstream()
    return await js.create_key_value(KeyValueConfig(bucket="TEST", history=5))


class TestBucketLifecycle:
    async def test_create_bind_delete(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        created = await js.create_key_value(KeyValueConfig(bucket="LIFE"))
        assert created.bucket == "LIFE"

        bound = await js.key_value("LIFE")
        assert bound.bucket == "LIFE"

        status = await bound.status()
        assert status.bucket == "LIFE"
        assert status.values == 0
        assert status.history == 1
        assert status.ttl is None  # never expires by default

        await js.delete_key_value("LIFE")
        with pytest.raises(BucketNotFoundError):
            await js.key_value("LIFE")

    async def test_no_default_expiry_regression(self, nc: natsio.Client) -> None:
        """The old codebase defaulted buckets to a 120s TTL — data vanished."""
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="NOTTL"))
        info = kv._stream.cached_info
        # The server echoes 0 for "never expires"; either form is fine, a
        # positive default (the old 120s bug) is not.
        assert not info.config.max_age


class TestCrud:
    async def test_put_get_roundtrip(self, kv) -> None:
        revision = await kv.put("theme", b"dark")
        assert revision == 1
        entry = await kv.get("theme")
        assert isinstance(entry, KvEntry)
        assert (entry.key, entry.value, entry.revision) == ("theme", b"dark", 1)
        assert entry.operation is Operation.PUT
        assert entry.created is not None

    async def test_get_missing_key(self, kv) -> None:
        with pytest.raises(KeyNotFoundError):
            await kv.get("nope")

    async def test_get_by_revision(self, kv) -> None:
        await kv.put("k", b"one")
        await kv.put("k", b"two")
        assert (await kv.get("k", revision=1)).value == b"one"
        assert (await kv.get("k")).value == b"two"

    async def test_get_revision_of_other_key_rejected(self, kv) -> None:
        await kv.put("a", b"1")
        await kv.put("b", b"2")
        with pytest.raises(KeyNotFoundError):
            await kv.get("a", revision=2)  # revision 2 belongs to "b"

    async def test_update_compare_and_set(self, kv) -> None:
        first = await kv.put("cas", b"v1")
        second = await kv.update("cas", b"v2", last=first)
        assert second == first + 1
        with pytest.raises(WrongLastSequenceError):
            await kv.update("cas", b"v3", last=first)

    async def test_create_semantics(self, kv) -> None:
        revision = await kv.create("fresh", b"x")
        assert revision >= 1
        with pytest.raises(KeyExistsError) as excinfo:
            await kv.create("fresh", b"y")
        assert excinfo.value.revision == revision

    async def test_create_after_delete_succeeds(self, kv) -> None:
        """The classic bug: create() must CAS against the MARKER's revision."""
        await kv.put("reborn", b"first-life")
        await kv.delete("reborn")
        revision = await kv.create("reborn", b"second-life")
        assert (await kv.get("reborn")).value == b"second-life"
        assert revision > 2

    async def test_delete_preserves_history(self, kv) -> None:
        await kv.put("d", b"v1")
        await kv.put("d", b"v2")
        await kv.delete("d")
        with pytest.raises(KeyDeletedError) as excinfo:
            await kv.get("d")
        assert excinfo.value.revision == 3
        # History still holds the values plus the marker.
        entries = await kv.history("d")
        assert [e.operation for e in entries] == [Operation.PUT, Operation.PUT, Operation.DELETE]
        assert entries[0].value == b"v1"

    async def test_delete_with_last_cas(self, kv) -> None:
        revision = await kv.put("dc", b"v")
        with pytest.raises(WrongLastSequenceError):
            await kv.delete("dc", last=revision + 5)
        await kv.delete("dc", last=revision)

    async def test_purge_removes_history(self, kv) -> None:
        await kv.put("p", b"v1")
        await kv.put("p", b"v2")
        await kv.purge("p")
        with pytest.raises(KeyDeletedError):
            await kv.get("p")
        entries = await kv.history("p")
        assert len(entries) == 1  # only the purge marker survives
        assert entries[0].operation is Operation.PURGE

    async def test_history_limit_enforced(self, kv) -> None:
        for i in range(8):
            await kv.put("h", b"%d" % i)
        entries = await kv.history("h")
        assert len(entries) == 5  # bucket history=5
        assert entries[-1].value == b"7"

    async def test_string_values_accepted(self, kv) -> None:
        await kv.put("s", "text-value")
        assert (await kv.get("s")).value == b"text-value"


class TestKeys:
    async def test_keys_lists_live_keys_only(self, kv) -> None:
        await kv.put("alive", b"1")
        await kv.put("dead", b"1")
        await kv.delete("dead")
        keys = await kv.keys()
        assert keys == ["alive"]

    async def test_keys_on_empty_bucket_returns_promptly(self, kv) -> None:
        """The old codebase deadlocked forever here."""
        async with asyncio.timeout(5):
            assert await kv.keys() == []


class TestWatch:
    async def test_initial_state_then_updates(self, kv) -> None:
        await kv.put("w1", b"a")
        await kv.put("w2", b"b")

        seen: list[KvEntry | None] = []
        async with kv.watch() as watcher:
            iterator = aiter(watcher)
            async with asyncio.timeout(5):
                while True:
                    item = await anext(iterator)
                    seen.append(item)
                    if item is None:
                        break
            assert {e.key for e in seen if e is not None} == {"w1", "w2"}

            await kv.put("w3", b"c")
            async with asyncio.timeout(5):
                live = await anext(iterator)
            assert live is not None
            assert (live.key, live.value) == ("w3", b"c")

    async def test_empty_bucket_yields_marker_immediately(self, kv) -> None:
        async with kv.watch() as watcher:
            async with asyncio.timeout(5):
                first = await anext(aiter(watcher))
            assert first is None

    async def test_updates_only_skips_existing(self, kv) -> None:
        await kv.put("old", b"x")
        async with kv.watch(updates_only=True) as watcher:
            iterator = aiter(watcher)
            async with asyncio.timeout(5):
                assert await anext(iterator) is None  # immediately caught up
            await kv.put("new", b"y")
            async with asyncio.timeout(5):
                entry = await anext(iterator)
            assert entry is not None
            assert entry.key == "new"

    async def test_watch_single_key_sees_deletes(self, kv) -> None:
        await kv.put("target", b"v")
        async with kv.watch("target") as watcher:
            iterator = aiter(watcher)
            async with asyncio.timeout(5):
                first = await anext(iterator)
            assert first is not None
            assert first.key == "target"
            assert await anext(iterator) is None

            await kv.delete("target")
            async with asyncio.timeout(5):
                marker = await anext(iterator)
            assert marker is not None
            assert marker.operation is Operation.DELETE


class TestCodecs:
    async def test_value_codec_round_trip(self, nc: natsio.Client) -> None:
        import zlib

        class Zlib:
            def encode(self, value: bytes) -> bytes:
                return zlib.compress(value)

            def decode(self, value: bytes) -> bytes:
                return zlib.decompress(value)

        js = nc.jetstream()
        await js.create_key_value(KeyValueConfig(bucket="CODEC"))
        kv = await js.key_value("CODEC", value_codec=Zlib())
        payload = b"compress me " * 100
        await kv.put("big", payload)
        assert (await kv.get("big")).value == payload

        # The stored bytes really are transformed (a codec-less handle sees them).
        raw = await js.key_value("CODEC")
        stored = (await raw._get_any("big")).value
        assert stored != payload
        assert len(stored) < len(payload)

    async def test_key_codec_round_trip(self, nc: natsio.Client) -> None:
        class Prefixed:
            def encode(self, key: str) -> str:
                return f"enc.{key}"

            def decode(self, key: str) -> str:
                return key.removeprefix("enc.")

        js = nc.jetstream()
        await js.create_key_value(KeyValueConfig(bucket="KCODEC"))
        kv = await js.key_value("KCODEC", key_codec=Prefixed())
        await kv.put("name", b"v")
        assert (await kv.get("name")).value == b"v"
        assert await kv.keys() == ["name"]

        raw = await js.key_value("KCODEC")
        assert await raw.keys() == ["enc.name"]


class TestPerKeyTTL:
    async def test_purge_marker_with_ttl_self_expires(self, nc: natsio.Client) -> None:
        """ADR-48: a purge marker carrying a TTL cleans itself up."""
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="MARKERS", limit_marker_ttl=timedelta(seconds=30)))
        await kv.put("gone", b"v")
        await kv.purge("gone", ttl=1)
        assert (await kv.status()).values == 1  # the marker
        await asyncio.sleep(2.0)
        assert (await kv.status()).values == 0  # marker expired away


class TestReviewRegressions:
    async def test_status_reports_unlimited_history_for_foreign_bucket(self, nc: natsio.Client) -> None:
        """Finding: max(1, -1) masked unlimited-history foreign buckets as 1."""
        from natsio.jetstream import StreamConfig as ForeignStreamConfig

        js = nc.jetstream()
        await js.create_stream(
            ForeignStreamConfig(
                name="KV_FOREIGN", subjects=["$KV.FOREIGN.>"], max_msgs_per_subject=-1, allow_direct=True
            )
        )
        kv = await js.key_value("FOREIGN")
        assert (await kv.status()).history == -1

    async def test_sub_100ms_ttl_rejected_client_side(self) -> None:
        from natsio.errors import ConfigError

        with pytest.raises(ConfigError, match="100ms"):
            KeyValueConfig(bucket="b", ttl=timedelta(milliseconds=50))

    async def test_purge_ttl_without_allow_msg_ttl_is_typed_error(self, kv) -> None:
        """Finding: was a raw APIError from the server."""
        from natsio.errors import ConfigError

        await kv.put("k", b"v")
        with pytest.raises(ConfigError, match="allow_msg_ttl"):
            await kv.purge("k", ttl=1)

    async def test_allow_msg_ttl_flag_decoupled_from_limit_markers(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="MSGTTL", allow_msg_ttl=True))
        await kv.put("k", b"v")
        await kv.purge("k", ttl=1)  # no limit_marker_ttl needed
        await asyncio.sleep(2.0)
        assert (await kv.status()).values == 0

    async def test_create_key_value_idempotent_and_conflicting(self, nc: natsio.Client) -> None:
        from natsio.kv import BucketExistsError

        js = nc.jetstream()
        config = KeyValueConfig(bucket="IDEM", history=3)
        await js.create_key_value(config)
        again = await js.create_key_value(config)  # identical: idempotent
        assert again.bucket == "IDEM"
        with pytest.raises(BucketExistsError):
            await js.create_key_value(KeyValueConfig(bucket="IDEM", history=7))

    async def test_wildcard_watch_with_key_codec_refused(self, nc: natsio.Client) -> None:
        from natsio.errors import ConfigError

        class Codec:
            def encode(self, key: str) -> str:
                return f"e.{key}"

            def decode(self, key: str) -> str:
                return key.removeprefix("e.")

        js = nc.jetstream()
        await js.create_key_value(KeyValueConfig(bucket="WCODEC"))
        kv = await js.key_value("WCODEC", key_codec=Codec())
        with pytest.raises(ConfigError, match="wildcard"):
            kv.watch("a.*")
        kv.watch(">")  # whole-bucket watch stays allowed

    async def test_stopped_watcher_does_not_resurrect_consumer(self, kv) -> None:
        """Finding: iterating after stop() rebuilt server-side state."""
        watcher = kv.watch()
        await watcher.stop()
        got = [item async for item in watcher]
        assert got == []
        names = [name async for name in kv._stream.consumer_names()]
        assert names == []

    async def test_iter_keys_streams(self, kv) -> None:
        for i in range(5):
            await kv.put(f"k{i}", b"v")
        collected = [key async for key in kv.iter_keys()]
        assert sorted(collected) == [f"k{i}" for i in range(5)]

    async def test_bind_over_mis_subjected_kv_stream_rejected(self, nc: natsio.Client) -> None:
        """Finding: binding to a KV_-named stream whose subjects don't cover the
        bucket keyspace bound fine, then failed confusingly at write time."""
        from natsio.jetstream import StreamConfig as ForeignStreamConfig

        js = nc.jetstream()
        await js.create_stream(ForeignStreamConfig(name="KV_BADBUCKET", subjects=["unrelated.foo"], allow_direct=True))
        with pytest.raises(BucketNotFoundError, match="not a Key-Value bucket"):
            await js.key_value("BADBUCKET")

    async def test_snapshot_survives_mid_delivery_self_heal(self, kv) -> None:
        """Finding (major): a heal during the initial snapshot duplicated keys
        and resurfaced stale values. Force a heal by deleting the ephemeral
        consumer right after the watch starts."""
        for i in range(50):
            await kv.put(f"key.{i}", b"old")
        await kv.put("key.0", b"new")  # key.0 latest is "new"

        seen: dict[str, bytes] = {}
        duplicates: list[str] = []
        async with kv.watch() as watcher:
            iterator = aiter(watcher)
            first = await anext(iterator)
            assert first is not None
            seen[first.key] = first.value
            # Sabotage: kill the consumer mid-snapshot; the watcher must heal
            # without duplicating or resurfacing stale revisions.
            assert watcher._ordered._consumer is not None
            await kv._stream.delete_consumer(watcher._ordered._consumer.name)
            async with asyncio.timeout(20):
                async for item in iterator:
                    if item is None:
                        break
                    if item.key in seen:
                        duplicates.append(item.key)
                    seen[item.key] = item.value
        assert duplicates == []
        assert len(seen) == 50
        assert seen["key.0"] == b"new"


async def _fill(kv, count: int) -> None:
    for start in range(0, count, 200):
        await asyncio.gather(*(kv.put(f"key-{i}", b"v") for i in range(start, min(start + 200, count))))


class TestPurgedStreamSnapshot:
    """nats.go TestListKeysFromPurgedStream: a purge (or emptying) mid-snapshot
    must not deadlock keys()/watch — the initial snapshot is idle-bounded."""

    async def test_keys_race_stream_purge_does_not_hang(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        for delay in (0.001, 0.005, 0.01):
            kv = await js.create_key_value(KeyValueConfig(bucket=f"PURGED{int(delay * 1000)}"))
            await _fill(kv, 2000)

            async def purge_later(stream=kv._stream, d=delay) -> None:
                await asyncio.sleep(d)
                await stream.purge()

            task = asyncio.create_task(purge_later())
            try:
                async with asyncio.timeout(20):
                    keys = await kv.keys()
            finally:
                await task
            # Bounded return — empty or partial — instead of the old forever-hang.
            assert isinstance(keys, list)

    async def test_plain_watcher_marker_arrives_after_purge(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="PURGEDWATCH"))
        await _fill(kv, 1000)

        async def purge_later(stream=kv._stream) -> None:
            await asyncio.sleep(0.002)
            await stream.purge()

        task = asyncio.create_task(purge_later())
        saw_marker = False
        try:
            async with asyncio.timeout(20), kv.watch() as watcher:
                async for entry in watcher:
                    if entry is None:
                        saw_marker = True
                        break
        finally:
            await task
        assert saw_marker


class TestNonDirectGet:
    async def test_get_falls_back_to_stream_msg_get(self, kv) -> None:
        """nats.go TestKeyValueNonDirectGet: get() still works when the backing
        stream disallows Direct Get (falls back to STREAM.MSG.GET)."""
        await kv.put("k", b"one")
        await kv.put("k", b"two")

        config = kv._stream.cached_info.config
        config.allow_direct = False
        await kv._ctx.update_stream(config)
        await kv._stream.info()  # refresh cached_info so get_msg picks the API path
        assert kv._stream.cached_info.config.allow_direct is False

        assert (await kv.get("k")).value == b"two"
        assert (await kv.get("k", revision=1)).value == b"one"


class TestHistoryExactness:
    async def test_history_returns_exact_last_revisions(self, nc: natsio.Client) -> None:
        """nats.go TestKeyValueHistory: history() returns exactly the last
        `history` revisions, oldest first, with correct values."""
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="HIST10", history=10))
        for i in range(1, 51):
            await kv.put("k", b"v%d" % i)
        entries = await kv.history("k")
        assert [e.revision for e in entries] == list(range(41, 51))
        assert [e.value for e in entries] == [b"v%d" % i for i in range(41, 51)]


class TestWildcardWatch:
    async def test_wildcard_subset_initial_then_live(self, nc: natsio.Client) -> None:
        """nats.go TestKeyValueWatch default wildcard watcher: watch('a.*')
        yields only the a-keys in the initial state, then only live a-updates."""
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="WSUB"))
        await kv.put("a.1", b"1")
        await kv.put("a.2", b"2")
        await kv.put("b.1", b"3")

        async with kv.watch("a.*") as watcher:
            iterator = aiter(watcher)
            initial: dict[str, bytes] = {}
            async with asyncio.timeout(5):
                while True:
                    item = await anext(iterator)
                    if item is None:
                        break
                    initial[item.key] = item.value
            assert initial == {"a.1": b"1", "a.2": b"2"}

            await kv.put("b.2", b"4")  # outside the filter: must not arrive
            await kv.put("a.3", b"5")
            async with asyncio.timeout(5):
                live = await anext(iterator)
            assert live is not None
            assert (live.key, live.value) == ("a.3", b"5")

    async def test_meta_only_watch_omits_values(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="METAW"))
        await kv.put("m", b"secret-value")
        async with kv.watch(meta_only=True) as watcher:
            iterator = aiter(watcher)
            async with asyncio.timeout(5):
                entry = await anext(iterator)
            assert entry is not None
            assert (entry.key, entry.revision, entry.operation) == ("m", 1, Operation.PUT)
            assert entry.value == b""
            async with asyncio.timeout(5):
                assert await anext(iterator) is None


class TestPerKeyTTLWrites:
    async def test_put_with_ttl_expires(self, nc: natsio.Client) -> None:
        """nats.go KeyTTL: a put carrying a TTL self-expires."""
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="PUTTTL", limit_marker_ttl=timedelta(seconds=30)))
        await kv.put("k", b"v", ttl=1)
        assert (await kv.get("k")).value == b"v"
        await asyncio.sleep(2.0)
        with pytest.raises(KeyNotFoundError):
            await kv.get("k")

    async def test_create_with_ttl_then_recreate_after_expiry(self, nc: natsio.Client) -> None:
        """nats.go TestKeyValueLimitMarkerTTL 'create with TTL': after the TTL'd
        value expires the key is gone and create() succeeds for it again."""
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="CREATETTL", limit_marker_ttl=timedelta(seconds=30)))
        await kv.create("age", b"22", ttl=1)
        assert (await kv.get("age")).value == b"22"
        await asyncio.sleep(2.0)
        with pytest.raises(KeyNotFoundError):
            await kv.get("age")
        # The key expired (a MaxAge marker sits where the value was); create must
        # CAS against that marker and succeed.
        revision = await kv.create("age", b"33")
        assert revision > 1
        assert (await kv.get("age")).value == b"33"

    async def test_put_ttl_without_allow_msg_ttl_rejected(self, kv) -> None:
        from natsio.errors import ConfigError

        with pytest.raises(ConfigError, match="per-message TTL"):
            await kv.put("k", b"v", ttl=1)

    async def test_create_ttl_without_allow_msg_ttl_rejected(self, kv) -> None:
        from natsio.errors import ConfigError

        with pytest.raises(ConfigError, match="per-message TTL"):
            await kv.create("k", b"v", ttl=1)


class TestPurgeCas:
    async def test_purge_with_stale_last_rejected_correct_succeeds(self, kv) -> None:
        """nats.go LastRevision on Purge: a stale expected revision conflicts."""
        first = await kv.put("p", b"v1")
        await kv.put("p", b"v2")
        with pytest.raises(WrongLastSequenceError):
            await kv.purge("p", last=first)  # stale
        current = (await kv.get("p")).revision
        await kv.purge("p", last=current)
        with pytest.raises(KeyDeletedError):
            await kv.get("p")
        entries = await kv.history("p")
        assert len(entries) == 1  # only the purge marker survives
        assert entries[0].operation is Operation.PURGE


class TestPurgeDeletes:
    async def test_removes_markers_drops_count_keeps_live(self, nc: natsio.Client) -> None:
        """nats.go TestKeyValueDeleteTombstones: purge_deletes(timedelta(0))
        removes every marker (and its history); live keys are untouched."""
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="TOMBS", history=10))
        for i in range(1, 6):
            await kv.put(f"key-{i}", b"v")
        for i in range(1, 6):
            await kv.delete(f"key-{i}")
        await kv.put("alive", b"still-here")

        before = (await kv.status()).values
        await kv.purge_deletes(older_than=timedelta(0))  # remove all markers
        after = (await kv.status()).values
        assert after < before
        for i in range(1, 6):
            with pytest.raises(KeyNotFoundError):
                await kv.history(f"key-{i}")  # marker and history both gone
        assert (await kv.get("alive")).value == b"still-here"

    async def test_fresh_markers_kept_with_default_threshold(self, nc: natsio.Client) -> None:
        """Default 30-minute threshold: a just-written marker survives, but the
        history beneath it is rolled away (keep=1)."""
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="TOMBSFRESH", history=10))
        await kv.put("foo", b"foo1")
        await kv.put("foo", b"foo2")
        await kv.delete("foo")

        await kv.purge_deletes()  # default older_than -> 30 minutes
        entries = await kv.history("foo")
        assert len(entries) == 1
        assert entries[0].operation is Operation.DELETE

    async def test_marker_threshold_mixed_ages(self, nc: natsio.Client) -> None:
        """nats.go TestKeyValuePurgeDeletesMarkerThreshold: an older marker is
        fully removed while a fresher one is kept."""
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="TOMBSMIX", history=10))
        await kv.put("foo", b"foo1")
        await kv.put("bar", b"bar1")
        await kv.put("foo", b"foo2")
        await kv.delete("foo")
        await asyncio.sleep(0.2)
        await kv.delete("bar")

        await kv.purge_deletes(older_than=timedelta(milliseconds=100))
        with pytest.raises(KeyNotFoundError):
            await kv.history("foo")  # older than threshold: gone entirely
        bar = await kv.history("bar")  # fresher: marker kept
        assert len(bar) == 1
        assert bar[0].operation is Operation.DELETE


class TestMultiKeyWatch:
    async def test_multi_filter_initial_then_live(self, nc: natsio.Client) -> None:
        """nats.go WatchFiltered: several filter subjects yield only the matching
        keys in the initial state, then only matching live updates."""
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="MULTIW"))
        await kv.put("name", b"ik")
        await kv.put("t.name", b"ik")
        await kv.put("age", b"44")  # outside both filters

        async with kv.watch("name", "t.name") as watcher:
            iterator = aiter(watcher)
            initial: dict[str, bytes] = {}
            async with asyncio.timeout(5):
                while True:
                    item = await anext(iterator)
                    if item is None:
                        break
                    initial[item.key] = item.value
            assert initial == {"name": b"ik", "t.name": b"ik"}

            await kv.put("age", b"45")  # excluded: must not arrive
            await kv.put("t.name", b"new")
            async with asyncio.timeout(5):
                live = await anext(iterator)
            assert live is not None
            assert (live.key, live.value) == ("t.name", b"new")

    async def test_no_keys_watches_whole_bucket(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="ALLW"))
        await kv.put("a", b"1")
        await kv.put("b", b"2")
        seen: set[str] = set()
        async with kv.watch() as watcher:  # zero keys -> ">"
            async with asyncio.timeout(5):
                async for entry in watcher:
                    if entry is None:
                        break
                    seen.add(entry.key)
        assert seen == {"a", "b"}


class TestResumeFromRevision:
    async def test_resume_delivers_from_revision_onward(self, nc: natsio.Client) -> None:
        """nats.go 'watcher with start revision': resume_from_revision replays
        every revision from that stream sequence onward."""
        js = nc.jetstream()
        kv = await js.create_key_value(KeyValueConfig(bucket="RESUME"))
        for i in range(1, 6):
            await kv.put(f"k{i}", b"v")  # distinct keys -> revisions 1..5 all kept

        seen: list[int] = []
        async with kv.watch(resume_from_revision=3) as watcher:
            async with asyncio.timeout(5):
                async for entry in watcher:
                    if entry is None:
                        break
                    seen.append(entry.revision)
        assert seen == [3, 4, 5]

    async def test_resume_conflicts_rejected(self, kv) -> None:
        from natsio.errors import ConfigError

        with pytest.raises(ConfigError, match="mutually exclusive"):
            kv.watch(resume_from_revision=1, include_history=True)
        with pytest.raises(ConfigError, match="mutually exclusive"):
            kv.watch(resume_from_revision=1, updates_only=True)


class TestStoreManagement:
    async def test_update_reflects_new_config(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        await js.create_key_value(KeyValueConfig(bucket="UPD", description="Test KV", history=1))
        updated = await js.update_key_value(KeyValueConfig(bucket="UPD", description="New KV", history=5))
        status = await updated.status()
        assert status.description == "New KV"
        assert status.history == 5

    async def test_update_missing_bucket_raises(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        with pytest.raises(BucketNotFoundError):
            await js.update_key_value(KeyValueConfig(bucket="NOPE", description="x"))

    async def test_create_or_update_creates_then_updates(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        # create path (backing stream absent -> create)
        created = await js.create_or_update_key_value(KeyValueConfig(bucket="COU", description="first"))
        assert (await created.status()).description == "first"
        # update path (backing stream present -> update in place)
        updated = await js.create_or_update_key_value(KeyValueConfig(bucket="COU", description="second"))
        assert (await updated.status()).description == "second"

    async def test_listings_return_only_kv_buckets(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        # KV_ prefix but no $KV subject -> excluded by the $KV.*.> subject filter.
        await js.create_stream(StreamConfig(name="KV_FOO", subjects=["foo.*"], storage=StorageType.MEMORY))
        # $KV subject but no KV_ prefix -> excluded by the client-side prefix check.
        await js.create_stream(StreamConfig(name="PLAIN", subjects=["$KV.ABC.>"], storage=StorageType.MEMORY))
        # An object store shares neither prefix nor keyspace -> excluded.
        await js.create_object_store(ObjectStoreConfig(bucket="OSX"))
        await js.create_key_value(KeyValueConfig(bucket="KVS1"))
        await js.create_key_value(KeyValueConfig(bucket="KVS2"))

        names = {n async for n in js.key_value_store_names()}
        assert names == {"KVS1", "KVS2"}

        statuses = {s.bucket: s async for s in js.key_value_stores()}
        assert set(statuses) == {"KVS1", "KVS2"}
        assert all(isinstance(s, KeyValueStatus) for s in statuses.values())
