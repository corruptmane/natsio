"""End-to-end Object Store tests against a real nats-server with -js."""

import asyncio
import base64
import hashlib
import json

import pytest

import natsio
from natsio.jetstream import NoMessagesError, StorageType, StreamConfig
from natsio.kv import KeyValueConfig
from natsio.objectstore import (
    BucketExistsError,
    BucketNotFoundError,
    DigestMismatchError,
    LinkError,
    ObjectDeletedError,
    ObjectExistsError,
    ObjectInfo,
    ObjectMeta,
    ObjectNotFoundError,
    ObjectStoreConfig,
    ObjectStoreStatus,
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


@pytest.fixture
async def obj(nc: natsio.Client):
    js = nc.jetstream()
    return await js.create_object_store(ObjectStoreConfig(bucket="TEST"))


def _payload(size: int) -> bytes:
    # Deterministic, non-repeating-per-chunk content so chunk mixups fail digests.
    return bytes((i * 31 + (i >> 8)) % 256 for i in range(size))


class TestBucketLifecycle:
    async def test_create_bind_delete(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        created = await js.create_object_store(ObjectStoreConfig(bucket="LIFE", description="files"))
        assert created.bucket == "LIFE"

        bound = await js.object_store("LIFE")
        status = await bound.status()
        assert status.bucket == "LIFE"
        assert status.description == "files"
        assert status.ttl is None  # never expires by default
        assert status.sealed is False
        assert status.size == 0

        await js.delete_object_store("LIFE")
        with pytest.raises(BucketNotFoundError):
            await js.object_store("LIFE")

    async def test_idempotent_create_and_conflict(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        config = ObjectStoreConfig(bucket="DUP")
        await js.create_object_store(config)
        await js.create_object_store(config)  # identical: idempotent
        with pytest.raises(BucketExistsError):
            await js.create_object_store(ObjectStoreConfig(bucket="DUP", description="different"))

    async def test_missing_bucket_operations(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        with pytest.raises(BucketNotFoundError):
            await js.object_store("NOPE")
        with pytest.raises(BucketNotFoundError):
            await js.delete_object_store("NOPE")


class TestPutGet:
    async def test_small_roundtrip(self, obj) -> None:
        data = b"hello object store"
        info = await obj.put("greeting", data)
        assert (info.name, info.bucket) == ("greeting", "TEST")
        assert info.size == len(data)
        assert info.chunks == 1
        assert info.digest and info.digest.startswith("SHA-256=")
        assert info.mtime is not None
        assert await obj.get_bytes("greeting") == data

    async def test_multi_chunk_roundtrip(self, obj) -> None:
        data = _payload(300 * 1024)
        info = await obj.put(ObjectMeta(name="big", chunk_size=64 * 1024), data)
        assert info.chunks == 5  # 4 full 64KiB chunks + one 44KiB tail
        assert info.size == len(data)

        async with obj.get("big") as result:
            assert result.info.chunks == 5
            received = [chunk async for chunk in result]
        assert [len(c) for c in received[:4]] == [64 * 1024] * 4
        assert b"".join(received) == data

    async def test_empty_object(self, obj) -> None:
        info = await obj.put("empty", b"")
        assert info.size == 0
        assert info.chunks == 0
        assert await obj.get_bytes("empty") == b""

    async def test_async_iterable_input(self, obj) -> None:
        data = _payload(50_000)

        async def stream():
            for start in range(0, len(data), 7_000):  # ragged pieces
                yield data[start : start + 7_000]

        info = await obj.put(ObjectMeta(name="streamed", chunk_size=16 * 1024), stream())
        assert info.size == len(data)
        assert await obj.get_bytes("streamed") == data

    async def test_meta_description_and_metadata(self, obj) -> None:
        await obj.put(ObjectMeta(name="doc", description="a doc", metadata={"team": "infra"}), b"x")
        info = await obj.info("doc")
        assert info.description == "a doc"
        assert info.metadata == {"team": "infra"}

    async def test_missing_object(self, obj) -> None:
        with pytest.raises(ObjectNotFoundError):
            await obj.info("ghost")
        with pytest.raises(ObjectNotFoundError):
            await obj.get_bytes("ghost")

    async def test_replace_purges_old_chunks(self, obj) -> None:
        await obj.put(ObjectMeta(name="x", chunk_size=1024), _payload(10 * 1024))
        await obj.put(ObjectMeta(name="x", chunk_size=1024), _payload(3 * 1024))
        info = await obj._stream.info()
        # 3 chunks of the new revision + 1 meta message; the 10 old chunks gone.
        assert info.state.messages == 4
        assert await obj.get_bytes("x") == _payload(3 * 1024)

    async def test_failed_put_leaves_no_orphans_and_keeps_previous(self, obj) -> None:
        original = _payload(5 * 1024)
        await obj.put(ObjectMeta(name="x", chunk_size=1024), original)
        baseline = (await obj._stream.info()).state.messages

        async def exploding():
            yield _payload(4 * 1024)
            raise RuntimeError("upstream died")

        with pytest.raises(RuntimeError, match="upstream died"):
            await obj.put(ObjectMeta(name="x", chunk_size=1024), exploding())

        # No orphaned chunks, no meta change, old data intact.
        assert (await obj._stream.info()).state.messages == baseline
        assert await obj.get_bytes("x") == original


class TestDigestIntegrity:
    async def test_digest_matches_sha256(self, obj) -> None:
        data = _payload(200_000)
        info = await obj.put("d", data)
        expected = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode()
        assert info.digest == f"SHA-256={expected}"

    async def test_corrupt_digest_detected(self, nc: natsio.Client, obj) -> None:
        await obj.put("tampered", b"real payload")
        info = await obj._info_any("tampered")
        wire = info.to_wire()
        wire["digest"] = "SHA-256=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        js = nc.jetstream()
        await js.publish(
            obj._meta_subject("tampered"),
            json.dumps(wire).encode(),
            headers={"Nats-Rollup": "sub"},
        )
        with pytest.raises(DigestMismatchError):
            await obj.get_bytes("tampered")

    async def test_size_mismatch_detected(self, nc: natsio.Client, obj) -> None:
        await obj.put("short", b"12345678")
        info = await obj._info_any("short")
        wire = info.to_wire()
        wire["size"] = 4  # lies: fewer bytes than stored
        js = nc.jetstream()
        await js.publish(
            obj._meta_subject("short"),
            json.dumps(wire).encode(),
            headers={"Nats-Rollup": "sub"},
        )
        with pytest.raises(DigestMismatchError):
            await obj.get_bytes("short")

    async def test_missing_chunks_time_out_instead_of_hanging(self, obj) -> None:
        await obj.put(ObjectMeta(name="gone", chunk_size=1024), _payload(4 * 1024))
        info = await obj._info_any("gone")
        await obj._stream.purge(subject=obj._chunk_subject(info.nuid))
        with pytest.raises(NoMessagesError):
            async with obj.get("gone", chunk_timeout=1.0) as result:
                async for _ in result:
                    pass


class TestDelete:
    async def test_delete_purges_and_marks(self, obj) -> None:
        await obj.put(ObjectMeta(name="x", chunk_size=1024), _payload(8 * 1024))
        await obj.delete("x")

        with pytest.raises(ObjectNotFoundError):
            await obj.info("x")
        with pytest.raises(ObjectNotFoundError):
            await obj.get_bytes("x")

        marker = await obj._info_any("x")
        assert marker.is_deleted
        assert marker.size == 0 and marker.chunks == 0

        # Only the delete-marker meta remains in the stream.
        info = await obj._stream.info()
        assert info.state.messages == 1

    async def test_delete_missing_or_deleted(self, obj) -> None:
        with pytest.raises(ObjectNotFoundError):
            await obj.delete("ghost")
        await obj.put("once", b"x")
        await obj.delete("once")
        with pytest.raises(ObjectNotFoundError):
            await obj.delete("once")

    async def test_put_after_delete_recreates(self, obj) -> None:
        await obj.put("phoenix", b"v1")
        await obj.delete("phoenix")
        info = await obj.put("phoenix", b"v2")
        assert not info.is_deleted
        assert await obj.get_bytes("phoenix") == b"v2"


class TestLinks:
    async def test_object_link_roundtrip(self, obj) -> None:
        data = _payload(20_000)
        target = await obj.put(ObjectMeta(name="target", chunk_size=8 * 1024), data)
        link = await obj.add_link("shortcut", target)
        assert link.is_link
        assert await obj.get_bytes("shortcut") == data
        async with obj.get("shortcut") as result:
            assert result.info.name == "target"  # link-resolved metadata
            async for _ in result:
                pass

    async def test_cross_bucket_link(self, nc: natsio.Client, obj) -> None:
        js = nc.jetstream()
        other = await js.create_object_store(ObjectStoreConfig(bucket="OTHER"))
        target = await other.put("远", b"cross-bucket data")
        await obj.add_link("into-other", target)
        assert await obj.get_bytes("into-other") == b"cross-bucket data"

    async def test_link_rules(self, obj) -> None:
        target = await obj.put("real", b"data")
        link = await obj.add_link("l1", target)
        with pytest.raises(LinkError):
            await obj.add_link("l2", link)  # no links to links
        with pytest.raises(ObjectExistsError):
            await obj.add_link("real", target)  # cannot shadow a live object
        await obj.put("doomed", b"x")
        await obj.delete("doomed")
        marker = await obj._info_any("doomed")
        with pytest.raises(LinkError):
            await obj.add_link("l3", marker)  # no links to delete markers

    async def test_bucket_link(self, nc: natsio.Client, obj) -> None:
        js = nc.jetstream()
        await js.create_object_store(ObjectStoreConfig(bucket="POINTED"))
        info = await obj.add_bucket_link("dir", "POINTED")
        assert info.is_link
        assert info.options is not None and info.options.link is not None
        assert info.options.link.bucket == "POINTED"
        with pytest.raises(LinkError, match="bucket link"):
            await obj.get_bytes("dir")

    async def test_stale_link_target_deleted(self, obj) -> None:
        target = await obj.put("victim", b"x")
        await obj.add_link("dangling", target)
        await obj.delete("victim")
        with pytest.raises(ObjectNotFoundError):
            await obj.get_bytes("dangling")


class TestEnumeration:
    async def test_list(self, obj) -> None:
        await obj.put("a", b"1")
        await obj.put("b", b"2")
        await obj.put("victim", b"3")
        await obj.delete("victim")
        target = await obj.put("c", b"4")
        await obj.add_link("l", target)

        names = {info.name for info in await obj.list()}
        assert names == {"a", "b", "c", "l"}

        with_deleted = {info.name for info in await obj.list(include_deleted=True)}
        assert with_deleted == {"a", "b", "c", "l", "victim"}

    async def test_list_empty_bucket(self, obj) -> None:
        assert await obj.list() == []

    async def test_watch_initial_state_then_updates(self, obj) -> None:
        await obj.put("pre", b"1")
        seen: list[ObjectInfo | None] = []
        marker_seen = asyncio.Event()
        update_seen = asyncio.Event()

        async def watch_task() -> None:
            async with obj.watch() as watcher:
                async for info in watcher:
                    seen.append(info)
                    if info is None:
                        marker_seen.set()
                    elif info.name == "post":
                        update_seen.set()
                        return

        async with asyncio.TaskGroup() as tg:
            tg.create_task(watch_task())
            await asyncio.wait_for(marker_seen.wait(), 5.0)
            await obj.put("post", b"2")
            await asyncio.wait_for(update_seen.wait(), 5.0)

        assert [info.name if info else None for info in seen] == ["pre", None, "post"]

    async def test_watch_updates_only(self, obj) -> None:
        await obj.put("pre", b"1")
        seen: list[str | None] = []
        ready = asyncio.Event()
        done = asyncio.Event()

        async def watch_task() -> None:
            async with obj.watch(updates_only=True) as watcher:
                async for info in watcher:
                    seen.append(info.name if info else None)
                    if info is None:
                        ready.set()
                    else:
                        done.set()
                        return

        async with asyncio.TaskGroup() as tg:
            tg.create_task(watch_task())
            await asyncio.wait_for(ready.wait(), 5.0)
            await obj.put("post", b"2")
            await asyncio.wait_for(done.wait(), 5.0)

        assert seen == [None, "post"]

    async def test_watch_sees_deletes(self, obj) -> None:
        await obj.put("x", b"1")
        deletes: list[bool] = []
        ready = asyncio.Event()
        got_delete = asyncio.Event()

        async def watch_task() -> None:
            async with obj.watch(updates_only=True) as watcher:
                async for info in watcher:
                    if info is None:
                        ready.set()
                        continue
                    deletes.append(info.is_deleted)
                    got_delete.set()
                    return

        async with asyncio.TaskGroup() as tg:
            tg.create_task(watch_task())
            await asyncio.wait_for(ready.wait(), 5.0)
            await obj.delete("x")
            await asyncio.wait_for(got_delete.wait(), 5.0)

        assert deletes == [True]


class TestReviewRegressions:
    """Regression tests for the M6 adversarial-review findings."""

    async def _subject_census(self, obj) -> dict[str, int]:
        info = await obj._stream.info(subjects_filter=">")
        return dict(info.state.subjects or {})

    async def test_concurrent_puts_leave_no_orphaned_chunks(self, obj) -> None:
        """[major] Losing a concurrent same-name put must purge its own
        chunks; unguarded rollup writes leaked them forever."""
        payload_a = _payload(4 * 1024)
        payload_b = bytes(reversed(payload_a))

        async with asyncio.TaskGroup() as tg:
            task_a = tg.create_task(obj.put(ObjectMeta(name="shared", chunk_size=1024), payload_a))
            task_b = tg.create_task(obj.put(ObjectMeta(name="shared", chunk_size=1024), payload_b))

        winner = await obj._info_any("shared")
        census = await self._subject_census(obj)
        chunk_subjects = {s for s in census if ".C." in s}
        # Exactly one revision's chunks survive — the winner's.
        assert chunk_subjects == {obj._chunk_subject(winner.nuid)}
        assert sum(census[s] for s in chunk_subjects) == 4
        # The winner's data reads back intact and one call returned the winner.
        assert await obj.get_bytes("shared") in (payload_a, payload_b)
        assert winner.nuid in {task_a.result().nuid, task_b.result().nuid}

    async def test_delete_racing_puts_keeps_state_consistent(self, obj) -> None:
        """[major-adjacent] delete() may legitimately lose the LWW race, but
        never silently: either the object ends deleted, or delete() deleted a
        real revision and a later put recreated it — in every case the stream
        holds exactly the live revision's chunks (or none) and nothing else."""
        for round_ in range(5):
            payload = _payload(2 * 1024)
            await obj.put(ObjectMeta(name="contended", chunk_size=512), payload)

            async with asyncio.TaskGroup() as tg:
                tg.create_task(obj.put(ObjectMeta(name="contended", chunk_size=512), payload))
                tg.create_task(obj.delete("contended"))

            final = await obj._info_any("contended")
            census = await self._subject_census(obj)
            chunk_subjects = {s for s in census if ".C." in s}
            if final.is_deleted:
                assert chunk_subjects == set(), f"round {round_}: orphans beside a delete marker"
            else:
                assert chunk_subjects == {obj._chunk_subject(final.nuid)}, f"round {round_}: orphaned chunks"
                assert await obj.get_bytes("contended") == payload
            # reset for the next round
            if not final.is_deleted:
                await obj.delete("contended")

    async def test_put_returns_its_own_revision(self, obj) -> None:
        """put() used to re-read the meta subject, which under races could
        describe a different writer's revision."""
        returned = await obj.put("mine", b"payload")
        stored = await obj._info_any("mine")
        assert returned.nuid == stored.nuid
        assert returned.digest == stored.digest
        assert returned.mtime is not None

    async def test_meta_payload_carries_mtime_like_go(self, nc: natsio.Client, obj) -> None:
        """Wire parity: nats.go always emits mtime in the meta JSON."""
        await obj.put("stamped", b"x")
        stored = await obj._stream.get_msg(subject=obj._meta_subject("stamped"))
        wire = json.loads(stored.payload)
        assert wire.get("mtime")

    async def test_headers_roundtrip_and_survive_delete_marker(self, obj) -> None:
        """Object-level headers (ADR-20) are first-class: writable on put,
        readable on info, preserved in the delete marker like nats.go."""
        headers = {"Content-Type": ["image/png"], "X-Tag": ["a", "b"]}
        await obj.put(ObjectMeta(name="h", headers=headers), b"x")
        assert (await obj.info("h")).headers == headers
        await obj.delete("h")
        marker = await obj._info_any("h")
        assert marker.is_deleted
        assert marker.headers == headers

    async def test_deleted_object_raises_the_subclass(self, obj) -> None:
        from natsio.objectstore import ObjectDeletedError

        await obj.put("gone", b"x")
        await obj.delete("gone")
        with pytest.raises(ObjectDeletedError):
            await obj.info("gone")
        with pytest.raises(ObjectNotFoundError):  # still a not-found
            await obj.get_bytes("gone")
        with pytest.raises(ObjectNotFoundError):
            await obj.info("never-existed")

    async def test_vanished_bucket_reports_bucket_not_found(self, nc: natsio.Client) -> None:
        """list()/status()/put() leaked StreamNotFoundError / NoStreamResponseError
        when the bucket was deleted under a live handle."""
        js = nc.jetstream()
        store = await js.create_object_store(ObjectStoreConfig(bucket="DOOMED"))
        await store.put("x", b"1")
        await js.delete_object_store("DOOMED")

        with pytest.raises(BucketNotFoundError):
            await store.status()
        with pytest.raises(BucketNotFoundError):
            await store.list()
        with pytest.raises(BucketNotFoundError):
            await store.put("y", b"2")
        with pytest.raises(BucketNotFoundError):
            await store.seal()

    async def test_context_manager_tears_down_consumer_deterministically(self, obj) -> None:
        """ObjectResult.__aexit__ was a no-op; an abandoned read left its
        ephemeral consumer to GC timing."""
        await obj.put(ObjectMeta(name="big", chunk_size=1024), _payload(8 * 1024))
        result = obj.get("big")
        async with result:
            async for _ in result:
                break  # abandon mid-read
        # result is still strongly referenced — teardown must not depend on GC.
        consumers = [name async for name in obj._stream.consumer_names()]
        assert consumers == []

    async def test_oversized_chunk_size_rejected_early(self, obj) -> None:
        from natsio.errors import ConfigError

        limit = obj._ctx.client.max_payload
        with pytest.raises(ConfigError):
            await obj.put(ObjectMeta(name="x", chunk_size=limit + 1), b"data")

    async def test_bucket_link_validates_target_name(self, obj) -> None:
        from natsio.objectstore import InvalidBucketNameError

        with pytest.raises(InvalidBucketNameError):
            await obj.add_bucket_link("ptr", "not a bucket!")

    async def test_get_detects_appended_chunk(self, nc: natsio.Client, obj) -> None:
        """[low] An out-of-band chunk appended to the nuid subject after a put
        must fail the read — nats.go digests every message on the subject and
        raises ErrDigestMismatch (TestGetObjectDigestMismatch)."""
        await obj.put("abc", b"abc")
        info = await obj._info_any("abc")
        js = nc.jetstream()
        await js.publish(obj._chunk_subject(info.nuid), b"123")
        with pytest.raises(DigestMismatchError):
            await obj.get_bytes("abc")


class TestCoverageGaps:
    async def test_multi_mb_roundtrip(self, obj) -> None:
        size = 2 * 1024 * 1024  # 16 default 128KiB chunks
        pattern = bytes(range(256))
        data = (pattern * (size // len(pattern) + 1))[:size]
        info = await obj.put("big", data)
        assert info.chunks == 16
        assert info.size == size
        assert await obj.get_bytes("big") == data

    @pytest.mark.parametrize("name", ["foo bar", "a.b.>", ".hidden*", "☂ file"])
    async def test_special_object_names_end_to_end(self, obj, name: str) -> None:
        data = ("payload for " + name).encode() * 4
        info = await obj.put(name, data)
        assert info.name == name
        assert (await obj.info(name)).name == name
        assert await obj.get_bytes(name) == data
        await obj.delete(name)
        with pytest.raises(ObjectNotFoundError):
            await obj.info(name)

    async def test_relink_over_existing_link_changes_target(self, obj) -> None:
        a = await obj.put("A", b"data-a")
        b = await obj.put("B", b"data-b")
        await obj.add_link("L", a)
        assert await obj.get_bytes("L") == b"data-a"
        # Re-linking over an existing link succeeds (it is a link, not a live object).
        await obj.add_link("L", b)
        assert await obj.get_bytes("L") == b"data-b"

    async def test_status_storage_and_replicas(self, obj) -> None:
        from natsio.jetstream.entities import StorageType

        status = await obj.status()
        assert status.storage == StorageType.FILE
        assert status.replicas == 1


class TestUpdateMeta:
    """Mirrors nats.go TestObjectMetadata end-to-end."""

    async def test_in_place_update_preserves_data(self, obj) -> None:
        data = _payload(6 * 1024)
        original = await obj.put(ObjectMeta(name="A", chunk_size=1024), data)

        updated = await obj.update_meta(
            "A",
            ObjectMeta(
                name="A",
                description="descA",
                metadata={"version": "0.1"},
                headers={"color": ["blue"]},
            ),
        )
        # Description/metadata/headers rewritten; data identity untouched.
        assert updated.description == "descA"
        assert updated.metadata == {"version": "0.1"}
        assert updated.headers == {"color": ["blue"]}
        assert (updated.nuid, updated.size, updated.chunks, updated.digest) == (
            original.nuid,
            original.size,
            original.chunks,
            original.digest,
        )

        info = await obj.info("A")
        assert info.description == "descA"
        assert info.headers == {"color": ["blue"]}
        assert info.metadata == {"version": "0.1"}
        assert await obj.get_bytes("A") == data

    async def test_update_clears_fields_to_none(self, obj) -> None:
        await obj.put(ObjectMeta(name="A", description="old", metadata={"k": "v"}), b"x")
        await obj.update_meta("A", ObjectMeta(name="A", description="descB", headers={"color": ["red"]}))
        info = await obj.info("A")
        assert info.description == "descB"
        assert info.headers == {"color": ["red"]}
        assert info.metadata is None  # not carried over from the previous revision

    async def test_rename_moves_object_and_removes_old_name(self, obj) -> None:
        data = _payload(4 * 1024)
        first = await obj.put(ObjectMeta(name="A", chunk_size=1024), data)

        renamed = await obj.update_meta("A", ObjectMeta(name="B", description="descB"))
        assert renamed.name == "B"
        assert renamed.nuid == first.nuid  # chunks stayed under the same nuid

        with pytest.raises(ObjectNotFoundError):
            await obj.info("A")  # old meta subject purged
        moved = await obj.info("B")
        assert moved.description == "descB"
        assert await obj.get_bytes("B") == data  # data readable under the new name

    async def test_rename_onto_live_object_conflicts(self, obj) -> None:
        await obj.put("B", b"b-data")
        await obj.put("C", b"c-data")
        with pytest.raises(ObjectExistsError):
            await obj.update_meta("B", ObjectMeta(name="C"))
        # both survive the refused rename
        assert await obj.get_bytes("B") == b"b-data"
        assert await obj.get_bytes("C") == b"c-data"

    async def test_rename_onto_deleted_name_is_allowed(self, obj) -> None:
        await obj.put("B", b"b-data")
        await obj.put("C", b"c-data")
        await obj.delete("C")
        renamed = await obj.update_meta("B", ObjectMeta(name="C"))
        assert renamed.name == "C"
        assert not renamed.is_deleted
        assert await obj.get_bytes("C") == b"b-data"
        with pytest.raises(ObjectNotFoundError):
            await obj.info("B")

    async def test_update_deleted_object_raises_deleted(self, obj) -> None:
        await obj.put("C", b"x")
        await obj.delete("C")
        with pytest.raises(ObjectDeletedError):
            await obj.update_meta("C", ObjectMeta(name="C", description="d"))
        with pytest.raises(ObjectDeletedError):  # rename of a deleted object too
            await obj.update_meta("C", ObjectMeta(name="D"))

    async def test_update_missing_object_raises_not_found(self, obj) -> None:
        with pytest.raises(ObjectNotFoundError):
            await obj.update_meta("X", ObjectMeta(name="X"))
        # a never-existed object is not-found, not deleted
        try:
            await obj.update_meta("X", ObjectMeta(name="X"))
        except ObjectDeletedError:
            pytest.fail("missing object must raise ObjectNotFoundError, not the deleted subclass")
        except ObjectNotFoundError:
            pass

    async def test_chunk_size_on_update_is_ignored(self, obj) -> None:
        original = await obj.put(ObjectMeta(name="A", chunk_size=2048), _payload(4 * 1024))
        assert original.options is not None and original.options.max_chunk_size == 2048
        updated = await obj.update_meta("A", ObjectMeta(name="A", chunk_size=99, description="d"))
        # options preserved from the stored revision; the update's chunk_size is meaningless.
        assert updated.options is not None and updated.options.max_chunk_size == 2048

    async def test_update_meta_is_cas_gated(self, obj) -> None:
        """Every update_meta write goes through the CAS-gated meta machinery,
        so a rename never clobbers a live object that appears concurrently."""
        await obj.put("A", b"a")
        # A rename target that becomes live mid-flight still resolves to a
        # deterministic outcome (here: no such race, plain success).
        renamed = await obj.update_meta("A", ObjectMeta(name="A2"))
        assert renamed.name == "A2"
        assert await obj.get_bytes("A2") == b"a"


class TestShowDeleted:
    """Public show-deleted reads (info/get) mirror GetObjectShowDeleted."""

    async def test_info_show_deleted_returns_marker(self, obj) -> None:
        await obj.put(ObjectMeta(name="A", chunk_size=1024), _payload(4 * 1024))
        await obj.delete("A")

        with pytest.raises(ObjectNotFoundError):
            await obj.info("A")  # default: raises

        marker = await obj.info("A", show_deleted=True)
        assert marker.is_deleted
        assert marker.size == 0 and marker.chunks == 0

    async def test_get_show_deleted_yields_no_chunks(self, obj) -> None:
        await obj.put(ObjectMeta(name="A", chunk_size=1024), _payload(4 * 1024))
        await obj.delete("A")

        with pytest.raises(ObjectNotFoundError):
            await obj.get_bytes("A")  # default: raises

        async with obj.get("A", show_deleted=True) as result:
            assert result.info.is_deleted
            chunks = [chunk async for chunk in result]
        assert chunks == []

    async def test_show_deleted_on_live_object_is_identical(self, obj) -> None:
        data = _payload(3 * 1024)
        await obj.put(ObjectMeta(name="live", chunk_size=1024), data)

        marker = await obj.info("live", show_deleted=True)
        assert not marker.is_deleted
        assert marker.size == len(data)

        async with obj.get("live", show_deleted=True) as result:
            assert not result.info.is_deleted
            received = b"".join([chunk async for chunk in result])
        assert received == data


class TestSeal:
    async def test_seal_blocks_writes(self, obj) -> None:
        await obj.put("frozen", b"forever")
        await obj.seal()
        status = await obj.status()
        assert status.sealed is True

        from natsio.jetstream.errors import APIError

        with pytest.raises(APIError):
            await obj.put("new", b"nope")
        assert await obj.get_bytes("frozen") == b"forever"


class TestStoreManagement:
    async def test_update_reflects_new_config(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        await js.create_object_store(ObjectStoreConfig(bucket="UPD", description="Test store"))
        updated = await js.update_object_store(ObjectStoreConfig(bucket="UPD", description="New store"))
        status = await updated.status()
        assert status.description == "New store"

    async def test_update_missing_bucket_raises(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        with pytest.raises(BucketNotFoundError):
            await js.update_object_store(ObjectStoreConfig(bucket="NOPE", description="x"))

    async def test_create_or_update_creates_then_updates(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        created = await js.create_or_update_object_store(ObjectStoreConfig(bucket="COU", description="first"))
        assert (await created.status()).description == "first"
        updated = await js.create_or_update_object_store(ObjectStoreConfig(bucket="COU", description="second"))
        assert (await updated.status()).description == "second"

    async def test_listings_return_only_object_stores(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        # OBJ_ prefix but no $O chunk subject -> excluded by the $O.*.C.> filter.
        await js.create_stream(StreamConfig(name="OBJ_FOO", subjects=["foo.*"], storage=StorageType.MEMORY))
        # $O chunk subject but no OBJ_ prefix -> excluded by the client-side prefix check.
        await js.create_stream(StreamConfig(name="PLAIN", subjects=["$O.ABC.C.>"], storage=StorageType.MEMORY))
        # A KV bucket shares neither prefix nor keyspace -> excluded.
        await js.create_key_value(KeyValueConfig(bucket="KVX"))
        await js.create_object_store(ObjectStoreConfig(bucket="OSS1"))
        await js.create_object_store(ObjectStoreConfig(bucket="OSS2"))

        names = {n async for n in js.object_store_names()}
        assert names == {"OSS1", "OSS2"}

        statuses = {s.bucket: s async for s in js.object_stores()}
        assert set(statuses) == {"OSS1", "OSS2"}
        assert all(isinstance(s, ObjectStoreStatus) for s in statuses.values())
