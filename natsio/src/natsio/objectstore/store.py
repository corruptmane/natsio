"""The Object Store bucket: chunked put/get, links, watchers (ADR-20)."""

import asyncio
import base64
import contextlib
import hashlib
import json
from collections.abc import AsyncGenerator, AsyncIterable, Generator
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Final, Self

from natsio._internal.nuid import next_nuid
from natsio.jetstream import headers as js_headers
from natsio.jetstream.consumer import OrderedConsumer
from natsio.jetstream.entities import DeliverPolicy, StreamConfig
from natsio.jetstream.errors import (
    MessageNotFoundError,
    NoStreamResponseError,
    StreamNotFoundError,
    WrongLastSequenceError,
)
from natsio.jetstream.message import JsMsg
from natsio.jetstream.stream import Stream

if TYPE_CHECKING:
    from natsio.jetstream.context import JetStreamContext

from .entities import (
    DEFAULT_CHUNK_SIZE,
    ObjectInfo,
    ObjectLink,
    ObjectMeta,
    ObjectMetaOptions,
    ObjectStoreStatus,
    validate_object_name,
)
from .errors import (
    BucketNotFoundError,
    DigestMismatchError,
    LinkError,
    ObjectDeletedError,
    ObjectExistsError,
    ObjectNotFoundError,
)

__all__ = ["ObjectResult", "ObjectStore", "ObjectWatcher"]

STREAM_PREFIX: Final = "OBJ_"
SUBJECT_PREFIX: Final = "$O"

_DIGEST_PREFIX: Final = "SHA-256="

type ObjectData = bytes | bytearray | memoryview | AsyncIterable[bytes]


def encode_object_name(name: str) -> str:
    """Object names travel base64url-encoded (padded, per ADR-20) as the last
    token of the metadata subject — any string is a valid object name."""
    return base64.urlsafe_b64encode(name.encode()).decode()


def _digest_value(digest: "hashlib._Hash") -> str:
    return _DIGEST_PREFIX + base64.urlsafe_b64encode(digest.digest()).decode()


def _digests_equal(a: str, b: str) -> bool:
    """Compare digests tolerantly: clients differ on base64 padding, and a
    standard-alphabet (``+/``) writer should not fail an otherwise-valid read."""

    def norm(value: str) -> str:
        return value.removeprefix(_DIGEST_PREFIX).replace("+", "-").replace("/", "_").rstrip("=")

    return norm(a) == norm(b)


async def _iter_chunks(data: ObjectData, chunk_size: int) -> AsyncGenerator[bytes]:
    """Re-chunk arbitrary input into uniform ``chunk_size`` pieces (last one
    may be short) so stored chunk boundaries never depend on how the caller's
    iterable happened to slice its data."""
    if isinstance(data, (bytes, bytearray, memoryview)):
        # cast("B"): chunk_size is BYTES — a memoryview over e.g. array('i')
        # indexes in elements, which would silently quadruple the chunk size.
        view = memoryview(data).cast("B")
        for start in range(0, len(view), chunk_size):
            yield bytes(view[start : start + chunk_size])
        return
    buffer = bytearray()
    async for piece in data:
        buffer += piece
        while len(buffer) >= chunk_size:
            yield bytes(buffer[:chunk_size])
            del buffer[:chunk_size]
    if buffer:
        yield bytes(buffer)


class ObjectStore:
    """A handle to one Object Store bucket.

    Obtain via :meth:`JetStreamContext.object_store` /
    :meth:`JetStreamContext.create_object_store`.
    """

    __slots__ = ("_ctx", "_stream", "bucket")

    def __init__(self, ctx: "JetStreamContext", bucket: str, stream: Stream) -> None:
        self._ctx = ctx
        self.bucket = bucket
        self._stream = stream

    def __repr__(self) -> str:
        return f"ObjectStore(bucket={self.bucket!r})"

    # -- subjects ------------------------------------------------------------

    def _meta_subject(self, name: str) -> str:
        return f"{SUBJECT_PREFIX}.{self.bucket}.M.{encode_object_name(name)}"

    def _chunk_subject(self, nuid: str) -> str:
        return f"{SUBJECT_PREFIX}.{self.bucket}.C.{nuid}"

    # -- metadata ------------------------------------------------------------

    async def info(self, name: str, *, show_deleted: bool = False) -> ObjectInfo:
        """The object's metadata. Raises :class:`ObjectNotFoundError` for a
        missing object — or its subclass :class:`ObjectDeletedError` when the
        latest revision is a delete marker.

        With ``show_deleted=True`` a delete marker is returned instead of
        raising (``deleted=True``, ``size``/``chunks`` 0) — the same view
        ``list(include_deleted=True)`` and ``watch()`` see."""
        info = await self._info_any(name)
        if info.is_deleted and not show_deleted:
            raise ObjectDeletedError(f"object {name!r} in bucket {self.bucket!r} was deleted")
        return info

    async def _info_any(self, name: str) -> ObjectInfo:
        """Like :meth:`info` but returns delete markers instead of raising."""
        info, _seq = await self._stored_meta(name)
        return info

    async def _stored_meta(self, name: str) -> tuple[ObjectInfo, int]:
        """The stored metadata plus its stream sequence (the CAS anchor for
        meta writes)."""
        validate_object_name(name)
        try:
            stored = await self._stream.get_msg(subject=self._meta_subject(name))
        except MessageNotFoundError:
            raise ObjectNotFoundError(f"object {name!r} not found in bucket {self.bucket!r}") from None
        info = ObjectInfo.from_wire(json.loads(stored.payload))
        info.mtime = stored.time
        return info, stored.seq

    async def _publish_meta(self, info: ObjectInfo, *, expected_seq: int) -> None:
        """Rollup-write the metadata, compare-and-set against the meta
        subject's last sequence.

        Every meta write is CAS-gated: unguarded rollup writes are how
        concurrent puts orphan chunks and racing deletes get silently rolled
        away (probe-confirmed). Raises
        :class:`~natsio.jetstream.WrongLastSequenceError` on conflict.
        """
        info.mtime = datetime.now(UTC)  # wire parity with nats.go; readers re-stamp from the message
        await self._ctx.publish(
            self._meta_subject(info.name),
            json.dumps(info.to_wire(), separators=(",", ":")).encode(),
            headers={js_headers.ROLLUP: js_headers.ROLLUP_SUBJECT},
            expected_last_subject_seq=expected_seq,
        )

    # -- writes --------------------------------------------------------------

    async def put(self, meta: str | ObjectMeta, data: ObjectData) -> ObjectInfo:
        """Store an object (replacing any existing object of that name).

        ``data`` may be in-memory bytes or an async iterable of byte chunks —
        the input is re-chunked to ``meta.chunk_size`` (default 128KiB) and
        SHA-256 digested as it streams. If the put fails partway — including
        losing a concurrent same-name race — its already published chunks are
        purged (best effort) so nothing is orphaned; the revision it replaces
        stays intact either way. Returns the info of THIS revision, exactly as
        written.
        """
        if isinstance(meta, str):
            meta = ObjectMeta(name=meta)
        chunk_size = meta.chunk_size if meta.chunk_size is not None else DEFAULT_CHUNK_SIZE
        limit = self._ctx.client.max_payload
        if 0 < limit < chunk_size:
            from natsio.errors import ConfigError

            raise ConfigError(f"chunk_size {chunk_size} exceeds the server's max_payload ({limit})")
        try:
            prev, prev_seq = await self._stored_meta(meta.name)
        except ObjectNotFoundError:
            prev, prev_seq = None, 0
        nuid = next_nuid()
        chunk_subject = self._chunk_subject(nuid)
        digest = hashlib.sha256()
        size = 0
        chunks = 0
        try:
            async for chunk in _iter_chunks(data, chunk_size):
                digest.update(chunk)
                size += len(chunk)
                chunks += 1
                await self._ctx.publish(chunk_subject, chunk)
            info = ObjectInfo(
                name=meta.name,
                description=meta.description,
                metadata=meta.metadata,
                headers=meta.headers,
                options=ObjectMetaOptions(max_chunk_size=chunk_size),
                bucket=self.bucket,
                nuid=nuid,
                size=size,
                chunks=chunks,
                digest=_digest_value(digest),
            )
            # CAS the meta write and re-anchor on conflict: last-writer-wins
            # like the rest of the ecosystem, but every loser knows it lost —
            # that is what lets it reclaim its own chunks (unguarded rollup
            # writes leaked the losing revision's chunks forever).
            for _ in range(8):
                try:
                    await self._publish_meta(info, expected_seq=prev_seq)
                    break
                except WrongLastSequenceError:
                    try:
                        prev, prev_seq = await self._stored_meta(meta.name)
                    except ObjectNotFoundError:
                        prev, prev_seq = None, 0
            else:
                raise WrongLastSequenceError(
                    f"gave up storing meta for {meta.name!r} under sustained concurrent writes"
                )
        except NoStreamResponseError:
            raise BucketNotFoundError(f"bucket {self.bucket!r} no longer exists") from None
        except BaseException:
            # Best-effort orphan cleanup; the original error matters more.
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await self._stream.purge(subject=chunk_subject)
            raise
        if prev is not None and not prev.is_deleted and prev.nuid != nuid:
            # The replaced revision's chunks are unreachable now. Best effort:
            # a failed purge only orphans OLD chunks — the put itself succeeded.
            with contextlib.suppress(Exception):
                await self._stream.purge(subject=self._chunk_subject(prev.nuid))
        return info

    async def delete(self, name: str) -> None:
        """Replace the metadata with a delete marker and purge the chunks.

        The marker keeps the object's name visible to ``watch()`` /
        ``list(include_deleted=True)``; the data is gone. The marker write is
        CAS-gated, so a delete racing a put deletes the put's revision (or the
        put lands after and legitimately recreates the object) — it can never
        report success while silently leaving the old data live.
        """
        info, seq = await self._stored_meta(name)
        try:
            for _ in range(8):
                if info.is_deleted:
                    raise ObjectDeletedError(f"object {name!r} in bucket {self.bucket!r} was already deleted")
                marker = ObjectInfo(
                    name=info.name,
                    description=info.description,
                    metadata=info.metadata,
                    headers=info.headers,
                    options=info.options,
                    bucket=self.bucket,
                    nuid=info.nuid,
                    deleted=True,
                )
                try:
                    await self._publish_meta(marker, expected_seq=seq)
                    break
                except WrongLastSequenceError:
                    # The object changed under us — re-anchor and delete THAT.
                    info, seq = await self._stored_meta(name)
            else:
                raise WrongLastSequenceError(f"gave up deleting {name!r} under sustained concurrent writes")
        except NoStreamResponseError:
            raise BucketNotFoundError(f"bucket {self.bucket!r} no longer exists") from None
        await self._stream.purge(subject=self._chunk_subject(info.nuid))

    async def update_meta(self, name: str, meta: ObjectMeta) -> ObjectInfo:
        """Update an object's description, metadata, and headers in place.

        The stored data is untouched: ``chunks``, ``nuid``, ``digest``,
        ``size`` and the object's options (link, chunk size) carry over from
        the current revision — only ``meta``'s ``description`` / ``metadata`` /
        ``headers`` (and name, see below) are rewritten. ``meta.chunk_size``
        is ignored here: it only governs how *new* data is split at ``put``
        time and is meaningless for an in-place meta rewrite (nats.go likewise
        refuses to touch the options on ``UpdateMeta``).

        RENAME — when ``meta.name`` differs from ``name`` the object moves:
        the metadata is rewritten under the new name's subject and the old
        name's meta subject is purged, so ``info(name)`` then raises
        :class:`ObjectNotFoundError`. The chunks stay under the same nuid (no
        data is copied). Renaming onto a *live* object raises
        :class:`ObjectExistsError`; onto a *deleted* name is allowed (it
        overwrites the marker).

        Raises :class:`ObjectNotFoundError` if ``name`` never existed and
        :class:`ObjectDeletedError` (its subclass) if the latest revision is a
        delete marker — a deleted object cannot be updated.

        Every meta write is CAS-gated like :meth:`put` / :meth:`delete`. On a
        rename the new meta is written first and the old subject purged second
        (nats.go's order): a crash in that window leaves the object readable
        under *both* names — same nuid, same chunks — until a later write
        cleans it up; it is never data loss. A rename is NOT safe against a
        concurrent writer of the *source* name (the two names share one nuid,
        so a racing put/delete on either name affects the other — inherent to
        the ADR-20 format, same as nats.go).
        """
        validate_object_name(meta.name)
        info, seq = await self._stored_meta(name)
        if info.is_deleted:
            raise ObjectDeletedError(f"cannot update meta of deleted object {name!r} in bucket {self.bucket!r}")

        def build(base: ObjectInfo) -> ObjectInfo:
            # nuid/size/chunks/digest and options (link, chunk size) preserved.
            return ObjectInfo(
                name=meta.name,
                description=meta.description,
                metadata=meta.metadata,
                headers=meta.headers,
                options=base.options,
                bucket=self.bucket,
                nuid=base.nuid,
                size=base.size,
                chunks=base.chunks,
                digest=base.digest,
            )

        if meta.name == name:
            # In-place: CAS against this subject, re-anchoring on conflict.
            try:
                for _ in range(8):
                    updated = build(info)
                    try:
                        await self._publish_meta(updated, expected_seq=seq)
                        return updated
                    except WrongLastSequenceError:
                        info, seq = await self._stored_meta(name)
                        if info.is_deleted:
                            raise ObjectDeletedError(
                                f"object {name!r} in bucket {self.bucket!r} was deleted during update"
                            ) from None
                raise WrongLastSequenceError(f"gave up updating meta for {name!r} under sustained concurrent writes")
            except NoStreamResponseError:
                raise BucketNotFoundError(f"bucket {self.bucket!r} no longer exists") from None

        # Rename: CAS against the TARGET subject, refusing to shadow a live
        # object (a deleted marker there is fair game to overwrite).
        updated = build(info)
        try:
            for _ in range(8):
                try:
                    existing, target_seq = await self._stored_meta(meta.name)
                except ObjectNotFoundError:
                    existing, target_seq = None, 0
                if existing is not None and not existing.is_deleted:
                    raise ObjectExistsError(
                        f"cannot rename {name!r} to {meta.name!r}: a live object already exists there"
                    )
                try:
                    await self._publish_meta(updated, expected_seq=target_seq)
                    break
                except WrongLastSequenceError:
                    continue  # the target subject changed under us — re-check it
            else:
                raise WrongLastSequenceError(f"gave up renaming {name!r} under sustained concurrent writes")
        except NoStreamResponseError:
            raise BucketNotFoundError(f"bucket {self.bucket!r} no longer exists") from None
        # New meta committed under the new name; drop the old name's meta.
        await self._stream.purge(subject=self._meta_subject(name))
        return updated

    # -- reads ---------------------------------------------------------------

    def get(self, name: str, *, chunk_timeout: float = 30.0, show_deleted: bool = False) -> "ObjectResult":
        """Stream an object's data with digest verification.

        Follows one link hop transparently. ``chunk_timeout`` bounds the wait
        for each chunk and is always finite — a stream that lost chunks raises
        :class:`~natsio.jetstream.NoMessagesError` instead of hanging forever
        (pass a larger value for very slow links, never ``None``).

        With ``show_deleted=True`` a deleted object resolves to its delete
        marker and yields no chunks (empty content, ``result.info`` is the
        marker) instead of raising — otherwise a delete marker raises
        :class:`ObjectDeletedError`. The flag applies only to ``name`` itself;
        a link target is always resolved live (a dangling link still raises).

        ::

            async with obj.get("report.pdf") as result:
                print(result.info.size)
                async for chunk in result:
                    ...
        """
        validate_object_name(name)
        return ObjectResult(self, name, chunk_timeout=chunk_timeout, show_deleted=show_deleted)

    async def get_bytes(self, name: str) -> bytes:
        """The whole object in one buffer (small objects, tests, scripts)."""
        async with self.get(name) as result:
            parts = [chunk async for chunk in result]
        return b"".join(parts)

    # -- links ---------------------------------------------------------------

    async def add_link(self, name: str, target: ObjectInfo) -> ObjectInfo:
        """Store ``name`` as a link to ``target`` (an info from any bucket).

        Links carry no data — ``get`` on the link streams the target. Refuses
        deleted targets and links-to-links; refuses to shadow an existing
        live non-link object (:class:`ObjectExistsError`).
        """
        validate_object_name(name)
        if not target.name or not target.bucket:
            raise LinkError("link target must carry a bucket and object name (use info() output)")
        if target.is_deleted:
            raise LinkError(f"cannot link to deleted object {target.name!r}")
        if target.is_link:
            raise LinkError(f"cannot link to another link ({target.name!r})")
        return await self._put_link(name, ObjectLink(bucket=target.bucket, name=target.name))

    async def add_bucket_link(self, name: str, bucket: "str | ObjectStore") -> ObjectInfo:
        """Store ``name`` as a link to a whole bucket (no object name).

        Bucket links are directory-entry style pointers: ``get`` on one raises
        :class:`LinkError` — open the linked bucket via
        :meth:`JetStreamContext.object_store` instead.
        """
        validate_object_name(name)
        target_bucket = bucket.bucket if isinstance(bucket, ObjectStore) else bucket
        from .entities import validate_bucket_name

        validate_bucket_name(target_bucket)
        return await self._put_link(name, ObjectLink(bucket=target_bucket))

    async def _put_link(self, name: str, link: ObjectLink) -> ObjectInfo:
        for _ in range(8):
            try:
                existing, seq = await self._stored_meta(name)
            except ObjectNotFoundError:
                existing, seq = None, 0
            if existing is not None and not existing.is_deleted and not existing.is_link:
                raise ObjectExistsError(f"a live object named {name!r} already exists; delete it before linking")
            info = ObjectInfo(
                name=name,
                options=ObjectMetaOptions(link=link),
                bucket=self.bucket,
                nuid=next_nuid(),
            )
            try:
                await self._publish_meta(info, expected_seq=seq)
            except WrongLastSequenceError:
                continue  # something wrote the meta in between — re-check it
            except NoStreamResponseError:
                raise BucketNotFoundError(f"bucket {self.bucket!r} no longer exists") from None
            return info
        raise WrongLastSequenceError(f"gave up linking {name!r} under sustained concurrent writes")

    # -- enumeration ---------------------------------------------------------

    def watch(
        self,
        *,
        updates_only: bool = False,
        ignore_deletes: bool = False,
    ) -> "ObjectWatcher":
        """Watch the bucket's metadata for changes.

        Yields :class:`ObjectInfo` items and exactly one ``None`` marker once
        the current state has been fully delivered (immediately for
        ``updates_only``); afterwards it streams live updates. Self-healing —
        backed by the ordered consumer.

        There is deliberately no ``include_history``: every meta write rolls
        up its subject, so the stream only ever holds each object's latest
        revision — Object Store has no history surface (unlike KV).
        """
        deliver = DeliverPolicy.NEW if updates_only else DeliverPolicy.LAST_PER_SUBJECT
        ordered = self._stream.ordered_consumer(
            filter_subjects=[f"{SUBJECT_PREFIX}.{self.bucket}.M.>"],
            deliver_policy=deliver,
        )
        return ObjectWatcher(self, ordered, updates_only=updates_only, ignore_deletes=ignore_deletes)

    async def list(self, *, include_deleted: bool = False) -> list[ObjectInfo]:
        """Every object in the bucket (links included; delete markers only
        with ``include_deleted``). An empty bucket returns ``[]`` promptly."""
        infos: list[ObjectInfo] = []
        async with self.watch(ignore_deletes=not include_deleted) as watcher:
            async for info in watcher:
                if info is None:
                    break
                infos.append(info)
        return infos

    # -- admin ---------------------------------------------------------------

    async def seal(self) -> None:
        """Make the bucket immutable: no further puts, deletes, or purges.

        Do not seal while writers are in flight: a put interrupted by the seal
        can leave chunks that are then permanently unreclaimable (nothing can
        purge a sealed stream — server-enforced, probe-confirmed).
        """
        try:
            info = await self._stream.info()
            config = StreamConfig.from_wire(info.config.to_wire())  # never mutate cached state
            config.sealed = True
            stream = await self._ctx.update_stream(config)
        except StreamNotFoundError:
            raise BucketNotFoundError(f"bucket {self.bucket!r} no longer exists") from None
        self._stream.cached_info = stream.cached_info

    async def status(self) -> ObjectStoreStatus:
        try:
            info = await self._stream.info()
        except StreamNotFoundError:
            raise BucketNotFoundError(f"bucket {self.bucket!r} no longer exists") from None
        config = info.config
        return ObjectStoreStatus(
            bucket=self.bucket,
            description=config.description,
            # The server echoes max_age=0 for "never expires"; normalize.
            ttl=config.max_age or None,
            storage=config.storage,
            replicas=config.num_replicas,
            sealed=bool(config.sealed),
            size=info.state.bytes,
            metadata=config.metadata,
            stream_info=info,
        )

    def _info_from_msg(self, msg: JsMsg) -> ObjectInfo:
        info = ObjectInfo.from_wire(json.loads(msg.payload))
        info.mtime = msg.metadata.timestamp
        return info


class ObjectResult:
    """An active read: async context manager + async iterator of chunks.

    ``info`` is the (link-resolved) metadata, available once iteration starts
    — or after ``__aenter__`` when used as a context manager. The digest and
    size are verified after the final chunk; a mismatch raises
    :class:`DigestMismatchError`, so a completed iteration is a verified read.
    """

    __slots__ = (
        "_active_messages",
        "_active_ordered",
        "_chunk_store",
        "_chunk_timeout",
        "_info",
        "_name",
        "_show_deleted",
        "_store",
    )

    def __init__(self, store: ObjectStore, name: str, *, chunk_timeout: float, show_deleted: bool = False) -> None:
        self._store = store
        self._name = name
        self._chunk_timeout = chunk_timeout
        self._show_deleted = show_deleted
        self._info: ObjectInfo | None = None
        self._chunk_store: ObjectStore | None = None
        self._active_messages: AsyncGenerator[JsMsg] | None = None
        self._active_ordered: OrderedConsumer | None = None

    @property
    def info(self) -> ObjectInfo:
        if self._info is None:
            raise RuntimeError("info is available once iteration starts (or after 'async with')")
        return self._info

    async def __aenter__(self) -> Self:
        await self._resolve()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        # Deterministic teardown of an abandoned read — don't leave the
        # ephemeral chunk consumer to GC finalization timing.
        messages, ordered = self._active_messages, self._active_ordered
        self._active_messages = self._active_ordered = None
        if messages is not None:
            await messages.aclose()
        if ordered is not None:
            await ordered.stop()

    async def _resolve(self) -> None:
        if self._info is not None:
            return
        store = self._store
        info = await store.info(self._name, show_deleted=self._show_deleted)
        if info.is_link:
            link = info.options.link if info.options is not None else None
            assert link is not None
            if not link.name:
                raise LinkError(f"{self._name!r} is a bucket link to {link.bucket!r}; open it via js.object_store(...)")
            if link.bucket != store.bucket:
                store = await store._ctx.object_store(link.bucket)
            info = await store.info(link.name)
            if info.is_link:
                raise LinkError(f"chained links are not supported ({self._name!r} -> {link.name!r})")
        self._info = info
        self._chunk_store = store

    def __aiter__(self) -> AsyncGenerator[bytes]:
        return self._iterate()

    async def _iterate(self) -> AsyncGenerator[bytes]:
        await self._resolve()
        assert self._info is not None and self._chunk_store is not None
        info = self._info
        digest = hashlib.sha256()
        if info.chunks == 0:
            self._verify(info, digest, size=0)
            return
        received = 0
        size = 0
        ordered = self._chunk_store._stream.ordered_consumer(
            filter_subjects=[self._chunk_store._chunk_subject(info.nuid)],
        )
        messages = ordered.messages(idle_timeout=self._chunk_timeout)
        self._active_ordered = ordered
        self._active_messages = messages
        try:
            async for msg in messages:
                digest.update(msg.payload)
                received += 1
                size += len(msg.payload)
                yield msg.payload
                if received >= info.chunks:
                    # A chunk appended out-of-band after the put makes the
                    # recorded-count-th message report pending successors — the
                    # digest would silently ignore it. nats.go digests every
                    # message on the subject; reject the extra data instead.
                    if msg.metadata.num_pending > 0:
                        raise DigestMismatchError(
                            f"object {info.name!r}: extra chunks beyond the recorded {info.chunks} — discard the data"
                        )
                    break
        finally:
            self._active_messages = self._active_ordered = None
            await messages.aclose()
            await ordered.stop()
        self._verify(info, digest, size=size)

    def _verify(self, info: ObjectInfo, digest: "hashlib._Hash", *, size: int) -> None:
        if size != info.size:
            raise DigestMismatchError(f"object {info.name!r}: read {size} bytes but metadata records {info.size}")
        if info.digest and not _digests_equal(_digest_value(digest), info.digest):
            raise DigestMismatchError(f"object {info.name!r}: SHA-256 digest mismatch — discard the data")


class ObjectWatcher:
    """An active watch: async iterator of ``ObjectInfo | None``.

    The single ``None`` is the initial-state marker — everything before it
    existed when the watch started; everything after is a live update. Use as
    an async context manager for deterministic teardown.
    """

    __slots__ = ("_ignore_deletes", "_init_done", "_ordered", "_stopped", "_store", "_updates_only")

    def __init__(
        self,
        store: ObjectStore,
        ordered: OrderedConsumer,
        *,
        updates_only: bool,
        ignore_deletes: bool,
    ) -> None:
        self._store = store
        self._ordered = ordered
        self._updates_only = updates_only
        self._ignore_deletes = ignore_deletes
        self._init_done = False
        self._stopped = False

    def __await__(self) -> Generator[None, None, "ObjectWatcher"]:
        """``await`` is optional and completes immediately (no I/O) — nats-py
        muscle memory support; see :meth:`Subscription.__await__`."""
        return self
        yield  # unreachable: makes this a generator that never suspends

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    async def stop(self) -> None:
        self._stopped = True
        await self._ordered.stop()

    def __aiter__(self) -> AsyncGenerator[ObjectInfo | None]:
        return self._iterate()

    async def _iterate(self) -> AsyncGenerator[ObjectInfo | None]:
        if self._stopped:
            return  # a stopped watcher must not resurrect its consumer
        try:
            info = await self._ordered.start()
        except StreamNotFoundError:
            raise BucketNotFoundError(f"bucket {self._store.bucket!r} no longer exists") from None
        if self._updates_only or info.num_pending == 0:
            self._init_done = True
            yield None
        # The ordered consumer self-heals by REPLAYING from its last good
        # point, so a heal landing mid-snapshot re-delivers metas; dedup by
        # (subject-name, revision) keeps the initial state exact (same hazard
        # the KV watcher guards against).
        snapshot_seen: dict[str, int] = {}
        async for msg in self._ordered.messages():
            if self._stopped:
                return
            entry = self._store._info_from_msg(msg)
            metadata = msg.metadata
            caught_up = not self._init_done and metadata.num_pending == 0
            duplicate = False
            if not self._init_done:
                previous = snapshot_seen.get(entry.name, 0)
                duplicate = metadata.stream_seq <= previous
                if not duplicate:
                    snapshot_seen[entry.name] = metadata.stream_seq
            if not duplicate and not (self._ignore_deletes and entry.is_deleted):
                yield entry
            if caught_up:
                self._init_done = True
                snapshot_seen.clear()
                yield None
