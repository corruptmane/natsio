"""The Key-Value bucket: CRUD, history, and watchers (ADR-8/31/48)."""

from collections.abc import AsyncGenerator, Generator
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Final, Self

from natsio.jetstream import headers as js_headers
from natsio.jetstream.consumer import OrderedConsumer
from natsio.jetstream.entities import DeliverPolicy, StreamConfig
from natsio.jetstream.errors import NoMessagesError, WrongLastSequenceError
from natsio.jetstream.message import JsMsg
from natsio.jetstream.stream import Stream

if TYPE_CHECKING:
    from natsio.jetstream.context import JetStreamContext

from .entities import KeyCodec, KeyValueStatus, KvEntry, Operation, ValueCodec, validate_key
from .errors import BucketNotFoundError, KeyDeletedError, KeyExistsError, KeyNotFoundError

__all__ = ["KeyValue", "KvWatcher"]

KV_OPERATION_HEADER: Final = "KV-Operation"
STREAM_PREFIX: Final = "KV_"
SUBJECT_PREFIX: Final = "$KV"

# ADR-48 limit-marker reasons map onto marker operations.
# All limit-marker reasons read as purge-class, matching how other clients
# surface server-authored subject-delete markers.
_MARKER_OPERATIONS: Final = {
    "MaxAge": Operation.PURGE,
    "Purge": Operation.PURGE,
    "Remove": Operation.PURGE,
}

# The initial-snapshot phase of a watch is bounded by this idle timeout: N
# seconds with zero deliveries means caught-up-or-empty (the stream may have
# been purged out from under a pending snapshot). The live phase that follows
# is unbounded.
_SNAPSHOT_IDLE_TIMEOUT: Final = 5.0


def validate_kv_stream(bucket: str, config: StreamConfig) -> None:
    """Ensure a resolved stream actually covers ``bucket``'s KV keyspace.

    Binding to a ``KV_``-named stream whose subjects don't include
    ``$KV.<bucket>.>`` otherwise surfaces much later as a confusing
    publish-time failure naming the raw subject; reject it up front.
    """
    prefix = f"{SUBJECT_PREFIX}.{bucket}."
    covering = {f"{SUBJECT_PREFIX}.{bucket}.>", f"{SUBJECT_PREFIX}.>", ">"}
    subjects = config.subjects or []
    if any(subject in covering or subject.startswith(prefix) for subject in subjects):
        return
    raise BucketNotFoundError(
        f"stream {config.name!r} is not a Key-Value bucket for {bucket!r}: "
        f"its subjects {subjects} do not cover {prefix}>"
    )


class KeyValue:
    """A handle to one Key-Value bucket.

    Obtain via `JetStreamContext.key_value()` /
    `JetStreamContext.create_key_value()`. Codecs (identity by default)
    transform keys and values on the way in and out — the seam that keeps
    codec packs a plug-in rather than a breaking change.
    """

    __slots__ = ("_ctx", "_key_codec", "_stream", "_value_codec", "bucket")

    def __init__(
        self,
        ctx: "JetStreamContext",
        bucket: str,
        stream: Stream,
        *,
        key_codec: KeyCodec | None = None,
        value_codec: ValueCodec | None = None,
    ) -> None:
        validate_kv_stream(bucket, stream.cached_info.config)
        self._ctx = ctx
        self.bucket = bucket
        self._stream = stream
        self._key_codec = key_codec
        self._value_codec = value_codec

    def __repr__(self) -> str:
        return f"KeyValue(bucket={self.bucket!r})"

    # -- codec plumbing ------------------------------------------------------

    def _encode_key(self, key: str) -> str:
        if self._key_codec is None:
            validate_key(key)
            return key
        # With a codec, the RAW key is the caller's domain — exotic characters
        # are exactly what key codecs exist for (kvcodec finding: validating
        # the raw key first defeated Base64KeyCodec's headline use case).
        # Only the encoded form must be a legal NATS subject.
        encoded = self._key_codec.encode(key)
        validate_key(encoded)
        return encoded

    def _decode_key(self, key: str) -> str:
        return self._key_codec.decode(key) if self._key_codec is not None else key

    def _encode_value(self, value: bytes | str) -> bytes:
        data = value.encode() if isinstance(value, str) else value
        return self._value_codec.encode(data) if self._value_codec is not None else data

    def _decode_value(self, value: bytes) -> bytes:
        return self._value_codec.decode(value) if self._value_codec is not None else value

    def _subject(self, encoded_key: str) -> str:
        return f"{SUBJECT_PREFIX}.{self.bucket}.{encoded_key}"

    def _key_from_subject(self, subject: str) -> str:
        return self._decode_key(subject.partition(f"{SUBJECT_PREFIX}.{self.bucket}.")[2])

    # -- reads ---------------------------------------------------------------

    async def get(self, key: str, *, revision: int | None = None) -> KvEntry:
        """The live entry for ``key`` (or a specific ``revision``).

        Raises `KeyNotFoundError` — or its subclass
        `KeyDeletedError` when the latest revision is a marker.
        """
        entry = await self._get_any(key, revision=revision)
        if entry.is_marker:
            raise KeyDeletedError(
                f"key {key!r} was deleted (marker revision {entry.revision})",
                revision=entry.revision,
            )
        return entry

    async def _get_any(self, key: str, *, revision: int | None = None) -> KvEntry:
        """Like `get()` but returns marker entries instead of raising."""
        encoded = self._encode_key(key)
        subject = self._subject(encoded)
        from natsio.jetstream.errors import MessageNotFoundError

        try:
            if revision is None:
                stored = await self._stream.get_msg(subject=subject)
            else:
                stored = await self._stream.get_msg(revision)
        except MessageNotFoundError:
            raise KeyNotFoundError(f"key {key!r} not found in bucket {self.bucket!r}") from None
        if stored.subject != subject:
            raise KeyNotFoundError(f"revision {revision} of bucket {self.bucket!r} is not key {key!r}")
        operation = Operation.PUT
        created = stored.time
        if stored.headers is not None:
            raw_op = stored.headers.get(KV_OPERATION_HEADER)
            if raw_op is not None:
                operation = Operation(raw_op)
            else:
                marker = stored.headers.get(js_headers.MARKER_REASON)
                if marker is not None:
                    operation = _MARKER_OPERATIONS.get(marker, Operation.PURGE)
        return KvEntry(
            bucket=self.bucket,
            key=key,
            value=self._decode_value(stored.payload) if operation is Operation.PUT else b"",
            revision=stored.seq,
            operation=operation,
            created=created,
        )

    # -- writes --------------------------------------------------------------

    def _require_msg_ttl(self, ttl: js_headers.TTLInput | None) -> None:
        """Reject a per-message ``ttl`` when the bucket wasn't built for it.

        Mirrors the server's own rejection but with an actionable error, so a
        ``ttl=`` write against a plain bucket fails client-side instead of
        surfacing as a raw APIError from the publish.
        """
        if ttl is not None and not self._stream.cached_info.config.allow_msg_ttl:
            from natsio.errors import ConfigError

            raise ConfigError(
                f"bucket {self.bucket!r} does not allow per-message TTLs; create it "
                "with KeyValueConfig(allow_msg_ttl=True) or limit_marker_ttl to use a "
                "per-key ttl"
            )

    async def put(self, key: str, value: bytes | str, *, ttl: js_headers.TTLInput | None = None) -> int:
        """Store a value; returns the new revision.

        ``ttl`` (a ``timedelta``, whole seconds, or ``"never"`` — ADR-43) makes this single
        revision self-expire — requires the bucket's ``allow_msg_ttl``
        (``KeyValueConfig(allow_msg_ttl=True)`` or ``limit_marker_ttl``),
        otherwise `ConfigError`.
        """
        self._require_msg_ttl(ttl)
        encoded = self._encode_key(key)
        ack = await self._ctx.publish(self._subject(encoded), self._encode_value(value), ttl=ttl)
        return ack.seq

    async def create(self, key: str, value: bytes | str, *, ttl: js_headers.TTLInput | None = None) -> int:
        """Store a value only if the key has no live value.

        Succeeds for brand-new keys and for deleted/purged keys; raises
        `KeyExistsError` when a live value exists. ``ttl`` (whole
        seconds, or ``"never"``) makes the created revision self-expire — same
        ``allow_msg_ttl`` requirement as `put()`; when it expires the key is
        gone and `create()` succeeds for it again.
        """
        self._require_msg_ttl(ttl)
        expected = 0
        last_error: WrongLastSequenceError | None = None
        for _ in range(4):  # bounded re-resolve under concurrent marker churn
            try:
                return await self._update_revision(key, value, last=expected, ttl=ttl)
            except WrongLastSequenceError as exc:
                last_error = exc
            # The key has revisions. If the latest is a marker, CAS against the
            # MARKER's revision (the classic bug is retrying against the
            # original error or the pre-delete value revision).
            try:
                entry = await self._get_any(key)
            except KeyNotFoundError:
                expected = 0  # purged away between attempts (markers self-expire)
                continue
            if not entry.is_marker:
                raise KeyExistsError(
                    f"key {key!r} already has a live value (revision {entry.revision})",
                    revision=entry.revision,
                ) from None
            expected = entry.revision
        assert last_error is not None
        raise last_error

    async def update(self, key: str, value: bytes | str, *, last: int) -> int:
        """Compare-and-set: store only if ``last`` is the key's latest revision.

        Raises `WrongLastSequenceError` on conflict.
        """
        return await self._update_revision(key, value, last=last, ttl=None)

    async def _update_revision(
        self, key: str, value: bytes | str, *, last: int, ttl: js_headers.TTLInput | None
    ) -> int:
        encoded = self._encode_key(key)
        ack = await self._ctx.publish(
            self._subject(encoded),
            self._encode_value(value),
            expected_last_subject_seq=last,
            ttl=ttl,
        )
        return ack.seq

    async def delete(self, key: str, *, last: int | None = None) -> None:
        """Write a delete marker; history below it is preserved."""
        encoded = self._encode_key(key)
        await self._ctx.publish(
            self._subject(encoded),
            b"",
            headers={KV_OPERATION_HEADER: Operation.DELETE.value},
            expected_last_subject_seq=last,
        )

    async def purge(self, key: str, *, ttl: js_headers.TTLInput | None = None, last: int | None = None) -> None:
        """Write a purge marker and roll up: prior revisions are removed.

        ``ttl`` (a ``timedelta``, whole seconds, or ``"never"``) lets the marker itself expire
        — requires per-message TTLs on the bucket
        (``KeyValueConfig(allow_msg_ttl=True)`` or ``limit_marker_ttl``).
        ``last`` makes the rollup a compare-and-set: it proceeds only if ``last``
        is the key's latest revision, else
        `WrongLastSequenceError`.
        """
        self._require_msg_ttl(ttl)
        encoded = self._encode_key(key)
        await self._ctx.publish(
            self._subject(encoded),
            b"",
            headers={
                KV_OPERATION_HEADER: Operation.PURGE.value,
                js_headers.ROLLUP: js_headers.ROLLUP_SUBJECT,
            },
            ttl=ttl,
            expected_last_subject_seq=last,
        )

    async def purge_deletes(self, *, older_than: timedelta | None = None) -> None:
        """Remove delete/purge markers (and the history they sit atop).

        For each marked key the whole subject is purged, so both the marker and
        any surviving history below it are erased. A marker YOUNGER than
        ``older_than`` is kept — its subject is purged only up to the marker
        (``keep=1``) so the tombstone itself survives for late watchers.

        ``older_than`` defaults to 30 minutes (matching nats.go). Pass
        ``timedelta(0)`` (or a negative delta) to remove every marker regardless
        of age.
        """
        if older_than is None:
            older_than = timedelta(minutes=30)
        limit: datetime | None = None
        if older_than > timedelta(0):
            limit = datetime.now(UTC) - older_than

        markers: list[KvEntry] = []
        async with self.watch(meta_only=True) as watcher:
            async for entry in watcher:
                if entry is None:
                    break  # initial state fully collected
                if entry.is_marker:
                    markers.append(entry)
        # Watcher is stopped (context exit) before purging so its num_pending
        # bookkeeping can't churn against the rollups we are about to issue.
        for entry in markers:
            subject = self._subject(self._encode_key(entry.key))
            if limit is not None and entry.created is not None and entry.created > limit:
                await self._stream.purge(subject=subject, keep=1)  # keep the marker
            else:
                await self._stream.purge(subject=subject)

    # -- enumeration ---------------------------------------------------------

    def watch(
        self,
        *keys: str,
        include_history: bool = False,
        updates_only: bool = False,
        ignore_deletes: bool = False,
        meta_only: bool = False,
        resume_from_revision: int | None = None,
    ) -> "KvWatcher":
        """Watch one or more keys (or wildcards) for changes.

        With no ``keys`` the whole bucket is watched (``">"``); one key behaves
        exactly as a single-key watch; several keys install an ordered consumer
        with one filter subject per key, so the initial state and live updates
        cover the union of the keys. Yields `KvEntry` items and exactly
        one ``None`` marker once the current state has been fully delivered
        (immediately for ``updates_only``); afterwards it streams live updates.
        Self-healing — backed by the ordered consumer.

        ``resume_from_revision`` replays from that stream revision onward
        (every revision, not last-per-subject) and is mutually exclusive with
        ``include_history`` and ``updates_only``.
        """
        from natsio.errors import ConfigError

        if include_history and updates_only:
            raise ConfigError("include_history and updates_only are mutually exclusive")
        if resume_from_revision is not None and (include_history or updates_only):
            raise ConfigError("resume_from_revision is mutually exclusive with include_history and updates_only")

        filters = list(keys) if keys else [">"]
        subjects: list[str] = []
        for key in filters:
            validate_key(key, wildcards=True)
            encoded = key if key == ">" else self._maybe_encode_watch_key(key)
            subjects.append(self._subject(encoded))

        opt_start_seq: int | None = None
        if resume_from_revision is not None:
            deliver = DeliverPolicy.BY_START_SEQUENCE
            opt_start_seq = resume_from_revision
        elif updates_only:
            deliver = DeliverPolicy.NEW
        elif include_history:
            deliver = DeliverPolicy.ALL
        else:
            deliver = DeliverPolicy.LAST_PER_SUBJECT
        ordered = self._stream.ordered_consumer(
            filter_subjects=subjects,
            deliver_policy=deliver,
            opt_start_seq=opt_start_seq,
            headers_only=meta_only,
        )
        return KvWatcher(
            self,
            ordered,
            updates_only=updates_only,
            ignore_deletes=ignore_deletes,
        )

    def _maybe_encode_watch_key(self, key: str) -> str:
        if self._key_codec is None:
            return key
        if "*" in key or ">" in key:
            from natsio.errors import ConfigError

            raise ConfigError(
                "wildcard watch keys cannot be combined with a key codec: the encoded "
                "keyspace would silently match nothing; watch('>') the whole bucket instead"
            )
        return self._key_codec.encode(key)

    async def keys(self) -> list[str]:
        """Every key with a live value. An empty bucket returns ``[]`` promptly."""
        return [key async for key in self.iter_keys()]

    async def iter_keys(self) -> AsyncGenerator[str]:
        """Stream keys with live values without buffering the whole keyspace."""
        yielded: set[str] = set()
        async with self.watch(ignore_deletes=True, meta_only=True) as watcher:
            async for entry in watcher:
                if entry is None:
                    break
                if entry.key not in yielded:
                    yielded.add(entry.key)
                    yield entry.key

    async def history(self, key: str) -> list[KvEntry]:
        """Every stored revision of ``key``, oldest first (markers included)."""
        entries: list[KvEntry] = []
        async with self.watch(key, include_history=True) as watcher:
            async for entry in watcher:
                if entry is None:
                    break
                entries.append(entry)
        if not entries:
            raise KeyNotFoundError(f"key {key!r} not found in bucket {self.bucket!r}")
        return entries

    async def status(self) -> KeyValueStatus:
        info = await self._stream.info()
        config = info.config
        return KeyValueStatus(
            bucket=self.bucket,
            values=info.state.messages,
            history=config.max_msgs_per_subject,  # -1 = unlimited (foreign buckets)
            # The server echoes max_age=0 for "never expires"; normalize.
            ttl=config.max_age or None,
            bytes=info.state.bytes,
            storage=config.storage,
            stream_info=info,
        )

    def _entry_from_msg(self, msg: JsMsg) -> KvEntry:
        operation = Operation.PUT
        headers = msg.headers
        if headers is not None:
            raw_op = headers.get(KV_OPERATION_HEADER)
            if raw_op is not None:
                operation = Operation(raw_op)
            else:
                marker = headers.get(js_headers.MARKER_REASON)
                if marker is not None:
                    operation = _MARKER_OPERATIONS.get(marker, Operation.PURGE)
        metadata = msg.metadata
        # Empty payload short-circuit: meta_only watches deliver PUT entries
        # with the payload stripped by the server, and a framing value codec
        # (zlib, encryption) would raise decoding b"" (kvcodec finding). A
        # genuinely-empty stored value is b"" with no codec (passthrough) and
        # never b"" with a framing codec, so the skip is always correct.
        payload = msg.payload
        return KvEntry(
            bucket=self.bucket,
            key=self._key_from_subject(msg.subject),
            value=self._decode_value(payload) if operation is Operation.PUT and payload else b"",
            revision=metadata.stream_seq,
            operation=operation,
            created=metadata.timestamp,
            delta=metadata.num_pending,
        )


class KvWatcher:
    """An active watch: async iterator of ``KvEntry | None``.

    The single ``None`` is the initial-state marker — everything before it
    existed when the watch started; everything after is a live update. Use as
    an async context manager for deterministic teardown.
    """

    __slots__ = ("_ignore_deletes", "_init_done", "_kv", "_ordered", "_stopped", "_updates_only")

    def __init__(
        self,
        kv: KeyValue,
        ordered: OrderedConsumer,
        *,
        updates_only: bool,
        ignore_deletes: bool,
    ) -> None:
        self._kv = kv
        self._ordered = ordered
        self._updates_only = updates_only
        self._ignore_deletes = ignore_deletes
        self._init_done = False
        self._stopped = False

    def __await__(self) -> Generator[None, None, "KvWatcher"]:
        """``await`` is optional and completes immediately (no I/O) — nats-py
        muscle memory support; see `Subscription.__await__()`."""
        return self
        yield  # unreachable: makes this a generator that never suspends

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    async def stop(self) -> None:
        self._stopped = True
        await self._ordered.stop()

    def __aiter__(self) -> AsyncGenerator[KvEntry | None]:
        return self._iterate()

    async def _iterate(self) -> AsyncGenerator[KvEntry | None]:
        if self._stopped:
            return  # a stopped watcher must not resurrect its consumer
        info = await self._ordered.start()
        if self._updates_only or info.num_pending == 0:
            self._init_done = True
            yield None
        # -- initial snapshot (bounded) --------------------------------------
        # Revisions yielded during the snapshot, by key. The ordered consumer
        # self-heals by REPLAYING from its last good point, so a heal landing
        # mid-snapshot re-delivers entries; without dedup that showed up as
        # duplicate keys and stale values in keys()/watch initial state.
        #
        # The snapshot is bounded by an idle timeout: if the pending messages
        # are purged before delivery the consumer never sees a delta==0 entry,
        # and an unbounded wait would hang forever (probe_deadlock.py). On the
        # NoMessagesError the snapshot is treated as complete.
        if not self._init_done:
            snapshot_seen: dict[str, int] = {}
            try:
                async for msg in self._ordered.messages(idle_timeout=_SNAPSHOT_IDLE_TIMEOUT):
                    if self._stopped:
                        return
                    entry = self._kv._entry_from_msg(msg)
                    caught_up = entry.delta == 0
                    previous = snapshot_seen.get(entry.key, 0)
                    duplicate = entry.revision <= previous
                    if not duplicate:
                        snapshot_seen[entry.key] = entry.revision
                    if not duplicate and not (self._ignore_deletes and entry.is_marker):
                        yield entry
                    if caught_up:
                        break
            except NoMessagesError:
                pass  # snapshot drained (quiet or purged-out): caught up
            self._init_done = True
            yield None
        # -- live updates (unbounded) ----------------------------------------
        # The snapshot generator is spent (broke out, or raised and ran its
        # teardown); a fresh messages() call resumes from the ordered
        # consumer's preserved position (_last_sseq), so the live phase neither
        # replays the snapshot nor skips past it.
        async for msg in self._ordered.messages():
            if self._stopped:
                return
            entry = self._kv._entry_from_msg(msg)
            if not (self._ignore_deletes and entry.is_marker):
                yield entry
