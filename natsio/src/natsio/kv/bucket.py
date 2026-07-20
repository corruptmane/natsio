"""The Key-Value bucket: CRUD, history, and watchers (ADR-8/31/48)."""

from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Final, Self

from natsio.jetstream import headers as js_headers
from natsio.jetstream.consumer import OrderedConsumer
from natsio.jetstream.entities import DeliverPolicy
from natsio.jetstream.errors import WrongLastSequenceError
from natsio.jetstream.message import JsMsg
from natsio.jetstream.stream import Stream

if TYPE_CHECKING:
    from natsio.jetstream.context import JetStreamContext

from .entities import KeyCodec, KeyValueStatus, KvEntry, Operation, ValueCodec, validate_key
from .errors import KeyDeletedError, KeyExistsError, KeyNotFoundError

__all__ = ["KeyValue", "KvWatcher"]

KV_OPERATION_HEADER: Final = "KV-Operation"
STREAM_PREFIX: Final = "KV_"
SUBJECT_PREFIX: Final = "$KV"

# ADR-48 limit-marker reasons map onto marker operations.
_MARKER_OPERATIONS: Final = {
    "MaxAge": Operation.PURGE,
    "Purge": Operation.PURGE,
    "Remove": Operation.DELETE,
}


class KeyValue:
    """A handle to one Key-Value bucket.

    Obtain via :meth:`JetStreamContext.key_value` /
    :meth:`JetStreamContext.create_key_value`. Codecs (identity by default)
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
        self._ctx = ctx
        self.bucket = bucket
        self._stream = stream
        self._key_codec = key_codec
        self._value_codec = value_codec

    def __repr__(self) -> str:
        return f"KeyValue(bucket={self.bucket!r})"

    # -- codec plumbing ------------------------------------------------------

    def _encode_key(self, key: str) -> str:
        validate_key(key)
        if self._key_codec is not None:
            key = self._key_codec.encode(key)
            validate_key(key)
        return key

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

        Raises :class:`KeyNotFoundError` — or its subclass
        :class:`KeyDeletedError` when the latest revision is a marker.
        """
        entry = await self._get_any(key, revision=revision)
        if entry.is_marker:
            raise KeyDeletedError(
                f"key {key!r} was deleted (marker revision {entry.revision})",
                revision=entry.revision,
            )
        return entry

    async def _get_any(self, key: str, *, revision: int | None = None) -> KvEntry:
        """Like :meth:`get` but returns marker entries instead of raising."""
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

    async def put(self, key: str, value: bytes | str) -> int:
        """Store a value; returns the new revision."""
        encoded = self._encode_key(key)
        ack = await self._ctx.publish(self._subject(encoded), self._encode_value(value))
        return ack.seq

    async def create(self, key: str, value: bytes | str) -> int:
        """Store a value only if the key has no live value.

        Succeeds for brand-new keys and for deleted/purged keys; raises
        :class:`KeyExistsError` when a live value exists.
        """
        try:
            return await self.update(key, value, last=0)
        except WrongLastSequenceError:
            pass
        # The key has revisions. If the latest is a marker, retry against the
        # MARKER's revision (the classic bug is retrying against the original
        # error or the pre-delete value revision).
        try:
            entry = await self._get_any(key)
        except KeyNotFoundError:
            # Purged away between attempts (markers can self-expire): race
            # once more against "no revisions".
            return await self.update(key, value, last=0)
        if entry.is_marker:
            return await self.update(key, value, last=entry.revision)
        raise KeyExistsError(
            f"key {key!r} already has a live value (revision {entry.revision})",
            revision=entry.revision,
        )

    async def update(self, key: str, value: bytes | str, *, last: int) -> int:
        """Compare-and-set: store only if ``last`` is the key's latest revision.

        Raises :class:`~natsio.jetstream.WrongLastSequenceError` on conflict.
        """
        encoded = self._encode_key(key)
        ack = await self._ctx.publish(
            self._subject(encoded),
            self._encode_value(value),
            expected_last_subject_seq=last,
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

    async def purge(self, key: str, *, ttl: int | str | None = None) -> None:
        """Write a purge marker and roll up: prior revisions are removed.

        ``ttl`` (whole seconds, or ``"never"``) lets the marker itself expire
        — requires the bucket's ``limit_marker_ttl``/per-message TTLs.
        """
        encoded = self._encode_key(key)
        await self._ctx.publish(
            self._subject(encoded),
            b"",
            headers={
                KV_OPERATION_HEADER: Operation.PURGE.value,
                js_headers.ROLLUP: js_headers.ROLLUP_SUBJECT,
            },
            ttl=ttl,
        )

    # -- enumeration ---------------------------------------------------------

    def watch(
        self,
        key: str = ">",
        *,
        include_history: bool = False,
        updates_only: bool = False,
        ignore_deletes: bool = False,
        meta_only: bool = False,
    ) -> "KvWatcher":
        """Watch a key (or wildcard) for changes.

        Yields :class:`KvEntry` items and exactly one ``None`` marker once the
        current state has been fully delivered (immediately for
        ``updates_only``); afterwards it streams live updates. Self-healing —
        backed by the ordered consumer.
        """
        validate_key(key, wildcards=True)
        encoded = key if key == ">" else self._maybe_encode_watch_key(key)
        if include_history and updates_only:
            from natsio.errors import ConfigError

            raise ConfigError("include_history and updates_only are mutually exclusive")
        if updates_only:
            deliver = DeliverPolicy.NEW
        elif include_history:
            deliver = DeliverPolicy.ALL
        else:
            deliver = DeliverPolicy.LAST_PER_SUBJECT
        ordered = self._stream.ordered_consumer(
            filter_subjects=[self._subject(encoded)],
            deliver_policy=deliver,
            headers_only=meta_only,
        )
        return KvWatcher(
            self,
            ordered,
            updates_only=updates_only,
            ignore_deletes=ignore_deletes,
        )

    def _maybe_encode_watch_key(self, key: str) -> str:
        if "*" in key or ">" in key or self._key_codec is None:
            return key  # wildcards cannot be codec-transformed meaningfully
        return self._key_codec.encode(key)

    async def keys(self) -> list[str]:
        """Every key with a live value. An empty bucket returns ``[]`` promptly."""
        found: list[str] = []
        async with self.watch(ignore_deletes=True, meta_only=True) as watcher:
            async for entry in watcher:
                if entry is None:
                    break
                found.append(entry.key)
        return found

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
            history=max(1, config.max_msgs_per_subject),
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
        return KvEntry(
            bucket=self.bucket,
            key=self._key_from_subject(msg.subject),
            value=self._decode_value(msg.payload) if operation is Operation.PUT else b"",
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

    __slots__ = ("_ignore_deletes", "_init_done", "_kv", "_ordered", "_updates_only")

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

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    async def stop(self) -> None:
        await self._ordered.stop()

    def __aiter__(self) -> AsyncGenerator[KvEntry | None]:
        return self._iterate()

    async def _iterate(self) -> AsyncGenerator[KvEntry | None]:
        info = await self._ordered.start()
        if self._updates_only or info.num_pending == 0:
            self._init_done = True
            yield None
        async for msg in self._ordered.messages():
            entry = self._kv._entry_from_msg(msg)
            caught_up = not self._init_done and entry.delta == 0
            if not (self._ignore_deletes and entry.is_marker):
                yield entry
            if caught_up:
                self._init_done = True
                yield None
