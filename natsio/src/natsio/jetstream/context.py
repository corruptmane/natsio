"""JetStreamContext: `$JS.API` plumbing, stream CRUD, and JetStream publish."""

import asyncio
import builtins
import json
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from natsio._internal.nuid import next_nuid
from natsio._internal.protocol import HeadersInput, StatusCode
from natsio._internal.validation import validate_stream_name, validate_subject
from natsio.errors import ConfigError, ConnectionClosedError, NoRespondersError
from natsio.errors import TimeoutError as NATSTimeoutError
from natsio.message import Msg

if TYPE_CHECKING:
    from natsio.client import Client

from . import headers as js_headers
from .entities import AccountInfo, DiscardPolicy, PubAck, StreamConfig, StreamInfo
from .errors import APIError, JetStreamError, JetStreamNotEnabledError, NoStreamResponseError
from .stream import Stream

if TYPE_CHECKING:
    from natsio.kv.bucket import KeyValue
    from natsio.kv.entities import KeyCodec, KeyValueConfig, KeyValueStatus, ValueCodec
    from natsio.objectstore.entities import ObjectStoreConfig, ObjectStoreStatus
    from natsio.objectstore.store import ObjectStore

__all__ = ["AsyncPublishTimeoutError", "JetStreamContext", "TooManyStalledMsgsError"]

_PUBLISH_RETRY_ATTEMPTS = 2
_PUBLISH_RETRY_WAIT = 0.25

# Async publish window/stall defaults mirror nats.go (jetstream/jetstream.go
# defaultAsyncPubAckInflight, jetstream/publish.go defaultStallWait). The
# per-message ack timeout is disabled by default, as in nats.go.
_ASYNC_MAX_PENDING = 4000
_ASYNC_STALL_WAIT = 0.2


class TooManyStalledMsgsError(JetStreamError):
    """The async publish window stayed full past ``publish_async_stall_wait``.

    Mirrors nats.go ``ErrTooManyStalledMsgs``: the outstanding-ack window is at
    ``publish_async_max_pending`` and did not drain in time. Back off and retry.
    """


class AsyncPublishTimeoutError(JetStreamError, builtins.TimeoutError):
    """An async publish received no PubAck within ``publish_async_timeout``.

    Also a `TimeoutError`, so a plain ``except TimeoutError`` works.
    """


@dataclass(slots=True)
class _PendingAck:
    """Bookkeeping for one in-flight async publish, keyed by its reply token."""

    future: "asyncio.Future[PubAck]"
    token: str
    subject: str
    payload: bytes | str
    headers: HeadersInput | None
    reply: str
    retries_remaining: int
    timeout_handle: asyncio.TimerHandle | None = None


class JetStreamContext:
    """Entry point to JetStream. Obtain via `Client.jetstream()`."""

    __slots__ = (
        "_acks",
        "_async_ack_timeout",
        "_async_done",
        "_async_max_pending",
        "_async_prefix",
        "_async_sid",
        "_async_stall",
        "_async_stall_wait",
        "_async_unsub_bus",
        "_client",
        "_prefix",
        "_timeout",
    )

    def __init__(
        self,
        client: "Client",
        *,
        domain: str | None = None,
        api_prefix: str | None = None,
        timeout: float = 5.0,
        publish_async_max_pending: int = _ASYNC_MAX_PENDING,
        publish_async_stall_wait: float = _ASYNC_STALL_WAIT,
        publish_async_timeout: float | None = None,
    ) -> None:
        if domain is not None and api_prefix is not None:
            raise ConfigError("provide either domain or api_prefix, not both")
        self._client = client
        if api_prefix is not None:
            if not api_prefix:
                raise ConfigError("API prefix cannot be empty")
            self._prefix = api_prefix.rstrip(".")
        elif domain is not None:
            if not domain:
                raise ConfigError("domain cannot be empty")
            self._prefix = f"$JS.{domain}.API"
        else:
            self._prefix = "$JS.API"
        self._timeout = timeout

        # -- async publish window --
        if publish_async_max_pending < 1:
            raise ConfigError("publish_async_max_pending must be >= 1")
        if publish_async_stall_wait <= 0:
            raise ConfigError("publish_async_stall_wait must be > 0")
        self._async_max_pending = publish_async_max_pending
        self._async_stall_wait = publish_async_stall_wait
        self._async_ack_timeout = publish_async_timeout
        self._async_prefix: str | None = None  # lazily created reply-inbox prefix
        self._async_sid: int | None = None
        self._async_unsub_bus: Callable[[], None] | None = None
        self._acks: dict[str, _PendingAck] = {}
        self._async_stall: asyncio.Future[None] | None = None
        self._async_done: asyncio.Future[None] | None = None

    @property
    def client(self) -> "Client":
        return self._client

    @property
    def api_prefix(self) -> str:
        return self._prefix

    @property
    def timeout(self) -> float:
        return self._timeout

    # -- API plumbing --------------------------------------------------------

    async def _api_request(
        self,
        endpoint: str,
        payload: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> dict[str, Any]:
        body = b"" if payload is None else json.dumps(payload, separators=(",", ":")).encode()
        try:
            msg = await self._client.request(
                f"{self._prefix}.{endpoint}",
                body,
                timeout=timeout if timeout is not None else self._timeout,
            )
        except NoRespondersError:
            raise JetStreamNotEnabledError(
                "JetStream is not enabled on the server (or not for this account/domain)"
            ) from None
        data: dict[str, Any] = json.loads(msg.payload)
        if "error" in data:
            raise APIError.from_error(data["error"])
        return data

    async def account_info(self) -> AccountInfo:
        return AccountInfo.from_wire(await self._api_request("INFO"))

    async def api_level(self) -> int:
        """The server's advertised JetStream API level (nats-server 2.14.3 reports 4)."""
        info = await self.account_info()
        return info.api.level or 0

    # -- streams -------------------------------------------------------------

    async def create_stream(self, config: StreamConfig) -> Stream:
        validate_stream_name(config.name)
        data = await self._api_request(f"STREAM.CREATE.{config.name}", config.to_wire())
        return Stream(self, StreamInfo.from_wire(data))

    async def update_stream(self, config: StreamConfig) -> Stream:
        validate_stream_name(config.name)
        data = await self._api_request(f"STREAM.UPDATE.{config.name}", config.to_wire())
        return Stream(self, StreamInfo.from_wire(data))

    async def create_or_update_stream(self, config: StreamConfig) -> Stream:
        """Update the stream, creating it when absent (nats.go CreateOrUpdateStream).

        The idempotent way to assert a stream in scripts and services:
        re-running never raises `StreamNameInUseError` — an existing
        stream is updated to ``config`` (a no-op when identical), a missing
        one is created. Update-then-create mirrors nats.go's order, and a
        create that loses a race to a concurrent creator is absorbed by a
        re-update, so the call stays idempotent under contention.
        """
        from .errors import StreamNameInUseError, StreamNotFoundError

        try:
            return await self.update_stream(config)
        except StreamNotFoundError:
            pass
        try:
            return await self.create_stream(config)
        except StreamNameInUseError:
            return await self.update_stream(config)

    async def stream(self, name: str) -> Stream:
        """A handle to an existing stream (fetches and caches its info)."""
        return Stream(self, await self.stream_info(name))

    async def stream_info(self, name: str, *, subjects_filter: str | None = None) -> StreamInfo:
        validate_stream_name(name)
        payload = {"subjects_filter": subjects_filter} if subjects_filter else None
        return StreamInfo.from_wire(await self._api_request(f"STREAM.INFO.{name}", payload))

    async def delete_stream(self, name: str) -> None:
        validate_stream_name(name)
        await self._api_request(f"STREAM.DELETE.{name}")

    async def purge_stream(
        self,
        name: str,
        *,
        subject: str | None = None,
        sequence: int | None = None,
        keep: int | None = None,
    ) -> int:
        """Purge messages; returns the number purged."""
        validate_stream_name(name)
        payload: dict[str, Any] = {}
        if subject is not None:
            payload["filter"] = subject
        if sequence is not None:
            payload["seq"] = sequence
        if keep is not None:
            payload["keep"] = keep
        data = await self._api_request(f"STREAM.PURGE.{name}", payload or None)
        return int(data.get("purged", 0))

    async def stream_names(self, *, subject: str | None = None) -> AsyncIterator[str]:
        offset = 0
        while True:
            payload: dict[str, Any] = {"offset": offset}
            if subject is not None:
                payload["subject"] = subject
            data = await self._api_request("STREAM.NAMES", payload)
            names: list[str] = data.get("streams") or []
            for name in names:
                yield name
            offset += len(names)
            if offset >= int(data.get("total", 0)) or not names:
                return

    async def streams(self, *, subject: str | None = None) -> AsyncIterator[StreamInfo]:
        offset = 0
        while True:
            payload: dict[str, Any] = {"offset": offset}
            if subject is not None:
                payload["subject"] = subject
            data = await self._api_request("STREAM.LIST", payload)
            infos: list[dict[str, Any]] = data.get("streams") or []
            for info in infos:
                yield StreamInfo.from_wire(info)
            offset += len(infos)
            if offset >= int(data.get("total", 0)) or not infos:
                return

    # -- key-value -----------------------------------------------------------

    async def create_key_value(
        self,
        config: "KeyValueConfig",
        *,
        key_codec: "KeyCodec | None" = None,
        value_codec: "ValueCodec | None" = None,
    ) -> "KeyValue":
        """Create a Key-Value bucket.

        Re-creating with an identical configuration is idempotent; an existing
        bucket with a DIFFERENT configuration raises
        `BucketExistsError`.
        """
        from natsio.kv.bucket import KeyValue
        from natsio.kv.errors import BucketExistsError

        from .errors import StreamNameInUseError

        try:
            stream = await self.create_stream(_kv_stream_config(config))
        except StreamNameInUseError:
            raise BucketExistsError(f"bucket {config.bucket!r} already exists with a different configuration") from None
        return KeyValue(
            self,
            config.bucket,
            stream,
            key_codec=key_codec,
            value_codec=value_codec,
        )

    async def update_key_value(
        self,
        config: "KeyValueConfig",
        *,
        key_codec: "KeyCodec | None" = None,
        value_codec: "ValueCodec | None" = None,
    ) -> "KeyValue":
        """Update an existing Key-Value bucket's configuration.

        Runs a STREAM.UPDATE on the mapped backing stream. Raises
        `BucketNotFoundError` when no such bucket exists —
        there is nothing to update.
        """
        from natsio.kv.bucket import KeyValue
        from natsio.kv.errors import BucketNotFoundError

        from .errors import StreamNotFoundError

        try:
            stream = await self.update_stream(_kv_stream_config(config))
        except StreamNotFoundError:
            raise BucketNotFoundError(f"no Key-Value bucket named {config.bucket!r}") from None
        return KeyValue(self, config.bucket, stream, key_codec=key_codec, value_codec=value_codec)

    async def create_or_update_key_value(
        self,
        config: "KeyValueConfig",
        *,
        key_codec: "KeyCodec | None" = None,
        value_codec: "ValueCodec | None" = None,
    ) -> "KeyValue":
        """Create the bucket, or update it in place if it already exists.

        Mirrors nats.go ``CreateOrUpdateStream``: attempt the update first and
        fall back to a create when the backing stream is absent. Race-tolerant
        under concurrent creators (see `create_or_update_stream()`).
        """
        from natsio.kv.bucket import KeyValue

        stream = await self.create_or_update_stream(_kv_stream_config(config))
        return KeyValue(self, config.bucket, stream, key_codec=key_codec, value_codec=value_codec)

    async def key_value(
        self,
        bucket: str,
        *,
        key_codec: "KeyCodec | None" = None,
        value_codec: "ValueCodec | None" = None,
    ) -> "KeyValue":
        """A handle to an existing Key-Value bucket."""
        from natsio.kv.bucket import STREAM_PREFIX, KeyValue
        from natsio.kv.entities import validate_bucket_name
        from natsio.kv.errors import BucketNotFoundError

        validate_bucket_name(bucket)
        from .errors import StreamNotFoundError

        try:
            stream = await self.stream(f"{STREAM_PREFIX}{bucket}")
        except StreamNotFoundError:
            raise BucketNotFoundError(f"no Key-Value bucket named {bucket!r}") from None
        return KeyValue(self, bucket, stream, key_codec=key_codec, value_codec=value_codec)

    async def delete_key_value(self, bucket: str) -> None:
        from natsio.kv.bucket import STREAM_PREFIX
        from natsio.kv.entities import validate_bucket_name
        from natsio.kv.errors import BucketNotFoundError

        validate_bucket_name(bucket)
        from .errors import StreamNotFoundError

        try:
            await self.delete_stream(f"{STREAM_PREFIX}{bucket}")
        except StreamNotFoundError:
            raise BucketNotFoundError(f"no Key-Value bucket named {bucket!r}") from None

    async def key_value_store_names(self) -> AsyncIterator[str]:
        """Yield the name of every Key-Value bucket (``KV_`` prefix stripped).

        Pages STREAM.NAMES filtered to the ``$KV.*.>`` keyspace, then keeps only
        streams carrying the ``KV_`` name prefix — excluding plain streams that
        merely publish on ``$KV`` subjects (matching nats.go's dual filter).
        """
        from natsio.kv.bucket import STREAM_PREFIX, SUBJECT_PREFIX

        async for name in self.stream_names(subject=f"{SUBJECT_PREFIX}.*.>"):
            if name.startswith(STREAM_PREFIX):
                yield name[len(STREAM_PREFIX) :]

    async def key_value_stores(self) -> AsyncIterator["KeyValueStatus"]:
        """Yield a `KeyValueStatus` for every KV bucket.

        Statuses are built straight from the paged STREAM.LIST info, so no extra
        per-bucket round-trip is made. Filtering matches
        `key_value_store_names()`.
        """
        from natsio.kv.bucket import STREAM_PREFIX, SUBJECT_PREFIX

        async for info in self.streams(subject=f"{SUBJECT_PREFIX}.*.>"):
            name = info.config.name
            if name.startswith(STREAM_PREFIX):
                yield _kv_status_from_info(name[len(STREAM_PREFIX) :], info)

    # -- object store --------------------------------------------------------

    async def create_object_store(self, config: "ObjectStoreConfig") -> "ObjectStore":
        """Create an Object Store bucket.

        Re-creating with an identical configuration is idempotent; an existing
        bucket with a DIFFERENT configuration raises
        `BucketExistsError`.
        """
        from natsio.objectstore.errors import BucketExistsError
        from natsio.objectstore.store import ObjectStore

        from .errors import StreamNameInUseError

        try:
            stream = await self.create_stream(_obj_stream_config(config))
        except StreamNameInUseError:
            raise BucketExistsError(f"bucket {config.bucket!r} already exists with a different configuration") from None
        return ObjectStore(self, config.bucket, stream)

    async def update_object_store(self, config: "ObjectStoreConfig") -> "ObjectStore":
        """Update an existing Object Store bucket's configuration.

        Runs a STREAM.UPDATE on the mapped backing stream. Raises
        `BucketNotFoundError` when no such bucket
        exists. The server rejects updates that would violate a sealed stream or
        otherwise change immutable config; those surface as their mapped
        `APIError`.
        """
        from natsio.objectstore.errors import BucketNotFoundError
        from natsio.objectstore.store import ObjectStore

        from .errors import StreamNotFoundError

        try:
            stream = await self.update_stream(_obj_stream_config(config))
        except StreamNotFoundError:
            raise BucketNotFoundError(f"no Object Store bucket named {config.bucket!r}") from None
        return ObjectStore(self, config.bucket, stream)

    async def create_or_update_object_store(self, config: "ObjectStoreConfig") -> "ObjectStore":
        """Create the bucket, or update it in place if it already exists.

        Mirrors nats.go ``CreateOrUpdateStream``: update first, create on absence.
        Race-tolerant under concurrent creators (see
        `create_or_update_stream()`).
        """
        from natsio.objectstore.store import ObjectStore

        stream = await self.create_or_update_stream(_obj_stream_config(config))
        return ObjectStore(self, config.bucket, stream)

    async def object_store(self, bucket: str) -> "ObjectStore":
        """A handle to an existing Object Store bucket."""
        from natsio.objectstore.entities import validate_bucket_name
        from natsio.objectstore.errors import BucketNotFoundError
        from natsio.objectstore.store import STREAM_PREFIX as OBJ_STREAM_PREFIX
        from natsio.objectstore.store import ObjectStore

        validate_bucket_name(bucket)
        from .errors import StreamNotFoundError

        try:
            stream = await self.stream(f"{OBJ_STREAM_PREFIX}{bucket}")
        except StreamNotFoundError:
            raise BucketNotFoundError(f"no Object Store bucket named {bucket!r}") from None
        return ObjectStore(self, bucket, stream)

    async def delete_object_store(self, bucket: str) -> None:
        from natsio.objectstore.entities import validate_bucket_name
        from natsio.objectstore.errors import BucketNotFoundError
        from natsio.objectstore.store import STREAM_PREFIX as OBJ_STREAM_PREFIX

        validate_bucket_name(bucket)
        from .errors import StreamNotFoundError

        try:
            await self.delete_stream(f"{OBJ_STREAM_PREFIX}{bucket}")
        except StreamNotFoundError:
            raise BucketNotFoundError(f"no Object Store bucket named {bucket!r}") from None

    async def object_store_names(self) -> AsyncIterator[str]:
        """Yield the name of every Object Store bucket (``OBJ_`` prefix stripped).

        Pages STREAM.NAMES filtered to the ``$O.*.C.>`` chunk keyspace, then keeps
        only streams carrying the ``OBJ_`` name prefix — excluding plain streams
        that merely publish on ``$O`` subjects (matching nats.go's dual filter).
        """
        from natsio.objectstore.store import STREAM_PREFIX, SUBJECT_PREFIX

        async for name in self.stream_names(subject=f"{SUBJECT_PREFIX}.*.C.>"):
            if name.startswith(STREAM_PREFIX):
                yield name[len(STREAM_PREFIX) :]

    async def object_stores(self) -> AsyncIterator["ObjectStoreStatus"]:
        """Yield an `ObjectStoreStatus` for every bucket.

        Statuses are built straight from the paged STREAM.LIST info, so no extra
        per-bucket round-trip is made. Filtering matches
        `object_store_names()`.
        """
        from natsio.objectstore.store import STREAM_PREFIX, SUBJECT_PREFIX

        async for info in self.streams(subject=f"{SUBJECT_PREFIX}.*.C.>"):
            name = info.config.name
            if name.startswith(STREAM_PREFIX):
                yield _obj_status_from_info(name[len(STREAM_PREFIX) :], info)

    # -- publish -------------------------------------------------------------

    async def publish(
        self,
        subject: str,
        payload: bytes | str = b"",
        *,
        headers: HeadersInput | None = None,
        msg_id: str | None = None,
        expected_stream: str | None = None,
        expected_last_seq: int | None = None,
        expected_last_subject_seq: int | None = None,
        expected_last_subject_seq_subject: str | None = None,
        expected_last_msg_id: str | None = None,
        ttl: js_headers.TTLInput | None = None,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> PubAck:
        """Publish to a stream and await its PubAck.

        ``ttl`` is a ``timedelta``, whole seconds, or ``"never"`` (ADR-43) and
        needs the stream's ``allow_msg_ttl``. A 503 (no stream bound / leader election in
        progress) is retried briefly per ADR-22 before raising
        `NoStreamResponseError`.

        ``expected_last_subject_seq_subject`` (server 2.12+) scopes an
        ``expected_last_subject_seq`` check to a different subject filter — e.g.
        publish to ``a.1`` while asserting the last sequence on ``a.*``.
        """
        validate_subject(subject)
        merged = _merge_headers(
            headers,
            _build_publish_headers(
                msg_id=msg_id,
                expected_stream=expected_stream,
                expected_last_seq=expected_last_seq,
                expected_last_subject_seq=expected_last_subject_seq,
                expected_last_subject_seq_subject=expected_last_subject_seq_subject,
                expected_last_msg_id=expected_last_msg_id,
                ttl=ttl,
            ),
        )

        deadline = timeout if timeout is not None else self._timeout
        attempt = 0
        while True:
            try:
                msg = await self._client.request(subject, payload, headers=merged, timeout=deadline)
                break
            except NoRespondersError:
                attempt += 1
                if attempt > _PUBLISH_RETRY_ATTEMPTS:
                    raise NoStreamResponseError(f"no JetStream stream is listening on {subject!r}") from None
                await asyncio.sleep(_PUBLISH_RETRY_WAIT)
        return _parse_pub_ack(msg)

    # -- async publish -------------------------------------------------------

    async def publish_async(
        self,
        subject: str,
        payload: bytes | str = b"",
        *,
        headers: HeadersInput | None = None,
        msg_id: str | None = None,
        expected_stream: str | None = None,
        expected_last_seq: int | None = None,
        expected_last_subject_seq: int | None = None,
        expected_last_subject_seq_subject: str | None = None,
        expected_last_msg_id: str | None = None,
        ttl: js_headers.TTLInput | None = None,
    ) -> "asyncio.Future[PubAck]":
        """Publish without waiting; return a future that resolves to the PubAck.

        The message is sent immediately with a per-message reply inbox; the
        server's ack is routed back to the returned future. Await the future to
        get the `PubAck` or the failure — an `APIError` (e.g.
        `WrongLastSequenceError`), `NoStreamResponseError` after
        503 retries are exhausted, `AsyncPublishTimeoutError`, or
        `ConnectionClosedError` if the connection drops
        with the ack still outstanding.

        Outstanding acks are capped at ``publish_async_max_pending``. When the
        window is full this call waits up to ``publish_async_stall_wait`` for it
        to drain, then raises `TooManyStalledMsgsError`.

        Header/expectation arguments behave exactly as `publish()`.
        """
        validate_subject(subject)
        merged = _merge_headers(
            headers,
            _build_publish_headers(
                msg_id=msg_id,
                expected_stream=expected_stream,
                expected_last_seq=expected_last_seq,
                expected_last_subject_seq=expected_last_subject_seq,
                expected_last_subject_seq_subject=expected_last_subject_seq_subject,
                expected_last_msg_id=expected_last_msg_id,
                ttl=ttl,
            ),
        )
        self._ensure_async_publisher()
        assert self._async_prefix is not None
        loop = asyncio.get_running_loop()
        token = next_nuid()
        reply = f"{self._async_prefix}.{token}"
        future: asyncio.Future[PubAck] = loop.create_future()
        pending = _PendingAck(
            future=future,
            token=token,
            subject=subject,
            payload=payload,
            headers=merged,
            reply=reply,
            retries_remaining=_PUBLISH_RETRY_ATTEMPTS,
        )
        self._acks[token] = pending

        # Register-then-stall, matching nats.go: the just-added message counts
        # toward the window, so the cap is a soft one under bursty publishing.
        if len(self._acks) > self._async_max_pending:
            if self._async_stall is None:
                self._async_stall = loop.create_future()
            stall = self._async_stall
            try:
                async with asyncio.timeout(self._async_stall_wait):
                    await asyncio.shield(stall)
            except builtins.TimeoutError:
                self._acks.pop(token, None)
                raise TooManyStalledMsgsError(
                    f"{self._async_max_pending} async publishes outstanding and "
                    f"the window did not drain within {self._async_stall_wait}s"
                ) from None

        # A disconnect during the stall wait may have already failed this ack.
        if token not in self._acks:
            return future

        self._arm_ack_timeout(pending)
        try:
            await self._client.publish(subject, payload, reply=reply, headers=merged, _validate_reply=False)
        except Exception:
            removed = self._acks.pop(token, None)
            if removed is not None:
                self._cancel_ack_timeout(removed)
            self._notify_window()
            raise
        return future

    @property
    def publish_async_pending(self) -> int:
        """Number of async publishes still awaiting their PubAck."""
        return len(self._acks)

    async def publish_async_complete(self, timeout: float | None = None) -> None:  # noqa: ASYNC109
        """Wait until every outstanding async publish has resolved (ack or error).

        Returns immediately when the window is already empty. With ``timeout``,
        raises `TimeoutError` if the window has not drained
        in time; the outstanding publishes keep their own futures.
        """
        if not self._acks:
            return
        if self._async_done is None:
            self._async_done = asyncio.get_running_loop().create_future()
        done = self._async_done
        if timeout is None:
            await asyncio.shield(done)
            return
        try:
            async with asyncio.timeout(timeout):
                await asyncio.shield(done)
        except builtins.TimeoutError:
            raise NATSTimeoutError(f"{len(self._acks)} async publish(es) still outstanding after {timeout}s") from None

    def _ensure_async_publisher(self) -> None:
        """Lazily create the shared reply-inbox subscription and lifecycle watch.

        One wildcard subscription per context receives every async ack; replies
        are routed to their per-token future. A bus hook fails all outstanding
        futures on disconnect or close so awaiters never hang (mirrors nats.go
        resetPendingAcksOnReconnect — we fail rather than re-arm, since the ack
        for a pre-disconnect publish is lost and a blind resend could duplicate).
        """
        if self._async_prefix is not None:
            return
        from natsio._internal.lifecycle import Closed, Disconnected

        client = self._client
        conn = client._conn
        prefix = f"{client.inbox_prefix}._js.{next_nuid()}"
        entry = conn.subscribe(f"{prefix}.*", None, self._on_async_reply)
        self._async_sid = entry.sid
        self._async_prefix = prefix

        def on_event(event: object) -> None:
            if isinstance(event, Disconnected):
                self._fail_all(lambda: ConnectionClosedError("connection lost before async ack"))
            elif isinstance(event, Closed):
                self._fail_all(lambda: ConnectionClosedError("connection closed before async ack"))
                if self._async_unsub_bus is not None:
                    self._async_unsub_bus()
                    self._async_unsub_bus = None

        self._async_unsub_bus = conn.bus.subscribe(on_event)

    def _on_async_reply(self, event: Any) -> None:
        token = event.subject.rpartition(".")[2]
        pending = self._acks.get(token)
        if pending is None:
            return
        self._cancel_ack_timeout(pending)
        msg = self._client._build_msg(event)
        if msg.status is not None and msg.status.code == StatusCode.NO_RESPONDERS:
            if pending.retries_remaining > 0:
                pending.retries_remaining -= 1
                self._client._spawn(self._retry_async(token), name="natsio-js-async-retry")
                return
            self._resolve(token, exc=NoStreamResponseError(f"no JetStream stream is listening on {pending.subject!r}"))
            return
        try:
            ack = _parse_pub_ack(msg)
        except Exception as exc:
            self._resolve(token, exc=exc)
            return
        self._resolve(token, ack=ack)

    async def _retry_async(self, token: str) -> None:
        await asyncio.sleep(_PUBLISH_RETRY_WAIT)
        pending = self._acks.get(token)
        if pending is None:
            return
        self._arm_ack_timeout(pending)
        try:
            await self._client.publish(
                pending.subject, pending.payload, reply=pending.reply, headers=pending.headers, _validate_reply=False
            )
        except Exception as exc:
            self._resolve(token, exc=exc)

    def _arm_ack_timeout(self, pending: _PendingAck) -> None:
        if self._async_ack_timeout is None:
            return
        loop = asyncio.get_running_loop()
        pending.timeout_handle = loop.call_later(
            self._async_ack_timeout,
            self._resolve,
            pending.token,
            None,
            AsyncPublishTimeoutError(f"no PubAck within {self._async_ack_timeout}s"),
        )

    @staticmethod
    def _cancel_ack_timeout(pending: _PendingAck) -> None:
        if pending.timeout_handle is not None:
            pending.timeout_handle.cancel()
            pending.timeout_handle = None

    def _resolve(self, token: str, ack: PubAck | None = None, exc: BaseException | None = None) -> None:
        pending = self._acks.pop(token, None)
        if pending is None:
            return
        self._cancel_ack_timeout(pending)
        if not pending.future.done():
            if exc is not None:
                pending.future.set_exception(exc)
            else:
                assert ack is not None
                pending.future.set_result(ack)
        self._notify_window()

    def _fail_all(self, error_factory: Callable[[], BaseException]) -> None:
        for token in list(self._acks):
            pending = self._acks.pop(token, None)
            if pending is None:
                continue
            self._cancel_ack_timeout(pending)
            if not pending.future.done():
                pending.future.set_exception(error_factory())
        self._notify_window()

    def _notify_window(self) -> None:
        """Wake anyone stalled on window space or waiting for a full drain."""
        if self._async_stall is not None and not self._async_stall.done() and len(self._acks) < self._async_max_pending:
            self._async_stall.set_result(None)
            self._async_stall = None
        if self._async_done is not None and not self._async_done.done() and not self._acks:
            self._async_done.set_result(None)
            self._async_done = None


def _build_publish_headers(
    *,
    msg_id: str | None,
    expected_stream: str | None,
    expected_last_seq: int | None,
    expected_last_subject_seq: int | None,
    expected_last_subject_seq_subject: str | None,
    expected_last_msg_id: str | None,
    ttl: js_headers.TTLInput | None,
) -> dict[str, str]:
    """The ``Nats-*`` publish-expectation headers implied by the keyword args.

    Shared by sync and async publish so both speak the identical wire contract.
    """
    extra: dict[str, str] = {}
    if msg_id is not None:
        extra[js_headers.MSG_ID] = msg_id
    if expected_stream is not None:
        extra[js_headers.EXPECTED_STREAM] = expected_stream
    if expected_last_seq is not None:
        extra[js_headers.EXPECTED_LAST_SEQUENCE] = str(expected_last_seq)
    if expected_last_subject_seq is not None:
        extra[js_headers.EXPECTED_LAST_SUBJECT_SEQUENCE] = str(expected_last_subject_seq)
    if expected_last_subject_seq_subject is not None:
        if not expected_last_subject_seq_subject:
            raise ConfigError("expected_last_subject_seq_subject cannot be empty")
        if expected_last_subject_seq is None:
            # nats.go couples the pair in one option; alone, the server just
            # rejects the request with a less actionable error.
            raise ConfigError("expected_last_subject_seq_subject requires expected_last_subject_seq")
        extra[js_headers.EXPECTED_LAST_SUBJECT_SEQUENCE_SUBJECT] = expected_last_subject_seq_subject
    if expected_last_msg_id is not None:
        extra[js_headers.EXPECTED_LAST_MSG_ID] = expected_last_msg_id
    if ttl is not None:
        extra[js_headers.TTL] = js_headers.encode_ttl(ttl)
    return extra


def _merge_headers(headers: HeadersInput | None, extra: dict[str, str]) -> HeadersInput | None:
    if not extra:
        return headers
    if headers is None:
        return extra
    from natsio._internal.protocol import Headers

    merged = Headers(headers)
    for key, value in extra.items():
        merged.set(key, value)
    return merged


def _parse_pub_ack(msg: Msg) -> PubAck:
    data: dict[str, Any] = json.loads(msg.payload)
    if "error" in data:
        raise APIError.from_error(data["error"])
    return PubAck.from_wire(data)


def _kv_status_from_info(bucket: str, info: StreamInfo) -> "KeyValueStatus":
    """Build a bucket status from already-fetched stream info (no round-trip).

    Kept in sync with ``KeyValue.status()`` — same fields, same normalization.
    """
    from natsio.kv.entities import KeyValueStatus

    config = info.config
    return KeyValueStatus(
        bucket=bucket,
        values=info.state.messages,
        history=config.max_msgs_per_subject,  # -1 = unlimited (foreign buckets)
        # The server echoes max_age=0 for "never expires"; normalize.
        ttl=config.max_age or None,
        bytes=info.state.bytes,
        storage=config.storage,
        stream_info=info,
    )


def _obj_status_from_info(bucket: str, info: StreamInfo) -> "ObjectStoreStatus":
    """Build a bucket status from already-fetched stream info (no round-trip).

    Kept in sync with ``ObjectStore.status()`` — same fields, same normalization.
    """
    from natsio.objectstore.entities import ObjectStoreStatus

    config = info.config
    return ObjectStoreStatus(
        bucket=bucket,
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


def _kv_stream_config(config: "KeyValueConfig") -> StreamConfig:
    """Map a bucket config onto its ADR-8 backing stream."""
    from datetime import timedelta

    from natsio.jetstream.entities import StorageCompression
    from natsio.kv.bucket import STREAM_PREFIX, SUBJECT_PREFIX

    duplicate_window = timedelta(minutes=2)
    if config.ttl is not None and timedelta(0) < config.ttl < duplicate_window:
        duplicate_window = config.ttl
    return StreamConfig(
        name=f"{STREAM_PREFIX}{config.bucket}",
        description=config.description,
        subjects=[f"{SUBJECT_PREFIX}.{config.bucket}.>"],
        max_msgs_per_subject=config.history,
        max_bytes=config.max_bytes,
        max_age=config.ttl if config.ttl and config.ttl > timedelta(0) else None,
        max_msg_size=config.max_value_size,
        storage=config.storage,
        num_replicas=config.replicas,
        placement=config.placement,
        republish=config.republish,
        discard=DiscardPolicy.NEW,
        duplicate_window=duplicate_window,
        allow_rollup_hdrs=True,
        deny_delete=True,
        allow_direct=True,
        compression=StorageCompression.S2 if config.compression else None,
        metadata=config.metadata,
        allow_msg_ttl=True if (config.allow_msg_ttl or config.limit_marker_ttl is not None) else None,
        subject_delete_marker_ttl=config.limit_marker_ttl,
    )


def _obj_stream_config(config: "ObjectStoreConfig") -> StreamConfig:
    """Map a bucket config onto its ADR-20 backing stream."""
    from datetime import timedelta

    from natsio.jetstream.entities import StorageCompression
    from natsio.objectstore.store import STREAM_PREFIX, SUBJECT_PREFIX

    duplicate_window = timedelta(minutes=2)
    if config.ttl is not None and timedelta(0) < config.ttl < duplicate_window:
        duplicate_window = config.ttl
    return StreamConfig(
        name=f"{STREAM_PREFIX}{config.bucket}",
        description=config.description,
        subjects=[
            f"{SUBJECT_PREFIX}.{config.bucket}.C.>",
            f"{SUBJECT_PREFIX}.{config.bucket}.M.>",
        ],
        max_bytes=config.max_bytes,
        max_age=config.ttl if config.ttl and config.ttl > timedelta(0) else None,
        storage=config.storage,
        num_replicas=config.replicas,
        placement=config.placement,
        discard=DiscardPolicy.NEW,
        duplicate_window=duplicate_window,
        allow_rollup_hdrs=True,
        allow_direct=True,
        compression=StorageCompression.S2 if config.compression else None,
        metadata=config.metadata,
    )
