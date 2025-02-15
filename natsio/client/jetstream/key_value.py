import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, MutableMapping, Self, cast
from dataclasses import dataclass

from natsio.exceptions.jetstream import (
    APIError,
    KeyDeletedError,
    KeyNotFoundError,
    KeyWrongLastSequenceError,
    NoKeysError,
    NotFoundError,
)
from natsio.exceptions.subscription import MessageRetrievalTimeoutError
from natsio.messages.jetstream import JetStreamMsg, Metadata
from natsio.protocol.headers import Header, KVOperation, Rollup
from natsio.subscriptions.jetstream import OrderedPushSubscription

from .entities import DeliverPolicy, GetMsgRequest, PushConsumerConfig, RawMsg
from .utils import RawMsgGetter


if TYPE_CHECKING:
    from .client import JetStream


@dataclass
class Entry:
    bucket: str
    key: str
    value: bytes | None
    revision: int | None
    created: datetime
    operation: KVOperation | None


def build_entry_from_raw_msg(msg: RawMsg, bucket_name: str, key: str) -> Entry:
    operation = None
    if msg.headers and Header.KV_OPERATION in msg.headers:
        operation = KVOperation(msg.headers[Header.KV_OPERATION])

    return Entry(
        bucket=bucket_name,
        key=key,
        value=msg.payload,
        revision=msg.seq,
        created=msg.time,
        operation=operation,
    )


def build_entry_from_js_msg(
    msg: JetStreamMsg, bucket_name: str, subject_prefix_length: int
) -> Entry:
    meta = cast(Metadata, msg.metadata)
    operation = None
    if msg.headers and Header.KV_OPERATION in msg.headers:
        operation = KVOperation(msg.headers[Header.KV_OPERATION])

    return Entry(
        bucket=bucket_name,
        key=msg.subject[subject_prefix_length:],
        value=msg.payload,
        revision=meta.stream_seq,
        created=meta.timestamp,
        operation=operation,
    )


class KeyValueWatcher:
    def __init__(
        self,
        jetstream: "JetStream",
        stream_name: str,
        bucket_name: str,
        key: str,
        subject_prefix_length: int,
        include_history: bool = False,
        ignore_deletes: bool = False,
        meta_only: bool = False,
    ) -> None:
        self._js = jetstream
        self._stream_name = stream_name
        self._bucket_name = bucket_name
        self._key = key
        self._subject_prefix_length = subject_prefix_length
        self._include_history = include_history
        self._ignore_deletes = ignore_deletes
        self._meta_only = meta_only
        self._sub: OrderedPushSubscription
        self._is_last_entry: bool = False

    async def start(
        self,
        filter_subject: str,
        inactive_threshold: timedelta | None = None,
    ) -> None:
        if not self._include_history:
            deliver_policy = DeliverPolicy.last_per_subject
        else:
            deliver_policy = DeliverPolicy.all

        if inactive_threshold is None:
            inactive_threshold = timedelta(minutes=5)

        config = PushConsumerConfig(
            deliver_policy=deliver_policy,
            deliver_subject=self._js.new_unique_deliver_subject(),
            filter_subject=filter_subject,
            inactive_threshold=inactive_threshold,
            headers_only=self._meta_only,
        )
        self._sub = await self._js.ordered_push_subscribe(
            stream_name=self._stream_name,
            consumer_config=config,
        )
        await asyncio.sleep(0)

    async def stop(self) -> None:
        await self._sub.unsubscribe()

    def _return_none_if_filtered(self, entry: Entry) -> Entry | None:
        if (
            self._ignore_deletes
            and entry.operation
            and entry.operation in (KVOperation.DEL, KVOperation.PURGE)
        ):
            return

    async def next_entry(self, timeout: float | int = 5) -> Entry:
        entry: Entry | None = None

        try:
            async with asyncio.timeout(timeout):
                while entry is None:
                    msg = await self._sub.next_msg(timeout=timeout)
                    entry = self._return_none_if_filtered(
                        build_entry_from_js_msg(
                            msg, self._bucket_name, self._subject_prefix_length
                        )
                    )
        except asyncio.TimeoutError:
            raise MessageRetrievalTimeoutError()

        return entry

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Entry:
        entry: Entry | None = None

        while entry is None:
            if self._is_last_entry:
                raise StopAsyncIteration()

            msg = await self._sub.next_msg(timeout=None)
            if msg.metadata and msg.metadata.num_pending == 0:
                self._is_last_entry = True

            entry = self._return_none_if_filtered(
                build_entry_from_js_msg(
                    msg, self._bucket_name, self._subject_prefix_length
                )
            )

        return entry


class KeyValue:
    def __init__(
        self,
        bucket_name: str,
        stream_name: str,
        pre: str,
        jetstream: "JetStream",
        raw_msg_getter: RawMsgGetter,
    ) -> None:
        self._bucket_name = bucket_name
        self._stream_name = stream_name
        self._pre = pre
        self._js = jetstream
        self._get_msg = raw_msg_getter

    def _build_key_subject(self, key: str) -> str:
        return f"{self._pre}{key}"

    async def get(self, key: str, revision: int | None = None) -> Entry:
        req = GetMsgRequest()
        subject = self._build_key_subject(key)
        if not revision:
            req.last_by_subj = subject
        else:
            req.seq = revision

        try:
            msg = await self._get_msg(self._stream_name, req)
        except NotFoundError:
            raise KeyNotFoundError(key=key)

        if subject != msg.subject:
            raise KeyNotFoundError(
                key=key,
                message=f"expected '{subject}', but got '{msg.subject}'",
            )

        entry = build_entry_from_raw_msg(msg, self._bucket_name, key)

        if (
            msg.headers
            and Header.KV_OPERATION in msg.headers
            and msg.headers[Header.KV_OPERATION]
            in (KVOperation.DEL, KVOperation.PURGE)
        ):
            raise KeyDeletedError(entry)

        return entry

    async def put(self, key: str, value: bytes) -> int:
        ack = await self._js.publish(
            subject=self._build_key_subject(key), data=value
        )
        return ack.seq

    async def update(
        self, key: str, value: bytes, last: int | None = None
    ) -> int:
        if not last:
            last = 0
        headers = {Header.EXPECTED_LAST_SUBJECT_SEQUENCE.value: last}

        try:
            ack = await self._js.publish(
                subject=self._build_key_subject(key),
                data=value,
                headers=headers,
            )
        except APIError as exc:
            if exc.err_code and exc.err_code == 10071:
                raise KeyWrongLastSequenceError(exc.description)
            raise exc

        return ack.seq

    async def create(self, key: str, value: bytes) -> int:
        try:
            revision = await self.update(key, value, 0)
        except KeyWrongLastSequenceError as exc:
            # NOTE: it is possible that key was deleted, so if it is so, we can actually create new entry
            try:
                await self.get(key)
                raise exc
            except KeyDeletedError as exc:
                revision = await self.update(
                    key, value, last=exc.entry.revision
                )

        return revision

    async def delete(self, key: str, last: int | None = None) -> bool:
        headers: MutableMapping[str, str | int] = {
            Header.KV_OPERATION.value: KVOperation.DEL.value
        }
        if last is not None and last > 0:
            headers[Header.EXPECTED_LAST_SUBJECT_SEQUENCE.value] = last

        await self._js.publish(
            subject=self._build_key_subject(key), data=b"", headers=headers
        )
        return True

    async def purge(self, key: str) -> bool:
        headers = {
            Header.KV_OPERATION.value: KVOperation.PURGE.value,
            Header.ROLLUP.value: Rollup.SUB,
        }

        await self._js.publish(
            subject=self._build_key_subject(key), data=b"", headers=headers
        )
        return True

    async def watch(
        self,
        key: str,  # TODO: implement use of `filter_subjects` config option for push subscription
        include_history: bool = False,
        ignore_deletes: bool = False,
        meta_only: bool = False,
        inactive_threshold: timedelta | None = None,
    ) -> KeyValueWatcher:
        subject = self._build_key_subject(key)
        watcher = KeyValueWatcher(
            jetstream=self._js,
            stream_name=self._stream_name,
            bucket_name=self._bucket_name,
            key=key,
            subject_prefix_length=len(self._pre),
            include_history=include_history,
            ignore_deletes=ignore_deletes,
            meta_only=meta_only,
        )

        await watcher.start(
            filter_subject=subject,
            inactive_threshold=inactive_threshold,
        )

        return watcher

    async def watch_all(
        self,
        include_history: bool = False,
        ignore_deletes: bool = False,
        meta_only: bool = False,
        inactive_threshold: timedelta | None = None,
    ) -> KeyValueWatcher:
        return await self.watch(
            key=">",
            include_history=include_history,
            ignore_deletes=ignore_deletes,
            meta_only=meta_only,
            inactive_threshold=inactive_threshold,
        )

    async def purge_deletes(self, older_than_seconds: int = 30 * 60) -> bool:
        watcher = await self.watch_all(meta_only=True)

        delete_markers: list[Entry] = []
        async for update in watcher:
            if update.operation in (KVOperation.DEL, KVOperation.PURGE):
                delete_markers.append(update)

        await watcher.stop()

        for entry in delete_markers:
            keep = 0
            subject = f"{self._pre}{entry.key}"
            duration = datetime.now(timezone.utc) - entry.created
            if (
                older_than_seconds > 0
                and older_than_seconds > duration.total_seconds()
            ):
                keep = 1
            await self._js.purge_stream(
                self._stream_name, filter_subject=subject, keep=keep
            )
        return True

    async def history(self, key: str) -> list[Entry]:
        watcher = await self.watch(key, include_history=True)

        entries: list[Entry] = []
        async for entry in watcher:
            entries.append(entry)

        await watcher.stop()

        if not entries:
            raise NoKeysError()

        return entries

    async def keys(self, filters: list[str] | None = None) -> list[str]:
        watcher = await self.watch_all(ignore_deletes=True, meta_only=True)

        keys: list[str] = []
        async for entry in watcher:
            if filters:
                if any(
                    entry.key in f for f in filters
                ):  # TODO: rework filters
                    keys.append(entry.key)
            else:
                keys.append(entry.key)

        await watcher.stop()

        if not keys:
            raise NoKeysError()

        return keys
