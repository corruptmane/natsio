"""Stream handle: consumer lifecycle and stored-message operations."""

import base64
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from natsio._internal.jsonmodel import RFC3339
from natsio._internal.nuid import next_nuid
from natsio._internal.protocol import Headers, parse_header_block
from natsio.errors import ConfigError, NoRespondersError

if TYPE_CHECKING:
    from .context import JetStreamContext

from . import headers as js_headers
from .consumer import Consumer, OrderedConsumer
from .entities import (
    AckPolicy,
    ConsumerConfig,
    ConsumerInfo,
    DeliverPolicy,
    ReplayPolicy,
    StreamInfo,
)
from .errors import MessageNotFoundError

__all__ = ["StoredMsg", "Stream"]


@dataclass(frozen=True, slots=True)
class StoredMsg:
    """A message read from a stream (direct get or STREAM.MSG.GET)."""

    subject: str
    seq: int
    payload: bytes
    time: datetime | None = None
    headers: Headers | None = None


class Stream:
    """A thin, stateful handle to one stream.

    ``cached_info`` is populated when the handle is created and refreshed only
    by an explicit :meth:`info` call.
    """

    __slots__ = ("_ctx", "cached_info")

    def __init__(self, ctx: "JetStreamContext", info: StreamInfo) -> None:
        self._ctx = ctx
        self.cached_info = info

    @property
    def name(self) -> str:
        return self.cached_info.config.name

    def __repr__(self) -> str:
        return f"Stream(name={self.name!r})"

    async def info(self, *, subjects_filter: str | None = None) -> StreamInfo:
        self.cached_info = await self._ctx.stream_info(self.name, subjects_filter=subjects_filter)
        return self.cached_info

    async def purge(
        self,
        *,
        subject: str | None = None,
        sequence: int | None = None,
        keep: int | None = None,
    ) -> int:
        return await self._ctx.purge_stream(self.name, subject=subject, sequence=sequence, keep=keep)

    # -- consumers -----------------------------------------------------------

    async def create_consumer(self, config: ConsumerConfig | None = None) -> Consumer:
        """Create (or idempotently assert) a consumer and return its handle.

        A ``durable_name`` (or ``name``) makes it durable; otherwise a name is
        generated client-side and the consumer is ephemeral (set
        ``inactive_threshold`` to bound its lifetime).
        """
        # Structural copy: never mutate the caller's config (a reused config
        # with a written-back generated name would silently collapse every
        # subsequent create into the same consumer — probe-confirmed).
        config = ConsumerConfig.from_wire(config.to_wire()) if config is not None else ConsumerConfig()
        name = config.name or config.durable_name
        if name is None:
            name = next_nuid()
            config.name = name
        endpoint = f"CONSUMER.CREATE.{self.name}.{name}"
        if config.filter_subject and "*" not in config.filter_subject and ">" not in config.filter_subject:
            endpoint += f".{config.filter_subject}"
        payload = {
            "stream_name": self.name,
            "config": config.to_wire(),
            "action": "",
        }
        data = await self._ctx._api_request(endpoint, payload)
        return Consumer(self, ConsumerInfo.from_wire(data))

    async def consumer(self, name: str) -> Consumer:
        """A handle to an existing consumer (fetches and caches its info)."""
        return Consumer(self, await self.consumer_info(name))

    async def consumer_info(self, name: str) -> ConsumerInfo:
        data = await self._ctx._api_request(f"CONSUMER.INFO.{self.name}.{name}")
        return ConsumerInfo.from_wire(data)

    async def delete_consumer(self, name: str) -> None:
        await self._ctx._api_request(f"CONSUMER.DELETE.{self.name}.{name}")

    async def pause_consumer(self, name: str, until: datetime) -> None:
        """Pause delivery until ``until`` (server 2.11+)."""
        await self._ctx._api_request(f"CONSUMER.PAUSE.{self.name}.{name}", {"pause_until": RFC3339.to_wire(until)})

    async def resume_consumer(self, name: str) -> None:
        await self._ctx._api_request(f"CONSUMER.PAUSE.{self.name}.{name}", {})

    async def consumer_names(self) -> AsyncIterator[str]:
        offset = 0
        while True:
            data = await self._ctx._api_request(f"CONSUMER.NAMES.{self.name}", {"offset": offset})
            names: list[str] = data.get("consumers") or []
            for name in names:
                yield name
            offset += len(names)
            if offset >= int(data.get("total", 0)) or not names:
                return

    def ordered_consumer(
        self,
        *,
        filter_subjects: list[str] | None = None,
        deliver_policy: DeliverPolicy = DeliverPolicy.ALL,
        opt_start_seq: int | None = None,
        opt_start_time: datetime | None = None,
        headers_only: bool = False,
    ) -> OrderedConsumer:
        """A single-threaded, always-in-order view of the stream (ADR-17).

        Ephemeral and self-healing: on a gap, a missed heartbeat, or consumer
        loss it recreates itself at the next unseen stream sequence.
        """
        base = ConsumerConfig(
            deliver_policy=deliver_policy,
            opt_start_seq=opt_start_seq,
            opt_start_time=opt_start_time,
            ack_policy=AckPolicy.NONE,
            replay_policy=ReplayPolicy.INSTANT,
            filter_subjects=filter_subjects,
            headers_only=headers_only or None,
            num_replicas=1,
            mem_storage=True,
        )
        return OrderedConsumer(self, base)

    # -- stored messages -----------------------------------------------------

    async def get_msg(
        self,
        sequence: int | None = None,
        *,
        subject: str | None = None,
        next_for: bool = False,
    ) -> StoredMsg:
        """Read one stored message by sequence or (last-by / next-from) subject.

        Uses Direct Get when the stream allows it (the 2.14-era default read
        path), else the ``STREAM.MSG.GET`` API.
        """
        if sequence is None and subject is None:
            raise ConfigError("provide sequence and/or subject")
        if self.cached_info.config.allow_direct:
            return await self._direct_get(sequence, subject, next_for)
        return await self._api_get(sequence, subject, next_for)

    async def delete_msg(self, sequence: int, *, no_erase: bool = False) -> None:
        payload: dict[str, Any] = {"seq": sequence}
        if no_erase:
            payload["no_erase"] = True
        await self._ctx._api_request(f"STREAM.MSG.DELETE.{self.name}", payload)

    async def _direct_get(self, sequence: int | None, subject: str | None, next_for: bool) -> StoredMsg:
        request: dict[str, Any] = {}
        if next_for and subject is not None:
            request["seq"] = sequence if sequence is not None else 0
            request["next_by_subj"] = subject
        elif subject is not None:
            request["last_by_subj"] = subject
        else:
            request["seq"] = sequence
        try:
            msg = await self._ctx.client.request(
                f"{self._ctx.api_prefix}.DIRECT.GET.{self.name}",
                json.dumps(request).encode(),
                timeout=self._ctx.timeout,
            )
        except NoRespondersError:
            raise MessageNotFoundError("direct get is not available for this stream") from None
        if msg.status is not None and msg.status.code != 200:
            raise MessageNotFoundError(f"no message matched ({msg.status.code} {msg.status.description})")
        headers = msg.headers if msg.headers is not None else Headers()
        time_raw = headers.get(js_headers.TIME_STAMP)
        return StoredMsg(
            subject=headers.get(js_headers.SUBJECT) or msg.subject,
            seq=int(headers.get(js_headers.SEQUENCE) or 0),
            payload=msg.payload,
            time=RFC3339.from_wire(time_raw) if time_raw else None,
            headers=headers,
        )

    async def _api_get(self, sequence: int | None, subject: str | None, next_for: bool) -> StoredMsg:
        request: dict[str, Any] = {}
        if next_for and subject is not None:
            request["seq"] = sequence if sequence is not None else 0
            request["next_by_subj"] = subject
        elif subject is not None:
            request["last_by_subj"] = subject
        else:
            request["seq"] = sequence
        data = await self._ctx._api_request(f"STREAM.MSG.GET.{self.name}", request)
        message: dict[str, Any] = data["message"]
        headers = None
        raw_headers = message.get("hdrs")
        if raw_headers:
            headers, _status = parse_header_block(base64.b64decode(raw_headers))
        time_raw = message.get("time")
        return StoredMsg(
            subject=str(message.get("subject", "")),
            seq=int(message.get("seq", 0)),
            payload=base64.b64decode(message.get("data", "")),
            time=RFC3339.from_wire(time_raw) if time_raw else None,
            headers=headers,
        )
