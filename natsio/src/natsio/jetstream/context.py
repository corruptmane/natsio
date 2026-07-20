"""JetStreamContext: `$JS.API` plumbing, stream CRUD, and JetStream publish."""

import asyncio
import json
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from natsio._internal.protocol import HeadersInput
from natsio._internal.validation import validate_subject
from natsio.errors import ConfigError, NoRespondersError
from natsio.message import Msg

if TYPE_CHECKING:
    from natsio.client import Client

from . import headers as js_headers
from .entities import AccountInfo, PubAck, StreamConfig, StreamInfo
from .errors import APIError, JetStreamNotEnabledError, NoStreamResponseError
from .stream import Stream

__all__ = ["JetStreamContext"]

_PUBLISH_RETRY_ATTEMPTS = 2
_PUBLISH_RETRY_WAIT = 0.25


class JetStreamContext:
    """Entry point to JetStream. Obtain via :meth:`Client.jetstream`."""

    __slots__ = ("_client", "_prefix", "_timeout")

    def __init__(
        self,
        client: "Client",
        *,
        domain: str | None = None,
        api_prefix: str | None = None,
        timeout: float = 5.0,
    ) -> None:
        if domain is not None and api_prefix is not None:
            raise ConfigError("provide either domain or api_prefix, not both")
        self._client = client
        if api_prefix is not None:
            self._prefix = api_prefix.rstrip(".")
        elif domain is not None:
            self._prefix = f"$JS.{domain}.API"
        else:
            self._prefix = "$JS.API"
        self._timeout = timeout

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
        if not config.name:
            raise ConfigError("stream config must carry a name")
        data = await self._api_request(f"STREAM.CREATE.{config.name}", config.to_wire())
        return Stream(self, StreamInfo.from_wire(data))

    async def update_stream(self, config: StreamConfig) -> Stream:
        if not config.name:
            raise ConfigError("stream config must carry a name")
        data = await self._api_request(f"STREAM.UPDATE.{config.name}", config.to_wire())
        return Stream(self, StreamInfo.from_wire(data))

    async def stream(self, name: str) -> Stream:
        """A handle to an existing stream (fetches and caches its info)."""
        return Stream(self, await self.stream_info(name))

    async def stream_info(self, name: str, *, subjects_filter: str | None = None) -> StreamInfo:
        payload = {"subjects_filter": subjects_filter} if subjects_filter else None
        return StreamInfo.from_wire(await self._api_request(f"STREAM.INFO.{name}", payload))

    async def delete_stream(self, name: str) -> None:
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

    async def streams(self) -> AsyncIterator[StreamInfo]:
        offset = 0
        while True:
            data = await self._api_request("STREAM.LIST", {"offset": offset})
            infos: list[dict[str, Any]] = data.get("streams") or []
            for info in infos:
                yield StreamInfo.from_wire(info)
            offset += len(infos)
            if offset >= int(data.get("total", 0)) or not infos:
                return

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
        expected_last_msg_id: str | None = None,
        ttl: int | str | None = None,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> PubAck:
        """Publish to a stream and await its PubAck.

        ``ttl`` is whole seconds (or the string ``"never"``) per ADR-43 and
        needs the stream's ``allow_msg_ttl``. A 503 (no stream bound / leader election in
        progress) is retried briefly per ADR-22 before raising
        :class:`NoStreamResponseError`.
        """
        validate_subject(subject)
        extra: dict[str, str] = {}
        if msg_id is not None:
            extra[js_headers.MSG_ID] = msg_id
        if expected_stream is not None:
            extra[js_headers.EXPECTED_STREAM] = expected_stream
        if expected_last_seq is not None:
            extra[js_headers.EXPECTED_LAST_SEQUENCE] = str(expected_last_seq)
        if expected_last_subject_seq is not None:
            extra[js_headers.EXPECTED_LAST_SUBJECT_SEQUENCE] = str(expected_last_subject_seq)
        if expected_last_msg_id is not None:
            extra[js_headers.EXPECTED_LAST_MSG_ID] = expected_last_msg_id
        if ttl is not None:
            if isinstance(ttl, str):
                extra[js_headers.TTL] = ttl
            else:
                if ttl < 1:
                    raise ConfigError("ttl is whole seconds and must be >= 1 (or the string 'never')")
                extra[js_headers.TTL] = str(ttl)
        merged = _merge_headers(headers, extra)

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
