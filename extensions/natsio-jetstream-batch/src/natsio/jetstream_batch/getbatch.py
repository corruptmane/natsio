"""Batch direct get: many stored messages from one request.

One request to ``$JS.API.DIRECT.GET.<stream>`` streams back up to ``batch``
messages, terminated by a ``204 EOB`` frame. This is the "from a sequence, a
time, or the lowest match" half of the batch-get API; the "last message per
subject" half already lives in core as
`natsio.jetstream.Stream.get_last_msgs_for`.
"""

import json
from collections.abc import AsyncIterator, Generator
from datetime import datetime
from typing import Any, Final, Self

from natsio._internal.jsonmodel import RFC3339
from natsio._internal.protocol import Headers, StatusCode
from natsio.errors import ConfigError
from natsio.jetstream import headers as js_headers
from natsio.jetstream.context import JetStreamContext
from natsio.jetstream.stream import StoredMsg, Stream
from natsio.message import Msg

from .errors import BatchGetError, BatchGetIncompleteError, BatchGetUnsupportedError

__all__ = ["BatchGet", "get_batch"]

_EOB_STATUS: Final = 204
"""``204 EOB`` closes a batch (ADR-31). Not in core's `StatusCode`."""


class BatchGet:
    """A prepared batch read. Iterate it to run the request.

    Returned by `get_batch`; the request body is fixed at construction (and
    exposed as `request` for tests and debugging), nothing is sent until
    iteration starts.
    """

    __slots__ = ("_js", "_request", "_stream", "_timeout")

    def __init__(
        self,
        js: JetStreamContext,
        stream: Stream | str,
        request: dict[str, Any],
        *,
        timeout: float | None = None,
    ) -> None:
        self._js = js
        self._stream = stream
        self._request = request
        self._timeout = timeout if timeout is not None else js.timeout

    @property
    def request(self) -> dict[str, Any]:
        """The JSON body that will be sent to ``$JS.API.DIRECT.GET.<stream>``."""
        return dict(self._request)

    @property
    def stream_name(self) -> str:
        return self._stream if isinstance(self._stream, str) else self._stream.name

    def __repr__(self) -> str:
        return f"BatchGet(stream={self.stream_name!r}, request={self._request!r})"

    def __await__(self) -> Generator[None, None, Self]:
        """``await`` is optional and completes immediately: preparing the read
        does no I/O (the request goes out when iteration starts)."""
        return self
        yield  # unreachable: makes this a generator that never suspends

    def __aiter__(self) -> AsyncIterator[StoredMsg]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[StoredMsg]:
        stream = self._stream
        if isinstance(stream, str):
            # A name costs one STREAM.INFO round-trip to check allow_direct;
            # pass the Stream handle to skip it.
            stream = await self._js.stream(stream)
        if not stream.cached_info.config.allow_direct:
            raise BatchGetError(
                f"batch get requires a stream created with allow_direct=True; {stream.name!r} does not allow direct get"
            )
        body = json.dumps(self._request, separators=(",", ":")).encode()
        endpoint = f"{self._js.api_prefix}.DIRECT.GET.{stream.name}"
        async for msg in self._js.client.request_many(endpoint, body, timeout=self._timeout):
            if msg.status is not None:
                if msg.status.code in (_EOB_STATUS, StatusCode.NOT_FOUND):
                    # 204 EOB ends a batch; 404 is the empty answer — no message
                    # matched the request at all. Either way the answer is whole.
                    return
                raise BatchGetError(f"batch get failed: {msg.status.code} {msg.status.description}")
            yield _stored_from_direct(msg)
        # Falling out of the loop is the only way here: request_many completes on
        # its deadline as well as on the batch terminator, so silence means a
        # truncated read, not the end.
        raise BatchGetIncompleteError(
            f"batch get on {stream.name!r} ended without the server's EOB marker "
            f"(no reply for {self._timeout}s) — the messages read so far are a partial answer"
        )


def get_batch(
    js: JetStreamContext,
    stream: Stream | str,
    batch: int,
    *,
    seq: int | None = None,
    start_time: datetime | None = None,
    subject: str | None = None,
    max_bytes: int | None = None,
    timeout: float | None = None,
) -> BatchGet:
    """Read up to ``batch`` stored messages in one round-trip.

        async for stored in get_batch(js, "ORDERS", 10, seq=100):
            print(stored.seq, stored.subject, stored.payload)

    Options are validated here, so a bad request fails in the caller's frame;
    no I/O happens until iteration starts (``await`` is therefore optional).

    - ``seq`` starts at that stream sequence, ``start_time`` at that timestamp;
      they are mutually exclusive and default to the start of the stream.
    - ``subject`` keeps only messages matching that filter, wildcards included.
    - ``max_bytes`` caps the total payload the server will send back; it stops
      early rather than exceeding it.
    - ``timeout`` bounds the whole batch (default: the context's timeout). A
      batch that does not finish inside it raises `BatchGetIncompleteError`
      instead of looking like a short read.

    The backing stream must have ``allow_direct=True``. Passing a `Stream`
    handle checks that locally; passing a name costs one ``STREAM.INFO`` call.
    """
    if batch < 1:
        raise ConfigError(f"batch must be >= 1, got {batch}")
    if seq is not None and start_time is not None:
        raise ConfigError("provide seq or start_time, not both")
    if seq is not None and seq < 1:
        raise ConfigError(f"seq must be >= 1, got {seq}")
    if max_bytes is not None and max_bytes < 1:
        raise ConfigError(f"max_bytes must be >= 1, got {max_bytes}")
    if subject is not None and not subject:
        raise ConfigError("subject filter must not be empty")
    if isinstance(stream, Stream) and not stream.cached_info.config.allow_direct:
        raise BatchGetError(
            f"batch get requires a stream created with allow_direct=True; {stream.name!r} does not allow direct get"
        )

    request: dict[str, Any] = {}
    # Key order is the orbit.go struct order (seq, next_by_subj, batch,
    # max_bytes, start_time); the server does not care, but a byte-stable body
    # keeps the wire-contract tests meaningful.
    if start_time is None:
        request["seq"] = seq if seq is not None else 1
    if subject is not None:
        request["next_by_subj"] = subject
    request["batch"] = batch
    if max_bytes is not None:
        request["max_bytes"] = max_bytes
    if start_time is not None:
        request["start_time"] = RFC3339.to_wire(start_time)
    return BatchGet(js, stream, request, timeout=timeout)


def _stored_from_direct(msg: Msg) -> StoredMsg:
    """Convert one Direct Get reply frame into a `StoredMsg`.

    Same header contract as core's single-message direct get, plus the
    ``Nats-Num-Pending`` check that tells a real batch response apart from a
    single message returned by a server without batch support.
    """
    headers = msg.headers if msg.headers is not None else Headers()
    if headers.get(js_headers.NUM_PENDING) is None:
        raise BatchGetUnsupportedError(
            "batch get response is missing Nats-Num-Pending; this server does not support batch direct get"
        )
    time_raw = headers.get(js_headers.TIME_STAMP)
    return StoredMsg(
        subject=headers.get(js_headers.SUBJECT) or msg.subject,
        seq=int(headers.get(js_headers.SEQUENCE) or 0),
        payload=msg.payload,
        time=RFC3339.from_wire(time_raw) if time_raw else None,
        headers=headers,
    )
