"""Unit tests for batch Direct Get (`Stream.get_last_msgs_for`) — no server.

Crafts ``DIRECT.GET`` reply frames (data frames plus the ``204 EOB`` terminator)
through a fake client, then asserts the ``multi_last`` request-body shape and the
per-frame parsing/termination without a live nats-server.
"""

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest

from natsio._internal.protocol import Headers, InlineStatus
from natsio.errors import ConfigError
from natsio.jetstream import headers as js_headers
from natsio.jetstream.entities import StreamConfig, StreamInfo
from natsio.jetstream.stream import StoredMsg, Stream
from natsio.message import Msg


def _data_frame(subject: str, seq: int, payload: bytes, *, time: str | None = None, num_pending: str = "0") -> Msg:
    hdrs = Headers()
    hdrs.add(js_headers.STREAM, "S")
    hdrs.add(js_headers.SUBJECT, subject)
    hdrs.add(js_headers.SEQUENCE, str(seq))
    hdrs.add(js_headers.NUM_PENDING, num_pending)
    if time is not None:
        hdrs.add(js_headers.TIME_STAMP, time)
    return Msg(subject="_INBOX.reply", payload=payload, headers=hdrs, status=None)


def _eob_frame() -> Msg:
    # The 204 "EOB" status frame that terminates a batch (ADR-31).
    return Msg(subject="_INBOX.reply", payload=b"", headers=None, status=InlineStatus(204, "EOB"))


def _not_found_frame() -> Msg:
    return Msg(subject="_INBOX.reply", payload=b"", headers=Headers(), status=InlineStatus(404, "Message Not Found"))


class _FakeClient:
    """Replays a fixed list of frames and records every request_many call."""

    def __init__(self, frames: list[Msg]) -> None:
        self._frames = frames
        self.requests: list[tuple[str, bytes, float | None]] = []

    async def request_many(
        self,
        subject: str,
        payload: bytes = b"",
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        max_msgs: int | None = None,
        stall: float | None = None,
        headers: object = None,
    ) -> AsyncIterator[Msg]:
        self.requests.append((subject, payload, timeout))
        for frame in self._frames:
            yield frame


class _FakeCtx:
    def __init__(self, client: _FakeClient, api_prefix: str = "$JS.API") -> None:
        self.client = client
        self.api_prefix = api_prefix
        self.timeout = 5.0


def _stream(
    frames: list[Msg],
    *,
    allow_direct: bool = True,
    name: str = "S",
    api_prefix: str = "$JS.API",
) -> tuple[Stream, _FakeClient]:
    client = _FakeClient(frames)
    ctx = _FakeCtx(client, api_prefix)
    config = StreamConfig(name=name, subjects=["events.>"], allow_direct=allow_direct)
    stream = Stream(ctx, StreamInfo(config=config))  # ty: ignore[invalid-argument-type]
    return stream, client


async def _collect(stream: Stream, *args: object, **kwargs: object) -> list[StoredMsg]:
    return [msg async for msg in stream.get_last_msgs_for(*args, **kwargs)]  # ty: ignore[invalid-argument-type]


class TestRequestBody:
    async def test_multi_last_body_and_endpoint(self) -> None:
        stream, client = _stream([_eob_frame()])
        await _collect(stream, ["events.a", "events.b"])
        subject, body, timeout = client.requests[0]
        assert subject == "$JS.API.DIRECT.GET.S"
        assert json.loads(body) == {"multi_last": ["events.a", "events.b"]}
        assert timeout == 5.0

    async def test_string_subject_is_wrapped(self) -> None:
        stream, client = _stream([_eob_frame()])
        await _collect(stream, "events.only")
        assert json.loads(client.requests[0][1]) == {"multi_last": ["events.only"]}

    async def test_optional_knobs(self) -> None:
        stream, client = _stream([_eob_frame()])
        await _collect(stream, ["events.a"], batch=10, up_to_seq=42)
        assert json.loads(client.requests[0][1]) == {"multi_last": ["events.a"], "batch": 10, "up_to_seq": 42}

    async def test_up_to_time_serialized(self) -> None:
        stream, client = _stream([_eob_frame()])
        when = datetime(2026, 7, 22, 12, 0, 0, tzinfo=UTC)
        await _collect(stream, ["events.a"], up_to_time=when)
        body = json.loads(client.requests[0][1])
        assert body["multi_last"] == ["events.a"]
        assert "up_to_time" in body and body["up_to_time"].startswith("2026-07-22T12:00:00")

    async def test_respects_domain_api_prefix(self) -> None:
        stream, client = _stream([_eob_frame()], api_prefix="$JS.hub.API")
        await _collect(stream, ["events.a"])
        assert client.requests[0][0] == "$JS.hub.API.DIRECT.GET.S"


class TestResponseParsing:
    async def test_parses_frames_and_stops_on_eob(self) -> None:
        # The frame *after* the EOB must never be yielded.
        frames = [
            _data_frame("events.a", 1, b'{"val":"5"}', num_pending="1"),
            _data_frame("events.b", 2, b'{"val":"9"}'),
            _eob_frame(),
            _data_frame("events.leak", 3, b"nope"),
        ]
        stream, _ = _stream(frames)
        out = await _collect(stream, ["events.>"])
        assert [(m.subject, m.seq, m.payload) for m in out] == [
            ("events.a", 1, b'{"val":"5"}'),
            ("events.b", 2, b'{"val":"9"}'),
        ]

    async def test_skips_404_subject_miss(self) -> None:
        frames = [
            _data_frame("events.a", 1, b'{"val":"5"}'),
            _not_found_frame(),
            _data_frame("events.b", 2, b'{"val":"9"}'),
            _eob_frame(),
        ]
        stream, _ = _stream(frames)
        out = await _collect(stream, ["events.a", "events.missing", "events.b"])
        assert [m.subject for m in out] == ["events.a", "events.b"]

    async def test_parses_timestamp(self) -> None:
        frames = [_data_frame("events.a", 1, b"x", time="2026-07-22T12:00:00.5Z"), _eob_frame()]
        stream, _ = _stream(frames)
        (msg,) = await _collect(stream, ["events.a"])
        assert isinstance(msg.time, datetime)

    async def test_empty_batch_yields_nothing(self) -> None:
        stream, _ = _stream([_eob_frame()])
        assert await _collect(stream, ["events.a"]) == []


class TestValidation:
    async def test_empty_subjects_raise(self) -> None:
        stream, _ = _stream([_eob_frame()])
        with pytest.raises(ConfigError):
            await _collect(stream, [])

    async def test_non_direct_stream_raises(self) -> None:
        stream, _ = _stream([_eob_frame()], allow_direct=False)
        with pytest.raises(ConfigError):
            await _collect(stream, ["events.a"])

    async def test_up_to_seq_and_time_mutually_exclusive(self) -> None:
        stream, _ = _stream([_eob_frame()])
        with pytest.raises(ConfigError):
            await _collect(stream, ["events.a"], up_to_seq=1, up_to_time=datetime.now(UTC))
