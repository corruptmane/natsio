"""End-to-end batch direct get against a real nats-server (pinned 2.14.3)."""

from datetime import UTC, datetime, timedelta

import pytest
from natsio.jetstream_batch import (  # ty: ignore[unresolved-import]
    BatchGetError,
    BatchGetIncompleteError,
    get_batch,
)

from natsio.jetstream.context import JetStreamContext
from natsio.jetstream.entities import StreamConfig
from natsio.jetstream.stream import Stream


@pytest.fixture
async def stream(js: JetStreamContext) -> Stream:
    """Ten messages on ``get.0``/``get.1``/``get.2``, sequences 1..10."""
    created = await js.create_stream(StreamConfig(name="GET", subjects=["get.>"], allow_direct=True))
    for index in range(10):
        await js.publish(f"get.{index % 3}", f"p{index}".encode())
    return created


class TestGetBatch:
    async def test_from_the_start(self, js: JetStreamContext, stream: Stream) -> None:
        messages = [msg async for msg in get_batch(js, stream, 3)]
        assert [msg.seq for msg in messages] == [1, 2, 3]
        assert [msg.payload for msg in messages] == [b"p0", b"p1", b"p2"]
        assert messages[0].subject == "get.0"
        assert messages[0].time is not None

    async def test_from_a_sequence(self, js: JetStreamContext, stream: Stream) -> None:
        messages = [msg async for msg in get_batch(js, stream, 4, seq=7)]
        assert [msg.seq for msg in messages] == [7, 8, 9, 10]

    async def test_batch_caps_the_result(self, js: JetStreamContext, stream: Stream) -> None:
        assert len([msg async for msg in get_batch(js, stream, 2)]) == 2

    async def test_by_subject_filter(self, js: JetStreamContext, stream: Stream) -> None:
        messages = [msg async for msg in get_batch(js, stream, 10, subject="get.1")]
        assert [msg.seq for msg in messages] == [2, 5, 8]
        assert {msg.subject for msg in messages} == {"get.1"}

    async def test_by_wildcard_subject(self, js: JetStreamContext, stream: Stream) -> None:
        messages = [msg async for msg in get_batch(js, stream, 4, subject="get.*")]
        assert [msg.seq for msg in messages] == [1, 2, 3, 4]

    async def test_by_start_time(self, js: JetStreamContext, stream: Stream) -> None:
        # Everything was published moments ago, so an hour back covers it all;
        # an hour ahead covers nothing.
        past = datetime.now(UTC) - timedelta(hours=1)
        assert len([msg async for msg in get_batch(js, stream, 10, start_time=past)]) == 10

        future = datetime.now(UTC) + timedelta(hours=1)
        assert [msg async for msg in get_batch(js, stream, 10, start_time=future)] == []

    async def test_start_time_between_publishes(self, js: JetStreamContext, stream: Stream) -> None:
        third = await anext(aiter(get_batch(js, stream, 1, seq=3)))
        assert third.time is not None
        messages = [msg async for msg in get_batch(js, stream, 3, start_time=third.time)]
        assert [msg.seq for msg in messages] == [3, 4, 5]

    async def test_max_bytes_stops_early(self, js: JetStreamContext, stream: Stream) -> None:
        # Two 2-byte payloads plus their headers overrun a 10-byte budget, so the
        # server stops before the full batch of 10.
        messages = [msg async for msg in get_batch(js, stream, 10, max_bytes=10)]
        assert 0 < len(messages) < 10

    async def test_empty_beyond_the_end(self, js: JetStreamContext, stream: Stream) -> None:
        assert [msg async for msg in get_batch(js, stream, 5, seq=9999)] == []

    async def test_empty_for_an_unmatched_subject(self, js: JetStreamContext, stream: Stream) -> None:
        assert [msg async for msg in get_batch(js, stream, 5, subject="get.nope")] == []

    async def test_accepts_a_stream_name(self, js: JetStreamContext, stream: Stream) -> None:
        messages = [msg async for msg in get_batch(js, "GET", 2)]
        assert [msg.seq for msg in messages] == [1, 2]

    async def test_await_is_optional(self, js: JetStreamContext, stream: Stream) -> None:
        reader = await get_batch(js, stream, 2)
        assert [msg.seq async for msg in reader] == [1, 2]

    async def test_headers_carry_the_stored_metadata(self, js: JetStreamContext, stream: Stream) -> None:
        first = await anext(aiter(get_batch(js, stream, 1)))
        assert first.headers is not None
        assert first.headers.get("Nats-Stream") == "GET"
        assert first.headers.get("Nats-Num-Pending") is not None

    async def test_stream_without_allow_direct_is_refused(self, js: JetStreamContext) -> None:
        await js.create_stream(StreamConfig(name="NODIRECT", subjects=["nd.>"]))
        await js.publish("nd.1", b"x")
        with pytest.raises(BatchGetError):
            [msg async for msg in get_batch(js, "NODIRECT", 5)]

    async def test_missing_terminator_is_loud(self, js: JetStreamContext, stream: Stream) -> None:
        # A handle outliving its stream: nothing answers, so the read produces
        # no EOB. Silence must not look like an empty result.
        await js.delete_stream("GET")
        with pytest.raises(BatchGetIncompleteError):
            [msg async for msg in get_batch(js, stream, 5, timeout=0.5)]
