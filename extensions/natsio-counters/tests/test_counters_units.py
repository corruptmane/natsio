"""Wire-contract and unit tests for natsio-counters — no server needed.

Header names and payload shapes are pinned byte-for-byte against the ADR-49 /
orbit.go oracle: ``Nats-Incr``, ``Nats-Counter-Sources``, and ``{"val": "..."}``.
"""

import pytest
from natsio.counters import (  # ty: ignore[unresolved-import]
    COUNTER_INCREMENT_HEADER,
    COUNTER_SOURCES_HEADER,
    Counter,
    CounterConfig,
    CounterNotEnabledError,
    DirectAccessRequiredError,
    InvalidCounterValueError,
    counter_from_stream,
    parse_counter_value,
    parse_sources,
)

from natsio._internal.protocol import Headers
from natsio.errors import ConfigError
from natsio.jetstream.entities import StreamConfig, StreamInfo
from natsio.jetstream.stream import Stream


class TestWireContractConstants:
    """The exact strings that go on the wire (pinned to the oracle)."""

    def test_increment_header(self) -> None:
        assert COUNTER_INCREMENT_HEADER == "Nats-Incr"

    def test_sources_header(self) -> None:
        assert COUNTER_SOURCES_HEADER == "Nats-Counter-Sources"


class TestParseCounterValue:
    """Mirrors orbit.go ``TestParseCounterValue``."""

    @pytest.mark.parametrize(
        ("data", "expected"),
        [
            (b'{"val":"42"}', 42),
            (b'{"val":"-10"}', -10),
            (b'{"val":"0"}', 0),
            (b'{"val":"123456789012345678901234567890"}', 123456789012345678901234567890),
        ],
    )
    def test_valid(self, data: bytes, expected: int) -> None:
        assert parse_counter_value(data) == expected

    @pytest.mark.parametrize(
        "data",
        [
            b'{"val": invalid}',  # malformed JSON
            b'{"other":"42"}',  # missing val field
            b'{"val":"not-a-number"}',  # non-integer
            b'{"val":"1.5"}',  # float string is not an integer
            b"",  # empty
        ],
    )
    def test_invalid(self, data: bytes) -> None:
        with pytest.raises(InvalidCounterValueError):
            parse_counter_value(data)

    def test_arbitrary_precision(self) -> None:
        """Beyond 2**53 — the reason the wire value is a string, not a number."""
        big = 2**80 + 1
        assert parse_counter_value(b'{"val":"%d"}' % big) == big


class TestParseSources:
    """Mirrors orbit.go ``TestParseSources``."""

    def test_empty_headers(self) -> None:
        assert parse_sources(Headers()) is None

    def test_none_headers(self) -> None:
        assert parse_sources(None) is None

    def test_single_source_single_subject(self) -> None:
        headers = Headers({COUNTER_SOURCES_HEADER: '{"source1":{"subject1":"10"}}'})
        assert parse_sources(headers) == {"source1": {"subject1": 10}}

    def test_single_source_multiple_subjects(self) -> None:
        headers = Headers({COUNTER_SOURCES_HEADER: '{"source1":{"subject1":"10","subject2":"20"}}'})
        assert parse_sources(headers) == {"source1": {"subject1": 10, "subject2": 20}}

    def test_multiple_sources(self) -> None:
        headers = Headers(
            {COUNTER_SOURCES_HEADER: '{"source1":{"subject1":"10"},"source2":{"subject2":"20","subject3":"90"}}'}
        )
        assert parse_sources(headers) == {
            "source1": {"subject1": 10},
            "source2": {"subject2": 20, "subject3": 90},
        }

    @pytest.mark.parametrize(
        "raw",
        [
            "{",  # malformed JSON
            '{"source1":{"subject1":"not-a-number"}}',  # non-integer contribution
        ],
    )
    def test_invalid(self, raw: str) -> None:
        with pytest.raises(InvalidCounterValueError):
            parse_sources(Headers({COUNTER_SOURCES_HEADER: raw}))


def _stream_with(config: StreamConfig) -> Stream:
    """A Stream handle over a config, without any server round-trip.

    The Counter constructor only reads ``stream.cached_info.config``, so a
    ``None`` context is enough to exercise its validation.
    """
    return Stream(None, StreamInfo(config=config))  # ty: ignore[invalid-argument-type]


class TestCounterConstructorValidation:
    def test_rejects_non_counter_stream(self) -> None:
        stream = _stream_with(StreamConfig(name="PLAIN", subjects=["x.>"], allow_direct=True))
        with pytest.raises(CounterNotEnabledError):
            counter_from_stream(None, stream)

    def test_rejects_missing_direct(self) -> None:
        stream = _stream_with(StreamConfig(name="C", subjects=["x.>"], allow_msg_counter=True))
        with pytest.raises(DirectAccessRequiredError):
            counter_from_stream(None, stream)

    def test_accepts_counter_stream(self) -> None:
        stream = _stream_with(StreamConfig(name="C", subjects=["x.>"], allow_msg_counter=True, allow_direct=True))
        counter = counter_from_stream(None, stream)
        assert isinstance(counter, Counter)
        assert counter.name == "C"


class TestCounterConfig:
    def test_requires_subjects(self) -> None:
        with pytest.raises(ConfigError):
            CounterConfig(name="C", subjects=[])

    def test_defaults(self) -> None:
        config = CounterConfig(name="C", subjects=["events.>"])
        assert config.replicas == 1
        assert config.max_bytes == -1
        assert config.compression is False
