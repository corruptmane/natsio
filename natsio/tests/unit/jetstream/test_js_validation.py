"""Client-side stream/consumer name validation (nats.go parity).

A dotted or otherwise illegal name is interpolated into a single-token API
subject that can never match, so the request would hang the full JS timeout and
raise a misleading TimeoutError. These check the name fails fast in the caller's
frame instead — mirroring nats.go's validateStreamName / validateConsumerName.
"""

from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from natsio.client import Client
from natsio.errors import ConfigError
from natsio.jetstream import ConsumerConfig, JetStreamContext, StreamConfig
from natsio.jetstream.entities import StreamInfo
from natsio.jetstream.stream import Stream

# The nats.go jetstream illegal set is ">*. /\\\t\r\n"; empty is rejected too.
_ILLEGAL_NAMES = [
    "foo.123",
    "foo bar",
    "foo*",
    "foo>",
    "foo/bar",
    "foo\\bar",
    "foo\tbar",
    "foo\rbar",
    "foo\nbar",
]
_ILLEGAL_OR_EMPTY = [*_ILLEGAL_NAMES, ""]


class _ExplodingClient:
    """Fails any network call loudly, proving validation runs before the request."""

    async def request(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError("validation must reject the name before any request is sent")


def _ctx() -> JetStreamContext:
    return JetStreamContext(cast(Client, _ExplodingClient()))


def _stream() -> Stream:
    return Stream(_ctx(), StreamInfo(config=StreamConfig(name="OK")))


@pytest.mark.parametrize("name", _ILLEGAL_OR_EMPTY)
class TestStreamNameValidation:
    async def test_create_stream(self, name: str) -> None:
        with pytest.raises(ConfigError):
            await _ctx().create_stream(StreamConfig(name=name))

    async def test_update_stream(self, name: str) -> None:
        with pytest.raises(ConfigError):
            await _ctx().update_stream(StreamConfig(name=name))

    async def test_stream_info(self, name: str) -> None:
        with pytest.raises(ConfigError):
            await _ctx().stream_info(name)

    async def test_delete_stream(self, name: str) -> None:
        with pytest.raises(ConfigError):
            await _ctx().delete_stream(name)

    async def test_purge_stream(self, name: str) -> None:
        with pytest.raises(ConfigError):
            await _ctx().purge_stream(name)

    async def test_stream_handle(self, name: str) -> None:
        with pytest.raises(ConfigError):
            await _ctx().stream(name)


@pytest.mark.parametrize("name", _ILLEGAL_NAMES)
class TestConsumerNameValidation:
    async def test_create_consumer_durable(self, name: str) -> None:
        with pytest.raises(ConfigError):
            await _stream().create_consumer(ConsumerConfig(durable_name=name))

    async def test_create_consumer_name(self, name: str) -> None:
        with pytest.raises(ConfigError):
            await _stream().create_consumer(ConsumerConfig(name=name))

    async def test_consumer_info(self, name: str) -> None:
        with pytest.raises(ConfigError):
            await _stream().consumer_info(name)

    async def test_consumer_handle(self, name: str) -> None:
        with pytest.raises(ConfigError):
            await _stream().consumer(name)

    async def test_delete_consumer(self, name: str) -> None:
        with pytest.raises(ConfigError):
            await _stream().delete_consumer(name)

    async def test_pause_consumer(self, name: str) -> None:
        with pytest.raises(ConfigError):
            await _stream().pause_consumer(name, datetime.now(tz=UTC) + timedelta(hours=1))

    async def test_resume_consumer(self, name: str) -> None:
        with pytest.raises(ConfigError):
            await _stream().resume_consumer(name)


class TestConsumerEphemeralNameStaysValid:
    async def test_empty_durable_generates_ephemeral(self) -> None:
        # nats.go generates a name when neither name nor durable is set, then
        # validates it — the empty case must not raise (it goes on to the server).
        with pytest.raises(AssertionError, match="before any request"):
            await _stream().create_consumer(ConsumerConfig())


class TestContextPrefixValidation:
    def test_empty_api_prefix_rejected(self) -> None:
        with pytest.raises(ConfigError, match="API prefix cannot be empty"):
            JetStreamContext(cast(Client, _ExplodingClient()), api_prefix="")

    def test_empty_domain_rejected(self) -> None:
        with pytest.raises(ConfigError, match="domain cannot be empty"):
            JetStreamContext(cast(Client, _ExplodingClient()), domain="")

    def test_domain_and_api_prefix_mutually_exclusive(self) -> None:
        with pytest.raises(ConfigError):
            JetStreamContext(cast(Client, _ExplodingClient()), domain="hub", api_prefix="$JS.API")

    def test_valid_domain_builds_prefix(self) -> None:
        js = JetStreamContext(cast(Client, _ExplodingClient()), domain="hub")
        assert js.api_prefix == "$JS.hub.API"
