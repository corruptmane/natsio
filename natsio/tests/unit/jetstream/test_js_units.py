from datetime import UTC, datetime

import pytest

from natsio.errors import ConfigError
from natsio.jetstream import (
    APIError,
    ConsumerNotFoundError,
    JetStreamNotEnabledError,
    NoMessagesError,
    StreamNotFoundError,
    WrongLastSequenceError,
)
from natsio.jetstream.consumer import Consumer
from natsio.jetstream.errors import error_for, register_error
from natsio.jetstream.message import AckMetadata, NotJSMessageError


class TestErrorMapping:
    @pytest.mark.parametrize(
        ("err_code", "expected"),
        [
            (10059, StreamNotFoundError),
            (10014, ConsumerNotFoundError),
            (10071, WrongLastSequenceError),
            (10076, JetStreamNotEnabledError),
            (99999, APIError),  # unknown err_code falls back to the base
        ],
    )
    def test_err_code_dispatch(self, err_code: int, expected: type[APIError]) -> None:
        error = APIError.from_error({"code": 404, "err_code": err_code, "description": "boom"})
        assert type(error) is expected
        assert error.err_code == err_code
        assert error.code == 404
        assert error.description == "boom"

    def test_unmapped_err_code_preserved_on_base(self) -> None:
        # An err_code with no dedicated class must still round-trip on the base
        # APIError (guards the registry fallthrough — no silent zeroing).
        error = APIError.from_error({"code": 400, "err_code": 10070, "description": "bad request"})
        assert type(error) is APIError
        assert error.err_code == 10070
        assert error.code == 400
        assert error.description == "bad request"

    def test_registry_is_extensible(self) -> None:
        class CustomError(APIError):
            pass

        register_error(77777, CustomError)
        try:
            assert error_for(77777) is CustomError
            assert type(APIError.from_error({"err_code": 77777})) is CustomError
        finally:
            register_error(77777, APIError)

    def test_no_messages_error_is_a_timeout(self) -> None:
        with pytest.raises(TimeoutError):
            raise NoMessagesError("empty")


class TestAckMetadata:
    def test_v1_nine_tokens(self) -> None:
        reply = "$JS.ACK.ORDERS.worker.1.100.42.1752969600000000000.5"
        meta = AckMetadata.from_reply(reply)
        assert meta.stream == "ORDERS"
        assert meta.consumer == "worker"
        assert meta.num_delivered == 1
        assert meta.stream_seq == 100
        assert meta.consumer_seq == 42
        assert meta.num_pending == 5
        assert meta.domain is None
        assert meta.timestamp == datetime.fromtimestamp(1752969600, tz=UTC)

    def test_v2_twelve_tokens_with_domain(self) -> None:
        reply = "$JS.ACK.hub.AcctHash.ORDERS.worker.2.200.84.1752969600000000000.0.rand"
        meta = AckMetadata.from_reply(reply)
        assert meta.domain == "hub"
        assert meta.stream == "ORDERS"
        assert meta.consumer == "worker"
        assert (meta.num_delivered, meta.stream_seq, meta.consumer_seq) == (2, 200, 84)

    def test_v2_underscore_domain_means_none(self) -> None:
        reply = "$JS.ACK._.AcctHash.S.c.1.1.1.1752969600000000000.0.x"
        assert AckMetadata.from_reply(reply).domain is None

    def test_non_ack_reply_rejected(self) -> None:
        with pytest.raises(NotJSMessageError, match="not a JetStream ack"):
            AckMetadata.from_reply("_INBOX.abc.1")

    def test_ten_token_reply_rejected(self) -> None:
        # $JS.ACK + 8 fields = 10 tokens: neither v1 (9) nor v2 (12+). Used to
        # raise a raw ValueError from int() on a misaligned token.
        reply = "$JS.ACK.hub.AcctHash.ORDERS.worker.2.200.84.1752969600000000000"
        assert len(reply.split(".")) == 10
        with pytest.raises(NotJSMessageError):
            AckMetadata.from_reply(reply)

    def test_eleven_token_reply_rejected(self) -> None:
        reply = "$JS.ACK.hub.AcctHash.ORDERS.worker.2.200.84.1752969600000000000.0"
        assert len(reply.split(".")) == 11
        with pytest.raises(NotJSMessageError):
            AckMetadata.from_reply(reply)


class TestFetchValidation:
    """fetch() must reject invalid options up front (mirrors ErrInvalidOption)."""

    @pytest.mark.parametrize("batch", [0, -1, -100])
    async def test_nonpositive_batch_rejected(self, batch: int) -> None:
        # Validation runs before any client access, so an uninitialised handle
        # is enough to exercise the guard.
        consumer = Consumer.__new__(Consumer)
        with pytest.raises(ConfigError, match="positive"):
            await consumer.fetch(batch)

    @pytest.mark.parametrize("bad_timeout", [-0.5, -1.0])
    async def test_negative_timeout_rejected(self, bad_timeout: float) -> None:
        consumer = Consumer.__new__(Consumer)
        with pytest.raises(ConfigError, match="negative"):
            await consumer.fetch(5, timeout=bad_timeout)
