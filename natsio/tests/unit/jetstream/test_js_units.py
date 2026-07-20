from datetime import UTC, datetime

import pytest

from natsio.errors import NATSError
from natsio.jetstream import (
    APIError,
    ConsumerNotFoundError,
    JetStreamNotEnabledError,
    NoMessagesError,
    StreamNotFoundError,
    WrongLastSequenceError,
)
from natsio.jetstream.errors import error_for, register_error
from natsio.jetstream.message import AckMetadata


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
        with pytest.raises(NATSError, match="not a JetStream ack"):
            AckMetadata.from_reply("_INBOX.abc.1")
