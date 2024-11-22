from typing import TYPE_CHECKING, Any, Mapping
from natsio.exceptions.base import NATSError
from natsio.exceptions.protocol import NoRespondersError
from natsio.protocol.headers import Header, StatusCode

if TYPE_CHECKING:
    from natsio.client.jetstream.key_value import Entry


class JetStreamError(NATSError):
    pass


class APIError(JetStreamError):
    def __init__(
        self,
        code: int | None = None,
        description: str | None = None,
        err_code: int | None = None,
        stream: str | None = None,
        seq: int | None = None,
    ) -> None:
        self.code = code
        self.err_code = err_code
        self.description = description or ""
        self.stream = stream
        self.seq = seq

    @classmethod
    def from_error(cls, **data: Any) -> "APIError":
        if "code" not in data:
            return cls(**data)
        code = data["code"]
        err_class: type[APIError]
        if code == 400:
            err_class = BadRequestError
        elif code == 404:
            err_class = NotFoundError
        elif code == 500:
            err_class = ServerError
        elif code == 503:
            err_class = ServiceUnavailableError
        else:
            err_class = APIError
        return err_class(**data)

    @classmethod
    def from_msg_headers(cls, headers: Mapping[str, str] | None) -> "APIError":
        if not headers:
            return cls()
        if Header.STATUS in headers and StatusCode.SERVICE_UNAVAILABLE:
            return ServiceUnavailableError(code=503)  # pyright: ignore[reportReturnType]
        return cls(code=int(headers[Header.STATUS]), description=headers[Header.DESCRIPTION])

    def __str__(self) -> str:
        return (
            f"NATS: {self.__class__.__name__}: "
            f"code={self.code} "
            f"err_code={self.err_code} "
            f"description={self.description}"
        )


class ServiceUnavailableError(APIError):
    pass


class ServerError(APIError):
    pass


class NotFoundError(APIError, LookupError):
    pass


class BadRequestError(APIError):
    pass


class NoStreamResponseError(APIError, NoRespondersError):
    description = "No response from stream"


class ConsumerSequenceMismatchError(JetStreamError):
    def __init__(
        self,
        stream_resume_sequence: int,
        consumer_sequence: int,
        last_consumer_sequence: int,
    ) -> None:
        self.stream_resume_sequence = stream_resume_sequence
        self.consumer_sequence = consumer_sequence
        self.last_consumer_sequence = last_consumer_sequence

    def __str__(self) -> str:
        gap = self.last_consumer_sequence - self.consumer_sequence
        return (
            f"NATS: sequence mismatch for consumer at sequence {self.consumer_sequence} "
            f"({gap} sequences behind), should restart consumer from stream sequence {self.stream_resume_sequence}"
        )


class BucketError(JetStreamError):
    pass


class KeyValueError(JetStreamError):
    pass


class BucketNotFoundError(BucketError, NotFoundError):
    pass


class BadBucketError(BucketError):
    pass


class KeyDeletedError(KeyValueError, NotFoundError):
    def __init__(self, entry: "Entry") -> None:
        self.entry = entry

    def __str__(self) -> str:
        return f"NATS: key ({self.entry.key}) was deleted"


class KeyNotFoundError(KeyValueError, NotFoundError):
    def __init__(self, key: str, message: str | None = None) -> None:
        self.key = key
        self.message = message

    def __str__(self) -> str:
        s = "NATS: key not found"
        if self.message:
            s += f": {self.message}"
        return s


class KeyWrongLastSequenceError(KeyValueError, BadRequestError):
    def __init__(self, description: str) -> None:
        self.description = description

    def __str__(self) -> str:
        return f"NATS: {self.description}"


class NoKeysError(KeyValueError, NotFoundError):
    def __str__(self) -> str:
        return "NATS: no keys found"


class KeyHistoryTooLargeError(KeyValueError, ValueError):
    def __str__(self) -> str:
        return "NATS: history limited to a max of 64"


class InvalidBucketNameError(BadBucketError, NameError):
    pass
