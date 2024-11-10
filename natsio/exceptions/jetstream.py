from typing import Any, Mapping
from natsio.exceptions.base import NATSError
from natsio.protocol.headers import Header, StatusCode


class APIError(NATSError):
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


class NotFoundError(APIError):
    pass


class BadRequestError(APIError):
    pass
