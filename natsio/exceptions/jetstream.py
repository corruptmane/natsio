from typing import Any
from natsio.exceptions.base import NATSError


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
        match code:
            case 503:
                err_class = ServiceUnavailableError
            case 500:
                err_class = ServerError
            case 404:
                err_class = NotFoundError
            case 400:
                err_class = BadRequestError
            case _:
                err_class = APIError
        return err_class(**data)

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
