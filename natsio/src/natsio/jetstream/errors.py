"""JetStream error hierarchy.

The JS API reports failures as ``{"error": {"code", "err_code", "description"}}``
(ADR-1). Mapping is keyed on ``err_code`` — the stable, specific registry from
ADR-7 — with the HTTP-ish ``code`` kept for context only.
"""

from typing import Any

from natsio.errors import NATSError

__all__ = [
    "APIError",
    "ConsumerDeletedError",
    "ConsumerNotFoundError",
    "JetStreamError",
    "JetStreamNotEnabledError",
    "MessageNotFoundError",
    "NoMessagesError",
    "NoStreamResponseError",
    "StreamNameInUseError",
    "StreamNotFoundError",
    "WrongLastSequenceError",
]


class JetStreamError(NATSError):
    """Root for everything JetStream-specific."""


class NoStreamResponseError(JetStreamError):
    """A JetStream publish got no PubAck — no stream is bound to the subject."""


class ConsumerDeletedError(JetStreamError):
    """The consumer disappeared while we were pulling from it (409 status)."""


class NoMessagesError(JetStreamError, TimeoutError):
    """``next()`` found no message within its deadline.

    Subclasses `TimeoutError`, so a plain ``except TimeoutError`` works.
    """


class APIError(JetStreamError):
    """An error object returned by the ``$JS.API`` request/response API."""

    def __init__(self, description: str = "", *, code: int = 0, err_code: int = 0) -> None:
        super().__init__(description)
        self.code = code
        self.err_code = err_code

    def __repr__(self) -> str:
        return f"{type(self).__name__}(code={self.code}, err_code={self.err_code}, description={self.description!r})"

    @classmethod
    def from_error(cls, error: dict[str, Any]) -> "APIError":
        err_code = int(error.get("err_code", 0))
        exc_type = _BY_ERR_CODE.get(err_code, APIError)
        return exc_type(
            str(error.get("description", "")),
            code=int(error.get("code", 0)),
            err_code=err_code,
        )


class StreamNotFoundError(APIError):
    pass


class StreamNameInUseError(APIError):
    pass


class ConsumerNotFoundError(APIError):
    pass


class MessageNotFoundError(APIError):
    pass


class WrongLastSequenceError(APIError):
    """An ``Nats-Expected-Last-*`` publish expectation was violated."""


class JetStreamNotEnabledError(APIError):
    pass


def _register() -> dict[int, type[APIError]]:
    return {
        10059: StreamNotFoundError,
        10058: StreamNameInUseError,
        10014: ConsumerNotFoundError,
        10037: MessageNotFoundError,
        10071: WrongLastSequenceError,
        10076: JetStreamNotEnabledError,  # not enabled
        10039: JetStreamNotEnabledError,  # not enabled for account
    }


_BY_ERR_CODE: dict[int, type[APIError]] = _register()


def error_for(err_code: int) -> type[APIError]:
    """The registered class for an ``err_code`` (mostly for tests)."""
    return _BY_ERR_CODE.get(err_code, APIError)


def register_error(err_code: int, exc_type: type[APIError]) -> None:
    """Extension hook: bind an err_code to a dedicated exception type."""
    _BY_ERR_CODE[err_code] = exc_type


__all__ += ["error_for", "register_error"]
