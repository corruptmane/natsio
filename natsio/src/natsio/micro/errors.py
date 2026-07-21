"""Micro (ADR-32) error types."""

from natsio.errors import ConfigError, NATSError

__all__ = ["ServiceConfigError", "ServiceError"]


class ServiceConfigError(ConfigError):
    """A service, group or endpoint was configured with invalid values."""


class ServiceError(NATSError):
    """A service-side error surfaced to the configured ``error_handler``.

    Carries the ``code``/``description`` that were (or would be) returned to the
    caller, plus the ``subject`` the request arrived on and the ``endpoint``
    name it was routed to. The originating exception, when any, is chained as
    ``__cause__``.
    """

    def __init__(
        self,
        description: str = "",
        *,
        code: str = "500",
        subject: str = "",
        endpoint: str = "",
    ) -> None:
        super().__init__(description)
        self.description = description
        self.code = code
        self.subject = subject
        self.endpoint = endpoint

    def __str__(self) -> str:
        return f"{self.code}:{self.description}"
