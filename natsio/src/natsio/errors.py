"""Public exception hierarchy.

Every exception raised by natsio derives from :class:`NATSError`. Where it
helps ``except`` ergonomics, subtrees additionally mix in the matching
builtin (e.g. :class:`TimeoutError` is also a :class:`builtins.TimeoutError`).
"""

import builtins

__all__ = [
    "AuthenticationExpiredError",
    "AuthorizationViolationError",
    "BadHeadersError",
    "ConfigError",
    "ConnectionClosedError",
    "MaxControlLineExceededError",
    "MaxPayloadExceededError",
    "MaxSubscriptionsExceededError",
    "NATSError",
    "NoServersAvailableError",
    "ParserError",
    "PermissionsViolationError",
    "ProtocolError",
    "ServerError",
    "StaleConnectionError",
    "TimeoutError",
]


class NATSError(Exception):
    """Root of the natsio exception hierarchy."""

    def __init__(self, description: str = "") -> None:
        super().__init__(description)
        self.description = description

    def __str__(self) -> str:
        return self.description or self.__class__.__name__


class TimeoutError(NATSError, builtins.TimeoutError):
    """An operation exceeded its deadline."""


class ConfigError(NATSError, ValueError):
    """Invalid client configuration."""


class ConnectionClosedError(NATSError, ConnectionError):
    """The operation cannot proceed because the connection is closed."""


class NoServersAvailableError(ConnectionClosedError):
    """Every server in the pool is unreachable or exhausted its retry budget."""


class ProtocolError(NATSError):
    """The peer violated the NATS wire protocol."""


class ParserError(ProtocolError):
    """Fatal framing/parsing failure.

    A byte stream cannot be resynchronized mid-frame: after this is raised the
    parser refuses further use and the connection must be torn down.
    """


class MaxControlLineExceededError(ParserError):
    """A control line exceeded the configured maximum length."""


class MaxPayloadExceededError(ProtocolError):
    """A message payload exceeded the maximum payload size."""


class BadHeadersError(NATSError):
    """A header block or user-supplied header is malformed or unsafe to send."""


class ServerError(NATSError):
    """An error reported by the server via ``-ERR``."""

    def __init__(self, description: str = "", *, fatal: bool = True) -> None:
        super().__init__(description)
        self.fatal = fatal


class StaleConnectionError(ServerError):
    """The server (or ping monitoring) declared the connection stale."""


class AuthorizationViolationError(ServerError):
    """Credentials were rejected by the server."""


class AuthenticationExpiredError(ServerError):
    """Credentials expired while the connection was active."""


class PermissionsViolationError(ServerError):
    """Publish/subscribe denied for a subject. Non-fatal: the connection stays open."""

    def __init__(self, description: str = "") -> None:
        super().__init__(description, fatal=False)


class MaxSubscriptionsExceededError(ServerError):
    """The account's subscription limit was hit. Non-fatal: the connection stays open."""

    def __init__(self, description: str = "") -> None:
        super().__init__(description, fatal=False)
