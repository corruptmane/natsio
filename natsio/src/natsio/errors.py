"""Public exception hierarchy.

Every exception raised by natsio derives from `NATSError`. Where it
helps ``except`` ergonomics, subtrees additionally mix in the matching
builtin (e.g. `TimeoutError` is also a `builtins.TimeoutError`).
"""

import builtins

__all__ = [
    "AuthenticationExpiredError",
    "AuthorizationViolationError",
    "BadHeadersError",
    "ConfigError",
    "ConnectionClosedError",
    "DrainTimeoutError",
    "MaxControlLineExceededError",
    "MaxPayloadExceededError",
    "MaxSubscriptionsExceededError",
    "MissingDependencyError",
    "NATSError",
    "NoReplySubjectError",
    "NoRespondersError",
    "NoServersAvailableError",
    "ParserError",
    "PermissionsViolationError",
    "ProtocolError",
    "ReconnectBufExceededError",
    "ServerError",
    "SlowConsumerError",
    "StaleConnectionError",
    "SubscriptionClosedError",
    "TimeoutError",
    "WebsocketError",
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


class DrainTimeoutError(TimeoutError):
    """A drain did not finish within ``drain_timeout``."""


class ConfigError(NATSError, ValueError):
    """Invalid client configuration."""


class ConnectionClosedError(NATSError, ConnectionError):
    """The operation cannot proceed because the connection is closed."""


class NoServersAvailableError(ConnectionClosedError):
    """Every server in the pool is unreachable or exhausted its retry budget."""


class ReconnectBufExceededError(NATSError):
    """A publish issued while disconnected exceeded ``reconnect_buf_size``.

    Non-fatal: the connection stays alive and reconnect continues; only this
    publish is rejected. Raised immediately for every disconnected publish when
    buffering is disabled (``reconnect_buf_size=-1``).
    """


class ProtocolError(NATSError):
    """The peer violated the NATS wire protocol."""


class ParserError(ProtocolError):
    """Fatal framing/parsing failure.

    A byte stream cannot be resynchronized mid-frame: after this is raised the
    parser refuses further use and the connection must be torn down.
    """


class MaxControlLineExceededError(ParserError):
    """A control line exceeded the configured maximum length."""


class WebsocketError(ProtocolError):
    """A WebSocket (RFC 6455) handshake or framing violation.

    Fatal like a framing error: a WebSocket stream cannot be resynchronized
    mid-frame, so the transport must be torn down.
    """


class MaxPayloadExceededError(ProtocolError):
    """A message payload exceeded the maximum payload size."""


class NoReplySubjectError(NATSError):
    """Attempted to respond to a message that carries no reply subject."""


class NoRespondersError(NATSError):
    """No subscriber is listening on the requested subject (server status 503)."""


class SubscriptionClosedError(NATSError):
    """The subscription has been unsubscribed or drained."""


class SlowConsumerError(NATSError):
    """A subscription exceeded its pending limits; messages were dropped."""

    def __init__(self, description: str = "", *, subject: str = "", sid: int = 0, dropped: int = 0) -> None:
        super().__init__(description)
        self.subject = subject
        self.sid = sid
        self.dropped = dropped


class MissingDependencyError(NATSError, ImportError):
    """An optional dependency is required for the requested feature."""


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
