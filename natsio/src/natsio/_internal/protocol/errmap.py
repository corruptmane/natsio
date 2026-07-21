"""Classification of server ``-ERR`` messages.

The server keeps the socket open after some errors (permissions, subscription
limits, pedantic-mode subject complaints) and closes it after the rest. The
connection layer keys its reaction off `ServerError.fatal`, so getting
this table right is what separates "log and carry on" from "tear down and
reconnect".
"""

from natsio.errors import (
    AuthenticationExpiredError,
    AuthorizationViolationError,
    MaxSubscriptionsExceededError,
    PermissionsViolationError,
    ServerError,
    StaleConnectionError,
)

__all__ = ["classify_server_error"]

_AUTH_EXPIRED_MARKERS = (
    "authentication expired",
    "user authentication expired",
    "user authentication revoked",
    "account authentication expired",
)

_NON_FATAL_MARKERS = (
    "invalid subject",  # pedantic-mode complaint about one operation
)


def classify_server_error(message: str) -> ServerError:
    """Map a ``-ERR`` message (quotes already stripped) to a typed exception."""
    lowered = message.lower()

    if "permissions violation" in lowered:
        return PermissionsViolationError(message)
    if "maximum subscriptions exceeded" in lowered:
        return MaxSubscriptionsExceededError(message)
    if lowered.startswith("stale connection"):
        return StaleConnectionError(message)
    for marker in _AUTH_EXPIRED_MARKERS:
        if lowered.startswith(marker):
            return AuthenticationExpiredError(message)
    if lowered.startswith("authorization violation"):
        return AuthorizationViolationError(message)
    for marker in _NON_FATAL_MARKERS:
        if lowered.startswith(marker):
            return ServerError(message, fatal=False)
    return ServerError(message, fatal=True)
