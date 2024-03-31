from typing import Mapping, Optional, Type

from .base import NATSError


class ProtocolError(NATSError):
    is_disconnected: bool = True

    def __init__(
        self,
        description: Optional[str] = None,
        is_disconnected: bool = True,
    ) -> None:
        super().__init__(description)
        self.is_disconnected = is_disconnected

    def __str__(self) -> str:
        return f"NATS: {self.description}"


class UnknownProtocol(ProtocolError):
    description = "Unknown Protocol Operation"
    extra: Optional[str] = None

    def __init__(
        self,
        extra: Optional[str] = None,
        is_disconnected: bool = True,
        description: Optional[str] = None,
    ) -> None:
        super().__init__(description=description, is_disconnected=is_disconnected)
        self.extra = extra
        self.is_disconnected = is_disconnected

    def __str__(self) -> str:
        if self.extra is not None:
            return f"NATS: Unknown error protocol message - {self.extra}"
        return super().__str__()


class RoutePortConnectAttempt(ProtocolError):
    description = "Attempted To Connect To Route Port"


class AuthorizationViolation(ProtocolError):
    description = "Authorization Violation"


class AuthorizationTimeout(ProtocolError):
    description = "Authorization Timeout"


class InvalidClientProtocol(ProtocolError):
    description = "Invalid Client Protocol"


class MaxControlLineExceeded(ProtocolError):
    description = "Maximum Control Line Exceeded"


class ParserError(ProtocolError):
    description = "Parser Error"


class TLSRequired(ProtocolError):
    description = "Secure Connection - TLS Required"


class StaleConnection(ProtocolError):
    description = "Stale Connection"


class MaxConnectionsExceeded(ProtocolError):
    description = "Maximum Connections Exceeded"


class SlowConsumer(ProtocolError):
    description = "Slow Consumer"
    subject: Optional[str] = None
    sid: Optional[str] = None

    def __init__(
        self,
        subject: Optional[str] = None,
        sid: Optional[str] = None,
        description: Optional[str] = None,
        is_disconnected: bool = True,
    ) -> None:
        super().__init__(description=description, is_disconnected=is_disconnected)
        self.subject = subject
        self.sid = sid

    def __str__(self) -> str:
        text = f"NATS: {self.description}"
        if self.subject is not None:
            text += f" - [subject:{self.subject}]"
        if self.sid is not None:
            text += f" - [sid:{self.sid}]"
        return text


class MaxPayloadExceeded(ProtocolError):
    description = "Maximum Payload Violation"


class InvalidSubject(ProtocolError):
    description = "Client sent a malformed subject"
    is_disconnected = False


class PermissionsViolation(ProtocolError):
    description = "Permissions Violation"
    is_disconnected = False

    def __init__(self, subject: str, description: Optional[str] = None) -> None:
        super().__init__(description=description, is_disconnected=False)
        self.subject = subject

    def __str__(self) -> str:
        return f"NATS: {self.description} - {self.subject}"


class SubscriptionPermissionsViolation(PermissionsViolation):
    description = "Permissions Violation for Subscription"


class PublishPermissionsViolation(PermissionsViolation):
    description = "Permissions Violation for Publish"


name_to_error: Mapping[str, Type[ProtocolError]] = {
    "Unknown Protocol Operation": UnknownProtocol,
    "Attempted To Connect To Route Port": RoutePortConnectAttempt,
    "Authorization Violation": AuthorizationViolation,
    "Authorization Timeout": AuthorizationTimeout,
    "Invalid Client Protocol": InvalidClientProtocol,
    "Maximum Control Line Exceeded": MaxControlLineExceeded,
    "Parser Error": ParserError,
    "Secure Connection - TLS Required": TLSRequired,
    "Stale Connection": StaleConnection,
    "Maximum Connections Exceeded": MaxConnectionsExceeded,
    "Slow Consumer": SlowConsumer,
    "Maximum Payload Violation": MaxPayloadExceeded,
    "Invalid Subject": InvalidSubject,
}
