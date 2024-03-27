from typing import Mapping, Optional, Type

from .base import NATSError


class ProtocolError(NATSError):
    name: str
    description: str
    is_disconnected: bool = True

    def __init__(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
        is_disconnected: bool = True,
    ) -> None:
        if name is not None:
            self.name = name
        if description is not None:
            self.description = description
        self.is_disconnected = is_disconnected

    def __str__(self) -> str:
        return f"NATS Protocol Error: {self.name}"


class UnknownProtocol(ProtocolError):
    name = "Unknown Protocol Operation"
    description = "Unknown protocol error"
    extra: Optional[str] = None

    def __init__(
        self, extra: Optional[str] = None, is_disconnected: bool = True
    ) -> None:
        self.extra = extra
        self.is_disconnected = is_disconnected


class RoutePortConnectAttempt(ProtocolError):
    name = "Attempted To Connect To Route Port"
    description = (
        "Client attempted to connect to a route port instead of the client port"
    )


class AuthorizationViolation(ProtocolError):
    name = "Authorization Violation"
    description = 'Client failed to authenticate to the server with credentials specified in the "CONNECT" message'


class AuthorizationTimeout(ProtocolError):
    name = "Authorization Timeout"
    description = "Client took too long to authenticate to the server after establishing a connection (default 1 second)"


class InvalidClientProtocol(ProtocolError):
    name = "Invalid Client Protocol"
    description = (
        'Client specified an invalid protocol version in the "CONNECT" message'
    )


class MaxControlLineExceeded(ProtocolError):
    name = "Maximum Control Line Exceeded"
    description = 'Message destination subject and reply subject length exceeded the maximum control line value specified by the "max_control_line" server option. The default is 1024 bytes'


class ParserError(ProtocolError):
    name = "Parser Error"
    description = "Cannot parse the protocol message sent by the client"


class TLSRequired(ProtocolError):
    name = "Secure Connection - TLS Required"
    description = "The server requires TLS and the client does not have TLS enabled"


class StaleConnection(ProtocolError):
    name = "Stale Connection"
    description = 'The server hasn\'t received a message from the client, including a "PONG" in too long'


class MaxConnectionsExceeded(ProtocolError):
    name = "Maximum Connections Exceeded"
    description = "This error is sent by the server when creating a new connection and the server has exceeded the maximum number of connections specified by the max_connections server option. The default is 64k"


class SlowConsumer(ProtocolError):
    name = "Slow Consumer"
    description = "The server pending data size for the connection has reached the maximum size (default 10MB)"


class MaxPayloadExceeded(ProtocolError):
    name = "Maximum Payload Violation"
    description = 'Client attempted to publish a message with a payload size that exceeds the "max_payload" size configured on the server. This value is supplied to the client upon connection in the initial "INFO" message. The client is expected to do proper accounting of byte size to be sent to the server in order to handle this error synchronously'


class InvalidSubject(ProtocolError):
    name = "Invalid Subject"
    description = "Client sent a malformed subject"
    is_disconnected = False


class PermissionsViolation(ProtocolError):
    name = "Permissions Violation"
    description = 'The user specified in the "CONNECT" message does not have permission to subscribe or publish to the subject'
    is_disconnected = False

    def __init__(self, name: str) -> None:
        self.name = name


class SubscriptionPermissionsViolation(PermissionsViolation):
    description = 'The user specified in the "CONNECT" message does not have permission to subscribe to the subject'


class PublishPermissionsViolation(PermissionsViolation):
    description = 'The user specified in the "CONNECT" message does not have permission to publish to the subject'


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
