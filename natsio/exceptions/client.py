from typing import Optional

from .base import NATSError, TimeoutError


class ClientError(NATSError):
    description = "Client error"


class ConfigError(NATSError):
    description = "Configuration error"


class WebSocketError(ConfigError, NotImplementedError):
    description = "WebSocket is not supported yet"


class TLSNotConfigured(ConfigError):
    description = "TLS is not configured"


class NoServersProvided(ConfigError):
    description = "No servers provided"


class NoServersAvailable(ClientError):
    description = "No servers available for connection"


class MaxPayloadError(ClientError):
    description = "Maximum payload size exceeded"


class ClientClosedError(ClientError):
    description = "Client is closed"


class ParserError(ClientError):
    description = "Parser error"


class InvalidHeaderVersion(ParserError):
    description = "Invalid headers version"

    def __init__(self, version: bytes, description: Optional[str] = None) -> None:
        super().__init__(description)
        self.version = version

    def __str__(self) -> str:
        return f"NATS: {self.description} - {self.version.decode()}"


class TLSError(ClientError):
    description = "TLS error"


class DrainTimeoutError(TimeoutError):
    description = "Drain timed out"


class FlushTimeoutError(TimeoutError):
    description = "Flush timed out"
