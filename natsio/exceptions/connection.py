import asyncio
from typing import Optional, Union

from .base import NATSError


class NATSConnectionError(NATSError, ConnectionError):
    description = "Connection error"


class TimeoutError(NATSConnectionError, asyncio.TimeoutError):
    description = "Operation timed out"


class ConnectionTimeoutError(TimeoutError):
    description = "Connection timed out"


class FlushTimeoutError(TimeoutError):
    description = "Flush timed out"


class DrainTimeoutError(TimeoutError):
    description = "Drain timed out"


class BadURIError(NATSConnectionError):
    description = "Bad URI"

    def __init__(
        self, value: Union[str, int, None] = None, description: Optional[str] = None
    ) -> None:
        super().__init__(description)
        self.value = value

    def __str__(self) -> str:
        return f"NATS: {self.description} - {self.value}"


class BadHostnameError(BadURIError):
    description = "Bad hostname"


class BadPortError(BadURIError):
    description = "Bad port"


class NoConnectionError(NATSConnectionError):
    description = "Connection is not established"


class ConnectionClosedError(NATSConnectionError):
    description = "Connection is closed"


class TLSError(NATSConnectionError):
    description = "TLS error"


class OutboundBufferLimitError(NATSConnectionError):
    description = "Outbound buffer limit reached"
