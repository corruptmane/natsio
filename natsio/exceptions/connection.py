from typing import Optional

from .base import NATSError, TimeoutError


class NATSConnectionError(NATSError, ConnectionError):
    description = "Connection error"
    uri: Optional[str]

    def __init__(
        self, uri: Optional[str] = None, description: Optional[str] = None
    ) -> None:
        super().__init__(description)
        self.uri = uri

    def __str__(self) -> str:
        text = f"NATS: {self.description}"
        if self.uri is not None:
            text += f" - {self.uri}"
        return text


class ConnectionFailedError(NATSConnectionError):
    description = "Connection failed"
    cause: Optional[str]

    def __init__(
        self,
        cause: Optional[str] = None,
        uri: Optional[str] = None,
        description: Optional[str] = None,
    ) -> None:
        super().__init__(uri=uri, description=description)
        self.cause = cause

    def set_cause(self, exception: BaseException) -> None:
        self.cause = exception.__class__.__name__
        self.__cause__ = exception

    def __str__(self) -> str:
        text = f"NATS: {self.description}"
        if self.cause is not None:
            text += f": {self.cause}"
        return text


class ConnectionTimeoutError(NATSConnectionError, TimeoutError):
    description = "Connection timed out"


class NoConnectionError(NATSConnectionError):
    description = "Connection is not established"


class ConnectionClosedError(NATSConnectionError):
    description = "Connection is closed"


class OutboundBufferLimitError(NATSConnectionError):
    description = "Outbound buffer limit reached"
