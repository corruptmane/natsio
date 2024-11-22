from .base import NATSError


class EndOfStream(NATSError, ConnectionRefusedError):
    description = "End of stream"
