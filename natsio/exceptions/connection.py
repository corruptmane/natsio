import asyncio
from .base import NATSError


class TimeoutError(NATSError, asyncio.TimeoutError):
    pass
