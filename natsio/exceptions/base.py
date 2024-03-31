import asyncio
from typing import Optional


class NATSError(Exception):
    description: str = "error"

    def __init__(self, description: Optional[str] = None) -> None:
        if description is not None:
            self.description = description

    def __str__(self) -> str:
        return f"NATS: {self.description}"


class TimeoutError(NATSError, asyncio.TimeoutError):
    description = "Operation timed out"
