"""Byte-stream transport seam.

A transport moves raw NATS protocol bytes and nothing else. The connection
layer wires two callbacks at construction time:

- ``on_bytes(data)`` — called for every received chunk (must not block);
- ``on_close(exc)``  — called exactly once when the transport is gone.

Implementations are structural (:class:`typing.Protocol` — no inheritance):
TCP today; a WebSocket transport can be added later without touching the
parser or connection.
"""

import ssl
from collections.abc import Callable
from typing import Protocol

__all__ = ["OnBytes", "OnClose", "Transport"]

type OnBytes = Callable[[bytes], None]
type OnClose = Callable[[Exception | None], None]


class Transport(Protocol):
    async def connect(
        self,
        host: str,
        port: int,
        *,
        tls: ssl.SSLContext | None = None,
        tls_hostname: str | None = None,
    ) -> None:
        """Establish the connection. ``tls`` here means TLS-first (before any read).

        Deadline enforcement is the caller's job (``asyncio.timeout``).
        """
        ...

    async def upgrade_tls(self, context: ssl.SSLContext, hostname: str | None) -> None:
        """In-place STARTTLS-style upgrade after INFO."""
        ...

    def write(self, data: bytes) -> None:
        """Buffered, non-blocking write."""
        ...

    async def wait_writable(self) -> None:
        """Block while the write side is flow-control paused."""
        ...

    def pause_reading(self) -> None: ...

    def resume_reading(self) -> None: ...

    @property
    def is_closing(self) -> bool: ...

    def close(self) -> None:
        """Graceful close; ``on_close`` fires asynchronously."""
        ...

    def abort(self) -> None:
        """Hard teardown; ``on_close`` fires asynchronously."""
        ...
