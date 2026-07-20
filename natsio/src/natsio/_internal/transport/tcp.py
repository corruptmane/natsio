"""TCP transport over a custom asyncio.Protocol.

A protocol (not streams) keeps exactly one buffer copy between the socket and
the parser and exposes ``pause_reading``/``pause_writing`` directly — both
load-bearing for backpressure.
"""

import asyncio
import ssl

from natsio.errors import ConnectionClosedError

from .base import OnBytes, OnClose

__all__ = ["TCPTransport"]


class _Proto(asyncio.Protocol):
    __slots__ = ("_owner",)

    def __init__(self, owner: "TCPTransport") -> None:
        self._owner = owner

    def data_received(self, data: bytes) -> None:
        self._owner._on_bytes(data)

    def eof_received(self) -> bool:
        return False  # treat EOF as connection loss; connection_lost follows

    def connection_lost(self, exc: Exception | None) -> None:
        self._owner._handle_connection_lost(exc)

    def pause_writing(self) -> None:
        self._owner._writable.clear()

    def resume_writing(self) -> None:
        self._owner._writable.set()


class TCPTransport:
    def __init__(self, *, on_bytes: OnBytes, on_close: OnClose) -> None:
        self._on_bytes = on_bytes
        self._on_close = on_close
        self._transport: asyncio.Transport | None = None
        self._protocol: _Proto | None = None
        self._writable = asyncio.Event()
        self._writable.set()
        self._close_reported = False

    # -- Transport protocol --------------------------------------------------

    async def connect(
        self,
        host: str,
        port: int,
        *,
        tls: ssl.SSLContext | None = None,
        tls_hostname: str | None = None,
    ) -> None:
        loop = asyncio.get_running_loop()
        transport, protocol = await loop.create_connection(
            lambda: _Proto(self),
            host,
            port,
            ssl=tls,
            server_hostname=tls_hostname if tls is not None else None,
        )
        self._transport = transport
        self._protocol = protocol

    async def upgrade_tls(self, context: ssl.SSLContext, hostname: str | None) -> None:
        transport, protocol = self._require_transport()
        loop = asyncio.get_running_loop()
        upgraded = await loop.start_tls(transport, protocol, context, server_hostname=hostname)
        if upgraded is None:  # pragma: no cover - start_tls only returns None on failure paths
            raise ConnectionClosedError("TLS upgrade failed")
        self._transport = upgraded

    def write(self, data: bytes) -> None:
        transport, _ = self._require_transport()
        transport.write(data)

    async def wait_writable(self) -> None:
        await self._writable.wait()

    def pause_reading(self) -> None:
        if self._transport is not None and not self._transport.is_closing():
            self._transport.pause_reading()

    def resume_reading(self) -> None:
        if self._transport is not None and not self._transport.is_closing():
            self._transport.resume_reading()

    @property
    def is_closing(self) -> bool:
        return self._transport is None or self._transport.is_closing()

    def close(self) -> None:
        if self._transport is not None and not self._transport.is_closing():
            self._transport.close()

    def abort(self) -> None:
        if self._transport is not None and not self._transport.is_closing():
            self._transport.abort()

    # -- internals -----------------------------------------------------------

    def _require_transport(self) -> tuple[asyncio.Transport, _Proto]:
        if self._transport is None or self._protocol is None or self._transport.is_closing():
            raise ConnectionClosedError("transport is not connected")
        return self._transport, self._protocol

    def _handle_connection_lost(self, exc: Exception | None) -> None:
        # Unblock any writer stuck on flow control, then report exactly once.
        self._writable.set()
        if not self._close_reported:
            self._close_reported = True
            self._on_close(exc)
