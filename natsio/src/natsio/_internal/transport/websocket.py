"""WebSocket transport: the same :class:`~.base.Transport` seam as TCP, layered
over the sans-io RFC 6455 core.

Connect order: TCP connect, then (for ``wss``) TLS wraps the socket BEFORE the
HTTP Upgrade — WebSocket TLS is transport-level, never an in-band STARTTLS, so
:meth:`upgrade_tls` is unsupported here. Once the ``101`` handshake completes,
NATS bytes stream both ways: every :meth:`write` becomes exactly one masked
binary frame; inbound frames are decoded and their payloads fed to the NATS
parser as an opaque byte stream. Server pings are answered transparently with a
pong; a server close frame is surfaced as connection loss (EOF-equivalent).

Like :class:`~.tcp.TCPTransport` this rides a raw ``asyncio.Protocol`` (one
buffer copy, direct ``pause_writing``/``pause_reading`` for backpressure).
"""

import asyncio
import contextlib
import ssl
from enum import Enum

from natsio.errors import ConnectionClosedError, WebsocketError

from ..protocol.websocket import (
    WS_NEED_DATA,
    WSClose,
    WSData,
    WSFrameDecoder,
    WSHandshake,
    WSHandshakeAccepted,
    WSPing,
    WSPong,
    encode_binary_frame,
    encode_close,
    encode_pong,
)
from .base import OnBytes, OnClose

__all__ = ["WSTransport"]


class _Phase(Enum):
    HANDSHAKE = "HANDSHAKE"
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class _WSProto(asyncio.Protocol):
    __slots__ = ("_owner",)

    def __init__(self, owner: "WSTransport") -> None:
        self._owner = owner

    def data_received(self, data: bytes) -> None:
        self._owner._on_data(data)

    def eof_received(self) -> bool:
        return False  # treat EOF as connection loss; connection_lost follows

    def connection_lost(self, exc: Exception | None) -> None:
        self._owner._handle_connection_lost(exc)

    def pause_writing(self) -> None:
        self._owner._writable.clear()

    def resume_writing(self) -> None:
        self._owner._writable.set()


class WSTransport:
    def __init__(self, *, on_bytes: OnBytes, on_close: OnClose, path: str = "/") -> None:
        self._on_bytes = on_bytes
        self._on_close = on_close
        self._path = path or "/"
        self._transport: asyncio.Transport | None = None
        self._protocol: _WSProto | None = None
        self._writable = asyncio.Event()
        self._writable.set()
        self._close_reported = False
        self._phase = _Phase.HANDSHAKE
        self._handshake: WSHandshake | None = None
        self._handshake_done: asyncio.Future[None] | None = None
        self._decoder = WSFrameDecoder()
        # A framing/handshake violation, remembered so it is the reason reported
        # to on_close when we abort the socket ourselves.
        self._error: Exception | None = None

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
        self._handshake_done = loop.create_future()
        self._handshake = WSHandshake(f"{host}:{port}", self._path)
        transport, protocol = await loop.create_connection(
            lambda: _WSProto(self),
            host,
            port,
            ssl=tls,  # wss: TLS is established here, before the HTTP Upgrade below
            server_hostname=tls_hostname if tls is not None else None,
        )
        self._transport = transport
        self._protocol = protocol
        transport.write(self._handshake.request)
        await self._handshake_done

    async def upgrade_tls(self, context: ssl.SSLContext, hostname: str | None) -> None:
        raise ConnectionClosedError(
            "WebSocket TLS is negotiated at connect time (wss://); in-band upgrade is not supported"
        )

    def write(self, data: bytes) -> None:
        transport, _ = self._require_transport()
        transport.write(encode_binary_frame(data))

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
            if self._phase is _Phase.OPEN:
                with contextlib.suppress(Exception):  # best-effort courtesy close frame
                    self._transport.write(encode_close())
            self._transport.close()

    def abort(self) -> None:
        if self._transport is not None and not self._transport.is_closing():
            self._transport.abort()

    # -- internals -----------------------------------------------------------

    def _require_transport(self) -> tuple[asyncio.Transport, _WSProto]:
        if (
            self._transport is None
            or self._protocol is None
            or self._transport.is_closing()
            or self._phase is _Phase.CLOSED
        ):
            raise ConnectionClosedError("transport is not connected")
        return self._transport, self._protocol

    def _on_data(self, data: bytes) -> None:
        if self._phase is _Phase.HANDSHAKE:
            self._on_handshake_data(data)
        elif self._phase is _Phase.OPEN:
            self._on_frame_data(data)
        # CLOSED: draining the doomed socket; ignore.

    def _on_handshake_data(self, data: bytes) -> None:
        assert self._handshake is not None
        self._handshake.receive_data(data)
        try:
            event = self._handshake.next_event()
        except WebsocketError as exc:
            self._fail(exc)
            return
        if event is WS_NEED_DATA:
            return
        assert isinstance(event, WSHandshakeAccepted)
        self._phase = _Phase.OPEN
        self._handshake = None
        if self._handshake_done is not None and not self._handshake_done.done():
            self._handshake_done.set_result(None)
        if event.leftover:
            self._on_frame_data(event.leftover)

    def _on_frame_data(self, data: bytes) -> None:
        self._decoder.receive_data(data)
        try:
            while True:
                event = self._decoder.next_event()
                if event is WS_NEED_DATA:
                    return
                if isinstance(event, WSData):
                    if event.payload:
                        self._on_bytes(event.payload)
                elif isinstance(event, WSPing):
                    self._reply_pong(event.payload)
                elif isinstance(event, WSPong):
                    pass  # liveness is driven by NATS PING/PONG, not WS ping
                elif isinstance(event, WSClose):
                    self._on_server_close()
                    return
        except WebsocketError as exc:
            self._fail(exc)

    def _reply_pong(self, payload: bytes) -> None:
        transport = self._transport
        if transport is not None and not transport.is_closing():
            with contextlib.suppress(Exception):  # pragma: no cover - write side already gone
                transport.write(encode_pong(payload))

    def _on_server_close(self) -> None:
        # Clean server-initiated close: answer with a close frame, then drop the
        # socket. connection_lost -> on_close(None) makes it an EOF-equivalent,
        # so the connection layer reconnects exactly as for a TCP EOF.
        self._phase = _Phase.CLOSED
        transport = self._transport
        if transport is not None and not transport.is_closing():
            with contextlib.suppress(Exception):  # pragma: no cover
                transport.write(encode_close())
            transport.close()

    def _fail(self, error: Exception) -> None:
        self._error = error
        transport = self._transport
        if transport is not None and not transport.is_closing():
            transport.abort()
        elif transport is None:  # pragma: no cover - no socket yet
            self._handle_connection_lost(error)

    def _handle_connection_lost(self, exc: Exception | None) -> None:
        self._writable.set()
        self._phase = _Phase.CLOSED
        reported = exc if exc is not None else self._error
        if self._handshake_done is not None and not self._handshake_done.done():
            self._handshake_done.set_exception(
                reported or ConnectionClosedError("connection closed during WebSocket handshake")
            )
        if not self._close_reported:
            self._close_reported = True
            self._on_close(reported)
