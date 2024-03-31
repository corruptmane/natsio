import asyncio
from collections import deque
from typing import Deque, Optional, cast

from natsio.utils.logger import connection_logger as log


class StreamProtocol(asyncio.Protocol):
    read_queue: Deque[bytes]
    read_event: asyncio.Event
    write_event: asyncio.Event
    exception: Optional[Exception] = None
    disconnect_event: asyncio.Event

    def __init__(self, disconnect_event: asyncio.Event) -> None:
        self.disconnect_event = disconnect_event

    def patch_transport(self, transport: asyncio.Transport) -> None:
        transport.set_write_buffer_limits(0)

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.read_queue = deque()
        self.read_event = asyncio.Event()
        self.write_event = asyncio.Event()
        self.write_event.set()
        self.disconnect_event.clear()
        log.debug("Connection established")
        self.patch_transport(cast(asyncio.Transport, transport))

    def connection_lost(self, exc: Optional[Exception]) -> None:
        log.debug("Connection lost")
        if exc:
            log.exception(exc)
            self.exception = exc

        self.read_event.set()
        self.write_event.set()
        self.disconnect_event.set()

    def data_received(self, data: bytes) -> None:
        log.debug("Received %d bytes: %a", len(data), data)
        self.read_queue.append(data)
        self.read_event.set()

    def eof_received(self) -> None:
        log.debug("EOF received")
        self.read_event.set()

    def pause_writing(self) -> None:
        self.write_event = asyncio.Event()

    def resume_writing(self) -> None:
        self.write_event.set()
