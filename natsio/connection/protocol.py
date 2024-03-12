import asyncio
import logging
from collections import deque
from typing import Deque, Optional, cast

log = logging.getLogger(__name__)


class StreamProtocol(asyncio.Protocol):
    read_queue: Deque[bytes]
    read_event: asyncio.Event
    write_event: asyncio.Event
    exception: Optional[Exception] = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.read_queue = deque()
        self.read_event = asyncio.Event()
        self.write_event = asyncio.Event()
        self.write_event.set()
        print("Connection established")
        cast(asyncio.Transport, transport).set_write_buffer_limits(0)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        print("Connection lost")
        if exc:
            log.exception(exc)
            self.exception = exc

        self.read_event.set()
        self.write_event.set()

    def data_received(self, data: bytes) -> None:
        print(b"Received:", data)
        self.read_queue.append(data)
        self.read_event.set()

    def eof_received(self) -> None:
        print("EOF received")
        self.read_event.set()

    def pause_writing(self) -> None:
        self.write_event = asyncio.Event()

    def resume_writing(self) -> None:
        self.write_event.set()
