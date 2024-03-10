import asyncio
from typing import Any, Optional
from uuid import uuid4

from natsio.const import CRLF
from natsio.protocol.operations.info import INFO_OP
from natsio.protocol.operations.ping_pong import PING_OP, Pong
from natsio.protocol.operations.sub import Sub

from .base import BaseNATSProtocol


class NATSTCPProtocol(BaseNATSProtocol):
    def __init__(self, on_con_made: asyncio.Future) -> None:
        super().__init__()
        self.transport: Optional[asyncio.Transport] = None
        self.on_con_made = on_con_made
        self.updates_queue = asyncio.Queue()

    def connection_made(self, transport: asyncio.BaseTransport):
        if not isinstance(transport, asyncio.Transport):
            raise TypeError("Transport is not an instance of asyncio.Transport")
        self.transport = transport
        self.on_con_made.set_result(True)

    def connection_lost(self, exc: Optional[Exception]):
        # TODO: handle connection lost
        print("Connection lost")

    def data_received(self, data: bytes):
        # TODO: parse incoming data
        self.updates_queue.put_nowait(data)
        print("New data received and put into queue")

    def send_data(self, data: bytes) -> None:
        assert self.transport is not None, "Transport is not set"
        print("Sending data:", data.strip(CRLF).decode("utf-8"))
        self.transport.write(data)

    def eof_received(self):
        # TODO: handle EOF from peer
        print("EOF received, connection closed by peer")
        assert self.transport is not None, "Transport is not set"
        self.transport.close()
