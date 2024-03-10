import asyncio
from typing import Optional

from natsio.protocol.connection.tcp import NATSTCPProtocol
from natsio.protocol.operations.base import BaseProtocolClientMessage


class NATSConnection:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.transport: Optional[asyncio.Transport] = None
        self.protocol: Optional[NATSTCPProtocol] = None

    async def connect(self, timeout: float = 5) -> asyncio.Queue:
        loop = asyncio.get_running_loop()
        connection_done: asyncio.Future = asyncio.Future()
        protocol = NATSTCPProtocol(on_con_made=connection_done)
        self.transport, self.protocol = await asyncio.wait_for(
            loop.create_connection(
                lambda: protocol,
                host=self.host,
                port=self.port,
            ),
            timeout=timeout,
        )
        await connection_done
        return protocol.updates_queue

    async def send_data(self, data: BaseProtocolClientMessage) -> None:
        if self.protocol is None:
            raise ValueError("Protocol is not set")
        self.protocol.send_data(data.build())

    async def close(self) -> None:
        if self.transport is not None:
            self.transport.write_eof()
            self.transport.close()
