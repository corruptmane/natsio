import asyncio
import logging
from typing import Optional
from uuid import uuid4

from natsio.connection.conn import NATSConnection
from natsio.protocol.operations.base import BaseProtocolServerMessage
from natsio.protocol.operations.sub import Sub
from natsio.protocol.operations.unsub import Unsub

log = logging.getLogger(__name__)


class NATSCore:
    def __init__(self) -> None:
        self._conn: Optional[NATSConnection] = None
        self._listener_task: Optional[asyncio.Task[None]] = None
        self._updates_queue: Optional[asyncio.Queue[BaseProtocolServerMessage]] = None

    async def _listen(self) -> None:
        print("listening")
        if self._updates_queue is None:
            raise ValueError("Updates queue is not set")
        while True:
            update = await self._updates_queue.get()
            print(update)
            self._updates_queue.task_done()
            await asyncio.sleep(1)

    async def connect(self, host: str, port: int, timeout: float = 5) -> None:
        self._conn = NATSConnection(host=host, port=port)
        self._updates_queue = await self._conn.connect()
        self._listener_task = asyncio.create_task(self._listen())
        print("connected")

    async def subscribe(self, subject: str, queue: Optional[str] = None) -> str:
        if self._conn is None:
            raise ValueError("Connection is not set")
        sub_id = str(uuid4())
        await self._conn.send_data(Sub(subject=subject, sid=sub_id, queue=queue))
        return sub_id

    async def unsubscribe(self, sid: str) -> None:
        if self._conn is None:
            raise ValueError("Connection is not set")
        await self._conn.send_data(Unsub(sid=sid))

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
        if self._listener_task is not None:
            self._listener_task.cancel()
