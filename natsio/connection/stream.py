import asyncio

from natsio.abc.connection import StreamProto
from natsio.exceptions.connection import ConnectionClosedError
from natsio.exceptions.stream import EndOfStream
from natsio.utils.logger import connection_logger as log

from .protocol import StreamProtocol


class Stream(StreamProto):
    def __init__(self, transport: asyncio.Transport, protocol: StreamProtocol) -> None:
        # _old_transport is used to keep a reference to the original transport (before TLS upgrade)
        self._old_transport = self._transport = transport
        self._protocol = protocol
        self._read_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._is_closed = False

    @property
    def is_closed(self) -> bool:
        return self._is_closed

    def upgraded_to_tls(self, transport: asyncio.Transport) -> None:
        self._transport = transport
        self._protocol.patch_transport(transport)

    async def _read_buffer(self) -> None:
        if not self._protocol.read_event.is_set() and not self._transport.is_closing():
            self._transport.resume_reading()
            await self._protocol.read_event.wait()
            self._transport.pause_reading()

    async def _read(self) -> bytes:
        # WARNING: use this only within reading lock
        await asyncio.sleep(0)
        try:
            return self._protocol.read_queue.popleft()
        except IndexError:
            if self.is_closed:
                raise ConnectionClosedError() from None
            if self._protocol.exception:
                raise self._protocol.exception from None
            raise EndOfStream() from None

    async def read(self, max_bytes: int) -> bytes:
        async with self._read_lock:
            await self._read_buffer()

            data = await self._read()

            if len(data) > max_bytes:
                data, leftover = data[:max_bytes], data[max_bytes:]
                self._protocol.read_queue.appendleft(leftover)

            if not self._protocol.read_queue:
                self._protocol.read_event.clear()

        return data

    async def read_exactly(self, size: int) -> bytes:
        async with self._read_lock:
            await self._read_buffer()
            data = b""

            while len(data) < size:
                data += await self._read()

            if len(data) > size:
                data, leftover = data[:size], data[size:]
                self._protocol.read_queue.appendleft(leftover)

            if not self._protocol.read_queue:
                self._protocol.read_event.clear()

        return data

    async def read_until(self, separator: bytes) -> bytes:
        async with self._read_lock:
            await self._read_buffer()
            data = b""

            while separator not in data:
                data += await self._read()

            data, leftover = data.split(separator, 1)
            if leftover:
                self._protocol.read_queue.appendleft(leftover)

            if not self._protocol.read_queue:
                self._protocol.read_event.clear()

        return data

    async def write(self, data: bytes) -> None:
        async with self._write_lock:
            log.debug("Writing %d bytes: %a", len(data), data)
            if self.is_closed:
                raise ConnectionClosedError()
            if self._protocol.exception:
                raise self._protocol.exception

            try:
                self._transport.write(data)
            except RuntimeError as exc:
                if self._transport.is_closing():
                    raise ConnectionClosedError() from None
                else:
                    raise exc

            await self._protocol.write_event.wait()

    async def close(self) -> None:
        if self.is_closed:
            return
        if self._transport.is_closing():
            return

        self._is_closed = True

        self._transport.close()
        await asyncio.sleep(0)
        self._transport.abort()
