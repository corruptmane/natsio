import asyncio
from natsio.abc.connection import StreamProto


class FakeStream(StreamProto):
    def __init__(self, data: bytes):
        self.buffer = data
        self.position = 0
        self._is_closed = False

    @property
    def is_closed(self) -> bool:
        return self._is_closed

    def upgraded_to_tls(self, transport: asyncio.Transport) -> None:
        pass  # Not needed for testing

    async def read(self, max_bytes: int) -> bytes:
        if self._is_closed:
            raise Exception("Stream is closed")
        if self.position >= len(self.buffer):
            return b''  # EOF
        data = self.buffer[self.position:self.position + max_bytes]
        self.position += len(data)
        return data

    async def read_exactly(self, size: int) -> bytes:
        if self._is_closed:
            raise Exception("Stream is closed")
        if self.position + size > len(self.buffer):
            raise Exception("Not enough data to read")
        data = self.buffer[self.position:self.position + size]
        self.position += size
        return data

    async def read_until(self, separator: bytes) -> bytes:
        if self._is_closed:
            raise Exception("Stream is closed")
        index = self.buffer.find(separator, self.position)
        if index == -1:
            raise Exception("Separator not found")
        data = self.buffer[self.position:index + len(separator)]
        self.position = index + len(separator)
        return data

    async def write(self, data: bytes) -> None:
        pass  # Not needed for testing

    async def close(self) -> None:
        self._is_closed = True
