import asyncio
import logging

from natsio.const import CRLF
from natsio.exceptions.protocol import UnknownProtocol
from natsio.exceptions.stream import EndOfStream
from natsio.protocol.operations.connect import Connect
from natsio.protocol.operations.err import ERR_OP
from natsio.protocol.operations.hmsg import HMSG_OP
from natsio.protocol.operations.info import INFO_OP
from natsio.protocol.operations.msg import MSG_OP
from natsio.protocol.operations.ok import OK_OP
from natsio.protocol.operations.ping_pong import PING_OP, PONG, PONG_OP
from natsio.protocol.parser import ProtocolParser

from .stream import Stream

log = logging.getLogger(__name__)


class NATSListener:
    def __init__(
        self,
        stream: Stream,
        parser: ProtocolParser,
        on_con_made: asyncio.Future[None],
    ) -> None:
        self._stream = stream
        self._parser = parser
        self._on_con_made = on_con_made
        self.outstanding_pings = 0

    async def _process_operation(self, operation: bytes, payload: bytes) -> None:
        operation = operation.upper()
        try:
            if operation == MSG_OP:
                return await self.process_msg(payload)
            if operation == HMSG_OP:
                return await self.process_hmsg(payload)
            if operation == INFO_OP:
                return await self.process_info(payload)
            if operation == PING_OP:
                return await self.process_ping()
            if operation == PONG_OP:
                return await self.process_pong()
            if operation == OK_OP:
                return
            if operation == ERR_OP:
                return await self.process_error(payload)
        except Exception as exc:
            log.exception(exc)
        raise UnknownProtocol()

    async def _listen(self) -> None:
        while True:
            try:
                data = await self._stream.read_until(CRLF)
            except EndOfStream:
                continue
            except Exception as exc:
                # TODO: add error handling
                log.exception(exc)
                continue

            data = data.strip(b" ")

            try:
                operation, payload = data.split(b" ", maxsplit=1)
            except ValueError:
                operation = data
                payload = b""

            try:
                await self._process_operation(operation, payload)
            except UnknownProtocol:
                log.error("Unknown protocol")
                continue

        print("Connection closed")

    async def listen(self) -> None:
        try:
            await self._listen()
        except asyncio.CancelledError:
            pass

    async def process_info(self, payload: bytes) -> None:
        await self._stream.write(
            Connect(
                verbose=False,
                pedantic=True,
                tls_required=False,
                lang="python/natsio",
                version="0.1.0",
                headers=True,
            ).build(),
        )
        self._on_con_made.set_result(None)

    async def process_ping(self) -> None:
        await self._stream.write(PONG)

    async def process_pong(self) -> None:
        self.outstanding_pings -= 1

    async def process_msg(self, payload: bytes) -> None:
        parsed = await self._parser.parse_msg(payload, self._stream)
        print(parsed)

    async def process_hmsg(self, payload: bytes) -> None:
        parsed = await self._parser.parse_hmsg(payload, self._stream)
        print(parsed)

    async def process_error(self, payload: bytes) -> None:
        print(payload.decode())
