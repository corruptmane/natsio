from typing import TYPE_CHECKING, Mapping

from natsio.utils.json import json_dumps
from natsio.utils.time import to_nanoseconds

from .core import CoreMsg

if TYPE_CHECKING:
    from natsio.client.jetstream import JetStream


class JetStreamMsg:
    def __init__(
        self,
        jetstream: "JetStream",
        msg: CoreMsg,
    ) -> None:
        self._js = jetstream
        self._msg = msg

    @property
    def subject(self) -> str:
        return self._msg.subject

    @property
    def payload(self) -> bytes:
        return self._msg.payload

    @property
    def reply_to(self) -> str | None:
        return self._msg.reply_to

    @property
    def headers(self) -> Mapping[str, str] | None:
        return self._msg.headers

    async def reply(self, data: bytes, headers: Mapping[str, str] | None = None) -> None:
        await self._msg.reply(data, headers)

    async def ack(self) -> None:
        await self.reply(b"")

    async def nak(self, delay: float | int | None = None) -> None:
        payload = b"-NAK"
        if delay is not None:
            payload = payload + b" " + json_dumps({"delay": to_nanoseconds(delay)})

        await self.reply(payload)
