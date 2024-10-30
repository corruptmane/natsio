from typing import TYPE_CHECKING, Mapping

from natsio.exceptions.client import ClientError

if TYPE_CHECKING:
    from natsio.client.core import NATSCore


class CoreMsg:
    def __init__(
        self,
        client: "NATSCore",
        subject: str,
        payload: bytes,
        reply_to: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self._client = client
        self.subject = subject
        self.payload = payload
        self.reply_to = reply_to
        self.headers = headers

    async def reply(self, data: bytes, headers: Mapping[str, str] | None = None) -> None:
        if self.reply_to is None:
            raise ClientError("reply_to is not set")
        await self._client.publish(self.reply_to, data, headers=headers)
