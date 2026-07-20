"""The message type delivered to subscribers and returned by requests."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from natsio._internal.protocol import Headers, HeadersInput, InlineStatus
from natsio.errors import NoReplySubjectError

if TYPE_CHECKING:
    from natsio.client import Client

__all__ = ["Msg"]


@dataclass(frozen=True, slots=True)
class Msg:
    """A message received from the server.

    ``headers`` is ``None`` when the message carried no header block. ``status``
    is set only for the server's control messages (e.g. a 503 no-responders
    reply), which carry a status line instead of, or alongside, headers.
    """

    subject: str
    payload: bytes
    reply: str | None = None
    headers: Headers | None = None
    status: InlineStatus | None = None
    sid: int = 0
    _client: "Client | None" = field(default=None, repr=False, compare=False)

    @property
    def data(self) -> bytes:
        """Alias for :attr:`payload`."""
        return self.payload

    async def respond(self, payload: bytes | str = b"", *, headers: HeadersInput | None = None) -> None:
        """Publish a reply to this message's reply subject."""
        if not self.reply:
            raise NoReplySubjectError(f"message on {self.subject!r} has no reply subject")
        if self._client is None:  # pragma: no cover - only constructible in tests
            raise NoReplySubjectError("message is not bound to a client")
        await self._client.publish(self.reply, payload, headers=headers)
