"""JetStream messages: the ack surface and ack-reply metadata."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from natsio.errors import NATSError
from natsio.message import Msg

if TYPE_CHECKING:
    from natsio.client import Client

from .errors import JetStreamError

__all__ = ["AckMetadata", "JsMsg", "MessageAlreadyAckedError"]


class MessageAlreadyAckedError(JetStreamError):
    """A terminal acknowledgement was already sent for this message."""


@dataclass(frozen=True, slots=True)
class AckMetadata:
    """Decoded ``$JS.ACK`` reply-subject tokens.

    Handles both the v1 (9-token) and v2 (12-token, with domain and account
    hash) forms; a v2 domain of ``_`` means "no domain".
    """

    stream: str
    consumer: str
    num_delivered: int
    stream_seq: int
    consumer_seq: int
    timestamp: datetime
    num_pending: int
    domain: str | None = None

    @classmethod
    def from_reply(cls, reply: str) -> "AckMetadata":
        tokens = reply.split(".")
        if len(tokens) < 9 or tokens[0] != "$JS" or tokens[1] != "ACK":
            raise NATSError(f"not a JetStream ack reply subject: {reply!r}")
        if len(tokens) >= 12:
            domain = None if tokens[2] == "_" else tokens[2]
            # tokens[3] is the account hash; unused client-side.
            stream, consumer = tokens[4], tokens[5]
            delivered, sseq, cseq, ts, pending = tokens[6:11]
        else:
            domain = None
            stream, consumer = tokens[2], tokens[3]
            delivered, sseq, cseq, ts, pending = tokens[4:9]
        return cls(
            stream=stream,
            consumer=consumer,
            num_delivered=int(delivered),
            stream_seq=int(sseq),
            consumer_seq=int(cseq),
            timestamp=datetime.fromtimestamp(int(ts) / 1_000_000_000, tz=UTC),
            num_pending=int(pending),
            domain=domain,
        )


class JsMsg:
    """A message delivered by a JetStream consumer.

    Wraps the core :class:`~natsio.message.Msg` and adds acknowledgement.
    ``ack``/``nak``/``term`` are terminal — sending a second terminal ack
    raises :class:`MessageAlreadyAckedError`; ``in_progress`` may be sent any
    number of times before a terminal ack.
    """

    __slots__ = ("_acked", "_client", "_metadata", "msg")

    def __init__(self, msg: Msg, client: "Client") -> None:
        self.msg = msg
        self._client = client
        self._acked = False
        self._metadata: AckMetadata | None = None

    # -- payload passthrough -------------------------------------------------

    @property
    def subject(self) -> str:
        return self.msg.subject

    @property
    def payload(self) -> bytes:
        return self.msg.payload

    @property
    def data(self) -> bytes:
        return self.msg.payload

    @property
    def headers(self):
        return self.msg.headers

    @property
    def metadata(self) -> AckMetadata:
        if self._metadata is None:
            if not self.msg.reply:
                raise NATSError("message carries no ack reply subject")
            self._metadata = AckMetadata.from_reply(self.msg.reply)
        return self._metadata

    def __repr__(self) -> str:
        return f"JsMsg(subject={self.subject!r}, len={len(self.payload)}, acked={self._acked})"

    # -- acknowledgement -----------------------------------------------------

    def _reply_subject(self) -> str:
        if not self.msg.reply:
            raise NATSError("message carries no ack reply subject")
        return self.msg.reply

    def _mark_terminal(self) -> None:
        if self._acked:
            raise MessageAlreadyAckedError(f"message on {self.subject!r} was already acknowledged")
        self._acked = True

    async def ack(self) -> None:
        """Acknowledge successful processing."""
        reply = self._reply_subject()
        self._mark_terminal()
        try:
            await self._client.publish(reply, b"+ACK")
        except BaseException:
            self._acked = False  # the frame never left; allow a retry
            raise

    async def ack_sync(self, timeout: float | None = 5.0) -> None:  # noqa: ASYNC109
        """Acknowledge and wait for the server to confirm it was recorded."""
        reply = self._reply_subject()
        self._mark_terminal()
        try:
            await self._client.request(reply, b"+ACK", timeout=timeout)
        except BaseException:
            self._acked = False  # not confirmed; allow a retry
            raise

    async def nak(self, *, delay: timedelta | float | None = None) -> None:
        """Negative-acknowledge: redeliver (optionally after ``delay``)."""
        reply = self._reply_subject()
        self._mark_terminal()
        if delay is None:
            payload = b"-NAK"
        else:
            seconds = delay.total_seconds() if isinstance(delay, timedelta) else float(delay)
            payload = b"-NAK " + json.dumps({"delay": int(seconds * 1_000_000_000)}).encode()
        try:
            await self._client.publish(reply, payload)
        except BaseException:
            self._acked = False
            raise

    async def term(self, reason: str = "") -> None:
        """Terminate delivery: never redeliver this message."""
        reply = self._reply_subject()
        self._mark_terminal()
        payload = b"+TERM" if not reason else b"+TERM " + reason.encode()
        try:
            await self._client.publish(reply, payload)
        except BaseException:
            self._acked = False
            raise

    async def in_progress(self) -> None:
        """Reset the ack-wait timer; the message is still being worked on."""
        if self._acked:
            raise MessageAlreadyAckedError(f"message on {self.subject!r} was already acknowledged")
        await self._client.publish(self._reply_subject(), b"+WPI")
