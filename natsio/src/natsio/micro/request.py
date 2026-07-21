"""The request handed to a micro endpoint handler."""

from natsio._internal.protocol import Headers, HeadersInput
from natsio.message import Msg

from .entities import ERROR_CODE_HEADER, ERROR_HEADER
from .errors import ServiceConfigError

__all__ = ["Request"]


class Request:
    """A single service request, exposing the payload and reply helpers.

    A handler is ``async def handler(req: Request) -> None``. It responds with
    :meth:`respond` (or :meth:`respond_error` for a failure) exactly once;
    returning without responding is allowed but leaves the caller waiting.
    """

    __slots__ = ("_msg", "_responded", "_response_error")

    def __init__(self, msg: Msg) -> None:
        self._msg = msg
        self._responded = False
        # Set to "<code>:<description>" once an error response is produced (by
        # respond_error or the automatic handler-exception path); read by the
        # service to bump num_errors / last_error, mirroring nats.go.
        self._response_error: str | None = None

    @property
    def subject(self) -> str:
        """The subject the request arrived on."""
        return self._msg.subject

    @property
    def reply(self) -> str | None:
        """The reply subject, or ``None`` when the caller expects no response."""
        return self._msg.reply

    @property
    def headers(self) -> Headers | None:
        """The request headers, or ``None`` when the message carried none."""
        return self._msg.headers

    @property
    def payload(self) -> bytes:
        """The request payload."""
        return self._msg.payload

    @property
    def data(self) -> bytes:
        """Alias for :attr:`payload`."""
        return self._msg.payload

    async def respond(self, payload: bytes | str = b"", *, headers: HeadersInput | None = None) -> None:
        """Send a successful reply. No-op semantics are the caller's concern:
        calling twice publishes twice to the (already-consumed) reply inbox."""
        await self._msg.respond(payload, headers=headers)
        self._responded = True

    async def respond_error(
        self,
        code: str,
        description: str,
        data: bytes | str = b"",
        *,
        headers: HeadersInput | None = None,
    ) -> None:
        """Send an error reply, setting the ``Nats-Service-Error`` and
        ``Nats-Service-Error-Code`` headers. Counts toward the endpoint's
        ``num_errors`` and becomes its ``last_error``."""
        if not code:
            raise ServiceConfigError("error code is required")
        if not description:
            raise ServiceConfigError("error description is required")
        out = Headers(headers) if headers is not None else Headers()
        out.set(ERROR_HEADER, description)
        out.set(ERROR_CODE_HEADER, code)
        await self._msg.respond(data, headers=out)
        self._responded = True
        self._response_error = f"{code}:{description}"
