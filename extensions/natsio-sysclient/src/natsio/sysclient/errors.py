"""Typed errors for the system/monitoring API (mirrors orbit.go `natssysclient/api.go`).

The oracle's sentinel errors map one-to-one:

| orbit.go             | natsio.sysclient        |
|----------------------|-------------------------|
| `ErrValidation`      | `SysValidationError`    |
| `ErrInvalidServerID` | `InvalidServerIDError`  |
| `ErrPingResponse`    | `NoResponsesError`      |
| (untyped decode)     | `InvalidResponseError`  |
| (untyped `resp.Error`) | `SysAPIError`         |

`ErrRequest` has no counterpart: natsio already raises typed
`natsio.errors.TimeoutError` / `ConnectionClosedError` for a failed request,
and re-wrapping them would hide which one happened.
"""

from natsio.errors import NATSError

__all__ = [
    "InvalidResponseError",
    "InvalidServerIDError",
    "NoResponsesError",
    "PagerStateError",
    "SysAPIError",
    "SysClientError",
    "SysValidationError",
]


class SysClientError(NATSError):
    """Root of the `natsio.sysclient` exception tree."""


class SysValidationError(SysClientError, ValueError):
    """A request argument or client option was rejected before any I/O."""


class InvalidServerIDError(SysClientError):
    """No server answered `$SYS.REQ.SERVER.<id>.<ENDPOINT>`.

    Each server subscribes to its *own* id only, so a no-responders reply means
    the id does not name a server this connection can reach (wrong id, wrong
    system account, or the server left the cluster).
    """


class NoResponsesError(SysClientError):
    """A cluster ping collected zero responses.

    Loud on purpose: an empty list would be indistinguishable from "a healthy
    cluster of zero servers". The message names which bound actually fired and
    how long the gather really took:

    - *the reply stream ended with no responders* — nothing is subscribed to
      `$SYS.REQ.SERVER.PING.<ENDPOINT>` on this connection, i.e. it is not
      bound to the system account (or every server was filtered out by an
      `EventFilterOptions` field);
    - *the overall timeout elapsed* — servers exist but none answered in time.

    `stall` can never be the bound here: it only limits the gap *between*
    replies, so with zero replies it is never armed. A connection that closed
    mid-gather raises `natsio.errors.ConnectionClosedError` instead.
    """


class PagerStateError(SysClientError, RuntimeError):
    """A `PagedResponses` was used out of order.

    Pagers are single-pass and single-consumer: re-iterating a spent pager,
    advancing one from two tasks at once, or closing one while another task is
    advancing it are all mistakes. Raised instead of silently yielding zero
    pages (or letting a bare `RuntimeError` out of the generator machinery).
    """


class InvalidResponseError(SysClientError):
    """A response payload was not a JSON object."""


class SysAPIError(SysClientError):
    """The server answered with an `error` envelope instead of data.

    Carries the server's own `code` (HTTP-shaped), optional `err_code`, and
    `description` — e.g. code 400 for an unparseable request payload.
    """

    def __init__(self, code: int, description: str, err_code: int | None = None) -> None:
        super().__init__(f"server API error {code}: {description}" if description else f"server API error {code}")
        self.code = code
        self.err_code = err_code
        self.api_description = description
