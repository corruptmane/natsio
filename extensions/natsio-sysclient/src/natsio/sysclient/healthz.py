"""`HEALTHZ` response models.

Field names pinned to orbit.go `natssysclient/healthz.go`.

A *failed health check* is not an API error: the server answers with a normal
`data` block whose `status` is `"error"` and whose `status_code` is 4xx/5xx.
`SysClient.healthz()` therefore returns it rather than raising — only an
`error` envelope (a malformed request, say) raises `SysAPIError`.
"""

from dataclasses import dataclass, field
from enum import StrEnum

from natsio._internal.jsonmodel import JsonModel

from .entities import APIError, ServerInfo

__all__ = ["Healthz", "HealthzError", "HealthzErrorType", "HealthzResponse"]


class HealthzErrorType(StrEnum):
    """Category of a detailed health-check failure.

    **Divergence from the oracle, verified live.** orbit.go declares
    `HealthZErrorType` as an `int` and enumerates six ordinals; nats-server
    2.14.3 marshals the field as an upper-case *string*, so the Go struct
    cannot actually decode a detailed `HEALTHZ` failure. Only the four values
    below were reproducible against 2.14.3 (bad request, unknown account,
    unknown stream, unknown consumer); the two the oracle also names
    (connection, JetStream) could not be provoked, so their exact spelling is
    unverified and deliberately not guessed at here.

    `HealthzError.type` is therefore a plain `str`: an unrecognised category
    from a newer server must not turn a health report into a decode failure.
    """

    BAD_REQUEST = "BAD_REQUEST"
    ACCOUNT = "ACCOUNT"
    STREAM = "STREAM"
    CONSUMER = "CONSUMER"


@dataclass(slots=True, kw_only=True)
class HealthzError(JsonModel):
    """One detailed failure — only present when the request set `details`.

    Compare `type` against `HealthzErrorType`; it is `str`-typed on purpose.
    """

    type: str = ""
    account: str | None = None
    stream: str | None = None
    consumer: str | None = None
    error: str | None = None


@dataclass(slots=True, kw_only=True)
class Healthz(JsonModel):
    """Health of one server. `status` is `"ok"` when healthy."""

    status: str = ""
    status_code: int | None = None
    error: str | None = None
    errors: list[HealthzError] | None = None


@dataclass(slots=True, kw_only=True)
class HealthzResponse(JsonModel):
    """`{server, data}` envelope returned by `$SYS.REQ.SERVER.<id>.HEALTHZ`."""

    server: ServerInfo = field(default_factory=ServerInfo)
    data: Healthz = field(default_factory=Healthz)
    error: APIError | None = None
