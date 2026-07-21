"""Micro (ADR-32) data types: config, monitoring responses, validation.

The monitoring responses (`PingResponse`, `InfoResponse`,
`StatsResponse`) are the wire contract answered on ``$SRV.*`` — their
``type`` strings and JSON field names are fixed by ADR-32 and must match
nats.go byte-for-byte, so they are built on the same `JsonModel`
framework the JetStream API uses (unknown fields round-trip, ``None`` is
omitted).
"""

import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Any, Final

from natsio._internal.jsonmodel import RFC3339, JsonModel
from natsio._internal.validation import validate_queue_group

from .errors import ServiceConfigError, ServiceError

if TYPE_CHECKING:
    from .service import Endpoint, Service

# Lazy PEP 695 aliases: the alias names exist at runtime, but their values
# (which reference the service types) are only evaluated by type checkers, so
# there is no import cycle with service.py.
type StatsHandler = Callable[[Endpoint], Any]
"""Per-endpoint custom stats data callback; returns any JSON-serializable value."""
type ErrorHandler = Callable[[Service, ServiceError], Awaitable[None] | None]
"""Invoked (awaited if a coroutine) when a handler fails or a response errors."""

__all__ = [
    "API_PREFIX",
    "DEFAULT_QUEUE_GROUP",
    "ERROR_CODE_HEADER",
    "ERROR_HEADER",
    "INFO_RESPONSE_TYPE",
    "PING_RESPONSE_TYPE",
    "STATS_RESPONSE_TYPE",
    "EndpointInfo",
    "EndpointStats",
    "ErrorHandler",
    "InfoResponse",
    "PingResponse",
    "ServiceConfig",
    "StatsHandler",
    "StatsResponse",
    "validate_endpoint_name",
    "validate_service_name",
    "validate_version",
]

# Root of every micro control subject (ADR-32).
API_PREFIX: Final = "$SRV"
# Queue group shared by all endpoints of all services unless overridden.
DEFAULT_QUEUE_GROUP: Final = "q"

# Service error headers set on an error reply (exact names from nats.go micro).
ERROR_HEADER: Final = "Nats-Service-Error"
ERROR_CODE_HEADER: Final = "Nats-Service-Error-Code"

# Response ``type`` discriminators (ADR-32, io.nats.micro.v1.*).
PING_RESPONSE_TYPE: Final = "io.nats.micro.v1.ping_response"
INFO_RESPONSE_TYPE: Final = "io.nats.micro.v1.info_response"
STATS_RESPONSE_TYPE: Final = "io.nats.micro.v1.stats_response"

# Service and endpoint names: alphanumerics, dash and underscore only.
_NAME_RE: Final = re.compile(r"\A[A-Za-z0-9_-]+\Z")
# The official SemVer 2.0.0 grammar (semver.org), matching nats.go's regexp.
_SEMVER_RE: Final = re.compile(
    r"\A(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?\Z"
)


def validate_service_name(name: str) -> None:
    """A service name must be non-empty alphanumerics, dashes and underscores."""
    if not _NAME_RE.match(name):
        raise ServiceConfigError(f"invalid service name {name!r}: allowed characters are A-Z a-z 0-9 - _ (non-empty)")


def validate_endpoint_name(name: str) -> None:
    """An endpoint name carries the same constraints as a service name."""
    if not _NAME_RE.match(name):
        raise ServiceConfigError(f"invalid endpoint name {name!r}: allowed characters are A-Z a-z 0-9 - _ (non-empty)")


def validate_version(version: str) -> None:
    """A version must be a valid SemVer 2.0.0 string."""
    if not _SEMVER_RE.match(version):
        raise ServiceConfigError(f"invalid version {version!r}: must be a SemVer 2.0.0 string")


@dataclass(frozen=True, slots=True, kw_only=True)
class ServiceConfig:
    """Configuration for a micro service.

    ``name`` and ``version`` are required and validated loudly on construction;
    ``queue_group`` (default ``"q"``) is inherited by every endpoint unless a
    group or endpoint overrides it. ``metadata`` is immutable once the service
    is created (ADR-32).
    """

    name: str
    version: str
    description: str = ""
    metadata: dict[str, str] | None = None
    queue_group: str = DEFAULT_QUEUE_GROUP
    stats_handler: StatsHandler | None = None
    error_handler: ErrorHandler | None = None

    def __post_init__(self) -> None:
        validate_service_name(self.name)
        validate_version(self.version)
        if self.queue_group:
            validate_queue_group(self.queue_group)


@dataclass(slots=True, kw_only=True)
class EndpointInfo(JsonModel):
    """One endpoint's identity, as reported by ``$SRV.INFO``."""

    name: str = ""
    subject: str = ""
    queue_group: str = ""
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True)
class EndpointStats(JsonModel):
    """One endpoint's counters, as reported by ``$SRV.STATS``.

    ``processing_time`` and ``average_processing_time`` are integer nanoseconds
    (matching the Go ``time.Duration`` wire form). ``data`` is the optional,
    handler-defined custom payload and is omitted when absent.
    """

    name: str = ""
    subject: str = ""
    queue_group: str = ""
    num_requests: int = 0
    num_errors: int = 0
    last_error: str = ""
    processing_time: int = 0
    average_processing_time: int = 0
    data: Any = None


@dataclass(slots=True, kw_only=True)
class PingResponse(JsonModel):
    """Reply to ``$SRV.PING`` (``io.nats.micro.v1.ping_response``)."""

    name: str = ""
    id: str = ""
    version: str = ""
    metadata: dict[str, str] = field(default_factory=dict)
    type: str = PING_RESPONSE_TYPE


@dataclass(slots=True, kw_only=True)
class InfoResponse(JsonModel):
    """Reply to ``$SRV.INFO`` (``io.nats.micro.v1.info_response``)."""

    name: str = ""
    id: str = ""
    version: str = ""
    metadata: dict[str, str] = field(default_factory=dict)
    type: str = INFO_RESPONSE_TYPE
    description: str = ""
    endpoints: list[EndpointInfo] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class StatsResponse(JsonModel):
    """Reply to ``$SRV.STATS`` (``io.nats.micro.v1.stats_response``)."""

    name: str = ""
    id: str = ""
    version: str = ""
    metadata: dict[str, str] = field(default_factory=dict)
    type: str = STATS_RESPONSE_TYPE
    started: Annotated[datetime | None, RFC3339] = None
    endpoints: list[EndpointStats] = field(default_factory=list)
