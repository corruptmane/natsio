"""Request options for the `$SYS.REQ.SERVER` endpoints.

JSON field names are pinned to orbit.go `natssysclient` (`varz.go`'s
`EventFilterOptions`, `connz.go`'s `ConnzOptions`, `jsz_server.go`'s
`JszOptions`, `healthz.go`'s `HealthzOptions`, `subsz_server.go`'s
`SubszOptions`).

Two deliberate departures from the Go structs, neither of them a wire change:

- Go embeds `ConnzOptions` *and* `EventFilterOptions` into `ConnzEventOptions`;
  Python has no struct embedding, so the filter fields are inherited from
  `EventFilterOptions` and flatten into the same single JSON object.
- Every field defaults to `None` and is **omitted** when unset. Some of the
  oracle's structs lack `omitempty` and therefore emit their zero values
  (`{"sort":"","auth":false,"offset":0,...}`); the server's decoder defaults a
  missing key to exactly that zero, so the two payloads are equivalent.
"""

from dataclasses import dataclass
from enum import IntEnum, StrEnum
from typing import Any

from natsio._internal.jsonmodel import JsonModel

from .errors import SysValidationError

__all__ = [
    "ConnState",
    "ConnzOptions",
    "EventFilterOptions",
    "HealthzOptions",
    "JszOptions",
    "SortOpt",
    "StatszOptions",
    "SubszOptions",
    "VarzOptions",
]


class SortOpt(StrEnum):
    """`CONNZ` sort order. Only `CID` sorts ascending; every other key descends."""

    CID = "cid"
    START = "start"
    SUBS = "subs"
    PENDING = "pending"
    OUT_MSGS = "msgs_to"
    IN_MSGS = "msgs_from"
    OUT_BYTES = "bytes_to"
    IN_BYTES = "bytes_from"
    LAST = "last"
    IDLE = "idle"
    UPTIME = "uptime"
    STOP = "stop"
    REASON = "reason"


class ConnState(IntEnum):
    """`CONNZ` connection-state filter. The wire value is the ordinal."""

    OPEN = 0
    CLOSED = 1
    ALL = 2


@dataclass(slots=True, kw_only=True)
class EventFilterOptions(JsonModel):
    """Filters shared by `STATSZ`, `VARZ`, `SUBSZ`, `CONNZ`, `JSZ`.

    On a ping they select which servers answer at all; `tags` must all match.
    """

    server_name: str | None = None
    cluster: str | None = None
    host: str | None = None
    tags: list[str] | None = None
    domain: str | None = None


@dataclass(slots=True, kw_only=True)
class VarzOptions(EventFilterOptions):
    """Options for `VARZ` — filters only."""


@dataclass(slots=True, kw_only=True)
class StatszOptions(EventFilterOptions):
    """Options for `STATSZ` — filters only."""


@dataclass(slots=True, kw_only=True)
class ConnzOptions(EventFilterOptions):
    """Options for `CONNZ`.

    `user`, `account` and `filter_subject` are only honoured when `auth` is set.
    `offset`/`limit` drive the paging that `SysClient.all_connz()` automates.
    """

    sort: SortOpt | None = None
    auth: bool | None = None
    subscriptions: bool | None = None
    subscriptions_detail: bool | None = None
    offset: int | None = None
    limit: int | None = None
    cid: int | None = None
    mqtt_client: str | None = None
    state: ConnState | None = None
    user: str | None = None
    # `acc` (not `account`) is the wire name — pinned to connz.go.
    acc: str | None = None
    filter_subject: str | None = None


@dataclass(slots=True, kw_only=True)
class JszOptions(EventFilterOptions):
    """Options for `JSZ`.

    The include flags cascade: `consumer` implies `streams`, `streams` implies
    `accounts`. Paging applies to `account_details`, so it only does anything
    with `accounts=True`.
    """

    account: str | None = None
    accounts: bool | None = None
    streams: bool | None = None
    consumer: bool | None = None
    config: bool | None = None
    leader_only: bool | None = None
    offset: int | None = None
    limit: int | None = None
    # `raft` is the wire name for "include raft group detail" — pinned to jsz_server.go.
    raft: bool | None = None
    stream_leader_only: bool | None = None


@dataclass(slots=True, kw_only=True)
class HealthzOptions(JsonModel):
    """Options for `HEALTHZ`.

    Not an `EventFilterOptions` subclass: the oracle's `HealthzOptions` does not
    embed the filters, and the hyphenated `js-enabled-only` / `js-server-only`
    keys are the server's own spelling.
    """

    js_enabled_only: bool | None = None
    js_server_only: bool | None = None
    account: str | None = None
    stream: str | None = None
    consumer: str | None = None
    details: bool | None = None

    def to_wire(self) -> dict[str, Any]:
        # The server spells these two keys with hyphens, which no Python
        # identifier can carry, so the rename happens on the way out.
        # Explicit base call, not zero-arg `super()`: `@dataclass(slots=True)`
        # rebuilds the class, so the implicit `__class__` cell would point at
        # the pre-decoration class and `super()` raises TypeError.
        out = JsonModel.to_wire(self)
        for python_name, wire_name in (("js_enabled_only", "js-enabled-only"), ("js_server_only", "js-server-only")):
            if python_name not in out:
                continue
            if wire_name in out:
                # `JsonModel.to_wire` has already merged `extra`, so the
                # hyphenated key being present means the caller set both
                # spellings. Renaming over it would silently drop the field.
                raise SysValidationError(
                    f"HealthzOptions sets both {python_name!r} and extra[{wire_name!r}] — they are the same wire key"
                )
            out[wire_name] = out.pop(python_name)
        return out


@dataclass(slots=True, kw_only=True)
class SubszOptions(JsonModel):
    """Options for `SUBSZ`.

    Not an `EventFilterOptions` subclass — matches the oracle's `SubszOptions`.
    `test` must be a literal publish subject; the server rejects wildcards with
    an `error` envelope.
    """

    offset: int | None = None
    limit: int | None = None
    subscriptions: bool | None = None
    account: str | None = None
    test: str | None = None
