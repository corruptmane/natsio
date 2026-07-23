"""`STATSZ` response models — the periodic server stats snapshot.

Field names pinned to orbit.go `natssysclient/statsz_server.go`.

The one envelope that is *not* `{server, data}`: `STATSZ` puts its payload
under `statsz`, because the same struct is what the server broadcasts on
`$SYS.SERVER.<id>.STATSZ`. `StatszResponse.statsz` keeps that wire name.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated

from natsio._internal.jsonmodel import RFC3339, JsonModel

from .entities import APIError, DataStats, JetStreamVarz, ServerInfo

__all__ = ["GatewayStat", "RouteStat", "ServerStats", "StatszResponse"]


@dataclass(slots=True, kw_only=True)
class RouteStat(JsonModel):
    """Per-route traffic. `rid` is the route id."""

    rid: int = 0
    name: str | None = None
    sent: DataStats = field(default_factory=DataStats)
    received: DataStats = field(default_factory=DataStats)
    pending: int = 0


@dataclass(slots=True, kw_only=True)
class GatewayStat(JsonModel):
    """Per-gateway traffic. `gwid` is the gateway id."""

    gwid: int = 0
    name: str = ""
    sent: DataStats = field(default_factory=DataStats)
    received: DataStats = field(default_factory=DataStats)
    inbound_connections: int = 0


@dataclass(slots=True, kw_only=True)
class ServerStats(JsonModel):
    """Counters this server periodically publishes."""

    start: Annotated[datetime | None, RFC3339] = None
    mem: int = 0
    cores: int = 0
    cpu: float = 0.0
    connections: int = 0
    total_connections: int = 0
    active_accounts: int = 0
    subscriptions: int = 0
    sent: DataStats = field(default_factory=DataStats)
    received: DataStats = field(default_factory=DataStats)
    slow_consumers: int = 0
    routes: list[RouteStat] | None = None
    gateways: list[GatewayStat] | None = None
    active_servers: int | None = None
    jetstream: JetStreamVarz | None = None


@dataclass(slots=True, kw_only=True)
class StatszResponse(JsonModel):
    """`{server, statsz}` envelope returned by `$SYS.REQ.SERVER.<id>.STATSZ`."""

    server: ServerInfo = field(default_factory=ServerInfo)
    statsz: ServerStats = field(default_factory=ServerStats)
    error: APIError | None = None
