"""Wire entities shared by more than one `$SYS.REQ.SERVER` endpoint.

Field names are pinned to orbit.go `natssysclient` (`api.go`, `varz.go`,
`statsz_server.go`, `connz.go`) and were re-verified against a live
nats-server 2.14.3. Every model is a `JsonModel`, so fields a newer server
adds land in `extra` and survive a round-trip instead of being dropped —
2.14.3 already sends several the oracle's structs do not name (`feature_flags`
on `ServerInfo`, `limits` on the JetStream block, `sent_to_clients` on
`ServerStats`).

Go `time.Duration` fields travel as integer **nanoseconds** and are decoded to
`timedelta`; `time.Time` fields are RFC 3339 strings decoded to aware
`datetime`. Fields the server sends as pre-formatted human strings (`uptime`,
`rtt`, `idle`) stay `str` — that is what is on the wire.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Annotated

from natsio._internal.jsonmodel import NS_DURATION, RFC3339, JsonModel

__all__ = [
    "APIError",
    "DataStats",
    "JetStreamAPIStats",
    "JetStreamConfig",
    "JetStreamStats",
    "JetStreamVarz",
    "MetaClusterInfo",
    "PeerInfo",
    "ServerInfo",
    "SlowConsumersStats",
    "SubDetail",
]


@dataclass(slots=True, kw_only=True)
class ServerInfo(JsonModel):
    """The `server` envelope every `$SYS` monitoring response carries.

    `id` is the server id to feed back into the by-id request shape.
    """

    name: str = ""
    host: str = ""
    id: str = ""
    cluster: str | None = None
    domain: str | None = None
    ver: str = ""
    tags: list[str] | None = None
    seq: int = 0
    jetstream: bool = False
    time: Annotated[datetime | None, RFC3339] = None


@dataclass(slots=True, kw_only=True)
class APIError(JsonModel):
    """The `error` envelope a server returns instead of `data`.

    Surfaced as `SysAPIError`; modeled so the raw shape stays inspectable.
    """

    code: int = 0
    err_code: int | None = None
    description: str | None = None


@dataclass(slots=True, kw_only=True)
class DataStats(JsonModel):
    """Message/byte counters, used for both directions of a `STATSZ` link."""

    msgs: int = 0
    bytes: int = 0


@dataclass(slots=True, kw_only=True)
class SubDetail(JsonModel):
    """One subscription, as reported by `SUBSZ` and by `CONNZ` detail mode."""

    account: str | None = None
    subject: str = ""
    qgroup: str | None = None
    sid: str = ""
    msgs: int = 0
    max: int | None = None
    cid: int = 0


@dataclass(slots=True, kw_only=True)
class JetStreamAPIStats(JsonModel):
    total: int = 0
    errors: int = 0
    inflight: int | None = None


@dataclass(slots=True, kw_only=True)
class JetStreamConfig(JsonModel):
    """This server's JetStream configuration. Sizes are bytes."""

    max_memory: int = 0
    max_storage: int = 0
    store_dir: str | None = None
    sync_interval: Annotated[timedelta | None, NS_DURATION] = None
    sync_always: bool | None = None
    domain: str | None = None
    compress_ok: bool | None = None
    unique_tag: str | None = None


@dataclass(slots=True, kw_only=True)
class JetStreamStats(JsonModel):
    """Aggregate JetStream usage. `storage` is the wire name for on-disk bytes."""

    memory: int = 0
    storage: int = 0
    reserved_memory: int = 0
    reserved_storage: int = 0
    accounts: int = 0
    ha_assets: int = 0
    api: JetStreamAPIStats = field(default_factory=JetStreamAPIStats)


@dataclass(slots=True, kw_only=True)
class PeerInfo(JsonModel):
    """A member of the JetStream meta group.

    Distinct from `natsio.jetstream.PeerInfo`: the monitoring variant also
    reports the raft `peer` name.
    """

    name: str = ""
    current: bool = False
    offline: bool | None = None
    active: Annotated[timedelta | None, NS_DURATION] = None
    lag: int | None = None
    peer: str = ""


@dataclass(slots=True, kw_only=True)
class MetaClusterInfo(JsonModel):
    """The JetStream meta (raft) group as this server sees it."""

    name: str | None = None
    leader: str | None = None
    peer: str | None = None
    replicas: list[PeerInfo] | None = None
    cluster_size: int = 0
    pending: int = 0


@dataclass(slots=True, kw_only=True)
class JetStreamVarz(JsonModel):
    """The `jetstream` block of `VARZ` / `STATSZ`."""

    config: JetStreamConfig | None = None
    stats: JetStreamStats | None = None
    meta: MetaClusterInfo | None = None


@dataclass(slots=True, kw_only=True)
class SlowConsumersStats(JsonModel):
    """Slow-consumer counters broken down by connection kind."""

    clients: int = 0
    routes: int = 0
    gateways: int = 0
    leafs: int = 0
