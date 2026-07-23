"""`JSZ` response models — per-server JetStream state, paged by account.

Field names pinned to orbit.go `natssysclient/jsz_server.go`. Where the oracle
reuses `nats.go` types (`StreamConfig`, `StreamState`, `ClusterInfo`,
`ConsumerInfo`) this module reuses natsio's equivalents, which are the same
entities pinned to the same ADRs.

Go embeds `JetStreamStats` into `JSInfo` and `AccountDetail` without a JSON
tag, so its keys (`memory`, `storage`, `accounts`, `api`, …) are *inlined* at
the top level of each. `JsonModel` has no inlining, so those fields are spelled
out here — that is the wire shape, verified live against 2.14.3.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Annotated

from natsio._internal.jsonmodel import NS_DURATION, RFC3339, JsonModel
from natsio.jetstream import ClusterInfo, ConsumerInfo, External, StreamConfig, StreamState, SubjectTransform

from .entities import APIError, JetStreamAPIStats, JetStreamConfig, MetaClusterInfo, ServerInfo

__all__ = [
    "AccountDetail",
    "JSInfo",
    "JszResponse",
    "RaftGroupDetail",
    "StreamDetail",
    "StreamSourceInfo",
]


@dataclass(slots=True, kw_only=True)
class StreamSourceInfo(JsonModel):
    """Mirror/source progress (nats.go `StreamSourceInfo`)."""

    name: str = ""
    lag: int = 0
    active: Annotated[timedelta | None, NS_DURATION] = None
    external: External | None = None
    error: APIError | None = None
    filter_subject: str | None = None
    subject_transforms: list[SubjectTransform] | None = None


@dataclass(slots=True, kw_only=True)
class RaftGroupDetail(JsonModel):
    name: str = ""
    raft_group: str | None = None


@dataclass(slots=True, kw_only=True)
class StreamDetail(JsonModel):
    """One stream. `config` and `consumer_detail` need `config`/`consumer` set."""

    name: str = ""
    created: Annotated[datetime | None, RFC3339] = None
    cluster: ClusterInfo | None = None
    config: StreamConfig | None = None
    state: StreamState | None = None
    consumer_detail: list[ConsumerInfo] | None = None
    mirror: StreamSourceInfo | None = None
    sources: list[StreamSourceInfo] | None = None
    stream_raft_group: str | None = None
    consumer_raft_groups: list[RaftGroupDetail] | None = None


@dataclass(slots=True, kw_only=True)
class AccountDetail(JsonModel):
    """One account's JetStream usage, with the embedded `JetStreamStats` inlined."""

    name: str = ""
    id: str = ""
    memory: int = 0
    storage: int = 0
    reserved_memory: int = 0
    reserved_storage: int = 0
    accounts: int = 0
    ha_assets: int = 0
    api: JetStreamAPIStats = field(default_factory=JetStreamAPIStats)
    stream_detail: list[StreamDetail] | None = None


@dataclass(slots=True, kw_only=True)
class JSInfo(JsonModel):
    """This server's JetStream state, with the embedded `JetStreamStats` inlined.

    `accounts` doubles as the paging total for `account_details` — that is what
    `SysClient.all_jsz()` walks. A server with JetStream switched off answers
    with `disabled` set and nothing else populated.
    """

    server_id: str = ""
    now: Annotated[datetime | None, RFC3339] = None
    disabled: bool | None = None
    config: JetStreamConfig = field(default_factory=JetStreamConfig)
    memory: int = 0
    storage: int = 0
    reserved_memory: int = 0
    reserved_storage: int = 0
    accounts: int = 0
    ha_assets: int = 0
    api: JetStreamAPIStats = field(default_factory=JetStreamAPIStats)
    streams: int = 0
    consumers: int = 0
    messages: int = 0
    bytes: int = 0
    meta_cluster: MetaClusterInfo | None = None
    account_details: list[AccountDetail] | None = None


@dataclass(slots=True, kw_only=True)
class JszResponse(JsonModel):
    """`{server, data}` envelope returned by `$SYS.REQ.SERVER.<id>.JSZ`."""

    server: ServerInfo = field(default_factory=ServerInfo)
    data: JSInfo = field(default_factory=JSInfo)
    error: APIError | None = None
