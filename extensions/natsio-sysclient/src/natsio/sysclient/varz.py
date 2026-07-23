"""`VARZ` response models — general server information.

Field names pinned to orbit.go `natssysclient/varz.go` and re-verified against
nats-server 2.14.3. Keys 2.14 added that the oracle does not name (for example
`config_digest`, `feature_flags`, `in_client_msgs`, `stale_connection_stats`)
are captured in each model's `extra`.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Annotated, Any

from natsio._internal.jsonmodel import NS_DURATION, RFC3339, JsonModel

from .entities import APIError, JetStreamVarz, ServerInfo, SlowConsumersStats

__all__ = [
    "ClusterOptsVarz",
    "DenyRules",
    "GatewayOptsVarz",
    "LeafNodeOptsVarz",
    "MQTTOptsVarz",
    "OCSPResponseCacheVarz",
    "RemoteGatewayOptsVarz",
    "RemoteLeafOptsVarz",
    "Varz",
    "VarzResponse",
    "WebsocketOptsVarz",
]


@dataclass(slots=True, kw_only=True)
class ClusterOptsVarz(JsonModel):
    """Route/cluster configuration. `addr` and `cluster_port` are wire names."""

    name: str | None = None
    addr: str | None = None
    cluster_port: int | None = None
    auth_timeout: float | None = None
    urls: list[str] | None = None
    tls_timeout: float | None = None
    tls_required: bool | None = None
    tls_verify: bool | None = None
    pool_size: int | None = None


@dataclass(slots=True, kw_only=True)
class RemoteGatewayOptsVarz(JsonModel):
    name: str = ""
    tls_timeout: float | None = None
    urls: list[str] | None = None


@dataclass(slots=True, kw_only=True)
class GatewayOptsVarz(JsonModel):
    name: str | None = None
    host: str | None = None
    port: int | None = None
    auth_timeout: float | None = None
    tls_timeout: float | None = None
    tls_required: bool | None = None
    tls_verify: bool | None = None
    advertise: str | None = None
    connect_retries: int | None = None
    gateways: list[RemoteGatewayOptsVarz] | None = None
    reject_unknown: bool | None = None


@dataclass(slots=True, kw_only=True)
class DenyRules(JsonModel):
    """Subjects a leafnode remote may not import/export."""

    exports: list[str] | None = None
    imports: list[str] | None = None


@dataclass(slots=True, kw_only=True)
class RemoteLeafOptsVarz(JsonModel):
    local_account: str | None = None
    tls_timeout: float | None = None
    urls: list[str] | None = None
    deny: DenyRules | None = None
    tls_ocsp_peer_verify: bool | None = None


@dataclass(slots=True, kw_only=True)
class LeafNodeOptsVarz(JsonModel):
    host: str | None = None
    port: int | None = None
    auth_timeout: float | None = None
    tls_timeout: float | None = None
    tls_required: bool | None = None
    tls_verify: bool | None = None
    remotes: list[RemoteLeafOptsVarz] | None = None
    tls_ocsp_peer_verify: bool | None = None


@dataclass(slots=True, kw_only=True)
class MQTTOptsVarz(JsonModel):
    host: str | None = None
    port: int | None = None
    no_auth_user: str | None = None
    auth_timeout: float | None = None
    tls_map: bool | None = None
    tls_timeout: float | None = None
    tls_pinned_certs: list[str] | None = None
    js_domain: str | None = None
    ack_wait: Annotated[timedelta | None, NS_DURATION] = None
    max_ack_pending: int | None = None
    tls_ocsp_peer_verify: bool | None = None


@dataclass(slots=True, kw_only=True)
class WebsocketOptsVarz(JsonModel):
    host: str | None = None
    port: int | None = None
    advertise: str | None = None
    no_auth_user: str | None = None
    jwt_cookie: str | None = None
    handshake_timeout: Annotated[timedelta | None, NS_DURATION] = None
    auth_timeout: float | None = None
    no_tls: bool | None = None
    tls_map: bool | None = None
    tls_pinned_certs: list[str] | None = None
    same_origin: bool | None = None
    allowed_origins: list[str] | None = None
    compression: bool | None = None
    tls_ocsp_peer_verify: bool | None = None


@dataclass(slots=True, kw_only=True)
class OCSPResponseCacheVarz(JsonModel):
    cache_type: str | None = None
    cache_hits: int | None = None
    cache_misses: int | None = None
    cached_responses: int | None = None
    cached_revoked_responses: int | None = None
    cached_good_responses: int | None = None
    cached_unknown_responses: int | None = None


@dataclass(slots=True, kw_only=True)
class Varz(JsonModel):
    """General information about one server.

    `uptime` is the server's own pre-formatted string (`"2h13m5s"`), not a
    duration — that is what the endpoint emits. `ping_interval` and
    `write_deadline` *are* durations (nanoseconds on the wire).
    """

    server_id: str = ""
    server_name: str = ""
    version: str = ""
    proto: int = 0
    git_commit: str | None = None
    go: str = ""
    host: str = ""
    port: int = 0
    auth_required: bool | None = None
    tls_required: bool | None = None
    tls_verify: bool | None = None
    tls_ocsp_peer_verify: bool | None = None
    ip: str | None = None
    connect_urls: list[str] | None = None
    ws_connect_urls: list[str] | None = None
    max_connections: int = 0
    max_subscriptions: int | None = None
    ping_interval: Annotated[timedelta | None, NS_DURATION] = None
    ping_max: int = 0
    http_host: str = ""
    http_port: int = 0
    http_base_path: str = ""
    https_port: int = 0
    auth_timeout: float = 0.0
    max_control_line: int = 0
    max_payload: int = 0
    max_pending: int = 0
    cluster: ClusterOptsVarz = field(default_factory=ClusterOptsVarz)
    gateway: GatewayOptsVarz = field(default_factory=GatewayOptsVarz)
    leaf: LeafNodeOptsVarz = field(default_factory=LeafNodeOptsVarz)
    mqtt: MQTTOptsVarz = field(default_factory=MQTTOptsVarz)
    websocket: WebsocketOptsVarz = field(default_factory=WebsocketOptsVarz)
    jetstream: JetStreamVarz = field(default_factory=JetStreamVarz)
    tls_timeout: float = 0.0
    write_deadline: Annotated[timedelta | None, NS_DURATION] = None
    start: Annotated[datetime | None, RFC3339] = None
    now: Annotated[datetime | None, RFC3339] = None
    uptime: str = ""
    mem: int = 0
    cores: int = 0
    gomaxprocs: int = 0
    cpu: float = 0.0
    connections: int = 0
    total_connections: int = 0
    routes: int = 0
    remotes: int = 0
    leafnodes: int = 0
    in_msgs: int = 0
    out_msgs: int = 0
    in_bytes: int = 0
    out_bytes: int = 0
    slow_consumers: int = 0
    subscriptions: int = 0
    http_req_stats: dict[str, int] | None = None
    config_load_time: Annotated[datetime | None, RFC3339] = None
    tags: list[str] | None = None
    trusted_operators_jwt: list[str] | None = None
    # Operator JWT claims are left as raw JSON: natsio ships no JWT decoder and
    # this package adds no dependency to gain one.
    trusted_operators_claim: list[dict[str, Any]] | None = None
    system_account: str | None = None
    pinned_account_fails: int | None = None
    ocsp_peer_cache: OCSPResponseCacheVarz | None = None
    slow_consumer_stats: SlowConsumersStats | None = None


@dataclass(slots=True, kw_only=True)
class VarzResponse(JsonModel):
    """`{server, data}` envelope returned by `$SYS.REQ.SERVER.<id>.VARZ`."""

    server: ServerInfo = field(default_factory=ServerInfo)
    data: Varz = field(default_factory=Varz)
    error: APIError | None = None
