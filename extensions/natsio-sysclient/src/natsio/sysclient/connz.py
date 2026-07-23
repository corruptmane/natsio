"""`CONNZ` response models — per-connection detail, offset/limit paged.

Field names pinned to orbit.go `natssysclient/connz.go`.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated

from natsio._internal.jsonmodel import RFC3339, JsonModel

from .entities import APIError, ServerInfo, SubDetail

__all__ = ["ConnInfo", "Connz", "ConnzResponse", "TLSPeerCert"]


@dataclass(slots=True, kw_only=True)
class TLSPeerCert(JsonModel):
    subject: str | None = None
    spki_sha256: str | None = None
    cert_sha256: str | None = None


@dataclass(slots=True, kw_only=True)
class ConnInfo(JsonModel):
    """One connection.

    `rtt`, `uptime` and `idle` are the server's pre-formatted strings
    (`"213µs"`, `"3m12s"`), not durations. `subscriptions_list` /
    `subscriptions_list_detail` are only populated when the request set
    `subscriptions` / `subscriptions_detail`.
    """

    cid: int = 0
    kind: str | None = None
    type: str | None = None
    ip: str = ""
    port: int = 0
    start: Annotated[datetime | None, RFC3339] = None
    last_activity: Annotated[datetime | None, RFC3339] = None
    stop: Annotated[datetime | None, RFC3339] = None
    reason: str | None = None
    rtt: str | None = None
    uptime: str = ""
    idle: str = ""
    pending_bytes: int = 0
    in_msgs: int = 0
    out_msgs: int = 0
    in_bytes: int = 0
    out_bytes: int = 0
    subscriptions: int = 0
    name: str | None = None
    lang: str | None = None
    version: str | None = None
    tls_version: str | None = None
    tls_cipher_suite: str | None = None
    tls_peer_certs: list[TLSPeerCert] | None = None
    tls_first: bool | None = None
    authorized_user: str | None = None
    account: str | None = None
    subscriptions_list: list[str] | None = None
    subscriptions_list_detail: list[SubDetail] | None = None
    jwt: str | None = None
    issuer_key: str | None = None
    name_tag: str | None = None
    tags: list[str] | None = None
    mqtt_client: str | None = None


@dataclass(slots=True, kw_only=True)
class Connz(JsonModel):
    """One page of connections.

    `num_connections` is the size of *this* page; `total` is the size of the
    full result set the page was cut from.
    """

    server_id: str = ""
    now: Annotated[datetime | None, RFC3339] = None
    num_connections: int = 0
    total: int = 0
    offset: int = 0
    limit: int = 0
    connections: list[ConnInfo] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class ConnzResponse(JsonModel):
    """`{server, data}` envelope returned by `$SYS.REQ.SERVER.<id>.CONNZ`."""

    server: ServerInfo = field(default_factory=ServerInfo)
    data: Connz = field(default_factory=Connz)
    error: APIError | None = None
