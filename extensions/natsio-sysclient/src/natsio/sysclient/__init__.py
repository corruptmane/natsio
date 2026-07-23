"""NATS system/monitoring API client — the `$SYS.REQ.SERVER` endpoints.

Port of orbit.go's `natssysclient`. Connect with system-account credentials,
wrap the client, and ask one server or the whole cluster:

    import natsio
    from natsio.sysclient import SysClient, ConnzOptions

    nc = await natsio.connect("nats://localhost:4222", user="sys", password="pw")
    sys = SysClient(nc)

    # every server that answers within the stall/timeout window
    for varz in await sys.varz_ping():
        print(varz.server.id, varz.data.version, varz.data.connections)

    # one named server
    health = await sys.healthz(varz.server.id)
    assert health.data.status == "ok"

    # offset/limit paging, walked for you
    async for page in sys.all_connz(varz.server.id, ConnzOptions(limit=64, auth=True)):
        for conn in page.data.connections:
            print(conn.cid, conn.authorized_user, conn.account)

Endpoints: `VARZ`, `CONNZ`, `JSZ`, `HEALTHZ`, `STATSZ`, `SUBSZ` — each with a
by-id method (`varz`), a cluster-ping method (`varz_ping`), and, where the
endpoint pages, an async iterator (`all_connz`, `all_subsz`, `all_jsz`).

Responses are `JsonModel`s: fields a newer server adds are preserved in `extra`
instead of being dropped.
"""

from natsio.sysclient.client import (
    DEFAULT_REQUEST_TIMEOUT,
    DEFAULT_STALL,
    PING_TARGET,
    SYS_REQ_SERVER_PREFIX,
    Endpoint,
    PagedResponses,
    SysClient,
    SysClientOptions,
)
from natsio.sysclient.connz import ConnInfo, Connz, ConnzResponse, TLSPeerCert
from natsio.sysclient.entities import (
    APIError,
    DataStats,
    JetStreamAPIStats,
    JetStreamConfig,
    JetStreamStats,
    JetStreamVarz,
    MetaClusterInfo,
    PeerInfo,
    ServerInfo,
    SlowConsumersStats,
    SubDetail,
)
from natsio.sysclient.errors import (
    InvalidResponseError,
    InvalidServerIDError,
    NoResponsesError,
    PagerStateError,
    SysAPIError,
    SysClientError,
    SysValidationError,
)
from natsio.sysclient.healthz import Healthz, HealthzError, HealthzErrorType, HealthzResponse
from natsio.sysclient.jsz import AccountDetail, JSInfo, JszResponse, RaftGroupDetail, StreamDetail, StreamSourceInfo
from natsio.sysclient.options import (
    ConnState,
    ConnzOptions,
    EventFilterOptions,
    HealthzOptions,
    JszOptions,
    SortOpt,
    StatszOptions,
    SubszOptions,
    VarzOptions,
)
from natsio.sysclient.statsz import GatewayStat, RouteStat, ServerStats, StatszResponse
from natsio.sysclient.subsz import Subsz, SubszResponse
from natsio.sysclient.varz import (
    ClusterOptsVarz,
    DenyRules,
    GatewayOptsVarz,
    LeafNodeOptsVarz,
    MQTTOptsVarz,
    OCSPResponseCacheVarz,
    RemoteGatewayOptsVarz,
    RemoteLeafOptsVarz,
    Varz,
    VarzResponse,
    WebsocketOptsVarz,
)

__all__ = [
    "DEFAULT_REQUEST_TIMEOUT",
    "DEFAULT_STALL",
    "PING_TARGET",
    "SYS_REQ_SERVER_PREFIX",
    "APIError",
    "AccountDetail",
    "ClusterOptsVarz",
    "ConnInfo",
    "ConnState",
    "Connz",
    "ConnzOptions",
    "ConnzResponse",
    "DataStats",
    "DenyRules",
    "Endpoint",
    "EventFilterOptions",
    "GatewayOptsVarz",
    "GatewayStat",
    "Healthz",
    "HealthzError",
    "HealthzErrorType",
    "HealthzOptions",
    "HealthzResponse",
    "InvalidResponseError",
    "InvalidServerIDError",
    "JSInfo",
    "JetStreamAPIStats",
    "JetStreamConfig",
    "JetStreamStats",
    "JetStreamVarz",
    "JszOptions",
    "JszResponse",
    "LeafNodeOptsVarz",
    "MQTTOptsVarz",
    "MetaClusterInfo",
    "NoResponsesError",
    "OCSPResponseCacheVarz",
    "PagedResponses",
    "PagerStateError",
    "PeerInfo",
    "RaftGroupDetail",
    "RemoteGatewayOptsVarz",
    "RemoteLeafOptsVarz",
    "RouteStat",
    "ServerInfo",
    "ServerStats",
    "SlowConsumersStats",
    "SortOpt",
    "StatszOptions",
    "StatszResponse",
    "StreamDetail",
    "StreamSourceInfo",
    "SubDetail",
    "Subsz",
    "SubszOptions",
    "SubszResponse",
    "SysAPIError",
    "SysClient",
    "SysClientError",
    "SysClientOptions",
    "SysValidationError",
    "TLSPeerCert",
    "Varz",
    "VarzOptions",
    "VarzResponse",
    "WebsocketOptsVarz",
]
