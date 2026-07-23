"""`SUBSZ` response models — the sublist, offset/limit paged.

Field names pinned to orbit.go `natssysclient/subsz_server.go`. Go embeds
`*SublistStats` into `Subsz` without a JSON tag, so its keys
(`num_subscriptions`, `cache_hit_rate`, …) are inlined at the top level and are
spelled out here.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated

from natsio._internal.jsonmodel import RFC3339, JsonModel

from .entities import APIError, ServerInfo, SubDetail

__all__ = ["Subsz", "SubszResponse"]


@dataclass(slots=True, kw_only=True)
class Subsz(JsonModel):
    """One page of subscriptions plus the sublist statistics.

    `total` is the full result-set size; `subscriptions_list` is populated only
    when the request set `subscriptions`. `num_subscriptions` counts the whole
    sublist and is unaffected by paging.
    """

    server_id: str = ""
    now: Annotated[datetime | None, RFC3339] = None
    num_subscriptions: int = 0
    num_cache: int = 0
    num_inserts: int = 0
    num_removes: int = 0
    num_matches: int = 0
    cache_hit_rate: float = 0.0
    max_fanout: int = 0
    avg_fanout: float = 0.0
    total: int = 0
    offset: int = 0
    limit: int = 0
    subscriptions_list: list[SubDetail] | None = None


@dataclass(slots=True, kw_only=True)
class SubszResponse(JsonModel):
    """`{server, data}` envelope returned by `$SYS.REQ.SERVER.<id>.SUBSZ`."""

    server: ServerInfo = field(default_factory=ServerInfo)
    data: Subsz = field(default_factory=Subsz)
    error: APIError | None = None
