# TODO: remove `type` from arguments processing
# TODO: only process specified arguments
from dataclasses import dataclass
from typing import Mapping


@dataclass
class Limits:
    max_memory: int
    max_storage: int
    max_streams: int
    max_consumers: int
    max_bytes_required: bool | None = False
    max_ack_pending: int | None = None
    memory_max_stream_bytes: int | None = -1
    storage_max_stream_bytes: int | None = -1


@dataclass
class Tiers:
    memory: int
    storage: int
    streams: int
    consumers: int
    limits: Limits
    reserved_memory: int | None = None
    reserved_storage: int | None = None


@dataclass
class Api:
    total: int
    errors: int


@dataclass
class AccountInfo:
    memory: int
    storage: int
    streams: int
    consumers: int
    limits: Limits
    api: Api
    domain: str | None = None
    tiers: Mapping[str, Tiers] | None = None
