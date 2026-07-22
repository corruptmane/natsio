"""JetStream API entities (ADR-1 JSON rules, 2.14-era field coverage).

Every model is a slotted dataclass over `JsonModel`:
durations are ``timedelta`` (nanoseconds on the wire), timestamps are aware
``datetime`` (RFC 3339 on the wire), and unknown server fields round-trip via
``extra`` — a config read from a newer server and written back loses nothing.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Annotated

from natsio._internal.jsonmodel import NS_DURATION, RFC3339, JsonModel

__all__ = [
    "APIStats",
    "AccountInfo",
    "AccountLimits",
    "AckPolicy",
    "ClusterInfo",
    "ConsumerConfig",
    "ConsumerInfo",
    "ConsumerLimits",
    "DeliverPolicy",
    "DiscardPolicy",
    "External",
    "PeerInfo",
    "Placement",
    "PriorityPolicy",
    "PubAck",
    "ReplayPolicy",
    "Republish",
    "RetentionPolicy",
    "SequenceInfo",
    "StorageCompression",
    "StorageType",
    "StreamConfig",
    "StreamInfo",
    "StreamSource",
    "StreamState",
    "SubjectTransform",
]


class RetentionPolicy(StrEnum):
    LIMITS = "limits"
    INTEREST = "interest"
    WORK_QUEUE = "workqueue"


class StorageType(StrEnum):
    FILE = "file"
    MEMORY = "memory"


class DiscardPolicy(StrEnum):
    OLD = "old"
    NEW = "new"


class AckPolicy(StrEnum):
    NONE = "none"
    ALL = "all"
    EXPLICIT = "explicit"


class DeliverPolicy(StrEnum):
    ALL = "all"
    LAST = "last"
    NEW = "new"
    BY_START_SEQUENCE = "by_start_sequence"
    BY_START_TIME = "by_start_time"
    LAST_PER_SUBJECT = "last_per_subject"


class ReplayPolicy(StrEnum):
    INSTANT = "instant"
    ORIGINAL = "original"


class PriorityPolicy(StrEnum):
    NONE = "none"
    OVERFLOW = "overflow"
    PINNED_CLIENT = "pinned_client"
    PRIORITIZED = "prioritized"


class StorageCompression(StrEnum):
    NONE = "none"
    S2 = "s2"


@dataclass(slots=True, kw_only=True)
class Placement(JsonModel):
    cluster: str | None = None
    tags: list[str] | None = None


@dataclass(slots=True, kw_only=True)
class External(JsonModel):
    api: str = ""
    deliver: str | None = None


@dataclass(slots=True, kw_only=True)
class SubjectTransform(JsonModel):
    src: str = ""
    dest: str = ""


@dataclass(slots=True, kw_only=True)
class StreamSource(JsonModel):
    name: str = ""
    opt_start_seq: int | None = None
    opt_start_time: Annotated[datetime | None, RFC3339] = None
    filter_subject: str | None = None
    subject_transforms: list[SubjectTransform] | None = None
    external: External | None = None


@dataclass(slots=True, kw_only=True)
class Republish(JsonModel):
    src: str = ""
    dest: str = ""
    headers_only: bool | None = None


@dataclass(slots=True, kw_only=True)
class ConsumerLimits(JsonModel):
    inactive_threshold: Annotated[timedelta | None, NS_DURATION] = None
    max_ack_pending: int | None = None


@dataclass(slots=True, kw_only=True)
class StreamConfig(JsonModel):
    name: str = ""
    description: str | None = None
    subjects: list[str] | None = None
    retention: RetentionPolicy = RetentionPolicy.LIMITS
    max_consumers: int = -1
    max_msgs: int = -1
    max_bytes: int = -1
    max_age: Annotated[timedelta | None, NS_DURATION] = None
    max_msgs_per_subject: int = -1
    max_msg_size: int = -1
    discard: DiscardPolicy = DiscardPolicy.OLD
    discard_new_per_subject: bool | None = None
    storage: StorageType = StorageType.FILE
    num_replicas: int = 1
    no_ack: bool | None = None
    duplicate_window: Annotated[timedelta | None, NS_DURATION] = None
    placement: Placement | None = None
    mirror: StreamSource | None = None
    sources: list[StreamSource] | None = None
    sealed: bool | None = None
    deny_delete: bool | None = None
    deny_purge: bool | None = None
    allow_rollup_hdrs: bool | None = None
    compression: StorageCompression | None = None
    first_seq: int | None = None
    subject_transform: SubjectTransform | None = None
    republish: Republish | None = None
    allow_direct: bool | None = None
    mirror_direct: bool | None = None
    consumer_limits: ConsumerLimits | None = None
    metadata: dict[str, str] | None = None
    # 2.11+ (per-message TTL, ADR-43)
    allow_msg_ttl: bool | None = None
    subject_delete_marker_ttl: Annotated[timedelta | None, NS_DURATION] = None
    # 2.12+ (atomic batch publish, counters)
    allow_atomic: bool | None = None
    allow_msg_counter: bool | None = None
    # 2.12+ (message schedules)
    allow_msg_schedules: bool | None = None


@dataclass(slots=True, kw_only=True)
class StreamState(JsonModel):
    messages: int = 0
    bytes: int = 0
    first_seq: int = 0
    first_ts: Annotated[datetime | None, RFC3339] = None
    last_seq: int = 0
    last_ts: Annotated[datetime | None, RFC3339] = None
    consumer_count: int = 0
    deleted: list[int] | None = None
    num_deleted: int | None = None
    num_subjects: int | None = None
    subjects: dict[str, int] | None = None


@dataclass(slots=True, kw_only=True)
class PeerInfo(JsonModel):
    name: str = ""
    current: bool = False
    offline: bool | None = None
    active: Annotated[timedelta | None, NS_DURATION] = None
    lag: int | None = None


@dataclass(slots=True, kw_only=True)
class ClusterInfo(JsonModel):
    name: str | None = None
    leader: str | None = None
    replicas: list[PeerInfo] | None = None


@dataclass(slots=True, kw_only=True)
class StreamInfo(JsonModel):
    config: StreamConfig = field(default_factory=StreamConfig)
    state: StreamState = field(default_factory=StreamState)
    created: Annotated[datetime | None, RFC3339] = None
    ts: Annotated[datetime | None, RFC3339] = None
    cluster: ClusterInfo | None = None


@dataclass(slots=True, kw_only=True)
class ConsumerConfig(JsonModel):
    """Pull-consumer configuration (ADR-37: pull is the only delivery model).

    Push-only fields a foreign consumer might carry survive round-trips via
    ``extra`` without being modeled here.
    """

    name: str | None = None
    durable_name: str | None = None
    description: str | None = None
    deliver_policy: DeliverPolicy = DeliverPolicy.ALL
    opt_start_seq: int | None = None
    opt_start_time: Annotated[datetime | None, RFC3339] = None
    ack_policy: AckPolicy = AckPolicy.EXPLICIT
    ack_wait: Annotated[timedelta | None, NS_DURATION] = None
    max_deliver: int | None = None
    backoff: list[Annotated[timedelta, NS_DURATION]] | None = None
    filter_subject: str | None = None
    filter_subjects: list[str] | None = None
    replay_policy: ReplayPolicy = ReplayPolicy.INSTANT
    sample_freq: str | None = None
    max_waiting: int | None = None
    max_ack_pending: int | None = None
    headers_only: bool | None = None
    max_batch: int | None = None
    max_expires: Annotated[timedelta | None, NS_DURATION] = None
    max_bytes: int | None = None
    inactive_threshold: Annotated[timedelta | None, NS_DURATION] = None
    num_replicas: int | None = None
    mem_storage: bool | None = None
    metadata: dict[str, str] | None = None
    # priority groups (ADR-42, 2.11+)
    priority_groups: list[str] | None = None
    priority_policy: PriorityPolicy | None = None
    priority_timeout: Annotated[timedelta | None, NS_DURATION] = None
    # consumer pause (2.11+)
    pause_until: Annotated[datetime | None, RFC3339] = None


@dataclass(slots=True, kw_only=True)
class SequenceInfo(JsonModel):
    consumer_seq: int = 0
    stream_seq: int = 0
    last_active: Annotated[datetime | None, RFC3339] = None


@dataclass(slots=True, kw_only=True)
class ConsumerInfo(JsonModel):
    stream_name: str = ""
    name: str = ""
    created: Annotated[datetime | None, RFC3339] = None
    config: ConsumerConfig = field(default_factory=ConsumerConfig)
    delivered: SequenceInfo = field(default_factory=SequenceInfo)
    ack_floor: SequenceInfo = field(default_factory=SequenceInfo)
    num_ack_pending: int = 0
    num_redelivered: int = 0
    num_waiting: int = 0
    num_pending: int = 0
    ts: Annotated[datetime | None, RFC3339] = None
    cluster: ClusterInfo | None = None
    paused: bool | None = None
    pause_remaining: Annotated[timedelta | None, NS_DURATION] = None


@dataclass(slots=True, kw_only=True)
class PubAck(JsonModel):
    stream: str = ""
    seq: int = 0
    domain: str | None = None
    duplicate: bool | None = None
    # 2.12+ counters / atomic batch
    val: str | None = None
    batch_id: str | None = None
    batch_size: int | None = None


@dataclass(slots=True, kw_only=True)
class APIStats(JsonModel):
    total: int = 0
    errors: int = 0
    level: int | None = None


@dataclass(slots=True, kw_only=True)
class AccountLimits(JsonModel):
    max_memory: int = 0
    max_storage: int = 0
    max_streams: int = 0
    max_consumers: int = 0


@dataclass(slots=True, kw_only=True)
class AccountInfo(JsonModel):
    memory: int = 0
    storage: int = 0
    streams: int = 0
    consumers: int = 0
    domain: str | None = None
    api: APIStats = field(default_factory=APIStats)
    limits: AccountLimits = field(default_factory=AccountLimits)
