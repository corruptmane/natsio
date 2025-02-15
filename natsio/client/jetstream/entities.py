from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Annotated, Any, Mapping, Self, Type

from natsio._internal.serialization.converters import (
    DATETIME_ISO,
    TIMEDELTA_NANO,
)
from natsio._internal.serialization.serializator import (
    deserialize_dataclass,
    serialize_dataclass,
)
from natsio._internal.serialization.types import Converter


@dataclass(kw_only=True)
class Base:
    def to_dict(self) -> Mapping[str, Any]:
        return serialize_dataclass(self)

    @classmethod
    def from_response(cls: Type[Self], **data: Any) -> Self:
        return deserialize_dataclass(data, cls)


@dataclass(kw_only=True)
class _PagedResult(Base):
    total: int
    offset: int
    limit: int


@dataclass(kw_only=True)
class PubAck(Base):
    stream: str
    seq: int
    domain: str | None = None
    duplicate: bool | None = None


@dataclass(kw_only=True)
class Limits(Base):
    max_memory: int
    max_storage: int
    max_streams: int
    max_consumers: int
    max_bytes_required: bool | None = False
    max_ack_pending: int | None = None
    memory_max_stream_bytes: int | None = -1
    storage_max_stream_bytes: int | None = -1


@dataclass(kw_only=True)
class Tiers(Base):
    memory: int
    storage: int
    streams: int
    consumers: int
    limits: Limits
    reserved_memory: int | None = None
    reserved_storage: int | None = None


@dataclass(kw_only=True)
class Api(Base):
    total: int
    errors: int


@dataclass(kw_only=True)
class AccountInfo(Base):
    memory: int
    storage: int
    streams: int
    consumers: int
    limits: Limits
    api: Api
    domain: str | None = None
    tiers: Mapping[str, Tiers] | None = None


@dataclass(kw_only=True)
class SubjectTransform(Base):
    src: str
    dest: str


class Retention(str, Enum):
    limits = "limits"
    interest = "interest"
    workqueue = "workqueue"


class Storage(str, Enum):
    file = "file"
    memory = "memory"


class Compression(str, Enum):
    none = "none"
    s2 = "s2"


class Discard(str, Enum):
    old = "old"
    new = "new"


@dataclass(kw_only=True)
class Placement(Base):
    cluster: str | None = None
    tags: list[str] = field(default_factory=list)


@dataclass(kw_only=True)
class External(Base):
    api: str
    deliver: str | None = None


@dataclass(kw_only=True)
class MirrorConfig(Base):
    name: str
    opt_start_seq: int | None = None
    opt_start_time: str | None = None
    filter_subject: str | None = None
    subject_transforms: list[SubjectTransform] = field(default_factory=list)
    external: External | None = None


@dataclass(kw_only=True)
class SourceConfig(Base):
    name: str
    opt_start_seq: int | None = None
    opt_start_time: str | None = None
    filter_subject: str | None = None
    subject_transforms: list[SubjectTransform] = field(default_factory=list)
    external: External | None = None


@dataclass(kw_only=True)
class Republish(Base):
    src: str
    dest: str
    headers_only: bool | None = False


@dataclass(kw_only=True)
class ConsumerLimits(Base):
    inactive_threshold: Annotated[timedelta | None, TIMEDELTA_NANO] = None
    max_ack_pending: int | None = None


@dataclass(kw_only=True)
class StreamConfig(Base):
    retention: Retention
    max_consumers: int
    max_msgs: int
    max_bytes: int
    max_age: Annotated[timedelta, TIMEDELTA_NANO]
    storage: Storage
    num_replicas: int
    name: str | None = None
    description: str | None = None
    subjects: list[str] = field(default_factory=list)
    subject_transform: SubjectTransform | None = None
    max_msgs_per_subject: int | None = -1
    max_msg_size: int | None = -1
    compression: Compression | None = Compression.none
    first_seq: int | None = None
    no_ack: bool | None = False
    template_owner: str | None = None
    discard: Discard | None = Discard.old
    duplicate_window: Annotated[timedelta | None, TIMEDELTA_NANO] = timedelta(
        0
    )
    placement: Placement | None = None
    mirror: MirrorConfig | None = None
    sources: list[SourceConfig] = field(default_factory=list)
    sealed: bool | None = False
    deny_delete: bool | None = False
    deny_purge: bool | None = False
    allow_rollup_hdrs: bool | None = False
    allow_direct: bool | None = False
    mirror_direct: bool | None = False
    republish: Republish | None = None
    discard_new_per_subject: bool | None = False
    metadata: Mapping[str, str] | None = None
    consumer_limits: ConsumerLimits | None = None


@dataclass(kw_only=True)
class Lost(Base):
    msgs: list[int] = field(default_factory=list)
    bytes: int | None = None


@dataclass(kw_only=True)
class StreamState(Base):
    messages: int
    bytes: int
    first_seq: int
    last_seq: int
    consumer_count: int
    first_ts: Annotated[datetime | None, DATETIME_ISO] = None
    last_ts: Annotated[datetime | None, DATETIME_ISO] = None
    deleted: list[int] = field(default_factory=list)
    subjects: Mapping[str, int] | None = None
    num_subjects: int | None = None
    num_deleted: int | None = None
    lost: Lost | None = None


@dataclass(kw_only=True)
class Replica(Base):
    name: str
    current: bool
    active: Annotated[timedelta, TIMEDELTA_NANO]
    observer: bool | None = False
    offline: bool | None = False
    lag: int | None = None


@dataclass(kw_only=True)
class Cluster(Base):
    name: str | None = None
    leader: str | None = None
    replicas: list[Replica] = field(default_factory=list)
    raft_group: str | None = None


@dataclass(kw_only=True)
class Mirror(Base):
    name: str
    lag: int
    active: Annotated[timedelta, TIMEDELTA_NANO]
    filter_subject: str | None = None
    subject_transforms: list[SubjectTransform] = field(default_factory=list)
    external: External | None = None


@dataclass(kw_only=True)
class Source(Base):
    name: str
    lag: int
    active: Annotated[timedelta, TIMEDELTA_NANO]
    filter_subject: str | None = None
    subject_transforms: list[SubjectTransform] = field(default_factory=list)
    external: External | None = None


@dataclass(kw_only=True)
class Alternate(Base):
    name: str
    cluster: str
    domain: str | None = None


@dataclass(kw_only=True)
class StreamInfo(Base):
    config: StreamConfig
    state: StreamState
    created: str
    total: int | None = None
    offset: int | None = None
    limit: int | None = None
    ts: Annotated[datetime | None, DATETIME_ISO] = None
    cluster: Cluster | None = None
    mirror: Mirror | None = None
    sources: list[Source] = field(default_factory=list)
    alternates: list[Alternate] = field(default_factory=list)


@dataclass(kw_only=True)
class StreamList(_PagedResult):
    streams: list[StreamInfo]


@dataclass(kw_only=True)
class UpdateStreamRequest(Base):
    retention: Retention
    max_consumers: int
    max_msgs: int
    max_bytes: int
    max_age: Annotated[timedelta, TIMEDELTA_NANO]
    storage: Storage
    num_replicas: int
    name: str | None = None
    description: str | None = None
    subjects: list[str] = field(default_factory=list)
    subject_transform: SubjectTransform | None = None
    max_msgs_per_subject: int | None = -1
    max_msg_size: int | None = -1
    compression: Compression | None = Compression.none
    first_seq: int | None = None
    no_ack: bool | None = False
    template_owner: str | None = None
    discard: Discard | None = Discard.old
    duplicate_window: Annotated[timedelta | None, TIMEDELTA_NANO] = timedelta(
        0
    )
    placement: Placement | None = None
    mirror: Mirror | None = None
    sources: list[Source] = field(default_factory=list)
    sealed: bool | None = False
    deny_delete: bool | None = False
    deny_purge: bool | None = False
    allow_rollup_hdrs: bool | None = False
    allow_direct: bool | None = False
    mirror_direct: bool | None = False
    republish: Republish | None = None
    discard_new_per_subject: bool | None = False
    metadata: Mapping[str, str] | None = None
    consumer_limits: ConsumerLimits | None = None


@dataclass(kw_only=True)
class CreateStreamRequest(UpdateStreamRequest):
    pedantic: bool | None = False


@dataclass(kw_only=True)
class GetMsgRequest(Base):
    seq: int | None = None
    last_by_subj: str | None = None
    next_by_subj: str | None = None

    def validate(self) -> None:
        if self.seq and self.last_by_subj:
            raise ValueError(
                "`seq` and `last_by_subj` properties can not be combined"
            )
        if self.seq is None and self.last_by_subj is None:
            raise ValueError(
                "One of `seq` or `last_by_subj` must be specified"
            )
        if self.seq is None and self.next_by_subj is not None:
            raise ValueError(
                "`seq` must be provided when using `next_by_subj`"
            )


@dataclass(kw_only=True)
class RawMsg:
    subject: str
    seq: int
    time: Annotated[datetime, DATETIME_ISO]
    payload: bytes | None
    headers: Mapping[str, str] | None


class AckPolicy(str, Enum):
    none = "none"
    all = "all"
    explicit = "explicit"


class ReplayPolicy(str, Enum):
    instant = "instant"
    original = "original"


class DeliverPolicy(str, Enum):
    all = "all"
    last = "last"
    new = "new"
    by_start_sequence = "by_start_sequence"
    by_start_time = "by_start_time"
    last_per_subject = "last_per_subject"


@dataclass(kw_only=True)
class _ConsumerConfig(Base):
    name: str | None = None
    durable_name: str | None = None
    description: str | None = None
    deliver_policy: DeliverPolicy = DeliverPolicy.all
    opt_start_time: int | None = None
    opt_start_seq: int | None = None
    ack_policy: AckPolicy = AckPolicy.explicit
    ack_wait: Annotated[timedelta | None, TIMEDELTA_NANO] = None
    max_deliver: int | None = None
    backoff: list[Annotated[timedelta, TIMEDELTA_NANO]] = field(
        default_factory=list
    )
    filter_subject: str | None = None
    filter_subjects: list[str] = field(default_factory=list)
    replay_policy: ReplayPolicy = ReplayPolicy.instant
    sample_freq: str | None = None
    max_ack_pending: int | None = None
    headers_only: bool | None = None
    inactive_threshold: Annotated[timedelta | None, TIMEDELTA_NANO] = None
    num_replicas: int | None = None
    mem_storage: bool | None = None
    metadata: Mapping[str, str] | None = None
    pause_until: str | None = None


@dataclass(kw_only=True)
class PushConsumerConfig(_ConsumerConfig):
    deliver_subject: str
    deliver_group: str | None = None
    flow_control: bool | None = None
    idle_heartbeat: Annotated[timedelta | None, TIMEDELTA_NANO] = None
    rate_limit_bps: int | None = None


@dataclass(kw_only=True)
class PullConsumerConfig(_ConsumerConfig):
    max_batch: int | None = None
    max_expires: Annotated[timedelta | None, TIMEDELTA_NANO] = None
    max_bytes: int | None = None
    max_waiting: int | None = None


class ConsumerConfigConverter(
    Converter[PushConsumerConfig | PullConsumerConfig, Mapping[str, Any]]
):
    def is_push_consumer(self, data: Mapping[str, Any]) -> bool:
        return any(
            prop in data and data[prop] is not None
            for prop in (
                "deliver_subject",
                "deliver_group",
                "flow_control",
                "idle_heartbeat",
                "rate_limit_bps",
            )
        )

    def to_wire(
        self, value: PushConsumerConfig | PullConsumerConfig
    ) -> Mapping[str, Any]:
        return serialize_dataclass(value)

    def from_wire(
        self, value: Mapping[str, Any]
    ) -> PushConsumerConfig | PullConsumerConfig:
        config_type = (
            PushConsumerConfig
            if self.is_push_consumer(value)
            else PullConsumerConfig
        )
        return deserialize_dataclass(value, config_type)


@dataclass(kw_only=True)
class SequenceInfo(Base):
    consumer_seq: int
    stream_seq: int
    last_active: Annotated[datetime | None, DATETIME_ISO] = None


@dataclass(kw_only=True)
class ConsumerInfo(Base):
    stream_name: str
    name: str
    config: Annotated[
        PushConsumerConfig | PullConsumerConfig, ConsumerConfigConverter()
    ]
    created: Annotated[datetime, DATETIME_ISO]
    delivered: SequenceInfo
    ack_floor: SequenceInfo
    num_ack_pending: int
    num_redelivered: int
    num_waiting: int
    num_pending: int
    ts: Annotated[datetime | None, DATETIME_ISO] = None
    cluster: Cluster | None = None
    push_bound: bool | None = None
    paused: bool | None = None
    pause_until: str | None = None


@dataclass(kw_only=True)
class ConsumerList(_PagedResult):
    consumers: list[ConsumerInfo]


@dataclass(kw_only=True)
class KeyValueConfig(Base):
    bucket_name: str
    description: str | None = None
    history: int = 1
    max_value_size: int = -1
    ttl_seconds: int = 120
    max_bytes: int = -1
    storage: Storage = Storage.file
    num_replicas: int = 1
    placement: Placement | None = None
    republish: Republish | None = None
    allow_direct: bool | None = None
