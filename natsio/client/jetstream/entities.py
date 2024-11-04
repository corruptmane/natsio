from dataclasses import Field, asdict, dataclass, fields, is_dataclass
from datetime import datetime
from enum import Enum
from functools import cached_property
from types import UnionType
from typing import (
    Any,
    ClassVar,
    Mapping,
    Protocol,
    Self,
    Type,
    TypeVar,
    cast,
    get_args,
    get_origin,
)
from base64 import b64decode

from natsio.protocol.parser import parse_headers
from natsio.utils.time import from_nanoseconds, fromisoformat


class DataclassInstance(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Field[Any]]]


T = TypeVar("T", bound=DataclassInstance)


def _map_to_dataclass(data: Mapping[str, Any], cls: Type[T]) -> T:
    field_names = {field.name: field.type for field in fields(cls)}
    filtered_data = {}

    for key, value in data.items():
        if key in field_names:
            field_type = cast(type, field_names[key])
            origin = get_origin(field_type)
            args = get_args(field_type)
            if len(args) >= 1:
                is_optional_list_of_dataclasses = (
                    origin in (UnionType,)
                    and get_origin(args[0]) is list
                    and is_dataclass(get_args(args[0])[0])
                )
            else:
                is_optional_list_of_dataclasses = False

            if is_dataclass(field_type):
                filtered_data[key] = _map_to_dataclass(value, field_type)
            elif is_optional_list_of_dataclasses:
                new_origin = get_args(args[0])[0]  # pyright: ignore[reportGeneralTypeIssues]
                filtered_data[key] = [_map_to_dataclass(val, new_origin) for val in value]  # type: ignore[assignment]
            elif origin in (list,) and is_dataclass(args[0]):  # pyright: ignore[reportGeneralTypeIssues]
                filtered_data[key] = [_map_to_dataclass(val, args[0]) for val in value]  # type: ignore[assignment,arg-type]
            elif isinstance(field_type, type) and issubclass(field_type, Enum):
                filtered_data[key] = field_type(value)  # type: ignore[assignment]
            elif (
                origin in (UnionType,) and isinstance(args[0], type) and issubclass(args[0], Enum)  # pyright: ignore[reportGeneralTypeIssues]
            ):
                filtered_data[key] = args[0](value)  # type: ignore[assignment]
            else:
                filtered_data[key] = value

    return cls(**filtered_data)


@dataclass(kw_only=True)
class Base:
    @classmethod
    def from_response(cls: Type[Self], **data: Any) -> Self:
        return _map_to_dataclass(data, cls)

    def to_dict(self) -> Mapping[str, Any]:
        return asdict(self)


@dataclass(kw_only=True)
class _PagedResult(Base):
    total: int
    offset: int
    limit: int


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
    tags: list[str] | None = None


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
    subject_transforms: list[SubjectTransform] | None = None
    external: External | None = None


@dataclass(kw_only=True)
class SourceConfig(Base):
    name: str
    opt_start_seq: int | None = None
    opt_start_time: str | None = None
    filter_subject: str | None = None
    subject_transforms: list[SubjectTransform] | None = None
    external: External | None = None


@dataclass(kw_only=True)
class Republish(Base):
    src: str
    dest: str
    headers_only: bool | None = False


@dataclass(kw_only=True)
class ConsumerLimits(Base):
    inactive_threshold: int | None = None
    max_ack_pending: int | None = None

    @cached_property
    def inactive_threshold_seconds(self) -> float | None:
        if self.inactive_threshold is None:
            return None
        return from_nanoseconds(self.inactive_threshold)


@dataclass(kw_only=True)
class StreamConfig(Base):
    retention: Retention
    max_consumers: int
    max_msgs: int
    max_bytes: int
    max_age: int
    storage: Storage
    num_replicas: int
    name: str | None = None
    description: str | None = None
    subjects: list[str] | None = None
    subject_transform: SubjectTransform | None = None
    max_msgs_per_subject: int | None = -1
    max_msg_size: int | None = -1
    compression: Compression | None = Compression.none
    first_seq: int | None = None
    no_ack: bool | None = False
    template_owner: str | None = None
    discard: Discard | None = Discard.old
    duplicate_window: int | None = 0
    placement: Placement | None = None
    mirror: MirrorConfig | None = None
    sources: list[SourceConfig] | None = None
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

    @cached_property
    def max_age_seconds(self) -> float:
        return from_nanoseconds(self.max_age)

    @cached_property
    def duplicate_window_seconds(self) -> float | None:
        if self.duplicate_window is None:
            return None
        return from_nanoseconds(self.duplicate_window)


@dataclass(kw_only=True)
class Lost(Base):
    msgs: list[int] | None = None
    bytes: int | None = None


@dataclass(kw_only=True)
class StreamState(Base):
    messages: int
    bytes: int
    first_seq: int
    last_seq: int
    consumer_count: int
    first_ts: str | None = None
    last_ts: str | None = None
    deleted: list[int] | None = None
    subjects: Mapping[str, int] | None = None
    num_subjects: int | None = None
    num_deleted: int | None = None
    lost: Lost | None = None

    @cached_property
    def first_ts_dt(self) -> datetime | None:
        if self.first_ts is None:
            return None
        return fromisoformat(self.first_ts)

    @cached_property
    def last_ts_dt(self) -> datetime | None:
        if self.last_ts is None:
            return None
        return fromisoformat(self.last_ts)


@dataclass(kw_only=True)
class Replica(Base):
    name: str
    current: bool
    active: int
    observer: bool | None = False
    offline: bool | None = False
    lag: int | None = None

    @cached_property
    def active_seconds(self) -> float:
        return from_nanoseconds(self.active)


@dataclass(kw_only=True)
class Cluster(Base):
    name: str | None = None
    leader: str | None = None
    replicas: list[Replica] | None = None
    raft_group: str | None = None


@dataclass(kw_only=True)
class Mirror(Base):
    name: str
    lag: int
    active: int
    filter_subject: str | None = None
    subject_transforms: list[SubjectTransform] | None = None
    external: External | None = None

    @cached_property
    def active_seconds(self) -> float:
        return from_nanoseconds(self.active)


@dataclass(kw_only=True)
class Source(Base):
    name: str
    lag: int
    active: int
    filter_subject: str | None = None
    subject_transforms: list[SubjectTransform] | None = None
    external: External | None = None

    @cached_property
    def active_seconds(self) -> float:
        return from_nanoseconds(self.active)


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
    ts: str | None = None
    cluster: Cluster | None = None
    mirror: Mirror | None = None
    sources: list[Source] | None = None
    alternates: list[Alternate] | None = None

    @cached_property
    def ts_dt(self) -> datetime | None:
        if self.ts is None:
            return None
        return fromisoformat(self.ts)


@dataclass(kw_only=True)
class StreamList(_PagedResult):
    streams: list[StreamInfo]


@dataclass(kw_only=True)
class UpdateStreamRequest(Base):
    retention: Retention
    max_consumers: int
    max_msgs: int
    max_bytes: int
    max_age: int
    storage: Storage
    num_replicas: int
    name: str | None = None
    description: str | None = None
    subjects: list[str] | None = None
    subject_transform: SubjectTransform | None = None
    max_msgs_per_subject: int | None = -1
    max_msg_size: int | None = -1
    compression: Compression | None = Compression.none
    first_seq: int | None = None
    no_ack: bool | None = False
    template_owner: str | None = None
    discard: Discard | None = Discard.old
    duplicate_window: int | None = 0
    placement: Placement | None = None
    mirror: Mirror | None = None
    sources: list[Source] | None = None
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
    batch: int | None = None
    max_bytes: int | None = None
    start_time: datetime | str | None = None
    multi_last: list[str] | None = None
    up_to_seq: int | None = None
    up_to_time: datetime | str | None = None

    def render_start_time(self) -> str | None:
        if self.start_time is None:
            return None
        if isinstance(self.start_time, datetime):
            return self.start_time.isoformat(sep="T", timespec="seconds")
        return self.start_time

    def render_up_to_time(self) -> str | None:
        if self.up_to_time is None:
            return None
        if isinstance(self.up_to_time, datetime):
            return self.up_to_time.isoformat(sep="T", timespec="seconds")
        return self.up_to_time


@dataclass(kw_only=True)
class RawMsg(Base):
    subject: str
    seq: int
    time: str
    data: str | None = None  # b64 encoded payload, use `payload` instead
    hdrs: str | None = None  # b64 encoded headers, use `headers` instead

    @cached_property
    def payload(self) -> bytes | None:
        if not self.data:
            return None
        return b64decode(self.data)

    @cached_property
    def headers(self) -> Mapping[str, str] | None:
        if not self.hdrs:
            return None
        decoded_headers = b64decode(self.hdrs)
        return parse_headers(decoded_headers)


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
    ack_wait: int | None = None
    max_deliver: int | None = None
    backoff: list[int] | None = None
    filter_subject: str | None = None
    filter_subjects: list[str] | None = None
    replay_policy: ReplayPolicy = ReplayPolicy.instant
    sample_freq: str | None = None
    max_ack_pending: int | None = None
    headers_only: bool | None = None
    inactive_threshold: int | None = None
    num_replicas: int | None = None
    mem_storage: bool | None = None
    metadata: Mapping[str, str] | None = None
    pause_until: str | None = None


@dataclass(kw_only=True)
class PushConsumerConfig(_ConsumerConfig):
    deliver_subject: str
    deliver_group: str | None = None
    flow_control: bool | None = None
    idle_heartbeat: int | None = None
    rate_limit_bps: int | None = None


@dataclass(kw_only=True)
class PullConsumerConfig(_ConsumerConfig):
    max_batch: int | None = None
    max_expires: int | None = None
    max_bytes: int | None = None
    max_waiting: int | None = None


@dataclass(kw_only=True)
class SequenceInfo(Base):
    consumer_seq: int
    stream_seq: int
    last_active: str | None = None

    @cached_property
    def last_active_dt(self) -> datetime | None:
        if self.last_active is None:
            return None
        return fromisoformat(self.last_active)


@dataclass(kw_only=True)
class ConsumerInfo(Base):
    stream_name: str
    name: str
    config: PushConsumerConfig | PullConsumerConfig
    created: str
    delivered: SequenceInfo
    ack_floor: SequenceInfo
    num_ack_pending: int
    num_redelivered: int
    num_waiting: int
    num_pending: int
    ts: str | None = None
    cluster: Cluster | None = None
    push_bound: bool | None = None
    paused: bool | None = None
    pause_until: str | None = None

    @cached_property
    def created_dt(self) -> datetime:
        return fromisoformat(self.created)

    @cached_property
    def ts_dt(self) -> datetime | None:
        if self.ts is None:
            return None
        return fromisoformat(self.ts)

    @classmethod
    def from_response(cls: Type[Self], **data: Any) -> Self:
        # NOTE: is a subject to change, need more testing
        config = data["config"]
        is_push_consumer = any(
            prop in config and config[prop] is not None
            for prop in (
                "deliver_subject",
                "deliver_group",
                "flow_control",
                "idle_heartbeat",
                "rate_limit_bps",
            )
        )
        tp: type[_ConsumerConfig]
        if is_push_consumer:
            tp = PushConsumerConfig
        else:
            tp = PullConsumerConfig
        data["config"] = tp.from_response(**data["config"])

        return _map_to_dataclass(data, cls)


@dataclass(kw_only=True)
class ConsumerList(_PagedResult):
    consumers: list[ConsumerInfo]

    @classmethod
    def from_response(cls: Type[Self], **data: Any) -> Self:
        paged = _PagedResult.from_response(**data)
        consumers = [ConsumerInfo.from_response(**consumer) for consumer in data["consumers"]]
        return cls(total=paged.total, offset=paged.offset, limit=paged.limit, consumers=consumers)
