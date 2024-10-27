from dataclasses import Field, asdict, dataclass, fields, is_dataclass
from enum import Enum
from functools import cached_property
from typing import Any, ClassVar, Mapping, Protocol, Self, Type, TypeVar

from natsio.utils.time import from_nanoseconds


class DataclassInstance(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Field[Any]]]


T = TypeVar("T", bound=DataclassInstance)


def _map_to_dataclass(data: Mapping[str, Any], cls: Type[T]) -> T:
    field_names = {field.name: field.type for field in fields(cls)}
    filtered_data = {}
    
    for key, value in data.items():
        if key in field_names:
            field_type = field_names[key]
            if is_dataclass(field_type):
                filtered_data[key] = _map_to_dataclass(value, field_type)  # type: ignore[arg-type]
            else:
                filtered_data[key] = value
    
    return cls(**filtered_data)


@dataclass
class Base:
    @classmethod
    def from_response(cls: Type[Self], **data: Any) -> Self:
        return _map_to_dataclass(data, cls)

    def to_dict(self) -> Mapping[str, Any]:
        return asdict(self)


@dataclass
class Limits(Base):
    max_memory: int
    max_storage: int
    max_streams: int
    max_consumers: int
    max_bytes_required: bool | None = False
    max_ack_pending: int | None = None
    memory_max_stream_bytes: int | None = -1
    storage_max_stream_bytes: int | None = -1


@dataclass
class Tiers(Base):
    memory: int
    storage: int
    streams: int
    consumers: int
    limits: Limits
    reserved_memory: int | None = None
    reserved_storage: int | None = None


@dataclass
class Api(Base):
    total: int
    errors: int


@dataclass
class AccountInfo(Base):
    memory: int
    storage: int
    streams: int
    consumers: int
    limits: Limits
    api: Api
    domain: str | None = None
    tiers: Mapping[str, Tiers] | None = None


@dataclass
class SubjectTransform(Base):
    src: str
    dest: str


class Retention(str, Enum):
    limits = 'limits'
    interest = 'interest'
    workqueue = 'workqueue'


class Storage(str, Enum):
    file = 'file'
    memory = 'memory'


class Compression(str, Enum):
    none = 'none'
    s2 = 's2'


class Discard(str, Enum):
    old = 'old'
    new = 'new'


@dataclass
class Placement(Base):
    cluster: str | None = None
    tags: list[str] | None = None


@dataclass
class External(Base):
    api: str
    deliver: str | None = None


@dataclass
class MirrorConfig(Base):
    name: str
    opt_start_seq: int | None = None
    opt_start_time: str | None = None
    filter_subject: str | None = None
    subject_transforms: list[SubjectTransform] | None = None
    external: External | None = None


@dataclass
class SourceConfig(Base):
    name: str
    opt_start_seq: int | None = None
    opt_start_time: str | None = None
    filter_subject: str | None = None
    subject_transforms: list[SubjectTransform] | None = None
    external: External | None = None


@dataclass
class Republish(Base):
    src: str
    dest: str
    headers_only: bool | None = False


@dataclass
class ConsumerLimits(Base):
    inactive_threshold: int | None = None
    max_ack_pending: int | None = None

    @cached_property
    def inactive_threshold_seconds(self) -> float | None:
        if self.inactive_threshold is None:
            return self.inactive_threshold
        return from_nanoseconds(self.inactive_threshold)


@dataclass
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
            return self.duplicate_window
        return from_nanoseconds(self.duplicate_window)


@dataclass
class Lost(Base):
    msgs: list[int] | None = None
    bytes: int | None = None


@dataclass
class State(Base):
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


@dataclass
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


@dataclass
class Cluster(Base):
    name: str | None = None
    leader: str | None = None
    replicas: list[Replica] | None = None
    raft_group: str | None = None


@dataclass
class Error(Base):
    code: int
    description: str | None = None
    err_code: int | None = None


@dataclass
class Mirror(Base):
    name: str
    lag: int
    active: int
    filter_subject: str | None = None
    subject_transforms: list[SubjectTransform] | None = None
    external: External | None = None
    error: Error | None = None

    @cached_property
    def active_seconds(self) -> float:
        return from_nanoseconds(self.active)


@dataclass
class Source(Base):
    name: str
    lag: int
    active: int
    filter_subject: str | None = None
    subject_transforms: list[SubjectTransform] | None = None
    external: External | None = None
    error: Error | None = None

    @cached_property
    def active_seconds(self) -> float:
        return from_nanoseconds(self.active)


@dataclass
class Alternate(Base):
    name: str
    cluster: str
    domain: str | None = None


@dataclass
class StreamInfo(Base):
    config: StreamConfig
    state: State
    created: str
    total: int | None = None
    offset: int | None = None
    limit: int | None = None
    ts: str | None = None
    cluster: Cluster | None = None
    mirror: Mirror | None = None
    sources: list[Source] | None = None
    alternates: list[Alternate] | None = None


@dataclass
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


@dataclass
class CreateStreamRequest(UpdateStreamRequest):
    pedantic: bool | None = False
