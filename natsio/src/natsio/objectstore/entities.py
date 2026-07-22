"""Object Store data types: config, metadata, status (ADR-20)."""

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Annotated, Final

from natsio._internal.jsonmodel import RFC3339, JsonModel
from natsio.errors import ConfigError
from natsio.jetstream.entities import Placement, StorageType, StreamInfo

from .errors import InvalidBucketNameError, InvalidObjectNameError

__all__ = [
    "DEFAULT_CHUNK_SIZE",
    "ObjectInfo",
    "ObjectLink",
    "ObjectMeta",
    "ObjectMetaOptions",
    "ObjectStoreConfig",
    "ObjectStoreStatus",
    "validate_bucket_name",
    "validate_object_name",
]

DEFAULT_CHUNK_SIZE: Final = 128 * 1024  # ADR-20's canonical chunk size

_BUCKET_RE: Final = re.compile(r"\A[a-zA-Z0-9_-]+\Z")


def validate_bucket_name(bucket: str) -> None:
    if not _BUCKET_RE.match(bucket):
        raise InvalidBucketNameError(f"invalid bucket name {bucket!r}: allowed characters are A-Z a-z 0-9 _ -")


def validate_object_name(name: str) -> None:
    """Object names may be any non-empty string — they travel base64-encoded
    inside the metadata subject, never as raw subject tokens."""
    if not name:
        raise InvalidObjectNameError("object name must not be empty")


@dataclass(frozen=True, slots=True, kw_only=True)
class ObjectStoreConfig:
    """Bucket configuration (maps onto an ADR-20 ``OBJ_<bucket>`` stream)."""

    bucket: str
    description: str | None = None
    # 0 / None means objects never expire.
    ttl: timedelta | None = None
    max_bytes: int = -1
    storage: StorageType = StorageType.FILE
    replicas: int = 1
    placement: Placement | None = None
    compression: bool = False
    metadata: dict[str, str] | None = None

    def __post_init__(self) -> None:
        validate_bucket_name(self.bucket)
        if self.ttl is not None and self.ttl != timedelta(0) and self.ttl < timedelta(milliseconds=100):
            raise ConfigError("ttl must be at least 100ms (server-enforced max_age floor) or 0/None for never")


@dataclass(frozen=True, slots=True, kw_only=True)
class ObjectMeta:
    """What the caller provides when storing an object."""

    name: str
    description: str | None = None
    metadata: dict[str, str] | None = None
    # ADR-20 object-level headers (multi-valued, like message headers).
    headers: dict[str, list[str]] | None = None
    # None means DEFAULT_CHUNK_SIZE (128KiB).
    chunk_size: int | None = None

    def __post_init__(self) -> None:
        validate_object_name(self.name)
        if self.chunk_size is not None and self.chunk_size <= 0:
            raise ConfigError("chunk_size must be positive")


@dataclass(slots=True, kw_only=True)
class ObjectLink(JsonModel):
    """A pointer at another object (or a whole bucket, when ``name`` is empty)."""

    bucket: str = ""
    name: str | None = None


@dataclass(slots=True, kw_only=True)
class ObjectMetaOptions(JsonModel):
    link: ObjectLink | None = None
    max_chunk_size: int | None = None


@dataclass(slots=True, kw_only=True)
class ObjectInfo(JsonModel):
    """An object's stored metadata (the payload of its rollup meta message).

    ``mtime`` is stamped from the meta message's stream timestamp on read —
    authoritative over anything found in the JSON.
    """

    name: str = ""
    description: str | None = None
    metadata: dict[str, str] | None = None
    headers: dict[str, list[str]] | None = None
    options: ObjectMetaOptions | None = None
    bucket: str = ""
    nuid: str = ""
    size: int = 0
    chunks: int = 0
    digest: str | None = None
    deleted: bool | None = None
    mtime: Annotated[datetime | None, RFC3339] = None

    @property
    def is_deleted(self) -> bool:
        return bool(self.deleted)

    @property
    def is_link(self) -> bool:
        return self.options is not None and self.options.link is not None


@dataclass(frozen=True, slots=True)
class ObjectStoreStatus:
    """A point-in-time view of a bucket, derived from its stream info."""

    bucket: str
    description: str | None
    ttl: timedelta | None
    storage: StorageType
    replicas: int
    sealed: bool
    size: int
    """Total stored bytes (chunks + metadata), from the stream state."""
    metadata: dict[str, str] | None
    stream_info: StreamInfo = field(repr=False)

    @property
    def is_compressed(self) -> bool:
        compression = self.stream_info.config.compression
        return bool(compression and compression.value != "none")
