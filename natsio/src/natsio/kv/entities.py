"""Key-Value data types: config, status, entries, and the codec seam."""

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Final, NoReturn, Protocol

from natsio.jetstream.entities import Placement, Republish, StorageType, StreamInfo

from .errors import InvalidBucketNameError, InvalidKeyError

__all__ = [
    "KeyCodec",
    "KeyValueConfig",
    "KeyValueStatus",
    "KvEntry",
    "Operation",
    "ValueCodec",
    "validate_bucket_name",
    "validate_key",
]

_BUCKET_RE: Final = re.compile(r"\A[a-zA-Z0-9_-]+\Z")
_KEY_RE: Final = re.compile(r"\A[-/_=.a-zA-Z0-9]+\Z")
# A single subject token (no dots): the per-token unit for wildcard validation.
_KEY_TOKEN_RE: Final = re.compile(r"\A[-/_=a-zA-Z0-9]+\Z")


def validate_bucket_name(bucket: str) -> None:
    if not _BUCKET_RE.match(bucket):
        raise InvalidBucketNameError(f"invalid bucket name {bucket!r}: allowed characters are A-Z a-z 0-9 _ -")


def validate_key(key: str, *, wildcards: bool = False) -> None:
    """Validate a KV key (ADR-8). ``wildcards=True`` additionally allows the
    ``*``/``>`` subject wildcards used by ``watch()`` — ``*`` as a whole token
    and ``>`` only as the final token, mirroring subject-filter grammar."""

    def _reject() -> NoReturn:
        raise InvalidKeyError(
            f"invalid key {key!r}: allowed characters are A-Z a-z 0-9 - / _ = . "
            "(no leading/trailing dot, no empty tokens; '*'/'>' only with wildcards, '>' only final)"
        )

    if not key or key.startswith(".") or key.endswith(".") or ".." in key:
        _reject()
    if not wildcards:
        if not _KEY_RE.match(key):
            _reject()
        return
    tokens = key.split(".")
    last = len(tokens) - 1
    for index, token in enumerate(tokens):
        if token == "*":
            continue
        if token == ">":
            if index != last:
                _reject()
            continue
        if not _KEY_TOKEN_RE.match(token):
            _reject()


class Operation(StrEnum):
    """What a stored revision represents."""

    PUT = "PUT"
    DELETE = "DEL"
    PURGE = "PURGE"


@dataclass(frozen=True, slots=True)
class KvEntry:
    """One revision of a key."""

    bucket: str
    key: str
    value: bytes
    revision: int
    operation: Operation = Operation.PUT
    created: datetime | None = None
    delta: int = 0
    """Revisions between this entry and the newest at read time (watch only)."""

    @property
    def is_marker(self) -> bool:
        return self.operation is not Operation.PUT


@dataclass(frozen=True, slots=True, kw_only=True)
class KeyValueConfig:
    """Bucket configuration (maps onto an ADR-8 ``KV_<bucket>`` stream)."""

    bucket: str
    description: str | None = None
    history: int = 1
    # 0 / None means keys never expire. (The obvious default — an implicit
    # short TTL silently expiring data is a foot-gun, not a feature.)
    ttl: timedelta | None = None
    max_value_size: int = -1
    max_bytes: int = -1
    storage: StorageType = StorageType.FILE
    replicas: int = 1
    placement: Placement | None = None
    republish: Republish | None = None
    compression: bool = False
    # Enable per-message TTLs on the bucket (needed by purge(key, ttl=...)).
    allow_msg_ttl: bool = False
    # ADR-48: markers left by server-side limit deletions (MaxAge etc.) expire
    # on their own after this long. Implies allow_msg_ttl.
    limit_marker_ttl: timedelta | None = None
    metadata: dict[str, str] | None = None

    def __post_init__(self) -> None:
        validate_bucket_name(self.bucket)
        if not 1 <= self.history <= 64:
            raise InvalidBucketNameError("history must be between 1 and 64")
        if self.ttl is not None and self.ttl != timedelta(0) and self.ttl < timedelta(milliseconds=100):
            from natsio.errors import ConfigError

            raise ConfigError("ttl must be at least 100ms (server-enforced max_age floor) or 0/None for never")


@dataclass(frozen=True, slots=True)
class KeyValueStatus:
    """A point-in-time view of a bucket, derived from its stream info."""

    bucket: str
    values: int
    history: int
    ttl: timedelta | None
    bytes: int
    storage: StorageType
    stream_info: StreamInfo = field(repr=False)

    @property
    def is_compressed(self) -> bool:
        compression = self.stream_info.config.compression
        return bool(compression and compression.value != "none")


class KeyCodec(Protocol):
    """Bidirectional key transformation (identity when not provided).

    The seam that keeps codec packs (path notation, encryption of key names,
    escaping of exotic characters — ADR-54) a plug-in, not a breaking change.
    Encoded keys must still satisfy :func:`validate_key`.
    """

    def encode(self, key: str) -> str: ...

    def decode(self, key: str) -> str: ...


class ValueCodec(Protocol):
    """Bidirectional value transformation (compression, encryption, ...)."""

    def encode(self, value: bytes) -> bytes: ...

    def decode(self, value: bytes) -> bytes: ...
