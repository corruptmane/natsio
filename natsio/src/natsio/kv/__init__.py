"""Key-Value store over JetStream (ADR-8, direct reads per ADR-31, ADR-48 markers).

js = nc.jetstream()
kv = await js.create_key_value(KeyValueConfig(bucket="settings", history=5))

revision = await kv.put("theme", b"dark")
entry = await kv.get("theme")

async with kv.watch() as watcher:
    async for entry in watcher:
        if entry is None:  # initial state fully delivered
            continue
        ...
"""

from natsio.kv.bucket import KV_OPERATION_HEADER, KeyValue, KvWatcher
from natsio.kv.entities import (
    KeyCodec,
    KeyValueConfig,
    KeyValueStatus,
    KvEntry,
    Operation,
    ValueCodec,
    validate_bucket_name,
    validate_key,
)
from natsio.kv.errors import (
    BucketExistsError,
    BucketNotFoundError,
    InvalidBucketNameError,
    InvalidKeyError,
    KeyDeletedError,
    KeyExistsError,
    KeyNotFoundError,
)

__all__ = [
    "KV_OPERATION_HEADER",
    "BucketExistsError",
    "BucketNotFoundError",
    "InvalidBucketNameError",
    "InvalidKeyError",
    "KeyCodec",
    "KeyDeletedError",
    "KeyExistsError",
    "KeyNotFoundError",
    "KeyValue",
    "KeyValueConfig",
    "KeyValueStatus",
    "KvEntry",
    "KvWatcher",
    "Operation",
    "ValueCodec",
    "validate_bucket_name",
    "validate_key",
]
