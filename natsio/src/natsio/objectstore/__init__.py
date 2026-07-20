"""Object Store over JetStream (ADR-20): chunked blobs with verified reads.

js = nc.jetstream()
obj = await js.create_object_store(ObjectStoreConfig(bucket="assets"))

info = await obj.put("logo.png", payload)          # bytes or async iterable
data = await obj.get_bytes("logo.png")             # digest-verified

async with obj.get("video.mp4") as result:         # streaming, 128KiB chunks
    async for chunk in result:
        ...

Data is chunked onto ``$O.<bucket>.C.<nuid>`` subjects; per-object metadata
lives as a rollup message on ``$O.<bucket>.M.<base64(name)>``. Reads stream
through the self-healing ordered consumer and verify the SHA-256 digest, so a
completed ``get`` is a verified read.
"""

from natsio.objectstore.entities import (
    DEFAULT_CHUNK_SIZE,
    ObjectInfo,
    ObjectLink,
    ObjectMeta,
    ObjectMetaOptions,
    ObjectStoreConfig,
    ObjectStoreStatus,
    validate_bucket_name,
    validate_object_name,
)
from natsio.objectstore.errors import (
    BucketExistsError,
    BucketNotFoundError,
    DigestMismatchError,
    InvalidBucketNameError,
    InvalidObjectNameError,
    LinkError,
    ObjectDeletedError,
    ObjectExistsError,
    ObjectNotFoundError,
)
from natsio.objectstore.store import ObjectResult, ObjectStore, ObjectWatcher, encode_object_name

__all__ = [
    "DEFAULT_CHUNK_SIZE",
    "BucketExistsError",
    "BucketNotFoundError",
    "DigestMismatchError",
    "InvalidBucketNameError",
    "InvalidObjectNameError",
    "LinkError",
    "ObjectDeletedError",
    "ObjectExistsError",
    "ObjectInfo",
    "ObjectLink",
    "ObjectMeta",
    "ObjectMetaOptions",
    "ObjectNotFoundError",
    "ObjectResult",
    "ObjectStore",
    "ObjectStoreConfig",
    "ObjectStoreStatus",
    "ObjectWatcher",
    "encode_object_name",
    "validate_bucket_name",
    "validate_object_name",
]
