"""Access to the two KV buckets the group configs live in.

Both flavours store their configs in a KV bucket keyed ``"<stream>.<group>"``
(`STATIC_BUCKET` / `ELASTIC_BUCKET`), created on demand with the same shape the
oracle uses: file storage, replicated like the stream the groups sit on.
"""

from natsio.jetstream import JetStreamContext, StorageType
from natsio.kv import KeyValue, KeyValueConfig
from natsio.kv.errors import BucketExistsError, BucketNotFoundError

from .errors import ConsumerGroupError, ConsumerGroupNotFoundError

__all__ = ["open_bucket", "open_or_create_bucket", "require_names"]


async def open_bucket(js: JetStreamContext, bucket: str) -> KeyValue:
    """Open a consumer-group KV bucket, mapping "no bucket" to "no group".

    A missing bucket means no group of that flavour has ever been created in
    this account, which is indistinguishable from "this group doesn't exist"
    for every caller here.
    """
    try:
        return await js.key_value(bucket)
    except BucketNotFoundError as exc:
        raise ConsumerGroupNotFoundError(f"the {bucket!r} KV bucket does not exist") from exc


async def open_or_create_bucket(js: JetStreamContext, bucket: str, replicas: int) -> KeyValue:
    """Open a consumer-group KV bucket, creating it if it isn't there yet."""
    try:
        return await js.key_value(bucket)
    except BucketNotFoundError:
        pass
    try:
        return await js.create_key_value(KeyValueConfig(bucket=bucket, replicas=replicas, storage=StorageType.FILE))
    except BucketExistsError:
        return await js.key_value(bucket)  # lost a creation race: theirs is as good as ours


def require_names(stream: str, group: str) -> None:
    """Reject blank stream/group names before they become a malformed KV key."""
    if not stream or not group:
        raise ConsumerGroupError(f"invalid stream name {stream!r} or consumer group name {group!r}")
