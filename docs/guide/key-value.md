# Key-Value

A KV bucket is a JetStream stream in disguise: each key is a subject, each write
a message, and the message sequence *is* the key's revision. That foundation is
what gives KV its distinctive features — atomic compare-and-set, full history,
and live watches — for free.

```python
import natsio
from natsio.kv import KeyValueConfig

async with await natsio.connect("nats://localhost:4222") as nc:
    js = nc.jetstream()
    kv = await js.create_key_value(KeyValueConfig(bucket="config", history=5))

    revision = await kv.put("theme", b"dark")
    entry = await kv.get("theme")
    print(entry.value, entry.revision)
```

## Buckets

A [`KeyValueConfig`][natsio.kv.KeyValueConfig] describes the bucket. The knobs
you actually reach for:

- `history` — revisions kept per key (1–64, default **1**).
- `storage` / `replicas` — where and how redundantly it lives.
- `ttl` — a *bucket-wide* max age. See the note below.

!!! warning "Keys never expire unless you ask"
    natsio has **no default bucket TTL**. `ttl=None` (the default) means keys
    live until deleted — the legacy client's silent 120 s default was a
    data-loss foot-gun. A non-`None` `ttl` must be at least 100 ms (the
    server's `max_age` floor). For *per-key* expiry, see
    [per-key TTLs](#per-key-ttls) — a different, opt-in mechanism.

Re-creating a bucket with an *identical* config is idempotent;
[`create_key_value`][natsio.jetstream.JetStreamContext.create_key_value] against
an existing bucket with a *different* config raises `BucketExistsError`. To
assert-or-migrate a bucket from a service, use
[`create_or_update_key_value`][natsio.jetstream.JetStreamContext.create_or_update_key_value].

## Reads and writes: the revision model

Every write returns the new **revision** (a stream sequence). `get` returns a
[`KvEntry`][natsio.kv.KvEntry] carrying `.value`, `.revision`, `.operation`, and
`.created`.

```python
rev = await kv.put("theme", b"dark")             # -> revision
entry = await kv.get("theme")                    # latest
old = await kv.get("theme", revision=rev)        # a specific revision
```

`create` and `update` are the atomic variants:

=== "create — only if absent"

    Succeeds for brand-new (or deleted/purged) keys; raises
    `KeyExistsError` when a live value exists.

    ```python
    from natsio.kv import KeyExistsError

    await kv.create("region", b"us-east")
    try:
        await kv.create("region", b"eu-west")
    except KeyExistsError:
        ...  # already has a live value
    ```

=== "update — compare-and-set"

    Optimistic locking: pass the revision you read. If another writer moved the
    key on, the update is rejected with
    `WrongLastSequenceError`.

    ```python
    from natsio.jetstream import WrongLastSequenceError

    current = await kv.get("theme")
    await kv.update("theme", b"solarized", last=current.revision)
    try:
        await kv.update("theme", b"light", last=current.revision)  # stale
    except WrongLastSequenceError:
        ...  # someone else won the race
    ```

## Delete vs. purge

Both stop `get` from returning a value, but they treat history differently.

- **`delete(key)`** writes a *delete marker* (tombstone) on top. `get` then
  raises `KeyDeletedError` (a subclass of
  `KeyNotFoundError`), but prior revisions are preserved in history.
- **`purge(key)`** writes a *purge marker* and rolls up: the marker stays, but
  every revision beneath it is erased.

```python
from natsio.kv import KeyDeletedError

await kv.delete("region")
try:
    await kv.get("region")
except KeyDeletedError:
    ...  # a delete marker sits on top; history below is intact

await kv.purge("region")   # history below the marker is now gone
```

Both accept `last=` for a compare-and-set variant. An entry's `.is_marker`
distinguishes a tombstone from a real value.

## Per-key TTLs

A single write can self-expire, independent of any bucket TTL. The bucket must
opt in with `allow_msg_ttl=True` (or `limit_marker_ttl=`); otherwise a `ttl=`
write raises `ConfigError` client-side. `ttl` is a `timedelta`, whole seconds,
or `"never"`.

```python
from datetime import timedelta

kv = await js.create_key_value(KeyValueConfig(bucket="sessions", allow_msg_ttl=True))
await kv.put("token", b"...", ttl=300)                    # expires in 5 minutes
await kv.create("lock", b"held", ttl=timedelta(seconds=30))
```

`purge(key, ttl=...)` lets the purge marker itself expire.

## History and keys

```python
history = await kv.history("theme")   # every stored revision, oldest first
for e in history:
    print(e.revision, e.value, e.operation.value)

keys = await kv.keys()                # every key with a live value; [] if empty
async for key in kv.iter_keys():      # streaming, no full-keyspace buffer
    ...
```

`history()` includes markers and raises `KeyNotFoundError` for an unknown key;
`keys()` returns `[]` for an empty bucket rather than raising.

## Watching a bucket

A watcher is a live feed. It first replays the **current state** (one entry per
live key), then yields exactly one `None` to signal *"you are now caught up"*,
then streams live updates. That `None` is the seam between snapshot and live
tail — the single most important thing to get right with `watch`.

```python
async with kv.watch() as watcher:          # no keys = the whole bucket
    async for change in watcher:
        if change is None:
            break                          # initial state fully delivered
        print(change.key, change.value, change.operation.value)
```

The watcher is self-healing (backed by the [ordered
consumer](jetstream.md#the-ordered-consumer)) and never duplicates keys or
resurrects stale values if a heal lands mid-snapshot. Options shape what it
delivers:

| Option | Effect |
|---|---|
| `watch("a", "b.*", ...)` | Watch specific keys/wildcards; the union of their state and updates. |
| `updates_only=True` | Skip the snapshot — the `None` marker arrives immediately, then only live changes. |
| `include_history=True` | Replay *every* stored revision, not just the latest per key. |
| `ignore_deletes=True` | Suppress delete/purge markers. |
| `meta_only=True` | Deliver entries without their values (keys/metadata only). |
| `resume_from_revision=N` | Replay from stream revision `N` onward (mutually exclusive with the two above). |

```python
# multi-key watch, values suppressed, tail only
async with kv.watch("session.*", updates_only=True, meta_only=True) as w:
    async for change in w:
        if change is not None:
            print("touched", change.key)
```

## Housekeeping: purge_deletes

Over time delete/purge markers accumulate.
[`purge_deletes`][natsio.kv.KeyValue.purge_deletes] removes them (and any history
beneath them). It keeps markers younger than `older_than` (default 30 minutes,
matching nats.go) so late watchers still see recent tombstones; pass
`timedelta(0)` to clear them all.

```python
from datetime import timedelta

await kv.purge_deletes(older_than=timedelta(0))
```

## Codecs

Keys and values pass through optional codecs on the way in and out — the seam
that keeps codec packs (path notation, encryption, compression) a plug-in rather
than a breaking change. A `KeyCodec` /
`ValueCodec` is any object with `encode`/`decode`;
identity is the default. Encoded keys must still satisfy the key grammar.

```python
class PrefixKeys:
    def __init__(self, prefix): self.prefix = prefix
    def encode(self, key): return f"{self.prefix}.{key}"
    def decode(self, key): return key.removeprefix(f"{self.prefix}.")

kv = await js.create_key_value(
    KeyValueConfig(bucket="app"),
    key_codec=PrefixKeys("v1"),
)
await kv.put("greeting", b"hello")   # stored under 'v1.greeting'
entry = await kv.get("greeting")     # entry.key is 'greeting' again
```

A wildcard `watch` key combined with a key codec is refused loudly — the encoded
keyspace would silently match nothing.

## Status and management

```python
status = await kv.status()
print(status.values, status.history, status.ttl, status.bytes)

# enumerate every bucket without a per-bucket round-trip
async for name in js.key_value_store_names():
    ...
async for st in js.key_value_stores():
    print(st.bucket, st.values)
```

Other management calls: [`key_value`][natsio.jetstream.JetStreamContext.key_value]
(handle to an existing bucket),
[`update_key_value`][natsio.jetstream.JetStreamContext.update_key_value], and
`delete_key_value`.

## See also

- [JetStream](jetstream.md) — the streams, consumers, and ordered consumer KV
  is built on.
- [Object Store](object-store.md) — the same foundation for large blobs.
- [Key-Value API reference](../reference/kv.md).
