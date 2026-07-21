# Object Store

KV is for small values; the **Object Store** is for arbitrarily large ones. An
object is split into fixed-size chunks (128 KiB by default) stored as separate
messages, with one rollup *metadata* message describing it. Reads stream the
chunks back through a self-healing ordered consumer and verify the SHA-256
digest at the end — so **a `get` that completes is a verified read**.

```python
import natsio
from natsio.objectstore import ObjectStoreConfig

async with await natsio.connect("nats://localhost:4222") as nc:
    js = nc.jetstream()
    obj = await js.create_object_store(ObjectStoreConfig(bucket="assets"))

    await obj.put("logo.png", payload)             # bytes or async iterable
    data = await obj.get_bytes("logo.png")         # digest-verified
```

## Buckets

An [`ObjectStoreConfig`][natsio.objectstore.ObjectStoreConfig] takes a `bucket`
name plus the usual `storage`, `replicas`, `max_bytes`, `ttl`, and
`compression`. As with KV, `ttl=None` means objects never expire. Re-creating
with an identical config is idempotent; a conflicting config raises
`BucketExistsError`. To assert-or-migrate from a service, use
[`create_or_update_object_store`][natsio.jetstream.JetStreamContext.create_or_update_object_store].

## Storing objects

[`put`][natsio.objectstore.ObjectStore.put] takes a name (or an
[`ObjectMeta`][natsio.objectstore.ObjectMeta]) and data. The data may be
in-memory bytes **or an async iterable of byte chunks** — natsio re-chunks
whatever you give it into uniform pieces, SHA-256-digesting as it streams, so
your producer's slicing never has to match the stored chunk size. It returns the
stored [`ObjectInfo`][natsio.objectstore.ObjectInfo] (`size`, `chunks`,
`digest`, `nuid`).

=== "From bytes"

    ```python
    info = await obj.put("readme.txt", b"hello object store")
    print(info.size, info.chunks)
    ```

=== "Streaming (async iterable)"

    ```python
    async def source():
        async for block in some_upload():
            yield block          # any sizes; never held whole in memory

    info = await obj.put("upload.bin", source())
    ```

Attach metadata by passing an `ObjectMeta` first: `description`, a `metadata`
dict, multi-valued `headers`, and a custom `chunk_size`.

```python
from natsio.objectstore import ObjectMeta

await obj.put(
    ObjectMeta(
        name="report.csv",
        description="Q3 numbers",
        metadata={"team": "finance"},
        headers={"Content-Type": ["text/csv"]},
        chunk_size=256 * 1024,
    ),
    data,
)
```

A `put` replaces any existing object of that name and reclaims the old
revision's chunks. If a put fails partway — including losing a concurrent
same-name race — its own published chunks are purged rather than orphaned.

## Reading objects

[`get`][natsio.objectstore.ObjectStore.get] returns an
[`ObjectResult`][natsio.objectstore.ObjectResult] — an async context manager and
async iterator of chunks. `result.info` is the (link-resolved) metadata, and the
digest and size are checked after the final chunk.

```python
async with obj.get("upload.bin") as result:
    print(result.info.size, "bytes")
    total = bytearray()
    async for chunk in result:
        total += chunk
```

If a chunk is missing, the read raises
`NoMessagesError` within a bounded
`chunk_timeout` (default 30 s) instead of hanging; if the digest or size does not
match, it raises
`DigestMismatchError`. Reaching the end
of the iteration is therefore proof the bytes are intact.

For small objects, [`get_bytes`][natsio.objectstore.ObjectStore.get_bytes]
returns the whole thing in one buffer:

```python
data = await obj.get_bytes("readme.txt")
```

## Delete and show_deleted

[`delete`][natsio.objectstore.ObjectStore.delete] replaces the metadata with a
delete marker and purges the chunks. The marker keeps the name visible to
`watch` / `list(include_deleted=True)`; the data is gone.

```python
await obj.delete("upload.bin")

info = await obj.info("upload.bin", show_deleted=True)   # the marker, not an error
print(info.is_deleted)                                    # True
```

Without `show_deleted`, [`info`][natsio.objectstore.ObjectStore.info] and `get`
raise `ObjectDeletedError` for a deleted
object.

## Links

A link is a named pointer; `get` on a link transparently streams the target.

```python
info = await obj.info("readme.txt")
await obj.add_link("readme.latest", info)          # -> follows to readme.txt
via_link = await obj.get_bytes("readme.latest")

await obj.add_bucket_link("shared", other_store)   # a directory-entry pointer
```

`add_link` refuses deleted targets, links-to-links, and shadowing a live
non-link object. A **bucket link** points at a whole bucket; `get` on one raises
`LinkError` — open the linked bucket via
[`object_store`][natsio.jetstream.JetStreamContext.object_store] instead.

## Updating metadata and renaming

[`update_meta`][natsio.objectstore.ObjectStore.update_meta] rewrites an object's
`description` / `metadata` / `headers` in place — the data (chunks, nuid,
digest, size) is untouched.

```python
await obj.update_meta("report.csv", ObjectMeta(name="report.csv", description="final"))
```

When `meta.name` **differs** from the current name, the object is **renamed**:
the metadata moves to the new name's subject and the old name then raises
`ObjectNotFoundError`. The chunks stay put under the same nuid — no data is
copied.

```python
await obj.update_meta("report.csv", ObjectMeta(name="report-q3.csv"))
# obj.info("report.csv") now raises ObjectNotFoundError
```

Renaming onto a *live* object raises
`ObjectExistsError`; onto a *deleted*
name is allowed. Every meta write is CAS-gated, so a rename is safe against
concurrent writers of the *target* name (though not the source — the two names
share one nuid, inherent to the ADR-20 format).

## Watch and list

`watch` and `list` share the KV watcher shape: `watch` replays current state,
yields one `None` "caught up" marker, then streams live changes.

```python
async with obj.watch() as watcher:
    async for item in watcher:
        if item is None:
            break                 # initial state delivered
        print(item.name, item.size)

names = [o.name for o in await obj.list()]                    # live objects
everything = await obj.list(include_deleted=True)             # markers too
```

There is deliberately no `include_history` — every meta write rolls up its
subject, so the store only ever holds each object's latest revision.

## Seal and status

[`seal`][natsio.objectstore.ObjectStore.seal] makes the bucket permanently
immutable — no more puts, deletes, or purges, ever. Only seal with no writers in
flight (an interrupted put could leave permanently unreclaimable chunks).

```python
await obj.seal()

status = await obj.status()
print(status.sealed, status.size, status.ttl)
```

Enumerate every bucket without a per-bucket round-trip via
[`object_store_names`][natsio.jetstream.JetStreamContext.object_store_names] and
[`object_stores`][natsio.jetstream.JetStreamContext.object_stores].

## See also

- [JetStream](jetstream.md) — the ordered consumer that powers streaming reads
  and watches.
- [Key-Value](key-value.md) — the same foundation for small values.
- [Object Store API reference](../reference/objectstore.md).
