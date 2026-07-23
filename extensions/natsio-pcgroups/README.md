# natsio-pcgroups

Partitioned consumer groups over JetStream — **static** and **elastic** —
mirroring [`orbit.go/pcgroups`](https://github.com/synadia-io/orbit.go/tree/main/pcgroups).
Distribution `natsio-pcgroups`, imported as `natsio.pcgroups`. Zero runtime
dependencies beyond `natsio`. Pre-1.0, no API-stability promises.

Requires **nats-server 2.11+** (ADR-42 pinned-client priority groups, subject
transforms). Verified against the repo-pinned **2.14.3**.

## What it is

Strictly ordered consumption of a JetStream stream means `max_ack_pending=1` —
one message in flight, no horizontal scaling. A *consumer group* fixes that by
splitting the stream into `max_members` **partitions** keyed by subject and
handing each **member** a fixed subset of them. Members run in parallel;
messages that share a partitioning key are still processed one at a time, in
order.

Each member may run as **several instances**. The server pins exactly one of
them (ADR-42 `pinned_client`) and the rest sit as hot standbys, so a member is
highly available without ever double-processing a key.

The group's config lives in a KV bucket; the library derives every member's
partition filters from it, creates the consumers, and hands your handler
messages with the **partition token already stripped from the subject**, so
existing handler code works unchanged.

| | Static | Elastic |
|---|---|---|
| Partition token in the source subjects | required (stream subject transform) | not needed |
| Extra stream | none — consumers sit on your stream | a sourced work-queue stream `<stream>-<group>` |
| Membership | fixed at creation | changed at any time |
| Config bucket | `static-consumer-groups` | `elastic-consumer-groups` |
| Ack policy | your choice | must be `explicit` |

## Static groups

The stream already carries the partition number as the first subject token —
put there on ingest by a stream-level subject transform:

```python
import natsio
from natsio.jetstream import AckPolicy, ConsumerConfig, StreamConfig, SubjectTransform
from natsio.pcgroups import PartitionedMsg, create_static, static_consume

nc = await natsio.connect("nats://localhost")
js = nc.jetstream()

# Partition "orders.<id>" over 10 partitions on ingest.
await js.create_stream(
    StreamConfig(
        name="ORDERS",
        subjects=["orders.*"],
        subject_transform=SubjectTransform(src="orders.*", dest="{{partition(10,1)}}.orders.{{wildcard(1)}}"),
    )
)

await create_static(js, "ORDERS", "cg", 10, ["orders.*"], ["m1", "m2"])

async def handle(msg: PartitionedMsg) -> None:
    print(msg.subject, msg.data)   # "orders.42" — the partition token is gone
    await msg.ack()

context = await static_consume(
    js, "ORDERS", "cg", "m1", handle,
    ConsumerConfig(ack_policy=AckPolicy.EXPLICIT, max_ack_pending=1),
)
...
await context.stop()
```

A static group's config is **immutable**. Changing it (or deleting the group)
stops every running member instance; to re-shape the membership, delete the
group and create it again.

## Elastic groups

The stream needs no partition token. Creating the group creates a sourced
**work-queue** stream that inserts one, and the membership can change at any
time — running members start and stop consuming as they are added and dropped:

```python
from natsio.pcgroups import PartitioningFilter, add_members, create_elastic, delete_members, elastic_consume

await js.create_stream(StreamConfig(name="ORDERS", subjects=["orders.*"]))

# Partition on the first wildcard of "orders.*", over 10 partitions.
await create_elastic(js, "ORDERS", "cg", 10, [PartitioningFilter(filter="orders.*", partitioning_wildcards=[1])])

# Members may join before they are in the membership: they simply wait.
context = await elastic_consume(js, "ORDERS", "cg", "m1", handle, ConsumerConfig(ack_policy=AckPolicy.EXPLICIT))

await add_members(js, "ORDERS", "cg", ["m1", "m2"])   # m1 starts consuming
await context.wait_for_consuming()                     # optional: block until it does
await delete_members(js, "ORDERS", "cg", ["m1"])       # ...and stops again
```

With no `partitioning_filters` the group covers the whole stream and partitions
on the entire subject.

## API

### Static

| Symbol | Purpose |
|---|---|
| `create_static(js, stream, group, max_members, filters=None, members=None, member_mappings=None)` | Create the group (idempotent for an identical config). |
| `get_static_config(js, stream, group)` | The stored `StaticConsumerGroupConfig`. |
| `delete_static(js, stream, group)` | Delete the config, then sweep the member consumers. |
| `list_static_groups(js, stream)` | Group names defined on a stream. |
| `list_static_active_members(js, stream, group)` | Members whose consumer has a waiting pull. |
| `static_member_step_down(js, stream, group, member)` | Unpin the member's active instance. |
| `static_consume(js, stream, group, member, handler, config=None)` | Join and consume → `StaticConsumeContext`. |

### Elastic

| Symbol | Purpose |
|---|---|
| `create_elastic(js, stream, group, max_members, partitioning_filters=None, max_buffered_msgs=-1, max_buffered_bytes=-1)` | Create the group and its work-queue stream. |
| `get_elastic_config(js, stream, group)` | The stored `ElasticConsumerGroupConfig`. |
| `delete_elastic(js, stream, group)` | Delete the config and the group's stream. |
| `list_elastic_groups(js, stream)` | Group names defined on a stream. |
| `add_members(js, stream, group, members)` / `delete_members(...)` | Change the membership (CAS-guarded). |
| `set_member_mappings(js, stream, group, mappings)` / `delete_member_mappings(...)` | Explicit partition assignment. |
| `list_elastic_active_members(js, stream, group)` | Members that currently have a consumer. |
| `elastic_is_in_membership_and_active(js, stream, group, member)` | `(in_membership, is_active)` for one member. |
| `elastic_member_step_down(js, stream, group, member)` | Unpin the member's active instance. |
| `elastic_consume(js, stream, group, member, handler, config=None)` | Join and consume → `ElasticConsumeContext`. |

### Consume context

Both `*_consume` calls perform I/O, so they are plain coroutines (no optional
`await` no-op — there is nothing to make optional). The context they return:

| Member | Purpose |
|---|---|
| `await ctx.stop()` | End the session. Idempotent, deterministic, safe to call **from inside a handler**. |
| `await ctx.wait()` | Block until the session ends; re-raises whatever terminated it. |
| `await ctx.wait_for_consuming(consuming=True, *, timeout=None)` | Block until this instance is (or stops being) attached to a consumer. |
| `async with ctx: ...` | Stops on exit. |
| `ctx.consuming` | Whether this instance holds a consumer (not whether it is the *pinned* one). |
| `ctx.error` | What terminated the session, or `None`. |
| `ctx.recovered_errors` / `ctx.last_recovered_error` | Recoverable faults absorbed while (re)joining — counted, never silent. |

`handler` is `async (PartitionedMsg) -> None`, invoked **serially**. A handler
exception terminates the session and surfaces from `wait()` / `error` — it is
never swallowed. `PartitionedMsg` exposes `subject` (partition token stripped),
`partitioned_subject`, `partition`, `data`/`payload`, `headers`, `metadata`,
`reply`, `message` (the underlying `JsMsg`) and the full ack surface
(`ack`, `ack_sync`, `nak`, `term`, `in_progress`).

### Pure logic (importable, unit-tested against the oracle)

`generate_partition_filters`, `static_get_partition_filters`,
`elastic_get_partition_filters`, `partitioning_transform_destination`,
`compose_key`, `compose_static_consumer_name`, `compose_group_stream_name`,
`strip_partition`, `validate_static_config`, `validate_elastic_config`.

Errors (all subclass `natsio.jetstream.JetStreamError`): `ConsumerGroupError`,
`ConsumerGroupConfigError`, `ConsumerGroupNotFoundError`,
`ConsumerGroupExistsError`, `MemberNotInGroupError`, `GroupConfigChangedError`.

## Wire contract

Everything below is pinned byte-for-byte to orbit.go so a group created by
either library is administered and consumed by the other.

- **KV buckets** `static-consumer-groups` / `elastic-consumer-groups`, key
  `"<stream>.<group>"`.
- **Static config JSON**: `max_members`, `filters`, `members`, `member_mappings`
  (the last three omitted when empty — Go tags them `omitempty` and compares
  `member_mappings` with `reflect.DeepEqual`, where `[]` ≠ `nil`).
- **Elastic config JSON**: `max_members`, `partitioning_filters`
  (`{filter, partitioning_wildcards}`), **`max_buffered_msg`** (singular — that
  is the tag orbit.go puts on `MaxBufferedMsgs`), `max_buffered_bytes`,
  `members`, `member_mappings`.
- **Static consumer name** `"<group>-<member>"`; **elastic consumer name**
  `"<member>"` on the group's stream `"<stream>-<group>"`.
- **Priority group** `"PCG"`, policy `pinned_client`, `priority_timeout`
  (nats.go's `PinnedTTL`) `= max(ack_wait, 1s)`; pulls carry `group=PCG` and
  expire after `max(ack_wait/2, 1s)`.
- **Elastic group stream**: work-queue retention, `discard=new`,
  `allow_direct`, same replicas/storage as the origin, one source with one
  subject transform per partitioning filter, destination
  `{{Partition(N[,w…])}}.<filter with {{Wildcard(i)}} substituted>`.
- **Partition assignment**: membership deduplicated, sorted, capped to
  `max_members`; each member gets a contiguous run of `max_members //
  len(members)` partitions and the remainder is dealt round-robin from the
  front. Member mappings always yield `"<partition>.>"` filters (the configured
  subject filters are ignored — the oracle's behaviour, load-bearing for
  interop).

## Ack wait drives failover

Every reactivity timer derives from the `ack_wait` in the `ConsumerConfig` you
pass (default **5s**): pinned TTL `max(ack_wait, 1s)`, pull expiry
`max(ack_wait/2, 1s)`, elastic inactive threshold and self-correction tick
`ack_wait` (+0.5s). **Use `ack_wait >= 2s`**: below that the pull-expiry floor
of 1s collides with the pinned TTL and the pinned instance can flap between
instances of the same member (harmless but noisy — the same caveat the oracle
documents).

## Deliberate divergences from the oracle

Everything on the wire matches; these are local behaviours where the oracle is
either buggy or silent:

1. **Duplicate partition filters are deduplicated.** The oracle emits one
   `"<partition>.>"` per configured filter when member mappings are used, so a
   group with mappings *and* more than one filter asks the server for the same
   filter twice and is rejected with err_code **10138**
   (`consumer subject filters cannot overlap`) — that combination can never
   create a consumer. We drop the duplicate; the requested subjects are
   identical, so nothing else changes. Live-regression-tested.
2. **`elastic_is_in_membership_and_active` answers about the member you asked
   about.** The oracle's version never compares against the member name, so it
   reports "active" whenever *any* member has a consumer.
3. **Creates are compare-and-set.** `create_static` / `create_elastic` write
   with `create()` (not `put()`), and every elastic membership/mapping update
   is CAS'd on the entry's revision, so a concurrent administrator loses loudly
   (`WrongLastSequenceError`) instead of silently clobbering. The oracle CASes
   only `AddMembers`/`DeleteMembers`.
4. **Membership is stored sorted and deduplicated.** The oracle stores it in Go
   map order (random). The assignment sorts anyway; this only makes the stored
   bytes stable.
5. **A deleted group is a normal end even if the consumer sweep wins the race.**
   `delete_static` deletes the config and then the consumers; if a member sees
   its consumer vanish first, it re-reads the config before deciding, so
   `wait()` still returns cleanly rather than raising `ConsumerDeletedError`.
6. **Purge counts as deletion.** A purged config key ends the session the same
   way a deleted one does; the oracle checks only for the delete operation.
7. **A handler exception is fatal and reported — for both flavours.** It ends
   the session and surfaces from `wait()` / `error`; it is never folded into
   the recoverable-fault path, so a poison message cannot be redelivered
   forever (with `max_ack_pending=1` that would head-of-line block the whole
   partition). Only transport-side churn — the consumer deleted underneath a
   member during elastic membership changes — is recoverable. Go's callback
   would just panic in its own goroutine.

## Scope limits

- **No CLI.** The oracle ships a `cg` command-line tool; this package is a
  library only.
- **Static groups are not elastic, by design.** Any change to a static group's
  stored config stops every member instance — including changes made by another
  tool. That is the oracle's contract, not an implementation shortcut.
- **`max_members` is the partition count** and cannot be changed for the life
  of a group (it is baked into the elastic stream's subject transform).
- **Members beyond `max_members` get nothing.** The membership is capped after
  sorting, so the extra names are silently unassigned — oracle parity.
- **Elastic membership changes are eventually consistent.** Members apply a
  change independently, so during the transition a message can still be served
  to the member that is about to lose that partition. Nothing is lost or
  duplicated (the work-queue stream and the ack guarantee that), but the split
  of a batch published *during* the change is not deterministic.
- **`max_buffered_msgs` pauses sourcing** for at least a second when the
  work-queue stream fills. Size it above a second of consumption or expect
  small delivery gaps (oracle behaviour, inherited from the server).
- **Single-replica testing.** The live suite runs against one server; the
  replica/storage inheritance for the KV bucket and the group stream follows
  the origin stream but is not exercised in a cluster here.
