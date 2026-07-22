# natsio-counters

Distributed counters over JetStream counter streams
([ADR-49](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-49.md)),
mirroring [`orbit.go/counters`](https://github.com/synadia-io/orbit.go/tree/main/counters).
Distribution `natsio-counters`, imported as `natsio.counters`. Zero runtime
dependencies beyond `natsio`. Pre-1.0, no API-stability promises.

Requires **nats-server 2.12+** (the `allow_msg_counter` stream feature).

## What it is

A *counter stream* is a normal JetStream stream created with
`allow_msg_counter`. Every subject the stream captures becomes an independent,
**arbitrary-precision** counter. Incrementing is an ordinary publish carrying a
`Nats-Incr` header; the server folds the delta into a running total, stores it
as the subject's latest message, and returns the new value in the PubAck — so a
bare `add` needs no follow-up read.

Python `int` is itself unbounded, so it is used directly for both deltas and
values — no big-integer wrapper.

## Usage

```python
import natsio
from natsio.counters import CounterConfig, create_counter, get_counter

nc = await natsio.connect("nats://localhost")
js = nc.jetstream()

# Create a counter stream (allow_msg_counter + allow_direct are forced on).
counter = await create_counter(js, CounterConfig(name="COUNTS", subjects=["events.>"]))
# ...or bind an existing one:  counter = await get_counter(js, "COUNTS")

# Increment / decrement — returns the new running total, straight from the PubAck.
await counter.add("events.orders", 1)      # -> 1
await counter.add("events.orders", 10)     # -> 11
await counter.add("events.orders", -4)     # negative deltas are fine  -> 7
await counter.add("events.orders", 0)      # zero is a valid no-op increment

# Read a single counter's current value.
value = await counter.load("events.orders")          # -> 7

# Full entry: value + most-recent increment + source history (aggregation only).
entry = await counter.get("events.orders")
print(entry.value, entry.incr, entry.sources)

# Enumerate many counters in one batch Direct Get (wildcards allowed).
async for entry in counter.get_multiple(["events.>"]):
    print(entry.subject, entry.value)
```

## API

| Symbol | Purpose |
|---|---|
| `create_counter(js, config)` | Create a counter stream and return its `Counter`. |
| `get_counter(js, name)` | Bind an existing counter stream by name. |
| `counter_from_stream(js, stream)` | Wrap an already-fetched counter `Stream` (no round-trip). |
| `Counter.add(subject, delta) -> int` | Increment/decrement; returns the new total. |
| `Counter.load(subject) -> int` | Current value of one subject's counter. |
| `Counter.get(subject) -> CounterEntry` | Value + last increment + sources. |
| `Counter.get_multiple(subjects) -> AsyncIterator[CounterEntry]` | Batch/wildcard enumeration. |
| `CounterConfig` | Counter-stream configuration. |
| `CounterEntry` | `subject`, `value`, `sources`, `incr`. |

Errors (all subclass `natsio.jetstream.JetStreamError`): `CounterNotEnabledError`,
`DirectAccessRequiredError`, `CounterNotFoundError`,
`CounterSubjectNotInitializedError`, `InvalidCounterValueError`.

## ADR-49 wire contract

Pinned by the wire-contract tests and exported as constants:

- **Increment header** `Nats-Incr` (`COUNTER_INCREMENT_HEADER`) — a base-10
  integer of any size, optional leading sign. A counter stream **rejects any
  publish without it** (`message counter increment is missing`).
- **Sources header** `Nats-Counter-Sources` (`COUNTER_SOURCES_HEADER`) —
  server-set JSON `{source_stream: {subject: contribution}}`; present only on
  aggregating counters.
- **Stored payload** `{"val": "<int-as-string>"}` — the value is a *string* so
  it survives languages whose JSON numbers cap at 2^53.
- **PubAck** carries the new total in its `val` field (fast feedback, no Get).

Notes: `allow_msg_counter` can be set **only at stream creation** (it is read-only
afterwards) and is **incompatible with per-message TTLs and message schedules**,
so `CounterConfig` intentionally does not expose those knobs. Reads use Direct
Get, so `allow_direct` is required (and forced on by `create_counter`).
