# natsio-jetstream-batch

JetStream **fast-ingest batch publishing** and **batch reads** for natsio,
mirroring [`orbit.go/jetstreamext`](https://github.com/synadia-io/orbit.go/tree/main/jetstreamext).
Distribution `natsio-jetstream-batch`, imported as `natsio.jetstream_batch`.
Zero runtime dependencies beyond `natsio`. Pre-1.0, no API-stability promises.

Requires **nats-server 2.14+** for fast ingest (the `allow_batched` stream
feature); batch direct get needs 2.11+ and `allow_direct`.

## What it is

**Fast ingest** is the high-throughput half of JetStream batching. An *atomic*
batch (`allow_atomic`) holds every message back until the commit; a *fast*
batch (`allow_batched`) persists each message as it is added, and the commit
only marks the end of the batch and returns the ack for the last stored
message. The cost of that speed is that acks are periodic instead of
per-message, so the client runs a small flow-control protocol:

- the server acks every `flow` messages (it may ack more often, never less),
- the publisher stalls once `flow * max_outstanding_acks` messages are unacked,
- pings re-request the latest ack, so a *lost* ack costs a third of the ack
  deadline instead of the whole batch.

**Batch reads** (`get_batch`) pull many stored messages out of one Direct Get,
starting from a sequence, a timestamp, or the lowest match. The complementary
"last message on each subject" read already lives in core as
`Stream.get_last_msgs_for`, and is not duplicated here.

## Usage

```python
import natsio
from natsio.jetstream import StreamConfig
from natsio.jetstream_batch import ALLOW_BATCHED, FlowControl, fast_publisher, get_batch

nc = await natsio.connect("nats://localhost")
js = nc.jetstream()

# Fast ingest needs allow_batched. Core's StreamConfig does not model the 2.14
# field yet, so it rides in `extra` (which round-trips untouched).
stream = await js.create_stream(
    StreamConfig(name="RAW", subjects=["raw.>"], allow_direct=True, extra={ALLOW_BATCHED: True})
)

# --- publish -------------------------------------------------------------
async with fast_publisher(js) as fp:
    for i in range(10_000):
        ack = await fp.add("raw.events", f"event {i}".encode())
        # ack.batch_sequence: this message's slot in the batch
        # ack.ack_sequence:   everything up to here is on disk (gap=fail mode)
    final = await fp.commit("raw.events", b"done")

print(final.stream, final.seq, final.size, final.batch_id)

# Leaving the block without committing sends an end-of-batch marker instead:
async with fast_publisher(js) as fp:
    await fp.add("raw.events", b"only this one")
# fp.ack is the batch ack; fp.is_closed is True

# Tuning and failure reporting. The handler runs on the batch's ack-reader
# task: keep it fast, and never call back into `fp` from it (that would wait
# for an ack only the reader can deliver — it raises `BatchReentrantUseError`;
# use `asyncio.create_task(fp.close())` if you must end the batch there).
def on_error(err: Exception) -> None:
    log.warning("fast batch: %s", err)

fp = fast_publisher(
    js,
    flow_control=FlowControl(flow=200, max_outstanding_acks=3, ack_timeout=10.0),
    continue_on_gap=True,      # tolerate holes instead of abandoning the batch
    error_handler=on_error,    # gaps, rejected messages, server-side abandon
)

# --- read ----------------------------------------------------------------
async for stored in get_batch(js, stream, 100, seq=1, subject="raw.events"):
    print(stored.seq, stored.subject, stored.payload)

async for stored in get_batch(js, "RAW", 10, start_time=an_hour_ago, max_bytes=64 * 1024):
    ...
```

## API

| Symbol | Purpose |
|---|---|
| `fast_publisher(js, *, flow_control=None, continue_on_gap=False, error_handler=None)` | Open a batch. No I/O, so `await` is optional. |
| `FastPublisher.add(subject, payload, **expectations) -> FastPubAck` | Publish (and persist) one message; stalls under back-pressure. |
| `FastPublisher.commit(subject, payload, **expectations) -> BatchAck` | Final message + end of batch. Valid as the first call. |
| `FastPublisher.close() -> BatchAck \| None` | End the batch without adding a message. Idempotent; `None` when nothing was added. |
| `FastPublisher` as `async with` | `commit` yourself, or the exit `close()`s the batch (and abandons it if the block raised). |
| `FastPublisher.is_closed` / `.batch_id` / `.size` / `.ack_sequence` / `.ack` | Introspection. |
| `FlowControl(flow=100, max_outstanding_acks=2, ack_timeout=None)` | Ack cadence, back-pressure window, total ack deadline. |
| `FastPubAck` | `batch_sequence`, `ack_sequence`. |
| `BatchAck` | `stream`, `seq`, `batch_id`/`batch`, `size`/`count`, `domain`, `val`. |
| `get_batch(js, stream, batch, *, seq=None, start_time=None, subject=None, max_bytes=None, timeout=None)` | Prepared batch read (`BatchGet`); iterate it, `await` optional. |
| `parse_ack_frame` / `build_reply_prefix` / `build_reply_subject` / `build_message_headers` | The wire contract, exported for tooling and tests. |

Per-message expectation keywords on `add`/`commit`: `headers`, `ttl`,
`expected_stream`, `expected_last_seq`, `expected_last_subject_seq`,
`expected_last_subject_seq_subject` — the same contract as core's
`JetStreamContext.publish`.

Errors (all subclass `natsio.jetstream.JetStreamError` via
`JetStreamBatchError`): `BatchClosedError`, `BatchAbandonedError`,
`BatchAckTimeoutError`, `ConcurrentBatchUseError`, `BatchReentrantUseError`,
`BatchGapError`, `BatchMessageError`, `InvalidBatchAckError`, `BatchGetError`,
`BatchGetUnsupportedError`, `BatchGetIncompleteError`, plus the four
server-reported `APIError` subclasses below.

## Wire contract

Pinned by the unit tests against frames captured from nats-server 2.14.3, and
cross-checked with orbit.go `jetstreamext/fastpublish.go` (`buildReplySubject`,
`ackMsgHandler`, `waitForStall`) and `getbatch.go`.

**A fast-ingest publish carries no batch headers at all.** Everything the
server needs is encoded in the reply subject:

```
<inbox>.<flow>.<gap>.<batch-seq>.<operation>.$FI
```

- `<inbox>` — the ack inbox. Its **last token is the batch id** (the server
  slices it out and echoes it back as the ack's `batch` field); over 64
  characters is rejected.
- `<flow>` — the ack cadence asked for. Fixed at the *initial* value for the
  whole batch even after the server lowers it (orbit.go pins this too).
- `<gap>` — `fail` (default: the server abandons the batch on a hole) or `ok`.
- `<operation>` — `0` start, `1` add, `2` commit-with-message, `3`
  end-of-batch commit, `4` ping.

Frames arriving on the ack inbox:

| Frame | Meaning |
|---|---|
| `{"type":"ack","seq":N,"msgs":M}` | Flow ack: persisted through batch sequence `N`, cadence is now `M`. |
| `{"type":"gap","last_seq":N,"seq":M}` | Messages `N`..`M-1` never arrived. |
| `{"type":"err","seq":N,"error":{...}}` | Message `N` was rejected. |
| `{"stream":S,"seq":N,"batch":B,"count":C}` | **Terminal**: the batch ack. `count` excludes the end-of-batch marker. |
| `{"error":{...},"stream":S,"seq":0}` | **Terminal**: the batch failed. |

A batch the server **abandons** (a rejected message, or a hole in `fail` mode)
is terminated with a frame of the *first* shape — a normal-looking ack. The only
tell is `count`: a batch we ended ourselves is counted whole (`count` equals
every batch sequence we sent, one less for an end-of-batch marker, in both gap
modes), an abandoned one stops short. Anything short raises `BatchAbandonedError`
from the parked `commit()`/`close()` — data loss is never returned as an ack.

A batch read is `$JS.API.DIRECT.GET.<stream>` with
`{"seq"|"start_time", "next_by_subj", "batch", "max_bytes"}`, answered by one
message per hit (carrying `Nats-Stream`, `Nats-Subject`, `Nats-Sequence`,
`Nats-Time-Stamp`, `Nats-Num-Pending`) and terminated by a `204 EOB` status
frame; a request that matches nothing gets a single `404`.

### Server error codes (a divergence worth knowing)

The four fast-ingest `err_code`s **nats-server 2.14.3 actually emits** are
registered into core's registry on import, so they arrive typed:

| Code | Description | Type |
|---|---|---|
| 10205 | `batch publish is disabled` | `FastBatchNotEnabledError` |
| 10206 | `batch publish pattern is invalid` | `FastBatchInvalidPatternError` |
| 10207 | `batch publish ID is invalid` | `FastBatchInvalidIdError` |
| 10208 | `batch publish ID unknown` | `FastBatchUnknownIdError` |

orbit.go's `jetstreamext/errors.go` publishes these as **10203-10206**, which
cannot be right for 2.14 GA: nats.go's own registry already uses 10203 for
`JSErrCodeScheduleSourceInvalid` and 10204 for `JSErrCodeConsumerInvalidReset`.
We follow the server, not the constant.

## Scope limits

- **Atomic batch publish is not implemented.** `PublishMsgBatch` /
  `BatchPublisher` (the `allow_atomic`, `Nats-Batch-Id` / `Nats-Batch-Sequence`
  / `Nats-Batch-Commit` header protocol) is a different feature with different
  guarantees; this package is the fast-ingest half only.
- **`GetLastMsgsFor` is not reimplemented** — core already has
  `Stream.get_last_msgs_for` (multi_last + 204 EOB).
- **One producer per batch.** A `FastPublisher` is not safe for concurrent use
  (the oracle says so, and the sequence numbering makes it structural). Here the
  dangerous window is *detected*: a task that calls in while another is **parked**
  inside `add`/`commit`/`close` raises `ConcurrentBatchUseError`. Two calls that
  never suspend simply serialise — the guard is a diagnostic, not a lock.
- **The `error_handler` may not drive the publisher.** It runs on the ack-reader
  task, so ending the batch from inside it would wait for an ack that only the
  blocked reader can deliver; that raises `BatchReentrantUseError` immediately
  instead of hanging until `ack_timeout`. Spawn a task if you need it.
- **`close()` on an untouched publisher returns `None`**, where orbit.go returns
  `ErrEmptyBatch`. Nothing was opened, so there is nothing to end and nothing to
  fail — same reasoning as the empty batch read below. (After a *failed or
  cancelled* `add`, by contrast, `close()` re-raises the cause: a batch that
  persisted messages and then aborted is not an empty batch.)
- **Always `close()` (or use `async with`).** A `FastPublisher` dropped without
  closing keeps its ack-reader task and ack subscription alive until the client
  itself closes — bounded, not a leak, but the server-side batch also stays
  inflight (no EOB) until it expires. The context-manager form frees both at
  block exit; prefer it.
- **A batch read that matches nothing yields nothing.** orbit.go surfaces an
  `ErrNoMessages` through the iterator; an empty result is not an error here.
  A read that ends *without* the server's EOB is a different story and raises
  `BatchGetIncompleteError`.
- **`ack_sequence` under `continue_on_gap=True` guarantees nothing** about the
  messages below it — that is the deal you make by tolerating gaps.
- `Nats-Msg-Id` / `Nats-Expected-Last-Msg-Id` are not offered as per-message
  options (orbit.go documents them as unsupported inside a batch), though
  2.14.3 does not reject a `Nats-Msg-Id` passed through `headers=`.
- Passing a stream **name** to `get_batch` costs one `STREAM.INFO` round-trip to
  verify `allow_direct`; pass a `Stream` handle to skip it.
- No client-side cap on batch length: fast ingest has no equivalent of the
  atomic batch's 1000-message server limit, and 20k-message batches are part of
  the local stress runs.

## Example

A runnable script is at [`examples/basic.py`](https://github.com/corruptmane/natsio/blob/main/extensions/natsio-jetstream-batch/examples/basic.py) — start a server with `just server`, then:

```bash
python extensions/natsio-jetstream-batch/examples/basic.py
```
