"""JetStream fast-ingest batch publishing and batch reads (nats-server 2.14+).

Two halves of the 2.14 batching story that core natsio does not carry:

**Fast-ingest publishing.** A batch whose messages are persisted *as they are
added* — no atomicity, all throughput. Instead of a PubAck per message the
server acks every N, the client stalls when too many ack windows are
outstanding, and the commit returns the ack for the last stored message. Needs a
stream created with ``allow_batched``:

    js = nc.jetstream()
    await js.create_stream(StreamConfig(name="RAW", subjects=["raw.>"], extra={ALLOW_BATCHED: True}))

    async with fast_publisher(js) as fp:
        for chunk in chunks:
            await fp.add("raw.events", chunk)
        ack = await fp.commit("raw.events", last_chunk)
    print(ack.stream, ack.seq, ack.size)

**Batch reads.** Many stored messages from one Direct Get, starting at a
sequence, a time, or the lowest match:

    async for stored in get_batch(js, "RAW", 100, seq=1, subject="raw.events"):
        print(stored.seq, stored.payload)

(The complementary "last message per subject" read is already core's
`natsio.jetstream.Stream.get_last_msgs_for`.)

Mirrors orbit.go's ``jetstreamext``; the wire contract is pinned against
nats-server 2.14.3. The atomic (``allow_atomic``) batch publish is deliberately
out of scope — see the README.
"""

from natsio.jetstream_batch.entities import (
    ALLOW_BATCHED,
    DEFAULT_FLOW,
    DEFAULT_MAX_OUTSTANDING_ACKS,
    FAST_INGEST_SUFFIX,
    GAP_FAIL,
    GAP_OK,
    MAX_BATCH_ID_LENGTH,
    MAX_FLOW,
    OP_ADD,
    OP_COMMIT,
    OP_COMMIT_EOB,
    OP_PING,
    OP_START,
    BatchAck,
    BatchFrame,
    FastPubAck,
    FlowAck,
    FlowControl,
    GapReport,
    SequenceError,
    TerminalError,
    build_message_headers,
    build_reply_prefix,
    build_reply_subject,
    parse_ack_frame,
)
from natsio.jetstream_batch.errors import (
    BatchAbandonedError,
    BatchAckTimeoutError,
    BatchClosedError,
    BatchGapError,
    BatchGetError,
    BatchGetIncompleteError,
    BatchGetUnsupportedError,
    BatchMessageError,
    BatchReentrantUseError,
    ConcurrentBatchUseError,
    FastBatchInvalidIdError,
    FastBatchInvalidPatternError,
    FastBatchNotEnabledError,
    FastBatchUnknownIdError,
    InvalidBatchAckError,
    JetStreamBatchError,
)
from natsio.jetstream_batch.fastpublish import ErrorHandler, FastPublisher, fast_publisher
from natsio.jetstream_batch.getbatch import BatchGet, get_batch

__all__ = [
    "ALLOW_BATCHED",
    "DEFAULT_FLOW",
    "DEFAULT_MAX_OUTSTANDING_ACKS",
    "FAST_INGEST_SUFFIX",
    "GAP_FAIL",
    "GAP_OK",
    "MAX_BATCH_ID_LENGTH",
    "MAX_FLOW",
    "OP_ADD",
    "OP_COMMIT",
    "OP_COMMIT_EOB",
    "OP_PING",
    "OP_START",
    "BatchAbandonedError",
    "BatchAck",
    "BatchAckTimeoutError",
    "BatchClosedError",
    "BatchFrame",
    "BatchGapError",
    "BatchGet",
    "BatchGetError",
    "BatchGetIncompleteError",
    "BatchGetUnsupportedError",
    "BatchMessageError",
    "BatchReentrantUseError",
    "ConcurrentBatchUseError",
    "ErrorHandler",
    "FastBatchInvalidIdError",
    "FastBatchInvalidPatternError",
    "FastBatchNotEnabledError",
    "FastBatchUnknownIdError",
    "FastPubAck",
    "FastPublisher",
    "FlowAck",
    "FlowControl",
    "GapReport",
    "InvalidBatchAckError",
    "JetStreamBatchError",
    "SequenceError",
    "TerminalError",
    "build_message_headers",
    "build_reply_prefix",
    "build_reply_subject",
    "fast_publisher",
    "get_batch",
    "parse_ack_frame",
]
