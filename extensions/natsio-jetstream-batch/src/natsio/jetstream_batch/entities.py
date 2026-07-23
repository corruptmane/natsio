"""The fast-ingest wire contract: reply-subject encoding, frames, acks.

Everything in this module is pinned to what nats-server 2.14.3 emits (probed
frame by frame) and cross-checked against orbit.go's
``jetstreamext/fastpublish.go``. The constants are plain ``str``/``int`` —
like `natsio.jetstream.headers` — because they go straight onto the wire.
"""

import json
from dataclasses import dataclass
from typing import Any, Final

from natsio._internal.jsonmodel import JsonModel
from natsio._internal.protocol import Headers, HeadersInput
from natsio.errors import ConfigError
from natsio.jetstream import headers as js_headers
from natsio.jetstream.errors import APIError

from .errors import InvalidBatchAckError

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
    "BatchAck",
    "BatchFrame",
    "FastPubAck",
    "FlowAck",
    "FlowControl",
    "GapReport",
    "SequenceError",
    "TerminalError",
    "build_message_headers",
    "build_reply_prefix",
    "build_reply_subject",
    "parse_ack_frame",
]

# -- reply-subject encoding --------------------------------------------------
# A fast-ingest publish carries no batch headers at all: everything the server
# needs is encoded in the reply subject
#
#     <inbox>.<flow>.<gap>.<batch-seq>.<operation>.$FI
#
# and the batch id is the LAST token of ``<inbox>`` (probe: the server echoes it
# back as the commit ack's ``batch`` field). This is orbit.go's
# ``buildReplySubject`` byte for byte.

FAST_INGEST_SUFFIX: Final = "$FI"
"""Final token marking a reply subject as a fast-ingest control subject."""

OP_START: Final = 0
"""First message of a batch — opens it."""
OP_ADD: Final = 1
"""Any subsequent message."""
OP_COMMIT: Final = 2
"""Final message plus commit; the server answers with the batch ack."""
OP_COMMIT_EOB: Final = 3
"""End-of-batch commit that adds no message (an empty publish)."""
OP_PING: Final = 4
"""Re-request the latest flow ack; reuses the highest sequence already sent."""

GAP_FAIL: Final = "fail"
"""Default gap mode: the server abandons the batch when it detects a hole."""
GAP_OK: Final = "ok"
"""Tolerant gap mode: the batch continues, the client is told about the hole."""

DEFAULT_FLOW: Final = 100
"""Initial ack cadence, in messages (orbit.go ``defaultFastFlow``)."""
DEFAULT_MAX_OUTSTANDING_ACKS: Final = 2
"""Ack windows in flight before the publisher stalls (orbit.go default)."""
MAX_FLOW: Final = 65535
"""``msgs`` is a ``uint16`` on the wire."""
MAX_BATCH_ID_LENGTH: Final = 64
"""The server's limit on the batch id token: longer ids are rejected with
err_code 10207. Documentation, not a validation constant — the id is always a
22-character nuid, so this client cannot generate one that violates it."""

ALLOW_BATCHED: Final = "allow_batched"
"""Stream config field enabling fast ingest (2.14+). Core's `StreamConfig`
does not model it yet, so pass it through ``extra``:
``StreamConfig(name=..., extra={ALLOW_BATCHED: True})``."""


def build_reply_prefix(inbox: str, flow: int, *, continue_on_gap: bool) -> str:
    """The fixed ``<inbox>.<flow>.<gap>.`` head of every reply subject.

    Built once per publisher and never rebuilt: the server may lower the ack
    cadence mid-batch, but the subject keeps advertising the *initial* flow
    (orbit.go pins this with a dedicated regression test — rewriting the prefix
    would make the client forget the cadence it asked for).
    """
    return f"{inbox}.{flow}.{GAP_OK if continue_on_gap else GAP_FAIL}."


def build_reply_subject(prefix: str, sequence: int, operation: int) -> str:
    """Complete a reply subject from `build_reply_prefix` output."""
    return f"{prefix}{sequence}.{operation}.{FAST_INGEST_SUFFIX}"


@dataclass(frozen=True, slots=True)
class FlowControl:
    """Ack cadence and back-pressure for a fast-ingest batch.

    - ``flow`` — how often, in messages, the server should ack. It may ack
      *more* often than asked (and says so in each ack), never less.
    - ``max_outstanding_acks`` — how many ack windows may be in flight before
      `FastPublisher.add` stalls waiting for the server to catch up.
    - ``ack_timeout`` — ceiling on the *total* wait for an ack, in seconds.
      Defaults to the JetStream context's timeout. Pings go out at a third of
      it to recover acks lost in transit.
    """

    flow: int = DEFAULT_FLOW
    max_outstanding_acks: int = DEFAULT_MAX_OUTSTANDING_ACKS
    ack_timeout: float | None = None

    def __post_init__(self) -> None:
        if not 1 <= self.flow <= MAX_FLOW:
            raise ConfigError(f"flow must be between 1 and {MAX_FLOW}, got {self.flow}")
        if not 1 <= self.max_outstanding_acks <= MAX_FLOW:
            raise ConfigError(f"max_outstanding_acks must be between 1 and {MAX_FLOW}, got {self.max_outstanding_acks}")
        if self.ack_timeout is not None and self.ack_timeout <= 0:
            raise ConfigError(f"ack_timeout must be > 0, got {self.ack_timeout}")


@dataclass(frozen=True, slots=True)
class FastPubAck:
    """What `FastPublisher.add` knows the moment it returns.

    ``batch_sequence`` is this message's position in the batch. ``ack_sequence``
    is the highest batch sequence the server has confirmed so far — with the
    default ``gap=fail`` mode every message up to and including it is persisted,
    so buffers held for retry below that point can be released. Under
    ``continue_on_gap=True`` it carries no such implication.
    """

    batch_sequence: int
    ack_sequence: int


@dataclass(slots=True, kw_only=True)
class BatchAck(JsonModel):
    """The server's answer to a commit — the end of the batch.

    Field names are the wire names (``batch``/``count``); `batch_id` and `size`
    are the readable aliases.
    """

    stream: str = ""
    seq: int = 0
    domain: str | None = None
    val: str | None = None
    batch: str | None = None
    count: int | None = None

    @property
    def batch_id(self) -> str | None:
        """The batch this ack closes (the last token of our ack inbox)."""
        return self.batch

    @property
    def size(self) -> int:
        """Messages the server counted in the batch.

        Under ``continue_on_gap=True`` this is the last batch *sequence* the
        server processed, which is larger than the number of messages actually
        stored when a gap was tolerated.
        """
        return self.count or 0


@dataclass(frozen=True, slots=True)
class FlowAck:
    """``{"type":"ack"}`` — periodic flow control.

    ``sequence`` is the highest batch sequence persisted (0 on the ack that
    opens the batch, before anything has been written); ``messages`` is the ack
    cadence the server intends to use from now on.
    """

    sequence: int
    messages: int


@dataclass(frozen=True, slots=True)
class GapReport:
    """``{"type":"gap"}`` — the server received a hole in the batch sequence.

    Messages from ``expected_last_sequence`` up to (but excluding) ``sequence``
    never arrived. Purely informational: it may itself be lost in transit.
    """

    expected_last_sequence: int
    sequence: int


@dataclass(frozen=True, slots=True)
class SequenceError:
    """``{"type":"err"}`` — one message of the batch was rejected."""

    sequence: int
    error: APIError


@dataclass(frozen=True, slots=True)
class TerminalError:
    """A typeless frame carrying an ``error`` — the batch is over and failed."""

    error: APIError


type BatchFrame = FlowAck | GapReport | SequenceError | BatchAck | TerminalError
"""Anything the server can send to a batch's ack inbox."""

_TYPE_ACK: Final = "ack"
_TYPE_GAP: Final = "gap"
_TYPE_ERR: Final = "err"


def parse_ack_frame(data: bytes) -> BatchFrame:
    """Classify one frame from the batch ack inbox.

    Dispatch is on the ``type`` field: ``ack``/``gap``/``err`` are in-batch
    control frames, and a frame *without* a type is terminal — either the batch
    ack or the error that ended the batch. Anything else is a
    `InvalidBatchAckError`; the client never guesses at a frame it cannot name.
    """
    try:
        payload: Any = json.loads(data)
    except (ValueError, UnicodeDecodeError) as exc:
        raise InvalidBatchAckError(f"batch ack is not valid JSON: {data!r}") from exc
    if not isinstance(payload, dict):
        raise InvalidBatchAckError(f"batch ack is not a JSON object: {data!r}")

    kind = payload.get("type")
    if kind is None:
        if "error" in payload:
            return TerminalError(error=_api_error(payload["error"], data))
        ack = BatchAck.from_wire(payload)
        if not ack.stream:
            raise InvalidBatchAckError(f"batch ack has no stream: {data!r}")
        return ack
    try:
        if kind == _TYPE_ACK:
            return FlowAck(sequence=int(payload["seq"]), messages=int(payload["msgs"]))
        if kind == _TYPE_GAP:
            return GapReport(expected_last_sequence=int(payload["last_seq"]), sequence=int(payload["seq"]))
        if kind == _TYPE_ERR:
            return SequenceError(sequence=int(payload["seq"]), error=_api_error(payload["error"], data))
    except (KeyError, TypeError, ValueError) as exc:
        raise InvalidBatchAckError(f"malformed {kind!r} frame: {data!r}") from exc
    raise InvalidBatchAckError(f"unknown batch frame type {kind!r}: {data!r}")


def _api_error(error: Any, data: bytes) -> APIError:
    """Build an `APIError` from a wire ``error`` field, defending its shape.

    `APIError.from_error` calls ``.get`` on the value, so a non-object ``error``
    (``"error":"boom"`` / ``"error":null`` — both legal JSON any client in the
    account can put on the ack inbox) would raise a bare `AttributeError` that
    escapes the reader and kills it silently. Hostile input must be a typed
    error (invariant 5), so it becomes an `InvalidBatchAckError` here.
    """
    if not isinstance(error, dict):
        raise InvalidBatchAckError(f"batch ack error is not a JSON object: {data!r}")
    return APIError.from_error(error)


def build_message_headers(
    *,
    headers: HeadersInput | None = None,
    ttl: js_headers.TTLInput | None = None,
    expected_stream: str | None = None,
    expected_last_seq: int | None = None,
    expected_last_subject_seq: int | None = None,
    expected_last_subject_seq_subject: str | None = None,
) -> HeadersInput | None:
    """Merge the per-message publish expectations onto ``headers``.

    The same ``Nats-Expected-*`` / ``Nats-TTL`` contract core's
    `JetStreamContext.publish` speaks, minus the two headers a batch cannot
    carry: ``Nats-Msg-Id`` and ``Nats-Expected-Last-Msg-Id``.
    """
    extra: dict[str, str] = {}
    if ttl is not None:
        extra[js_headers.TTL] = js_headers.encode_ttl(ttl)
    if expected_stream is not None:
        extra[js_headers.EXPECTED_STREAM] = expected_stream
    if expected_last_seq is not None:
        extra[js_headers.EXPECTED_LAST_SEQUENCE] = str(expected_last_seq)
    if expected_last_subject_seq is not None:
        extra[js_headers.EXPECTED_LAST_SUBJECT_SEQUENCE] = str(expected_last_subject_seq)
    if expected_last_subject_seq_subject is not None:
        if not expected_last_subject_seq_subject:
            raise ConfigError("expected_last_subject_seq_subject cannot be empty")
        if expected_last_subject_seq is None:
            raise ConfigError("expected_last_subject_seq_subject requires expected_last_subject_seq")
        extra[js_headers.EXPECTED_LAST_SUBJECT_SEQUENCE_SUBJECT] = expected_last_subject_seq_subject
    if not extra:
        return headers
    if headers is None:
        return extra
    merged = Headers(headers)
    for key, value in extra.items():
        merged.set(key, value)
    return merged
