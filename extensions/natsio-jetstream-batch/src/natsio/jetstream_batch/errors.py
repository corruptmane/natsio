"""Error types for fast-ingest batch publishing and batch reads.

Everything raised by this package descends from `JetStreamBatchError`, itself a
`natsio.jetstream.JetStreamError`. The four server-reported fast-ingest
failures are additionally `natsio.jetstream.APIError` subclasses and are
registered into the core ``err_code`` registry on import, so a fast-batch
rejection arrives as its own type rather than a bare ``APIError``.

The registered codes are the ones **nats-server 2.14.3 actually emits**
(10205-10208, probe-verified). They are NOT the constants published in
orbit.go's ``jetstreamext/errors.go`` (10203-10206) — those collide with
``JSErrCodeScheduleSourceInvalid`` (10203) and ``JSErrCodeConsumerInvalidReset``
(10204) in nats.go's own registry, so they appear to predate the 2.14 GA
numbering. See the README's wire-contract section.
"""

import logging

from natsio.jetstream.errors import APIError, JetStreamError, error_for, register_error

__all__ = [
    "BatchAbandonedError",
    "BatchAckTimeoutError",
    "BatchClosedError",
    "BatchGapError",
    "BatchGetError",
    "BatchGetIncompleteError",
    "BatchGetUnsupportedError",
    "BatchMessageError",
    "BatchReentrantUseError",
    "ConcurrentBatchUseError",
    "FastBatchInvalidIdError",
    "FastBatchInvalidPatternError",
    "FastBatchNotEnabledError",
    "FastBatchUnknownIdError",
    "InvalidBatchAckError",
    "JetStreamBatchError",
]

log = logging.getLogger("natsio.jetstream_batch")


class JetStreamBatchError(JetStreamError):
    """Root for every failure raised by ``natsio.jetstream_batch``."""


class BatchClosedError(JetStreamBatchError):
    """The batch is over — it was committed, closed, or ended by the server."""


class BatchAbandonedError(JetStreamBatchError):
    """The server ended the batch before we asked it to.

    Happens when a message in the batch is rejected (a publish expectation
    fails) or when a sequence gap is detected in the default ``gap=fail`` mode.
    Messages already persisted stay persisted; the terminating ack reports how
    far the batch got.
    """


class BatchAckTimeoutError(JetStreamBatchError, TimeoutError):
    """No ack arrived within ``FlowControl.ack_timeout``.

    Also a `TimeoutError`, so a plain ``except TimeoutError`` works. The
    deadline is a ceiling on the *total* wait, not on each individual ack:
    pings are sent at a third of it to recover acks lost in transit.
    """


class ConcurrentBatchUseError(JetStreamBatchError):
    """Two tasks used one `FastPublisher` at the same time.

    A fast-ingest batch is an ordered, sequence-numbered conversation with the
    server: interleaving two producers renumbers messages and corrupts the
    batch. Mirrors the oracle's "not safe for concurrent use" contract, but
    detected and raised instead of left to chance — for the window that matters,
    which is one caller parked inside `add`/`commit`/`close` while another calls
    in. Two calls that never suspend simply serialise.
    """


class BatchReentrantUseError(JetStreamBatchError):
    """The publisher was driven from its own ``error_handler``.

    The handler runs on the batch's ack-reader task, so `FastPublisher.add`,
    `commit` and `close` called from inside it would park waiting for an ack
    that only the — now blocked — reader could deliver. Raised instead of
    deadlocking until ``ack_timeout``; dispatch the call to another task
    (``asyncio.create_task(fp.close())``) if you need it.
    """


class BatchGapError(JetStreamBatchError):
    """The server saw a hole in the batch sequence it received.

    Reported through the publisher's ``error_handler``. In the default
    ``continue_on_gap=False`` mode the server also abandons the batch; with
    ``continue_on_gap=True`` the batch continues and this is informational —
    but then no ack sequence implies that everything below it was persisted.
    """

    def __init__(self, expected_last_sequence: int, sequence: int) -> None:
        super().__init__(f"gap in fast batch: expected batch sequence {expected_last_sequence}, got {sequence}")
        self.expected_last_sequence = expected_last_sequence
        self.sequence = sequence


class BatchMessageError(JetStreamBatchError):
    """One message of the batch was rejected by the server.

    Reported through the publisher's ``error_handler``. ``cause`` is the
    server's `natsio.jetstream.APIError` (e.g. a wrong-last-sequence
    expectation), ``sequence`` the batch sequence that triggered it. There is no
    guarantee about which messages below ``sequence`` were persisted.
    """

    def __init__(self, sequence: int, cause: APIError) -> None:
        super().__init__(f"fast batch message {sequence} rejected: {cause}")
        self.sequence = sequence
        self.cause = cause


class InvalidBatchAckError(JetStreamBatchError):
    """A frame on the batch's ack inbox was not a valid batch response."""


class BatchGetError(JetStreamBatchError):
    """A batch direct-get request failed."""


class BatchGetUnsupportedError(BatchGetError):
    """The server answered a batch get without the ``Nats-Num-Pending`` header.

    Batch direct get is a 2.11+ feature; a server that does not implement it
    answers with a single plain message instead of a batch, which would
    otherwise look like a one-element result.
    """


class BatchGetIncompleteError(BatchGetError):
    """The batch ended without the server's ``204 EOB`` terminator.

    The request timed out (or the connection dropped) mid-batch, so the
    messages delivered so far are a silent prefix of the answer — raised rather
    than returned, because a truncated read that looks complete is exactly the
    failure this client refuses to have.
    """


# -- server-reported fast-ingest failures (nats-server 2.14.3 err_codes) ------


class FastBatchNotEnabledError(APIError, JetStreamBatchError):
    """The stream was not created with ``allow_batched`` (err_code 10205).

    ``allow_atomic`` enables the *atomic* batch publish, not fast ingest —
    probe-verified: a fast batch against an atomic-only stream is rejected with
    this same code.
    """


class FastBatchInvalidPatternError(APIError, JetStreamBatchError):
    """The reply subject did not parse as a fast-ingest pattern (err_code 10206).

    The batch must open with the ``start`` operation, and every element of
    ``<flow>.<gap>.<seq>.<op>.$FI`` must be well-formed.
    """


class FastBatchInvalidIdError(APIError, JetStreamBatchError):
    """The batch id token is unusable — over 64 characters (err_code 10207)."""


class FastBatchUnknownIdError(APIError, JetStreamBatchError):
    """The server has no batch with this id (err_code 10208).

    Typically a batch that was already committed or abandoned: the messages
    that followed have nowhere to go.
    """


_FAST_BATCH_ERRORS: dict[int, type[APIError]] = {
    10205: FastBatchNotEnabledError,
    10206: FastBatchInvalidPatternError,
    10207: FastBatchInvalidIdError,
    10208: FastBatchUnknownIdError,
}


def _register_fast_batch_errors() -> None:
    """Bind our err_codes into core's shared registry (import-time).

    The registry is a process-global that core owns, so importing this package
    must not silently take a code out from under it: anything already bound to a
    different class is reported at WARNING level before being overridden, rather
    than changing another module's error type without a trace.
    """
    for err_code, exc_type in _FAST_BATCH_ERRORS.items():
        bound = error_for(err_code)
        if bound is not APIError and bound is not exc_type:
            log.warning(
                "err_code %d is already registered as %s; natsio.jetstream_batch is overriding it with %s",
                err_code,
                bound.__name__,
                exc_type.__name__,
            )
        register_error(err_code, exc_type)


# Registration happens on import (the documented
# `natsio.jetstream.errors.register_error` extension hook), so these arrive typed
# from `APIError.from_error` wherever a JetStream response is parsed.
_register_fast_batch_errors()
