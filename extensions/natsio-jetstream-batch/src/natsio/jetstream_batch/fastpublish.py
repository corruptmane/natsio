"""FastPublisher: 2.14 fast-ingest batch publishing.

Fast ingest is the high-throughput half of JetStream batching. Unlike an atomic
batch, which holds everything back until the commit, messages here are
**persisted as they are added**; the commit only marks the end of the batch and
returns the ack for the last message the server stored. The cost of that speed
is that acks are periodic rather than per-message, so the publisher runs a small
flow-control protocol: the server acks every ``flow`` messages, the client
stalls when too many ack windows are outstanding, and pings recover acks lost in
transit.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Generator
from contextlib import contextmanager, suppress
from types import TracebackType
from typing import Any, Self

from natsio._internal.nuid import next_nuid
from natsio._internal.protocol import HeadersInput, StatusCode
from natsio.errors import ConnectionClosedError
from natsio.jetstream import headers as js_headers
from natsio.jetstream.context import JetStreamContext
from natsio.jetstream.errors import NoStreamResponseError
from natsio.message import Msg
from natsio.subscription import Subscription

from .entities import (
    OP_ADD,
    OP_COMMIT,
    OP_COMMIT_EOB,
    OP_PING,
    OP_START,
    BatchAck,
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
from .errors import (
    BatchAbandonedError,
    BatchAckTimeoutError,
    BatchClosedError,
    BatchGapError,
    BatchMessageError,
    BatchReentrantUseError,
    ConcurrentBatchUseError,
    InvalidBatchAckError,
)

__all__ = ["ErrorHandler", "FastPublisher", "fast_publisher"]

log = logging.getLogger("natsio.jetstream_batch")

type ErrorHandler = Callable[[Exception], Awaitable[None] | None]
"""Sink for the failures nobody is waiting on: gaps, rejected messages, and a
batch the server ended on its own. Called from the ack-reader task, so keep it
fast — while it runs, no ack is being processed. May be async.

It must **not** call back into the publisher: `FastPublisher.add`, `commit` and
`close` all need acks that only the (blocked) reader task can deliver, so the
call would deadlock. Doing it raises `BatchReentrantUseError`; dispatch the work
to another task (``asyncio.create_task(fp.close())``) instead."""

# Pings go out at a third of the ack timeout, so a lost ack costs at most that
# much progress instead of the whole deadline (orbit.go newPingTimers).
_PING_DIVISOR = 3


class FastPublisher:
    """One fast-ingest batch.

    Create it with `fast_publisher`, feed it with `add`, and end it with either
    `commit` (adds a final message and returns its ack) or `close` (ends the
    batch without adding anything). As an async context manager it closes
    itself on the way out:

        async with fast_publisher(js) as fp:
            for i in range(1000):
                await fp.add("events.raw", payload)
            ack = await fp.commit("events.raw", last_payload)

    **Not safe for concurrent use.** A second task that calls
    `add`/`commit`/`close` while another one is *parked* inside such a call gets
    a `ConcurrentBatchUseError` rather than a silently renumbered batch. Two
    calls that never suspend simply serialise, so the guard is not a lock — it
    catches exactly the window in which interleaving could corrupt the batch.
    """

    __slots__ = (
        "_abandon_cause",
        "_ack",
        "_ack_sequence",
        "_ack_timeout",
        "_batch_id",
        "_busy",
        "_client",
        "_closed",
        "_commit",
        "_continue_on_gap",
        "_error_handler",
        "_expected_count",
        "_failure",
        "_first_ack",
        "_flow",
        "_handler_task",
        "_inbox",
        "_max_outstanding_acks",
        "_reader",
        "_reply_prefix",
        "_sequence",
        "_stall",
        "_sub",
        "_subject",
        "_terminated",
    )

    def __init__(
        self,
        js: JetStreamContext,
        *,
        flow_control: FlowControl | None = None,
        continue_on_gap: bool = False,
        error_handler: ErrorHandler | None = None,
    ) -> None:
        control = flow_control if flow_control is not None else FlowControl()
        self._client = js.client
        self._flow = control.flow
        self._max_outstanding_acks = control.max_outstanding_acks
        self._ack_timeout = control.ack_timeout if control.ack_timeout is not None else js.timeout
        self._continue_on_gap = continue_on_gap
        self._error_handler = error_handler
        # The batch id IS the last token of the ack inbox: the server slices it
        # out of the reply subject and echoes it in the commit ack.
        self._batch_id = next_nuid()
        self._inbox = f"{js.client.inbox_prefix}._fi.{self._batch_id}"
        self._reply_prefix = build_reply_prefix(self._inbox, control.flow, continue_on_gap=continue_on_gap)
        self._sequence = 0
        self._ack_sequence = 0
        self._subject = ""
        self._closed = False
        self._terminated = False
        self._busy = False
        self._ack: BatchAck | None = None
        self._failure: Exception | None = None
        # How many messages the terminal ack must report for the batch we asked
        # to end to have been stored whole; None until we ask (see _finish).
        self._expected_count: int | None = None
        self._abandon_cause: Exception | None = None
        self._sub: Subscription | None = None
        self._reader: asyncio.Task[None] | None = None
        self._handler_task: asyncio.Task[Any] | None = None
        self._first_ack: asyncio.Future[FlowAck] | None = None
        self._commit: asyncio.Future[BatchAck] | None = None
        self._stall: asyncio.Future[None] | None = None

    # -- introspection -------------------------------------------------------

    @property
    def batch_id(self) -> str:
        """The batch id the server will echo in the commit ack."""
        return self._batch_id

    @property
    def is_closed(self) -> bool:
        """True once the batch has been committed, closed, or ended by the server."""
        return self._closed

    @property
    def size(self) -> int:
        """Batch sequences issued so far (messages sent, commit included)."""
        return self._sequence

    @property
    def ack_sequence(self) -> int:
        """Highest batch sequence the server has acknowledged."""
        return self._ack_sequence

    @property
    def ack(self) -> BatchAck | None:
        """The terminating ack, once the batch has ended.

        Set for a batch the server abandoned too — there it reports how far the
        batch got, which is why the abandon is raised rather than returned.
        """
        return self._ack

    def __repr__(self) -> str:
        return f"FastPublisher(batch_id={self._batch_id!r}, size={self._sequence}, closed={self._closed})"

    # -- lifecycle -----------------------------------------------------------

    def __await__(self) -> Generator[None, None, Self]:
        """``await`` is optional and completes immediately: creating a publisher
        does no I/O (the ack inbox is subscribed on the first message)."""
        return self
        yield  # unreachable: makes this a generator that never suspends

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc is not None:
            await self._abandon()
            return
        await self.close()

    # -- publishing ----------------------------------------------------------

    async def add(
        self,
        subject: str,
        payload: bytes | str = b"",
        *,
        headers: HeadersInput | None = None,
        ttl: js_headers.TTLInput | None = None,
        expected_stream: str | None = None,
        expected_last_seq: int | None = None,
        expected_last_subject_seq: int | None = None,
        expected_last_subject_seq_subject: str | None = None,
    ) -> FastPubAck:
        """Add one message to the batch. It is published — and persisted — now.

        The first call opens the batch and waits for the server's opening ack,
        so a stream that cannot take the batch fails here rather than 999
        messages later. Later calls return as soon as the frame is buffered,
        unless ``flow * max_outstanding_acks`` messages are already unacked — in
        which case this stalls until the server catches up, pinging along the
        way, and raises `BatchAckTimeoutError` if it never does.

        The publish-expectation keywords mirror
        `natsio.jetstream.JetStreamContext.publish`.
        """
        with self._exclusive():
            self._check_open()
            merged = build_message_headers(
                headers=headers,
                ttl=ttl,
                expected_stream=expected_stream,
                expected_last_seq=expected_last_seq,
                expected_last_subject_seq=expected_last_subject_seq,
                expected_last_subject_seq_subject=expected_last_subject_seq_subject,
            )
            self._ensure_started()
            first = self._sequence == 0
            sequence = self._sequence = self._sequence + 1
            self._subject = subject
            try:
                await self._publish(subject, payload, sequence, OP_START if first else OP_ADD, merged)
                if first:
                    opening = await self._await_first_ack(sequence)
                    return FastPubAck(batch_sequence=sequence, ack_sequence=opening.sequence)
                if self._needs_stall():
                    await self._wait_for_stall()
            except BaseException as exc:
                # A frame that never landed renumbers everything after it: the
                # batch cannot be repaired, so end it here rather than let the
                # server discover the hole later. The cause is recorded so a
                # later close() re-raises it instead of returning None.
                await self._fail_batch(exc)
                raise
            return FastPubAck(batch_sequence=sequence, ack_sequence=self._ack_sequence)

    async def commit(
        self,
        subject: str,
        payload: bytes | str = b"",
        *,
        headers: HeadersInput | None = None,
        ttl: js_headers.TTLInput | None = None,
        expected_stream: str | None = None,
        expected_last_seq: int | None = None,
        expected_last_subject_seq: int | None = None,
        expected_last_subject_seq_subject: str | None = None,
    ) -> BatchAck:
        """Add a final message and end the batch, returning the server's ack.

        The ack names the stream, the stream sequence of that final message, the
        batch id, and how many messages the server counted. Valid as the very
        first call — a commit with no preceding `add` publishes a one-message
        batch.

        Raises `BatchAbandonedError` if the server ended the batch behind our
        back (a rejected message, or a hole in the default gap mode): that
        terminator looks like an ordinary ack but counts fewer messages than we
        sent, and a batch that lost messages must not return one.
        """
        with self._exclusive():
            self._check_open()
            merged = build_message_headers(
                headers=headers,
                ttl=ttl,
                expected_stream=expected_stream,
                expected_last_seq=expected_last_seq,
                expected_last_subject_seq=expected_last_subject_seq,
                expected_last_subject_seq_subject=expected_last_subject_seq_subject,
            )
            self._ensure_started()
            self._subject = subject
            return await self._end_batch(subject, payload, OP_COMMIT, merged)

    async def close(self) -> BatchAck | None:
        """End the batch without adding a message; return the ack.

        Idempotent, and safe on an untouched publisher: with nothing added there
        is no batch to end, so it returns ``None`` (the server rejects an
        end-of-batch marker that opens a batch). If the *server* ended the batch
        early, the failure that ended it is raised here — a batch that quietly
        stopped persisting is exactly what must not pass silently.
        """
        with self._exclusive():
            if self._closed:
                await self._teardown()
                if self._failure is not None:
                    raise self._failure
                return self._ack
            if self._sequence == 0:
                self._closed = True
                await self._teardown()
                return None
            return await self._end_batch(self._subject, b"", OP_COMMIT_EOB, None)

    # -- internals -----------------------------------------------------------

    @contextmanager
    def _exclusive(self) -> Generator[None]:
        """Fail loudly when two tasks drive one batch (the oracle's contract).

        The re-entrancy check comes first on purpose: driving the batch from its
        own error handler *also* trips the busy flag whenever a caller is parked,
        and "another task is using it" would be the wrong diagnosis for what is
        really a deadlock against the ack reader.
        """
        if self._handler_task is not None and asyncio.current_task() is self._handler_task:
            raise BatchReentrantUseError(
                f"batch {self._batch_id} cannot be driven from its own error handler: "
                "add/commit/close run on the ack-reader task and would wait for an ack "
                "only that task can deliver — dispatch the call to another task instead "
                "(asyncio.create_task(...))"
            )
        if self._busy:
            raise ConcurrentBatchUseError(
                f"batch {self._batch_id} is already being used by another task; "
                "a fast-ingest batch is a single-producer conversation"
            )
        self._busy = True
        try:
            yield
        finally:
            self._busy = False

    def _check_open(self) -> None:
        if not self._closed:
            return
        if self._failure is not None:
            raise self._failure
        raise BatchClosedError(f"batch {self._batch_id} is closed")

    def _ensure_started(self) -> None:
        """Subscribe the ack inbox and start its reader (first message only).

        Subscribing is synchronous in natsio — the SUB frame is buffered ahead of
        the publish on the same connection — so no ack can slip past between
        opening the batch and listening for it.
        """
        if self._sub is not None:
            return
        self._sub = self._client.subscribe(f"{self._inbox}.>")
        self._reader = asyncio.get_running_loop().create_task(
            self._read_acks(self._sub), name=f"natsio-fastbatch-{self._batch_id}"
        )

    async def _publish(
        self,
        subject: str,
        payload: bytes | str,
        sequence: int,
        operation: int,
        headers: HeadersInput | None,
    ) -> None:
        reply = build_reply_subject(self._reply_prefix, sequence, operation)
        await self._client.publish(subject, payload, reply=reply, headers=headers)

    async def _end_batch(
        self,
        subject: str,
        payload: bytes | str,
        operation: int,
        headers: HeadersInput | None,
    ) -> BatchAck:
        loop = asyncio.get_running_loop()
        waiter = self._commit = loop.create_future()
        sequence = self._sequence = self._sequence + 1
        # A healthy terminator counts every batch sequence we sent; the
        # end-of-batch marker is not itself a message, so it does not count.
        self._expected_count = sequence if operation == OP_COMMIT else sequence - 1
        try:
            await self._publish(subject, payload, sequence, operation, headers)
            return await self._wait_for_commit(waiter)
        finally:
            # Cleared before the teardown so a disconnect racing the commit
            # cannot leave an exception on a future nobody will ever await.
            self._commit = None
            self._closed = True
            await self._teardown()

    async def _await_first_ack(self, sequence: int) -> FlowAck:
        """Wait for the ack that opens the batch (no pings: nothing to ping yet)."""
        loop = asyncio.get_running_loop()
        waiter = self._first_ack = loop.create_future()
        try:
            done, _ = await asyncio.wait((waiter,), timeout=self._ack_timeout)
            if not done:
                raise BatchAckTimeoutError(
                    f"batch {self._batch_id} message {sequence}: no opening ack within {self._ack_timeout}s"
                )
            return waiter.result()
        finally:
            self._first_ack = None

    async def _fail_batch(self, cause: BaseException | None = None) -> None:
        """Mark the batch dead and release its ack inbox.

        Deliberately local: no end-of-batch marker goes out, because the publish
        that got us here is exactly the one that failed. The batch stays in the
        server's inflight table until it expires — orbit.go's ``fastPublisher``
        does the same on a publish error, and `_abandon` (the context-manager
        path, where the connection is still good) is the one that tells the
        server.

        The cause is recorded as a sticky failure so a later `close()` re-raises
        it instead of returning the empty-batch sentinel (`None`) — a batch that
        persisted messages and then aborted mid-`add` is not "nothing was added".
        A real exception is kept as-is; a bare cancellation is wrapped so close()
        still signals loudly without re-raising a `CancelledError` nobody at the
        close() call site asked for.
        """
        self._closed = True
        if self._failure is None and cause is not None:
            if isinstance(cause, Exception):
                self._failure = cause
            else:
                aborted = BatchClosedError(
                    f"batch {self._batch_id} was aborted mid-add after {self._sequence} message(s); "
                    "they persisted without a terminator"
                )
                aborted.__cause__ = cause
                self._failure = aborted
        await self._teardown()

    def _needs_stall(self) -> bool:
        """True while more than one full ack window is outstanding.

        ``<=`` on purpose (orbit.go fixed a bug here): at exactly
        ``flow * max_outstanding_acks`` unacked messages the window IS full.
        """
        return self._ack_sequence + self._flow * self._max_outstanding_acks <= self._sequence

    async def _wait_for_stall(self) -> None:
        """Park until the server acks enough to reopen the window.

        The deadline covers the whole stall, not each wake-up: ``ack_timeout`` is
        a ceiling on total wait time. Every third of it, a ping re-requests the
        latest flow ack, which is how a dropped ack recovers instead of failing.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._ack_timeout
        interval = self._ack_timeout / _PING_DIVISOR
        while self._needs_stall():
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise BatchAckTimeoutError(
                    f"batch {self._batch_id} stalled at sequence {self._sequence}: "
                    f"no flow ack past {self._ack_sequence} within {self._ack_timeout}s"
                )
            waiter = self._stall = loop.create_future()
            try:
                await asyncio.wait((waiter,), timeout=min(interval, remaining))
                if waiter.done():
                    waiter.result()  # re-raises when the batch ended under us
                else:
                    await self._send_ping()
            finally:
                self._stall = None

    async def _wait_for_commit(self, waiter: "asyncio.Future[BatchAck]") -> BatchAck:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._ack_timeout
        interval = self._ack_timeout / _PING_DIVISOR
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise BatchAckTimeoutError(f"batch {self._batch_id}: no commit ack within {self._ack_timeout}s")
            await asyncio.wait((waiter,), timeout=min(interval, remaining))
            if waiter.done():
                return waiter.result()
            await self._send_ping()

    async def _send_ping(self) -> None:
        """Re-request the latest flow ack, reusing the highest sequence sent.

        A ping adds nothing to the batch — the server answers it with the same
        flow ack it would have sent anyway.
        """
        await self._client.publish(
            self._subject,
            b"",
            reply=build_reply_subject(self._reply_prefix, self._sequence, OP_PING),
        )

    async def _abandon(self) -> None:
        """Give up on the batch because the caller's block failed.

        Best effort: tell the server the batch is over so it does not sit in the
        server's inflight table, but never mask the exception on its way out.
        `asyncio.CancelledError` still propagates.
        """
        if not self._closed:
            self._closed = True
            if self._sequence:
                self._sequence += 1
                with suppress(Exception):
                    await self._client.publish(
                        self._subject,
                        b"",
                        reply=build_reply_subject(self._reply_prefix, self._sequence, OP_COMMIT_EOB),
                    )
        await self._teardown()

    async def _teardown(self) -> None:
        sub, self._sub = self._sub, None
        reader, self._reader = self._reader, None
        if sub is not None:
            await sub.unsubscribe()
        if reader is not None and reader is not asyncio.current_task() and not reader.done():
            reader.cancel()
            # asyncio.wait neither swallows OUR cancellation nor re-raises the
            # reader's own exception.
            await asyncio.wait((reader,))

    # -- ack reader ----------------------------------------------------------

    async def _read_acks(self, sub: Subscription) -> None:
        """Consume the batch's ack inbox until the subscription ends.

        Iteration ending is also the wake path for a closed connection: every
        subscription is closed when the client is, so a batch parked on a stall
        or a commit is woken with `ConnectionClosedError` instead of waiting out
        its whole ack timeout.
        """
        try:
            async for msg in sub:
                await self._on_frame(msg)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # a dead reader must not turn into a silent hang
            await self._report(exc)
        finally:
            self._wake_on_disconnect()

    async def _on_frame(self, msg: Msg) -> None:
        if self._terminated:
            # The batch is over; anything still arriving is the tail of what was
            # already in flight (the server answers every one of them).
            return
        if msg.status is not None and msg.status.code == StatusCode.NO_RESPONDERS:
            # A 503 on the ack inbox means nothing is capturing the subject —
            # a subject typo, the single likeliest user error. It arrives with
            # an empty body, so feeding it to parse_ack_frame reported a bogus
            # "not valid JSON" and then cost the whole ack timeout. The 503 is
            # the definitive answer: end the batch now with the right error.
            subject = msg.headers.get("Nats-Subject") if msg.headers is not None else None
            await self._finish(error=NoStreamResponseError(f"no JetStream stream is listening on {subject!r}"))
            return
        try:
            frame = parse_ack_frame(msg.payload)
        except InvalidBatchAckError as exc:
            await self._report(exc)
            return
        match frame:
            case FlowAck():
                self._on_flow_ack(frame)
            case GapReport():
                gap = BatchGapError(frame.expected_last_sequence, frame.sequence)
                if not self._continue_on_gap:
                    # In fail mode the server ends the batch behind this report.
                    self._abandon_cause = gap
                await self._report(gap)
            case SequenceError():
                # A rejected message always ends the batch, in either gap mode.
                rejected = BatchMessageError(frame.sequence, frame.error)
                self._abandon_cause = rejected
                await self._report(rejected)
            case TerminalError():
                await self._finish(error=frame.error)
            case BatchAck():
                await self._finish(ack=frame)

    def _on_flow_ack(self, frame: FlowAck) -> None:
        if frame.messages:
            # The server dictates the cadence; the reply subject keeps
            # advertising the initial value on purpose (see build_reply_prefix).
            #
            # Divergence from orbit.go, which adopts `msgs` unconditionally: a
            # zero would make `flow * max_outstanding_acks` zero, so _needs_stall
            # would be true forever and every add would park. Guarding costs
            # nothing (2.14.3 never sends 0) and cannot deadlock.
            self._flow = frame.messages
        self._ack_sequence = frame.sequence
        if self._first_ack is not None and not self._first_ack.done():
            self._first_ack.set_result(frame)
            return
        if self._stall is not None and not self._stall.done():
            self._stall.set_result(None)

    async def _finish(self, *, ack: BatchAck | None = None, error: Exception | None = None) -> None:
        """Process the terminal frame: the batch is over, one way or another."""
        self._terminated = True
        self._closed = True
        self._ack = ack
        if error is None and ack is not None and self._is_abandoned(ack):
            error = self._abandoned_error(ack)
        waiter = self._commit
        if waiter is not None and not waiter.done():
            if error is None:
                assert ack is not None
                waiter.set_result(ack)
                return
            # Sticky, so a later close() re-raises instead of handing back the
            # ack of a batch that lost messages.
            self._failure = error
            waiter.set_exception(error)
            return
        if error is None:
            return  # a healthy terminator nobody is parked on: nothing to report
        # Nobody asked for this: the server ended the batch itself (a rejected
        # message, or a gap in the default fail mode). Whoever is parked hears
        # about it; if nobody is, the error handler does.
        self._failure = error
        if self._stall is not None and not self._stall.done():
            self._stall.set_exception(error)
        elif self._first_ack is not None and not self._first_ack.done():
            self._first_ack.set_exception(error)
        else:
            await self._report(error)

    def _is_abandoned(self, ack: BatchAck) -> bool:
        """Did the server end this batch behind our back?

        An abandoned batch is terminated with a **normal-shaped** ack, not an
        error frame — the only tell is ``count``. A batch we ended ourselves is
        counted whole (probe-verified against 2.14.3 in both gap modes: ``count``
        is the last batch sequence the server processed, so it equals every
        sequence we sent), while an abandoned one stops at the message before the
        gap or the rejection. Anything short is data loss, and must never be
        handed back as a successful commit. A terminator carrying no ``count``
        at all therefore reads as short: erring loud is the deliberate side.
        """
        if self._expected_count is None:
            return True  # we never asked to end the batch, so the server did
        return ack.size < self._expected_count

    def _abandoned_error(self, ack: BatchAck | None) -> BatchAbandonedError:
        persisted = ack.size if ack is not None else 0
        cause = self._abandon_cause
        detail = f" after {cause}" if cause is not None else ""
        error = BatchAbandonedError(
            f"server ended batch {self._batch_id}{detail}: counted {persisted} of "
            f"{self._sequence} batch sequence(s); messages already persisted are kept"
        )
        error.__cause__ = cause
        return error

    def _wake_on_disconnect(self) -> None:
        """Fail anything still parked once the ack inbox is gone."""
        pending = [f for f in (self._first_ack, self._commit, self._stall) if f is not None and not f.done()]
        if not pending:
            return
        error = ConnectionClosedError(f"connection closed before batch {self._batch_id} was acknowledged")
        self._closed = True
        self._failure = error
        for future in pending:
            future.set_exception(error)

    async def _report(self, error: Exception) -> None:
        if self._error_handler is None:
            log.error("fast batch %s: %s", self._batch_id, error)
            return
        # Remember which task is inside the handler so `_exclusive` can tell a
        # re-entrant call apart from a genuinely concurrent one. A task the
        # handler spawns is a different task, and is allowed.
        outer, self._handler_task = self._handler_task, asyncio.current_task()
        try:
            result = self._error_handler(error)
            if result is not None:
                await result
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("fast batch %s: error handler itself failed", self._batch_id)
        finally:
            self._handler_task = outer


def fast_publisher(
    js: JetStreamContext,
    *,
    flow_control: FlowControl | None = None,
    continue_on_gap: bool = False,
    error_handler: ErrorHandler | None = None,
) -> FastPublisher:
    """Open a fast-ingest batch on ``js``.

    No I/O happens here, so ``await`` is optional (and a no-op). The stream must
    have been created with ``allow_batched`` — see `ALLOW_BATCHED`.

    - ``flow_control`` tunes ack cadence, back-pressure, and the ack deadline.
    - ``continue_on_gap`` keeps the batch alive when the server reports a hole
      in the sequence instead of abandoning it. The hole is still reported to
      ``error_handler``, and the ack sequence stops implying that everything
      below it was persisted.
    - ``error_handler`` receives the failures nobody is waiting on. Without one
      they are logged at ERROR level — never dropped.
    """
    return FastPublisher(
        js,
        flow_control=flow_control,
        continue_on_gap=continue_on_gap,
        error_handler=error_handler,
    )
