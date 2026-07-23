"""Wire-contract and behavior tests for natsio-jetstream-batch — no server.

The frames replayed here are byte-for-byte captures from nats-server 2.14.3
(see the README's wire-contract section); the fake client/server pair below
speaks the same protocol, so the publisher's flow control, stall/ping recovery,
and termination paths are exercised without a live broker.
"""

import asyncio
import json
from collections.abc import Iterable
from typing import Any, cast

import pytest
from natsio.jetstream_batch import (  # ty: ignore[unresolved-import]
    ALLOW_BATCHED,
    DEFAULT_FLOW,
    DEFAULT_MAX_OUTSTANDING_ACKS,
    FAST_INGEST_SUFFIX,
    GAP_FAIL,
    GAP_OK,
    OP_ADD,
    OP_COMMIT,
    OP_COMMIT_EOB,
    OP_PING,
    OP_START,
    BatchAbandonedError,
    BatchAck,
    BatchAckTimeoutError,
    BatchClosedError,
    BatchGapError,
    BatchGetError,
    BatchMessageError,
    BatchReentrantUseError,
    ConcurrentBatchUseError,
    FastBatchNotEnabledError,
    FastPublisher,
    FlowAck,
    FlowControl,
    GapReport,
    InvalidBatchAckError,
    SequenceError,
    TerminalError,
    build_message_headers,
    build_reply_prefix,
    build_reply_subject,
    fast_publisher,
    get_batch,
    parse_ack_frame,
)

from natsio._internal.protocol import Headers
from natsio.client import Client
from natsio.errors import ConfigError, ConnectionClosedError
from natsio.jetstream.context import JetStreamContext
from natsio.jetstream.entities import StreamConfig, StreamInfo
from natsio.jetstream.errors import APIError, WrongLastSequenceError, error_for
from natsio.jetstream.stream import Stream
from natsio.message import Msg

# -- captured frames (nats-server 2.14.3) ------------------------------------

FLOW_ACK_FRAME = b'{"type":"ack","seq":0,"msgs":5}'
GAP_FRAME = b'{"type":"gap","last_seq":2,"seq":3}'
ERR_FRAME = b'{"type":"err","seq":2,"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 3"}}'
COMMIT_FRAME = b'{"stream":"TEST","seq":13,"batch":"wscrbbDk2TPCGsu5xoNhav","count":13}'
UNKNOWN_ID_FRAME = (
    b'{"error":{"code":400,"err_code":10208,"description":"batch publish ID unknown"},"stream":"TEST","seq":0}'
)
NOT_ENABLED_FRAME = (
    b'{"error":{"code":400,"err_code":10205,"description":"batch publish is disabled"},"stream":"TEST","seq":0}'
)


class TestWireConstants:
    """The exact tokens that go on the wire."""

    def test_suffix_and_operations(self) -> None:
        assert FAST_INGEST_SUFFIX == "$FI"
        assert (OP_START, OP_ADD, OP_COMMIT, OP_COMMIT_EOB, OP_PING) == (0, 1, 2, 3, 4)

    def test_gap_tokens(self) -> None:
        assert (GAP_FAIL, GAP_OK) == ("fail", "ok")

    def test_defaults_match_the_oracle(self) -> None:
        assert DEFAULT_FLOW == 100
        assert DEFAULT_MAX_OUTSTANDING_ACKS == 2

    def test_stream_config_field(self) -> None:
        assert ALLOW_BATCHED == "allow_batched"


class TestReplySubject:
    """``<inbox>.<flow>.<gap>.<seq>.<op>.$FI`` — orbit.go buildReplySubject."""

    def test_prefix_fail_mode(self) -> None:
        assert build_reply_prefix("_INBOX.abc", 100, continue_on_gap=False) == "_INBOX.abc.100.fail."

    def test_prefix_gap_ok_mode(self) -> None:
        assert build_reply_prefix("_INBOX.abc", 50, continue_on_gap=True) == "_INBOX.abc.50.ok."

    def test_full_subject(self) -> None:
        prefix = build_reply_prefix("_INBOX.abc", 100, continue_on_gap=False)
        assert build_reply_subject(prefix, 1, OP_START) == "_INBOX.abc.100.fail.1.0.$FI"
        assert build_reply_subject(prefix, 2, OP_ADD) == "_INBOX.abc.100.fail.2.1.$FI"
        assert build_reply_subject(prefix, 3, OP_COMMIT) == "_INBOX.abc.100.fail.3.2.$FI"
        assert build_reply_subject(prefix, 3, OP_COMMIT_EOB) == "_INBOX.abc.100.fail.3.3.$FI"
        assert build_reply_subject(prefix, 7, OP_PING) == "_INBOX.abc.100.fail.7.4.$FI"

    def test_batch_id_is_the_last_inbox_token(self) -> None:
        # The server slices the batch id out of the reply subject: it is the
        # token immediately before <flow>. Probe-confirmed against 2.14.3.
        subject = build_reply_subject(build_reply_prefix("_INBOX.x._fi.BATCHID", 10, continue_on_gap=False), 4, OP_ADD)
        assert subject.split(".")[-6] == "BATCHID"


class TestParseAckFrame:
    def test_flow_ack(self) -> None:
        frame = parse_ack_frame(FLOW_ACK_FRAME)
        assert frame == FlowAck(sequence=0, messages=5)

    def test_gap(self) -> None:
        frame = parse_ack_frame(GAP_FRAME)
        assert frame == GapReport(expected_last_sequence=2, sequence=3)

    def test_sequence_error(self) -> None:
        frame = parse_ack_frame(ERR_FRAME)
        assert isinstance(frame, SequenceError)
        assert frame.sequence == 2
        assert isinstance(frame.error, WrongLastSequenceError)
        assert frame.error.err_code == 10071

    def test_commit_ack(self) -> None:
        frame = parse_ack_frame(COMMIT_FRAME)
        assert isinstance(frame, BatchAck)
        assert frame.stream == "TEST"
        assert frame.seq == 13
        assert frame.batch_id == "wscrbbDk2TPCGsu5xoNhav"
        assert frame.size == 13

    def test_terminal_error_is_typed(self) -> None:
        frame = parse_ack_frame(UNKNOWN_ID_FRAME)
        assert isinstance(frame, TerminalError)
        assert frame.error.err_code == 10208
        assert type(frame.error).__name__ == "FastBatchUnknownIdError"

    def test_not_enabled_error_is_typed(self) -> None:
        frame = parse_ack_frame(NOT_ENABLED_FRAME)
        assert isinstance(frame, TerminalError)
        assert isinstance(frame.error, FastBatchNotEnabledError)

    @pytest.mark.parametrize(
        "data",
        [
            b"",
            b"not json",
            b"[1, 2]",
            b'{"type":"ack","seq":0}',  # missing msgs
            b'{"type":"ack","seq":"x","msgs":1}',  # non-numeric
            b'{"type":"gap","seq":3}',  # missing last_seq
            b'{"type":"err","seq":1}',  # missing error
            b'{"type":"whatever"}',  # unknown frame type
            b'{"seq":1,"count":1}',  # terminal ack without a stream
            # A non-object `error` used to raise a bare AttributeError out of
            # APIError.from_error — an untyped escape that killed the reader.
            # Any client on the account can put these on the ack inbox.
            b'{"error":"boom"}',  # terminal, string error
            b'{"error":null}',  # terminal, null error
            b'{"type":"err","seq":1,"error":"boom"}',  # sequence, string error
            b'{"type":"err","seq":1,"error":42}',  # sequence, numeric error
        ],
    )
    def test_malformed_is_loud(self, data: bytes) -> None:
        with pytest.raises(InvalidBatchAckError):
            parse_ack_frame(data)


class TestErrorRegistration:
    """Importing the package binds our err_codes into core's shared registry."""

    def test_codes_arrive_typed(self) -> None:
        assert error_for(10205) is FastBatchNotEnabledError
        assert error_for(10208).__name__ == "FastBatchUnknownIdError"

    def test_stealing_a_code_from_core_is_reported(self, caplog: pytest.LogCaptureFixture) -> None:
        # The registry is a process global core owns: overriding a binding is
        # allowed (that is the extension hook) but never silent.
        import logging

        from natsio.jetstream_batch.errors import _register_fast_batch_errors  # ty: ignore[unresolved-import]

        from natsio.jetstream.errors import register_error

        class DecoyError(APIError):
            pass

        register_error(10205, DecoyError)
        try:
            with caplog.at_level(logging.WARNING, logger="natsio.jetstream_batch"):
                _register_fast_batch_errors()
        finally:
            register_error(10205, FastBatchNotEnabledError)

        assert "10205" in caplog.text
        assert "DecoyError" in caplog.text
        assert error_for(10205) is FastBatchNotEnabledError


class TestFlowControl:
    def test_defaults(self) -> None:
        control = FlowControl()
        assert (control.flow, control.max_outstanding_acks, control.ack_timeout) == (100, 2, None)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"flow": 0},
            {"flow": 65536},
            {"max_outstanding_acks": 0},
            {"ack_timeout": 0},
            {"ack_timeout": -1.0},
        ],
    )
    def test_rejects_nonsense(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(ConfigError):
            FlowControl(**kwargs)


class TestMessageHeaders:
    def test_no_options_passes_headers_through(self) -> None:
        assert build_message_headers() is None
        original = {"X": "1"}
        assert build_message_headers(headers=original) is original

    def test_expectations(self) -> None:
        headers = build_message_headers(
            ttl=60,
            expected_stream="ORDERS",
            expected_last_seq=10,
            expected_last_subject_seq=4,
            expected_last_subject_seq_subject="a.*",
        )
        assert headers == {
            "Nats-TTL": "60",
            "Nats-Expected-Stream": "ORDERS",
            "Nats-Expected-Last-Sequence": "10",
            "Nats-Expected-Last-Subject-Sequence": "4",
            "Nats-Expected-Last-Subject-Sequence-Subject": "a.*",
        }

    def test_merges_onto_user_headers(self) -> None:
        merged = build_message_headers(headers=Headers({"X-Trace": "abc"}), expected_stream="S")
        assert isinstance(merged, Headers)
        assert merged.get("X-Trace") == "abc"
        assert merged.get("Nats-Expected-Stream") == "S"

    def test_subject_scope_needs_a_sequence(self) -> None:
        with pytest.raises(ConfigError):
            build_message_headers(expected_last_subject_seq_subject="a.*")


# -- fake client / fake fast-ingest server -----------------------------------


class _FakeSubscription:
    """Stand-in for `natsio.Subscription`: async-iterable, unsubscribable."""

    def __init__(self, subject: str) -> None:
        self.subject = subject
        self.unsubscribed = False
        self._queue: asyncio.Queue[Msg] = asyncio.Queue()
        self._closed = asyncio.Event()

    def deliver(self, payload: bytes, subject: str | None = None) -> None:
        self._queue.put_nowait(Msg(subject=subject or self.subject, payload=payload))

    async def unsubscribe(self) -> None:
        self.unsubscribed = True
        self._closed.set()

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        while True:
            if not self._queue.empty():
                yield self._queue.get_nowait()
                continue
            if self._closed.is_set():
                return
            getter = asyncio.ensure_future(self._queue.get())
            closer = asyncio.ensure_future(self._closed.wait())
            try:
                done, _ = await asyncio.wait((getter, closer), return_when=asyncio.FIRST_COMPLETED)
            finally:
                getter.cancel()
                closer.cancel()
            if getter in done and not getter.cancelled():
                yield getter.result()


class _FakeServer:
    """A hand-rolled fast-ingest server: parses reply subjects, replays frames.

    ``reject_at`` reproduces what 2.14.3 does to an abandoned batch: the message
    at that batch sequence is rejected with an ``err`` frame, and the batch is
    then terminated with a **normal-shaped** ack whose ``count`` stops at the
    last message that made it. The terminator is held back until the *next*
    frame arrives, which is how the client ends up parked on ``commit`` when it
    lands (the ordering the live suite could not pin down).
    """

    def __init__(
        self,
        *,
        stream: str = "TEST",
        swallow_flow_acks: bool = False,
        silent: bool = False,
        reject_at: int | None = None,
    ) -> None:
        self.stream = stream
        self.swallow_flow_acks = swallow_flow_acks
        self.silent = silent
        self.reject_at = reject_at
        self.flow_override: int | None = None
        self.calls: list[tuple[str, bytes, int, int]] = []  # subject, payload, seq, op
        self.replies: list[str] = []
        self.persisted = 0
        self.rejected = False
        self.abandoned = False

    def respond(self, subject: str, payload: bytes, reply: str) -> Iterable[bytes]:
        tokens = reply.split(".")
        assert tokens[-1] == FAST_INGEST_SUFFIX
        operation, sequence, flow = int(tokens[-2]), int(tokens[-3]), int(tokens[-5])
        batch_id = tokens[-6]
        self.calls.append((subject, payload, sequence, operation))
        self.replies.append(reply)
        cadence = self.flow_override if self.flow_override is not None else flow
        if self.silent:
            return ()
        if self.abandoned:
            # Every later frame of an abandoned batch gets "ID unknown".
            return (UNKNOWN_ID_FRAME,)
        if self.rejected:
            self.abandoned = True
            assert self.reject_at is not None
            return (self._terminal_ack(batch_id, self.reject_at - 1),)
        if sequence == self.reject_at:
            self.rejected = True
            self.persisted = sequence - 1
            return (ERR_FRAME,)
        if operation == OP_PING:
            return (self._flow_ack(self.persisted, cadence),)
        if operation in (OP_COMMIT, OP_COMMIT_EOB):
            count = sequence if operation == OP_COMMIT else sequence - 1
            self.persisted = count
            return (self._terminal_ack(batch_id, count),)
        self.persisted = sequence
        if operation == OP_START:
            return (self._flow_ack(0, cadence),)
        if self.swallow_flow_acks or sequence % cadence:
            return ()
        return (self._flow_ack(sequence, cadence),)

    @staticmethod
    def _flow_ack(sequence: int, messages: int) -> bytes:
        return json.dumps({"type": "ack", "seq": sequence, "msgs": messages}).encode()

    def _terminal_ack(self, batch_id: str, count: int) -> bytes:
        return json.dumps({"stream": self.stream, "seq": count, "batch": batch_id, "count": count}).encode()


class _FakeClient:
    """The three `natsio.Client` members `FastPublisher` touches."""

    inbox_prefix = "_INBOX.unit"

    def __init__(self, server: _FakeServer | None = None) -> None:
        self.server = server
        self.subscription: _FakeSubscription | None = None
        self.published: list[tuple[str, bytes, str | None]] = []
        # Event-driven handoff so tests never poll for "has it started yet".
        self.subscribed = asyncio.Event()
        self.sent = asyncio.Event()

    def subscribe(self, subject: str, **_: Any) -> _FakeSubscription:
        self.subscription = _FakeSubscription(subject)
        self.subscribed.set()
        return self.subscription

    async def publish(
        self,
        subject: str,
        payload: bytes | str = b"",
        *,
        reply: str | None = None,
        headers: Any = None,
    ) -> None:
        data = payload.encode() if isinstance(payload, str) else payload
        self.published.append((subject, data, reply))
        self.sent.set()
        if self.server is None or reply is None or self.subscription is None:
            return
        for frame in self.server.respond(subject, data, reply):
            self.subscription.deliver(frame, subject=reply)


class _Recorder:
    """Error-handler double with an event-driven wait (no sleeps)."""

    def __init__(self) -> None:
        self.errors: list[Exception] = []
        self._event = asyncio.Event()

    def __call__(self, error: Exception) -> None:
        self.errors.append(error)
        self._event.set()

    async def wait(self, deadline: float = 2.0) -> Exception:
        async with asyncio.timeout(deadline):
            await self._event.wait()
        self._event.clear()
        return self.errors[-1]


def _context(client: _FakeClient, timeout: float = 1.0) -> JetStreamContext:
    # The fake only implements what FastPublisher/BatchGet call; a real context
    # carries it so `js.timeout` and `js.api_prefix` behave normally.
    return JetStreamContext(cast("Client", client), timeout=timeout)


class TestFastPublisherProtocol:
    async def test_first_message_opens_the_batch(self) -> None:
        server = _FakeServer()
        client = _FakeClient(server)
        publisher = fast_publisher(_context(client))

        ack = await publisher.add("test.a", b"one")

        assert ack.batch_sequence == 1
        # The opening ack reports sequence 0: nothing is persisted yet at that point.
        assert ack.ack_sequence == 0
        subject, payload, reply = client.published[0]
        assert (subject, payload) == ("test.a", b"one")
        assert reply is not None
        assert reply.endswith(f".{DEFAULT_FLOW}.{GAP_FAIL}.1.{OP_START}.{FAST_INGEST_SUFFIX}")
        assert reply.split(".")[-6] == publisher.batch_id
        await publisher.close()

    async def test_optional_await_is_a_no_op(self) -> None:
        publisher = await fast_publisher(_context(_FakeClient(_FakeServer())))
        assert isinstance(publisher, FastPublisher)
        assert publisher.size == 0

    async def test_subsequent_messages_use_the_add_operation(self) -> None:
        server = _FakeServer()
        publisher = fast_publisher(_context(_FakeClient(server)))
        await publisher.add("test.a", b"one")
        await publisher.add("test.a", b"two")
        await publisher.add("test.a", b"three")
        await publisher.close()

        assert [(seq, op) for _, _, seq, op in server.calls] == [
            (1, OP_START),
            (2, OP_ADD),
            (3, OP_ADD),
            (4, OP_COMMIT_EOB),
        ]

    async def test_commit_returns_the_batch_ack_and_closes(self) -> None:
        server = _FakeServer()
        client = _FakeClient(server)
        publisher = fast_publisher(_context(client))
        await publisher.add("test.a", b"one")

        ack = await publisher.commit("test.a", b"two")

        assert ack.stream == "TEST"
        assert ack.size == 2
        assert ack.batch_id == publisher.batch_id
        assert publisher.is_closed
        assert publisher.ack is ack
        assert client.subscription is not None
        assert client.subscription.unsubscribed
        with pytest.raises(BatchClosedError):
            await publisher.add("test.a", b"three")

    async def test_commit_without_a_prior_add_is_a_one_message_batch(self) -> None:
        # Probe-confirmed against 2.14.3: a commit may open the batch itself.
        server = _FakeServer()
        publisher = fast_publisher(_context(_FakeClient(server)))
        ack = await publisher.commit("test.a", b"only")
        assert ack.size == 1
        assert server.calls == [("test.a", b"only", 1, OP_COMMIT)]

    async def test_close_sends_an_empty_end_of_batch(self) -> None:
        server = _FakeServer()
        publisher = fast_publisher(_context(_FakeClient(server)))
        await publisher.add("test.a", b"one")
        await publisher.add("test.a", b"two")

        ack = await publisher.close()

        assert ack is not None
        assert ack.size == 2  # the EOB marker itself is not a message
        assert server.calls[-1] == ("test.a", b"", 3, OP_COMMIT_EOB)

    async def test_close_on_an_untouched_publisher_is_a_no_op(self) -> None:
        client = _FakeClient(_FakeServer())
        publisher = fast_publisher(_context(client))
        assert await publisher.close() is None
        assert client.published == []
        assert publisher.is_closed

    async def test_close_is_idempotent(self) -> None:
        publisher = fast_publisher(_context(_FakeClient(_FakeServer())))
        await publisher.add("test.a", b"one")
        first = await publisher.close()
        assert await publisher.close() is first

    async def test_context_manager_closes_the_batch(self) -> None:
        server = _FakeServer()
        client = _FakeClient(server)
        async with fast_publisher(_context(client)) as publisher:
            await publisher.add("test.a", b"one")
        assert publisher.is_closed
        assert server.calls[-1][3] == OP_COMMIT_EOB
        assert client.subscription is not None
        assert client.subscription.unsubscribed

    async def test_context_manager_abandons_on_error(self) -> None:
        server = _FakeServer()
        client = _FakeClient(server)
        with pytest.raises(RuntimeError):
            async with fast_publisher(_context(client)) as publisher:
                await publisher.add("test.a", b"one")
                raise RuntimeError("boom")
        assert publisher.is_closed
        # Best effort: the server is told the batch is over, but the body's
        # exception is what surfaces.
        assert server.calls[-1][3] == OP_COMMIT_EOB
        assert client.subscription is not None
        assert client.subscription.unsubscribed


class TestFlowControlBehaviour:
    async def test_stall_waits_for_the_flow_ack(self) -> None:
        # flow=1 with one window outstanding: every message stalls until acked.
        server = _FakeServer()
        publisher = fast_publisher(
            _context(_FakeClient(server)),
            flow_control=FlowControl(flow=1, max_outstanding_acks=1, ack_timeout=2.0),
        )
        for index in range(5):
            ack = await publisher.add("test.a", str(index))
            assert ack.ack_sequence == ack.batch_sequence or ack.batch_sequence == 1
        assert publisher.ack_sequence == 5
        await publisher.close()

    async def test_ping_recovers_a_lost_flow_ack(self) -> None:
        # The server swallows flow acks but still answers pings — exactly the
        # case pings exist for.
        server = _FakeServer(swallow_flow_acks=True)
        publisher = fast_publisher(
            _context(_FakeClient(server)),
            flow_control=FlowControl(flow=1, max_outstanding_acks=1, ack_timeout=0.6),
        )
        await publisher.add("test.a", b"one")
        ack = await publisher.add("test.a", b"two")

        assert ack.ack_sequence == 2
        assert any(op == OP_PING for _, _, _, op in server.calls)
        await publisher.close()

    async def test_stall_times_out_loudly(self) -> None:
        server = _FakeServer(silent=True)
        client = _FakeClient(server)
        publisher = fast_publisher(
            _context(client),
            flow_control=FlowControl(flow=1, max_outstanding_acks=1, ack_timeout=0.3),
        )
        with pytest.raises(BatchAckTimeoutError):
            await publisher.add("test.a", b"one")
        assert publisher.is_closed
        assert client.subscription is not None
        assert client.subscription.unsubscribed

    async def test_server_may_lower_the_cadence_without_rewriting_the_subject(self) -> None:
        # orbit.go pins this: the reply subject keeps advertising the INITIAL
        # flow even after the server dictates a smaller one.
        server = _FakeServer()
        server.flow_override = 2
        publisher = fast_publisher(
            _context(_FakeClient(server)),
            flow_control=FlowControl(flow=10, max_outstanding_acks=1, ack_timeout=2.0),
        )
        for index in range(6):
            await publisher.add("test.a", str(index))
        await publisher.close()

        assert all(".10.fail." in reply for reply in server.replies)
        assert publisher.ack_sequence == 6

    async def test_commit_pings_while_waiting(self) -> None:
        server = _FakeServer(silent=True)
        publisher = fast_publisher(
            _context(_FakeClient(server)),
            flow_control=FlowControl(ack_timeout=0.3),
        )
        with pytest.raises(BatchAckTimeoutError):
            await publisher.commit("test.a", b"one")
        assert any(op == OP_PING for _, _, _, op in server.calls)
        assert publisher.is_closed


class TestAsyncFailures:
    async def test_gap_is_reported(self) -> None:
        recorder = _Recorder()
        client = _FakeClient(_FakeServer())
        publisher = fast_publisher(_context(client), error_handler=recorder)
        await publisher.add("test.a", b"one")
        assert client.subscription is not None
        client.subscription.deliver(GAP_FRAME)

        error = await recorder.wait()
        assert isinstance(error, BatchGapError)
        assert (error.expected_last_sequence, error.sequence) == (2, 3)
        await publisher.close()

    async def test_rejected_message_is_reported(self) -> None:
        recorder = _Recorder()
        client = _FakeClient(_FakeServer())
        publisher = fast_publisher(_context(client), error_handler=recorder)
        await publisher.add("test.a", b"one")
        assert client.subscription is not None
        client.subscription.deliver(ERR_FRAME)

        error = await recorder.wait()
        assert isinstance(error, BatchMessageError)
        assert error.sequence == 2
        assert isinstance(error.cause, APIError)
        assert error.cause.err_code == 10071
        await publisher.close()

    async def test_unparsable_frame_is_reported(self) -> None:
        recorder = _Recorder()
        client = _FakeClient(_FakeServer())
        publisher = fast_publisher(_context(client), error_handler=recorder)
        await publisher.add("test.a", b"one")
        assert client.subscription is not None
        client.subscription.deliver(b"{not json")

        assert isinstance(await recorder.wait(), InvalidBatchAckError)
        await publisher.close()

    async def test_server_ending_the_batch_is_reported_and_sticky(self) -> None:
        recorder = _Recorder()
        client = _FakeClient(_FakeServer())
        publisher = fast_publisher(_context(client), error_handler=recorder)
        await publisher.add("test.a", b"one")
        assert client.subscription is not None
        # What 2.14.3 sends after a gap in `fail` mode: a terminating ack for a
        # batch nobody asked to end.
        client.subscription.deliver(b'{"stream":"TEST","seq":16,"batch":"x","count":1}')

        error = await recorder.wait()
        assert isinstance(error, BatchAbandonedError)
        assert publisher.is_closed
        with pytest.raises(BatchAbandonedError):
            await publisher.add("test.a", b"two")
        with pytest.raises(BatchAbandonedError):
            await publisher.close()

    async def test_abandoned_batch_does_not_resolve_a_parked_commit(self) -> None:
        # Regression (review [high]): the server ends an abandoned batch with a
        # NORMAL-shaped terminal ack. Resolving the parked commit() with it
        # reported four sent messages, one stored, and no exception at all.
        recorder = _Recorder()
        server = _FakeServer(reject_at=2)
        client = _FakeClient(server)
        publisher = fast_publisher(_context(client), error_handler=recorder)
        await publisher.add("test.a", b"one")
        await publisher.add("test.a", b"two")  # rejected; the batch is doomed
        await publisher.add("test.a", b"three")

        with pytest.raises(BatchAbandonedError) as caught:
            await publisher.commit("test.a", b"four")

        # The failure names the count that gave it away, and keeps the cause.
        assert "1 of 4" in str(caught.value)
        assert isinstance(caught.value.__cause__, BatchMessageError)
        # The ack is still readable for whoever wants the details...
        assert publisher.ack is not None
        assert publisher.ack.size == 1
        # ...and the failure is sticky rather than a one-shot.
        with pytest.raises(BatchAbandonedError):
            await publisher.close()

    async def test_abandoned_batch_surfaces_on_a_parked_close(self) -> None:
        # Same defect through the end-of-batch terminator, whose healthy count
        # is one *less* than the sequence sent — the off-by-one that makes a
        # naive "count == size" check reject good batches.
        server = _FakeServer(reject_at=2)
        client = _FakeClient(server)
        publisher = fast_publisher(_context(client))
        await publisher.add("test.a", b"one")
        await publisher.add("test.a", b"two")

        with pytest.raises(BatchAbandonedError):
            await publisher.close()

    async def test_healthy_end_of_batch_is_not_mistaken_for_an_abandon(self) -> None:
        # The other side of the discriminator: count == sequence - 1 for an EOB
        # and == sequence for a commit are both whole batches.
        publisher = fast_publisher(_context(_FakeClient(_FakeServer())))
        await publisher.add("test.a", b"one")
        await publisher.add("test.a", b"two")
        closed = await publisher.close()
        assert closed is not None
        assert closed.size == 2

        other = fast_publisher(_context(_FakeClient(_FakeServer())))
        await other.add("test.a", b"one")
        assert (await other.commit("test.a", b"two")).size == 2

    async def test_close_after_a_cancelled_add_does_not_report_an_empty_batch(self) -> None:
        # Regression (review [low]): a cancelled add ends the batch with messages
        # already persisted but no EOB. close() took the "closed" branch and
        # returned None — the documented "nothing was added" sentinel — hiding
        # the abort. It must re-raise loudly instead.
        server = _FakeServer(swallow_flow_acks=True)  # opening ack only; then silence
        client = _FakeClient(server)
        publisher = fast_publisher(
            _context(client), flow_control=FlowControl(flow=1, max_outstanding_acks=1, ack_timeout=5.0)
        )
        await publisher.add("test.a", b"one")  # opens the batch (1 message persisted)

        client.sent.clear()
        parked = asyncio.ensure_future(publisher.add("test.a", b"two"))  # stalls: acks are swallowed
        await client.sent.wait()  # the second frame is on the wire; the add is stalling
        parked.cancel()
        with pytest.raises(asyncio.CancelledError):
            await parked

        assert publisher.is_closed
        with pytest.raises(BatchClosedError, match="aborted mid-add"):
            await publisher.close()

    async def test_error_handler_cannot_drive_the_batch(self) -> None:
        # Regression (review [medium]): close()/commit() from the handler ran ON
        # the ack-reader task, so it waited for an ack only that task could
        # deliver — a full ack_timeout of deadlock reported as a misleading
        # ConcurrentBatchUseError.
        caught: list[Exception] = []
        reported: list[Exception] = []
        seen = asyncio.Event()

        async def handler(error: Exception) -> None:
            reported.append(error)
            try:
                await publisher.close()
            except Exception as exc:
                caught.append(exc)
            seen.set()

        client = _FakeClient(_FakeServer())
        publisher = fast_publisher(_context(client), error_handler=handler, flow_control=FlowControl(ack_timeout=30.0))
        await publisher.add("test.a", b"one")
        assert client.subscription is not None
        client.subscription.deliver(ERR_FRAME)

        async with asyncio.timeout(2.0):  # nowhere near ack_timeout: no deadlock
            await seen.wait()
        assert isinstance(caught[0], BatchReentrantUseError)
        assert not isinstance(caught[0], ConcurrentBatchUseError)

        # The reader kept draining: the terminal frame is still processed.
        seen.clear()
        client.subscription.deliver(b'{"stream":"TEST","seq":9,"batch":"x","count":1}')
        async with asyncio.timeout(2.0):
            await seen.wait()
        assert isinstance(reported[-1], BatchAbandonedError)
        assert publisher.is_closed
        with pytest.raises(BatchAbandonedError):
            await publisher.close()  # also releases the reader

    async def test_error_handler_may_end_the_batch_from_another_task(self) -> None:
        # The documented way out of the re-entrancy guard: hand the work to a
        # task of its own, which is not the reader and so may park.
        spawned: list[asyncio.Task[BatchAck | None]] = []
        started = asyncio.Event()

        def handler(error: Exception) -> None:
            if isinstance(error, BatchGapError) and not spawned:
                spawned.append(asyncio.ensure_future(publisher.close()))
                started.set()

        client = _FakeClient(_FakeServer())
        publisher = fast_publisher(_context(client), continue_on_gap=True, error_handler=handler)
        await publisher.add("test.a", b"one")
        assert client.subscription is not None
        client.subscription.deliver(GAP_FRAME)

        async with asyncio.timeout(2.0):
            await started.wait()
            ack = await spawned[0]
        assert ack is not None
        assert ack.size == 1
        assert publisher.is_closed

    async def test_terminal_error_surfaces_on_the_opening_ack(self) -> None:
        client = _FakeClient()  # no auto-responder: the test drives the frames
        publisher = fast_publisher(_context(client), flow_control=FlowControl(ack_timeout=2.0))

        async def reject() -> None:
            await client.subscribed.wait()
            assert client.subscription is not None
            client.subscription.deliver(NOT_ENABLED_FRAME)

        task = asyncio.ensure_future(reject())
        with pytest.raises(FastBatchNotEnabledError):
            await publisher.add("test.a", b"one")
        await task
        assert publisher.is_closed

    async def test_connection_loss_wakes_a_pending_commit(self) -> None:
        client = _FakeClient()
        publisher = fast_publisher(_context(client), flow_control=FlowControl(ack_timeout=5.0))

        async def drop() -> None:
            await client.subscribed.wait()
            assert client.subscription is not None
            await client.subscription.unsubscribe()

        task = asyncio.ensure_future(drop())
        with pytest.raises(ConnectionClosedError):
            await publisher.commit("test.a", b"one")
        await task

    async def test_concurrent_use_is_refused(self) -> None:
        server = _FakeServer(silent=True)
        client = _FakeClient(server)
        publisher = fast_publisher(_context(client), flow_control=FlowControl(ack_timeout=0.4))
        first = asyncio.ensure_future(publisher.add("test.a", b"one"))
        await client.sent.wait()  # the first add is now parked on its opening ack

        with pytest.raises(ConcurrentBatchUseError):
            await publisher.add("test.a", b"two")
        with pytest.raises(ConcurrentBatchUseError):
            await publisher.commit("test.a", b"three")

        with pytest.raises(BatchAckTimeoutError):
            await first


class TestGetBatchRequest:
    """The JSON body of a batch direct get (orbit.go getBatchOpts)."""

    def _js(self) -> JetStreamContext:
        return _context(_FakeClient())

    def test_defaults_to_the_start_of_the_stream(self) -> None:
        assert get_batch(self._js(), "S", 10).request == {"seq": 1, "batch": 10}

    def test_by_sequence(self) -> None:
        assert get_batch(self._js(), "S", 5, seq=100).request == {"seq": 100, "batch": 5}

    def test_by_subject(self) -> None:
        assert get_batch(self._js(), "S", 5, subject="a.*").request == {
            "seq": 1,
            "next_by_subj": "a.*",
            "batch": 5,
        }

    def test_by_start_time_drops_the_sequence(self) -> None:
        from datetime import UTC, datetime

        request = get_batch(self._js(), "S", 5, start_time=datetime(2026, 7, 22, 21, 0, 0, tzinfo=UTC)).request
        assert request == {"batch": 5, "start_time": "2026-07-22T21:00:00Z"}

    def test_max_bytes(self) -> None:
        assert get_batch(self._js(), "S", 5, max_bytes=1024).request == {
            "seq": 1,
            "batch": 5,
            "max_bytes": 1024,
        }

    def test_request_is_a_copy(self) -> None:
        reader = get_batch(self._js(), "S", 5)
        reader.request["batch"] = 99
        assert reader.request["batch"] == 5

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"batch": 0},
            {"batch": 5, "seq": 0},
            {"batch": 5, "max_bytes": 0},
            {"batch": 5, "subject": ""},
        ],
    )
    def test_rejects_nonsense(self, kwargs: dict[str, Any]) -> None:
        batch = kwargs.pop("batch")
        with pytest.raises(ConfigError):
            get_batch(self._js(), "S", batch, **kwargs)

    def test_seq_and_start_time_are_exclusive(self) -> None:
        from datetime import UTC, datetime

        with pytest.raises(ConfigError):
            get_batch(self._js(), "S", 5, seq=1, start_time=datetime.now(UTC))

    def test_stream_handle_without_direct_get_is_refused(self) -> None:
        js = self._js()
        stream = Stream(js, StreamInfo(config=StreamConfig(name="S", allow_direct=False)))
        with pytest.raises(BatchGetError):
            get_batch(js, stream, 5)

    def test_await_is_optional(self) -> None:
        reader = get_batch(self._js(), "S", 5)
        assert reader.stream_name == "S"
        assert repr(reader).startswith("BatchGet(")
