"""End-to-end fast-ingest publishing against a real nats-server (pinned 2.14.3).

Skipped loudly on any server that does not echo ``allow_batched`` back from
STREAM.CREATE — fast ingest is 2.14+ and there is nothing to fake.
"""

import asyncio

import pytest
from natsio.jetstream_batch import (  # ty: ignore[unresolved-import]
    BatchAbandonedError,
    BatchClosedError,
    BatchGapError,
    BatchMessageError,
    BatchReentrantUseError,
    ConcurrentBatchUseError,
    FastBatchNotEnabledError,
    FastPublisher,
    FlowControl,
    fast_publisher,
    get_batch,
)

import natsio
from conftest import batch_stream  # ty: ignore[unresolved-import]
from natsio.errors import ConnectionClosedError
from natsio.jetstream.context import JetStreamContext
from natsio.jetstream.entities import StreamConfig
from natsio.jetstream.errors import NoStreamResponseError


class _Collector:
    """Async-error sink with an event-driven wait."""

    def __init__(self) -> None:
        self.errors: list[Exception] = []
        self._event = asyncio.Event()

    def __call__(self, error: Exception) -> None:
        self.errors.append(error)
        self._event.set()

    async def wait_for[E: Exception](self, kind: type[E], deadline: float = 5.0) -> E:
        """The first recorded error of ``kind``, waiting for it if need be."""
        async with asyncio.timeout(deadline):
            while True:
                for error in self.errors:
                    if isinstance(error, kind):
                        return error
                self._event.clear()
                await self._event.wait()

    async def wait_until_closed(self, publisher: FastPublisher, deadline: float = 5.0) -> None:
        async with asyncio.timeout(deadline):
            while not publisher.is_closed:
                self._event.clear()
                await self._event.wait()


class TestFastPublish:
    async def test_thousand_messages_persist_in_order(self, js: JetStreamContext) -> None:
        stream = await batch_stream(js, "BULK", ["bulk.>"])

        async with fast_publisher(js) as publisher:
            for index in range(1000):
                ack = await publisher.add("bulk.data", f"m{index}".encode())
                assert ack.batch_sequence == index + 1
            final = await publisher.commit("bulk.data", b"last")

        assert final.stream == "BULK"
        assert final.size == 1001
        assert final.seq == 1001
        assert final.batch_id == publisher.batch_id

        info = await stream.info()
        assert info.state.messages == 1001
        assert info.state.first_seq == 1
        assert info.state.last_seq == 1001

        # Read them back: sequences are dense and payloads are in publish order.
        seen = [msg async for msg in get_batch(js, stream, 1001)]
        assert len(seen) == 1001
        assert [msg.seq for msg in seen] == list(range(1, 1002))
        assert [msg.payload for msg in seen[:3]] == [b"m0", b"m1", b"m2"]
        assert seen[-1].payload == b"last"

    async def test_ack_sequence_advances_with_the_flow(self, js: JetStreamContext) -> None:
        await batch_stream(js, "FLOWY", ["flowy.>"])
        async with fast_publisher(js, flow_control=FlowControl(flow=10, max_outstanding_acks=1)) as publisher:
            acks = [await publisher.add("flowy.a", b"x") for _ in range(50)]
            await publisher.commit("flowy.a", b"end")

        # The opening ack reports nothing persisted yet; by the end the server
        # has confirmed nearly everything (it may ack more often than asked).
        assert acks[0].ack_sequence == 0
        assert acks[-1].ack_sequence >= 40
        assert publisher.ack_sequence >= 40

    async def test_small_ack_window_forces_the_stall_path(self, js: JetStreamContext) -> None:
        stream = await batch_stream(js, "STALL", ["stall.>"])
        # flow=1 + one outstanding window: every message waits for its own ack.
        control = FlowControl(flow=1, max_outstanding_acks=1, ack_timeout=10.0)
        async with fast_publisher(js, flow_control=control) as publisher:
            for index in range(50):
                ack = await publisher.add("stall.a", str(index))
                assert ack.ack_sequence == ack.batch_sequence or ack.batch_sequence == 1
            final = await publisher.commit("stall.a", b"end")

        assert final.size == 51
        assert (await stream.info()).state.messages == 51

    async def test_close_without_commit(self, js: JetStreamContext) -> None:
        stream = await batch_stream(js, "CLOSED", ["closed.>"])
        publisher = fast_publisher(js)
        await publisher.add("closed.a", b"one")
        await publisher.add("closed.a", b"two")

        ack = await publisher.close()

        assert ack is not None
        assert ack.size == 2  # the end-of-batch marker is not itself a message
        assert ack.stream == "CLOSED"
        assert publisher.is_closed
        assert (await stream.info()).state.messages == 2
        with pytest.raises(BatchClosedError):
            await publisher.add("closed.a", b"three")

    async def test_close_on_an_empty_batch_is_a_no_op(self, js: JetStreamContext) -> None:
        await batch_stream(js, "EMPTY", ["empty.>"])
        publisher = fast_publisher(js)
        assert await publisher.close() is None
        assert publisher.is_closed

    async def test_context_manager_closes_an_uncommitted_batch(self, js: JetStreamContext) -> None:
        stream = await batch_stream(js, "AUTO", ["auto.>"])
        async with fast_publisher(js) as publisher:
            for index in range(5):
                await publisher.add("auto.a", str(index))

        assert publisher.is_closed
        assert publisher.ack is not None
        assert publisher.ack.size == 5
        assert (await stream.info()).state.messages == 5

    async def test_commit_alone_publishes_a_one_message_batch(self, js: JetStreamContext) -> None:
        stream = await batch_stream(js, "SOLO", ["solo.>"])
        async with fast_publisher(js) as publisher:
            ack = await publisher.commit("solo.a", b"only")
        assert ack.size == 1
        assert (await stream.info()).state.messages == 1

    async def test_per_message_expectations_travel_with_the_batch(self, js: JetStreamContext) -> None:
        stream = await batch_stream(js, "EXPECT", ["expect.>"])
        async with fast_publisher(js) as publisher:
            await publisher.add("expect.a", b"one", expected_last_seq=0)
            ack = await publisher.commit("expect.a", b"two", expected_stream="EXPECT")
        assert ack.size == 2
        assert (await stream.info()).state.messages == 2

    async def test_stream_without_allow_batched_is_refused(self, js: JetStreamContext) -> None:
        await js.create_stream(StreamConfig(name="PLAIN", subjects=["plain.>"]))
        publisher = fast_publisher(js, flow_control=FlowControl(ack_timeout=5.0))
        with pytest.raises(FastBatchNotEnabledError):
            await publisher.add("plain.a", b"one")
        assert publisher.is_closed

    async def test_atomic_only_stream_is_refused(self, js: JetStreamContext) -> None:
        # allow_atomic is the *other* batching feature; it does not enable fast
        # ingest (same err_code as a stream with neither).
        await js.create_stream(StreamConfig(name="ATOMIC", subjects=["at.>"], allow_atomic=True))
        publisher = fast_publisher(js, flow_control=FlowControl(ack_timeout=5.0))
        with pytest.raises(FastBatchNotEnabledError):
            await publisher.add("at.a", b"one")

    async def test_typo_subject_fails_fast_with_no_stream_response(self, js: JetStreamContext) -> None:
        """A 503 on the ack inbox — a subject no stream captures, the likeliest
        user error — is the definitive answer, delivered in ~0ms. It used to be
        fed to the JSON parser, misreported as "not valid JSON", and then cost
        the whole ack_timeout. The generous deadline here makes a pass prove
        *promptness*, not merely eventual failure.
        """
        publisher = fast_publisher(js, flow_control=FlowControl(ack_timeout=60.0))
        loop = asyncio.get_running_loop()
        started = loop.time()
        with pytest.raises(NoStreamResponseError):
            async with asyncio.timeout(10.0):
                await publisher.add("nostream.typo", b"x")
        assert loop.time() - started < 5.0  # nowhere near the 60s ack_timeout
        # The 503 terminated the batch; close() is sticky and would re-raise it.
        assert publisher.is_closed

    async def test_client_close_wakes_a_parked_commit(self, nc: natsio.Client, js: JetStreamContext) -> None:
        # Termination discipline: a parked await must have a wake path from the
        # Closed lifecycle event, not just an eventual timeout — hence the
        # deliberately huge ack deadline.
        await batch_stream(js, "PARK", ["park.>"])
        publisher = fast_publisher(js, flow_control=FlowControl(ack_timeout=300.0))
        await publisher.add("park.a", b"one")

        # Commit onto a subject no stream captures: nothing will ever ack it.
        witness = nc.subscribe("nostream.a")
        commit = asyncio.ensure_future(publisher.commit("nostream.a", b"two"))
        await witness.next_msg(timeout=5.0)  # the commit frame is on the wire

        await nc.close()
        async with asyncio.timeout(5.0):
            with pytest.raises(ConnectionClosedError):
                await commit


class TestAsyncFailuresLive:
    async def test_rejected_message_ends_the_batch(self, js: JetStreamContext) -> None:
        stream = await batch_stream(js, "REJECT", ["reject.>"])
        collector = _Collector()
        publisher = fast_publisher(js, error_handler=collector)
        await publisher.add("reject.a", b"one")
        # A publish expectation that cannot hold: the server reports the failure
        # on the batch's inbox and then ends the batch.
        await publisher.add("reject.a", b"two", expected_last_seq=9999)

        rejected = await collector.wait_for(BatchMessageError)
        assert rejected.sequence == 2
        assert rejected.cause.err_code == 10071

        # ...and the batch is dead: the next call says so instead of silently
        # publishing into nothing.
        await collector.wait_until_closed(publisher)
        assert any(isinstance(error, BatchAbandonedError) for error in collector.errors)
        with pytest.raises(BatchAbandonedError):
            await publisher.add("reject.a", b"three")
        assert (await stream.info()).state.messages == 1

    async def test_rejected_message_fails_a_parked_commit(self, js: JetStreamContext) -> None:
        # Regression (review [high]): the realistic ordering. Messages are added
        # (each returns as soon as its frame is buffered), the caller parks on
        # commit, and only THEN does the server report the rejection and end the
        # batch — with a normal-shaped ack whose `count` is the only tell.
        # Returning that as a successful BatchAck reported data loss as success.
        stream = await batch_stream(js, "REJPARK", ["rejpark.>"])
        publisher = fast_publisher(js)
        await publisher.add("rejpark.a", b"one")
        await publisher.add("rejpark.a", b"two", expected_last_seq=9999)  # cannot hold
        await publisher.add("rejpark.a", b"three")

        with pytest.raises(BatchAbandonedError):
            await publisher.commit("rejpark.a", b"four")

        assert publisher.is_closed
        assert (await stream.info()).state.messages == 1

    async def test_gap_fails_a_parked_close(self, js: JetStreamContext) -> None:
        # Same defect through the end-of-batch terminator and the gap path.
        stream = await batch_stream(js, "GAPPARK", ["gappark.>"])
        publisher = fast_publisher(js)
        await publisher.add("gappark.a", b"one")
        publisher._sequence += 1  # deliberate hole; see the gap tests below
        await publisher.add("gappark.a", b"three")

        with pytest.raises(BatchAbandonedError):
            await publisher.close()

        assert (await stream.info()).state.messages == 1

    async def test_error_handler_cannot_drive_the_batch(self, js: JetStreamContext) -> None:
        # Regression (review [medium]): ending the batch from the error handler
        # ran on the ack-reader task and deadlocked it for a whole ack_timeout,
        # surfacing as a misleading ConcurrentBatchUseError. The deliberately
        # long deadline is the point: the guard must fire immediately.
        await batch_stream(js, "REENTER", ["reenter.>"])
        caught: list[Exception] = []
        seen = asyncio.Event()

        async def handler(error: Exception) -> None:
            if isinstance(error, BatchMessageError):
                try:
                    await publisher.close()
                except Exception as exc:
                    caught.append(exc)
                seen.set()

        publisher = fast_publisher(js, error_handler=handler, flow_control=FlowControl(ack_timeout=120.0))
        await publisher.add("reenter.a", b"one")
        await publisher.add("reenter.a", b"two", expected_last_seq=9999)

        async with asyncio.timeout(10.0):
            await seen.wait()
        assert isinstance(caught[0], BatchReentrantUseError)
        assert not isinstance(caught[0], ConcurrentBatchUseError)

    async def test_gap_in_fail_mode_ends_the_batch(self, js: JetStreamContext) -> None:
        stream = await batch_stream(js, "GAPFAIL", ["gapfail.>"])
        collector = _Collector()
        publisher = fast_publisher(js, error_handler=collector)
        await publisher.add("gapfail.a", b"one")
        # White-box: skip a batch sequence so the server sees a hole. Nothing in
        # the public API can drop a message on purpose, and the gap machinery is
        # worth exercising against the real server.
        publisher._sequence += 1
        await publisher.add("gapfail.a", b"three")

        await collector.wait_for(BatchGapError)
        await collector.wait_until_closed(publisher)
        assert (await stream.info()).state.messages == 1

    async def test_gap_in_continue_mode_keeps_the_batch_alive(self, js: JetStreamContext) -> None:
        stream = await batch_stream(js, "GAPOK", ["gapok.>"])
        collector = _Collector()
        publisher = fast_publisher(js, continue_on_gap=True, error_handler=collector)
        await publisher.add("gapok.a", b"one")
        publisher._sequence += 1  # same deliberate hole as above
        await publisher.add("gapok.a", b"three")

        reported = await collector.wait_for(BatchGapError)
        assert reported.sequence > reported.expected_last_sequence

        ack = await publisher.commit("gapok.a", b"four")
        assert ack.stream == "GAPOK"
        # Both real messages plus the commit landed; the batch survived the hole.
        assert (await stream.info()).state.messages == 3
