"""Unit tests for JetStream async-publish window bookkeeping and header building.

These exercise the sans-server machinery (stall cap, ack routing, drain/complete,
disconnect handling, header construction) against a lightweight fake client so the
logic is verified without a live nats-server.
"""

import asyncio
import json
from contextlib import suppress
from dataclasses import dataclass

import pytest

from natsio._internal.lifecycle import Closed, Disconnected
from natsio._internal.protocol import InlineStatus
from natsio.errors import ConfigError, ConnectionClosedError
from natsio.errors import TimeoutError as NATSTimeoutError
from natsio.jetstream import headers as js_headers
from natsio.jetstream.context import (
    AsyncPublishTimeoutError,
    JetStreamContext,
    TooManyStalledMsgsError,
    _build_publish_headers,
)
from natsio.jetstream.errors import NoStreamResponseError, WrongLastSequenceError
from natsio.message import Msg


@dataclass
class _FakeEvent:
    subject: str
    payload: bytes = b""
    status: InlineStatus | None = None


class _FakeEntry:
    def __init__(self, sid: int) -> None:
        self.sid = sid


class _FakeBus:
    def __init__(self) -> None:
        self.hooks: list = []

    def subscribe(self, hook):
        self.hooks.append(hook)

        def unsub() -> None:
            with suppress(ValueError):
                self.hooks.remove(hook)

        return unsub

    def emit(self, event) -> None:
        for hook in list(self.hooks):
            hook(event)


class _FakeConn:
    def __init__(self) -> None:
        self.bus = _FakeBus()
        self.subs: dict[int, tuple] = {}
        self._sid = 0

    def subscribe(self, subject, queue, handler):
        self._sid += 1
        self.subs[self._sid] = (subject, handler)
        return _FakeEntry(self._sid)


class _FakeClient:
    """Minimal Client surface used by JetStreamContext's async publisher."""

    def __init__(self) -> None:
        self._conn = _FakeConn()
        self.published: list[tuple] = []
        self.tasks: set[asyncio.Task] = set()
        self.publish_error: BaseException | None = None

    @property
    def inbox_prefix(self) -> str:
        return "_INBOX.testbox"

    async def publish(self, subject, payload=b"", *, reply=None, headers=None, _validate_reply=True) -> None:
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append((subject, payload, reply, headers))

    def _build_msg(self, event: _FakeEvent) -> Msg:
        return Msg(subject=event.subject, payload=event.payload, status=event.status)

    def _spawn(self, coro, *, name: str) -> asyncio.Task:
        task = asyncio.ensure_future(coro)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task


def _ctx(**kwargs) -> tuple[JetStreamContext, _FakeClient]:
    client = _FakeClient()
    ctx = JetStreamContext(client, **kwargs)  # ty: ignore[invalid-argument-type]
    return ctx, client


def _ack_event(reply: str, *, stream: str = "S", seq: int = 1, duplicate: bool = False) -> _FakeEvent:
    body = {"stream": stream, "seq": seq}
    if duplicate:
        body["duplicate"] = True
    return _FakeEvent(subject=reply, payload=json.dumps(body).encode())


def _error_event(reply: str, *, err_code: int, code: int = 400) -> _FakeEvent:
    body = {"error": {"code": code, "err_code": err_code, "description": "nope"}}
    return _FakeEvent(subject=reply, payload=json.dumps(body).encode())


def _no_responders_event(reply: str) -> _FakeEvent:
    return _FakeEvent(subject=reply, payload=b"", status=InlineStatus(503, "No Responders"))


class TestHeaderBuilding:
    def test_expected_last_subject_seq_subject_sets_header(self) -> None:
        extra = _build_publish_headers(
            msg_id=None,
            expected_stream=None,
            expected_last_seq=None,
            expected_last_subject_seq=5,
            expected_last_subject_seq_subject="a.*",
            expected_last_msg_id=None,
            ttl=None,
        )
        assert extra[js_headers.EXPECTED_LAST_SUBJECT_SEQUENCE] == "5"
        assert extra[js_headers.EXPECTED_LAST_SUBJECT_SEQUENCE_SUBJECT] == "a.*"

    def test_empty_subject_filter_rejected(self) -> None:
        with pytest.raises(ConfigError):
            _build_publish_headers(
                msg_id=None,
                expected_stream=None,
                expected_last_seq=None,
                expected_last_subject_seq=1,
                expected_last_subject_seq_subject="",
                expected_last_msg_id=None,
                ttl=None,
            )

    def test_subject_filter_without_sequence_rejected(self) -> None:
        """Review regression: nats.go couples the pair in one option; the
        subject filter alone is a wasted server round-trip."""
        with pytest.raises(ConfigError, match="requires expected_last_subject_seq"):
            _build_publish_headers(
                msg_id=None,
                expected_stream=None,
                expected_last_seq=None,
                expected_last_subject_seq=None,
                expected_last_subject_seq_subject="a.*",
                expected_last_msg_id=None,
                ttl=None,
            )

    def test_ttl_and_identity_headers(self) -> None:
        extra = _build_publish_headers(
            msg_id="id-1",
            expected_stream="S",
            expected_last_seq=3,
            expected_last_subject_seq=None,
            expected_last_subject_seq_subject=None,
            expected_last_msg_id="prev",
            ttl="never",
        )
        assert extra == {
            js_headers.MSG_ID: "id-1",
            js_headers.EXPECTED_STREAM: "S",
            js_headers.EXPECTED_LAST_SEQUENCE: "3",
            js_headers.EXPECTED_LAST_MSG_ID: "prev",
            js_headers.TTL: "never",
        }

    def test_ttl_below_one_rejected(self) -> None:
        with pytest.raises(ConfigError):
            _build_publish_headers(
                msg_id=None,
                expected_stream=None,
                expected_last_seq=None,
                expected_last_subject_seq=None,
                expected_last_subject_seq_subject=None,
                expected_last_msg_id=None,
                ttl=0,
            )


class TestScheduleConstants:
    def test_schedule_header_names(self) -> None:
        assert js_headers.SCHEDULE == "Nats-Schedule"
        assert js_headers.SCHEDULE_TARGET == "Nats-Schedule-Target"
        assert js_headers.SCHEDULE_SOURCE == "Nats-Schedule-Source"
        assert js_headers.SCHEDULE_TTL == "Nats-Schedule-TTL"
        assert js_headers.SCHEDULE_TIME_ZONE == "Nats-Schedule-Time-Zone"
        assert js_headers.SCHEDULER == "Nats-Scheduler"
        assert js_headers.SCHEDULE_NEXT == "Nats-Schedule-Next"


class TestAsyncPublishBookkeeping:
    async def test_pending_count_and_ack_resolution(self) -> None:
        ctx, client = _ctx()
        futures = [await ctx.publish_async(f"s.{i}") for i in range(3)]
        assert ctx.publish_async_pending == 3
        assert len(client.published) == 3

        # Resolve each via its own reply subject.
        for i, (_subject, _payload, reply, _headers) in enumerate(client.published):
            ctx._on_async_reply(_ack_event(reply, seq=i + 1))
        acks = await asyncio.gather(*futures)
        assert [a.seq for a in acks] == [1, 2, 3]
        assert ctx.publish_async_pending == 0

    async def test_wrong_seq_fails_only_that_future(self) -> None:
        ctx, client = _ctx()
        f0 = await ctx.publish_async("s.0")
        f1 = await ctx.publish_async("s.1")
        reply1 = client.published[1][2]
        ctx._on_async_reply(_error_event(reply1, err_code=10071))
        with pytest.raises(WrongLastSequenceError):
            await f1
        assert not f0.done()  # the sibling publish is untouched
        assert ctx.publish_async_pending == 1

    async def test_window_full_raises_stall(self) -> None:
        ctx, _client = _ctx(publish_async_max_pending=2, publish_async_stall_wait=0.05)
        await ctx.publish_async("s.0")
        await ctx.publish_async("s.1")
        with pytest.raises(TooManyStalledMsgsError):
            await ctx.publish_async("s.2")  # would push window to 3 > 2
        assert ctx.publish_async_pending == 2  # the stalled message was rolled back

    async def test_stall_clears_when_ack_drains_window(self) -> None:
        ctx, client = _ctx(publish_async_max_pending=2, publish_async_stall_wait=5.0)
        await ctx.publish_async("s.0")
        await ctx.publish_async("s.1")
        parked = asyncio.ensure_future(ctx.publish_async("s.2"))
        try:
            for _ in range(10):  # let the parked publish reach its stall wait
                await asyncio.sleep(0)
                if ctx.publish_async_pending >= 3:
                    break
            assert ctx.publish_async_pending == 3
            # Drain the window below the cap (2 acks: 3 -> 1 < max_pending=2);
            # the stall releases and the parked publish goes out.
            ctx._on_async_reply(_ack_event(client.published[0][2]))
            ctx._on_async_reply(_ack_event(client.published[1][2]))
            async with asyncio.timeout(1):
                await parked
            assert client.published[-1][0] == "s.2"
        finally:
            parked.cancel()

    async def test_publish_async_complete_drains(self) -> None:
        ctx, client = _ctx()
        await ctx.publish_async("s.0")
        await ctx.publish_async("s.1")
        waiter = asyncio.ensure_future(ctx.publish_async_complete())
        await asyncio.sleep(0)
        assert not waiter.done()
        for _subject, _payload, reply, _headers in client.published:
            ctx._on_async_reply(_ack_event(reply))
        async with asyncio.timeout(1):
            await waiter
        assert ctx.publish_async_pending == 0

    async def test_complete_returns_immediately_when_idle(self) -> None:
        ctx, _client = _ctx()
        async with asyncio.timeout(1):
            await ctx.publish_async_complete()

    async def test_complete_times_out_when_not_drained(self) -> None:
        ctx, _client = _ctx()
        await ctx.publish_async("s.0")
        with pytest.raises(NATSTimeoutError):
            await ctx.publish_async_complete(timeout=0.05)
        assert ctx.publish_async_pending == 1  # still outstanding

    async def test_disconnect_fails_all_pending(self) -> None:
        ctx, client = _ctx()
        f0 = await ctx.publish_async("s.0")
        f1 = await ctx.publish_async("s.1")
        client._conn.bus.emit(Disconnected(None))
        for fut in (f0, f1):
            with pytest.raises(ConnectionClosedError):
                await fut
        assert ctx.publish_async_pending == 0

    async def test_close_fails_pending_and_detaches_bus(self) -> None:
        ctx, client = _ctx()
        f0 = await ctx.publish_async("s.0")
        client._conn.bus.emit(Closed())
        with pytest.raises(ConnectionClosedError):
            await f0
        assert client._conn.bus.hooks == []  # bus hook unsubscribed on close

    async def test_no_responders_gives_up_after_retries(self) -> None:
        ctx, client = _ctx()
        fut = await ctx.publish_async("s.0")
        token = next(iter(ctx._acks))
        ctx._acks[token].retries_remaining = 0  # simulate retries exhausted
        ctx._on_async_reply(_no_responders_event(client.published[0][2]))
        with pytest.raises(NoStreamResponseError):
            await fut

    async def test_no_responders_retries_then_acks(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import natsio.jetstream.context as context_module

        monkeypatch.setattr(context_module, "_PUBLISH_RETRY_WAIT", 0.0)
        ctx, client = _ctx()
        fut = await ctx.publish_async("s.0")
        reply = client.published[0][2]
        ctx._on_async_reply(_no_responders_event(reply))  # triggers a resend
        for _ in range(10):  # let the spawned retry task republish
            await asyncio.sleep(0)
            if len(client.published) >= 2:
                break
        assert client.published[1][0] == "s.0"  # same message re-sent on same reply
        assert client.published[1][2] == reply
        ctx._on_async_reply(_ack_event(reply, seq=7))
        assert (await fut).seq == 7

    async def test_ack_timeout_fails_future(self) -> None:
        ctx, _client = _ctx(publish_async_timeout=0.05)
        fut = await ctx.publish_async("s.0")
        with pytest.raises(AsyncPublishTimeoutError):
            async with asyncio.timeout(1):
                await fut
        assert ctx.publish_async_pending == 0

    async def test_publish_frame_failure_rolls_back(self) -> None:
        ctx, client = _ctx()
        client.publish_error = ConnectionClosedError("down")
        with pytest.raises(ConnectionClosedError):
            await ctx.publish_async("s.0")
        assert ctx.publish_async_pending == 0  # no leaked pending ack
