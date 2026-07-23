"""End-to-end schedule tests against a real nats-server (pinned 2.14.3) with -js.

Everything here is event-driven: deliveries are awaited through a pull
consumer's `next()`, never slept for. Schedules use 1-second granularity (the
server minimum) so the suite stays fast.
"""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from natsio.schedules import (  # ty: ignore[unresolved-import]
    HOURLY,
    MAX_SUBJECTS_PER_BATCH,
    MessageSchedulesDisabledError,
    MirrorWithMsgSchedulesError,
    ScheduleExpressionError,
    ScheduleNotFoundError,
    SchedulePatternInvalidError,
    SchedulerInvalidError,
    ScheduleRollupInvalidError,
    Schedules,
    SchedulesNotEnabledError,
    ScheduleSourceInvalidError,
    ScheduleStreamConfig,
    ScheduleTargetError,
    ScheduleTargetInvalidError,
    ScheduleTimeZoneInvalidError,
    ScheduleTTLInvalidError,
    after,
    at,
    create_schedule_stream,
    cron,
    delivery_info,
    every,
    is_scheduled,
    parse_schedule,
    publish_schedule,
    schedules,
    schedules_from_stream,
)

import natsio
from natsio.jetstream import (
    APIError,
    Consumer,
    ConsumerConfig,
    MessageNotFoundError,
    StreamConfig,
    StreamNotFoundError,
    StreamSource,
    WrongLastSequenceError,
)

SCHEDULE_SUBJECTS = "schedules.>"
TARGET_SUBJECTS = "target.>"


@pytest.fixture
async def sched(nc: natsio.Client) -> Schedules:
    js = nc.jetstream()
    return await create_schedule_stream(
        js,
        ScheduleStreamConfig(name="SCHED", subjects=[SCHEDULE_SUBJECTS, TARGET_SUBJECTS]),
    )


@pytest.fixture
async def target(sched: Schedules) -> Consumer:
    """A pull consumer over every generated (target) message."""
    return await sched.stream.create_consumer(ConsumerConfig(name="TARGETS", filter_subject=TARGET_SUBJECTS))


class TestLifecycle:
    async def test_create_sets_the_adr_flags(self, sched: Schedules) -> None:
        config = sched.stream.cached_info.config
        assert config.allow_msg_schedules is True
        assert config.allow_direct is True
        # The server implies rollup support and clears deny_purge for schedule streams.
        assert config.allow_rollup_hdrs is True
        assert config.deny_purge is False

    async def test_bind_existing(self, nc: natsio.Client, sched: Schedules) -> None:
        bound = await schedules(nc.jetstream(), "SCHED")
        assert bound.stream_name == "SCHED"

    async def test_bind_missing_stream(self, nc: natsio.Client) -> None:
        with pytest.raises(ScheduleNotFoundError):
            await schedules(nc.jetstream(), "NOPE")

    async def test_plain_stream_rejected(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        stream = await js.create_stream(StreamConfig(name="PLAIN", subjects=["p.>"], allow_direct=True))
        with pytest.raises(SchedulesNotEnabledError):
            schedules_from_stream(js, stream)

    async def test_from_stream_optional_await(self, nc: natsio.Client, sched: Schedules) -> None:
        js = nc.jetstream()
        stream = await js.stream("SCHED")
        # The factory does no I/O; `await` on the handle is a tolerated no-op.
        handle = await schedules_from_stream(js, stream)
        assert handle.stream_name == "SCHED"


class TestOneShotDelivery:
    async def test_at_definition_is_stored_verbatim(self, sched: Schedules) -> None:
        # Far enough out that the definition cannot fire (and purge itself)
        # while we are reading it back — the delivery path is tested below.
        when = datetime.now(UTC) + timedelta(hours=1)
        ack = await sched.create(
            "schedules.later", at(when), target="target.later", payload=b"hello", ttl=timedelta(minutes=5)
        )
        assert ack.seq >= 1

        entry = await sched.get("schedules.later")
        assert entry.schedule == at(when).value
        assert entry.target == "target.later"
        assert entry.ttl == "5m0s"
        assert entry.is_one_shot is True
        assert entry.fires_at == when.replace(microsecond=0)
        # The server auto-applies the rollup that keeps one definition per subject.
        assert entry.headers is not None
        assert entry.headers.get("Nats-Rollup") == "sub"

    async def test_at_fires_and_purges_itself(self, sched: Schedules, target: Consumer) -> None:
        when = datetime.now(UTC) + timedelta(seconds=1)
        ack = await sched.create(
            "schedules.at", at(when), target="target.at", payload=b"hello", ttl=timedelta(minutes=5)
        )
        assert ack.seq >= 1

        msg = await target.next(timeout=10.0)
        await msg.ack()
        assert msg.subject == "target.at"
        assert msg.payload == b"hello"
        assert is_scheduled(msg) is True
        info = delivery_info(msg)
        assert info is not None
        assert info.scheduler == "schedules.at"
        assert info.final is True
        assert info.next_run is None
        assert info.ttl == "5m0s"

        # A one-shot schedule removes itself once it has fired.
        with pytest.raises(ScheduleNotFoundError):
            await sched.get("schedules.at")

    async def test_after_helper(self, sched: Schedules, target: Consumer) -> None:
        await sched.create("schedules.soon", after(timedelta(seconds=1)), target="target.soon", payload=b"soon")
        msg = await target.next(timeout=10.0)
        await msg.ack()
        assert msg.payload == b"soon"

    async def test_past_at_fires_immediately(self, sched: Schedules, target: Consumer) -> None:
        await sched.create(
            "schedules.past", at(datetime(2000, 1, 1, tzinfo=UTC)), target="target.past", payload=b"late"
        )
        msg = await target.next(timeout=10.0)
        await msg.ack()
        assert msg.payload == b"late"

    async def test_user_headers_travel_to_the_target(self, sched: Schedules, target: Consumer) -> None:
        await sched.create(
            "schedules.hdr",
            after(timedelta(seconds=1)),
            target="target.hdr",
            payload=b"x",
            headers={"X-Trace": "abc123"},
        )
        msg = await target.next(timeout=10.0)
        await msg.ack()
        assert msg.headers is not None
        assert msg.headers.get("X-Trace") == "abc123"


class TestRecurringDelivery:
    async def test_every_repeats(self, sched: Schedules, target: Consumer) -> None:
        await sched.create("schedules.every", every(timedelta(seconds=1)), target="target.every", payload=b"tick")

        first = await target.next(timeout=10.0)
        await first.ack()
        second = await target.next(timeout=10.0)
        await second.ack()

        for msg in (first, second):
            assert msg.subject == "target.every"
            assert msg.payload == b"tick"
        one, two = delivery_info(first), delivery_info(second)
        assert one is not None
        assert two is not None
        assert one.scheduler == two.scheduler == "schedules.every"
        assert one.final is two.final is False
        assert one.next_run is not None
        assert two.next_run is not None
        assert two.next_run > one.next_run

        # The definition survives its firings.
        entry = await sched.get("schedules.every")
        assert entry.interval == timedelta(seconds=1)

    async def test_cron_every_second(self, sched: Schedules, target: Consumer) -> None:
        await sched.create("schedules.cron", cron("* * * * * *"), target="target.cron", payload=b"cron")
        msg = await target.next(timeout=10.0)
        await msg.ack()
        assert msg.payload == b"cron"
        info = delivery_info(msg)
        assert info is not None
        assert info.next_run is not None

    async def test_source_sampling(self, nc: natsio.Client, sched: Schedules, target: Consumer) -> None:
        js = nc.jetstream()
        await js.publish("target.src", b"sample-1")
        await sched.create(
            "schedules.sampled",
            every(timedelta(seconds=1)),
            target="target.sampled",
            source="target.src",
            payload=b"fallback",
        )
        while True:
            msg = await target.next(timeout=10.0)
            await msg.ack()
            if msg.subject == "target.sampled":
                break
        # The source's latest body wins over the schedule's own payload.
        assert msg.payload == b"sample-1"

    async def test_rollup_stamped_on_generated_message(self, sched: Schedules, target: Consumer) -> None:
        await sched.create(
            "schedules.rollup", after(timedelta(seconds=1)), target="target.rollup", payload=b"r", rollup=True
        )
        msg = await target.next(timeout=10.0)
        await msg.ack()
        info = delivery_info(msg)
        assert info is not None
        assert info.rollup == "sub"


class TestReadAndCancel:
    async def test_list_only_returns_definitions(self, nc: natsio.Client, sched: Schedules) -> None:
        await sched.create("schedules.a", HOURLY, target="target.a", payload=b"a")
        await sched.create("schedules.b", every("1h"), target="target.b", payload=b"b")
        # Ordinary traffic on the same stream must not be mistaken for a schedule.
        await nc.jetstream().publish("target.noise", b"noise")

        found = {entry.subject: entry async for entry in sched.list()}
        assert set(found) == {"schedules.a", "schedules.b"}
        assert found["schedules.a"].schedule == "@hourly"
        assert found["schedules.a"].target == "target.a"
        assert found["schedules.a"].payload == b"a"
        assert found["schedules.b"].schedule == "@every 1h"

    async def test_list_on_an_empty_stream_is_empty(self, sched: Schedules) -> None:
        """The server answers a batch matching nothing with a lone `404 No
        Results` and no EOB. That is an empty result, not an error and not a
        timeout — core terminates on it cleanly."""
        loop = asyncio.get_running_loop()
        started = loop.time()
        assert [entry async for entry in sched.list()] == []
        assert loop.time() - started < 2.0

    async def test_list_narrowed_by_filter(self, sched: Schedules) -> None:
        await sched.create("schedules.a", HOURLY, target="target.a")
        await sched.create("schedules.b", HOURLY, target="target.b")
        subjects = [entry.subject async for entry in sched.list("schedules.a")]
        assert subjects == ["schedules.a"]

    async def test_list_pages_past_the_server_subject_cap(self, sched: Schedules) -> None:
        """ADR-51 gives every schedule its own subject, so this is the intended layout.

        One batch Direct Get cannot answer more than `MAX_SUBJECTS_PER_BATCH`
        matching subjects; the server refuses the whole request. `list()` must
        page around that — it used to hand back an empty set, silently.
        """
        count = MAX_SUBJECTS_PER_BATCH + 101
        subjects = [f"schedules.bulk.{index:05d}" for index in range(count)]
        for start in range(0, count, 100):
            await asyncio.gather(
                *(sched.create(subject, HOURLY, target="target.bulk") for subject in subjects[start : start + 100])
            )

        found = [entry.subject async for entry in sched.list("schedules.bulk.>")]
        assert len(found) == count
        assert sorted(found) == subjects
        # ...and the stream's own subjects (schedules + targets) enumerate too.
        assert len([entry async for entry in sched.list()]) == count

    async def test_list_and_get_return_the_same_entry(self, sched: Schedules) -> None:
        await sched.create("schedules.same", HOURLY, target="target.same", payload=b"p", headers={"X-Trace": "t1"})
        fetched = await sched.get("schedules.same")
        # Direct Get stamps different transport metadata on single and batch
        # replies; neither belongs to the definition.
        assert [entry async for entry in sched.list("schedules.same")] == [fetched]
        assert fetched.headers is not None
        assert fetched.headers.get("X-Trace") == "t1"
        assert "Nats-Sequence" not in fetched.headers
        assert "Nats-Num-Pending" not in fetched.headers

    async def test_default_list_reads_subjects_live_not_from_the_bound_snapshot(
        self, nc: natsio.Client, sched: Schedules
    ) -> None:
        """A subject added to the stream after this handle was bound must still
        enumerate. `cached_info` is a bind-time snapshot; reading it on the
        default path silently dropped schedules on later-added subjects."""
        await sched.create("schedules.old", HOURLY, target="target.old")

        # Widen the stream *after* the handle was bound. Preserve every other
        # field (allow_msg_ttl etc.) — reconstructing bare would try to disable
        # them and the server refuses.
        info = await sched.stream.info()
        widened = replace(info.config, subjects=[*(info.config.subjects or ()), "more.>"])
        await nc.jetstream().update_stream(widened)
        await sched.create("more.new", HOURLY, target="target.new")

        # The same handle, default enumeration, must see the new subject.
        found = {entry.subject async for entry in sched.list()}
        assert found == {"schedules.old", "more.new"}

    async def test_get_missing(self, sched: Schedules) -> None:
        with pytest.raises(ScheduleNotFoundError):
            await sched.get("schedules.nothing")

    async def test_get_on_a_deleted_stream_is_not_a_missing_schedule(self, nc: natsio.Client, sched: Schedules) -> None:
        """Core reuses `MessageNotFoundError` for "direct get unavailable" (a
        gone stream), so `get()` used to report an infra failure as "no such
        schedule" — the caller would then recreate a duplicate or treat a cancel
        as confirmed. It must surface the real fault instead."""
        await sched.create("schedules.here", HOURLY, target="target.here")
        await nc.jetstream().delete_stream(sched.stream_name)
        with pytest.raises(StreamNotFoundError):
            await sched.get("schedules.here")

    async def test_get_non_schedule_message(self, nc: natsio.Client, sched: Schedules) -> None:
        await nc.jetstream().publish("target.plain", b"plain")
        with pytest.raises(ScheduleNotFoundError):
            await sched.get("target.plain")

    async def test_replace_definition(self, sched: Schedules) -> None:
        await sched.create("schedules.dup", HOURLY, target="target.dup")
        await sched.create("schedules.dup", every("1h"), target="target.dup")
        entry = await sched.get("schedules.dup")
        assert entry.schedule == "@every 1h"
        # Rollup keeps exactly one definition per subject.
        assert len([e async for e in sched.list("schedules.dup")]) == 1

    async def test_cas_create(self, sched: Schedules) -> None:
        await sched.create("schedules.cas", HOURLY, target="target.cas", expected_last_subject_seq=0)
        with pytest.raises(WrongLastSequenceError):
            await sched.create("schedules.cas", HOURLY, target="target.cas", expected_last_subject_seq=0)

    async def test_cancel(self, sched: Schedules, target: Consumer) -> None:
        await sched.create("schedules.gone", every(timedelta(seconds=1)), target="target.gone", payload=b"g")
        first = await target.next(timeout=10.0)
        await first.ack()

        assert await sched.cancel("schedules.gone") == 1
        with pytest.raises(ScheduleNotFoundError):
            await sched.get("schedules.gone")

    async def test_cancel_missing_is_loud(self, sched: Schedules) -> None:
        with pytest.raises(ScheduleNotFoundError):
            await sched.cancel("schedules.never-existed")

    async def test_cancel_refuses_a_non_schedule_message(self, nc: natsio.Client, sched: Schedules) -> None:
        # Target subjects share the stream: a mistyped cancel must not purge
        # user data and report success.
        await nc.jetstream().publish("target.keepme", b"user data")
        with pytest.raises(ScheduleNotFoundError):
            await sched.cancel("target.keepme")
        assert (await sched.stream.get_msg(subject="target.keepme")).payload == b"user data"

    async def test_cancel_rejects_wildcards(self, sched: Schedules) -> None:
        with pytest.raises(ScheduleTargetError):
            await sched.cancel("schedules.>")

    async def test_cancel_many(self, sched: Schedules) -> None:
        await sched.create("schedules.bulk.a", HOURLY, target="target.a")
        await sched.create("schedules.bulk.b", HOURLY, target="target.b")
        assert await sched.cancel_many("schedules.bulk.*") == 2
        assert [entry async for entry in sched.list("schedules.bulk.*")] == []

    async def test_cancel_by_sequence(self, sched: Schedules) -> None:
        ack = await sched.create("schedules.seq", HOURLY, target="target.seq")
        await sched.cancel_by_sequence(ack.seq)
        with pytest.raises(ScheduleNotFoundError):
            await sched.get("schedules.seq")


class TestStopAndPublish:
    async def test_atomic_stop(self, sched: Schedules, target: Consumer) -> None:
        # A delayed publish far enough out that only the explicit stop can fire it.
        await sched.create("schedules.delayed", after(timedelta(hours=1)), target="target.delayed", payload=b"early")

        ack = await sched.stop_and_publish("schedules.delayed", publish_to="target.delayed", payload=b"early-now")
        assert ack.seq >= 1

        with pytest.raises(ScheduleNotFoundError):
            await sched.get("schedules.delayed")
        msg = await target.next(timeout=10.0)
        await msg.ack()
        assert msg.payload == b"early-now"

    async def test_cas_gated_stop(self, sched: Schedules) -> None:
        await sched.create("schedules.cas2", after(timedelta(hours=1)), target="target.cas2")
        entry = await sched.get("schedules.cas2")

        # A stale sequence must not cancel anything.
        with pytest.raises(WrongLastSequenceError):
            await sched.stop_and_publish(
                "schedules.cas2", publish_to="schedules.cancelled", expected_schedule_seq=entry.sequence - 1
            )
        assert (await sched.get("schedules.cas2")).sequence == entry.sequence

        await sched.stop_and_publish(
            "schedules.cas2", publish_to="schedules.cancelled", expected_schedule_seq=entry.sequence
        )
        with pytest.raises(ScheduleNotFoundError):
            await sched.get("schedules.cas2")

    async def test_missing_schedule_is_loud(self, sched: Schedules) -> None:
        # The server accepts a stop for a schedule that is not there (it just
        # publishes and cancels nothing); `require_existing` turns that into
        # the same ScheduleNotFoundError `cancel` raises.
        with pytest.raises(ScheduleNotFoundError):
            await sched.stop_and_publish("schedules.ghost", publish_to="target.ghost", payload=b"x")
        with pytest.raises(MessageNotFoundError):
            await sched.stream.get_msg(subject="target.ghost")

    async def test_fire_and_forget_skips_the_existence_check(self, sched: Schedules) -> None:
        ack = await sched.stop_and_publish(
            "schedules.ghost", publish_to="target.ghost", payload=b"x", require_existing=False
        )
        assert ack.seq >= 1
        assert (await sched.stream.get_msg(subject="target.ghost")).payload == b"x"

    async def test_self_reference_rejected_locally(self, sched: Schedules) -> None:
        # The server answers err_code 10212; we never get there.
        with pytest.raises(ScheduleTargetError, match="10212"):
            await sched.stop_and_publish("schedules.self", publish_to="schedules.self")


class TestServerRejections:
    """The ADR-51 err_codes, bound to typed errors through the core's `register_error` hook."""

    async def test_target_outside_stream(self, sched: Schedules) -> None:
        with pytest.raises(ScheduleTargetInvalidError) as excinfo:
            await sched.create("schedules.bad", HOURLY, target="nowhere.else")
        assert excinfo.value.err_code == 10190

    async def test_source_outside_stream(self, sched: Schedules) -> None:
        with pytest.raises(ScheduleSourceInvalidError) as excinfo:
            await sched.create("schedules.badsrc", HOURLY, target="target.ok", source="nowhere.else")
        assert excinfo.value.err_code == 10203

    async def test_unknown_time_zone(self, sched: Schedules) -> None:
        with pytest.raises(ScheduleTimeZoneInvalidError) as excinfo:
            await sched.create("schedules.badtz", HOURLY, target="target.ok", time_zone="Mars/Phobos")
        assert excinfo.value.err_code == 10223

    async def test_bad_ttl(self, nc: natsio.Client, sched: Schedules) -> None:
        with pytest.raises(ScheduleTTLInvalidError) as excinfo:
            await nc.jetstream().publish(
                "schedules.badttl",
                b"",
                headers={
                    "Nats-Schedule": "@hourly",
                    "Nats-Schedule-Target": "target.ok",
                    "Nats-Schedule-TTL": "nonsense",
                },
            )
        assert excinfo.value.err_code == 10191

    async def test_bad_rollup(self, nc: natsio.Client, sched: Schedules) -> None:
        with pytest.raises(ScheduleRollupInvalidError) as excinfo:
            await nc.jetstream().publish(
                "schedules.badrollup",
                b"",
                headers={
                    "Nats-Schedule": "@hourly",
                    "Nats-Schedule-Target": "target.ok",
                    "Nats-Schedule-Rollup": "all",
                },
            )
        assert excinfo.value.err_code == 10192

    async def test_schedules_disabled_on_stream(self, nc: natsio.Client) -> None:
        js = nc.jetstream()
        await js.create_stream(StreamConfig(name="PLAIN", subjects=["p.>"]))
        with pytest.raises(MessageSchedulesDisabledError) as excinfo:
            await publish_schedule(js, "p.one", HOURLY, target="p.two")
        assert excinfo.value.err_code == 10188

    async def test_scheduler_self_reference(self, nc: natsio.Client, sched: Schedules) -> None:
        # The header pair the client-side guard prevents; verified straight on the wire.
        with pytest.raises(SchedulerInvalidError) as excinfo:
            await nc.jetstream().publish(
                "schedules.self",
                b"",
                headers={"Nats-Schedule-Next": "purge", "Nats-Scheduler": "schedules.self"},
            )
        assert excinfo.value.err_code == 10212

    async def test_mirror_cannot_schedule(self, nc: natsio.Client, sched: Schedules) -> None:
        with pytest.raises(MirrorWithMsgSchedulesError) as excinfo:
            await nc.jetstream().create_stream(
                StreamConfig(name="MIRROR", mirror=StreamSource(name="SCHED"), allow_msg_schedules=True)
            )
        assert excinfo.value.err_code == 10186

    async def test_typed_errors_are_api_errors(self, sched: Schedules) -> None:
        # Callers that only know the core's type still catch these.
        with pytest.raises(APIError):
            await sched.create("schedules.bad2", HOURLY, target="nowhere.else")

    async def test_publish_schedule_function(self, nc: natsio.Client, sched: Schedules) -> None:
        # The free function is the same wire path as the handle method.
        await publish_schedule(nc.jetstream(), "schedules.fn", HOURLY, target="target.fn", payload=b"fn")
        assert (await sched.get("schedules.fn")).payload == b"fn"


PARITY_ACCEPTED = [
    "@at 2030-01-01T00:00:00Z",
    "@every 1s",
    "@every 1m30s",
    "@every 1.5s",
    "@every 90s",
    "@yearly",
    "@annually",
    "@monthly",
    "@weekly",
    "@daily",
    "@midnight",
    "@hourly",
    "0 30 * * * *",
    "* * * * * *",
    "0 0 0-23/2 * * *",
    "0 0 */2 * * *",
    "0 0 3/2 * * *",
    "0 0 0 * jan-mar mon,wed,fri",
    "0 0 0 ? * *",
    "0 0 0 * * 6",
    # A leading `*`/`?` is the whole range and swallows the rest of the range
    # expression — the server's parser accepts these, so the validator must too.
    "*-* * * * * *",
    "?-? * * * * *",
    "*-5 * * * * *",
    # Reachable day-of-month/month pairs, including the leap-year case and the
    # day-of-week escape (the server matches day-of-month OR day-of-week there).
    "0 0 0 29 2 *",
    "0 0 0 31 2,3 *",
    "0 0 0 31 2 mon",
]

# NOTE: an *empty* `Nats-Schedule` is deliberately not in this list. The server
# accepts it (the message is simply stored without a schedule);
# `natsio.schedules` rejects it locally, because silently publishing a
# non-schedule from a schedule API is exactly the drop this client refuses.
PARITY_REJECTED = [
    "@every 500ms",
    "@every 0s",
    "@every",
    "@EVERY 1m",
    "@Hourly",
    "@AT 2030-01-01T00:00:00Z",
    "30 * * * *",
    "0 30 * * * * *",
    "0 60 * * * *",
    "0 0 24 * * *",
    "0 0 0 0 * *",
    "0 0 0 32 * *",
    "0 0 0 * 13 *",
    "0 0 0 * * 7",
    "5-* * * * * *",  # only the low end of a range may be `*`
    # Unreachable day-of-month/month pairs: the server refuses a schedule that
    # can never fire (err_code 10189).
    "0 0 0 31 2 *",
    "0 0 0 30 2 *",
    "0 0 0 31 4 *",
    "0 0 0 31 feb *",
    "0 0 0 30,31 2 *",
    "0 0 0 30-31 2 *",
]


class TestGrammarParity:
    """The local validator's verdict must match the pinned server's, expression by expression.

    Accepted expressions must be accepted by the server; rejected ones must be
    rejected by it too. This is the regression net for the cron/duration
    grammar — it is the only thing standing between a hand-written validator
    and quietly diverging from the oracle.
    """

    @pytest.mark.parametrize("expr", PARITY_ACCEPTED)
    async def test_locally_accepted_is_server_accepted(self, sched: Schedules, expr: str) -> None:
        schedule = parse_schedule(expr)
        await sched.create("schedules.parity", schedule, target="target.parity")
        assert (await sched.get("schedules.parity")).schedule == schedule.value

    @pytest.mark.parametrize("expr", PARITY_REJECTED)
    async def test_locally_rejected_is_server_rejected(self, nc: natsio.Client, sched: Schedules, expr: str) -> None:
        with pytest.raises(ScheduleExpressionError):
            await sched.create("schedules.parity", expr, target="target.parity")
        # ...and the raw header, published past the validator, is rejected too.
        with pytest.raises(SchedulePatternInvalidError):
            await nc.jetstream().publish(
                "schedules.parity",
                b"",
                headers={"Nats-Schedule": expr, "Nats-Schedule-Target": "target.parity"},
            )
