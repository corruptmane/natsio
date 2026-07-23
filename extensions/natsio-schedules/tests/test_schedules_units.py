"""Wire-contract unit tests: exact emitted strings and every rejection path.

The expected values are the bytes nats.go emits (``jetstream/jetstream_options.go``
`WithScheduleAt` / `WithScheduleEvery` / `WithScheduleCron`) and the header
names from ``jetstream/message.go``. The accept/reject boundary for cron,
durations and time zones was additionally probed against the pinned
nats-server 2.14.3 — see ``test_schedules_live.py::TestGrammarParity``.
"""

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest
from natsio.schedules import (  # ty: ignore[unresolved-import]
    ANNUALLY,
    DAILY,
    HOURLY,
    MIDNIGHT,
    MONTHLY,
    PREDEFINED,
    WEEKLY,
    YEARLY,
    Schedule,
    ScheduleConfigError,
    ScheduleDelivery,
    ScheduleEntry,
    ScheduleExpressionError,
    ScheduleNotFoundError,
    Schedules,
    ScheduleSourceError,
    ScheduleTargetError,
    ScheduleTimeZoneError,
    ScheduleTTLError,
    after,
    at,
    build_schedule_headers,
    cron,
    delivery_info,
    encode_schedule_ttl,
    every,
    format_go_duration,
    is_scheduled,
    parse_go_duration,
    parse_schedule,
)
from natsio.schedules.manager import MAX_SUBJECTS_PER_BATCH  # ty: ignore[unresolved-import]

from natsio._internal.protocol import Headers
from natsio.errors import ConfigError
from natsio.jetstream import (
    APIError,
    JetStreamError,
    StoredMsg,
    Stream,
    StreamConfig,
    StreamInfo,
    StreamState,
)


class TestAt:
    def test_utc(self) -> None:
        when = datetime(2009, 11, 10, 23, 0, 0, tzinfo=UTC)
        assert at(when).value == "@at 2009-11-10T23:00:00Z"
        assert at(when).is_cron is False

    def test_offset_is_converted_to_utc(self) -> None:
        when = datetime(2030, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=2)))
        assert at(when).value == "@at 2029-12-31T22:00:00Z"

    def test_microseconds_truncated_like_go_rfc3339(self) -> None:
        when = datetime(2030, 1, 1, 0, 0, 0, 999_999, tzinfo=UTC)
        assert at(when).value == "@at 2030-01-01T00:00:00Z"

    def test_naive_datetime_rejected(self) -> None:
        with pytest.raises(ScheduleExpressionError, match="timezone-aware"):
            at(datetime(2030, 1, 1, 0, 0, 0))  # noqa: DTZ001  # the point of the test

    def test_past_is_allowed(self) -> None:
        # ADR-51: a past @at fires immediately; that is a supported use, not an error.
        assert at(datetime(2000, 1, 1, tzinfo=UTC)).value == "@at 2000-01-01T00:00:00Z"

    def test_after_is_relative_to_now(self) -> None:
        schedule = after(timedelta(seconds=30))
        stamp = datetime.fromisoformat(schedule.value[len("@at ") :])
        assert timedelta(seconds=28) <= stamp - datetime.now(UTC) <= timedelta(seconds=31)

    def test_after_negative_rejected(self) -> None:
        with pytest.raises(ScheduleExpressionError, match="negative"):
            after(timedelta(seconds=-1))


class TestEvery:
    @pytest.mark.parametrize(
        ("interval", "expected"),
        [
            (timedelta(seconds=1), "@every 1s"),
            (timedelta(seconds=5), "@every 5s"),
            (timedelta(seconds=90), "@every 1m30s"),
            (timedelta(minutes=1), "@every 1m0s"),
            (timedelta(hours=1), "@every 1h0m0s"),
            (timedelta(hours=1, minutes=2, seconds=3), "@every 1h2m3s"),
            (timedelta(seconds=1, milliseconds=500), "@every 1.5s"),
        ],
    )
    def test_timedelta_uses_go_duration_string(self, interval: timedelta, expected: str) -> None:
        assert every(interval).value == expected
        assert every(interval).is_cron is False

    @pytest.mark.parametrize("text", ["1s", "90s", "1m30s", "1h", "1500ms", "1.5s"])
    def test_string_passes_through_verbatim(self, text: str) -> None:
        assert every(text).value == f"@every {text}"

    @pytest.mark.parametrize("interval", [timedelta(milliseconds=500), timedelta(0), timedelta(seconds=-5)])
    def test_sub_second_rejected(self, interval: timedelta) -> None:
        with pytest.raises(ScheduleExpressionError, match="at least 1s"):
            every(interval)

    @pytest.mark.parametrize("text", ["500ms", "0s", "0"])
    def test_sub_second_string_rejected(self, text: str) -> None:
        with pytest.raises(ScheduleExpressionError, match="at least 1s"):
            every(text)

    @pytest.mark.parametrize("text", ["", "  ", "1", "abc", "1x", "s", "1m30", "."])
    def test_malformed_string_rejected(self, text: str) -> None:
        with pytest.raises(ScheduleExpressionError):
            every(text)


class TestGoDuration:
    @pytest.mark.parametrize(
        ("interval", "expected"),
        [
            (timedelta(0), "0s"),
            (timedelta(seconds=1), "1s"),
            (timedelta(seconds=59), "59s"),
            (timedelta(seconds=60), "1m0s"),
            (timedelta(seconds=3600), "1h0m0s"),
            (timedelta(seconds=3661), "1h1m1s"),
            (timedelta(days=1), "24h0m0s"),
            (timedelta(seconds=2, microseconds=500_000), "2.5s"),
            (timedelta(seconds=1, microseconds=1), "1.000001s"),
            (timedelta(seconds=-90), "-1m30s"),
        ],
    )
    def test_format(self, interval: timedelta, expected: str) -> None:
        assert format_go_duration(interval) == expected

    def test_format_rejects_sub_second(self) -> None:
        with pytest.raises(ScheduleExpressionError, match="sub-second"):
            format_go_duration(timedelta(milliseconds=1))

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("0", timedelta(0)),
            ("1s", timedelta(seconds=1)),
            ("1m30s", timedelta(seconds=90)),
            ("1h", timedelta(hours=1)),
            ("1.5s", timedelta(seconds=1.5)),
            ("1500ms", timedelta(seconds=1.5)),
            ("2h45m", timedelta(hours=2, minutes=45)),
            ("-1m", timedelta(minutes=-1)),
            ("+30s", timedelta(seconds=30)),
            ("1000us", timedelta(milliseconds=1)),
            ("1000µs", timedelta(milliseconds=1)),
            ("1000μs", timedelta(milliseconds=1)),
        ],
    )
    def test_parse(self, text: str, expected: timedelta) -> None:
        assert parse_go_duration(text) == expected

    @pytest.mark.parametrize("text", ["", "1", "s", "1sm", "1h2", "--1s", "1..5s", "."])
    def test_parse_rejects(self, text: str) -> None:
        with pytest.raises(ScheduleExpressionError):
            parse_go_duration(text)

    def test_parse_rejects_sub_microsecond(self) -> None:
        with pytest.raises(ScheduleExpressionError, match="sub-microsecond"):
            parse_go_duration("500ns")


class TestCron:
    @pytest.mark.parametrize(
        "expr",
        [
            "0 30 * * * *",
            "* * * * * *",
            "0 0 5 * * *",
            "0 0 0-23/2 * * *",
            "0 0 */2 * * *",
            "0 0 3/2 * * *",
            "0 0 0 * jan-mar mon,wed,fri",
            "0 0 0 * JAN-MAR MON,WED,FRI",
            "0 0 0 ? * *",
            "0 0 0 * * ?",
            "0 0 0 * * 6",
            "0 0 0 1 1 *",
            "30 4 1,15 * * 5",
            "0 0 0-4,8-12 * * *",
            # The oracle reads a leading `*`/`?` as the whole range and ignores
            # whatever follows the dash — server-accepted, so accepted here.
            "*-* * * * * *",
            "?-? * * * * *",
            "*-5 * * * * *",
            # Reachable day-of-month/month pairs (see test_unreachable_dates_rejected).
            "0 0 0 29 2 *",
            "0 0 0 31 1 *",
            "0 0 0 31 2,3 *",
            "0 0 0 31 */2 *",
            "0 0 0 31 2 mon",  # day-of-week is not `*`: the server matches dom OR dow
        ],
    )
    def test_accepted(self, expr: str) -> None:
        assert cron(expr).value == expr
        assert cron(expr).is_cron is True

    def test_whitespace_normalised(self) -> None:
        assert cron("  0   30 * * * *  ").value == "0 30 * * * *"

    @pytest.mark.parametrize(
        "expr",
        [
            "",
            "   ",
            "30 * * * *",  # 5-field crontab form: rejected by the server
            "0 30 * * * * *",  # 7 fields
            "0 60 * * * *",  # seconds/minutes out of range
            "0 0 24 * * *",  # hour out of range
            "0 0 0 0 * *",  # day-of-month is 1-based
            "0 0 0 32 * *",
            "0 0 0 * 13 *",
            "0 0 0 * 0 *",
            "0 0 0 * * 7",  # day-of-week is 0-6; 7 is not Sunday here
            "0 0 0 * xyz *",
            "0 0 5-2 * * *",  # inverted range
            "0 0 */0 * * *",  # zero step
            "0 0 */x * * *",
            "0 0 ,1 * * *",
            "0 0 /2 * * *",
            "5-* * * * * *",  # only the *low* end may be `*` (oracle parity)
        ],
    )
    def test_rejected(self, expr: str) -> None:
        with pytest.raises(ScheduleExpressionError):
            cron(expr)

    @pytest.mark.parametrize(
        "expr",
        [
            "0 0 0 31 2 *",  # February never has 31 days
            "0 0 0 30 2 *",
            "0 0 0 31 4 *",
            "0 0 0 31 feb *",
            "0 0 0 30,31 2 *",  # every listed pair impossible
            "0 0 0 30-31 2 *",
            "0 0 0 31 2-2 *",
        ],
    )
    def test_unreachable_dates_rejected(self, expr: str) -> None:
        # The server answers err_code 10189 for a schedule that can never fire.
        with pytest.raises(ScheduleExpressionError, match="can never occur"):
            cron(expr)

    @pytest.mark.parametrize(
        ("schedule", "value"),
        [
            (YEARLY, "@yearly"),
            (ANNUALLY, "@annually"),
            (MONTHLY, "@monthly"),
            (WEEKLY, "@weekly"),
            (DAILY, "@daily"),
            (MIDNIGHT, "@midnight"),
            (HOURLY, "@hourly"),
        ],
    )
    def test_predefined(self, schedule: Schedule, value: str) -> None:
        assert schedule.value == value
        assert schedule.is_cron is True
        assert cron(value) is schedule
        assert PREDEFINED[value] is schedule

    @pytest.mark.parametrize("expr", ["@Hourly", "@HOURLY", "@nope", "@"])
    def test_predefined_is_case_sensitive(self, expr: str) -> None:
        with pytest.raises(ScheduleExpressionError):
            cron(expr)


class TestParseSchedule:
    def test_schedule_passthrough(self) -> None:
        assert parse_schedule(HOURLY) is HOURLY

    def test_at(self) -> None:
        parsed = parse_schedule("@at 2030-01-01T00:00:00Z")
        assert parsed.value == "@at 2030-01-01T00:00:00Z"
        assert parsed.is_cron is False

    def test_at_offset_normalised_to_utc(self) -> None:
        assert parse_schedule("@at 2030-01-01T00:00:00+02:00").value == "@at 2029-12-31T22:00:00Z"

    def test_every(self) -> None:
        parsed = parse_schedule("@every 90s")
        assert parsed.value == "@every 90s"
        assert parsed.is_cron is False

    def test_cron(self) -> None:
        assert parse_schedule("0 30 * * * *").is_cron is True

    @pytest.mark.parametrize(
        "value", ["", "@at", "@at nonsense", "@at 2030-01-01T00:00:00", "@every", "@AT 2030-01-01T00:00:00Z"]
    )
    def test_rejects(self, value: str) -> None:
        with pytest.raises(ScheduleExpressionError):
            parse_schedule(value)


class TestScheduleTTL:
    @pytest.mark.parametrize(
        ("ttl", "expected"),
        [
            (timedelta(minutes=5), "5m0s"),
            (timedelta(seconds=1), "1s"),
            # A sub-second-precision timedelta at or above 1s is valid on the
            # wire (the server accepts "1.5s") and must agree with the "1.5s"
            # string form — the two used to disagree (timedelta rejected).
            (timedelta(seconds=1, milliseconds=500), "1.5s"),
            ("1.5s", "1.5s"),
            (300, "5m0s"),
            (1, "1s"),
            ("5m", "5m"),
            ("never", "never"),
        ],
    )
    def test_encode(self, ttl: timedelta | int | str, expected: str) -> None:
        assert encode_schedule_ttl(ttl) == expected

    @pytest.mark.parametrize("ttl", [timedelta(milliseconds=500), 0, -1, "0s", "500ms", "bogus", ""])
    def test_rejects(self, ttl: timedelta | int | str) -> None:
        with pytest.raises(ScheduleTTLError):
            encode_schedule_ttl(ttl)


class TestHeaderBlock:
    def test_minimal(self) -> None:
        assert build_schedule_headers(HOURLY, target="orders") == {
            "Nats-Schedule": "@hourly",
            "Nats-Schedule-Target": "orders",
        }

    def test_full(self) -> None:
        assert build_schedule_headers(
            cron("0 0 5 * * *"),
            target="orders",
            source="sensors.temp",
            ttl=timedelta(minutes=5),
            time_zone="Europe/Amsterdam",
            rollup=True,
        ) == {
            "Nats-Schedule": "0 0 5 * * *",
            "Nats-Schedule-Target": "orders",
            "Nats-Schedule-Source": "sensors.temp",
            "Nats-Schedule-TTL": "5m0s",
            "Nats-Schedule-Time-Zone": "Europe/Amsterdam",
            "Nats-Schedule-Rollup": "sub",
        }

    def test_accepts_raw_expression(self) -> None:
        headers = build_schedule_headers("@every 30s", target="orders")
        assert headers["Nats-Schedule"] == "@every 30s"

    @pytest.mark.parametrize("target", ["", "orders.*", "orders.>", "*", "orders..new"])
    def test_bad_target(self, target: str) -> None:
        with pytest.raises(ScheduleTargetError):
            build_schedule_headers(HOURLY, target=target)

    @pytest.mark.parametrize("source", ["", "sensors.*", "sensors.>"])
    def test_bad_source(self, source: str) -> None:
        with pytest.raises(ScheduleSourceError):
            build_schedule_headers(HOURLY, target="orders", source=source)

    @pytest.mark.parametrize("schedule", ["@every 1m", "@at 2030-01-01T00:00:00Z"])
    def test_time_zone_only_on_cron(self, schedule: str) -> None:
        with pytest.raises(ScheduleTimeZoneError, match="only valid for cron"):
            build_schedule_headers(schedule, target="orders", time_zone="Europe/Amsterdam")

    @pytest.mark.parametrize("zone", ["+02:00", "-05:00"])
    def test_time_zone_rejects_fixed_offset(self, zone: str) -> None:
        with pytest.raises(ScheduleTimeZoneError, match="IANA"):
            build_schedule_headers(HOURLY, target="orders", time_zone=zone)

    def test_time_zone_rejects_empty(self) -> None:
        with pytest.raises(ScheduleTimeZoneError, match="omit it"):
            build_schedule_headers(HOURLY, target="orders", time_zone="")

    def test_errors_are_config_errors(self) -> None:
        # Every local rejection is catchable as the client's ConfigError (a ValueError).
        with pytest.raises(ConfigError):
            build_schedule_headers(HOURLY, target="")


class TestScheduleEntry:
    def _stored(self, **headers: str) -> StoredMsg:
        return StoredMsg(
            subject="schedules.one",
            seq=7,
            payload=b"body",
            time=datetime(2026, 1, 1, tzinfo=UTC),
            headers=Headers(headers),
        )

    def test_from_stored(self) -> None:
        entry = ScheduleEntry.from_stored(
            self._stored(
                **{
                    "Nats-Schedule": "@every 30s",
                    "Nats-Schedule-Target": "orders",
                    "Nats-Schedule-Source": "sensors.temp",
                    "Nats-Schedule-TTL": "5m",
                    "Nats-Rollup": "sub",
                }
            )
        )
        assert entry.subject == "schedules.one"
        assert entry.sequence == 7
        assert entry.schedule == "@every 30s"
        assert entry.target == "orders"
        assert entry.source == "sensors.temp"
        assert entry.ttl == "5m"
        assert entry.time_zone is None
        assert entry.payload == b"body"
        assert entry.interval == timedelta(seconds=30)
        assert entry.fires_at is None
        assert entry.is_one_shot is False
        assert entry.expression.is_cron is False

    def test_one_shot(self) -> None:
        entry = ScheduleEntry.from_stored(
            self._stored(**{"Nats-Schedule": "@at 2030-01-01T00:00:00Z", "Nats-Schedule-Target": "orders"})
        )
        assert entry.is_one_shot is True
        assert entry.fires_at == datetime(2030, 1, 1, tzinfo=UTC)
        assert entry.interval is None

    def test_cron_entry(self) -> None:
        entry = ScheduleEntry.from_stored(
            self._stored(
                **{
                    "Nats-Schedule": "@hourly",
                    "Nats-Schedule-Target": "orders",
                    "Nats-Schedule-Time-Zone": "Europe/Amsterdam",
                }
            )
        )
        assert entry.expression.is_cron is True
        assert entry.time_zone == "Europe/Amsterdam"
        assert entry.interval is None
        assert entry.fires_at is None

    def test_non_schedule_message_is_loud(self) -> None:
        with pytest.raises(ScheduleNotFoundError, match="Nats-Schedule"):
            ScheduleEntry.from_stored(self._stored(**{"Nats-Scheduler": "schedules.one"}))

    def test_headerless_message_is_loud(self) -> None:
        with pytest.raises(ScheduleNotFoundError):
            ScheduleEntry.from_stored(StoredMsg(subject="x", seq=1, payload=b""))

    @pytest.mark.parametrize(
        ("expression", "is_cron"),
        [
            # Server-valid values this package's *write* validator would reject
            # (`*-*` before the parity fix, `1s1ns` always): reading must not throw.
            ("*-* * * * * *", True),
            ("0 0 0 31 2 *", True),
            ("@every 1s1ns", False),
            ("@at 2030-01-01T00:00:00Z", False),
            ("@hourly", True),
        ],
    )
    def test_expression_does_not_revalidate_the_wire(self, expression: str, is_cron: bool) -> None:
        entry = ScheduleEntry.from_stored(self._stored(**{"Nats-Schedule": expression}))
        assert entry.expression == Schedule(expression, is_cron=is_cron)

    def test_interval_is_loud_about_unrepresentable_precision(self) -> None:
        entry = ScheduleEntry.from_stored(self._stored(**{"Nats-Schedule": "@every 1s1ns"}))
        with pytest.raises(ScheduleExpressionError, match="sub-microsecond"):
            _ = entry.interval

    def test_fires_at_is_loud_about_a_bad_timestamp(self) -> None:
        entry = ScheduleEntry.from_stored(self._stored(**{"Nats-Schedule": "@at not-a-timestamp"}))
        with pytest.raises(ScheduleExpressionError, match="RFC3339"):
            _ = entry.fires_at

    def test_direct_get_transport_headers_are_stripped(self) -> None:
        entry = ScheduleEntry.from_stored(
            self._stored(
                **{
                    "Nats-Schedule": "@hourly",
                    "Nats-Schedule-Target": "orders",
                    "X-Trace": "abc",
                    "Nats-Stream": "SCHED",
                    "Nats-Subject": "schedules.one",
                    "Nats-Sequence": "7",
                    "Nats-Time-Stamp": "2026-01-01T00:00:00Z",
                    "Nats-Num-Pending": "3",
                    "Nats-Last-Sequence": "4",
                }
            )
        )
        assert entry.headers is not None
        assert dict(entry.headers) == {
            "Nats-Schedule": "@hourly",
            "Nats-Schedule-Target": "orders",
            "X-Trace": "abc",
        }

    def test_entries_are_hashable_and_compare_across_read_paths(self) -> None:
        # `get()` and `list()` get different server-added metadata; the same
        # definition must still be one value (and usable as a set member).
        common = {"Nats-Schedule": "@hourly", "Nats-Schedule-Target": "orders"}
        single = ScheduleEntry.from_stored(self._stored(**common, **{"Nats-Sequence": "7"}))
        batched = ScheduleEntry.from_stored(self._stored(**common, **{"Nats-Num-Pending": "12"}))
        assert single == batched
        assert len({single, batched}) == 1


class _Delivered:
    """Minimal stand-in for the `HasHeaders` shape (Msg / JsMsg / StoredMsg)."""

    def __init__(self, headers: Headers | None) -> None:
        self.headers = headers


class TestDeliveryInfo:
    def test_recurring(self) -> None:
        info = delivery_info(
            _Delivered(Headers({"Nats-Scheduler": "schedules.every", "Nats-Schedule-Next": "2026-07-22T20:59:40Z"}))
        )
        assert info == ScheduleDelivery(
            scheduler="schedules.every",
            next_run=datetime(2026, 7, 22, 20, 59, 40, tzinfo=UTC),
            final=False,
        )

    def test_final_purge(self) -> None:
        info = delivery_info(
            _Delivered(Headers({"Nats-Scheduler": "schedules.at", "Nats-Schedule-Next": "purge", "Nats-TTL": "5m"}))
        )
        assert info is not None
        assert info.final is True
        assert info.next_run is None
        assert info.ttl == "5m"

    def test_rollup_stamp(self) -> None:
        info = delivery_info(
            _Delivered(Headers({"Nats-Scheduler": "s.one", "Nats-Schedule-Next": "purge", "Nats-Rollup": "sub"}))
        )
        assert info is not None
        assert info.rollup == "sub"

    @pytest.mark.parametrize("headers", [None, Headers({}), Headers({"Nats-Msg-Id": "x"})])
    def test_not_scheduled(self, headers: Headers | None) -> None:
        msg = _Delivered(headers)
        assert delivery_info(msg) is None
        assert is_scheduled(msg) is False

    def test_is_scheduled(self) -> None:
        assert is_scheduled(_Delivered(Headers({"Nats-Scheduler": "s.one"}))) is True


def _definition(subject: str, seq: int) -> StoredMsg:
    return StoredMsg(
        subject=subject,
        seq=seq,
        payload=b"",
        headers=Headers({"Nats-Schedule": "@hourly", "Nats-Schedule-Target": "target.x"}),
    )


def _schedule_stream_info() -> StreamInfo:
    return StreamInfo(
        config=StreamConfig(name="SCHED", subjects=["schedules.>"], allow_msg_schedules=True, allow_direct=True)
    )


class _CappedStream(Stream):
    """A `Stream` whose batch Direct Get refuses more than ``cap`` matching subjects.

    Models the server's `TOO_MANY_RESULTS` exactly as 2.14.3 answers it: a
    status frame, before any data, for the whole request.
    """

    def __init__(self, stored: dict[str, StoredMsg], *, cap: int) -> None:
        super().__init__(cast("Any", None), _schedule_stream_info())
        self.stored = stored
        self.cap = cap
        self.requests: list[list[str]] = []

    async def info(self, *, subjects_filter: str | None = None) -> StreamInfo:
        # `list()` reads the live config for its default filters; the fake has no
        # ctx, so serve the cached snapshot the test set up (and may have edited).
        return self.cached_info

    async def get_last_msgs_for(
        self,
        subjects: list[str] | str,
        *,
        batch: int | None = None,
        up_to_seq: int | None = None,
        up_to_time: datetime | None = None,
    ) -> AsyncIterator[StoredMsg]:
        filters = [subjects] if isinstance(subjects, str) else list(subjects)
        self.requests.append(filters)
        matched = [
            subject
            for subject in self.stored
            if any(subject == f or (f.endswith(".>") and subject.startswith(f[:-1])) for f in filters)
        ]
        if len(matched) > self.cap:
            raise APIError("batch direct get failed: 413 Too Many Results", code=413)
        for subject in matched:
            yield self.stored[subject]


class _BrokenStream(Stream):
    """A `Stream` whose batch Direct Get fails after ``after`` messages."""

    def __init__(self, error: Exception, *, after: int = 0) -> None:
        super().__init__(cast("Any", None), _schedule_stream_info())
        self.error = error
        self.after = after

    async def info(self, *, subjects_filter: str | None = None) -> StreamInfo:
        return self.cached_info

    async def get_last_msgs_for(
        self,
        subjects: list[str] | str,
        *,
        batch: int | None = None,
        up_to_seq: int | None = None,
        up_to_time: datetime | None = None,
    ) -> AsyncIterator[StoredMsg]:
        for index in range(self.after):
            yield _definition(f"schedules.{index}", index + 1)
        raise self.error


class _SubjectListingContext:
    """Just enough `JetStreamContext` for the paged path's ``STREAM.INFO`` calls."""

    def __init__(self, subjects: list[str]) -> None:
        self.subjects = subjects
        self.filters: list[str | None] = []

    async def stream_info(self, name: str, *, subjects_filter: str | None = None) -> StreamInfo:
        self.filters.append(subjects_filter)
        info = _schedule_stream_info()
        info.state = StreamState(subjects=dict.fromkeys(self.subjects, 1))
        return info


class TestListPaging:
    """`list()` above the server's per-request subject cap — the enumeration cliff."""

    def _handle(self, count: int, *, cap: int) -> tuple[Schedules, _CappedStream]:
        stored = {
            f"schedules.s{index:05d}": _definition(f"schedules.s{index:05d}", index + 1) for index in range(count)
        }
        stream = _CappedStream(stored, cap=cap)
        ctx = _SubjectListingContext(sorted(stored))
        return Schedules(cast("Any", ctx), stream), stream

    async def test_single_batch_when_under_the_cap(self) -> None:
        sched, stream = self._handle(10, cap=1024)
        assert len([entry async for entry in sched.list()]) == 10
        assert stream.requests == [["schedules.>"]]

    async def test_pages_past_the_cap_instead_of_returning_nothing(self) -> None:
        # 2500 > 1024: the fast path is refused wholesale, so the enumeration
        # must page. Before the fix this yielded zero entries, silently.
        sched, stream = self._handle(2500, cap=1024)
        subjects = [entry.subject async for entry in sched.list()]
        assert len(subjects) == 2500
        assert len(set(subjects)) == 2500
        assert [len(request) for request in stream.requests] == [1, 1024, 1024, 452]

    async def test_page_size_is_the_documented_cap(self) -> None:
        assert MAX_SUBJECTS_PER_BATCH == 1024

    async def test_truncation_is_not_mistaken_for_the_cap(self) -> None:
        # The core raises when a batch ends without its EOB terminator (dead
        # connection / deadline). While the stream still holds matching
        # subjects that must escape, not be reported as an empty stream.
        ctx = _SubjectListingContext(["schedules.a"])
        sched = Schedules(cast("Any", ctx), _BrokenStream(JetStreamError("ended without the 204 EOB terminator")))
        with pytest.raises(JetStreamError, match="EOB"):
            _ = [entry async for entry in sched.list()]

    async def test_other_api_errors_escape(self) -> None:
        sched = Schedules(cast("Any", None), _BrokenStream(APIError("nope", code=500)))
        with pytest.raises(APIError):
            _ = [entry async for entry in sched.list()]

    async def test_no_retry_once_entries_were_yielded(self) -> None:
        # A mid-stream failure cannot be re-run: the caller already has a
        # prefix, so re-reading would duplicate it. Fail loudly instead.
        sched = Schedules(cast("Any", None), _BrokenStream(APIError("413", code=413), after=2))
        seen: list[ScheduleEntry] = []
        with pytest.raises(APIError):
            async for entry in sched.list():
                seen.append(entry)  # noqa: PERF401  # the prefix already handed out is the assertion
        assert len(seen) == 2

    async def test_empty_filter_list_is_loud(self) -> None:
        stream = _CappedStream({}, cap=1024)
        stream.cached_info.config.subjects = []
        sched = Schedules(cast("Any", None), stream)
        with pytest.raises(ScheduleConfigError):
            _ = [entry async for entry in sched.list()]
