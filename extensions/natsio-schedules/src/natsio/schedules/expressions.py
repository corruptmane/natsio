"""Builders and validators for the `Nats-Schedule` header value (ADR-51).

Three expression families, all produced as the exact bytes nats.go emits:

- `at(when)` -> ``"@at 2026-07-22T20:59:10Z"`` (RFC3339, UTC, whole seconds)
- `every(interval)` -> ``"@every 1m30s"`` (Go ``time.ParseDuration`` syntax, min ``1s``)
- `cron(expr)` -> ``"0 30 * * * *"`` (6 fields) or an ``@`` alias such as `HOURLY`

A `Schedule` remembers whether it is a cron expression, because
`Nats-Schedule-Time-Zone` is only accepted alongside cron schedules — the
server rejects the pairing otherwise.
"""

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from fractions import Fraction

from .errors import ScheduleExpressionError

__all__ = [
    "ANNUALLY",
    "DAILY",
    "HOURLY",
    "MIDNIGHT",
    "MONTHLY",
    "PREDEFINED",
    "WEEKLY",
    "YEARLY",
    "Schedule",
    "after",
    "at",
    "cron",
    "every",
    "format_go_duration",
    "parse_go_duration",
    "parse_schedule",
]

AT_PREFIX = "@at "
EVERY_PREFIX = "@every "
MIN_INTERVAL = timedelta(seconds=1)
"""ADR-51: `@every` intervals shorter than one second are rejected by the server."""


@dataclass(frozen=True, slots=True)
class Schedule:
    """A validated `Nats-Schedule` header value.

    `value` is what goes on the wire. `is_cron` gates the
    `Nats-Schedule-Time-Zone` header, which ADR-51 allows only for cron
    schedules (including the `@` aliases) — never for `@at` or `@every`.
    """

    value: str
    is_cron: bool

    def __str__(self) -> str:
        return self.value


# -- @at ---------------------------------------------------------------------


def at(when: datetime) -> Schedule:
    """A one-shot schedule firing at ``when``.

    ``when`` must be timezone-aware; it is converted to UTC and formatted as
    RFC3339 with whole seconds (Go's ``time.RFC3339`` layout carries no
    fractional part, so nats.go truncates the same way).

    A time in the past is deliberately **not** rejected: ADR-51 defines it as
    "fire immediately", which is a legitimate way to publish through the
    scheduler. Guard against a server that was down for a month by putting a
    `Nats-TTL` on the schedule message itself.
    """
    if when.tzinfo is None or when.tzinfo.utcoffset(when) is None:
        raise ScheduleExpressionError(
            f"@at needs a timezone-aware datetime (got naive {when!r}); use datetime.now(UTC) or attach a tzinfo"
        )
    stamp = when.astimezone(UTC).replace(microsecond=0)
    return Schedule(AT_PREFIX + stamp.isoformat().replace("+00:00", "Z"), is_cron=False)


def after(delay: timedelta) -> Schedule:
    """A one-shot schedule firing ``delay`` from now — `at(now + delay)`."""
    if delay < timedelta(0):
        raise ScheduleExpressionError(f"@at delay must not be negative (got {delay!r})")
    return at(datetime.now(UTC) + delay)


# -- @every ------------------------------------------------------------------


def every(interval: timedelta | str) -> Schedule:
    """A repeating schedule firing every ``interval``, starting when it is stored.

    A ``timedelta`` is formatted the way Go's ``time.Duration.String()`` does
    (``timedelta(minutes=1, seconds=30)`` -> ``"1m30s"``), which is what
    nats.go puts on the wire. A ``str`` is validated against Go's
    ``time.ParseDuration`` grammar and then passed through **verbatim**, so
    ``"90s"`` stays ``"90s"``.

    The minimum is one second; anything shorter is rejected here rather than
    round-tripping into an "invalid pattern" API error.
    """
    if isinstance(interval, str):
        text = interval.strip()
        parsed = parse_go_duration(text)
        if parsed < MIN_INTERVAL:
            raise ScheduleExpressionError(f"@every interval must be at least 1s (got {interval!r})")
        return Schedule(EVERY_PREFIX + text, is_cron=False)
    if interval < MIN_INTERVAL:
        raise ScheduleExpressionError(f"@every interval must be at least 1s (got {interval!r})")
    return Schedule(EVERY_PREFIX + format_go_duration(interval), is_cron=False)


_UNIT_NS: dict[str, int] = {
    "ns": 1,
    "us": 1_000,
    "µs": 1_000,  # U+00B5 MICRO SIGN, what Go itself emits
    "μs": 1_000,  # U+03BC GREEK SMALL LETTER MU, what Go also accepts
    "ms": 1_000_000,
    "s": 1_000_000_000,
    "m": 60_000_000_000,
    "h": 3_600_000_000_000,
}
# Longest units first: the alternation is ordered, so "ms" must be tried before "s".
_DURATION_COMPONENT = re.compile(r"(\d*\.?\d*)(ns|us|µs|μs|ms|s|m|h)")


def format_go_duration(interval: timedelta) -> str:
    """Format ``interval`` exactly as Go's ``time.Duration.String()`` would.

    Only defined for zero or magnitudes of at least one second — the shorter
    forms Go prints (``1.5ms``, ``900ns``) are unreachable from every ADR-51
    field, and a silent unit switch would be a wire surprise.
    """
    micros = round(interval / timedelta(microseconds=1))
    if micros == 0:
        return "0s"
    sign = "-" if micros < 0 else ""
    micros = abs(micros)
    seconds, remainder = divmod(micros, 1_000_000)
    if seconds == 0:
        raise ScheduleExpressionError(f"sub-second durations are not usable here (got {interval!r})")
    fraction = f".{remainder:06d}".rstrip("0") if remainder else ""
    minutes, secs = divmod(seconds, 60)
    hours, mins = divmod(minutes, 60)
    if hours:
        return f"{sign}{hours}h{mins}m{secs}{fraction}s"
    if mins:
        return f"{sign}{mins}m{secs}{fraction}s"
    return f"{sign}{secs}{fraction}s"


def parse_go_duration(text: str) -> timedelta:
    """Parse a Go ``time.ParseDuration`` string (``"1h30m"``, ``"1.5s"``, ``"500ms"``).

    Sub-microsecond precision is rejected rather than truncated: ``timedelta``
    cannot represent it, and silently dropping it would make the returned value
    disagree with the string that goes on the wire.
    """
    body = text
    negative = False
    if body[:1] in ("+", "-"):
        negative = body[0] == "-"
        body = body[1:]
    if not body:
        raise ScheduleExpressionError(f"invalid duration {text!r}: expected a Go duration such as '1m30s'")
    if body == "0":
        return timedelta(0)
    total_ns = Fraction(0)
    position = 0
    while position < len(body):
        match = _DURATION_COMPONENT.match(body, position)
        number = match.group(1) if match is not None else ""
        if match is None or not number or number == ".":
            raise ScheduleExpressionError(f"invalid duration {text!r}: expected a Go duration such as '1m30s'")
        total_ns += Fraction(number) * _UNIT_NS[match.group(2)]
        position = match.end()
    micros, remainder = divmod(total_ns, 1_000)
    if remainder:
        raise ScheduleExpressionError(f"duration {text!r} has sub-microsecond precision, which timedelta cannot hold")
    return timedelta(microseconds=-int(micros) if negative else int(micros))


# -- cron --------------------------------------------------------------------

YEARLY = Schedule("@yearly", is_cron=True)
"""Midnight, January 1st — equivalent to ``0 0 0 1 1 *``."""
ANNUALLY = Schedule("@annually", is_cron=True)
"""Alias of `YEARLY`."""
MONTHLY = Schedule("@monthly", is_cron=True)
"""Midnight on the first of the month — ``0 0 0 1 * *``."""
WEEKLY = Schedule("@weekly", is_cron=True)
"""Midnight between Saturday and Sunday — ``0 0 0 * * 0``."""
DAILY = Schedule("@daily", is_cron=True)
"""Midnight — ``0 0 0 * * *``."""
MIDNIGHT = Schedule("@midnight", is_cron=True)
"""Alias of `DAILY`."""
HOURLY = Schedule("@hourly", is_cron=True)
"""Top of the hour — ``0 0 * * * *``."""

PREDEFINED: dict[str, Schedule] = {
    schedule.value: schedule for schedule in (YEARLY, ANNUALLY, MONTHLY, WEEKLY, DAILY, MIDNIGHT, HOURLY)
}
"""Every predefined alias, keyed by its wire value. The names are case-sensitive: the server rejects `@Hourly`."""

_MONTH_NAMES = ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")
_DOW_NAMES = ("sun", "mon", "tue", "wed", "thu", "fri", "sat")


@dataclass(frozen=True, slots=True)
class _CronField:
    name: str
    low: int
    high: int
    names: tuple[str, ...] = ()


# ADR-51 "6 field crontab format". Day-of-week is 0-6 with 0 = Sunday; unlike
# some cron dialects the server does NOT accept 7 as Sunday.
_CRON_FIELDS = (
    _CronField("seconds", 0, 59),
    _CronField("minutes", 0, 59),
    _CronField("hours", 0, 23),
    _CronField("day-of-month", 1, 31),
    _CronField("month", 1, 12, _MONTH_NAMES),
    _CronField("day-of-week", 0, 6, _DOW_NAMES),
)


def cron(expr: str) -> Schedule:
    """A cron schedule: the 6-field form, or one of the `@` aliases.

    Fields are ``seconds minutes hours day-of-month month day-of-week``, each
    accepting ``*``, ``?``, values, ``a-b`` ranges, ``,`` lists, and ``/step``
    suffixes; month and day-of-week also accept three-letter names
    (case-insensitive). Note that day-of-week runs ``0-6`` with ``0`` = Sunday —
    ``7`` is rejected — and that the 5-field crontab form is **not** accepted.

    Whitespace between fields is normalised to single spaces; the field text
    itself is passed through untouched.
    """
    text = expr.strip()
    if not text:
        raise ScheduleExpressionError("cron expression must not be empty")
    if text.startswith("@"):
        predefined = PREDEFINED.get(text)
        if predefined is None:
            raise ScheduleExpressionError(
                f"unknown predefined schedule {text!r}; expected one of {', '.join(sorted(PREDEFINED))}"
            )
        return predefined
    fields = text.split()
    if len(fields) != len(_CRON_FIELDS):
        raise ScheduleExpressionError(
            f"cron expression must have 6 fields (seconds minutes hours day-of-month month day-of-week), "
            f"got {len(fields)} in {expr!r}"
        )
    selected = [_validate_cron_field(spec, value, expr) for spec, value in zip(_CRON_FIELDS, fields, strict=True)]
    _check_reachable(selected[3], selected[4], selected[5], expr)
    return Schedule(" ".join(fields), is_cron=True)


@dataclass(frozen=True, slots=True)
class _FieldValues:
    """The values one cron field selects, plus whether it is a ``*``/``?`` field."""

    values: frozenset[int]
    starred: bool


def _validate_cron_field(spec: _CronField, text: str, expr: str) -> _FieldValues:
    values: set[int] = set()
    starred = False
    for item in text.split(","):
        if not item:
            raise ScheduleExpressionError(f"empty entry in cron {spec.name} field {text!r} of {expr!r}")
        range_part, slash, step_part = item.partition("/")
        if slash and (not step_part.isdigit() or int(step_part) < 1):
            raise ScheduleExpressionError(f"cron {spec.name} step must be a positive integer, got {item!r}")
        step = int(step_part) if slash else 1
        low_text, dash, high_text = range_part.partition("-")
        if low_text in ("*", "?"):
            # Oracle parity: the server's parser takes a leading `*`/`?` as the
            # whole range and never looks at the rest of the range expression,
            # so `*-*` and `*-anything` are accepted while `5-*` is not.
            starred = True
            values.update(range(spec.low, spec.high + 1, step))
            continue
        low = _cron_value(spec, low_text, expr)
        if dash:
            high = _cron_value(spec, high_text, expr)
            if high < low:
                raise ScheduleExpressionError(f"cron {spec.name} range {range_part!r} is inverted in {expr!r}")
        else:
            # A bare value with a step means "from here to the end of the field".
            high = spec.high if slash else low
        values.update(range(low, high + 1, step))
    return _FieldValues(frozenset(values), starred)


# February gets 29: a leap year is always within the server's search horizon.
_DAYS_IN_MONTH = (31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)


def _check_reachable(dom: _FieldValues, month: _FieldValues, dow: _FieldValues, expr: str) -> None:
    """Reject day-of-month / month pairs that no calendar date can satisfy.

    The server answers err_code 10189 for these (it gives up after searching a
    few years ahead for a firing), so ``0 0 0 31 2 *`` is a hard error rather
    than a schedule that never runs. Only checked when day-of-week is a
    ``*``/``?`` field: otherwise the oracle matches day-of-month **or**
    day-of-week, and any day-of-month becomes reachable.

    This is not a next-fire calculation — it is a static impossibility check.
    """
    if not dow.starred:
        return
    if any(day <= _DAYS_IN_MONTH[value - 1] for value in month.values for day in dom.values):
        return
    raise ScheduleExpressionError(
        f"cron day-of-month/month combination in {expr!r} can never occur "
        "(the server rejects unreachable schedules with err_code 10189)"
    )


def _cron_value(spec: _CronField, token: str, expr: str) -> int:
    if not token:
        raise ScheduleExpressionError(f"empty value in cron {spec.name} field of {expr!r}")
    if spec.names:
        try:
            index = spec.names.index(token.lower())
        except ValueError:
            pass
        else:
            # Month names are 1-based (jan == 1), day names 0-based (sun == 0).
            return index + spec.low
    if not token.isdigit():
        raise ScheduleExpressionError(f"cron {spec.name} value {token!r} is not a number in {expr!r}")
    value = int(token)
    if not spec.low <= value <= spec.high:
        raise ScheduleExpressionError(f"cron {spec.name} value {value} is outside {spec.low}-{spec.high} in {expr!r}")
    return value


# -- round-tripping ----------------------------------------------------------


def parse_schedule(value: str | Schedule) -> Schedule:
    """Validate an already-formed `Nats-Schedule` value into a `Schedule`.

    Accepts a `Schedule` unchanged, so publish helpers can take either. Used to
    classify a stored definition read back off the wire, and to validate
    hand-written expressions before publishing them.

    An `@at` value is re-emitted in canonical form (UTC, whole seconds), so
    ``"@at 2030-01-01T00:00:00+02:00"`` comes back as
    ``"@at 2029-12-31T22:00:00Z"`` — the same instant, and the same bytes
    nats.go would have sent. `@every` and cron values are passed through.
    """
    if isinstance(value, Schedule):
        return value
    text = value.strip()
    if not text:
        raise ScheduleExpressionError("schedule expression must not be empty")
    if text.startswith(AT_PREFIX):
        return at(_parse_at_timestamp(text))
    if text.startswith(EVERY_PREFIX):
        return every(text[len(EVERY_PREFIX) :].strip())
    return cron(text)


def _parse_at_timestamp(text: str) -> datetime:
    stamp = text[len(AT_PREFIX) :].strip()
    normalised = stamp[:-1] + "+00:00" if stamp[-1:] in ("Z", "z") else stamp
    try:
        parsed = datetime.fromisoformat(normalised)
    except ValueError:
        raise ScheduleExpressionError(f"@at timestamp {stamp!r} is not RFC3339") from None
    if parsed.tzinfo is None:
        raise ScheduleExpressionError(f"@at timestamp {stamp!r} has no timezone offset")
    return parsed
