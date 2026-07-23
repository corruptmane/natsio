"""Typed errors for message schedules (ADR-51).

Everything roots at `ScheduleError` (a `natsio.jetstream.JetStreamError`), in
two branches:

- **Local, pre-flight** failures additionally inherit `natsio.errors.ConfigError`
  — so they are `ValueError`s and can be caught with the same
  `except ConfigError` that covers the rest of the client's configuration
  surface.
- **Server** failures additionally inherit `natsio.jetstream.APIError` and are
  bound to their ADR-51 `err_code` through the core's documented
  `register_error` hook, so a rejected publish raises e.g.
  `SchedulePatternInvalidError` instead of a generic `APIError` nobody can
  match on without string-comparing the description.

Importing `natsio.schedules` performs that registration; it is idempotent and
scoped to the schedule codes.
"""

from natsio.errors import ConfigError
from natsio.jetstream.errors import APIError, JetStreamError, register_error

__all__ = [
    "SCHEDULE_ERR_CODES",
    "MessageSchedulesDisabledError",
    "MirrorWithMsgSchedulesError",
    "ScheduleAPIError",
    "ScheduleConfigError",
    "ScheduleError",
    "ScheduleExpressionError",
    "ScheduleNotFoundError",
    "SchedulePatternInvalidError",
    "ScheduleRollupInvalidError",
    "ScheduleSourceError",
    "ScheduleSourceInvalidError",
    "ScheduleTTLError",
    "ScheduleTTLInvalidError",
    "ScheduleTargetError",
    "ScheduleTargetInvalidError",
    "ScheduleTimeZoneError",
    "ScheduleTimeZoneInvalidError",
    "SchedulerInvalidError",
    "SchedulesNotEnabledError",
    "SourceWithMsgSchedulesError",
]


class ScheduleError(JetStreamError):
    """Root of every `natsio.schedules` error."""


class ScheduleConfigError(ScheduleError, ConfigError):
    """A schedule was rejected locally, before anything reached the wire."""


class ScheduleExpressionError(ScheduleConfigError):
    """The `Nats-Schedule` expression is malformed.

    Raised by `at`, `every`, `cron` and `parse_schedule` for a naive
    ``datetime``, a sub-second `@every` interval, a cron expression that is not
    the 6-field form, an out-of-range field, or an unknown `@` alias.
    """


class ScheduleTargetError(ScheduleConfigError):
    """The `Nats-Schedule-Target` subject is missing or not a concrete subject."""


class ScheduleSourceError(ScheduleConfigError):
    """The `Nats-Schedule-Source` subject is empty or contains a wildcard."""


class ScheduleTimeZoneError(ScheduleConfigError):
    """The `Nats-Schedule-Time-Zone` value is empty, an offset, or paired with a non-cron schedule."""


class ScheduleTTLError(ScheduleConfigError):
    """The `Nats-Schedule-TTL` value is not a positive whole-second duration or ``"never"``."""


class SchedulesNotEnabledError(ScheduleConfigError):
    """The stream was not created with ``allow_msg_schedules``.

    ADR-51 allows enabling the flag on an existing stream (never disabling it),
    so this is recoverable with an `update_stream` — but not silently.
    """


class ScheduleNotFoundError(ScheduleError):
    """No schedule definition is stored on that subject.

    Either nothing was ever published there, or the definition already fired
    (`@at` schedules purge themselves) or was cancelled.
    """


# -- server-reported failures ------------------------------------------------
#
# err_codes verified against nats-server 2.14.3 (and, where nats.go carries
# them, against jetstream/errors.go). 10212 and 10223 postdate the Go
# constants and come from the server's responses directly.


class ScheduleAPIError(ScheduleError, APIError):
    """A schedule request the server rejected. Also an `APIError`."""


class MirrorWithMsgSchedulesError(ScheduleAPIError):
    """A stream mirror cannot also schedule messages (10186)."""


class SourceWithMsgSchedulesError(ScheduleAPIError):
    """A sourcing stream cannot also schedule messages (10187)."""


class MessageSchedulesDisabledError(ScheduleAPIError):
    """The target stream was not created with ``allow_msg_schedules`` (10188)."""


class SchedulePatternInvalidError(ScheduleAPIError):
    """The server could not parse the `Nats-Schedule` expression (10189)."""


class ScheduleTargetInvalidError(ScheduleAPIError):
    """The `Nats-Schedule-Target` is missing, malformed, or outside the stream (10190)."""


class ScheduleTTLInvalidError(ScheduleAPIError):
    """The `Nats-Schedule-TTL` is not a usable per-message TTL (10191)."""


class ScheduleRollupInvalidError(ScheduleAPIError):
    """The `Nats-Schedule-Rollup` value is not ``"sub"`` (10192)."""


class ScheduleSourceInvalidError(ScheduleAPIError):
    """The `Nats-Schedule-Source` is malformed or outside the stream (10203)."""


class SchedulerInvalidError(ScheduleAPIError):
    """The `Nats-Scheduler` is empty, invalid, or equals the publish subject (10212)."""


class ScheduleTimeZoneInvalidError(ScheduleAPIError):
    """The `Nats-Schedule-Time-Zone` is empty, an offset, or unknown to the server's tzdata (10223)."""


SCHEDULE_ERR_CODES: dict[int, type[ScheduleAPIError]] = {
    10186: MirrorWithMsgSchedulesError,
    10187: SourceWithMsgSchedulesError,
    10188: MessageSchedulesDisabledError,
    10189: SchedulePatternInvalidError,
    10190: ScheduleTargetInvalidError,
    10191: ScheduleTTLInvalidError,
    10192: ScheduleRollupInvalidError,
    10203: ScheduleSourceInvalidError,
    10212: SchedulerInvalidError,
    10223: ScheduleTimeZoneInvalidError,
}

for _err_code, _exc_type in SCHEDULE_ERR_CODES.items():
    register_error(_err_code, _exc_type)
del _err_code, _exc_type
