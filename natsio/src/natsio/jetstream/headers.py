"""JetStream header names and well-known values (plain strings, on purpose).

These go straight onto the wire as header keys/values; keeping them ``str``
constants rules out the classic ``f"{Enum.MEMBER}"`` class of bugs.
"""

from datetime import timedelta
from typing import Final

type TTLInput = timedelta | int | str
"""A per-message TTL (ADR-43): a ``timedelta``, whole seconds, or ``"never"``."""


def encode_ttl(ttl: TTLInput) -> str:
    """Normalize a per-message TTL to its wire value (whole seconds or ``"never"``).

    The wire format is second-granular, so a ``timedelta`` with sub-second
    precision is rejected rather than silently rounded.
    """
    from natsio.errors import ConfigError

    if isinstance(ttl, str):
        return ttl
    if isinstance(ttl, timedelta):
        fractional = ttl.total_seconds()
        seconds = int(fractional)
        if seconds != fractional:
            raise ConfigError("per-message TTLs are second-granular on the wire; use a whole-second timedelta")
    else:
        seconds = ttl
    if seconds < 1:
        raise ConfigError("ttl must be at least 1 second (or the string 'never')")
    return str(seconds)


# -- publish expectations / identity --
MSG_ID: Final = "Nats-Msg-Id"
EXPECTED_STREAM: Final = "Nats-Expected-Stream"
EXPECTED_LAST_SEQUENCE: Final = "Nats-Expected-Last-Sequence"
EXPECTED_LAST_SUBJECT_SEQUENCE: Final = "Nats-Expected-Last-Subject-Sequence"
# 2.12+: scopes EXPECTED_LAST_SUBJECT_SEQUENCE to a (wildcard) subject filter
# other than the one being published to (nats.go WithExpectLastSequenceForSubject).
EXPECTED_LAST_SUBJECT_SEQUENCE_SUBJECT: Final = "Nats-Expected-Last-Subject-Sequence-Subject"
EXPECTED_LAST_MSG_ID: Final = "Nats-Expected-Last-Msg-Id"

# -- per-message TTL (ADR-43, server 2.11+) --
TTL: Final = "Nats-TTL"
TTL_NEVER: Final = "never"

# -- message schedules (server 2.12+, requires StreamConfig.allow_msg_schedules) --
# Client-set. SCHEDULE holds the schedule expression: "@at <RFC3339>",
# "@every <duration>", a 6-field cron expression, or a predefined value below.
SCHEDULE: Final = "Nats-Schedule"
SCHEDULE_TARGET: Final = "Nats-Schedule-Target"  # subject scheduled messages are delivered to
SCHEDULE_SOURCE: Final = "Nats-Schedule-Source"  # subject to sample the latest message from
SCHEDULE_TTL: Final = "Nats-Schedule-TTL"  # TTL for generated messages (needs allow_msg_ttl)
SCHEDULE_TIME_ZONE: Final = "Nats-Schedule-Time-Zone"  # IANA zone, cron schedules only
# Predefined SCHEDULE values (cron aliases).
SCHEDULE_YEARLY: Final = "@yearly"
SCHEDULE_MONTHLY: Final = "@monthly"
SCHEDULE_WEEKLY: Final = "@weekly"
SCHEDULE_DAILY: Final = "@daily"
SCHEDULE_HOURLY: Final = "@hourly"
# Server-set on generated messages (do not set these when publishing).
SCHEDULER: Final = "Nats-Scheduler"  # subject holding the schedule definition
SCHEDULE_NEXT: Final = "Nats-Schedule-Next"  # next invocation timestamp, or "purge"

# -- rollup (ADR-8 KV purge and friends) --
ROLLUP: Final = "Nats-Rollup"
ROLLUP_SUBJECT: Final = "sub"
ROLLUP_ALL: Final = "all"

# -- direct get / stored-message metadata --
STREAM: Final = "Nats-Stream"
SUBJECT: Final = "Nats-Subject"
SEQUENCE: Final = "Nats-Sequence"
TIME_STAMP: Final = "Nats-Time-Stamp"
LAST_SEQUENCE: Final = "Nats-Last-Sequence"
NUM_PENDING: Final = "Nats-Num-Pending"
UP_TO_SEQUENCE: Final = "Nats-UpTo-Sequence"

# -- pull consumer status metadata --
PENDING_MESSAGES: Final = "Nats-Pending-Messages"
PENDING_BYTES: Final = "Nats-Pending-Bytes"
PIN_ID: Final = "Nats-Pin-Id"

# -- markers (ADR-48) --
MARKER_REASON: Final = "Nats-Marker-Reason"

# -- api-level fail-fast (2.12+) --
REQUIRED_API_LEVEL: Final = "Nats-Required-Api-Level"
