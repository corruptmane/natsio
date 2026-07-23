"""The ADR-51 header names, re-exported from the core plus the one it lacks.

Every constant below is a plain `str` that goes on the wire verbatim. The
client-set names come from `natsio.jetstream.headers` (single source of truth
for the core's wire contract); `SCHEDULE_ROLLUP` is defined here because it
landed in ADR-51 revision 4 and is not in the core's header module yet.
"""

from typing import Final

from natsio.jetstream.headers import (
    ROLLUP,
    ROLLUP_SUBJECT,
    SCHEDULE,
    SCHEDULE_NEXT,
    SCHEDULE_SOURCE,
    SCHEDULE_TARGET,
    SCHEDULE_TIME_ZONE,
    SCHEDULE_TTL,
    SCHEDULER,
    TTL,
    TTL_NEVER,
)

__all__ = [
    "ROLLUP",
    "ROLLUP_SUBJECT",
    "SCHEDULE",
    "SCHEDULER",
    "SCHEDULE_NEXT",
    "SCHEDULE_NEXT_PURGE",
    "SCHEDULE_ROLLUP",
    "SCHEDULE_SOURCE",
    "SCHEDULE_TARGET",
    "SCHEDULE_TIME_ZONE",
    "SCHEDULE_TTL",
    "TTL",
    "TTL_NEVER",
]

# ADR-51 rev 4 (server 2.14): sets `Nats-Rollup` on the *generated* message.
# Only "sub" is accepted; the server rejects anything else with err_code 10192.
# Not present in natsio.jetstream.headers as of natsio 0.12.
SCHEDULE_ROLLUP: Final = "Nats-Schedule-Rollup"

# The `Nats-Schedule-Next` value the server stamps on the final message of a
# one-shot (`@at`) schedule, and the value a client sets to stop a schedule
# early. Anything else is an RFC3339 timestamp.
SCHEDULE_NEXT_PURGE: Final = "purge"
