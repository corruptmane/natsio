"""Counter data types and the ADR-49 wire contract (headers, payload).

The constants here are the pinned wire contract — kept as plain ``str`` (like
``natsio.jetstream.headers``) because they go straight onto the wire, and
asserted byte-for-byte against the oracle in the wire-contract tests.
"""

import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Final

from natsio._internal.protocol import Headers
from natsio.errors import ConfigError
from natsio.jetstream.entities import Placement, StorageType

from .errors import InvalidCounterValueError

__all__ = [
    "COUNTER_INCREMENT_HEADER",
    "COUNTER_SOURCES_HEADER",
    "CounterConfig",
    "CounterEntry",
    "CounterSources",
    "parse_counter_value",
    "parse_sources",
]

# -- ADR-49 wire contract ----------------------------------------------------
# Client-set on a publish to increment (or, with a leading '-', decrement) the
# per-subject counter. The value is a base-10 integer of arbitrary size.
COUNTER_INCREMENT_HEADER: Final = "Nats-Incr"
# Server-set on stored counter messages: a JSON object mapping each contributing
# source stream to its per-subject contributions (populated on aggregating
# streams that source from other counters). Absent on plain single-stream
# counters.
COUNTER_SOURCES_HEADER: Final = "Nats-Counter-Sources"

# The stored counter payload is ``{"val": "<base-10 integer as a string>"}``.
# The value is a *string* on the wire so it survives languages whose JSON
# numbers top out at 2**53; Python ``int`` is itself arbitrary-precision, so it
# is the natural in-memory type on both the read and the write side.
_PAYLOAD_VALUE_KEY: Final = "val"

type CounterSources = dict[str, dict[str, int]]
"""``{source_stream: {subject: contribution}}`` — see ``COUNTER_SOURCES_HEADER``."""


@dataclass(frozen=True, slots=True)
class CounterEntry:
    """A counter's current state for one subject.

    ``value`` is the running total; ``incr`` is the most recent increment
    (useful for auditing/recounting); ``sources`` is present only when the
    counter aggregates other streams.
    """

    subject: str
    value: int
    sources: CounterSources | None = None
    incr: int | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class CounterConfig:
    """Configuration for a counter stream (ADR-49).

    Maps onto a JetStream stream with ``allow_msg_counter`` (the counter
    behaviour) and ``allow_direct`` (the read path) forced on. Every subject the
    stream captures is an independent counter. ``allow_msg_counter`` is settable
    only at creation and is incompatible with per-message TTLs and message
    schedules, so those knobs are intentionally not exposed here.
    """

    name: str
    subjects: list[str]
    description: str | None = None
    storage: StorageType = StorageType.FILE
    replicas: int = 1
    max_bytes: int = -1
    # Cap the retained increment history per subject (-1 = unlimited). The
    # current value is always the last message per subject regardless.
    max_msgs_per_subject: int = -1
    # Age off old increments (None = never). Does not affect the current value
    # unless a subject goes fully idle past this age.
    max_age: timedelta | None = None
    placement: Placement | None = None
    compression: bool = False
    metadata: dict[str, str] | None = None

    def __post_init__(self) -> None:
        if not self.subjects:
            raise ConfigError("a counter stream needs at least one subject")


def parse_counter_value(data: bytes) -> int:
    """Parse a stored counter payload ``{"val": "<int>"}`` into an ``int``.

    Raises `InvalidCounterValueError` on empty data, malformed JSON,
    a missing ``val`` key, or a non-integer value.
    """
    if not data:
        raise InvalidCounterValueError("empty counter value")
    try:
        payload: dict[str, Any] = json.loads(data)
    except (ValueError, TypeError) as exc:
        raise InvalidCounterValueError(f"malformed counter payload: {exc}") from exc
    if not isinstance(payload, dict) or _PAYLOAD_VALUE_KEY not in payload:
        raise InvalidCounterValueError(f"counter payload missing {_PAYLOAD_VALUE_KEY!r} field")
    raw = payload[_PAYLOAD_VALUE_KEY]
    try:
        # ``int(str, 10)`` semantics: accept the string form the server writes,
        # reject floats/booleans/None that would slip past a bare ``int()``.
        return int(str(raw), 10)
    except (ValueError, TypeError) as exc:
        raise InvalidCounterValueError(f"invalid counter value: {raw!r}") from exc


def parse_sources(headers: Headers | None) -> CounterSources | None:
    """Parse the ``Nats-Counter-Sources`` header, or ``None`` when absent.

    Raises `InvalidCounterValueError` on malformed JSON or a
    non-integer contribution.
    """
    if headers is None:
        return None
    raw = headers.get(COUNTER_SOURCES_HEADER)
    if not raw:
        return None
    try:
        decoded: dict[str, dict[str, str]] = json.loads(raw)
    except (ValueError, TypeError) as exc:
        raise InvalidCounterValueError(f"malformed {COUNTER_SOURCES_HEADER} header: {exc}") from exc
    result: CounterSources = {}
    for source_id, contributions in decoded.items():
        parsed: dict[str, int] = {}
        for subject, value in contributions.items():
            try:
                parsed[subject] = int(str(value), 10)
            except (ValueError, TypeError) as exc:
                raise InvalidCounterValueError(f"invalid source contribution for {subject!r}: {value!r}") from exc
        result[source_id] = parsed
    return result
