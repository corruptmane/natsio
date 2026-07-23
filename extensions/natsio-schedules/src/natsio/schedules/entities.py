"""The stored schedule definition, and the server's stamps on generated messages."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Protocol

from natsio._internal.jsonmodel import RFC3339
from natsio._internal.protocol import Headers
from natsio.jetstream import headers as js_hdr
from natsio.jetstream.stream import StoredMsg

from . import headers as hdr
from .errors import ScheduleExpressionError, ScheduleNotFoundError
from .expressions import AT_PREFIX, EVERY_PREFIX, Schedule, parse_go_duration

__all__ = [
    "TRANSPORT_HEADERS",
    "HasHeaders",
    "ScheduleDelivery",
    "ScheduleEntry",
    "delivery_info",
    "is_scheduled",
]

TRANSPORT_HEADERS: frozenset[str] = frozenset(
    {
        js_hdr.STREAM,
        js_hdr.SUBJECT,
        js_hdr.SEQUENCE,
        js_hdr.TIME_STAMP,
        js_hdr.LAST_SEQUENCE,
        js_hdr.NUM_PENDING,
        js_hdr.UP_TO_SEQUENCE,
    }
)
"""Headers Direct Get *adds* to a reply — never part of what was stored.

Stripped from `ScheduleEntry.headers` so a definition reads the same whichever
path fetched it (single get and batch get add different subsets), and so the
block can be handed straight back to `Schedules.create`.
"""


class HasHeaders(Protocol):
    """Anything carrying an optional header block: `Msg`, `JsMsg`, `StoredMsg`."""

    @property
    def headers(self) -> Headers | None: ...


@dataclass(frozen=True, slots=True)
class ScheduleEntry:
    """One schedule definition as it is stored in the stream.

    A definition is an ordinary stream message: the *last* message on its own
    unique subject, carrying the `Nats-Schedule` header (the server stamps
    `Nats-Rollup: sub` on it, so there is never more than one per subject).
    """

    subject: str
    """The schedule's subject — its identity, and what `Nats-Scheduler` names."""
    sequence: int
    """Stream sequence of the definition message (usable for CAS and delete)."""
    schedule: str
    """The raw `Nats-Schedule` value."""
    target: str
    """The `Nats-Schedule-Target` subject generated messages are published to."""
    source: str | None = None
    """`Nats-Schedule-Source`: sample the last message on this subject instead of the body."""
    ttl: str | None = None
    """`Nats-Schedule-TTL` applied to generated messages, verbatim."""
    time_zone: str | None = None
    """`Nats-Schedule-Time-Zone` for cron evaluation; ``None`` means UTC."""
    rollup: str | None = None
    """`Nats-Schedule-Rollup` (only ``"sub"``), set on generated messages."""
    payload: bytes = b""
    """The body that will be republished on every firing."""
    time: datetime | None = None
    """When the definition was stored."""
    headers: Headers | None = field(default=None, compare=False)
    """The stored header block, minus `TRANSPORT_HEADERS` (the Direct Get envelope).

    Excluded from ``==`` and ``hash()``: `Headers` is mutable and therefore
    unhashable, and the same definition read through `Schedules.get` and
    `Schedules.list` would otherwise compare unequal over server-added
    metadata. Every schedule field is modelled above; this is the escape hatch
    for user headers travelling to the target.
    """

    @property
    def expression(self) -> Schedule:
        """The stored value as a `Schedule` — mainly for its `is_cron` classification.

        Deliberately **not** re-validated: this is a read path over bytes the
        server already accepted, and the local grammar is a pre-flight, not a
        re-implementation (it is not identical to the server's — see the
        package README). Classification is by prefix, which is exactly what
        `is_cron` needs; `schedule` always holds the raw value.
        """
        return Schedule(self.schedule, is_cron=not self.schedule.startswith((AT_PREFIX, EVERY_PREFIX)))

    @property
    def is_one_shot(self) -> bool:
        """True for an `@at` schedule, which purges itself after it fires."""
        return self.schedule.startswith(AT_PREFIX)

    @property
    def fires_at(self) -> datetime | None:
        """The instant an `@at` schedule fires, else ``None``.

        Raises `ScheduleExpressionError` if the stored timestamp is not
        RFC3339 — only reachable for a definition written by something that
        bypassed the server's own parser.
        """
        if not self.is_one_shot:
            return None
        stamp = self.schedule[len(AT_PREFIX) :].strip()
        try:
            return RFC3339.from_wire(stamp)
        except ValueError:
            raise ScheduleExpressionError(f"stored @at timestamp {stamp!r} is not RFC3339") from None

    @property
    def interval(self) -> timedelta | None:
        """The period of an `@every` schedule, else ``None``.

        Raises `ScheduleExpressionError` for the one server-valid form a
        ``timedelta`` cannot hold: sub-microsecond precision (``@every 1s1ns``,
        writable by nats.go but not by this package). Rounding it would make
        the returned value disagree with the bytes on the wire, so it is loud;
        `schedule` always holds the raw value.
        """
        if not self.schedule.startswith(EVERY_PREFIX):
            return None
        return parse_go_duration(self.schedule[len(EVERY_PREFIX) :].strip())

    @classmethod
    def from_stored(cls, stored: StoredMsg) -> "ScheduleEntry":
        """Build an entry from a stored message.

        Raises `ScheduleNotFoundError` when the message is not a schedule
        definition — a plain message on the subject, or a cancellation marker.
        """
        stored_headers = stored.headers
        expression = stored_headers.get(hdr.SCHEDULE) if stored_headers is not None else None
        if stored_headers is None or not expression:
            raise ScheduleNotFoundError(
                f"message on {stored.subject!r} (seq {stored.seq}) carries no {hdr.SCHEDULE} header"
            )
        kept = Headers(stored_headers)
        for name in TRANSPORT_HEADERS:
            kept.discard(name)
        return cls(
            subject=stored.subject,
            sequence=stored.seq,
            schedule=expression,
            target=stored_headers.get(hdr.SCHEDULE_TARGET, ""),
            source=stored_headers.get(hdr.SCHEDULE_SOURCE),
            ttl=stored_headers.get(hdr.SCHEDULE_TTL),
            time_zone=stored_headers.get(hdr.SCHEDULE_TIME_ZONE),
            rollup=stored_headers.get(hdr.SCHEDULE_ROLLUP),
            payload=stored.payload,
            time=stored.time,
            headers=kept,
        )


@dataclass(frozen=True, slots=True)
class ScheduleDelivery:
    """What the server stamped on a message it generated from a schedule."""

    scheduler: str
    """`Nats-Scheduler`: the subject holding the schedule that produced this message."""
    next_run: datetime | None
    """The next firing time, or ``None`` when this was the last one (`final`).

    Timezone-aware, but **not necessarily UTC**: the server stamps it in the
    schedule's own `Nats-Schedule-Time-Zone` when one is set. Compare instants,
    not wall clocks.
    """
    final: bool
    """True when `Nats-Schedule-Next` was ``"purge"`` — the schedule is gone."""
    ttl: str | None = None
    """`Nats-TTL` mirrored from the definition's `Nats-Schedule-TTL`."""
    rollup: str | None = None
    """`Nats-Rollup` mirrored from the definition's `Nats-Schedule-Rollup`."""


def delivery_info(msg: HasHeaders) -> ScheduleDelivery | None:
    """Read the scheduler stamps off a delivered message.

    Returns ``None`` unless the message carries the `Nats-Scheduler` /
    `Nats-Schedule-Next` stamps, so it doubles as the test for "did this arrive
    on a schedule?". Works on a core `Msg`, a JetStream `JsMsg`, or a `StoredMsg`.

    Caveat: this reads *stamps*, it does not prove provenance. The record
    `stop_and_publish` writes carries the same `Nats-Scheduler` +
    `Nats-Schedule-Next: purge` pair (the server stores client-set headers
    verbatim), so it is byte-identical to a genuine final firing and reads here
    as one. If you must tell them apart, put your own marker on the stop record.
    """
    headers = msg.headers
    if headers is None:
        return None
    scheduler = headers.get(hdr.SCHEDULER)
    if not scheduler:
        return None
    raw_next = headers.get(hdr.SCHEDULE_NEXT)
    final = raw_next == hdr.SCHEDULE_NEXT_PURGE
    next_run = RFC3339.from_wire(raw_next) if raw_next and not final else None
    return ScheduleDelivery(
        scheduler=scheduler,
        next_run=next_run,
        final=final,
        ttl=headers.get(hdr.TTL),
        rollup=headers.get(hdr.ROLLUP),
    )


def is_scheduled(msg: HasHeaders) -> bool:
    """True when the message was generated by a schedule (carries `Nats-Scheduler`)."""
    headers = msg.headers
    return headers is not None and bool(headers.get(hdr.SCHEDULER))
