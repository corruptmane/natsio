"""Publishing schedule definitions, and reading/cancelling them back off a stream."""

from collections.abc import AsyncIterator, Generator, Iterator, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Self

from natsio._internal.protocol import Headers, HeadersInput
from natsio.jetstream.entities import DiscardPolicy, PubAck, RetentionPolicy, StorageType, StreamConfig
from natsio.jetstream.errors import APIError, MessageNotFoundError, StreamNotFoundError
from natsio.jetstream.stream import StoredMsg, Stream

from . import headers as hdr
from .entities import ScheduleEntry
from .errors import (
    ScheduleConfigError,
    ScheduleExpressionError,
    ScheduleNotFoundError,
    SchedulesNotEnabledError,
    ScheduleSourceError,
    ScheduleTargetError,
    ScheduleTimeZoneError,
    ScheduleTTLError,
)
from .expressions import Schedule, format_go_duration, parse_go_duration, parse_schedule

if TYPE_CHECKING:
    from natsio.jetstream.context import JetStreamContext

__all__ = [
    "MAX_SUBJECTS_PER_BATCH",
    "ScheduleStreamConfig",
    "ScheduleTTLInput",
    "Schedules",
    "build_schedule_headers",
    "create_schedule_stream",
    "encode_schedule_ttl",
    "publish_schedule",
    "schedules",
    "schedules_from_stream",
]

type ScheduleTTLInput = timedelta | int | str
"""A `Nats-Schedule-TTL`: a ``timedelta`` (>= 1s), seconds, a Go duration string, or ``"never"``."""

_TOO_MANY_RESULTS = 413
"""``413 Too Many Results``: the batch Direct Get refusal `Schedules.list` pages around."""

MAX_SUBJECTS_PER_BATCH = 1024
"""Matching subjects one batch Direct Get will answer.

Above this the server refuses the whole request — status frame, no data — and
`Schedules.list` pages instead. Probe-confirmed on 2.14.3 (1024 answered, 1025
refused); the request's ``batch`` field does not raise the cap, which is keyed
on how many subjects match, not on how many messages were asked for.
"""

_MAX_FILTER_BYTES = 256 * 1024
"""Extra guard on the paged request body, for streams with very long subjects."""

type _SubjectList = list[str]
"""Spelled once, at module scope: inside `Schedules`, ``list`` is the method."""


def encode_schedule_ttl(ttl: ScheduleTTLInput) -> str:
    """Normalize a schedule TTL to its wire value.

    Durations are emitted in Go's ``time.Duration.String()`` form (``"5m"``,
    ``"1.5s"``), matching nats.go's `WithScheduleTTL`. The server also accepts
    bare seconds, but the duration form is what the ADR and the oracle put on
    the wire. The one bound is ``>= 1s`` (the server rejects ``500ms`` with
    err_code 10191) — applied identically to a ``timedelta``, an ``int`` of
    seconds, and a Go-duration string, so ``timedelta(seconds=1.5)`` and
    ``"1.5s"`` no longer disagree.
    """
    if isinstance(ttl, str):
        if ttl == hdr.TTL_NEVER:
            return ttl
        try:
            parsed = parse_go_duration(ttl)
        except ScheduleExpressionError as exc:
            raise ScheduleTTLError(f"schedule TTL {ttl!r} is not a Go duration or 'never': {exc}") from None
        if parsed < timedelta(seconds=1):
            raise ScheduleTTLError(f"schedule TTL must be at least 1 second (or 'never'), got {ttl!r}")
        return ttl
    interval = ttl if isinstance(ttl, timedelta) else timedelta(seconds=ttl)
    if interval < timedelta(seconds=1):
        raise ScheduleTTLError(f"schedule TTL must be at least 1 second (or 'never'), got {ttl!r}")
    return format_go_duration(interval)


def _check_concrete(subject: str, *, what: str, error: type[Exception]) -> None:
    if not subject:
        raise error(f"{what} must not be empty")
    tokens = subject.split(".")
    if "" in tokens:
        raise error(f"{what} must not contain empty tokens: {subject!r}")
    if any(token in ("*", ">") for token in tokens):
        raise error(f"{what} must be a concrete subject, not a wildcard: {subject!r}")


def build_schedule_headers(
    schedule: Schedule | str,
    *,
    target: str,
    source: str | None = None,
    ttl: ScheduleTTLInput | None = None,
    time_zone: str | None = None,
    rollup: bool = False,
) -> dict[str, str]:
    """The exact `Nats-Schedule*` header block for a definition message.

    Split out from `publish_schedule` so the wire contract is testable without
    a server, and reusable by anyone driving `js.publish` directly.
    """
    expression = parse_schedule(schedule)
    _check_concrete(target, what="schedule target", error=ScheduleTargetError)
    extra = {hdr.SCHEDULE: expression.value, hdr.SCHEDULE_TARGET: target}
    if source is not None:
        # ADR-51: "Wildcards are not supported" for the sampled source subject.
        _check_concrete(source, what="schedule source", error=ScheduleSourceError)
        extra[hdr.SCHEDULE_SOURCE] = source
    if ttl is not None:
        extra[hdr.SCHEDULE_TTL] = encode_schedule_ttl(ttl)
    if time_zone is not None:
        if not time_zone:
            raise ScheduleTimeZoneError("time_zone must not be empty; omit it to evaluate the schedule in UTC")
        if time_zone[0] in "+-":
            raise ScheduleTimeZoneError(
                f"time_zone must be an IANA name such as 'Europe/Amsterdam', not a fixed offset: {time_zone!r}"
            )
        if not expression.is_cron:
            raise ScheduleTimeZoneError(f"time_zone is only valid for cron schedules; {expression.value!r} is not one")
        extra[hdr.SCHEDULE_TIME_ZONE] = time_zone
    if rollup:
        extra[hdr.SCHEDULE_ROLLUP] = hdr.ROLLUP_SUBJECT
    return extra


def _is_definition(stored: StoredMsg) -> bool:
    """True when a stored message carries a `Nats-Schedule` — i.e. is a definition.

    What separates definitions from generated messages, cancellation markers,
    and ordinary traffic sharing the stream.
    """
    stored_headers = stored.headers
    return stored_headers is not None and bool(stored_headers.get(hdr.SCHEDULE))


def _pages(subjects: list[str]) -> Iterator[list[str]]:
    """Split concrete subjects into batches one Direct Get will answer."""
    page: list[str] = []
    size = 0
    for subject in subjects:
        if page and (len(page) >= MAX_SUBJECTS_PER_BATCH or size + len(subject) > _MAX_FILTER_BYTES):
            yield page
            page, size = [], 0
        page.append(subject)
        size += len(subject) + 3  # quotes and comma in the JSON request body
    if page:
        yield page


def _merge(headers: HeadersInput | None, extra: dict[str, str]) -> HeadersInput:
    """Overlay the schedule headers on the caller's, preserving repeated values."""
    if headers is None:
        return extra
    merged = Headers(headers)
    for key, value in extra.items():
        merged.set(key, value)
    return merged


async def publish_schedule(
    js: "JetStreamContext",
    subject: str,
    schedule: Schedule | str,
    *,
    target: str,
    payload: bytes | str = b"",
    source: str | None = None,
    ttl: ScheduleTTLInput | None = None,
    time_zone: str | None = None,
    rollup: bool = False,
    headers: HeadersInput | None = None,
    msg_id: str | None = None,
    expected_last_subject_seq: int | None = None,
    timeout: float | None = None,  # noqa: ASYNC109  # mirrors JetStreamContext.publish
) -> PubAck:
    """Store a schedule definition on ``subject``.

    ``subject`` is the schedule's identity: one schedule per subject, and
    publishing again to the same subject replaces the previous definition
    (the server stores schedules as `Nats-Rollup: sub` messages). Give each
    schedule its own subject — ``schedules.orders.<uuid>`` — and point them all
    at the same ``target`` if you want many pending deliveries.

    ``payload`` and any extra ``headers`` are republished verbatim to ``target``
    on every firing, unless ``source`` is set, in which case the last message on
    the source subject is sampled instead (its body is used, falling back to
    this one when the source subject is empty).

    ``expected_last_subject_seq`` CAS-gates the write against the current
    definition — pass ``0`` to create only if no schedule exists yet, or an
    entry's `ScheduleEntry.sequence` to replace only that exact definition.
    """
    _check_concrete(subject, what="schedule subject", error=ScheduleTargetError)
    extra = build_schedule_headers(schedule, target=target, source=source, ttl=ttl, time_zone=time_zone, rollup=rollup)
    return await js.publish(
        subject,
        payload,
        headers=_merge(headers, extra),
        msg_id=msg_id,
        expected_last_subject_seq=expected_last_subject_seq,
        timeout=timeout,
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduleStreamConfig:
    """Configuration for a stream that holds schedules.

    ``subjects`` must cover both the schedule subjects and every target subject:
    the server only accepts a `Nats-Schedule-Target` that the same stream
    captures. ``allow_msg_schedules`` implies `allow_rollup_hdrs` and clears
    `deny_purge` server-side, and Discard New is not supported — so `discard` is
    pinned to old.
    """

    name: str
    subjects: Sequence[str]
    description: str | None = None
    storage: StorageType = StorageType.FILE
    retention: RetentionPolicy = RetentionPolicy.LIMITS
    replicas: int = 1
    max_age: timedelta | None = None
    max_bytes: int = -1
    """Unbounded by default (``-1``), as in `StreamConfig`."""
    allow_msg_ttl: bool = True
    """On by default: `Nats-Schedule-TTL` requires it, and it costs nothing otherwise."""
    metadata: dict[str, str] | None = None


def _stream_config(config: ScheduleStreamConfig) -> StreamConfig:
    return StreamConfig(
        name=config.name,
        description=config.description,
        subjects=list(config.subjects),
        storage=config.storage,
        retention=config.retention,
        num_replicas=config.replicas,
        max_age=config.max_age,
        max_bytes=config.max_bytes,
        metadata=config.metadata,
        discard=DiscardPolicy.OLD,
        allow_msg_schedules=True,
        allow_msg_ttl=config.allow_msg_ttl,
        allow_direct=True,
    )


class Schedules:
    """A handle to one stream's schedule definitions.

    Obtain it with `schedules`, `schedules_from_stream`, or
    `create_schedule_stream`. Every method is a thin, typed layer over the
    core's publish / direct-get / purge — there is no schedule-specific server
    API to call.

    The ``allow_msg_schedules`` check below reads the handle's cached info (it is
    fixed for the stream's life). `list`'s default subject filters, by contrast,
    are read live from ``STREAM.INFO`` on each call, so a subject added after the
    handle was bound still enumerates.
    """

    __slots__ = ("_js", "_stream")

    def __init__(self, js: "JetStreamContext", stream: Stream) -> None:
        config = stream.cached_info.config
        if not config.allow_msg_schedules:
            raise SchedulesNotEnabledError(
                f"stream {config.name!r} does not allow message schedules (needs allow_msg_schedules=True)"
            )
        self._js = js
        self._stream = stream

    def __repr__(self) -> str:
        return f"Schedules(stream={self.stream_name!r})"

    def __await__(self) -> Generator[None, None, Self]:
        """``await`` is optional and completes immediately: binding does no I/O.
        Supported so `schedules_from_stream(...)` reads the same whether or not
        the caller awaits it."""
        return self
        yield  # unreachable: makes this a generator that never suspends

    @property
    def stream(self) -> Stream:
        """The backing stream handle."""
        return self._stream

    @property
    def stream_name(self) -> str:
        return self._stream.name

    # -- writes --------------------------------------------------------------

    async def create(
        self,
        subject: str,
        schedule: Schedule | str,
        *,
        target: str,
        payload: bytes | str = b"",
        source: str | None = None,
        ttl: ScheduleTTLInput | None = None,
        time_zone: str | None = None,
        rollup: bool = False,
        headers: HeadersInput | None = None,
        msg_id: str | None = None,
        expected_last_subject_seq: int | None = None,
        timeout: float | None = None,  # noqa: ASYNC109  # mirrors JetStreamContext.publish
    ) -> PubAck:
        """Store (or replace) the schedule on ``subject`` — see `publish_schedule`."""
        return await publish_schedule(
            self._js,
            subject,
            schedule,
            target=target,
            payload=payload,
            source=source,
            ttl=ttl,
            time_zone=time_zone,
            rollup=rollup,
            headers=headers,
            msg_id=msg_id,
            expected_last_subject_seq=expected_last_subject_seq,
            timeout=timeout,
        )

    # -- reads ---------------------------------------------------------------

    async def get(self, subject: str) -> ScheduleEntry:
        """The schedule definition stored on ``subject``.

        Raises `ScheduleNotFoundError` if the subject holds nothing, or holds a
        message that is not a schedule (an already-fired `@at`, a cancellation
        marker, or an ordinary publish).
        """
        try:
            stored = await self._stream.get_msg(subject=subject)
        except MessageNotFoundError:
            # Core overloads `MessageNotFoundError` for "direct get is not
            # available for this stream" (raised from `NoRespondersError`), so a
            # gone stream or a disabled Direct Get would otherwise be reported as
            # "this schedule does not exist" — and the caller's natural response
            # is to recreate it (a duplicate) or treat a cancel as confirmed.
            # Confirm the miss against STREAM.INFO first: a missing stream (or an
            # API error) propagates as itself; only a live stream is a real miss.
            await self._stream.info()
            raise ScheduleNotFoundError(f"no schedule stored on {subject!r}") from None
        return ScheduleEntry.from_stored(stored)

    async def list(self, subjects: Sequence[str] | str | None = None) -> AsyncIterator[ScheduleEntry]:
        """Yield every schedule definition in the stream.

        One batch Direct Get (`multi_last`, ADR-31) over ``subjects`` — the
        stream's own configured subjects by default — keeping only the last
        message per subject that actually carries a `Nats-Schedule` header.
        That filter is what separates definitions from generated messages,
        cancellation markers, and ordinary traffic sharing the stream.

        **Enumeration is complete or it raises.** A stream matching more than
        `MAX_SUBJECTS_PER_BATCH` subjects cannot be answered in one batch (the
        server refuses it with ``413 Too Many Results``), which is the *expected*
        shape here: ADR-51 gives every schedule its own subject. That case is
        paged transparently — the matching subjects are read from
        ``STREAM.INFO`` and fetched in batches — at the cost of one extra
        round-trip per filter plus one per page. Anything else the server (or a
        dying connection) reports propagates; a partial enumeration is never
        returned as if it were the whole set.

        Narrowing ``subjects`` to the schedule pattern (``"schedules.>"``) keeps
        the fast single-batch path on streams that also carry high-cardinality
        target subjects.

        With no ``subjects`` given, the stream's subjects are read live from
        ``STREAM.INFO`` on each call, so a subject added after this handle was
        bound is enumerated too (the bound snapshot would silently drop it).

        Requires ``allow_direct`` on the stream (`create_schedule_stream` sets
        it). Order follows the server's — by stream sequence — within a batch;
        pages are walked in subject order. The subject set is sampled once when
        paging starts, so a schedule created mid-enumeration may be missed.
        """
        filters = await self._filters(subjects)
        yielded = 0
        try:
            async for entry in self._batch(filters):
                yielded += 1
                yield entry
        except APIError as exc:
            if yielded or exc.code != _TOO_MANY_RESULTS:
                raise
        else:
            return
        # Too many matching subjects for one request, and nothing was yielded
        # (the server refuses the whole batch, it does not truncate it).
        for page in _pages(await self._matching_subjects(filters)):
            async for entry in self._batch(page):
                yield entry

    async def _filters(self, subjects: Sequence[str] | str | None) -> _SubjectList:
        if subjects is None:
            # Read the LIVE config, not `cached_info`: the snapshot is taken when
            # the handle is bound, so a subject added to the stream afterwards
            # would be silently dropped from the default enumeration — exactly
            # the silent-drop shape invariant 5 forbids (every other read here is
            # loud). One STREAM.INFO on the default path is the price.
            filters = list((await self._stream.info()).config.subjects or ())
        elif isinstance(subjects, str):
            filters = [subjects]
        else:
            filters = list(subjects)
        if not filters:
            raise ScheduleConfigError(f"stream {self.stream_name!r} has no subjects to list schedules from")
        return filters

    async def _batch(self, filters: _SubjectList) -> AsyncIterator[ScheduleEntry]:
        """One batch Direct Get, filtered down to actual schedule definitions.

        Core terminates an empty batch cleanly (a lone ``404 No Results``), so
        an empty range simply yields nothing; a genuinely truncated read still
        raises out of `Stream.get_last_msgs_for`.
        """
        async for stored in self._stream.get_last_msgs_for(filters):
            if _is_definition(stored):
                yield ScheduleEntry.from_stored(stored)

    async def _matching_subjects(self, filters: _SubjectList) -> _SubjectList:
        """Every concrete subject holding a message under ``filters``, deduplicated.

        Read through ``STREAM.INFO`` (one request per filter) rather than the
        handle's cached info, so paging never depends on — or overwrites —
        `Stream.cached_info`.
        """
        found: dict[str, None] = {}
        for subject_filter in filters:
            info = await self._js.stream_info(self.stream_name, subjects_filter=subject_filter)
            found.update(dict.fromkeys(info.state.subjects or ()))
        return sorted(found)

    # -- cancellation --------------------------------------------------------

    async def cancel(self, subject: str) -> int:
        """Stop the schedule on ``subject`` by purging the subject.

        Reads the definition first and bounds the purge to it: raises
        `ScheduleNotFoundError` when the subject holds nothing *or* holds a
        message that is not a schedule, so a mistyped subject cannot destroy
        ordinary traffic sharing the stream — and a definition republished
        between the read and the purge survives. A cancel that removed nothing
        is a bug worth hearing about, not a silent success.

        Returns the number of messages purged.
        """
        _check_concrete(subject, what="schedule subject", error=ScheduleTargetError)
        entry = await self.get(subject)
        # STREAM.PURGE's `seq` is exclusive: purge everything up to and
        # including the definition we just read, and nothing published after it.
        purged = await self._stream.purge(subject=subject, sequence=entry.sequence + 1)
        if purged == 0:
            raise ScheduleNotFoundError(f"no schedule stored on {subject!r}")
        return purged

    async def cancel_many(self, subject_filter: str) -> int:
        """Stop every schedule matching a wildcard ``subject_filter``.

        Returns the number of messages purged (``0`` when nothing matched —
        a wildcard sweep over an empty range is a legitimate outcome).

        Unlike `cancel`, this is an unconditional purge: **everything** under
        the filter goes, schedule definition or not. Schedule streams also hold
        the generated messages, so point it at your schedule subjects only, or
        loop over `list` and `cancel` when the stream is mixed.
        """
        return await self._stream.purge(subject=subject_filter)

    async def cancel_by_sequence(self, sequence: int) -> None:
        """Stop a schedule by deleting its definition message by stream sequence."""
        await self._stream.delete_msg(sequence)

    async def stop_and_publish(
        self,
        schedule_subject: str,
        *,
        publish_to: str,
        payload: bytes | str = b"",
        headers: HeadersInput | None = None,
        expected_schedule_seq: int | None = None,
        require_existing: bool = True,
        timeout: float | None = None,  # noqa: ASYNC109  # mirrors JetStreamContext.publish
    ) -> PubAck:
        """Atomically stop a schedule *and* publish a message (ADR-51 "advanced").

        Publishes to ``publish_to`` with `Nats-Schedule-Next: purge` and
        `Nats-Scheduler: <schedule_subject>`, so the schedule is removed only if
        that publish is persisted. Use it to fire a delayed message early
        without letting the schedule fire again, or to record the cancellation
        on a subject your consumers watch.

        ``publish_to`` must not be the schedule's own subject: the cancel
        message would be rolled up together with the schedule, so the server
        rejects it with err_code 10212.

        ``expected_schedule_seq`` CAS-gates the whole operation on the schedule
        still being at that sequence (`Nats-Expected-Last-Subject-Sequence`
        scoped to the schedule subject) — the only way to cancel exactly-once
        against a schedule that may have already fired. When it is not given,
        ``require_existing`` (the default) reads the definition first and uses
        *its* sequence as the gate: the server would otherwise happily accept a
        stop for a schedule that is not there, publishing the message and
        cancelling nothing, which is exactly the case a cancel API must be loud
        about (`cancel` raises for it). Pass ``require_existing=False`` for
        fire-and-forget — one round-trip, no `ScheduleNotFoundError`, and no
        guarantee anything was stopped.

        The published record carries `Nats-Scheduler` + `Nats-Schedule-Next:
        purge`, so downstream it is indistinguishable from a genuine final
        firing: `is_scheduled()` / `delivery_info()` report it as scheduler-
        generated. Add your own header if a consumer must tell the two apart.
        """
        _check_concrete(schedule_subject, what="schedule subject", error=ScheduleTargetError)
        _check_concrete(publish_to, what="publish_to subject", error=ScheduleTargetError)
        if publish_to == schedule_subject:
            raise ScheduleTargetError(
                f"publish_to must differ from the schedule subject {schedule_subject!r}; "
                "the server rejects a self-referencing Nats-Scheduler (err_code 10212)"
            )
        if expected_schedule_seq is None and require_existing:
            expected_schedule_seq = (await self.get(schedule_subject)).sequence
        extra = {hdr.SCHEDULE_NEXT: hdr.SCHEDULE_NEXT_PURGE, hdr.SCHEDULER: schedule_subject}
        return await self._js.publish(
            publish_to,
            payload,
            headers=_merge(headers, extra),
            expected_last_subject_seq=expected_schedule_seq,
            expected_last_subject_seq_subject=schedule_subject if expected_schedule_seq is not None else None,
            timeout=timeout,
        )


# -- factories ---------------------------------------------------------------


def schedules_from_stream(js: "JetStreamContext", stream: Stream) -> Schedules:
    """Wrap an already-fetched `Stream` as a `Schedules` handle (no round-trip).

    Raises `SchedulesNotEnabledError` if the stream lacks ``allow_msg_schedules``.
    """
    return Schedules(js, stream)


async def schedules(js: "JetStreamContext", stream_name: str) -> Schedules:
    """Bind an existing schedule-capable stream by name."""
    try:
        stream = await js.stream(stream_name)
    except StreamNotFoundError:
        raise ScheduleNotFoundError(f"no stream named {stream_name!r}") from None
    return Schedules(js, stream)


async def create_schedule_stream(js: "JetStreamContext", config: ScheduleStreamConfig) -> Schedules:
    """Create a schedule-capable stream from ``config`` and return its handle.

    ``allow_msg_schedules`` and ``allow_direct`` are forced on; re-creating with
    an identical configuration is idempotent.
    """
    stream = await js.create_stream(_stream_config(config))
    return Schedules(js, stream)
