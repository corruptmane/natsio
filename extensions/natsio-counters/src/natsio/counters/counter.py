"""The Counter handle: increment, load, and enumerate ADR-49 counters."""

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from natsio.jetstream.entities import DiscardPolicy, StorageCompression, StreamConfig
from natsio.jetstream.errors import MessageNotFoundError, StreamNotFoundError
from natsio.jetstream.stream import Stream

from .entities import (
    COUNTER_INCREMENT_HEADER,
    CounterConfig,
    CounterEntry,
    parse_counter_value,
    parse_sources,
)
from .errors import (
    CounterNotEnabledError,
    CounterNotFoundError,
    CounterSubjectNotInitializedError,
    DirectAccessRequiredError,
    InvalidCounterValueError,
)

if TYPE_CHECKING:
    from natsio.jetstream.context import JetStreamContext

__all__ = ["Counter", "counter_from_stream", "create_counter", "get_counter"]


class Counter:
    """A handle to a JetStream stream configured for distributed counters.

    Every subject the stream captures is an independent counter. Obtain via
    `create_counter`, `get_counter`, or `counter_from_stream`.

    Python ``int`` is arbitrary-precision, so it is used directly for both
    deltas and values — no big-integer wrapper is needed to match the ADR-49
    unbounded-integer semantics.
    """

    __slots__ = ("_ctx", "_stream", "name")

    def __init__(self, ctx: "JetStreamContext", stream: Stream) -> None:
        config = stream.cached_info.config
        if not config.allow_msg_counter:
            raise CounterNotEnabledError(
                f"stream {config.name!r} is not a counter stream (needs allow_msg_counter=True)"
            )
        if not config.allow_direct:
            raise DirectAccessRequiredError(f"stream {config.name!r} needs allow_direct=True for counter reads")
        self._ctx = ctx
        self._stream = stream
        self.name = config.name

    def __repr__(self) -> str:
        return f"Counter(name={self.name!r})"

    # -- writes --------------------------------------------------------------

    async def add(self, subject: str, delta: int) -> int:
        """Increment (or, with a negative ``delta``, decrement) ``subject``.

        Returns the new running total, taken from the PubAck's ``val`` field —
        no follow-up read. ``delta`` of ``0`` is a valid no-op increment that
        still initializes the subject. The ``subject`` must be covered by the
        stream's subjects, else `NoStreamResponseError`.
        """
        if isinstance(delta, bool) or not isinstance(delta, int):
            raise InvalidCounterValueError(f"counter delta must be an int, got {type(delta).__name__}")
        ack = await self._ctx.publish(subject, b"", headers={COUNTER_INCREMENT_HEADER: str(delta)})
        if not ack.val:
            raise InvalidCounterValueError(f"counter increment for {subject!r} returned no value")
        return int(ack.val, 10)

    # -- reads ---------------------------------------------------------------

    async def load(self, subject: str) -> int:
        """The current value of ``subject``'s counter.

        Raises `CounterSubjectNotInitializedError` if nothing has been
        added to ``subject`` yet.
        """
        stored = await self._get_last(subject)
        return parse_counter_value(stored.payload)

    async def get(self, subject: str) -> CounterEntry:
        """The full `CounterEntry` for ``subject`` (value, sources, last increment).

        Raises `CounterSubjectNotInitializedError` if nothing has been
        added to ``subject`` yet.
        """
        stored = await self._get_last(subject)
        return _entry_from_parts(subject, stored.payload, stored.headers)

    async def _get_last(self, subject: str) -> Any:
        try:
            return await self._stream.get_msg(subject=subject)
        except MessageNotFoundError:
            raise CounterSubjectNotInitializedError(f"counter not initialized for subject {subject!r}") from None

    async def get_multiple(self, subjects: list[str]) -> AsyncIterator[CounterEntry]:
        """Yield a `CounterEntry` for each subject matching ``subjects``.

        ``subjects`` may contain wildcards (``*`` / ``>``); each matched subject
        yields its current entry. Backed by a single batch Direct Get
        (``Stream.get_last_msgs_for``, ``multi_last`` per ADR-31) rather than one
        round-trip per subject.
        """
        if not subjects:
            return
        async for stored in self._stream.get_last_msgs_for(subjects):
            if not stored.subject:
                continue
            yield _entry_from_parts(stored.subject, stored.payload, stored.headers)


def _entry_from_parts(subject: str, payload: bytes, headers: Any) -> CounterEntry:
    value = parse_counter_value(payload)
    sources = parse_sources(headers)
    incr: int | None = None
    if headers is not None:
        raw_incr = headers.get(COUNTER_INCREMENT_HEADER)
        if raw_incr:
            try:
                incr = int(raw_incr, 10)
            except ValueError as exc:
                raise InvalidCounterValueError(f"invalid {COUNTER_INCREMENT_HEADER} header: {raw_incr!r}") from exc
    return CounterEntry(subject=subject, value=value, sources=sources, incr=incr)


def _counter_stream_config(config: CounterConfig) -> StreamConfig:
    """Map a `CounterConfig` onto its ADR-49 backing stream."""
    return StreamConfig(
        name=config.name,
        description=config.description,
        subjects=list(config.subjects),
        storage=config.storage,
        num_replicas=config.replicas,
        max_bytes=config.max_bytes,
        max_msgs_per_subject=config.max_msgs_per_subject,
        max_age=config.max_age,
        placement=config.placement,
        compression=StorageCompression.S2 if config.compression else None,
        metadata=config.metadata,
        discard=DiscardPolicy.OLD,
        allow_msg_counter=True,
        allow_direct=True,
    )


def counter_from_stream(js: "JetStreamContext", stream: Stream) -> Counter:
    """Wrap an existing counter `Stream` as a `Counter` (no round-trip).

    Raises `CounterNotEnabledError` /
    `DirectAccessRequiredError` if the stream is not counter-capable.
    """
    return Counter(js, stream)


async def create_counter(js: "JetStreamContext", config: CounterConfig) -> Counter:
    """Create a counter stream from ``config`` and return its `Counter`.

    Re-creating with an identical configuration is idempotent; an existing
    stream with a different configuration surfaces as the mapped
    `APIError` (``StreamNameInUseError``).
    """
    stream = await js.create_stream(_counter_stream_config(config))
    return Counter(js, stream)


async def get_counter(js: "JetStreamContext", name: str) -> Counter:
    """Bind an existing counter stream by ``name``.

    Raises `CounterNotFoundError` if no such stream exists, or
    `CounterNotEnabledError` /
    `DirectAccessRequiredError` if it exists but is not a counter.
    """
    try:
        stream = await js.stream(name)
    except StreamNotFoundError:
        raise CounterNotFoundError(f"no counter stream named {name!r}") from None
    return Counter(js, stream)
