"""Distributed counters over JetStream counter streams (ADR-49).

A counter stream (``allow_msg_counter``) turns every subject it captures into
an independent, arbitrary-precision counter. Increments are ordinary publishes
carrying a ``Nats-Incr`` header; the server folds them into a running total and
returns the new value in the PubAck, so a bare ``add`` needs no follow-up read.

    js = nc.jetstream()
    counter = await create_counter(js, CounterConfig(name="COUNTS", subjects=["events.>"]))

    total = await counter.add("events.orders", 1)      # -> 1
    total = await counter.add("events.orders", 10)     # -> 11
    total = await counter.add("inventory.widgets", -3) # negative deltas are fine

    value = await counter.load("events.orders")        # -> 11
    entry = await counter.get("events.orders")          # value + last increment + sources

    async for entry in counter.get_multiple(["events.>"]):
        print(entry.subject, entry.value)

Requires server 2.12+ (``allow_msg_counter``). ``allow_msg_counter`` can only
be set when the stream is created and is incompatible with per-message TTLs and
message schedules.
"""

from natsio.counters.counter import Counter, counter_from_stream, create_counter, get_counter
from natsio.counters.entities import (
    COUNTER_INCREMENT_HEADER,
    COUNTER_SOURCES_HEADER,
    CounterConfig,
    CounterEntry,
    CounterSources,
    parse_counter_value,
    parse_sources,
)
from natsio.counters.errors import (
    CounterNotEnabledError,
    CounterNotFoundError,
    CounterSubjectNotInitializedError,
    DirectAccessRequiredError,
    InvalidCounterValueError,
)

__all__ = [
    "COUNTER_INCREMENT_HEADER",
    "COUNTER_SOURCES_HEADER",
    "Counter",
    "CounterConfig",
    "CounterEntry",
    "CounterNotEnabledError",
    "CounterNotFoundError",
    "CounterSources",
    "CounterSubjectNotInitializedError",
    "DirectAccessRequiredError",
    "InvalidCounterValueError",
    "counter_from_stream",
    "create_counter",
    "get_counter",
    "parse_counter_value",
    "parse_sources",
]
