"""JetStream header names and well-known values (plain strings, on purpose).

These go straight onto the wire as header keys/values; keeping them ``str``
constants rules out the classic ``f"{Enum.MEMBER}"`` class of bugs.
"""

from typing import Final

# -- publish expectations / identity --
MSG_ID: Final = "Nats-Msg-Id"
EXPECTED_STREAM: Final = "Nats-Expected-Stream"
EXPECTED_LAST_SEQUENCE: Final = "Nats-Expected-Last-Sequence"
EXPECTED_LAST_SUBJECT_SEQUENCE: Final = "Nats-Expected-Last-Subject-Sequence"
EXPECTED_LAST_MSG_ID: Final = "Nats-Expected-Last-Msg-Id"

# -- per-message TTL (ADR-43, server 2.11+) --
TTL: Final = "Nats-TTL"
TTL_NEVER: Final = "never"

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
