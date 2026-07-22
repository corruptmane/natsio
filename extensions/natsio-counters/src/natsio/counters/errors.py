"""Counter error types (mirrors orbit.go ``counters/errors.go``)."""

from natsio.jetstream.errors import JetStreamError

__all__ = [
    "CounterNotEnabledError",
    "CounterNotFoundError",
    "CounterSubjectNotInitializedError",
    "DirectAccessRequiredError",
    "InvalidCounterValueError",
]


class CounterNotEnabledError(JetStreamError):
    """Wrapped a stream that lacks ``allow_msg_counter``.

    ``allow_msg_counter`` can only be set at stream creation (ADR-49); an
    existing plain stream cannot be turned into a counter after the fact.
    """


class DirectAccessRequiredError(JetStreamError):
    """Wrapped a stream that lacks ``allow_direct``.

    Reads (``load``/``get``/``get_multiple``) go through Direct Get, so the
    backing stream must have ``allow_direct=True``.
    """


class InvalidCounterValueError(JetStreamError):
    """A counter delta or a stored counter payload was not a valid integer."""


class CounterNotFoundError(JetStreamError):
    """No counter stream with that name exists."""


class CounterSubjectNotInitializedError(JetStreamError):
    """The subject has no stored increment yet — its counter is uninitialized."""
