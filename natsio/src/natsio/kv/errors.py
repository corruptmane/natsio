"""Key-Value error types."""

from natsio.errors import ConfigError
from natsio.jetstream.errors import JetStreamError

__all__ = [
    "BucketNotFoundError",
    "InvalidBucketNameError",
    "InvalidKeyError",
    "KeyDeletedError",
    "KeyExistsError",
    "KeyNotFoundError",
]


class BucketNotFoundError(JetStreamError):
    """No Key-Value bucket with that name exists."""


class KeyNotFoundError(JetStreamError):
    """The key has no live value."""


class KeyDeletedError(KeyNotFoundError):
    """The key's latest revision is a delete/purge marker.

    ``revision`` is the marker's revision — useful for ``create()``-style
    compare-and-set flows on previously-deleted keys.
    """

    def __init__(self, description: str = "", *, revision: int = 0) -> None:
        super().__init__(description)
        self.revision = revision


class KeyExistsError(JetStreamError):
    """``create()`` was called for a key that already has a live value."""

    def __init__(self, description: str = "", *, revision: int = 0) -> None:
        super().__init__(description)
        self.revision = revision


class InvalidBucketNameError(ConfigError):
    pass


class InvalidKeyError(ConfigError):
    pass
