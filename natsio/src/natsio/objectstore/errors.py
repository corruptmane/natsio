"""Object Store error types."""

from natsio.errors import ConfigError
from natsio.jetstream.errors import JetStreamError

__all__ = [
    "BucketExistsError",
    "BucketNotFoundError",
    "DigestMismatchError",
    "InvalidBucketNameError",
    "InvalidObjectNameError",
    "LinkError",
    "ObjectDeletedError",
    "ObjectExistsError",
    "ObjectNotFoundError",
]


class BucketNotFoundError(JetStreamError):
    """No Object Store bucket with that name exists."""


class BucketExistsError(JetStreamError):
    """A bucket with that name exists with a different configuration."""


class ObjectNotFoundError(JetStreamError):
    """The object does not exist (or its latest revision is a delete marker)."""


class ObjectDeletedError(ObjectNotFoundError):
    """The object's latest revision is a delete marker (a *kind* of not-found,
    matching the ecosystem; catch this subclass to tell "deleted" from "never
    existed")."""


class ObjectExistsError(JetStreamError):
    """``add_link`` would overwrite a live object that is not a link."""


class DigestMismatchError(JetStreamError):
    """The data read back does not match the object's recorded SHA-256 digest.

    Either the stream lost/gained chunks or the metadata is corrupt — the
    partially-yielded data must be discarded.
    """


class LinkError(JetStreamError):
    """The link cannot be created or followed (bucket link, chained link,
    deleted target, ...)."""


class InvalidBucketNameError(ConfigError):
    pass


class InvalidObjectNameError(ConfigError):
    pass
