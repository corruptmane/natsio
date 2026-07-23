"""Partitioned consumer group errors.

Rooted in `natsio.jetstream.JetStreamError`, so a caller that already
catches JetStream failures catches these too.
"""

from natsio.jetstream.errors import JetStreamError

__all__ = [
    "ConsumerGroupConfigError",
    "ConsumerGroupError",
    "ConsumerGroupExistsError",
    "ConsumerGroupNotFoundError",
    "GroupConfigChangedError",
    "MemberNotInGroupError",
]


class ConsumerGroupError(JetStreamError):
    """Root of the partitioned-consumer-group hierarchy."""


class ConsumerGroupConfigError(ConsumerGroupError):
    """A consumer group config failed validation.

    Raised both on the write path (`create_static`, `create_elastic`,
    `set_member_mappings`) and on the read path — a config stored by another
    client that does not validate is an error, never a silent default.
    """


class ConsumerGroupNotFoundError(ConsumerGroupError):
    """No consumer group config exists for that (stream, group) pair.

    Also raised when the whole KV bucket is missing — i.e. no consumer group
    of that flavour has ever been created in this account.
    """


class ConsumerGroupExistsError(ConsumerGroupError):
    """A consumer group of that name exists with a different configuration.

    Creation is idempotent only for an identical config; anything else has to
    be deleted and re-created (the partition-to-member assignment is baked into
    the JetStream consumers).
    """


class MemberNotInGroupError(ConsumerGroupError):
    """The member name is not in the group's current membership.

    Fatal when joining a **static** group (its membership never changes). For
    an **elastic** group this is not raised at join time: a member may join
    before being added, and starts consuming when the membership changes.
    """


class GroupConfigChangedError(ConsumerGroupError):
    """A running member saw its group's config change underneath it.

    Static groups have an immutable config, so any change (or an unparseable
    one) terminates every member instance — the oracle's contract. For elastic
    groups this covers only the immutable part (max members, buffering limits,
    partitioning filters); membership changes are applied, not fatal.
    """
