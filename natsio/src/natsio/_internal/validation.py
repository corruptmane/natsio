"""Subject and queue-group validation.

Applied at the public API boundary so malformed input fails loudly in the
caller's frame rather than becoming a server ``-ERR`` (or, worse, a wire-framing
hazard) later on.
"""

import re

from natsio.errors import ConfigError

__all__ = [
    "validate_consumer_name",
    "validate_queue_group",
    "validate_stream_name",
    "validate_subject",
]

_ILLEGAL = frozenset(" \t\r\n")
# Stream/consumer names become a single subject token, so they reject everything
# that would break routing (mirrors nats.go jetstream.validateStreamName).
_ILLEGAL_NAME = frozenset(">*. /\\\t\r\n")

# Fast-reject probe for validate_subject: a subject containing NONE of these
# characters is a plain dotted name (no whitespace, no wildcard markers), so only
# the empty-token checks can still reject it. The class MUST include ``*`` and
# ``>`` for BOTH wildcard modes — a whitespace-only probe would wrongly let the
# fast path accept ``foo.*bar`` / ``fo>o`` (mid-token wildcards) in wildcards
# mode, which the full scan below rejects. Any hit here falls through to the
# unchanged full scan, so every error message stays byte-identical.
_SUBJECT_SPECIAL = re.compile(r"[ \t\r\n*>]")


def validate_subject(subject: str, *, wildcards: bool = False, argument: str = "subject") -> None:
    """Validate a subject.

    ``wildcards=False`` (publish, reply-to) rejects ``*`` and ``>`` entirely;
    ``wildcards=True`` (subscribe) allows ``*`` as a whole token and ``>`` as a
    whole final token.
    """
    if not subject:
        raise ConfigError(f"{argument} must not be empty")
    if _SUBJECT_SPECIAL.search(subject) is None:
        # Plain dotted name: no whitespace, no wildcard markers. The only way the
        # full scan below could still reject it is an empty token (leading dot,
        # trailing dot, or ``..``), and it reaches the identical error there.
        if subject[0] == "." or subject[-1] == "." or ".." in subject:
            raise ConfigError(f"{argument} must not contain empty tokens: {subject!r}")
        return
    if any(char in _ILLEGAL for char in subject):
        raise ConfigError(f"{argument} must not contain whitespace or line breaks: {subject!r}")

    tokens = subject.split(".")
    last = len(tokens) - 1
    for index, token in enumerate(tokens):
        if not token:
            raise ConfigError(f"{argument} must not contain empty tokens: {subject!r}")
        if ">" in token:
            if not wildcards:
                raise ConfigError(f"{argument} must not contain wildcards: {subject!r}")
            if token != ">":
                raise ConfigError(f"'>' must occupy a whole token in {argument}: {subject!r}")
            if index != last:
                raise ConfigError(f"'>' must be the last token in {argument}: {subject!r}")
        elif "*" in token:
            if not wildcards:
                raise ConfigError(f"{argument} must not contain wildcards: {subject!r}")
            if token != "*":
                raise ConfigError(f"'*' must occupy a whole token in {argument}: {subject!r}")


def validate_stream_name(name: str, *, argument: str = "stream name") -> None:
    """Reject names that would not survive interpolation into an API subject.

    Empty raises ``<argument> is required``; any of ``> * . / \\``, whitespace,
    or a line break raises. Matches nats.go's ``validateStreamName`` character
    set so a dotted name fails in the caller's frame instead of hanging the JS
    timeout against a single-token subject wildcard that can never match.
    """
    if not name:
        raise ConfigError(f"{argument} is required")
    if any(char in _ILLEGAL_NAME for char in name):
        raise ConfigError(f"{argument} must not contain '.', spaces, wildcards, slashes, or line breaks: {name!r}")


def validate_consumer_name(name: str, *, argument: str = "consumer name") -> None:
    """Consumer names carry the same single-token constraints as stream names."""
    validate_stream_name(name, argument=argument)


def validate_queue_group(queue: str) -> None:
    if not queue:
        raise ConfigError("queue group must not be empty")
    if any(char in _ILLEGAL for char in queue):
        raise ConfigError(f"queue group must not contain whitespace: {queue!r}")
    if "*" in queue or ">" in queue:
        raise ConfigError(f"queue group must not contain wildcards: {queue!r}")
