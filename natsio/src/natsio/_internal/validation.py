"""Subject and queue-group validation.

Applied at the public API boundary so malformed input fails loudly in the
caller's frame rather than becoming a server ``-ERR`` (or, worse, a wire-framing
hazard) later on.
"""

from natsio.errors import ConfigError

__all__ = ["validate_queue_group", "validate_subject"]

_ILLEGAL = frozenset(" \t\r\n")


def validate_subject(subject: str, *, wildcards: bool = False, argument: str = "subject") -> None:
    """Validate a subject.

    ``wildcards=False`` (publish, reply-to) rejects ``*`` and ``>`` entirely;
    ``wildcards=True`` (subscribe) allows ``*`` as a whole token and ``>`` as a
    whole final token.
    """
    if not subject:
        raise ConfigError(f"{argument} must not be empty")
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


def validate_queue_group(queue: str) -> None:
    if not queue:
        raise ConfigError("queue group must not be empty")
    if any(char in _ILLEGAL for char in queue):
        raise ConfigError(f"queue group must not contain whitespace: {queue!r}")
    if "*" in queue or ">" in queue:
        raise ConfigError(f"queue group must not contain wildcards: {queue!r}")
