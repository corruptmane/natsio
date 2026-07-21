import pytest

from natsio._internal.validation import validate_queue_group, validate_subject
from natsio.errors import ConfigError


class TestSubjects:
    @pytest.mark.parametrize("subject", ["foo", "foo.bar", "foo.bar.baz", "_INBOX.abc.1", "a"])
    def test_valid_plain_subjects(self, subject: str) -> None:
        validate_subject(subject)

    @pytest.mark.parametrize(
        "subject",
        ["", ".foo", "foo.", "foo..bar", "foo bar", "foo\tbar", "foo\r\nbar", "foo\nbar"],
    )
    def test_structurally_invalid(self, subject: str) -> None:
        with pytest.raises(ConfigError):
            validate_subject(subject)

    @pytest.mark.parametrize("subject", ["foo.*", "foo.>", "*", ">", "*.bar"])
    def test_wildcards_rejected_for_publish(self, subject: str) -> None:
        with pytest.raises(ConfigError, match="wildcard"):
            validate_subject(subject)

    @pytest.mark.parametrize("subject", ["foo.*", "foo.>", "*", ">", "*.bar", "foo.*.baz", "foo.*.>"])
    def test_wildcards_allowed_for_subscribe(self, subject: str) -> None:
        validate_subject(subject, wildcards=True)

    @pytest.mark.parametrize("subject", ["foo.>.bar", ">.foo"])
    def test_gt_must_be_final_token(self, subject: str) -> None:
        with pytest.raises(ConfigError, match="last token"):
            validate_subject(subject, wildcards=True)

    @pytest.mark.parametrize("subject", ["foo.ba*", "foo.*bar", "fo>o"])
    def test_wildcards_must_be_whole_tokens(self, subject: str) -> None:
        with pytest.raises(ConfigError, match="whole token"):
            validate_subject(subject, wildcards=True)

    def test_error_names_the_argument(self) -> None:
        with pytest.raises(ConfigError, match="reply subject"):
            validate_subject("", argument="reply subject")


class TestQueueGroups:
    def test_valid(self) -> None:
        validate_queue_group("workers")

    @pytest.mark.parametrize("queue", ["", "with space", "star*", "gt>"])
    def test_invalid(self, queue: str) -> None:
        with pytest.raises(ConfigError):
            validate_queue_group(queue)


# A byte-for-byte copy of validate_subject as it stood BEFORE the compiled-regex
# fast path, used as the differential oracle below. The fast path must produce
# the identical accept/reject decision AND the identical error message for every
# input, in both wildcard modes.
_REF_ILLEGAL = frozenset(" \t\r\n")


def _reference_validate_subject(subject: str, *, wildcards: bool = False, argument: str = "subject") -> None:
    if not subject:
        raise ConfigError(f"{argument} must not be empty")
    if any(char in _REF_ILLEGAL for char in subject):
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


# ~40 cases spanning every branch: valid plain, valid wildcarded, empty tokens,
# leading/trailing dots, embedded whitespace (space/tab/CR/LF), mid-token */>,
# lone */> in the wrong position, whitespace+dot combinations, and empty string.
_DIFFERENTIAL_CORPUS = [
    # valid plain (fast path returns)
    "foo",
    "foo.bar",
    "foo.bar.baz",
    "a",
    "_INBOX.abc.1",
    "x.y.z.w.v",
    # valid wildcarded (must fall through to the full scan)
    "foo.*",
    "foo.>",
    "*",
    ">",
    "*.bar",
    "foo.*.baz",
    "foo.*.>",
    "*.*.>",
    # empty interior tokens
    "foo..bar",
    "a..b",
    # leading / trailing dots
    ".foo",
    "foo.",
    ".",
    "..",
    "...",
    "foo.bar.",
    ".foo.bar",
    # embedded whitespace (space, tab, CR, LF, CRLF)
    "foo bar",
    "foo\tbar",
    "foo\rbar",
    "foo\nbar",
    "foo\r\nbar",
    "a b.c",
    " ",
    "\t",
    # mid-token wildcards — the trap: must reject in wildcards mode, not accept
    "foo.*bar",
    "foo.ba*",
    "fo>o",
    "a*b",
    "x.>y",
    "foo->bar",
    # lone wildcard in the wrong position
    "foo.>.bar",
    ">.foo",
    "a.*.>.b",
    # whitespace combined with structural errors (whitespace must win)
    ".foo bar",
    "foo. .bar",
    "foo.*.bar baz",
    # empty
    "",
]


def _outcome(fn, subject: str, wildcards: bool):
    """Return ('ok', None) or ('err', message) for one validation call."""
    try:
        fn(subject, wildcards=wildcards)
    except ConfigError as exc:
        return ("err", str(exc))
    return ("ok", None)


class TestSubjectFastPathDifferential:
    """The regex fast path must be indistinguishable from the pre-fast-path scan."""

    @pytest.mark.parametrize("subject", _DIFFERENTIAL_CORPUS)
    @pytest.mark.parametrize("wildcards", [False, True])
    def test_matches_reference(self, subject: str, wildcards: bool) -> None:
        expected = _outcome(_reference_validate_subject, subject, wildcards)
        actual = _outcome(validate_subject, subject, wildcards)
        # Same accept/reject, same error type (ConfigError), same message bytes.
        assert actual == expected, f"{subject!r} wildcards={wildcards}: {actual} != {expected}"

    def test_trap_midtoken_wildcards_are_rejected_in_wildcard_mode(self) -> None:
        # A whitespace-only fast-path trigger would wrongly accept these.
        for subject in ("foo.*bar", "fo>o"):
            with pytest.raises(ConfigError, match="whole token"):
                validate_subject(subject, wildcards=True)
