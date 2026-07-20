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
