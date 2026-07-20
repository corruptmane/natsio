import pytest
from helpers import header_block

from natsio._internal.protocol import Headers, encode_header_block, parse_header_block
from natsio.errors import BadHeadersError


class TestParse:
    def test_empty_block(self) -> None:
        headers, status = parse_header_block(header_block())
        assert headers is None
        assert status is None

    def test_single_header(self) -> None:
        headers, status = parse_header_block(header_block("Key: value"))
        assert status is None
        assert headers is not None
        assert headers["Key"] == "value"
        assert len(headers) == 1

    def test_multi_value_headers_are_retained(self) -> None:
        headers, _ = parse_header_block(header_block("K: one", "K: two", "K: three"))
        assert headers is not None
        assert headers["K"] == "one"
        assert headers.get_all("K") == ["one", "two", "three"]

    def test_lookup_is_case_sensitive_and_preserving(self) -> None:
        headers, _ = parse_header_block(header_block("Nats-Msg-Id: 7"))
        assert headers is not None
        assert headers["Nats-Msg-Id"] == "7"
        assert headers.get("nats-msg-id") is None
        assert list(headers) == ["Nats-Msg-Id"]

    def test_value_whitespace_is_stripped(self) -> None:
        headers, _ = parse_header_block(header_block("K:    padded   \t"))
        assert headers is not None
        assert headers["K"] == "padded"

    def test_value_may_contain_colons_and_utf8(self) -> None:
        headers, _ = parse_header_block(header_block("K: a:b:c", "U: приліт"))
        assert headers is not None
        assert headers["K"] == "a:b:c"
        assert headers["U"] == "приліт"

    def test_empty_value(self) -> None:
        headers, _ = parse_header_block(header_block("K:"))
        assert headers is not None
        assert headers["K"] == ""

    def test_malformed_lines_are_skipped_not_fatal(self) -> None:
        block = header_block("no-colon-line", "Good: yes", ": empty-key", "Bad key: x")
        headers, _ = parse_header_block(block)
        assert headers is not None
        assert dict(headers) == {"Good": "yes"}

    def test_inline_status_bare(self) -> None:
        _, status = parse_header_block(header_block(status="503"))
        assert status is not None
        assert (status.code, status.description) == (503, "")

    def test_inline_status_with_description(self) -> None:
        _, status = parse_header_block(header_block(status="409 Consumer Deleted"))
        assert status is not None
        assert (status.code, status.description) == (409, "Consumer Deleted")

    def test_inline_status_with_headers(self) -> None:
        block = header_block("Nats-Last-Consumer: 5", status="100 Idle Heartbeat")
        headers, status = parse_header_block(block)
        assert status is not None
        assert status.description == "Idle Heartbeat"
        assert headers is not None
        assert headers["Nats-Last-Consumer"] == "5"

    def test_inline_status_irregular_whitespace(self) -> None:
        _, status = parse_header_block(b"NATS/1.0  404   No Messages\r\n\r\n")
        assert status is not None
        assert (status.code, status.description) == (404, "No Messages")

    def test_inline_status_bare_code_no_description(self) -> None:
        _, status = parse_header_block(b"NATS/1.0 503\r\n\r\n")
        assert status is not None
        assert (status.code, status.description) == (503, "")

    def test_inline_status_whitespace_only_is_no_status(self) -> None:
        _, status = parse_header_block(b"NATS/1.0   \r\n\r\n")
        assert status is None

    def test_non_numeric_status_is_not_a_status(self) -> None:
        _, status = parse_header_block(b"NATS/1.0 xyz\r\n\r\n")
        assert status is None

    def test_wrong_version_line_raises(self) -> None:
        with pytest.raises(BadHeadersError, match="version"):
            parse_header_block(b"HTTP/1.1 200\r\n\r\n")

    def test_missing_terminator_raises(self) -> None:
        with pytest.raises(BadHeadersError, match="CRLF"):
            parse_header_block(b"NATS/1.0\r\nK: v\r\n")


class TestEncode:
    def test_round_trip(self) -> None:
        original = Headers({"A": "1", "B": ["x", "y"], "Empty": ""})
        headers, status = parse_header_block(encode_header_block(original))
        assert status is None
        assert headers == original

    def test_exact_bytes(self) -> None:
        assert encode_header_block({"A": "1"}) == b"NATS/1.0\r\nA: 1\r\n\r\n"

    def test_multi_value_from_plain_mapping(self) -> None:
        block = encode_header_block({"K": ["one", "two"]})
        assert block == b"NATS/1.0\r\nK: one\r\nK: two\r\n\r\n"

    def test_key_charset_boundaries_are_valid(self) -> None:
        headers, _ = parse_header_block(encode_header_block({"!": "a", "~": "b"}))
        assert headers is not None
        assert dict(headers) == {"!": "a", "~": "b"}

    @pytest.mark.parametrize(
        "key",
        ["", "with space", "colon:key", "tab\tkey", "newline\nkey", "über", "high\x7f"],
    )
    def test_invalid_keys_rejected(self, key: str) -> None:
        with pytest.raises(BadHeadersError, match="key"):
            encode_header_block({key: "v"})

    @pytest.mark.parametrize("value", ["a\r\nInjected: x", "a\rb", "a\nb"])
    def test_crlf_injection_in_value_rejected(self, value: str) -> None:
        with pytest.raises(BadHeadersError, match="CR/LF"):
            encode_header_block({"K": value})


class TestHeadersType:
    def test_mapping_semantics_first_value(self) -> None:
        headers = Headers()
        headers.add("K", "1")
        headers.add("K", "2")
        assert headers["K"] == "1"
        assert headers.get("K") == "1"
        assert headers.get("missing") is None
        assert "K" in headers
        assert len(headers) == 1

    def test_set_replaces_all_values(self) -> None:
        headers = Headers({"K": ["1", "2"]})
        headers.set("K", "3")
        assert headers.get_all("K") == ["3"]

    def test_discard(self) -> None:
        headers = Headers({"K": "1"})
        headers.discard("K")
        headers.discard("K")  # idempotent
        assert len(headers) == 0

    def test_allitems_includes_repeats(self) -> None:
        headers = Headers({"A": ["1", "2"], "B": "3"})
        assert list(headers.allitems()) == [("A", "1"), ("A", "2"), ("B", "3")]

    def test_equality_with_plain_mapping(self) -> None:
        assert Headers({"A": "1"}) == {"A": "1"}
        assert Headers({"A": ["1", "2"]}) != {"A": "1"}


class TestCopy:
    def test_copy_from_headers_preserves_multivalue(self) -> None:
        src = Headers({"A": ["1", "2"], "B": "3"})
        copy = Headers(src)
        assert copy.get_all("A") == ["1", "2"]
        assert copy == src
        # And the copy is independent.
        copy.add("A", "4")
        assert src.get_all("A") == ["1", "2"]
