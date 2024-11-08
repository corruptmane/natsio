import pytest

from natsio.exceptions.client import InvalidHeaderVersion
from natsio.protocol.parser import parse_headers


def test_basic_headers_parsing():
    data = b"NATS/1.0\r\nContent-Type: text/plain\r\nSubject: greetings\r\n\r\n"
    expected_output = {
        "Content-Type": "text/plain",
        "Subject": "greetings"
    }
    result = parse_headers(data)
    assert result == expected_output


def test_multiple_headers_parsing():
    data = b"NATS/1.0\r\nHeader1: Value1\r\nHeader2: Value2\r\nHeader3: Value3\r\n\r\n"
    expected_output = {
        "Header1": "Value1",
        "Header2": "Value2",
        "Header3": "Value3"
    }
    result = parse_headers(data)
    assert result == expected_output


def test_headers_with_whitespace_variations():
    data = b"NATS/1.0\r\nKey1:Value1\r\nKey2 : Value2\r\nKey3    :    Value3\r\n\r\n"
    expected_output = {
        "Key1": "Value1",
        "Key2": "Value2",
        "Key3": "Value3"
    }
    result = parse_headers(data)
    assert result == expected_output


def test_duplicate_keys():
    data = b"NATS/1.0\r\nDupKey: FirstValue\r\nDupKey: SecondValue\r\n\r\n"
    expected_output = {
        "DupKey": "SecondValue"
    }
    result = parse_headers(data)
    assert result == expected_output


def test_case_sensitivity():
    data = b"NATS/1.0\r\nContent-Type: text/plain\r\ncontent-type: text/html\r\n\r\n"
    expected_output = {
        "Content-Type": "text/plain",
        "content-type": "text/html"
    }
    result = parse_headers(data)
    assert result == expected_output


def test_headers_with_leading_and_trailing_whitespace():
    data = b"NATS/1.0\r\n  KeyWithSpaces  :   ValueWithSpaces   \r\n\r\n"
    expected_output = {
        "KeyWithSpaces": "ValueWithSpaces"
    }
    result = parse_headers(data)
    assert result == expected_output


def test_empty_headers():
    data = b"NATS/1.0\r\n\r\n"
    expected_output = None
    result = parse_headers(data)
    assert result is expected_output


def test_malformed_headers():
    data = b"NATS/1.0\r\nInvalidHeader\r\nAnotherHeader WithoutColon\r\n\r\n"
    with pytest.raises(ValueError, match="Malformed header line"):
        parse_headers(data)


def test_headers_with_empty_values():
    data = b"NATS/1.0\r\nEmptyValueHeader:\r\nAnotherHeader: Value\r\n\r\n"
    expected_output = {
        "EmptyValueHeader": "",
        "AnotherHeader": "Value"
    }
    result = parse_headers(data)
    assert result == expected_output


def test_missing_version_line():
    data = b"Content-Type: text/plain\r\n\r\n"
    with pytest.raises(InvalidHeaderVersion):
        parse_headers(data)


def test_headers_with_special_characters():
    data = b"NATS/1.0\r\nX-Custom-Header: Value-With-Special_Characters!@#$%^&*\r\n\r\n"
    expected_output = {
        "X-Custom-Header": "Value-With-Special_Characters!@#$%^&*"
    }
    result = parse_headers(data)
    assert result == expected_output


def test_large_number_of_headers():
    headers = "".join([f"Header{i}: Value{i}\r\n" for i in range(1000)])
    data = ("NATS/1.0\r\n" + headers + "\r\n").encode()
    expected_output = {f"Header{i}": f"Value{i}" for i in range(1000)}
    result = parse_headers(data)
    assert result == expected_output


def test_unicode_characters_in_headers():
    data = "NATS/1.0\r\nшо: нішо\r\n\r\n".encode("utf-8")
    with pytest.raises(ValueError, match="Invalid characters in header key"):
        parse_headers(data)


def test_invalid_encoding():
    data = b"NATS/1.0\r\nInvalidEncoding: \xff\xfe\xfd\r\n\r\n"
    with pytest.raises(UnicodeDecodeError):
        parse_headers(data)


def test_header_value_with_whitespace():
    data = b"NATS/1.0\r\nHeader:    Value with spaces    \r\n\r\n"
    expected_output = {
        "Header": "Value with spaces"
    }
    result = parse_headers(data)
    assert result == expected_output


def test_no_headers_after_version_line():
    data = b"NATS/1.0\r\n\r\n"
    expected_output = None
    result = parse_headers(data)
    assert result is expected_output


def test_headers_with_multiple_colons_in_value():
    data = b"NATS/1.0\r\nTime: 12:34:56\r\n\r\n"
    expected_output = {
        "Time": "12:34:56"
    }
    result = parse_headers(data)
    assert result == expected_output


def test_headers_with_tabs_and_whitespace():
    data = b"NATS/1.0\r\nKey\t:\tValue\t\r\n\r\n"
    expected_output = {
        "Key": "Value"
    }
    result = parse_headers(data)
    assert result == expected_output


def test_headers_with_empty_lines_between_headers():
    data = b"NATS/1.0\r\nHeader1: Value1\r\n\r\nHeader2: Value2\r\n\r\n"
    expected_output = {
        "Header1": "Value1"
    }
    result = parse_headers(data)
    assert result == expected_output


def test_malformed_version_line():
    data = b"NATS/1.1\r\nHeader: Value\r\n\r\n"
    with pytest.raises(InvalidHeaderVersion):
        parse_headers(data)


def test_headers_with_no_colon():
    data = b"NATS/1.0\r\nHeaderWithoutColon\r\n\r\n"
    with pytest.raises(ValueError, match="Malformed header line"):
        parse_headers(data)


def test_header_with_colon_in_key():
    data = b"NATS/1.0\r\nKey:Part1: Value\r\n\r\n"
    expected_output = {
        "Key": "Part1: Value"
    }
    result = parse_headers(data)
    assert result == expected_output


def test_headers_with_non_ascii_characters_in_key():
    data = "NATS/1.0\r\nключ: Value\r\n\r\n".encode("utf-8")
    with pytest.raises(ValueError, match="Invalid characters in header key"):
        parse_headers(data)


def test_headers_with_non_ascii_characters_in_value():
    data = "NATS/1.0\r\nHeader: значення\r\n\r\n".encode("utf-8")
    expected_output = {
        "Header": "значення"
    }
    result = parse_headers(data)
    assert result == expected_output


def test_headers_with_folded_lines():
    data = b"NATS/1.0\r\nFoldedHeader: Line1\r\n Line2\r\n\r\n"
    with pytest.raises(ValueError, match="Malformed header line"):
        parse_headers(data)
