import pytest
from helpers import drain_events, msg_frame

from natsio._internal.protocol import NEED_DATA, Parser
from natsio.errors import MaxControlLineExceededError, ParserError


def test_unknown_operation_is_fatal() -> None:
    parser = Parser()
    parser.receive_data(b"BOGUS stuff\r\n")
    with pytest.raises(ParserError, match="unknown protocol operation"):
        parser.next_event()


def test_parser_refuses_use_after_fatal_error() -> None:
    parser = Parser()
    parser.receive_data(b"BOGUS\r\n")
    with pytest.raises(ParserError):
        parser.next_event()
    with pytest.raises(ParserError):
        parser.next_event()
    with pytest.raises(ParserError):
        parser.receive_data(b"PING\r\n")


@pytest.mark.parametrize(
    "line",
    [
        b"MSG foo\r\n",  # too few args
        b"MSG foo 1 2 3 4 5\r\n",  # too many args
        b"MSG foo 1 -5\r\nxxxxx\r\n",  # negative size
        b"MSG foo 1 5x\r\n",  # non-numeric size
        b"MSG foo bar 5\r\n",  # non-numeric sid
        b"HMSG foo 1 10 5\r\n",  # header size > total size
        b"HMSG foo 1 5\r\n",  # too few args
    ],
)
def test_malformed_control_lines_are_fatal(line: bytes) -> None:
    parser = Parser()
    parser.receive_data(line)
    with pytest.raises(ParserError):
        parser.next_event()


def test_non_ascii_subject_is_fatal() -> None:
    parser = Parser()
    parser.receive_data("MSG føø 1 2\r\nhi\r\n".encode())
    with pytest.raises(ParserError, match="non-ASCII"):
        parser.next_event()


def test_payload_not_terminated_by_crlf_is_fatal() -> None:
    parser = Parser()
    parser.receive_data(b"MSG foo 1 5\r\nhelloXXtrailing")
    with pytest.raises(ParserError, match="not terminated by CRLF"):
        parser.next_event()


def test_control_line_limit_without_crlf() -> None:
    parser = Parser(max_control_line=64)
    parser.receive_data(b"MSG " + b"a" * 100)
    with pytest.raises(MaxControlLineExceededError):
        parser.next_event()


def test_control_line_limit_with_crlf_present() -> None:
    parser = Parser(max_control_line=32)
    parser.receive_data(b"MSG " + b"a" * 60 + b" 1 2\r\nhi\r\n")
    with pytest.raises(MaxControlLineExceededError):
        parser.next_event()


def test_slow_growth_below_limit_is_fine() -> None:
    parser = Parser(max_control_line=4096)
    for chunk in (b"MSG fo", b"o 1", b" 2\r", b"\nh", b"i\r\n"):
        parser.receive_data(chunk)
    events = drain_events(parser)
    assert len(events) == 1


def test_announced_payload_beyond_max_is_fatal() -> None:
    parser = Parser(max_payload=1024)
    parser.receive_data(b"MSG foo 1 2048\r\n")
    with pytest.raises(ParserError, match="exceeds max"):
        parser.next_event()


def test_set_max_payload_takes_effect() -> None:
    parser = Parser(max_payload=4)
    parser.set_max_payload(1024)
    parser.receive_data(msg_frame("foo", 1, b"longer than four"))
    assert len(drain_events(parser)) == 1


def test_need_data_mid_payload_is_not_an_error() -> None:
    parser = Parser()
    parser.receive_data(b"MSG foo 1 10\r\n12345")
    assert parser.next_event() is NEED_DATA
    parser.receive_data(b"67890\r\n")
    assert len(drain_events(parser)) == 1


def test_huge_digit_run_in_size_is_parser_error_not_valueerror() -> None:
    # CPython's int() raises ValueError past ~4300 digits; that must never
    # escape as a non-NATSError or leave the parser resumable.
    parser = Parser(max_control_line=8192)
    parser.receive_data(b"MSG foo 1 " + b"9" * 4400 + b"\r\n")
    with pytest.raises(ParserError, match="invalid size"):
        parser.next_event()
    with pytest.raises(ParserError):
        parser.next_event()  # terminal


def test_huge_digit_run_in_sid_is_parser_error() -> None:
    parser = Parser(max_control_line=8192)
    parser.receive_data(b"MSG foo " + b"9" * 4400 + b" 5\r\nhello\r\n")
    with pytest.raises(ParserError, match="invalid sid"):
        parser.next_event()
