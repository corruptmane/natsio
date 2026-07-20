import pytest
from helpers import (
    concat,
    err_frame,
    header_block,
    hmsg_frame,
    info_frame,
    msg_frame,
    parse_whole,
)

from natsio._internal.protocol import (
    NEED_DATA,
    ErrEvent,
    HMsgEvent,
    InfoEvent,
    MsgEvent,
    OkEvent,
    Parser,
    PingEvent,
    PongEvent,
)


def test_empty_parser_needs_data() -> None:
    assert Parser().next_event() is NEED_DATA


def test_ping_pong_ok() -> None:
    events = parse_whole(b"PING\r\nPONG\r\n+OK\r\n")
    assert events == [PingEvent(), PongEvent(), OkEvent()]


@pytest.mark.parametrize(
    "stream",
    [
        b"PING  \r\n",
        b"PING x\r\n",
        b"PING\t\r\n",
    ],
)
def test_ping_with_trailing_content_tolerated(stream: bytes) -> None:
    assert parse_whole(stream) == [PingEvent()]


@pytest.mark.parametrize(
    "stream",
    [
        b"PONG \r\n",
        b"PONG whatever\r\n",
    ],
)
def test_pong_with_trailing_content_tolerated(stream: bytes) -> None:
    assert parse_whole(stream) == [PongEvent()]


@pytest.mark.parametrize(
    "stream",
    [
        b"+OK\r\n",
        b"+OKay\r\n",
        b"+OK something\r\n",
    ],
)
def test_ok_with_trailing_content_tolerated(stream: bytes) -> None:
    assert parse_whole(stream) == [OkEvent()]


def test_info() -> None:
    (event,) = parse_whole(info_frame(b'{"server_id":"x","max_payload":1048576}'))
    assert event == InfoEvent(raw=b'{"server_id":"x","max_payload":1048576}')


def test_err_quotes_stripped() -> None:
    (event,) = parse_whole(err_frame("Stale Connection"))
    assert event == ErrEvent(message="Stale Connection")


def test_err_without_quotes() -> None:
    (event,) = parse_whole(b"-ERR Unknown Protocol Operation\r\n")
    assert event == ErrEvent(message="Unknown Protocol Operation")


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        (b"-ERR Unknown Protocol Operation\r\n", "Unknown Protocol Operation"),
        (b"-ERR 'Stale Connection'\r\n", "Stale Connection"),
        (b"-ERR 'Authorization Violation\r\n", "Authorization Violation"),
        (b"-ERR Permissions Violation'\r\n", "Permissions Violation"),
        (b"-ERR ''double''\r\n", "double"),
    ],
)
def test_err_quote_normalization(line: bytes, expected: str) -> None:
    (event,) = parse_whole(line)
    assert event == ErrEvent(message=expected)


def test_msg_without_reply() -> None:
    (event,) = parse_whole(msg_frame("foo.bar", 9, b"hello"))
    assert event == MsgEvent(subject="foo.bar", sid=9, reply_to=None, payload=b"hello")


def test_msg_with_reply() -> None:
    (event,) = parse_whole(msg_frame("foo", 1, b"x", reply="_INBOX.abc.1"))
    assert event == MsgEvent(subject="foo", sid=1, reply_to="_INBOX.abc.1", payload=b"x")


def test_msg_empty_payload() -> None:
    (event,) = parse_whole(msg_frame("foo", 2, b""))
    assert isinstance(event, MsgEvent)
    assert event.payload == b""


def test_msg_payload_containing_crlf() -> None:
    payload = b"line1\r\nline2\r\n\r\nMSG fake 1 3\r\n"
    (event,) = parse_whole(msg_frame("foo", 3, payload))
    assert isinstance(event, MsgEvent)
    assert event.payload == payload


def test_msg_tab_separated_args() -> None:
    (event,) = parse_whole(b"MSG foo\t7\t2\r\nhi\r\n")
    assert event == MsgEvent(subject="foo", sid=7, reply_to=None, payload=b"hi")


def test_hmsg_tab_separated_args_no_reply() -> None:
    block = header_block("A: 1")
    frame = f"HMSG s\t3\t{len(block)}\t{len(block) + 2}\r\n".encode() + block + b"hi\r\n"
    (event,) = parse_whole(frame)
    assert isinstance(event, HMsgEvent)
    assert event.subject == "s"
    assert event.sid == 3
    assert event.reply_to is None
    assert event.payload == b"hi"
    assert event.headers is not None
    assert event.headers["A"] == "1"


def test_hmsg_tab_separated_args_with_reply() -> None:
    block = header_block("A: 1")
    frame = f"HMSG s\t3\trep\t{len(block)}\t{len(block) + 2}\r\n".encode() + block + b"hi\r\n"
    (event,) = parse_whole(frame)
    assert isinstance(event, HMsgEvent)
    assert event.subject == "s"
    assert event.sid == 3
    assert event.reply_to == "rep"
    assert event.payload == b"hi"
    assert event.headers is not None
    assert event.headers["A"] == "1"


def test_lowercase_operations_accepted() -> None:
    events = parse_whole(b"ping\r\npong\r\n")
    assert events == [PingEvent(), PongEvent()]


def test_hmsg_with_headers_and_payload() -> None:
    block = header_block("A: 1", "B: two")
    (event,) = parse_whole(hmsg_frame("subj", 4, block, b"payload", reply="rep"))
    assert isinstance(event, HMsgEvent)
    assert event.subject == "subj"
    assert event.sid == 4
    assert event.reply_to == "rep"
    assert event.payload == b"payload"
    assert event.status is None
    assert event.headers is not None
    assert event.headers["A"] == "1"
    assert event.headers["B"] == "two"
    assert event.headers_error is None


def test_hmsg_status_only_no_headers() -> None:
    block = header_block(status="503")
    (event,) = parse_whole(hmsg_frame("s", 1, block, b""))
    assert isinstance(event, HMsgEvent)
    assert event.headers is None
    assert event.status is not None
    assert event.status.code == 503
    assert event.status.description == ""
    assert event.payload == b""


def test_hmsg_status_with_description_and_headers() -> None:
    block = header_block("Nats-Pending-Messages: 5", status="100 Idle Heartbeat")
    (event,) = parse_whole(hmsg_frame("s", 1, block, b""))
    assert isinstance(event, HMsgEvent)
    assert event.status is not None
    assert event.status.code == 100
    assert event.status.description == "Idle Heartbeat"
    assert event.headers is not None
    assert event.headers["Nats-Pending-Messages"] == "5"


def test_hmsg_corrupt_block_still_delivers_payload() -> None:
    corrupt = b"GARBAGE/9.9\r\nA: 1\r\n\r\n"
    (event,) = parse_whole(hmsg_frame("s", 1, corrupt, b"data"))
    assert isinstance(event, HMsgEvent)
    assert event.headers is None
    assert event.status is None
    assert event.headers_error is not None
    assert event.payload == b"data"


def test_back_to_back_frames() -> None:
    stream = concat(
        [
            msg_frame("a", 1, b"one"),
            b"PING\r\n",
            hmsg_frame("b", 2, header_block("K: v"), b"two"),
            msg_frame("c", 3, b"three", reply="r"),
        ]
    )
    events = parse_whole(stream)
    assert [type(e) for e in events] == [MsgEvent, PingEvent, HMsgEvent, MsgEvent]


def test_interleaved_receive_and_events() -> None:
    parser = Parser()
    parser.receive_data(msg_frame("x", 1, b"1"))
    first = parser.next_event()
    assert isinstance(first, MsgEvent)
    assert parser.next_event() is NEED_DATA
    parser.receive_data(b"PONG\r\n")
    assert isinstance(parser.next_event(), PongEvent)


def test_lowercase_op_preserves_argument_case() -> None:
    (event,) = parse_whole(b"msg MySubject 1 5\r\nhello\r\n")
    assert event == MsgEvent(subject="MySubject", sid=1, reply_to=None, payload=b"hello")


def test_lowercase_info_preserves_json_case() -> None:
    (event,) = parse_whole(b'info {"server_id":"AbC"}\r\n')
    assert event == InfoEvent(raw=b'{"server_id":"AbC"}')


def test_corrupt_hmsg_then_valid_frames_still_parse() -> None:
    stream = concat(
        [
            hmsg_frame("s", 1, b"GARBAGE/9.9\r\nA: 1\r\n\r\n", b"data"),
            b"PING\r\n",
            msg_frame("ok", 2, b"y"),
        ]
    )
    events = parse_whole(stream)
    assert [type(e) for e in events] == [HMsgEvent, PingEvent, MsgEvent]
    assert isinstance(events[0], HMsgEvent)
    assert events[0].headers_error is not None
    assert isinstance(events[2], MsgEvent)
    assert events[2].subject == "ok"
    assert events[2].payload == b"y"
