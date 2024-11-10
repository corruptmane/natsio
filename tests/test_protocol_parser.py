import pytest

from natsio.const import CRLF
from natsio.exceptions.protocol import UnknownProtocol
from natsio.protocol.parser import ProtocolParser
from natsio.utils.json import json_dumps
from tests.utils import FakeStream


@pytest.mark.asyncio
async def test_parse_msg_without_reply() -> None:
    subject = 'subject'
    sid = '1'
    payload = 'Hello World'
    payload_size = str(len(payload.encode()))

    data = b' '.join([part.encode() for part in (subject, sid, payload_size)])

    stream_data = payload.encode() + CRLF
    stream = FakeStream(stream_data)

    parser = ProtocolParser()

    msg = await parser.parse_msg(data, stream)

    assert msg.subject == subject
    assert msg.sid == sid
    assert msg.reply_to is None
    assert msg.payload_size == int(payload_size)
    assert msg.payload == payload.encode()


@pytest.mark.asyncio
async def test_parse_msg_with_reply() -> None:
    subject = 'subject'
    sid = '1'
    reply_to = 'inbox'
    payload = 'Hello, NATS'
    payload_size = str(len(payload.encode()))

    data = b' '.join(part.encode() for part in (subject, sid, reply_to, payload_size))

    stream_data = payload.encode() + CRLF
    stream = FakeStream(stream_data)

    parser = ProtocolParser()

    msg = await parser.parse_msg(data, stream)

    assert msg.subject == subject
    assert msg.sid == sid
    assert msg.reply_to == reply_to
    assert msg.payload_size == int(payload_size)
    assert msg.payload == payload.encode()


@pytest.mark.asyncio
async def test_parse_hmsg_without_reply() -> None:
    subject = 'subject'
    sid = '1'
    headers = 'NATS/1.0\r\nHeader: Value\r\n\r\n'
    payload = 'Hello Headers'

    headers_size = str(len(headers.encode()))
    total_size = str(len(headers.encode()) + len(payload))
    
    data = b' '.join(part.encode() for part in (subject, sid, headers_size, total_size))
    
    stream_data = headers.encode() + payload.encode() + CRLF
    stream = FakeStream(stream_data)
    
    parser = ProtocolParser()
    
    hmsg = await parser.parse_hmsg(data, stream)
    
    assert hmsg.subject == subject
    assert hmsg.sid == sid
    assert hmsg.reply_to is None
    assert hmsg.headers_size == len(headers.encode())
    assert hmsg.total_size == len(headers.encode()) + len(payload.encode())
    assert hmsg.headers == {'Header': 'Value'}
    assert hmsg.payload == payload.encode()


@pytest.mark.asyncio
async def test_parse_hmsg_with_reply() -> None:
    subject = 'subject'
    sid = '1'
    reply_to = 'inbox'
    headers = 'NATS/1.0\r\nKey: Value\r\n\r\n'
    payload = 'Payload with reply'

    headers_size = str(len(headers.encode()))
    total_size = str(len(headers.encode()) + len(payload.encode()))

    data = b' '.join(part.encode() for part in (subject, sid, reply_to, headers_size, total_size))
    
    stream_data = headers.encode() + payload.encode() + CRLF
    stream = FakeStream(stream_data)
    
    parser = ProtocolParser()
    
    hmsg = await parser.parse_hmsg(data, stream)
    
    assert hmsg.subject == subject
    assert hmsg.sid == sid
    assert hmsg.reply_to == reply_to
    assert hmsg.headers_size == len(headers.encode())
    assert hmsg.total_size == len(headers.encode()) + len(payload.encode())
    assert hmsg.headers == {'Key': 'Value'}
    assert hmsg.payload == payload.encode()


def test_parse_info() -> None:
    info_dict = {
        "server_id": "test-server",
        "server_name": "Test NATS Server",
        "version": "2.1.9",
        "go": "go1.14.1",
        "host": "localhost",
        "port": 4222,
        "headers": True,
        "max_payload": 1048576,
        "proto": 1,
        "client_id": 42,
    }
    data = json_dumps(info_dict)

    parser = ProtocolParser()

    info = parser.parse_info(data)

    assert info.server_id == "test-server"
    assert info.server_name == "Test NATS Server"
    assert info.version == "2.1.9"
    assert info.go == "go1.14.1"
    assert info.host == "localhost"
    assert info.port == 4222
    assert info.headers == True  # noqa: E712
    assert info.max_payload == 1048576
    assert info.proto == 1
    assert info.client_id == 42


def test_parse_and_raise_error_known() -> None:
    data = b"'Unknown Protocol Operation'\r\n"

    parser = ProtocolParser()

    with pytest.raises(UnknownProtocol):
        parser.parse_and_raise_error(data)

def test_parse_and_raise_error_unknown() -> None:
    data = b"'Some Unknown Error'\r\n"

    parser = ProtocolParser()

    with pytest.raises(UnknownProtocol) as exc_info:
        parser.parse_and_raise_error(data)
    assert exc_info.value.extra == 'Some Unknown Error'
