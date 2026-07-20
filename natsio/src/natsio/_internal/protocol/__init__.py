"""Sans-io NATS protocol layer: bytes in, typed events out. No asyncio, no sockets."""

from .const import DEFAULT_MAX_CONTROL_LINE, DEFAULT_MAX_PAYLOAD
from .errmap import classify_server_error
from .events import (
    NEED_DATA,
    OK_EVENT,
    PING_EVENT,
    PONG_EVENT,
    ErrEvent,
    HMsgEvent,
    InfoEvent,
    MsgEvent,
    NeedData,
    OkEvent,
    ParserOutput,
    PingEvent,
    PongEvent,
    ServerEvent,
)
from .headers import (
    Headers,
    HeadersInput,
    InlineStatus,
    StatusCode,
    encode_header_block,
    parse_header_block,
)
from .parser import Parser
from .wire import (
    PING_FRAME,
    PONG_FRAME,
    build_connect_payload,
    encode_connect,
    encode_hpub,
    encode_pub,
    encode_sub,
    encode_unsub,
)

__all__ = [
    "DEFAULT_MAX_CONTROL_LINE",
    "DEFAULT_MAX_PAYLOAD",
    "NEED_DATA",
    "OK_EVENT",
    "PING_EVENT",
    "PING_FRAME",
    "PONG_EVENT",
    "PONG_FRAME",
    "ErrEvent",
    "HMsgEvent",
    "Headers",
    "HeadersInput",
    "InfoEvent",
    "InlineStatus",
    "MsgEvent",
    "NeedData",
    "OkEvent",
    "Parser",
    "ParserOutput",
    "PingEvent",
    "PongEvent",
    "ServerEvent",
    "StatusCode",
    "build_connect_payload",
    "classify_server_error",
    "encode_connect",
    "encode_header_block",
    "encode_hpub",
    "encode_pub",
    "encode_sub",
    "encode_unsub",
    "parse_header_block",
]
