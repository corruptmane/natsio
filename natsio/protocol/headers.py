from enum import Enum


class Header(str, Enum):
    MSG_ID = "Nats-Msg-Id"
    EXPECTED_STREAM = "Nats-Expected-Stream"
    EXPECTED_LAST_MSG_ID = "Nats-Expected-Last-Msg-Id"
    EXPECTED_LAST_SEQUENCE = "Nats-Expected-Last-Sequence"
    EXPECTED_LAST_SUBJECT_SEQUENCE = "Nats-Expected-Last-Subject-Sequence"
    ROLLUP = "Nats-Rollup"
    STREAM = "Nats-Stream"
    SUBJECT = "Nats-Subject"
    SEQUENCE = "Nats-Sequence"
    LAST_SEQUENCE = "Nats-Last-Sequence"
    TIMESTAMP = "Nats-Time-Stamp"
    STREAM_SOURCE = "Nats-Stream-Source"
    MSG_SIZE = "Nats-Msg-Size"
    LAST_CONSUMER = "Nats-Last-Consumer"
    LAST_STREAM = "Nats-Last-Stream"
    CONSUMER_STALLED = "Nats-Consumer-Stalled"
    STATUS = "Status"
    DESCRIPTION = "Description"
    KV_OPERATION = "KV-Operation"


class StatusCode(str, Enum):
    SERVICE_UNAVAILABLE = "503"
    NO_MESSAGES = "404"
    REQUEST_TIMEOUT = "408"
    CONFLICT = "409"
    CONTROL_MESSAGE = "100"


class KVOperation(str, Enum):
    PUT = "PUT"
    DEL = "DEL"
    PURGE = "PURGE"


class Rollup(str, Enum):
    SUB = "sub"
    ALL = "all"
