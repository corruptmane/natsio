import json

from natsio._internal.protocol import (
    PING_FRAME,
    PONG_FRAME,
    build_connect_payload,
    encode_connect,
    encode_header_block,
    encode_hpub,
    encode_pub,
    encode_sub,
    encode_unsub,
)


def test_ping_pong_frames() -> None:
    assert PING_FRAME == b"PING\r\n"
    assert PONG_FRAME == b"PONG\r\n"


def test_pub_without_reply() -> None:
    assert encode_pub("foo.bar", None, b"hello") == b"PUB foo.bar 5\r\nhello\r\n"


def test_pub_with_reply() -> None:
    assert encode_pub("foo", "_INBOX.x.1", b"hi") == b"PUB foo _INBOX.x.1 2\r\nhi\r\n"


def test_pub_empty_payload() -> None:
    assert encode_pub("foo", None, b"") == b"PUB foo 0\r\n\r\n"


def test_hpub_sizes_are_header_and_total() -> None:
    block = encode_header_block({"A": "1"})  # NATS/1.0\r\nA: 1\r\n\r\n -> 18 bytes
    frame = encode_hpub("subj", None, block, b"body")
    assert frame == b"HPUB subj 18 22\r\nNATS/1.0\r\nA: 1\r\n\r\nbody\r\n"


def test_hpub_with_reply_and_empty_payload() -> None:
    block = encode_header_block({"K": "v"})
    frame = encode_hpub("s", "r", block, b"")
    head, _, rest = frame.partition(b"\r\n")
    assert head == b"HPUB s r %d %d" % (len(block), len(block))
    assert rest == block + b"\r\n"


def test_sub_without_queue() -> None:
    assert encode_sub("orders.>", 42) == b"SUB orders.> 42\r\n"


def test_sub_with_queue() -> None:
    assert encode_sub("orders.*", 7, queue="workers") == b"SUB orders.* workers 7\r\n"


def test_unsub_without_limit() -> None:
    assert encode_unsub(42) == b"UNSUB 42\r\n"


def test_unsub_with_max_msgs() -> None:
    assert encode_unsub(42, 5) == b"UNSUB 42 5\r\n"


def test_connect_frame_wraps_json() -> None:
    assert encode_connect(b'{"verbose":false}') == b'CONNECT {"verbose":false}\r\n'


def test_connect_payload_hard_sets_modern_flags() -> None:
    payload = build_connect_payload(version="0.1.0")
    assert payload["protocol"] == 1
    assert payload["headers"] is True
    assert payload["no_responders"] is True
    assert payload["lang"] == "natsio"
    assert payload["version"] == "0.1.0"
    assert payload["verbose"] is False
    assert payload["pedantic"] is False
    assert payload["echo"] is True
    # Optional auth fields are omitted entirely when unset.
    assert not ({"user", "pass", "auth_token", "jwt", "nkey", "sig", "name"} & payload.keys())


def test_connect_payload_auth_fields() -> None:
    payload = build_connect_payload(
        version="0.1.0",
        name="svc",
        user="u",
        password="p",
        jwt="JWT",
        nkey="UABC",
        signature="SIG",
    )
    assert payload["name"] == "svc"
    assert payload["user"] == "u"
    assert payload["pass"] == "p"
    assert payload["jwt"] == "JWT"
    assert payload["nkey"] == "UABC"
    assert payload["sig"] == "SIG"


def test_connect_exact_bytes_with_stdlib_json() -> None:
    payload = build_connect_payload(version="0.1.0")
    frame = encode_connect(json.dumps(payload, separators=(",", ":")).encode())
    assert frame == (
        b'CONNECT {"verbose":false,"pedantic":false,"tls_required":false,'
        b'"lang":"natsio","version":"0.1.0","protocol":1,"echo":true,'
        b'"headers":true,"no_responders":true}\r\n'
    )


def test_connect_payload_distinct_boolean_values() -> None:
    payload = build_connect_payload(version="v", verbose=True, pedantic=False, tls_required=True, echo=False)
    assert payload["verbose"] is True
    assert payload["pedantic"] is False
    assert payload["tls_required"] is True
    assert payload["echo"] is False


def test_connect_payload_auth_token_positive_mapping() -> None:
    payload = build_connect_payload(version="v", auth_token="tok-1")
    assert payload["auth_token"] == "tok-1"
