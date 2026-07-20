"""Client→server wire-frame builders.

Builders are dumb and fast: framing only, single-join assembly, no I/O.
Subject/queue validation happens at the public API boundary; header safety is
enforced by :func:`natsio._internal.protocol.headers.encode_header_block`,
which is the only sanctioned way to produce ``header_block`` for
:func:`encode_hpub`.
"""

from typing import Any, Final

from .const import CRLF

__all__ = [
    "PING_FRAME",
    "PONG_FRAME",
    "build_connect_payload",
    "encode_connect",
    "encode_hpub",
    "encode_pub",
    "encode_sub",
    "encode_unsub",
]

PING_FRAME: Final = b"PING\r\n"
PONG_FRAME: Final = b"PONG\r\n"

LANG: Final = "natsio"


def encode_pub(subject: str, reply_to: str | None, payload: bytes) -> bytes:
    if reply_to is None:
        head = b"PUB %b %d\r\n" % (subject.encode("ascii"), len(payload))
    else:
        head = b"PUB %b %b %d\r\n" % (subject.encode("ascii"), reply_to.encode("ascii"), len(payload))
    return b"".join((head, payload, CRLF))


def encode_hpub(subject: str, reply_to: str | None, header_block: bytes, payload: bytes) -> bytes:
    header_size = len(header_block)
    total_size = header_size + len(payload)
    if reply_to is None:
        head = b"HPUB %b %d %d\r\n" % (subject.encode("ascii"), header_size, total_size)
    else:
        head = b"HPUB %b %b %d %d\r\n" % (
            subject.encode("ascii"),
            reply_to.encode("ascii"),
            header_size,
            total_size,
        )
    return b"".join((head, header_block, payload, CRLF))


def encode_sub(subject: str, sid: int, queue: str | None = None) -> bytes:
    if queue is None:
        return b"SUB %b %d\r\n" % (subject.encode("ascii"), sid)
    return b"SUB %b %b %d\r\n" % (subject.encode("ascii"), queue.encode("ascii"), sid)


def encode_unsub(sid: int, max_msgs: int | None = None) -> bytes:
    if max_msgs is None:
        return b"UNSUB %d\r\n" % sid
    return b"UNSUB %d %d\r\n" % (sid, max_msgs)


def encode_connect(json_payload: bytes) -> bytes:
    return b"".join((b"CONNECT ", json_payload, CRLF))


def build_connect_payload(
    *,
    version: str,
    verbose: bool = False,
    pedantic: bool = False,
    tls_required: bool = False,
    echo: bool = True,
    name: str | None = None,
    user: str | None = None,
    password: str | None = None,
    auth_token: str | None = None,
    jwt: str | None = None,
    nkey: str | None = None,
    signature: str | None = None,
) -> dict[str, Any]:
    """CONNECT options dict, ready for JSON serialization.

    ``protocol=1`` (async INFO), ``headers=true`` and ``no_responders=true``
    are hard-set: a 2.14-floor client always speaks them, and without
    ``headers`` the server would never emit HMSG at all.
    """
    payload: dict[str, Any] = {
        "verbose": verbose,
        "pedantic": pedantic,
        "tls_required": tls_required,
        "lang": LANG,
        "version": version,
        "protocol": 1,
        "echo": echo,
        "headers": True,
        "no_responders": True,
    }
    if name is not None:
        payload["name"] = name
    if user is not None:
        payload["user"] = user
    if password is not None:
        payload["pass"] = password
    if auth_token is not None:
        payload["auth_token"] = auth_token
    if jwt is not None:
        payload["jwt"] = jwt
    if nkey is not None:
        payload["nkey"] = nkey
    if signature is not None:
        payload["sig"] = signature
    return payload
