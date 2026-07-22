"""NATS message headers: parsing and encoding.

The wire format is an HTTP/1.1-like block:

    NATS/1.0[ <code>[ <description>]]\r\n
    Key: Value\r\n
    Key: Another\r\n
    \r\n

Repeated keys are preserved (multi-value). Lookup is exact-match and
case-preserving; the canonical ``Nats-*`` spellings are provided as constants
in `natsio._internal.protocol.wire` users should prefer.
"""

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from enum import IntEnum

from natsio.errors import BadHeadersError

from .const import CRLF, HEADER_VERSION

__all__ = [
    "Headers",
    "HeadersInput",
    "InlineStatus",
    "StatusCode",
    "encode_header_block",
    "parse_header_block",
]


class StatusCode(IntEnum):
    """Well-known inline status codes carried on header-only messages."""

    CONTROL = 100  # idle heartbeat / flow control
    OK = 200
    NOT_FOUND = 404  # no messages (pull)
    TIMEOUT = 408  # pull request timed out / interest expired
    CONFLICT = 409  # pull terminated (consumer deleted, max-bytes, leadership change...)
    PIN_MISMATCH = 423  # priority-group pin lost
    NO_RESPONDERS = 503


@dataclass(frozen=True, slots=True)
class InlineStatus:
    """Status line of a header-only message: numeric code plus full description.

    Both parts are load-bearing: JetStream distinguishes e.g. ``409 Consumer
    Deleted`` from ``409 Leadership Change`` only by the description.
    """

    code: int
    description: str


class Headers(Mapping[str, str]):
    """Multi-value, case-preserving header map with exact-match lookup.

    ``headers[key]`` / ``headers.get(key)`` return the *first* value;
    ``get_all(key)`` returns every value for the key in arrival order.
    """

    __slots__ = ("_data",)

    # Mutable (add/set/discard) — intentionally unhashable. Explicit so it
    # reads as a decision, not an oversight (Python sets this implicitly once
    # __eq__ is defined).
    __hash__ = None

    def __init__(self, initial: "HeadersInput | None" = None) -> None:
        self._data: dict[str, list[str]] = {}
        if isinstance(initial, Headers):
            # Mapping.items() would collapse repeats via first-value __getitem__.
            for key, value in initial.allitems():
                self.add(key, value)
        elif initial is not None:
            for key, value in initial.items():
                if isinstance(value, str):
                    self.add(key, value)
                else:
                    for item in value:
                        self.add(key, item)

    def __getitem__(self, key: str) -> str:
        return self._data[key][0]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        pairs = ", ".join(f"{k!r}: {v!r}" for k, v in self.allitems())
        return f"Headers({{{pairs}}})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Headers):
            return self._data == other._data
        if isinstance(other, Mapping):
            return self._data == {k: [v] for k, v in other.items()}
        return NotImplemented

    def get_all(self, key: str) -> list[str]:
        """Every value recorded for ``key``, in arrival order (empty if absent)."""
        return list(self._data.get(key, ()))

    def add(self, key: str, value: str) -> None:
        """Append a value, preserving existing values for the key."""
        self._data.setdefault(key, []).append(value)

    def set(self, key: str, value: str) -> None:
        """Replace all values for the key with a single value."""
        self._data[key] = [value]

    def discard(self, key: str) -> None:
        """Remove the key if present."""
        self._data.pop(key, None)

    def allitems(self) -> Iterator[tuple[str, str]]:
        """Iterate ``(key, value)`` for every value, including repeats."""
        for key, values in self._data.items():
            for value in values:
                yield key, value


type HeadersInput = Headers | Mapping[str, str | Sequence[str]]


def _is_valid_key(key: str) -> bool:
    # Printable ASCII 33..126 inclusive, excluding ':' (which delimits the value).
    return bool(key) and all(33 <= ord(ch) <= 126 and ch != ":" for ch in key)


def parse_header_block(block: bytes) -> tuple[Headers | None, InlineStatus | None]:
    """Parse a complete, length-delimited header block.

    Raises `BadHeadersError` only when the envelope itself is wrong
    (missing ``NATS/1.0`` version line or missing terminating CRLF CRLF) —
    the caller knows the block's exact length, so this is never a framing
    hazard. Individually malformed header *lines* are skipped: the block is
    server-forwarded application data and must not kill the message.
    """
    if not block.startswith(HEADER_VERSION):
        raise BadHeadersError(f"invalid header version line: {block[:16]!r}")
    if not block.endswith(CRLF + CRLF):
        raise BadHeadersError("header block does not end with CRLF CRLF")

    lines = block[:-4].split(CRLF)
    status = _parse_status_line(lines[0])

    headers = Headers()
    for raw in lines[1:]:
        if not raw:
            continue
        sep = raw.find(b":")
        if sep < 1:
            continue  # no key/value separator (or empty key): skip
        try:
            key = raw[:sep].decode("ascii").strip()
            value = raw[sep + 1 :].decode("utf-8").strip(" \t")
        except UnicodeDecodeError:
            continue
        if not _is_valid_key(key):
            continue
        headers.add(key, value)

    return (headers if len(headers) else None, status)


def _parse_status_line(line: bytes) -> InlineStatus | None:
    rest = line[len(HEADER_VERSION) :].strip()
    if not rest:
        return None
    code_part, _, description = rest.partition(b" ")
    if len(code_part) != 3 or not code_part.isdigit():
        return None
    return InlineStatus(int(code_part), description.decode("utf-8", "replace").strip())


def encode_header_block(headers: HeadersInput) -> bytes:
    """Encode headers for HPUB, validating against wire injection.

    Raises `BadHeadersError` for keys/values that could break framing:
    CR or LF anywhere, ``:`` or non-printable-ASCII in keys.
    """
    out = [HEADER_VERSION, CRLF]
    items = headers.allitems() if isinstance(headers, Headers) else _mapping_items(headers)
    for key, value in items:
        if not _is_valid_key(key):
            raise BadHeadersError(f"invalid header key: {key!r}")
        if "\r" in value or "\n" in value:
            raise BadHeadersError(f"header value for {key!r} contains CR/LF")
        out += (key.encode("ascii"), b": ", value.encode("utf-8"), CRLF)
    out.append(CRLF)
    return b"".join(out)


def _mapping_items(headers: Mapping[str, str | Sequence[str]]) -> Iterator[tuple[str, str]]:
    for key, value in headers.items():
        if isinstance(value, str):
            yield key, value
        else:
            for item in value:
                yield key, item
