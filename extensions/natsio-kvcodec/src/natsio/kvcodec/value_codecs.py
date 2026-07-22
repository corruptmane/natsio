"""Value codecs — bidirectional ``bytes -> bytes`` transforms over the core's
:class:`natsio.kv.ValueCodec` seam.

Values are never validated by the core (only keys are), so a value codec may
produce arbitrary bytes.
"""

import base64
import binascii
import zlib

from natsio.kv import ValueCodec

from .errors import NoCodecsError, ValueDecodeError

__all__ = [
    "Base64ValueCodec",
    "ChainValueCodec",
    "NoOpValueCodec",
    "ZlibValueCodec",
]


class NoOpValueCodec:
    """Identity value codec (orbit ``NoOpCodec``, value side).

    ``value_codec=None`` already means identity in the core; use this only as an
    explicit filler in a :class:`ChainValueCodec`.
    """

    def encode(self, value: bytes) -> bytes:
        return value

    def decode(self, value: bytes) -> bytes:
        return value


class Base64ValueCodec:
    """Whole-value raw URL-safe base64 (orbit ``Base64Codec`` value side).

    Rarely useful on its own for a KV value (it only inflates size by ~33% and
    adds no safety, since values are opaque bytes to NATS), but it mirrors
    orbit.go and is handy as a chain link or for producing ASCII-safe payloads.
    """

    def encode(self, value: bytes) -> bytes:
        return base64.urlsafe_b64encode(value).rstrip(b"=")

    def decode(self, value: bytes) -> bytes:
        padded = value + b"=" * (-len(value) % 4)
        try:
            return base64.urlsafe_b64decode(padded)
        except (binascii.Error, ValueError) as exc:
            raise ValueDecodeError(f"invalid base64 value: {exc}") from exc


class ZlibValueCodec:
    """Transparent DEFLATE compression via stdlib :mod:`zlib`.

    Not in orbit.go (which ships only base64 for values) — a natsio judgment
    call, since a value codec is the natural home for compression.

    Tradeoffs, so you use it deliberately:

    * **CPU for bytes.** Every put compresses and every get decompresses. For
      hot, tiny values that overhead can dwarf the savings.
    * **Small values grow.** zlib adds a ~2-byte header + 4-byte checksum, so
      short/incompressible payloads come out *larger*. Compression is a win for
      larger, repetitive values (JSON, text, logs).
    * **Not encryption.** It hides nothing; it only shrinks.
    * **Server-side compression exists.** ``KeyValueConfig(compression=True)``
      compresses the whole stream at rest (S2), transparently and without client
      CPU. Prefer that unless you specifically want the bytes compressed *over
      the wire* and in ``max_bytes`` accounting — which is what this codec buys
      you.

    ``level`` is the standard zlib 0-9 (default ``-1`` = library default ~6).
    A corrupt/non-zlib payload raises :class:`~natsio.kvcodec.ValueDecodeError`.
    """

    __slots__ = ("_level",)

    def __init__(self, level: int = -1) -> None:
        if not (-1 <= level <= 9):
            raise ValueError(f"zlib level must be between -1 and 9, got {level}")
        self._level = level

    def encode(self, value: bytes) -> bytes:
        return zlib.compress(value, self._level)

    def decode(self, value: bytes) -> bytes:
        try:
            return zlib.decompress(value)
        except zlib.error as exc:
            raise ValueDecodeError(f"invalid zlib value: {exc}") from exc


class ChainValueCodec:
    """Apply several value codecs in sequence (orbit ``ValueChainCodec``).

    Encoding runs first-to-last, decoding last-to-first. Construct with at least
    one codec, else :class:`~natsio.kvcodec.NoCodecsError`.

    Order matters: ``ChainValueCodec(ZlibValueCodec(), Base64ValueCodec())``
    compresses *then* base64s (ASCII-safe compressed payload); the reverse would
    base64 first and compress incompressible base64 output — usually pointless.
    """

    __slots__ = ("_codecs",)

    def __init__(self, *codecs: ValueCodec) -> None:
        if not codecs:
            raise NoCodecsError("at least one codec must be provided")
        self._codecs: tuple[ValueCodec, ...] = codecs

    def encode(self, value: bytes) -> bytes:
        for codec in self._codecs:
            value = codec.encode(value)
        return value

    def decode(self, value: bytes) -> bytes:
        for codec in reversed(self._codecs):
            value = codec.decode(value)
        return value
