"""Typed errors for kvcodec.

Codecs fail *loud*: a corrupt or non-encoded input raises rather than silently
passing through. This mirrors orbit.go/kvcodec (which returns an ``error`` from
every ``Decode*``) and keeps a bucket that somehow accumulated foreign/garbage
data from being misread as if it round-tripped cleanly.
"""

from __future__ import annotations

__all__ = [
    "KeyDecodeError",
    "KeyEncodeError",
    "KvCodecError",
    "NoCodecsError",
    "ValueDecodeError",
    "ValueEncodeError",
    "WildcardNotSupportedError",
]


class KvCodecError(Exception):
    """Base class for every kvcodec failure."""


class NoCodecsError(KvCodecError, ValueError):
    """A chain codec was constructed with zero codecs (orbit ``ErrNoCodecs``)."""


class KeyEncodeError(KvCodecError):
    """A key codec could not encode a key (orbit ``ErrKeyEncodeFailed``)."""


class KeyDecodeError(KvCodecError):
    """A key codec could not decode a stored key (orbit ``ErrKeyDecodeFailed``).

    Raised on corrupt/non-encoded key tokens — e.g. a base64 token that is not
    valid base64.
    """


class ValueEncodeError(KvCodecError):
    """A value codec could not encode a value (orbit ``ErrValueEncodeFailed``)."""


class ValueDecodeError(KvCodecError):
    """A value codec could not decode a stored value (orbit ``ErrValueDecodeFailed``).

    Raised on corrupt/non-encoded payloads — e.g. a zlib stream with a bad
    checksum, or non-base64 bytes.
    """


class WildcardNotSupportedError(KvCodecError):
    """A filter with ``*``/``>`` was handed to a codec that cannot preserve them
    (orbit ``ErrWildcardNotSupported``).

    NOTE: the natsio core does not yet call ``encode_filter`` — it refuses
    wildcard watches under any key codec outright (see the extension README's
    "Core friction" section). This error exists for codecs that *do* implement
    :class:`~natsio.kvcodec.FilterableKeyCodec`, and for the day the seam grows
    a filter hook.
    """
