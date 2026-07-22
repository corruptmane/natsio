"""Key codecs — bidirectional ``str -> str`` transforms over the core's
:class:`natsio.kv.KeyCodec` seam.

Every codec here guarantees that the *encoded* form of any NATS-valid key is
itself a NATS-valid key (it satisfies :func:`natsio.kv.validate_key`), so the
core accepts it as a subject token. See the extension README for the one place
this is not enough — the core validates the *raw* key too, which blocks
Base64's escape-exotic-characters use case.
"""

from __future__ import annotations

import base64
import binascii
from typing import cast

from natsio.kv import FilterableKeyCodec as CoreFilterableKeyCodec
from natsio.kv import KeyCodec

from .errors import (
    KeyDecodeError,
    NoCodecsError,
    WildcardNotSupportedError,
)

__all__ = [
    "Base64KeyCodec",
    "ChainKeyCodec",
    "FilterableKeyCodec",
    "NoOpKeyCodec",
    "PathKeyCodec",
]

# Subject wildcards. A whole-token ``*`` matches one token; a final ``>``
# matches the rest. A filter is a key that may contain these; encode_filter
# must pass them through untouched.
_WILDCARDS = frozenset({"*", ">"})


class FilterableKeyCodec(CoreFilterableKeyCodec):
    """Marker/base for key codecs that can encode a *filter* (a key that may
    contain ``*``/``>`` wildcards) while preserving the wildcards.

    Explicitly implements the core's runtime-checkable
    :class:`natsio.kv.FilterableKeyCodec` protocol, so the core's ``watch()``
    recognises these codecs (via ``isinstance``) and encodes wildcard filters
    per token instead of refusing them. Mirrors orbit.go's optional
    ``FilterableKeyCodec`` interface. A codec is "filterable" iff it provides
    ``encode_filter``; :class:`ChainKeyCodec` is filterable only when *every*
    member is.

    Subclassing is not required — the core protocol is structural — but
    subclassing documents intent and gives you the :meth:`is_filterable`
    helper.
    """

    @staticmethod
    def is_filterable(codec: object) -> bool:
        return isinstance(codec, CoreFilterableKeyCodec)

    def encode_filter(self, key_filter: str) -> str:
        # Overridden by every concrete filterable codec here; the default keeps
        # the type visible to callers that ``cast`` a checked member.
        raise NotImplementedError


class NoOpKeyCodec(FilterableKeyCodec):
    """Identity key codec (orbit ``NoOpCodec``).

    The core already treats ``key_codec=None`` as identity, so this is only
    useful as an explicit filler inside a :class:`ChainKeyCodec` or when a
    non-optional codec argument is wanted.
    """

    def encode(self, key: str) -> str:
        return key

    def decode(self, key: str) -> str:
        return key

    def encode_filter(self, key_filter: str) -> str:
        return key_filter


def _b64_encode_token(token: str) -> str:
    # Raw URL-safe base64 (no ``=`` padding), matching orbit.go's
    # base64.RawURLEncoding. The alphabet ``A-Za-z0-9-_`` is a subset of what a
    # NATS key token allows, so the result is always a valid token.
    return base64.urlsafe_b64encode(token.encode()).rstrip(b"=").decode("ascii")


def _b64_decode_token(token: str) -> str:
    padded = token + "=" * (-len(token) % 4)
    try:
        return base64.urlsafe_b64decode(padded).decode("utf-8")
    except (binascii.Error, ValueError) as exc:
        raise KeyDecodeError(f"invalid base64 key token {token!r}: {exc}") from exc


class Base64KeyCodec(FilterableKeyCodec):
    """Per-token URL-safe base64 (orbit ``Base64Codec`` key side).

    The key is split on ``.`` and each token is base64-encoded independently, so
    dots keep their meaning as subject-token separators and wildcard filters
    stay per-token (:meth:`encode_filter`). Because the raw URL-safe alphabet is
    a subset of the NATS key alphabet, encoding a NATS-valid key yields a
    NATS-valid key.

    Its *purpose* — per orbit — is to let keys carry characters that are not
    valid in NATS subjects (spaces, ``@``, ``:`` ...). The codec does that
    correctly, BUT the natsio core validates the *raw* key before any codec
    runs, so such keys are rejected before this codec sees them. See the
    README's "Core friction" section.
    """

    def encode(self, key: str) -> str:
        return ".".join(_b64_encode_token(t) for t in key.split("."))

    def decode(self, key: str) -> str:
        return ".".join(_b64_decode_token(t) for t in key.split("."))

    def encode_filter(self, key_filter: str) -> str:
        return ".".join(t if t in _WILDCARDS else _b64_encode_token(t) for t in key_filter.split("."))


# Path keys that start with ``/`` are stored with this sentinel first token,
# because a NATS subject cannot start with ``.`` (which is what a bare leading
# ``/`` would otherwise become). Matches orbit.go's ``_root_``.
_ROOT_PREFIX = "_root_"


class PathKeyCodec(FilterableKeyCodec):
    """Filesystem-style keys ``/a/b/c`` <-> NATS ``a.b.c`` (orbit ``PathCodec``).

    Rules (mirrored from orbit.go, verified against its test vectors):

    * a leading ``/`` becomes the ``_root_`` sentinel token (``/`` alone ->
      ``_root_``); this keeps the leading-slash distinction round-trippable
      without producing a subject that starts with ``.``;
    * a trailing ``/`` is trimmed (subjects cannot end with ``.``);
    * remaining ``/`` become ``.``.

    Key-only: values pass through untouched, so pair it with a value codec (or
    none) via :func:`natsio.jetstream.JetStreamContext.create_key_value`'s
    separate ``value_codec=``.

    Caveat (shared with orbit.go): the codec maps ``/`` <-> ``.`` and cannot
    tell a *literal* ``.`` in the input from a separator, so feed it path-style
    keys (separators are ``/``). A dotted input like ``a.b`` decodes back as
    ``a/b`` — it does not round-trip.

    Edge case worth knowing: an *empty* path segment (``/a//b``) encodes to a
    key with an empty token (``_root_.a..b``), which is NOT a valid NATS key —
    the core's post-encode ``validate_key`` rejects it with ``InvalidKeyError``.
    orbit.go has the same latent output; natsio just catches it at the seam.
    """

    def encode(self, key: str) -> str:
        if key.startswith("/"):
            if key == "/":
                return _ROOT_PREFIX
            key = _ROOT_PREFIX + "." + key[1:]
        key = key.removesuffix("/")
        return key.replace("/", ".")

    def decode(self, key: str) -> str:
        if key == _ROOT_PREFIX:
            return "/"
        prefix = _ROOT_PREFIX + "."
        if key.startswith(prefix):
            return "/" + key[len(prefix) :].replace(".", "/")
        return key.replace(".", "/")

    def encode_filter(self, key_filter: str) -> str:
        # PathCodec's encode already leaves ``*``/``>`` alone (they contain no
        # ``/``), so filter-encoding is just encode. Matches orbit.go.
        return self.encode(key_filter)


class ChainKeyCodec(FilterableKeyCodec):
    """Apply several key codecs in sequence (orbit ``KeyChainCodec``).

    Encoding runs the codecs first-to-last; decoding runs them last-to-first, so
    ``decode(encode(k)) == k``. Construct with at least one codec, else
    :class:`~natsio.kvcodec.NoCodecsError`.

    Example — path notation *then* base64-escape the segments::

        ChainKeyCodec(PathKeyCodec(), Base64KeyCodec())

    :meth:`encode_filter` is available only when *every* member is filterable;
    otherwise it raises :class:`~natsio.kvcodec.WildcardNotSupportedError`
    naming the offending position (a chain cannot promise wildcard preservation
    if any link would mangle a ``*``/``>``).
    """

    __slots__ = ("_codecs",)

    def __init__(self, *codecs: KeyCodec) -> None:
        if not codecs:
            raise NoCodecsError("at least one codec must be provided")
        self._codecs: tuple[KeyCodec, ...] = codecs

    def encode(self, key: str) -> str:
        for codec in self._codecs:
            key = codec.encode(key)
        return key

    def decode(self, key: str) -> str:
        for codec in reversed(self._codecs):
            key = codec.decode(key)
        return key

    def encode_filter(self, key_filter: str) -> str:
        for index, codec in enumerate(self._codecs):
            if not FilterableKeyCodec.is_filterable(codec):
                raise WildcardNotSupportedError(
                    f"codec {index} ({type(codec).__name__}) does not support wildcard filtering"
                )
        for codec in self._codecs:
            key_filter = cast(FilterableKeyCodec, codec).encode_filter(key_filter)
        return key_filter
