"""natsio.kvcodec — KV key/value codecs over the core's codec seam.

Ready-made :class:`natsio.kv.KeyCodec` / :class:`natsio.kv.ValueCodec`
implementations, mirroring `orbit.go/kvcodec
<https://github.com/synadia-io/orbit.go/tree/main/kvcodec>`_, for
:func:`natsio.jetstream.JetStreamContext.create_key_value`'s ``key_codec=`` /
``value_codec=`` arguments::

    from natsio.kv import KeyValueConfig
    from natsio.kvcodec import PathKeyCodec, ZlibValueCodec

    kv = await js.create_key_value(
        KeyValueConfig(bucket="cfg"),
        key_codec=PathKeyCodec(),        # "/app/db" stored as "app.db"
        value_codec=ZlibValueCodec(),    # values compressed at rest
    )
    await kv.put("/app/db/url", b"postgres://...")
    (await kv.get("/app/db/url")).value  # transparently decoded

Codecs are transparent: keys/values are encoded on write and decoded on read,
including the keys returned by ``keys()``/``iter_keys()`` and the entries from
``watch()``/``history()``.

See the package README for the mapping to orbit.go codecs and the "Core
friction" notes (raw-key pre-validation, and wildcard watches under a key
codec).
"""

from .errors import (
    KeyDecodeError,
    KeyEncodeError,
    KvCodecError,
    NoCodecsError,
    ValueDecodeError,
    ValueEncodeError,
    WildcardNotSupportedError,
)
from .key_codecs import (
    Base64KeyCodec,
    ChainKeyCodec,
    FilterableKeyCodec,
    NoOpKeyCodec,
    PathKeyCodec,
)
from .value_codecs import (
    Base64ValueCodec,
    ChainValueCodec,
    NoOpValueCodec,
    ZlibValueCodec,
)

__all__ = [
    "Base64KeyCodec",
    "Base64ValueCodec",
    "ChainKeyCodec",
    "ChainValueCodec",
    "FilterableKeyCodec",
    "KeyDecodeError",
    "KeyEncodeError",
    "KvCodecError",
    "NoCodecsError",
    "NoOpKeyCodec",
    "NoOpValueCodec",
    "PathKeyCodec",
    "ValueDecodeError",
    "ValueEncodeError",
    "WildcardNotSupportedError",
    "ZlibValueCodec",
]
