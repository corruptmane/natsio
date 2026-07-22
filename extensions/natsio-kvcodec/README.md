# natsio-kvcodec

Transparent **key/value codecs** for natsio's KV store, over the core's
`KeyCodec` / `ValueCodec` seam. Encode keys and values on the way in, decode
them on the way out — `get()`, `keys()`, `watch()`, and `history()` all speak the
*decoded* form while the bucket stores the *encoded* form.

Mirrors [`orbit.go/kvcodec`](https://github.com/synadia-io/orbit.go/tree/main/kvcodec),
pythonized. Stdlib only; no runtime dependencies beyond `natsio`.

```bash
pip install natsio-kvcodec
```

## Usage

Pass codecs to any `create_key_value` / `key_value` variant — the natsio core
already accepts `key_codec=` and `value_codec=`:

```python
from natsio.kv import KeyValueConfig
from natsio.kvcodec import PathKeyCodec, ZlibValueCodec

kv = await js.create_key_value(
    KeyValueConfig(bucket="cfg"),
    key_codec=PathKeyCodec(),      # "/app/db/url" stored as "app.db.url"
    value_codec=ZlibValueCodec(),  # values compressed at rest & over the wire
)

await kv.put("/app/db/url", b"postgres://localhost")
entry = await kv.get("/app/db/url")   # entry.key == "/app/db/url"
await kv.keys()                        # ["/app/db/url"] — DECODED keys
```

Key and value codecs are independent; use either, both, or neither.

## Codecs

### Key codecs (`str -> str`)

| Codec | What it does | orbit.go parity |
|---|---|---|
| `Base64KeyCodec` | Per-token raw URL-safe base64 (dots kept as separators). Lets a key carry characters illegal in NATS subjects — modulo the core-friction caveat below. | `Base64Codec` (key side) |
| `PathKeyCodec` | Filesystem keys `/a/b/c` <-> NATS `a.b.c`. Leading `/` becomes the `_root_` sentinel, trailing `/` trimmed. | `PathCodec` |
| `NoOpKeyCodec` | Identity (the core's `key_codec=None` already means identity; useful as a chain filler). | `NoOpCodec` |
| `ChainKeyCodec(*codecs)` | Apply key codecs in sequence (encode first->last, decode last->first). | `KeyChainCodec` |

Every key codec guarantees its **encoded output satisfies
`natsio.kv.validate_key`** (the raw URL-safe base64 alphabet and `_root_`
sentinel are all NATS-legal), so the core accepts the encoded key as a subject.

`Base64KeyCodec`, `PathKeyCodec`, `NoOpKeyCodec`, and an all-filterable
`ChainKeyCodec` also implement `encode_filter(pattern)` (the core's
runtime-checkable `natsio.kv.FilterableKeyCodec` protocol), which encodes a
wildcard pattern while preserving `*`/`>`. **The natsio core calls this** from
`watch()`, so a wildcard watch under one of these codecs is encoded per token
(`orders.>` -> `b3JkZXJz.>`) and works end-to-end — see Core Friction.

### Value codecs (`bytes -> bytes`)

| Codec | What it does | orbit.go parity |
|---|---|---|
| `ZlibValueCodec(level=-1)` | Transparent DEFLATE via stdlib `zlib`. | *natsio addition* |
| `Base64ValueCodec` | Whole-value raw URL-safe base64. | `Base64Codec` (value side) |
| `NoOpValueCodec` | Identity. | `NoOpCodec` |
| `ChainValueCodec(*codecs)` | Apply value codecs in sequence. | `ValueChainCodec` |

**`ZlibValueCodec` tradeoffs.** It trades CPU for bytes, and *small or
incompressible values come out larger* (a ~6-byte zlib envelope). It is not
encryption. If you only want at-rest compression, prefer
`KeyValueConfig(compression=True)` (server-side S2, zero client CPU); reach for
this codec when you want the bytes compressed **over the wire** and counted in
`max_bytes`.

## Errors

Codecs fail loud (like orbit.go's `error` returns), never silently pass corrupt
data through:

- `NoCodecsError` — empty `ChainKeyCodec()` / `ChainValueCodec()`.
- `KeyDecodeError` / `ValueDecodeError` — corrupt/non-encoded input (a non-base64
  token, a bad zlib stream).
- `WildcardNotSupportedError` — `encode_filter` on a chain with a non-filterable
  member.

All derive from `KvCodecError`.

## orbit.go parity notes

- **Two classes per base64, not one.** Go's structural typing lets a single
  `Base64Codec` satisfy both `KeyCodec` and `ValueCodec`. natsio's protocols
  have different `encode` signatures (`str->str` vs `bytes->bytes`), so this
  ships `Base64KeyCodec` and `Base64ValueCodec`.
- **Naming.** orbit uses `EncodeKey`/`DecodeKey`; the natsio seam uses
  `encode`/`decode`, so codecs follow the natsio spelling.
- **`ZlibValueCodec` is new.** orbit ships only base64 for values; a value codec
  is the natural home for compression, so natsio adds one.
- Exact base64/path/filter **test vectors are ported** from orbit's
  `codec_test.go` / `chain_codec_test.go`, so behavior matches byte-for-byte
  (`Base64KeyCodec().encode("test.key...") == "dGVzdA.a2V5..."`, etc.).
- **`PathKeyCodec` dot caveat (shared with orbit).** It maps `/`<->`.` and cannot
  distinguish a literal `.` in the input from a separator, so feed it
  path-style keys; `a.b` decodes back as `a/b`.

## Core friction

Building this extension stress-tested the codec seam. Two real gaps surfaced
(neither is a codec bug — both are in the core, documented here so the behavior
isn't surprising):

1. **The raw key is validated *before* the codec runs, defeating Base64's main
   use case.** `KeyValue._encode_key` calls `validate_key(raw_key)` and *then*
   `validate_key(codec.encode(raw_key))`. So `put("Acme Inc.contact", ...)` with
   `Base64KeyCodec` raises `InvalidKeyError` on the space — even though the codec
   would turn it into the perfectly valid `QWNtZSBJbmM.Y29udGFjdA`. The escape-
   exotic-characters scenario from orbit's own README cannot work end-to-end.
   *Proposal:* when a `key_codec` is set, validate only the **encoded** key; the
   raw key is the user's domain and only the stored subject must be NATS-legal.

2. **`keys()` / `iter_keys()` (and `purge_deletes()`) still run the value codec
   on a payload the server deliberately stripped.** Those paths use a
   headers-only (`meta_only`) watch, so every delivery has an empty payload —
   but `_entry_from_msg` unconditionally calls `value_codec.decode(payload)` for
   `PUT` entries. Any value codec that can't decode `b""` (`ZlibValueCodec`,
   encryption, length-prefixed framing) makes `keys()` raise `ValueDecodeError`.
   *Proposal:* skip value decoding for headers-only deliveries (or when the
   payload is empty under `meta_only`). Pinned by a strict `xfail` in
   `test_kvcodec_live.py::test_keys_under_framing_value_codec_is_broken`.

A third one — **the filter hook — is now closed.** orbit's `FilterableKeyCodec`
lets a wildcard watch encode tokens individually (`orders.>` -> `b3JkZXJz.>`).
The natsio core mirrors this: `watch()` recognises the runtime-checkable
`natsio.kv.FilterableKeyCodec` protocol (which the codecs here implement) and
calls `encode_filter()`, so per-token wildcard watches work end-to-end. Under a
codec the *raw* filter is the caller's domain (it may be in the codec's own
notation, e.g. `PathKeyCodec`'s `/a/*`) — only the encoded filter must be a
legal subject filter. A wildcard watch under a *non*-filterable codec is still
refused (`ConfigError`): encoding the whole key would mangle the `*`/`>`.
