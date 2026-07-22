# Changelog

All notable changes to `natsio-kvcodec` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project is
pre-1.0 and makes no API-stability promises.

## [0.1.0] - 2026-07-22

Initial release. KV key/value codecs over the core `KeyCodec` / `ValueCodec`
seam, mirroring [`orbit.go/kvcodec`](https://github.com/synadia-io/orbit.go/tree/main/kvcodec).
Stdlib only.

### Added

- **Key codecs** (`str -> str`):
  - `Base64KeyCodec` — per-token raw URL-safe base64, dots preserved as subject
    separators; encoded output always satisfies `natsio.kv.validate_key`.
    (orbit `Base64Codec`, key side.)
  - `PathKeyCodec` — `/a/b/c` <-> `a.b.c`, leading `/` as the `_root_` sentinel,
    trailing `/` trimmed. (orbit `PathCodec`.)
  - `NoOpKeyCodec` — identity. (orbit `NoOpCodec`.)
  - `ChainKeyCodec(*codecs)` — sequence composition, encode first->last / decode
    last->first. (orbit `KeyChainCodec`.)
- **Value codecs** (`bytes -> bytes`):
  - `ZlibValueCodec(level=-1)` — transparent stdlib DEFLATE (a natsio addition;
    orbit ships only base64 for values).
  - `Base64ValueCodec` — whole-value raw URL-safe base64. (orbit `Base64Codec`,
    value side.)
  - `NoOpValueCodec` — identity.
  - `ChainValueCodec(*codecs)` — sequence composition. (orbit `ValueChainCodec`.)
- **Filter support**: `FilterableKeyCodec` protocol + `encode_filter()` on the
  base64, path, no-op, and (all-filterable) chain key codecs — encodes a
  wildcard pattern while preserving `*`/`>`. Ported from orbit's
  `FilterableKeyCodec`. Not yet consulted by the core (see below).
- **Typed errors** under `KvCodecError`: `NoCodecsError`, `KeyEncodeError`,
  `KeyDecodeError`, `ValueEncodeError`, `ValueDecodeError`,
  `WildcardNotSupportedError`. Decoding fails loud on corrupt/non-encoded input.
- Exact base64/path/filter **test vectors ported** from orbit's
  `codec_test.go` / `chain_codec_test.go`; property (round-trip) tests via
  Hypothesis; live end-to-end tests (put/get/delete/history/keys/watch) against
  a real `nats-server`.

### Known limitations (core seam friction)

Surfaced while building this extension. All three are in the natsio core, not in
the codecs:

- **Raw-key pre-validation defeats Base64's escape-exotic-characters use case.**
  `KeyValue._encode_key` validates the raw key before the codec runs, so a key
  with a space/`@`/`:` is rejected even though `Base64KeyCodec` would encode it
  to a valid subject. *Proposal:* with a `key_codec` set, validate only the
  encoded key.
- **`keys()`/`iter_keys()`/`purge_deletes()` decode a stripped payload.** These
  use a headers-only (`meta_only`) watch, yet `_entry_from_msg` still runs
  `value_codec.decode(b"")`, so any framing value codec (`ZlibValueCodec`,
  encryption) makes `keys()` raise. Pinned by a strict `xfail`
  (`test_keys_under_framing_value_codec_is_broken`). *Proposal:* skip value
  decoding for headers-only deliveries.
- **No filter hook.** The core refuses wildcard watches under a key codec
  (`ConfigError`) rather than encoding per token. The codecs already implement
  `encode_filter`; a `FilterableKeyCodec`-aware `_maybe_encode_watch_key` would
  unlock per-token wildcard watches with no codec changes.
