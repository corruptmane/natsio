"""Unit tests for the kvcodec codecs.

Round-trip properties (hypothesis), the exact orbit.go test vectors, the
validate_key guarantee for key codecs, and predictable typed errors on corrupt
input. No server needed.
"""

import base64
import zlib

import pytest
from hypothesis import given
from hypothesis import strategies as st
from natsio.kvcodec import (  # ty: ignore[unresolved-import]
    Base64KeyCodec,
    Base64ValueCodec,
    ChainKeyCodec,
    ChainValueCodec,
    KeyDecodeError,
    NoCodecsError,
    NoOpKeyCodec,
    NoOpValueCodec,
    PathKeyCodec,
    ValueDecodeError,
    WildcardNotSupportedError,
    ZlibValueCodec,
)

from natsio.kv import FilterableKeyCodec, InvalidKeyError, validate_key

# A strategy for strings that are already valid NATS key *tokens* (no dots): the
# base64 codec is a superset-safe transform, but the natsio core pre-validates
# the raw key, so live-relevant keys must themselves be valid. For pure codec
# round-trips we can be far more adversarial (see `any_text`).
# No "/" here: it is a valid NATS-key character, but PathKeyCodec reinterprets
# "/" as a path separator, so a "/"-bearing token is not a valid *path* input.
# Keeping the shared strategy dot-and-path-safe lets it drive every key codec.
_TOKEN_ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_="
valid_tokens = st.text(alphabet=_TOKEN_ALPHABET, min_size=1, max_size=12)
valid_keys = st.lists(valid_tokens, min_size=1, max_size=5).map(".".join)

# Arbitrary text (including characters illegal in NATS subjects) for pure codec
# round-trips. Tokens are non-empty so re-joining on "." is unambiguous.
any_token = st.text(min_size=1, max_size=20).filter(lambda t: "." not in t)
any_text_keys = st.lists(any_token, min_size=1, max_size=5).map(".".join)


class TestBase64KeyCodec:
    def test_orbit_vector(self) -> None:
        codec = Base64KeyCodec()
        key = "test.key.with.special.chars!@#$%^&*()"
        expected = "dGVzdA.a2V5.d2l0aA.c3BlY2lhbA.Y2hhcnMhQCMkJV4mKigp"
        assert codec.encode(key) == expected
        assert codec.decode(expected) == key

    def test_escapes_exotic_characters_into_valid_key(self) -> None:
        # The whole point of Base64: a key with characters illegal in a NATS
        # subject encodes to a *valid* key. (The core's raw-key pre-validation
        # blocks this end-to-end — see the README's Core Friction section — but
        # the codec itself fulfils its contract.)
        codec = Base64KeyCodec()
        encoded = codec.encode("Acme Inc.contact")
        validate_key(encoded)  # must not raise
        assert codec.decode(encoded) == "Acme Inc.contact"

    @given(any_text_keys)
    def test_roundtrip_any_text(self, key: str) -> None:
        codec = Base64KeyCodec()
        assert codec.decode(codec.encode(key)) == key

    @given(valid_keys)
    def test_encoded_key_always_validates(self, key: str) -> None:
        encoded = Base64KeyCodec().encode(key)
        validate_key(encoded)  # must not raise

    def test_decode_corrupt_token_raises(self) -> None:
        # "!" is outside the base64 alphabet.
        with pytest.raises(KeyDecodeError):
            Base64KeyCodec().decode("not!valid")

    def test_encode_filter_orbit_vectors(self) -> None:
        codec = Base64KeyCodec()
        assert codec.encode_filter("user.123") == "dXNlcg.MTIz"
        assert codec.encode_filter("user.*") == "dXNlcg.*"
        assert codec.encode_filter("user.>") == "dXNlcg.>"
        assert codec.encode_filter("app.*.config.>") == "YXBw.*.Y29uZmln.>"

    @pytest.mark.parametrize("pattern", ["user.*", "user.>", "app.*.config.>", "orders.1"])
    def test_encode_filter_output_is_valid_subject_filter(self, pattern: str) -> None:
        validate_key(Base64KeyCodec().encode_filter(pattern), wildcards=True)  # must not raise


class TestPathKeyCodec:
    @pytest.mark.parametrize(
        ("raw", "encoded", "decoded"),
        [
            ("/foo/bar", "_root_.foo.bar", "/foo/bar"),
            ("foo/bar", "foo.bar", "foo/bar"),
            ("/foo/bar/baz/qux", "_root_.foo.bar.baz.qux", "/foo/bar/baz/qux"),
            ("/foo", "_root_.foo", "/foo"),
            ("foo/bar/", "foo.bar", "foo/bar"),
            ("/", "_root_", "/"),
            ("/foo/bar/", "_root_.foo.bar", "/foo/bar"),
        ],
    )
    def test_orbit_vectors(self, raw: str, encoded: str, decoded: str) -> None:
        codec = PathKeyCodec()
        assert codec.encode(raw) == encoded
        assert codec.decode(encoded) == decoded

    @pytest.mark.parametrize(
        ("pattern", "expected"),
        [
            ("/user/123", "_root_.user.123"),
            ("/user/*", "_root_.user.*"),
            ("/user/>", "_root_.user.>"),
            ("/app/*/config/>", "_root_.app.*.config.>"),
            ("user/*", "user.*"),
        ],
    )
    def test_encode_filter_orbit_vectors(self, pattern: str, expected: str) -> None:
        assert PathKeyCodec().encode_filter(pattern) == expected

    @given(st.lists(valid_tokens, min_size=1, max_size=5))
    def test_roundtrip_slash_paths(self, segments: list[str]) -> None:
        # segments never contain "/" or "." (alphabet excludes ".").
        segments = [s.replace("/", "_") for s in segments]
        key = "/" + "/".join(segments)
        codec = PathKeyCodec()
        assert codec.decode(codec.encode(key)) == key

    @given(valid_keys)
    def test_encoded_key_always_validates(self, key: str) -> None:
        # A NATS-valid key (already dot-separated, no leading/trailing slash)
        # must encode to a NATS-valid key.
        encoded = PathKeyCodec().encode(key)
        validate_key(encoded)

    def test_dot_in_input_is_ambiguous(self) -> None:
        # Documented caveat (shared with orbit.go): PathKeyCodec maps "/"<->"."
        # and cannot distinguish a literal "." in the input from a separator, so
        # a dotted input does NOT round-trip. Feed it path-style keys only.
        codec = PathKeyCodec()
        assert codec.decode(codec.encode("a.b")) == "a/b"  # "." read back as "/"

    def test_empty_segment_produces_invalid_key(self) -> None:
        # Documented edge: "//" -> empty token -> not a valid NATS key. The
        # codec itself does not raise; the core's post-encode validate_key does.
        encoded = PathKeyCodec().encode("/a//b")
        assert encoded == "_root_.a..b"
        with pytest.raises(InvalidKeyError):  # rejected by the core seam
            validate_key(encoded)


class TestNoOpKeyCodec:
    def test_identity(self) -> None:
        codec = NoOpKeyCodec()
        assert codec.encode("foo.bar.baz") == "foo.bar.baz"
        assert codec.decode("foo.bar.baz") == "foo.bar.baz"
        assert codec.encode_filter("user.*") == "user.*"


class TestChainKeyCodec:
    def test_empty_raises(self) -> None:
        with pytest.raises(NoCodecsError):
            ChainKeyCodec()

    def test_order_and_roundtrip(self) -> None:
        # Path first, then base64 the path-converted tokens.
        chain = ChainKeyCodec(PathKeyCodec(), Base64KeyCodec())
        raw = "/config/app/db"
        encoded = chain.encode(raw)
        # _root_.config.app.db, each token base64'd
        assert encoded == ".".join(
            base64.urlsafe_b64encode(t.encode()).rstrip(b"=").decode() for t in ["_root_", "config", "app", "db"]
        )
        validate_key(encoded)
        assert chain.decode(encoded) == raw

    @given(st.lists(valid_tokens, min_size=1, max_size=5))
    def test_roundtrip_path_style(self, segments: list[str]) -> None:
        # A PathKeyCodec-led chain round-trips *path-style* inputs (separators
        # are "/"). Literal dots in the input are ambiguous to PathKeyCodec (it
        # maps "/"<->"."), so we feed slash-separated keys — same contract as
        # orbit.go's PathCodec.
        key = "/" + "/".join(segments)
        chain = ChainKeyCodec(PathKeyCodec(), Base64KeyCodec())
        assert chain.decode(chain.encode(key)) == key

    def test_encode_filter_all_filterable(self) -> None:
        chain = ChainKeyCodec(NoOpKeyCodec(), Base64KeyCodec())
        result = chain.encode_filter("orders.*.status")
        assert "*" in result

    def test_encode_filter_non_filterable_member_raises(self) -> None:
        class Custom:
            def encode(self, key: str) -> str:
                return key.upper()

            def decode(self, key: str) -> str:
                return key.lower()

        chain = ChainKeyCodec(Base64KeyCodec(), Custom())
        with pytest.raises(WildcardNotSupportedError):
            chain.encode_filter("orders.*")


class TestFilterableProtocol:
    """The kvcodec key codecs must satisfy the core's runtime-checkable
    `natsio.kv.FilterableKeyCodec` protocol so `watch()` recognises them."""

    @pytest.mark.parametrize("codec", [Base64KeyCodec(), PathKeyCodec(), NoOpKeyCodec()])
    def test_key_codecs_are_filterable(self, codec: object) -> None:
        assert isinstance(codec, FilterableKeyCodec)

    def test_all_filterable_chain_is_filterable(self) -> None:
        assert isinstance(ChainKeyCodec(PathKeyCodec(), Base64KeyCodec()), FilterableKeyCodec)

    def test_plain_codec_is_not_filterable(self) -> None:
        class Plain:
            def encode(self, key: str) -> str:
                return key

            def decode(self, key: str) -> str:
                return key

        assert not isinstance(Plain(), FilterableKeyCodec)


class TestBase64ValueCodec:
    @given(st.binary(max_size=64))
    def test_roundtrip(self, value: bytes) -> None:
        codec = Base64ValueCodec()
        assert codec.decode(codec.encode(value)) == value

    def test_decode_corrupt_raises(self) -> None:
        with pytest.raises(ValueDecodeError):
            Base64ValueCodec().decode(b"!!!not-base64!!!")


class TestZlibValueCodec:
    @given(st.binary(max_size=256))
    def test_roundtrip(self, value: bytes) -> None:
        codec = ZlibValueCodec()
        assert codec.decode(codec.encode(value)) == value

    def test_compresses_repetitive(self) -> None:
        value = b"A" * 10_000
        encoded = ZlibValueCodec().encode(value)
        assert len(encoded) < len(value)

    def test_decode_corrupt_raises(self) -> None:
        with pytest.raises(ValueDecodeError):
            ZlibValueCodec().decode(b"not a zlib stream")

    def test_bad_level_rejected(self) -> None:
        with pytest.raises(ValueError):
            ZlibValueCodec(level=99)

    def test_level_is_honored(self) -> None:
        value = b"the quick brown fox " * 100
        assert ZlibValueCodec(level=0).encode(value) != ZlibValueCodec(level=9).encode(value)
        # level 0 = stored (no compression), so it round-trips too.
        codec0 = ZlibValueCodec(level=0)
        assert codec0.decode(codec0.encode(value)) == value


class TestNoOpValueCodec:
    def test_identity(self) -> None:
        codec = NoOpValueCodec()
        assert codec.encode(b"x") == b"x"
        assert codec.decode(b"x") == b"x"


class TestChainValueCodec:
    def test_empty_raises(self) -> None:
        with pytest.raises(NoCodecsError):
            ChainValueCodec()

    @given(st.binary(max_size=256))
    def test_zlib_then_base64_roundtrip(self, value: bytes) -> None:
        chain = ChainValueCodec(ZlibValueCodec(), Base64ValueCodec())
        encoded = chain.encode(value)
        # base64 output is ASCII-safe
        assert all(0x2D <= b <= 0x7A for b in encoded) or encoded == b""
        assert chain.decode(encoded) == value

    def test_order_matters(self) -> None:
        value = b"hello world " * 50
        chain_zb = ChainValueCodec(ZlibValueCodec(), Base64ValueCodec())
        chain_bz = ChainValueCodec(Base64ValueCodec(), ZlibValueCodec())
        zlib_first = chain_zb.encode(value)
        b64_first = chain_bz.encode(value)
        # Different pipelines produce different bytes, and each still round-trips
        # (proving decode reverses in the right order).
        assert zlib_first != b64_first
        assert chain_zb.decode(zlib_first) == value
        assert chain_bz.decode(b64_first) == value
        # zlib-then-b64 is ASCII-safe; b64-then-zlib is not.
        assert all(0x2D <= b <= 0x7A for b in zlib_first)


def test_zlib_encode_matches_stdlib() -> None:
    # Sanity: our encode is exactly stdlib zlib.compress at the given level.
    value = b"payload"
    assert ZlibValueCodec(level=6).encode(value) == zlib.compress(value, 6)
