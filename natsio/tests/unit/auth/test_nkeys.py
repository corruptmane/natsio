import base64

import pytest

from natsio._internal.auth import nkeys, signer
from natsio.errors import ConfigError


def make_seed(role: nkeys.Role, raw: bytes) -> str:
    """Encode a raw 32-byte seed the way `nk` does, for test fixtures."""
    return nkeys.encode_seed(role, raw)


def test_round_trip_seed_encode_decode() -> None:
    raw = bytes(range(32))
    seed = make_seed(nkeys.Role.USER, raw)
    assert seed.startswith("SU")
    role, decoded = nkeys.decode_seed(seed)
    assert role is nkeys.Role.USER
    assert decoded == raw


def test_public_key_derivation_matches_backend() -> None:
    raw = bytes(range(32))
    pair = nkeys.from_seed(make_seed(nkeys.Role.USER, raw))
    assert pair.public_key.startswith("U")
    # Decode the public nkey back to raw bytes and compare.
    padding = "=" * (-len(pair.public_key) % 8)
    decoded = base64.b32decode(pair.public_key + padding)
    assert decoded[0] == nkeys.Role.USER.value
    assert decoded[1:33] == signer.public_key(raw)


def test_sign_nonce_b64_is_urlsafe_unpadded() -> None:
    pair = nkeys.from_seed(make_seed(nkeys.Role.USER, bytes(32)))
    encoded = pair.sign_nonce_b64(b"some-nonce")
    assert "=" not in encoded
    padded = encoded + "=" * (-len(encoded) % 4)
    signature = base64.urlsafe_b64decode(padded)
    assert signature == pair.sign(b"some-nonce")


def test_corrupted_checksum_rejected() -> None:
    seed = make_seed(nkeys.Role.USER, bytes(32))
    corrupted = seed[:-1] + ("A" if seed[-1] != "A" else "B")
    with pytest.raises(ConfigError, match=r"checksum|encoding"):
        nkeys.decode_seed(corrupted)


def test_non_seed_prefix_rejected() -> None:
    raw = bytes([nkeys.Role.USER.value]) + bytes(32)
    encoded = nkeys._b32_encode(raw + nkeys._crc16(raw).to_bytes(2, "little"))
    with pytest.raises(ConfigError, match=r"prefix|length"):
        nkeys.decode_seed(encoded)


def test_garbage_rejected() -> None:
    with pytest.raises(ConfigError):
        nkeys.decode_seed("definitely not an nkey!!")
