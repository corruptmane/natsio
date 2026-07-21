"""NKey seed/public-key codec (the subset a client needs).

An NKey is a base32-encoded (RFC 4648 alphabet, no padding) byte string:
``prefix byte(s) + 32 raw key bytes + CRC-16/XMODEM (little-endian)``.
Seeds carry two prefix bytes — the seed marker with the role folded in —
and decode to the Ed25519 *private seed*; public keys carry one role prefix
byte and hold the Ed25519 public key.

This is a checksummed encoding, not cryptography: no package publishes it
standalone, and owning it buys ``str`` in/out, typed errors, and independence
from a thinly-maintained wrapper. The Ed25519 math it needs is delegated to
`natsio._internal.auth.signer`.
"""

import base64
from dataclasses import dataclass, field
from enum import IntEnum

from natsio.errors import ConfigError

from . import signer

__all__ = ["KeyPair", "Role", "decode_seed", "encode_seed", "from_seed"]

_PREFIX_SEED = 18 << 3  # 0x90, base32 'S'


class Role(IntEnum):
    OPERATOR = 14 << 3  # 'O'
    ACCOUNT = 0  # 'A'
    USER = 20 << 3  # 'U'
    CLUSTER = 2 << 3  # 'C'
    SERVER = 13 << 3  # 'N'


def _crc16(data: bytes) -> int:
    # CRC-16/XMODEM: poly 0x1021, init 0, no reflection, no final xor.
    crc = 0
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021 if crc & 0x8000 else crc << 1) & 0xFFFF
    return crc


def _b32_decode(text: str) -> bytes:
    padding = "=" * (-len(text) % 8)
    try:
        return base64.b32decode(text + padding)
    except Exception as exc:
        raise ConfigError(f"invalid nkey encoding: {exc}") from None


def _b32_encode(raw: bytes) -> str:
    return base64.b32encode(raw).decode("ascii").rstrip("=")


def _check_crc(raw: bytes) -> bytes:
    if len(raw) < 3:
        raise ConfigError("nkey too short")
    body, crc = raw[:-2], int.from_bytes(raw[-2:], "little")
    if _crc16(body) != crc:
        raise ConfigError("nkey checksum mismatch")
    return body


def encode_seed(role: Role, raw_seed: bytes) -> str:
    """Encode a 32-byte private seed as an ``S…`` seed string (inverse of decode_seed)."""
    if len(raw_seed) != 32:
        raise ConfigError("raw seed must be exactly 32 bytes")
    b1 = _PREFIX_SEED | (role.value >> 5)
    b2 = (role.value & 0x1F) << 3
    body = bytes([b1, b2]) + raw_seed
    return _b32_encode(body + _crc16(body).to_bytes(2, "little"))


def decode_seed(seed: str) -> tuple[Role, bytes]:
    """Decode an ``S...`` seed string into its role and 32-byte private seed."""
    body = _check_crc(_b32_decode(seed.strip()))
    if len(body) != 34:
        raise ConfigError("nkey seed has wrong length")
    b1, b2 = body[0], body[1]
    if b1 & 0xF8 != _PREFIX_SEED:
        raise ConfigError("not an nkey seed (missing 'S' prefix)")
    role_value = ((b1 & 0x07) << 5) | ((b2 & 0xF8) >> 3)
    try:
        role = Role(role_value)
    except ValueError:
        raise ConfigError(f"unknown nkey role prefix: {role_value:#x}") from None
    return role, body[2:]


def _encode_public(role: Role, public: bytes) -> str:
    raw = bytes([role.value]) + public
    return _b32_encode(raw + _crc16(raw).to_bytes(2, "little"))


@dataclass(frozen=True, slots=True)
class KeyPair:
    """A usable client identity: public nkey string + nonce signing."""

    role: Role
    public_key: str
    _seed: bytes = field(repr=False)

    def sign(self, data: bytes) -> bytes:
        return signer.sign(self._seed, data)

    def sign_nonce_b64(self, nonce: bytes) -> str:
        """Signature encoded the way CONNECT's ``sig`` field expects: raw base64url, no padding."""
        return base64.urlsafe_b64encode(self.sign(nonce)).decode("ascii").rstrip("=")


def from_seed(seed: str) -> KeyPair:
    role, raw_seed = decode_seed(seed)
    public = signer.public_key(raw_seed)
    return KeyPair(role=role, public_key=_encode_public(role, public), _seed=raw_seed)
