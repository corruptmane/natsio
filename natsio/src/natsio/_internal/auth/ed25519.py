"""Pure-Python Ed25519 signing (RFC 8032, edwards25519).

Vendored so NKey/JWT authentication works with zero dependencies. This is the
straightforward reference construction: arbitrary-precision field arithmetic,
extended homogeneous coordinates, no constant-time guarantees. That trade-off
is deliberate and acceptable here — the client signs exactly one 16-byte
server nonce per (re)connect with no adversary-controlled timing surface at
Python granularity. Installations wanting hardened crypto install the
``nkeys``/``cryptography``-backed signer, which is auto-preferred when present
(see :mod:`natsio._internal.auth.signer`).

Validated against the RFC 8032 §7.1 test vectors in the unit suite and
end-to-end against a real nats-server (which verifies our signatures) in the
integration suite.
"""

import hashlib

__all__ = ["public_key", "sign"]

_P = 2**255 - 19
_L = 2**252 + 27742317777372353535851937790883648493


def _inv(x: int) -> int:
    return pow(x, _P - 2, _P)


_D = -121665 * _inv(121666) % _P
_I = pow(2, (_P - 1) // 4, _P)


def _x_recover(y: int) -> int:
    xx = (y * y - 1) * _inv(_D * y * y + 1) % _P
    x = pow(xx, (_P + 3) // 8, _P)
    if (x * x - xx) % _P != 0:
        x = x * _I % _P
    if x % 2 != 0:
        x = _P - x
    return x


_BY = 4 * _inv(5) % _P
_BX = _x_recover(_BY)
# Base point in extended homogeneous coordinates (X, Y, Z, T), T = X*Y/Z.
_BASE = (_BX, _BY, 1, _BX * _BY % _P)
_IDENTITY = (0, 1, 1, 0)

type _Point = tuple[int, int, int, int]


def _add(p: _Point, q: _Point) -> _Point:
    # "add-2008-hwcd-3" unified addition, complete for edwards25519.
    x1, y1, z1, t1 = p
    x2, y2, z2, t2 = q
    a = (y1 - x1) * (y2 - x2) % _P
    b = (y1 + x1) * (y2 + x2) % _P
    c = t1 * 2 * _D * t2 % _P
    d = z1 * 2 * z2 % _P
    e = b - a
    f = d - c
    g = d + c
    h = b + a
    return (e * f % _P, g * h % _P, f * g % _P, e * h % _P)


def _scalar_mult(point: _Point, scalar: int) -> _Point:
    result = _IDENTITY
    while scalar:
        if scalar & 1:
            result = _add(result, point)
        point = _add(point, point)
        scalar >>= 1
    return result


def _compress(point: _Point) -> bytes:
    x, y, z, _ = point
    zi = _inv(z)
    x = x * zi % _P
    y = y * zi % _P
    return ((y & ((1 << 255) - 1)) | ((x & 1) << 255)).to_bytes(32, "little")


def _expand_seed(seed: bytes) -> tuple[int, bytes]:
    if len(seed) != 32:
        raise ValueError("Ed25519 seed must be exactly 32 bytes")
    digest = hashlib.sha512(seed).digest()
    scalar = int.from_bytes(digest[:32], "little")
    scalar &= (1 << 254) - 8
    scalar |= 1 << 254
    return scalar, digest[32:]


def public_key(seed: bytes) -> bytes:
    """32-byte public key for a 32-byte private seed."""
    scalar, _ = _expand_seed(seed)
    return _compress(_scalar_mult(_BASE, scalar))


def sign(seed: bytes, message: bytes) -> bytes:
    """64-byte Ed25519 signature of ``message`` under ``seed``."""
    scalar, prefix = _expand_seed(seed)
    pub = _compress(_scalar_mult(_BASE, scalar))
    r = int.from_bytes(hashlib.sha512(prefix + message).digest(), "little") % _L
    r_compressed = _compress(_scalar_mult(_BASE, r))
    k = int.from_bytes(hashlib.sha512(r_compressed + pub + message).digest(), "little") % _L
    s = (r + k * scalar) % _L
    return r_compressed + s.to_bytes(32, "little")
