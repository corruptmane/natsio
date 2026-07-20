"""Ed25519 signing, delegated to an audited external implementation.

natsio deliberately ships no cryptography of its own. NKey and JWT
authentication need Ed25519, so this module resolves one of two interchangeable
backends at first use — PyNaCl (what the official NATS clients get, one layer
down, via the ``nkeys`` package) or ``cryptography``. Both were verified to
produce byte-identical keys and signatures.

Everything else in natsio keeps working with zero dependencies; only NKey/JWT
auth requires an extra, and users holding keys in a KMS/HSM can skip it
entirely with :class:`~natsio._internal.auth.authenticators.CallbackAuth`.
"""

from collections.abc import Callable
from functools import cache

from natsio.errors import MissingDependencyError

__all__ = ["public_key", "require_backend", "sign"]

_MISSING = """\
NKey/JWT authentication requires an Ed25519 backend, which is not installed.

  pip install 'natsio[nkeys]'         # PyNaCl (recommended)
  pip install 'natsio[cryptography]'  # if you already depend on `cryptography`

Alternatively, keep natsio dependency-free by signing the nonce yourself with
natsio.CallbackAuth(jwt_callback=..., signature_callback=...) — the right choice
for KMS/HSM-held keys and auth-callout flows.\
"""

type _Sign = Callable[[bytes, bytes], bytes]
type _Public = Callable[[bytes], bytes]


@cache
def _backend() -> tuple[_Sign, _Public]:
    try:
        from nacl.signing import SigningKey
    except ImportError:
        pass
    else:
        return (
            lambda seed, message: SigningKey(seed).sign(message).signature,
            lambda seed: bytes(SigningKey(seed).verify_key),
        )

    try:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    except ImportError:
        raise MissingDependencyError(_MISSING) from None

    return (
        lambda seed, message: Ed25519PrivateKey.from_private_bytes(seed).sign(message),
        lambda seed: (
            Ed25519PrivateKey.from_private_bytes(seed)
            .public_key()
            .public_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PublicFormat.Raw,
            )
        ),
    )


def require_backend() -> None:
    """Fail now, not mid-handshake, if no Ed25519 backend is available."""
    _backend()


def sign(seed: bytes, message: bytes) -> bytes:
    """64-byte Ed25519 signature of ``message`` under a 32-byte private ``seed``."""
    return _backend()[0](seed, message)


def public_key(seed: bytes) -> bytes:
    """32-byte Ed25519 public key for a 32-byte private ``seed``."""
    return _backend()[1](seed)
