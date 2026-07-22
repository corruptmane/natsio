"""Authenticators for NKey, JWT/.creds, token, user/password, and custom flows.

NKey and JWT auth need an Ed25519 backend — see the ``natsio[nkeys]`` /
``natsio[cryptography]`` extras. `CallbackAuth` needs neither: you sign
the server nonce yourself (KMS/HSM-held keys, auth-callout flows) and natsio
stays fully dependency-free.
"""

from natsio._internal.auth import (
    Authenticator,
    AuthResult,
    CallbackAuth,
    CredsAuth,
    CredsFileAuth,
    NKeyAuth,
    NKeyFileAuth,
    TokenAuth,
    UserPasswordAuth,
)

__all__ = [
    "AuthResult",
    "Authenticator",
    "CallbackAuth",
    "CredsAuth",
    "CredsFileAuth",
    "NKeyAuth",
    "NKeyFileAuth",
    "TokenAuth",
    "UserPasswordAuth",
]
