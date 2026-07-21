"""Authentication and TLS: every credential type natsio speaks.

NATS supports several auth schemes, and natsio exposes each one both as a flat
``connect`` keyword and as an :class:`~natsio.Authenticator` object (use the
object when you need a custom or callback-driven flow). This script is a
*reference*: it shows how to wire each scheme, but the credential-specific
sections only run when the matching environment variable is set, so it executes
cleanly against a plain, no-auth server too.

Schemes covered:

* user/password        — ``user=`` / ``password=``   (or ``UserPasswordAuth``)
* token                — ``token=``                   (or ``TokenAuth``)
* NKey seed            — ``nkey_seed=``               (or ``NKeyAuth``) *
* .creds file (JWT)    — ``credentials=<path>``       (or ``CredsFileAuth``) *
* TLS                  — ``tls=TLSConfig(context=...)``

    * NKey and JWT need an Ed25519 backend — install ``natsio[nkeys]`` (PyNaCl)
      or ``natsio[cryptography]``. The core client ships no crypto. If your keys
      live in a KMS/HSM, use ``CallbackAuth`` and stay dependency-free.

A subtle but important rule: the flat auth fields are mutually exclusive and
``user``/``password`` must be given together — natsio raises ``ConfigError`` up
front rather than letting an ambiguous CONNECT reach the server.

Run it (start a server first with ``just server``)::

    python examples/05_auth_tls.py
    NATS_USER=app NATS_PASSWORD=s3cret python examples/05_auth_tls.py
    NATS_TOKEN=... / NATS_NKEY_SEED=SU... / NATS_CREDS=/path/app.creds
"""

import asyncio
import os
import ssl

import natsio

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")


def describe_auth_options() -> None:
    """Build (but don't connect with) each authenticator, to show the shapes.

    These objects are cheap and side-effect-free to construct (except NKey/JWT,
    which validate that a crypto backend is installed), so a reference like this
    can lay them all out side by side.
    """
    print("== authenticator objects (equivalent to the flat connect kwargs) ==")

    # user/password: connect(user=..., password=...) is shorthand for this.
    up = natsio.UserPasswordAuth(user="app", password="s3cret")
    print(f"  user/password: {up!r}")  # note: the password is kept out of repr

    # token: connect(token=...) is shorthand for this.
    tok = natsio.TokenAuth(token="s3cret-token")
    print(f"  token:         {tok!r}")

    # Any of these fields may also be a zero-arg callable (sync or async) that
    # returns a fresh string on every reconnect — handy for rotating secrets:
    #   TokenAuth(token=lambda: read_current_token())


async def connect_plain() -> None:
    """The baseline: connect with no credentials at all."""
    print("== no-auth connect (baseline) ==")
    async with await natsio.connect(NATS_URL) as nc:
        print(f"  connected to {nc.connected_url} (auth: none)")


async def connect_user_password() -> None:
    user, password = os.environ.get("NATS_USER"), os.environ.get("NATS_PASSWORD")
    if not (user and password):
        print("  [skip] set NATS_USER and NATS_PASSWORD to try user/password auth")
        return
    print("== user/password ==")
    # The flat form. `user` and `password` must be supplied together.
    async with await natsio.connect(NATS_URL, user=user, password=password) as nc:
        print(f"  connected as {user!r} to {nc.connected_url}")


async def connect_token() -> None:
    token = os.environ.get("NATS_TOKEN")
    if not token:
        print("  [skip] set NATS_TOKEN to try token auth")
        return
    print("== token ==")
    async with await natsio.connect(NATS_URL, token=token) as nc:
        print(f"  connected with token to {nc.connected_url}")


async def connect_nkey() -> None:
    seed = os.environ.get("NATS_NKEY_SEED")
    if not seed:
        print("  [skip] set NATS_NKEY_SEED (an 'SU...' seed) to try NKey auth — needs natsio[nkeys]")
        return
    print("== NKey ==")
    # natsio signs the server's nonce with the seed. Requires natsio[nkeys].
    async with await natsio.connect(NATS_URL, nkey_seed=seed) as nc:
        print(f"  connected via NKey to {nc.connected_url}")


async def connect_creds() -> None:
    path = os.environ.get("NATS_CREDS")
    if not path:
        print("  [skip] set NATS_CREDS=/path/to/app.creds to try JWT auth — needs natsio[nkeys]")
        return
    print("== .creds file (JWT + seed) ==")
    # The .creds file is re-read on every (re)connect, so credential rotation on
    # disk is picked up without reconnecting the client yourself.
    async with await natsio.connect(NATS_URL, credentials=path) as nc:
        print(f"  connected via {path} to {nc.connected_url}")


async def connect_tls() -> None:
    tls_url = os.environ.get("NATS_TLS_URL")
    if not tls_url:
        print("  [skip] set NATS_TLS_URL=tls://host:4222 to try a TLS connection")
        # Even without a server, show how the config is built: TLSConfig wraps a
        # standard library ssl.SSLContext, so all of Python's TLS knobs apply
        # (custom CA, client certs, verification mode, ...).
        ctx = ssl.create_default_context()
        _ = natsio.TLSConfig(context=ctx, hostname="nats.example.com")
        print("  (built a TLSConfig(context=ssl.create_default_context()) for reference)")
        return
    print("== TLS ==")
    ctx = ssl.create_default_context()  # trusts the system CA bundle
    # handshake_first upgrades to TLS *before* the server sends INFO (2.10.4+);
    # leave it False for the standard STARTTLS-style upgrade.
    tls = natsio.TLSConfig(context=ctx, handshake_first=False)
    async with await natsio.connect(tls_url, tls=tls) as nc:
        print(f"  connected over TLS to {nc.connected_url}")


async def main() -> None:
    describe_auth_options()
    await connect_plain()
    await connect_user_password()
    await connect_token()
    await connect_nkey()
    await connect_creds()
    await connect_tls()
    print("done")


if __name__ == "__main__":
    asyncio.run(main())
