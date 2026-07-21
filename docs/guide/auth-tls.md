# Authentication & TLS

NATS supports several credential schemes. natsio exposes each one two ways: as a
flat `connect` keyword for the common case, and as an `Authenticator` object for
custom or callback-driven flows. TLS is configured with a standard-library
`ssl.SSLContext`.

## The flat auth fields are mutually exclusive

You may supply **at most one** of `user`/`password`, `token`, `nkey_seed`,
`credentials`, or `authenticator`. Conflicting fields — or `user` without
`password` — raise `ConfigError` up front, before any CONNECT reaches the
server:

```python
natsio.ConnectOptions(token="t", user="u", password="p")  # ConfigError
natsio.ConnectOptions(user="u")                           # ConfigError (needs password)
```

Every authenticator is re-invoked on **every** (re)connect — nonces change,
`.creds` files may have been rotated on disk, and callables may return fresh
tokens. Nothing is cached.

## User / password

```python
nc = await natsio.connect("nats://localhost:4222", user="app", password="s3cret")
```

Equivalent object form:

```python
auth = natsio.UserPasswordAuth(user="app", password="s3cret")
nc = await natsio.connect("nats://localhost:4222", authenticator=auth)
```

Any credential value may also be a zero-arg callable (sync or async) returning a
fresh string on each reconnect — handy for rotating secrets:

```python
natsio.UserPasswordAuth(user="app", password=lambda: read_current_password())
```

## Token

```python
nc = await natsio.connect("nats://localhost:4222", token="s3cret-token")
```

A token may also be carried **in the URL** — and if both are given, **the URL
wins**:

```python
# the URL token takes precedence over the token= keyword
nc = await natsio.connect("nats://s3cret-token@localhost:4222")
```

The object form is `natsio.TokenAuth(token=...)`.

## NKey seed

An NKey seed (`SU...`) is signed against the server's nonce. This needs an
Ed25519 backend — see [Crypto extras](#crypto-extras).

```python
nc = await natsio.connect("nats://localhost:4222", nkey_seed="SUAG...")
# object form: natsio.NKeyAuth(seed="SUAG...")
```

## .creds files (JWT)

A decorated `.creds` file bundles a JWT and its seed. Point `credentials=` at
the path — it is **re-read on every (re)connect**, so credential rotation on
disk is picked up without you reconnecting the client yourself. Also needs a
crypto backend.

```python
nc = await natsio.connect("nats://localhost:4222", credentials="/etc/app.creds")
# object form: natsio.CredsFileAuth(path="/etc/app.creds")
```

If the JWT and seed already live in memory (not a file), use
`natsio.CredsAuth(jwt=..., seed=...)`.

## CallbackAuth — KMS / HSM signing

When the private key must never leave a KMS, HSM, or auth-callout service, use
`CallbackAuth`: you provide the JWT and sign the server's nonce yourself.
natsio never touches the key, so this scheme needs **no crypto extra** — the
client stays fully dependency-free.

```python
def fetch_jwt() -> str:
    return kms.get_user_jwt()

def sign_nonce(nonce: bytes) -> bytes:
    return kms.sign(key_id="nats-user", data=nonce)   # returns 64 raw Ed25519 bytes

auth = natsio.CallbackAuth(jwt_callback=fetch_jwt, signature_callback=sign_nonce)
nc = await natsio.connect("nats://localhost:4222", authenticator=auth)
```

Both callbacks may be sync or async. natsio base64url-encodes the raw signature
bytes for the wire.

## Crypto extras

natsio deliberately ships **no cryptography of its own**. NKey and JWT/`.creds`
auth are the only features that need Ed25519, and they delegate it to an audited
external backend that you opt into:

```bash
uv add "natsio[nkeys]"         # PyNaCl (recommended)
uv add "natsio[cryptography]"  # if you already depend on `cryptography`
```

Either backend works — they are verified to produce identical keys and
signatures. Constructing an `NKeyAuth`, `CredsAuth`, or `CredsFileAuth` without
a backend installed raises `MissingDependencyError` immediately.

!!! tip "You may not need any extra"
    Token and user/password auth need nothing. And if your keys live in a
    KMS/HSM, `CallbackAuth` keeps natsio dependency-free — you do the signing.

## TLS

TLS is configured with a `TLSConfig` wrapping a standard-library
`ssl.SSLContext`, so every one of Python's TLS knobs applies (custom CA, client
certificates, verification mode, ...). A `context=None` uses
`ssl.create_default_context()`, which trusts the system CA bundle:

```python
import ssl
import natsio

ctx = ssl.create_default_context()                 # system trust store
# ctx.load_verify_locations("ca.pem")              # custom CA
# ctx.load_cert_chain("client.pem", "client.key")  # mutual TLS

tls = natsio.TLSConfig(context=ctx)
nc = await natsio.connect("tls://nats.example.com:4222", tls=tls)
```

Use the `tls://` URL scheme (or let a server that *requires* TLS force the
upgrade — natsio upgrades the socket when the server's INFO demands it, even
over a `nats://` URL).

### handshake-first (TLS before INFO)

NATS 2.10.4+ supports upgrading to TLS *before* the server sends its INFO line.
Set `handshake_first=True` for that mode; leave it `False` (the default) for the
standard STARTTLS-style upgrade:

```python
tls = natsio.TLSConfig(context=ctx, handshake_first=True)
```

`TLSConfig` also takes a `hostname=` to override the name checked against the
server certificate when it differs from the URL host.

## Permission errors

When the server denies a publish or subscribe for a subject, natsio raises
`PermissionsViolationError` — **non-fatal**: the connection stays open.

By default a denied *subscription* surfaces only as a background error (via
`error_cb`), stays registered, and is re-sent — and re-denied — on every
reconnect. Set `permission_err_on_subscribe=True` to instead **latch** the
denial: the offending subscription is terminated and its `next_msg` / iteration
raise `PermissionsViolationError`, matching nats.go's behavior.

```python
nc = await natsio.connect(
    "nats://localhost:4222",
    user="restricted", password="pw",
    permission_err_on_subscribe=True,   # denied subs raise into the consumer
)
```

## See also

- [Connection & lifecycle](connection.md) — the `error_cb` and reconnect knobs
  referenced above, including the 2-strikes auth abort.
- [Errors reference](../reference/errors.md) — `AuthorizationViolationError`,
  `PermissionsViolationError`, `MissingDependencyError`, and the rest.
- `examples/05_auth_tls.py` — a runnable reference covering every scheme.
