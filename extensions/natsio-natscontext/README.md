# natsio-natscontext

Connect [natsio](https://pypi.org/project/natsio/) using the connection
profiles ("contexts") managed by the [`nats` command-line
tool](https://github.com/nats-io/natscli) — the same JSON files under
`~/.config/nats/context/`. This is the Python counterpart of the Go helper
[`orbit.go/natscontext`](https://github.com/synadia-io/orbit.go/tree/main/natscontext),
following [ADR-21](https://github.com/nats-io/nats-architecture-and-design).

```bash
pip install natsio-natscontext
```

## Usage

Create a context with the CLI, then use it from Python:

```bash
nats context add staging --server nats://staging:4222 --creds ~/staging.creds
nats context select staging
```

```python
import natsio.natscontext as natscontext

# Connect using a named context...
async with await natscontext.connect("staging") as nc:
    await nc.publish("greet", b"hi")

# ...or the currently selected context (from `nats context select`).
async with await natscontext.connect() as nc:
    ...

# Overrides are forwarded to natsio.connect and win over the context:
nc = await natscontext.connect("staging", name="my-app", ping_interval=30)
```

Inspect or reuse a context without connecting:

```python
ctx = natscontext.load("staging")          # -> Context
print(ctx.url, ctx.jetstream_domain)
kwargs = ctx.connect_kwargs()              # dict of natsio.connect(**kwargs)

natscontext.list_contexts()                # ['prod', 'staging', ...]
natscontext.selected_context()             # 'staging' or None
```

An absolute path is loaded directly instead of by name:

```python
ctx = natscontext.load("/etc/nats/prod.json")
```

## Discovery

Contexts are resolved under `$XDG_CONFIG_HOME/nats` (falling back to
`~/.config/nats`):

| Path | Purpose |
|---|---|
| `nats/context/<name>.json` | one context per file |
| `nats/context.txt` | name of the active/selected context |

## Field mapping

| Context field | natsio option | Notes |
|---|---|---|
| `url` | `servers` | split on `,` into a tuple |
| `user` + `password` | `user`, `password` | |
| `creds` | `credentials` | `~` and `$VAR` expanded |
| `nkey` | `nkey_seed` | the **file** is read and the seed extracted |
| `token` | `token` | |
| `cert` + `key` | `tls` | `SSLContext.load_cert_chain` |
| `ca` | `tls` | `SSLContext.load_verify_locations` |
| `tls_first` | `tls.handshake_first` | |
| `inbox_prefix` | `inbox_prefix` | |

**Auth precedence** mirrors the Go helper: `user` › `creds` › `nkey` › `token`.
natsio does not allow combining mechanisms, so a `token` set *alongside*
another mechanism is ignored (nats.go would send both).

### Parsed but not applied

These fields are read and preserved on the returned `Context` (so you can use
them yourself — e.g. the JetStream domain when creating a JetStream context),
but natsio's connection layer has no equivalent and does **not** apply them:

| Context field | `Context` attribute | Why not applied |
|---|---|---|
| `jetstream_domain` | `.jetstream_domain` | connection-independent; used at the JetStream API layer |
| `jetstream_api_prefix` | `.jetstream_api_prefix` | JetStream API layer |
| `jetstream_event_prefix` | `.jetstream_event_prefix` | JetStream API layer |
| `socks_proxy` | `.socks_proxy` | natsio has no custom-dialer / proxy hook |
| `nsc` | `.nsc` | would shell out to the `nsc` binary |
| `user_jwt` | `.user_jwt` | the Go helper parses but never applies it either |
| `color_scheme` | `.color_scheme` | CLI cosmetic |
| `description` | `.description` | metadata |
| `windows_cert_store*` | `.windows_cert_store` | unsupported — `connect_kwargs()` raises `ContextError` |

Unrecognised JSON keys are kept in `Context.raw` and reported by
`Context.unknown_keys`.

## Interop with the `nats` CLI

This package only ever **reads** context files; it never writes them. Manage
contexts with the CLI (`nats context add|select|edit|ls|rm`). Files written by
this library's format are byte-compatible with what the CLI expects.

## License

Apache-2.0.
