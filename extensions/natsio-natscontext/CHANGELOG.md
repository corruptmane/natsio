# Changelog

All notable changes to `natsio-natscontext` are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## 0.1.0

Initial release.

- Discover `nats` CLI contexts under `$XDG_CONFIG_HOME/nats` (fallback
  `~/.config/nats`): `config_dir()`, `context_dir()`, `list_contexts()`,
  `selected_context()`.
- `load(name=None)` returns a typed, frozen `Context` — by name, from the
  active `context.txt` selection, or from an absolute JSON path. Typed errors
  `ContextNotFoundError` / `ContextMalformedError` (both `ContextError`, a
  `natsio.errors.NATSError`).
- `Context.connect_kwargs()` projects the context onto `natsio.connect`
  keyword arguments — servers, user/password, token, creds, nkey (seed read
  from the referenced file), TLS (`cert`/`key`/`ca`/`tls_first` built into an
  `ssl.SSLContext`), and `inbox_prefix`.
- `async connect(name=None, **overrides)` convenience wrapper; overrides win
  over context-derived settings.
- Fields with no natsio connection equivalent (JetStream domain/prefixes,
  SOCKS proxy, `nsc`, `user_jwt`, CLI cosmetics) are parsed and preserved on
  the `Context` and documented as not-applied; a Windows certificate store is
  rejected. Unknown keys are surfaced via `Context.unknown_keys`.
