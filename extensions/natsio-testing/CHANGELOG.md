# Changelog

## 0.1.1 — 2026-10-22

- Require `natsio>=1.0.1` as the shared stable baseline, including the core security and reliability fixes.
- Verify installed wheels and namespace composition outside editable workspace installs.

## 0.1.0

Initial release. `NatsServerProcess` — start, stop, and fault-inject a real
`nats-server` for integration tests (JetStream, custom config, TLS, persistent
store dirs, SIGKILL restart-on-the-same-port), plus binary discovery
(`find_server_binary`, `NATS_SERVER_BIN`) and free-port helpers. Used by
natsio's own integration suite.
