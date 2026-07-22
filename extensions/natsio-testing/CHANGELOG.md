# Changelog

## 0.1.0

Initial release. `NatsServerProcess` — start, stop, and fault-inject a real
`nats-server` for integration tests (JetStream, custom config, TLS, persistent
store dirs, SIGKILL restart-on-the-same-port), plus binary discovery
(`find_server_binary`, `NATS_SERVER_BIN`) and free-port helpers. Used by
natsio's own integration suite.
