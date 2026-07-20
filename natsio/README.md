# natsio

Zero-dependency asyncio NATS client for modern Python.

- Python 3.13+, NATS server 2.14+ (JetStream API level 3)
- No runtime dependencies — stdlib only
- Sans-io protocol core under a structured-concurrency asyncio shell
- JetStream (ADR-37 simplified API), Key-Value, and Object Store included

> ⚠️ Under active development — a ground-up rewrite. Not yet usable.
> The previous implementation is preserved on the `legacy` branch.
