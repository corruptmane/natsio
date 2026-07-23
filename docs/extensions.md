# Extensions

The core `natsio` package is deliberately small and zero-dependency. Everything
optional — test helpers, observability exporters, experimental server features —
ships as a separate distribution that installs *into* the `natsio` namespace.
The model follows [synadia-io/orbit.py](https://github.com/synadia-io/orbit.py),
with the `natsio` brand: a distribution named **`natsio-<name>`** imports as
**`natsio.<name>`**.

```python
from natsio.testing import NatsServerProcess   # pip install natsio-testing
```

## How the shared namespace works

The core `natsio` package is a regular package — its `__init__.py` provides the
flat `natsio.connect(...)` API — whose `__path__` is `pkgutil`-extended.
Extension wheels ship **only** their `natsio/<name>/` subpackage (never a
top-level `natsio/__init__.py`), so installers merge them into the namespace,
and the `extend_path` call in the core covers split-path setups (editable
installs, mixed roots). This is the same hybrid Airflow uses for its provider
distributions.

Each extension's `pyproject.toml` declares its subpackage as a namespace module:

```toml
[tool.uv.build-backend]
module-name = "natsio.<name>"
namespace = true
```

## Depending on an extension

Extensions are independent distributions with their own versions and release
tags. Add one exactly as you would any dependency; it will pull in a compatible
`natsio` core.

```bash
uv add natsio-testing        # or: pip install natsio-testing
```

Then import from its namespaced module:

```python
import natsio
from natsio.testing import NatsServerProcess, find_server_binary

async def test_something():
    server = await NatsServerProcess(find_server_binary(), jetstream=True).start()
    try:
        nc = await natsio.connect(server.url)
        ...
    finally:
        await server.stop()
```

Conventions worth knowing:

- Each extension lives under `extensions/natsio-<name>/` as a uv-workspace
  member with its own version, changelog, and release tags (`<name>/vX.Y.Z`),
  depending on `natsio` — **never** the reverse.
- Pre-1.0 extensions make **no API-stability promises**.

## Roster

Each extension's own page (linked below) renders its full README — install,
usage, wire contract, and scope limits.

| Extension | Import | Status |
|---|---|---|
| [`natsio-testing`](extensions/testing.md) | `natsio.testing` | nats-server process manager for tests |
| [`natsio-counters`](extensions/counters.md) | `natsio.counters` | distributed counters (ADR-49) |
| [`natsio-schedules`](extensions/schedules.md) | `natsio.schedules` | message schedules (ADR-51) |
| [`natsio-jetstream-batch`](extensions/jetstream-batch.md) | `natsio.jetstream_batch` | 2.14 fast-ingest batch publish + batch reads |
| [`natsio-kvcodec`](extensions/kvcodec.md) | `natsio.kvcodec` | KV key/value codecs (ADR-54) |
| [`natsio-natscontext`](extensions/natscontext.md) | `natsio.natscontext` | NATS CLI context files (ADR-21) |
| [`natsio-sysclient`](extensions/sysclient.md) | `natsio.sysclient` | `$SYS` monitoring API client |
| [`natsio-pcgroups`](extensions/pcgroups.md) | `natsio.pcgroups` | partitioned consumer groups (static + elastic) |
| [`natsio-otel`](extensions/otel.md) | `natsio.otel` | OpenTelemetry adapter over the instrumentation seam |

`natsio-testing` is the real-server process manager (start/stop, configs, free
ports, readiness probing, JetStream store dirs, SIGKILL fault injection) that
natsio's own integration suite runs on.

## The instrumentation seam

natsio never imports a metrics or tracing library. Instead the client calls a
small `Instrumentation` protocol at its
load-bearing moments — connect/disconnect/reconnect/close, bytes in and out,
messages published and delivered, slow consumers, and errors. The default is a
no-op that costs nothing (it is not even wrapped on the hot path).

An exporter lives *outside* the core: implement the protocol and pass an instance
via `ConnectOptions(instrumentation=...)`.

```python
from natsio import ConnectOptions
from natsio.instrumentation import Instrumentation

class Metrics(Instrumentation):
    def on_message_published(self, subject: str, payload_size: int) -> None:
        published.labels(subject).inc()
    def on_message_delivered(self, subject: str, payload_size: int) -> None:
        delivered.labels(subject).inc()
    # ...the remaining hooks default to no-ops on the Protocol

nc = await natsio.connect(options=ConnectOptions(
    servers=("nats://localhost:4222",),
    instrumentation=Metrics(),
))
```

Hooks are invoked **synchronously** on hot paths, so implementations must be fast
and must never raise — a broken metrics backend cannot take down the connection
(exceptions are swallowed and logged). The **`natsio-otel`** extension is a
ready-made OpenTelemetry adapter over this exact seam.

## The orbit.py relationship

These extensions are shaped so that adoption into the official
[orbit.py](https://github.com/synadia-io/orbit.py) workspace is a mechanical
transplant: rename the distribution to `orbit-<name>` and move `natsio/<name>/`
to `orbit/<name>/` (orbit is a pure PEP 420 namespace). Nothing in an
extension's public API assumes the `natsio` name.
