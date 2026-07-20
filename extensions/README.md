# Extensions

Orbit-style extension packages, following the model of
[synadia-io/orbit.py](https://github.com/synadia-io/orbit.py) with the `natsio`
brand: distribution **`natsio-<name>`**, imported as **`natsio.<name>`**.

```python
from natsio.testing import NatsServerProcess   # pip install natsio-testing
```

## How the shared namespace works

The core `natsio` package is a regular package (its `__init__.py` provides the
flat `natsio.connect(...)` API) whose `__path__` is pkgutil-extended.
Extension wheels ship **only** their `natsio/<name>/` subpackage — never a
top-level `natsio/__init__.py` — so installers merge them into the namespace,
and the `extend_path` call in the core covers split-path setups (editable
installs, mixed roots). This is the same hybrid Airflow uses for its provider
distributions. In each extension's `pyproject.toml`:

```toml
[tool.uv.build-backend]
module-name = "natsio.<name>"
namespace = true
```

## Conventions

- Each extension is an independent uv-workspace member under
  `extensions/natsio-<name>/` with its own version, changelog, and release
  tags (`<name>/vX.Y.Z`), depending on `natsio` (never the reverse).
- Pre-1.0 extensions make no API-stability promises.
- If a module is ever adopted into the official orbit.py workspace, the
  transplant is mechanical: rename the distribution to `orbit-<name>` and move
  `natsio/<name>/` to `orbit/<name>/` (orbit is a pure PEP 420 namespace).

## Roster

| Extension | Import | Status |
|---|---|---|
| `natsio-testing` | `natsio.testing` | nats-server process manager for tests — implemented |
| `natsio-counters` | `natsio.counters` | distributed counters (ADR-49) — planned |
| `natsio-schedules` | `natsio.schedules` | message schedules (ADR-51) — planned |
| `natsio-jetstream-batch` | `natsio.jetstream_batch` | 2.14 fast-ingest batch publish — planned |
| `natsio-kvcodec` | `natsio.kvcodec` | KV key/value codecs (ADR-54) — planned |
| `natsio-natscontext` | `natsio.natscontext` | NATS CLI context files (ADR-21) — planned |
| `natsio-sysclient` | `natsio.sysclient` | system/monitoring API client — planned |
| `natsio-pcgroups` | `natsio.pcgroups` | partitioned consumer groups — planned |
| `natsio-otel` | `natsio.otel` | OpenTelemetry adapter over the instrumentation seam — planned |
