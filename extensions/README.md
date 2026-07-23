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
- **Releasing.** Pushing a `<name>/vX.Y.Z` tag builds and publishes
  `natsio-<name>` to PyPI (`.github/workflows/release-extension.yml`,
  trusted publishing via the `pypi` environment). The tag is all that's
  required. **GitHub Releases are core-only**: don't create a Release object
  for extension tags — they clutter the Releases tab and can steal the
  "Latest" badge from the core `natsio` release. PyPI plus each extension's
  `CHANGELOG.md` is the record. Revisit per-extension only if one gets real
  traction (and then mark those Releases as *not* latest).
- If a module is ever adopted into the official orbit.py workspace, the
  transplant is mechanical: rename the distribution to `orbit-<name>` and move
  `natsio/<name>/` to `orbit/<name>/` (orbit is a pure PEP 420 namespace).

## Roster

| Extension | Import | Status |
|---|---|---|
| `natsio-testing` | `natsio.testing` | nats-server process manager for tests — implemented |
| `natsio-counters` | `natsio.counters` | distributed counters (ADR-49) — implemented |
| `natsio-schedules` | `natsio.schedules` | message schedules (ADR-51) — implemented |
| `natsio-jetstream-batch` | `natsio.jetstream_batch` | 2.14 fast-ingest batch publish + batch reads — implemented |
| `natsio-kvcodec` | `natsio.kvcodec` | KV key/value codecs (ADR-54) — implemented |
| `natsio-natscontext` | `natsio.natscontext` | NATS CLI context files (ADR-21) — implemented |
| `natsio-sysclient` | `natsio.sysclient` | `$SYS` monitoring API client — implemented |
| `natsio-pcgroups` | `natsio.pcgroups` | partitioned consumer groups (static + elastic) — implemented |
| `natsio-otel` | `natsio.otel` | OpenTelemetry metrics + trace-context propagation — implemented |
