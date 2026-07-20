# Extensions

Orbit-style extension packages, following the model of
[synadia-io/orbit.py](https://github.com/synadia-io/orbit.py) with the `natsio` prefix
in place of `orbit`:

- Distribution name: `natsio-<name>` (e.g. `natsio-kvcodec`), directory `extensions/natsio-<name>/`.
- Import name: `natsio_<name>` (e.g. `import natsio_kvcodec`). The core `natsio` package is a
  regular package with a real `__init__.py`, so extensions use their own top-level module rather
  than a PEP 420 namespace — this keeps the core's flat re-exporting `__init__` and avoids
  multi-distribution namespace merging.
- Each extension is an independent uv-workspace member with its own version, changelog, and
  release tags (`<name>/vX.Y.Z`), depending on `natsio` (never the reverse).
- Pre-1.0 extensions make no API-stability promises.

Planned roster: `natsio-counters` (ADR-49), `natsio-schedules` (ADR-51), `natsio-jetstream-batch`
(2.14 fast-ingest), `natsio-kvcodec` (ADR-54), `natsio-natscontext` (ADR-21), `natsio-sysclient`,
`natsio-pcgroups`, `natsio-otel` (metrics/tracing adapter over the core instrumentation seam).

If a module is ever adopted into the official orbit.py workspace, the transplant is mechanical:
rename the distribution to `orbit-<name>` and the module to the `orbit.<name>` namespace form.
