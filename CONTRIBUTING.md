# Contributing to natsio

Thanks for your interest. natsio is a zero-dependency asyncio NATS client;
the bar is high on correctness and low on ceremony.

## Setup

The repo is a [uv](https://docs.astral.sh/uv/) workspace. One command:

```bash
uv sync
```

Everything else runs through [`just`](https://github.com/casey/just) — CI runs
the same recipes. Local success is necessary; CI also checks other Python,
platform and server versions:

```bash
just gates          # format check, lint, types (ty), full test suite — the bar for every change
just unit           # fast, hermetic tests only
just integration    # tests against a real nats-server
just docs           # build the docs site (strict: broken links fail)
just bench --quick  # benchmark against nats-py / nats-core
```

`just install-server v2.15.1` downloads an official server and verifies its
SHA-256 against the release manifest and reviewed digest. Use
`just install-server v2.14.0` for the minimum baseline. `NATS_SERVER_BIN`
selects another executable. `just integration` runs core **and extension** live
tests and fails if the binary is missing; ordinary `just test` can skip tests
when infrastructure is absent. CI covers Linux/macOS Python 3.13–3.15, with
2.14.0 as a floor and current stable as a merge gate. Windows is unvalidated.
Upstream `main` is a scheduled radar, not a merge gate.

## Ground rules

These are load-bearing — read [`docs/decisions.md`](docs/decisions.md) before
changing anything non-trivial. In short:

- **Zero runtime dependencies.** Stdlib only; no C extensions. Ed25519 is an
  optional extra.
- **Wire contracts are pinned** to nats.go / the ADRs, often byte-level.
  Changing emitted bytes is an interop event, not a refactor.
- **Loud failure.** Nothing drops silently; every parked `await` has a wake
  path; `CancelledError` is never suppressed.
- **Tests for everything.** Unit for local logic, live-server for anything that
  touches the wire. Event-driven waits, never `sleep`. Name a regression test
  for its finding.
- **Docstrings are plain markdown** (mkdocstrings renders them) — never Sphinx
  `:meth:`/`:class:` roles.
- **Suppressions are visible and justified.** No blanket `# type: ignore`; a
  `# noqa` carries a reason. Lazy imports are banned in source except for a
  documented circular/optional break.

## Pull requests

Branch from `main`, keep the change focused, and fill in the PR checklist.
Update `CHANGELOG.md`'s `## Unreleased` section for anything user-visible — the
release tooling turns that into the dated release notes.

## Extensions

New capabilities that aren't core-shaped belong in an extension distribution
(`natsio-<name>`, imported as `natsio.<name>`) — see
[`extensions/README.md`](extensions/README.md).

## Release preparation and publication

For core fixes use the next patch version; additions require a minor release
and breaking changes wait for 2.0. Write non-empty `## Unreleased` notes, then
run `just release X.Y.Z`. This dates the notes, updates version pins and the
lockfile, and runs gates. For each changed extension use its own notes and
`just release-extension <name> X.Y.Z`. Extension minimum core versions must
cover the API and behavioral contracts actually used. The current extension
releases share `natsio>=1.0.1` as their stable baseline so they also receive the
core security and reliability fixes; pre-1.0 core compatibility is no longer a
release target.

Before handing off to the owner, run `just integration`, `just docs`,
`just build-all` and `just wheel-check`. Wheel checks install outside the
workspace, compose every namespace extension, and test both optional signing
backends in separate environments. Keep the raw benchmark JSON when hot paths
change; use repeated runs and report ranges rather than unsupported winners.

The owner reviews, signs commits, creates the core `vX.Y.Z` or extension
`<name>/vX.Y.Z` tags, pushes, and approves the protected `pypi` environment.
Agents must not perform those publication steps. On each tag the release
workflow validates that exact commit: tag/package/changelog agreement, baseline
and current server tests, gates, strict docs, all builds, clean wheel installs,
and embedded artifact metadata. Only artifacts produced in that validation run
are passed to the trusted publisher with attestations; it does not rebuild.
Repository branch protection should require CI and the owner should restrict
the `pypi` environment to release tags (repository settings are owner-managed).

Actions are pinned to reviewed commits and Dependabot proposes weekly action
and uv dependency updates. Server downloads are checksum-verified. Updating a
gating server also requires updating its reviewed digest in
`tools/install_server.py`; the moving radar validates upstream SHA256SUMS.

## Reliability validation

`just reliability` runs five bounded cycles per recovery scenario on a real
server. Set `NATSIO_RELIABILITY_CYCLES=200` for the weekly scheduled workload
and `NATSIO_RELIABILITY_REPORT=/tmp/reliability.json` for resource samples and
task stacks. The tests exercise alternating configured nodes with token
rotation, repeated transport disconnects with long-lived JetStream/KV/Object
Store handles, slow-consumer drop reporting, and cancelled blocked writes.
Each operation has an explicit deadline; synchronization uses events.

Samples after two warm-up cycles require stable task, dispatcher subscription,
public subscription, owned-task and request-sink counts. `tracemalloc` retained
allocations attributed to core source files may grow by at most a fixed 256 KiB
from the warm-up baseline, regardless of cycle count. This permits observed
parser/cache sawtooth and detects accumulating retained objects; it is not an
RSS, kernel-buffer, native-allocation, or total-process-memory guarantee.
Failures retain JSON samples/stacks, pytest output and JUnit in Actions artifacts.

The local 200-cycle run on Python 3.13.13/NATS 2.15.1 passed. Final retained core
allocations grew about 11 KiB (node failover), 28 KiB (store handles), and 44 KiB
(slow consumers/blocked writes) over warm-up, with stable resource counts and no
remaining natsio tasks after teardown. The separately bounded multi-reply probe
recorded 512 queued 1 KiB replies despite `max_msgs=1` while iteration was paused;
explicit `aclose()` released the queue. See the resource limitation documented
in the core messaging guide before using multi-reply requests for large fan-out.

Compatibility sources checked on 2026-10-22:
[Python 3.15.1](https://www.python.org/downloads/release/python-3151/),
[NATS Server 2.15.1](https://github.com/nats-io/nats-server/releases/tag/v2.15.1),
[nats-py 2.16.0](https://pypi.org/project/nats-py/2.16.0/), and
[nats-core 0.2.0](https://pypi.org/project/nats-core/0.2.0/).
