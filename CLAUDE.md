# natsio — agent guide

Zero-dependency asyncio NATS client (Python ≥3.13, NATS server ≥2.14).
Built as a ground-up rewrite to be faster and more idiomatic than nats-py,
with nats.go as the behavioral oracle. **Read `docs/decisions.md` before
changing anything load-bearing — every non-obvious choice there has a
reason, usually a scar.**

## Commands

Everything goes through `just` (CI runs the same recipes — never duplicate):

- `just gates` — format check, lint, types (`ty`, not mypy), full test suite. The bar for every change.
- `just unit` / `just integration` — integration needs `tools/.bin/nats-server` (or `NATS_SERVER_BIN`).
- `just docs` — strict MkDocs build; broken links/autorefs FAIL.
- `just bench --quick` — benchmark vs nats-py / nats-core.
- `just release X.Y.Z` — full version-bump ritual (guards changelog + monotonicity).

## Hard invariants (do not weaken)

1. **Zero runtime dependencies.** Stdlib only. Ed25519 is delegated to
   optional extras (`natsio[nkeys]`/`[cryptography]`); natsio ships no crypto.
   No C extensions. No third-party WebSocket lib — RFC 6455 is in-house.
2. **Sans-io protocol cores** (`_internal/protocol/`): no asyncio imports,
   pull-based, chunk-boundary safe. Any parser change must keep the
   differential chunking suites green (result identical under every byte
   split). Do NOT collapse them for performance — measured and rejected.
3. **Wire contracts are pinned to nats.go/ADRs**, often byte-level (JetStream
   entities, `$SRV` micro responses, KV/Object Store subjects and headers).
   Changing emitted bytes is an interop event, not a refactor.
4. **Termination discipline**: Event-latch closure (never in-band sentinels),
   every parked await must have a wake path from the `Closed` lifecycle
   event, never suppress `CancelledError`, self-teardown from inside a
   callback must not cancel the current task.
5. **Loud failure**: nothing drops silently — drops are counted and reported,
   reads are digest-verified, hostile input is a typed error, validation
   errors are byte-stable (fast paths are differential-tested against the
   reference implementation).
6. **Every rollup meta write is CAS-gated** (`expected_last_subject_seq`).
7. **Optional-await**: session factories (`subscribe`, `consume`, watchers,
   micro registration) tolerate `await` as a no-op returning self.
   `ObjectStore.get()` is deliberately excluded.

## Working style

- Tests: unit for local logic, live-server for anything touching the wire;
  event-driven waits, never sleeps; regression tests named for their finding.
- Reviews for substantial changes: adversarial reviewers with MANDATORY live
  probes, then refutation passes. Findings as markdown, not structured
  schemas. nats.go source (not memory of it) is the oracle for parity claims.
- Perf work: patch → measure → revert before landing; contract-preserving
  only; suite must stay green.
- Docstrings are **plain markdown** (mkdocstrings renders them) — never
  Sphinx `:meth:`/`:class:` roles or `::` blocks. Docs snippets are
  run-verified against a live server before publication.

## Git

- The owner signs and pushes; agents may commit locally but NEVER push.
- No AI attribution in commit messages (no footers, no Co-Authored-By).
- `git add` with **explicit paths only** — never `-A` (a real `.env` key
  once shipped that way).
- Trunk-based: `main` + release tags. Changelog lives under `## Unreleased`
  until `just release` dates it. Fix = patch, addition = minor, 1.0 = freeze.
