# Changelog

All notable changes to `natsio-sysclient` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
is pre-1.0 and makes no API-stability promises.

## 0.1.0

Initial release. Client for the NATS system/monitoring API (`$SYS.REQ.SERVER`),
mirroring [`orbit.go/natssysclient`](https://github.com/synadia-io/orbit.go/tree/main/natssysclient).
Verified against nats-server 2.14.3. Stdlib only.

### Added

- **`SysClient(nc, options=SysClientOptions(...))`** over a `natsio.Client`
  connected with system-account credentials. Both oracle request shapes for
  every endpoint:
  - by server id — `$SYS.REQ.SERVER.<id>.<ENDPOINT>`, one typed response;
  - cluster ping — `$SYS.REQ.SERVER.PING.<ENDPOINT>`, scatter-gather over
    `Client.request_many`, bounded by `timeout`, an optional `stall` gap
    *between* replies (the first reply always gets the full `timeout`, matching
    `natsext.RequestMany`), and an optional expected `server_count` (per-call
    overridable). `DEFAULT_STALL` is the oracle's 300 ms.
- **Endpoints**: `varz`/`varz_ping`, `statsz`/`statsz_ping`,
  `healthz`/`healthz_ping`, `connz`/`connz_ping`, `subsz`/`subsz_ping`,
  `jsz`/`jsz_ping` — each with its own options dataclass (`VarzOptions`,
  `StatszOptions`, `HealthzOptions`, `ConnzOptions`, `SubszOptions`,
  `JszOptions`) and typed response model.
- **Paged endpoints as async iterators**: `all_connz`, `all_subsz`, `all_jsz`
  walk `offset` to `total` (for `JSZ`, `account_details` against `accounts`);
  `all_connz_ping`, `all_subsz_ping`, `all_jsz_ping` return one independent
  pager per responding server, for concurrent consumption. Pagers are lazy,
  tolerate `await` as a no-op, and support `aclose()` / `async with`. They are
  single-pass and single-consumer: misuse raises `PagerStateError` instead of
  silently yielding zero pages. Negative starting offsets are clamped to 0.
- **Response models are `JsonModel`s** (`Varz`, `Connz`/`ConnInfo`,
  `JSInfo`/`AccountDetail`/`StreamDetail`, `Healthz`, `ServerStats`, `Subsz`,
  plus shared `ServerInfo`, `JetStreamVarz`, `SubDetail`, …), so fields a newer
  server adds round-trip through `extra` instead of being dropped. Durations
  decode from nanoseconds to `timedelta`, timestamps to aware `datetime`.
- **Typed errors** under `SysClientError` (a `natsio.errors.NATSError`):
  `SysValidationError` (including a pre-flight guard against subject injection
  through a server id, and the same non-positive-timeout bound on per-call
  overrides as on `SysClientOptions`), `InvalidServerIDError` (no responders on
  a by-id request), `NoResponsesError` (a ping that gathered nothing — never an
  empty list; the message names the bound that fired and the elapsed time),
  `SysAPIError` (the server's `error` envelope, with `code`/`err_code`/
  `api_description`), `InvalidResponseError` (payload was not a JSON object, or
  its `error` block was malformed), `PagerStateError` (a pager reused, shared
  or closed out of order). *Envelope* decoding is a hard boundary — a non-JSON
  body, a non-object body, or a malformed `error` block is a typed
  `InvalidResponseError`, never a raw `ValueError`. Field-level decoding
  (`*.from_wire`) is not part of that boundary: a malformed `data` block
  surfaces whatever `JsonModel` raises, which is core-wide behaviour.
- **Paging is complete only under a stable page order.** The `offset` walk
  (oracle parity) assumes each request returns entries in the same order. That
  holds for the default `CONNZ` `cid` sort (a total order); it does not for
  `SUBSZ` or a tie-prone `CONNZ` sort, where entries can duplicate or drop
  between pages. Documented on `all_connz`/`all_subsz` and in the README —
  prefer a single large page when the order is not total.
- **Tests**: unit tests for subject composition, request encoding (including
  the hyphenated `js-enabled-only` / `js-server-only` keys), envelope decoding
  and the paging walk against a stub connection; live tests against a real
  `nats-server` with a `$SYS` account and JetStream — both request shapes,
  `CONNZ` with real connections and subscription detail, `SUBSZ` with the
  test-subject filter, `JSZ` with accounts/streams/config, `HEALTHZ` success and
  detailed failure, `STATSZ`, paged iteration for all three paged endpoints, and
  every typed error path.

### Divergences from the oracle (all verified live, all documented in the README)

- `HealthzError.type` is a `str`, not an `int`: nats-server 2.14.3 marshals the
  category as an upper-case string, which orbit.go's `HealthZErrorType int`
  cannot decode. `HealthzErrorType` is a `StrEnum` of the four spellings that
  could be provoked (`BAD_REQUEST`, `ACCOUNT`, `STREAM`, `CONSUMER`).
- Unset request options are omitted rather than emitted as zero values; the
  server's decoder defaults a missing key to the same zero.
- `JszOptions(account=...)` yields a single page. nats-server ignores `offset`
  under that filter but still reports `accounts` as the count of *all*
  JS-enabled accounts, so the oracle's `AllJsz` loop re-requests and re-yields
  the same account once per JS account (measured: 5 accounts → 5 `JSZ` round
  trips, 5 identical pages, on one of the most expensive monitoring endpoints).
- `"PING"` is rejected as a server id. The oracle interpolates it like any
  other string, which turns `varz("PING")` into a cluster ping that keeps only
  whichever reply arrives first.
- A `PagedResponses` is single-pass and single-consumer; the oracle's closures
  have no such guard.

### Known limitations (core seam friction)

Surfaced while building this extension from outside the core:

- **A connection closed mid-ping is now surfaced by core.** `Client.close()`
  wakes a parked `request_many` with a closure sentinel, and core now raises
  `ConnectionClosedError` at that seam instead of ending the stream silently —
  so a mid-ping close is a loud, correctly-typed error rather than an empty
  cluster. `_ping`'s local state check is retained only as a backstop and is
  restricted to `CLOSED`: a client that is merely `RECONNECTING` is healthy and
  recovering, so it no longer misreports the recoverable outage as a closure (it
  falls through to `NoResponsesError`, a `SysClientError` a poll loop can catch
  and retry).
- **`JsonModel` subclasses cannot use zero-argument `super()`.** Because
  `@dataclass(slots=True)` rebuilds the class, the implicit `__class__` cell
  points at the pre-decoration class and `super().to_wire()` raises
  `TypeError: obj is not an instance or subtype of type`. `HealthzOptions` calls
  `JsonModel.to_wire(self)` explicitly instead.
