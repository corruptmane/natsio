# natsio-sysclient

Client for the NATS **system/monitoring API** — the `$SYS.REQ.SERVER` endpoints
a connection in the system account can ask about any server in the cluster.
A port of [orbit.go's `natssysclient`](https://github.com/synadia-io/orbit.go/tree/main/natssysclient),
with the same two request shapes, the same options, and the same paging model.

```bash
pip install natsio-sysclient
```

Zero runtime dependencies beyond `natsio` itself.

## Usage

```python
import natsio
from natsio.sysclient import SysClient, ConnzOptions, JszOptions, SysClientOptions

nc = await natsio.connect("nats://localhost:4222", user="sys", password="pw")
sys = SysClient(nc)

# --- cluster ping: every server that answers ---------------------------------
for varz in await sys.varz_ping():
    print(varz.server.id, varz.data.version, varz.data.connections)

# --- by server id: one named server ------------------------------------------
server_id = (await sys.varz_ping())[0].server.id
health = await sys.healthz(server_id)
assert health.data.status == "ok"

stats = await sys.statsz(server_id)          # NOTE: payload lives under .statsz
print(stats.statsz.sent.msgs, stats.statsz.received.bytes)

# --- paged endpoints: an async iterator that walks offset -> total ------------
async for page in sys.all_connz(server_id, ConnzOptions(limit=64, auth=True)):
    for conn in page.data.connections:
        print(conn.cid, conn.account, conn.authorized_user, conn.rtt)

# --- one pager per server, consumable concurrently ---------------------------
for pager in await sys.all_jsz_ping(JszOptions(accounts=True, limit=10)):
    async for page in pager:
        for account in page.data.account_details or []:
            print(page.server.id, account.name, account.storage)
```

Bounding a cluster ping — the knobs are independent and whichever fires first
ends the gather:

```python
sys = SysClient(nc, options=SysClientOptions(
    timeout=5.0,        # overall deadline; the FIRST reply always gets all of it
    stall=0.3,          # ...then stop after a 300 ms gap with no new reply
    server_count=3,     # ...or stop as soon as 3 servers have answered
))
await sys.varz_ping(server_count=5)   # per-call override
```

`stall` bounds only the gap *between* replies (nats.go `natsext.RequestMany`
parity), so the 300 ms default never penalises a slow cluster — it just stops
waiting once the cluster has gone quiet. A ping that collects nothing at all
therefore only ever ran out of `timeout`, or found no responders; both are
`NoResponsesError`, and the message says which.

Two things a ping deliberately does *not* do, matching the oracle's
`pingServers`:

- **one bad server fails the whole call.** If any responder answers with an
  `error` envelope, `SysAPIError` is raised and the healthy servers' data is
  discarded. Use the by-id shape per server if you need partial results.
- **a short gather is silent.** `server_count` is an upper bound, not a
  contract: if only 3 of 5 servers answer before the stall, you get 3
  responses and no signal. Compare `len(...)` against your expected size if
  that matters.

## What's covered

| Endpoint | by id | cluster ping | paged iterator |
|---|---|---|---|
| `VARZ` | `varz()` | `varz_ping()` | — |
| `STATSZ` | `statsz()` | `statsz_ping()` | — |
| `HEALTHZ` | `healthz()` | `healthz_ping()` | — |
| `CONNZ` | `connz()` | `connz_ping()` | `all_connz()` / `all_connz_ping()` |
| `SUBSZ` | `subsz()` | `subsz_ping()` | `all_subsz()` / `all_subsz_ping()` |
| `JSZ` | `jsz()` | `jsz_ping()` | `all_jsz()` / `all_jsz_ping()` |

Each endpoint has its own options dataclass (`VarzOptions`, `ConnzOptions`,
`JszOptions`, `HealthzOptions`, `StatszOptions`, `SubszOptions`) and its own
typed response model.

**Responses are `JsonModel`s.** Fields a newer server adds that this package
does not name are kept in `extra` and re-emitted on `to_wire()`, so upgrading
the server never silently drops data:

```python
varz = await sys.varz(server_id)
varz.server.extra["feature_flags"]   # 2.14 addition, not in the oracle's structs
```

**Paging** is driven by the endpoint's own `offset`/`total`. `all_connz` and
`all_subsz` walk until the page is short or `total` is reached; `all_jsz` pages
`account_details` against the `accounts` total and short-circuits to a single
page unless `JszOptions(accounts=True)` is set — the same rule the oracle's
`AllJsz` applies — and also to a single page when `JszOptions(account=...)`
names one account, because nats-server ignores `offset` for that filter while
still reporting the *unfiltered* `accounts` count (see Wire contract below).
A negative starting `offset` is clamped to 0, the way the server clamps it.

> **Paging is only complete when the page order is stable across requests.**
> `offset`-based walking (which the oracle's `AllConnz`/`AllServerSubsz` do
> identically) assumes each request returns entries in the same order. That
> holds for the default `CONNZ` sort (by `cid`, a total order), and the live
> suite exercises exactly that. It does **not** hold for `SUBSZ` (whose order is
> unspecified) or for a `CONNZ` sort whose key has ties: entries can be
> duplicated or skipped between pages, with no error. Treat `all_subsz` and
> tie-prone `CONNZ` sorts as best-effort; for a guaranteed-complete walk use the
> default `CONNZ` ordering, or a single large page.

Pagers are lazy (nothing is requested until you iterate), support `await` as a
no-op, and can be closed early with `aclose()` or `async with`. They are
**single-pass and single-consumer**: re-iterating a spent pager, advancing one
from two tasks at once, or closing one mid-advance raises `PagerStateError`
rather than quietly yielding nothing.

## Errors

Everything derives from `SysClientError` (itself a `natsio.errors.NATSError`).
Nothing fails quietly:

| Error | When |
|---|---|
| `SysValidationError` | empty/dotted/wildcard server id, `"PING"` used as a server id, non-positive timeout (client-wide **or** per-call), stall or server count — raised before any I/O |
| `InvalidServerIDError` | no responders on `$SYS.REQ.SERVER.<id>.<ENDPOINT>` — unknown id, or the connection is not in `$SYS` |
| `NoResponsesError` | a cluster ping collected **zero** responses (an empty list would be indistinguishable from a zero-server cluster); the message names the bound that fired and the elapsed time |
| `SysAPIError` | the server answered with an `error` envelope; carries `code`, `err_code`, `api_description` |
| `InvalidResponseError` | the response payload was not a JSON object, or its `error` block was malformed (non-object, non-integer `code`/`err_code`) |
| `PagerStateError` | a `PagedResponses` was re-iterated, shared between tasks, or closed mid-advance |

`natsio.errors.TimeoutError` and `ConnectionClosedError` pass through
unwrapped — re-typing them would hide which one happened.

A *failed health check* is deliberately not an error: `healthz()` returns
`status="error"` with `status_code` and `errors` as data, because the request
itself succeeded.

## Wire contract

Subjects, JSON field names and option keys are pinned to orbit.go
`natssysclient` (`api.go`, `varz.go`, `connz.go`, `jsz_server.go`, `healthz.go`,
`statsz_server.go`, `subsz_server.go`) and re-verified against nats-server
2.14.3.

- `$SYS.REQ.SERVER.<id>.<ENDPOINT>` and `$SYS.REQ.SERVER.PING.<ENDPOINT>`, with
  `ENDPOINT` one of `VARZ`, `CONNZ`, `JSZ`, `HEALTHZ`, `STATSZ`, `SUBSZ`.
- Envelopes are `{"server": …, "data": …}` — except `STATSZ`, whose payload key
  is `statsz`, and any response may carry `error` instead.
- Go `time.Duration` fields arrive as integer nanoseconds and decode to
  `timedelta`; `time.Time` fields decode to aware `datetime`. `uptime`, `rtt`
  and `idle` are the server's own pre-formatted **strings** and stay strings.
- Go's embedded `JetStreamStats` (in `JSZ`) and `SublistStats` (in `SUBSZ`)
  are inlined at the top level on the wire, so they are spelled out flat here.

Deliberate departures, none of which changes what a *correct* request looks
like on the wire:

1. **Unset options are omitted.** Several of the oracle's structs lack
   `omitempty` and therefore emit their zero values (`{"sort":"","auth":false,
   "offset":0,…}`). A missing key decodes to exactly that zero server-side, so
   the payloads are equivalent.
2. **`JszOptions(account=...)` is not paged.** nats-server ignores `offset`
   when `account` is set, but still reports `accounts` as the count of *all*
   JS-enabled accounts — the very number the oracle's `AllJsz` walks to. On a
   server with N JS accounts, the oracle's loop therefore issues N `JSZ`
   requests and yields the same single account N times. `all_jsz` /
   `all_jsz_ping` yield one page instead. (Bug inherited from the oracle,
   fixed here; verified against 2.14.3 with five JS accounts.)
3. **`"PING"` is rejected as a server id.** The oracle interpolates it into the
   by-id subject like any other string, which silently turns `varz("PING")`
   into a cluster ping that keeps only the first reply. Here it is a
   `SysValidationError`; use `varz_ping()`.
4. **`HealthzError.type` is a `str`, not an enum.** The oracle declares
   `HealthZErrorType` as an `int`; nats-server 2.14.3 marshals it as an
   upper-case string (`"BAD_REQUEST"`, `"ACCOUNT"`, `"STREAM"`, `"CONSUMER"` —
   all four verified live), which the Go struct cannot decode at all. The
   `HealthzErrorType` `StrEnum` lists the verified values for comparison, but
   the field stays `str` so an unknown category from a newer server is data,
   not a decode failure.

## Scope limits

- **Server endpoints only.** The account-scoped `$SYS.REQ.ACCOUNT.*` family
  (`STATZ`, `CONNZ`, `SUBSZ`, `JSZ`, `INFO`) and `$SYS.REQ.USER.INFO` are out of
  scope, matching the oracle. So are `ROUTEZ`, `GATEWAYZ`, `LEAFZ` and
  `ACCOUNTZ`, which the oracle also does not model.
- **No cluster-shaped tests.** The suite runs a single server, so the ping path
  is proven to return exactly one response whose id matches the by-id shape;
  multi-server gather (`server_count` > 1, stall short-circuiting across
  servers) is exercised only in its degenerate one-server form. The slow-reply
  and mute-reply paths are covered with a stand-in responder in a regular
  account, where `$SYS.REQ.SERVER.PING.VARZ` is an ordinary subject.
- **A connection closed mid-ping can still surface as `NoResponsesError`.**
  `Client.close()` closes its request sinks *before* it advances the connection
  state, so at the instant the gather ends the connection can still read
  `CONNECTED` and the closure is indistinguishable from "nobody answered". The
  wake path itself is correct — the ping unparks in milliseconds, it never
  hangs to the deadline — and an already-closed or reconnecting connection does
  raise `ConnectionClosedError`. Removing the residual window needs
  `Client.request_many` to end its stream distinguishably on closure.
- **Operator JWT claims are raw JSON.** `Varz.trusted_operators_claim` is a
  `list[dict]`; decoding JWTs would mean a dependency, and this package has
  none.
- **`HealthzErrorType.CONNECTION` / `.JETSTREAM` are absent.** The oracle names
  two more categories whose string spelling could not be provoked on 2.14.3;
  guessing them would be an unverified wire claim. They still decode fine — as
  their raw string.

## Tests

```bash
uv run pytest extensions/natsio-sysclient -q
```

Live tests need `tools/.bin/nats-server` (or `NATS_SERVER_BIN`) and start it
with a `$SYS` account plus a JetStream-enabled `APP` account.
