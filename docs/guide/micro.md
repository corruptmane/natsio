# Microservices

Request/reply is the primitive; a **service** is the pattern built on top. The
micro framework (ADR-32) wraps a set of request handlers with a name, a version,
and a standard `$SRV` control plane — so any instance can be discovered, described,
and monitored over NATS itself, with no sidecar and no HTTP. It is the same
protocol nats.go, nats.rs, and the `nats` CLI speak, so a natsio service is a
first-class citizen of a polyglot mesh.

```python
import natsio
from natsio.micro import Request, add_service


async def add(req: Request) -> None:
    a, b = req.data.split(b"+")
    await req.respond(str(int(a) + int(b)).encode())


async with await natsio.connect("nats://localhost:4222") as nc:
    svc = add_service(nc, name="calc", version="1.0.0")
    svc.add_endpoint("add", add)
    async with svc:            # $SRV responders live for the block
        await svc.stopped      # run until stopped
```

[`add_service`][natsio.micro.add_service] starts the instance immediately: it
takes a unique id and begins answering `$SRV.PING`/`INFO`/`STATS` before you
register a single endpoint. Pass a [`ServiceConfig`][natsio.micro.ServiceConfig]
or its fields as keywords — `name` and `version` are required and validated
loudly (a bad name or a non-SemVer version raises
[`ServiceConfigError`][natsio.micro.ServiceConfigError] on the spot).

## Endpoints and handlers

An endpoint is a named handler bound to a subject. A handler is
`async def handler(req: Request) -> None`; it receives a
[`Request`][natsio.micro.Request] and replies exactly once.

```python
async def handler(req: Request) -> None:
    print(req.subject, req.headers)      # what arrived
    await req.respond(b"result")         # a successful reply
```

The subject defaults to the endpoint name; override it with `subject=`. `req.data`
(aliased `req.payload`) is the request bytes, `req.headers` the request headers,
and `req.reply` the inbox to answer on.

### Reporting errors

A handler signals failure with
[`respond_error`][natsio.micro.Request.respond_error] instead of `respond`. It
sets two headers on the reply — `Nats-Service-Error` (the description) and
`Nats-Service-Error-Code` (the code) — and counts the request as an error in the
endpoint's stats.

```python
async def divide(req: Request) -> None:
    a, b = req.data.split(b"/")
    if int(b) == 0:
        await req.respond_error("400", "division by zero")
        return
    await req.respond(str(int(a) // int(b)).encode())
```

The caller sees the error headers on an otherwise ordinary reply:

```python
reply = await nc.request("divide", b"1/0")
reply.headers["Nats-Service-Error"]        # "division by zero"
reply.headers["Nats-Service-Error-Code"]   # "400"
```

!!! note "An uncaught exception becomes a 500"
    If a handler raises, the framework never lets the fault kill the
    subscription: it sends a best-effort `respond_error("500", str(exc))` (unless
    the handler already responded), counts the error, and records it as the
    endpoint's `last_error`. A handler that deliberately called `respond_error`
    *before* raising keeps **its** code and description — the raise is secondary.
    Configure a `ServiceConfig(error_handler=...)` to observe every fault:

    ```python
    async def on_error(service, error) -> None:
        log.warning("micro error on %s: %s", error.endpoint, error)

    svc = add_service(nc, ServiceConfig(name="calc", version="1.0.0", error_handler=on_error))
    ```

## Groups and queue groups

A **group** is a dotted subject prefix. Endpoints registered on it inherit the
prefix; groups nest.

```python
svc = add_service(nc, name="calc", version="1.0.0")

math = svc.add_group("math")
math.add_endpoint("add", add)                        # subject "math.add"
math.add_group("adv").add_endpoint("pow", power)     # subject "math.adv.pow"
```

Every request endpoint is subscribed with a **queue group** (default `"q"`,
overridable per service, group, or endpoint). That is what makes a service
horizontally scalable: run *N* identical instances and the server load-balances
each request to exactly one of them.

```python
# override the shared queue group for one subtree
workers = svc.add_group("jobs", queue_group="job-workers")
```

The monitoring endpoints are deliberately **queue-group-less**. Discovery must
reach *every* instance — `nats micro list` needs to see all three replicas, not
whichever one the server happened to pick — so `$SRV.PING`/`INFO`/`STATS` are
plain subscriptions and every instance answers. Request endpoints load-balance;
monitoring fans out. That asymmetry is the whole point of the split.

## Discovery: the `$SRV` control plane

Each instance answers three verbs on three subject variants — bare (all
services), `.<name>` (all instances of a service), and `.<name>.<id>` (one
specific instance):

| Verb | Answers with |
|---|---|
| `$SRV.PING` | Liveness + identity (name, id, version, metadata). |
| `$SRV.INFO` | The above plus description and the endpoint list. |
| `$SRV.STATS` | Per-endpoint counters and timings. |

A plain request to any of these returns JSON. `$SRV.INFO` for the `calc` service
above:

```json
{
  "name": "calc",
  "id": "ackZMkR8yN3eApZFVuqX8K",
  "version": "1.0.0",
  "metadata": {},
  "type": "io.nats.micro.v1.info_response",
  "description": "arithmetic service",
  "endpoints": [
    { "name": "add", "subject": "math.add", "queue_group": "q", "metadata": {} },
    { "name": "pow", "subject": "math.adv.pow", "queue_group": "q", "metadata": {} }
  ]
}
```

The `type` discriminators (`io.nats.micro.v1.ping_response`, `…info_response`,
`…stats_response`) and every field name are fixed by ADR-32 and match nats.go
byte-for-byte. To collect replies from a whole fleet, use `request_many`:

```python
async for msg in nc.request_many("$SRV.PING.calc", b"", max_msgs=10, timeout=1.0):
    print(json.loads(msg.data)["id"])     # one line per live instance
```

## Statistics

`$SRV.STATS` reports one [`EndpointStats`][natsio.micro.EndpointStats] block per
endpoint. The counters are maintained on the subscription's own task, so they are
always consistent:

```json
{
  "name": "add",
  "subject": "math.add",
  "queue_group": "q",
  "num_requests": 4,
  "num_errors": 0,
  "last_error": "",
  "processing_time": 23792,
  "average_processing_time": 5948
}
```

`processing_time` and `average_processing_time` are **integer nanoseconds**
(matching the Go `time.Duration` wire form) — total handler time and its mean
over `num_requests`. `num_errors` and `last_error` track failures, including the
automatic 500 path.

Attach a `stats_handler` to enrich each block with a custom, JSON-serializable
`data` field — a queue depth, a cache hit rate, anything:

```python
def stats_handler(endpoint) -> dict[str, int]:
    return {"in_flight": in_flight_for(endpoint.name)}

svc = add_service(nc, ServiceConfig(name="calc", version="1.0.0", stats_handler=stats_handler))
```

The same content is available locally, without a round-trip, as typed objects via
[`Service.info()`][natsio.micro.Service.info] and
[`Service.stats()`][natsio.micro.Service.stats];
[`Service.reset()`][natsio.micro.Service.reset] zeroes every counter and restarts
the `started` clock.

## Lifecycle

A [`Service`][natsio.micro.Service] is an async context manager: entering it is a
no-op (it is already running from `add_service`), and exiting **stops** it. Stop
is [`drain`](core-messaging.md)-based — in-flight requests finish before their
subscription closes — and idempotent.

```python
async with svc:
    await svc.stopped         # block until something stops the service
# here: svc.is_stopped is True, all $SRV interest is gone
```

Three surfaces let you wait for or trigger shutdown:

- [`svc.stopped`][natsio.micro.Service.stopped] — an `asyncio.Future` resolved
  when the service stops. `await` it to run until then; it is the idiomatic
  "serve forever" of a micro process.
- [`svc.stop()`][natsio.micro.Service.stop] — drain and stop explicitly.
- `async with svc:` — stop on block exit, exceptions included.

After a stop, adding an endpoint raises `ServiceConfigError`, and callers of the
service's subjects get a `NoRespondersError` — the interest is genuinely gone.

## Interop with the `nats` CLI

Because natsio answers the exact ADR-32 `$SRV` contract, the standard tooling
works against a natsio service unchanged:

```console
$ nats micro list
$ nats micro info calc
$ nats micro stats calc
```

The same is true of micro clients in other languages — they discover, describe,
and monitor a natsio service exactly as they would a nats.go one. natsio brings
nothing proprietary to the wire.

## See also

- [Core messaging](core-messaging.md) — request/reply, queue groups, and drain,
  the primitives micro is built from.
- [Microservices API reference](../reference/micro.md) — every field and method.
