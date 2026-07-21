"""Micro services (ADR-32): endpoints, groups, and ``$SRV`` monitoring.

``natsio.micro`` is request/reply with a discovery contract bolted on. You
register async handlers; the framework subscribes them (load-balanced across
instances via a shared queue group) and *also* answers three well-known
control subjects — ``$SRV.PING``, ``$SRV.INFO``, ``$SRV.STATS`` — with JSON
that every NATS ecosystem tool understands. That JSON is the whole point of
using micro over a bare subscription: ``nats micro`` on the CLI, dashboards,
and other-language clients can find, describe, and monitor your service
without knowing anything about it in advance.

This script demonstrates:

* ``add_service`` with name/version/description/metadata,
* a top-level endpoint that answers with ``req.respond``,
* a *nested group* endpoint — the dotted subject (``math.divide``) and the
  queue group it inherits from the service,
* ``req.respond_error`` and the two ``Nats-Service-Error*`` headers it sets,
  read back on the caller side,
* querying ``$SRV.PING`` / ``$SRV.STATS`` with a *plain* ``request`` and
  pretty-printing the JSON (this is exactly what ``nats micro`` speaks),
* ``service.stats()`` — the same numbers, as a typed object, in-process,
* the ``async with`` lifecycle (stop drains every subscription on exit).

Run it (start a server first with ``just server``)::

    python examples/11_micro_service.py
"""

import asyncio
import json
import os

import natsio
from natsio.micro import ERROR_CODE_HEADER, ERROR_HEADER, Request, add_service

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")


async def echo(req: Request) -> None:
    """A trivial success handler: reply with the request payload, uppercased.

    A handler is ``async def handler(req: Request) -> None`` and must respond
    exactly once. ``req.data`` is the payload (an alias for ``req.payload``).
    """
    await req.respond(req.data.decode().upper().encode())


async def divide(req: Request) -> None:
    """A handler that can *fail* deliberately, via ``respond_error``.

    ``respond_error`` sends a normal reply body but also sets two headers —
    ``Nats-Service-Error`` (the description) and ``Nats-Service-Error-Code`` —
    so the caller can distinguish a business error from a success without a
    side-channel. It also counts toward this endpoint's ``num_errors`` and
    becomes its ``last_error`` in ``$SRV.STATS``.
    """
    a, _, b = req.data.partition(b"/")
    if b == b"0":
        # A structured failure, not an exception. (An *un*caught exception would
        # also be turned into a 500 error reply — the service never dies — but
        # here we choose the code and message ourselves.)
        await req.respond_error("400", "division by zero")
        return
    await req.respond(str(int(a) // int(b)).encode())


def pretty(payload: bytes) -> str:
    """Decode a monitoring reply and re-dump it indented — the JSON is the API."""
    return json.dumps(json.loads(payload), indent=2)


async def main() -> None:
    async with await natsio.connect(NATS_URL) as nc:
        # `add_service` starts answering $SRV.PING/INFO/STATS immediately (even
        # before any endpoint exists). name + version are required and validated
        # loudly; metadata is free-form and surfaces in PING/INFO/STATS.
        svc = add_service(
            nc,
            name="calc",
            version="1.0.0",
            description="a tiny calculator service",
            metadata={"env": "example"},
        )
        # `async with` guarantees `stop()` runs on exit, draining every
        # subscription (endpoints *and* monitoring) so in-flight work finishes.
        async with svc:
            # A top-level endpoint: its subject is just its name, "echo".
            svc.add_endpoint("echo", echo)

            # A nested group gives endpoints a dotted subject prefix. The group
            # (and its endpoints) inherit the service's queue group ("q" by
            # default) unless overridden — so every instance of `calc` shares
            # the load on "math.divide", exactly like the bare "echo" endpoint.
            math = svc.add_group("math")
            div = math.add_endpoint("divide", divide)
            print(f"registered endpoint {div.name!r}: subject={div.subject!r} queue_group={div.queue_group!r}")

            # Flush so the SUB frames have reached the server before we act as
            # our own client below — otherwise the first request could race the
            # subscription and get a spurious NoRespondersError.
            await nc.flush()

            # -- call the service like any request/reply peer -----------------
            # There is nothing special about calling a micro service: it is a
            # plain `request`. The framework did the subscribing; the wire is
            # ordinary NATS.
            reply = await nc.request("echo", b"hello")
            print(f"echo('hello') -> {reply.data.decode()!r}")

            ok = await nc.request("math.divide", b"84/2")
            print(f"math.divide('84/2') -> {ok.data.decode()!r}")

            # -- the error path: same request, but headers tell the story -----
            bad = await nc.request("math.divide", b"1/0")
            assert bad.headers is not None  # respond_error always sets headers
            code = bad.headers.get(ERROR_CODE_HEADER)
            desc = bad.headers.get(ERROR_HEADER)
            print(f"math.divide('1/0') -> error {code} {desc!r} (body={bad.data!r})")

            # -- discovery: what `nats micro` sees ----------------------------
            # PING is the liveness probe. It has NO queue group, so in a scaled
            # deployment *every* instance answers (here there is one). The reply
            # is the ADR-32 JSON contract, byte-for-byte what the CLI parses.
            ping = await nc.request("$SRV.PING.calc", b"")
            print("\n$SRV.PING.calc ->")
            print(pretty(ping.data))

            # STATS aggregates per-endpoint counters. Note num_errors=1 on the
            # divide endpoint (our 1/0 call) and its last_error carrying the
            # "400:division by zero" we set above.
            stats = await nc.request("$SRV.STATS.calc", b"")
            print("\n$SRV.STATS.calc ->")
            print(pretty(stats.data))

            # -- the same numbers, in-process, as a typed object --------------
            # `service.stats()` returns the identical data without a round-trip
            # — handy for exporting metrics from inside the service.
            local = svc.stats()
            for ep in local.endpoints:
                print(
                    f"local stat: {ep.name:8} requests={ep.num_requests} errors={ep.num_errors} last={ep.last_error!r}"
                )

        # Outside the `async with`, the service is stopped: its subjects have no
        # responders anymore. A discovery probe now fast-fails.
        assert svc.is_stopped
        try:
            await nc.request("$SRV.PING.calc", b"", timeout=0.5)
        except natsio.NoRespondersError:
            print("\nafter stop: $SRV.PING.calc -> NoRespondersError (all interest drained)")

    print("done")


if __name__ == "__main__":
    asyncio.run(main())
