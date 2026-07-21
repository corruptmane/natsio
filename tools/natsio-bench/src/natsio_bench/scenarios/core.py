"""Core pub/sub and request/reply scenarios (no JetStream)."""

import asyncio
from time import perf_counter
from typing import Any

from natsio_bench.adapters import Adapter, Capability
from natsio_bench.scenarios.base import (
    PAYLOAD_1K,
    PAYLOAD_16B,
    PAYLOAD_64K,
    BenchConfig,
    Result,
    count,
    register,
    warmup_count,
)
from natsio_bench.stats import latency_percentiles, mb_per_s, msgs_per_s

_REQUEST_TIMEOUT = 5.0


# -- publish throughput ------------------------------------------------------
#
# Measures enqueue + flush completion, NOT fire-and-forget: the clock stops only
# after the client's own flush() has round-tripped a PING/PONG, so every byte is
# on the socket and acknowledged by the server before we compute the rate.


async def _pub_throughput(adapter: Adapter, payload: bytes, timed_n: int, warm_n: int) -> tuple[float, int]:
    subject = "bench.pub"
    for _ in range(warm_n):
        await adapter.publish(subject, payload)
    await adapter.flush()

    start = perf_counter()
    for _ in range(timed_n):
        await adapter.publish(subject, payload)
    await adapter.flush()
    elapsed = perf_counter() - start
    return elapsed, timed_n


def _make_pub(name: str, payload: bytes, full: int, quick: int, *, as_mb: bool) -> None:
    @register(name, capability=Capability.CORE, group="core")
    async def scenario(adapter: Adapter, _url: str, config: BenchConfig) -> Result:
        timed_n = count(config, full, quick)
        elapsed, done = await _pub_throughput(adapter, payload, timed_n, warmup_count(config, timed_n))
        detail = {
            "msgs_per_s": msgs_per_s(done, elapsed),
            "MB_per_s": mb_per_s(done * len(payload), elapsed),
        }
        if as_mb:
            return Result(value=detail["MB_per_s"], unit="MB/s", detail=detail, ops=done, seconds=elapsed)
        return Result(value=detail["msgs_per_s"], unit="msgs/s", detail=detail, ops=done, seconds=elapsed)


_make_pub("pub_16b", PAYLOAD_16B, full=300_000, quick=10_000, as_mb=False)
_make_pub("pub_1k", PAYLOAD_1K, full=200_000, quick=10_000, as_mb=False)
_make_pub("pub_64k", PAYLOAD_64K, full=20_000, quick=1_000, as_mb=True)


# -- pub/sub delivery --------------------------------------------------------
#
# One publisher + one subscriber on the SAME connection (echo is on by default,
# so the client receives what it publishes). The clock stops at the Nth received
# message via an Event set inside the delivery callback — never by polling.


async def _pubsub_delivery(adapter: Adapter, payload: bytes, timed_n: int, warm_n: int) -> tuple[float, int]:
    subject = "bench.delivery"
    state = {"received": 0, "target": 0}
    done = asyncio.Event()

    def on_msg(_msg: object) -> None:
        state["received"] += 1
        if state["received"] >= state["target"]:
            done.set()

    sub = await adapter.subscribe(subject, on_msg)
    try:

        async def run_round(n: int) -> float:
            state["received"] = 0
            state["target"] = n
            done.clear()

            async def publish_all() -> None:
                for _ in range(n):
                    await adapter.publish(subject, payload)
                await adapter.flush()

            start = perf_counter()
            publisher = asyncio.create_task(publish_all())
            await done.wait()
            elapsed = perf_counter() - start
            await publisher
            return elapsed

        await run_round(warm_n)
        elapsed = await run_round(timed_n)
    finally:
        await sub.unsubscribe()
    return elapsed, timed_n


def _make_pubsub(name: str, payload: bytes, full: int, quick: int) -> None:
    @register(name, capability=Capability.CORE, group="core")
    async def scenario(adapter: Adapter, _url: str, config: BenchConfig) -> Result:
        timed_n = count(config, full, quick)
        elapsed, done = await _pubsub_delivery(adapter, payload, timed_n, warmup_count(config, timed_n))
        return Result(
            value=msgs_per_s(done, elapsed),
            unit="msgs/s",
            detail={"MB_per_s": mb_per_s(done * len(payload), elapsed)},
            ops=done,
            seconds=elapsed,
        )


_make_pubsub("pubsub_16b", PAYLOAD_16B, full=200_000, quick=10_000)
_make_pubsub("pubsub_1k", PAYLOAD_1K, full=150_000, quick=8_000)


# -- round-trip latency ------------------------------------------------------


@register("roundtrip_latency", capability=Capability.CORE, group="core")
async def roundtrip_latency(adapter: Adapter, _url: str, config: BenchConfig) -> Result:
    """Ping-pong echo over the client; report p50/p90/p99/max in milliseconds."""
    subject = "bench.echo"
    payload = PAYLOAD_16B
    timed_n = count(config, 2_000, 500)
    warm_n = warmup_count(config, timed_n)

    async def echo(msg: Any) -> None:
        await adapter.publish(msg.reply, msg.data)

    sub = await adapter.subscribe(subject, echo)
    try:
        for _ in range(warm_n):
            await adapter.request(subject, payload, _REQUEST_TIMEOUT)
        samples_ms: list[float] = []
        for _ in range(timed_n):
            start = perf_counter()
            await adapter.request(subject, payload, _REQUEST_TIMEOUT)
            samples_ms.append((perf_counter() - start) * 1000)
    finally:
        await sub.unsubscribe()

    pcts = latency_percentiles(samples_ms)
    total = sum(samples_ms) / 1000
    return Result(value=pcts["p50"], unit="ms", higher_is_better=False, detail=pcts, ops=timed_n, seconds=total)


# -- request/reply throughput ------------------------------------------------


@register("reqrep_throughput", capability=Capability.CORE, group="core")
async def reqrep_throughput(adapter: Adapter, _url: str, config: BenchConfig) -> Result:
    """Echo responder + K concurrent requesters; report req/s."""
    subject = "bench.rr"
    payload = PAYLOAD_16B
    workers = count(config, 32, 8)
    timed_n = count(config, 30_000, 3_000)
    warm_n = warmup_count(config, timed_n)

    async def echo(msg: Any) -> None:
        await adapter.publish(msg.reply, msg.data)

    sub = await adapter.subscribe(subject, echo)
    try:
        for _ in range(warm_n):
            await adapter.request(subject, payload, _REQUEST_TIMEOUT)

        remaining = timed_n

        async def worker() -> None:
            nonlocal remaining
            while remaining > 0:
                remaining -= 1  # claim before awaiting so K workers never over-issue
                await adapter.request(subject, payload, _REQUEST_TIMEOUT)

        start = perf_counter()
        await asyncio.gather(*(worker() for _ in range(workers)))
        elapsed = perf_counter() - start
    finally:
        await sub.unsubscribe()

    return Result(
        value=msgs_per_s(timed_n, elapsed),
        unit="req/s",
        detail={"workers": float(workers)},
        ops=timed_n,
        seconds=elapsed,
    )
