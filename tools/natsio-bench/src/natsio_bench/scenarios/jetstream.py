"""JetStream scenarios: stream publish/consume, KV, and Object Store.

Every scenario names its stream/bucket with a per-call unique suffix so the
runner's repeats never collide or read each other's data (each repeat re-fills
its own stream). Clients that lack JetStream (nats-core) declare no such
capability and the runner skips these entirely.
"""

from time import perf_counter

from natsio_bench.adapters import Adapter, Capability
from natsio_bench.adapters.util import unique
from natsio_bench.scenarios.base import (
    JS_PAYLOAD,
    KV_VALUE,
    OS_SIZE_FULL,
    OS_SIZE_QUICK,
    BenchConfig,
    Result,
    count,
    register,
    warmup_count,
)
from natsio_bench.stats import mb_per_s, msgs_per_s

# -- stream publish (awaited acks) -------------------------------------------


@register("js_publish_sync", capability=Capability.JETSTREAM, group="jetstream")
async def js_publish_sync(adapter: Adapter, _url: str, config: BenchConfig) -> Result:
    """Publish to a file-storage stream, awaiting each PubAck."""
    stream = unique("bench_pubsync")
    subject = f"{stream}.s"
    timed_n = count(config, 2_000, 200)
    warm_n = warmup_count(config, timed_n)
    await adapter.js_create_stream(stream, [subject])

    for _ in range(warm_n):
        await adapter.js_publish(subject, JS_PAYLOAD)
    start = perf_counter()
    for _ in range(timed_n):
        await adapter.js_publish(subject, JS_PAYLOAD)
    elapsed = perf_counter() - start
    return Result(value=msgs_per_s(timed_n, elapsed), unit="msgs/s", ops=timed_n, seconds=elapsed)


# -- stream publish (async ack window) ---------------------------------------


@register("js_publish_async", capability=Capability.JETSTREAM, group="jetstream")
async def js_publish_async(adapter: Adapter, _url: str, config: BenchConfig) -> Result:
    """Fill the async-publish ack window, then wait for all acks to land."""
    stream = unique("bench_pubasync")
    subject = f"{stream}.s"
    timed_n = count(config, 50_000, 2_000)
    warm_n = warmup_count(config, timed_n)
    await adapter.js_create_stream(stream, [subject])

    for _ in range(warm_n):
        await adapter.js_publish_async(subject, JS_PAYLOAD)
    await adapter.js_publish_async_complete()

    start = perf_counter()
    for _ in range(timed_n):
        await adapter.js_publish_async(subject, JS_PAYLOAD)
    await adapter.js_publish_async_complete()
    elapsed = perf_counter() - start
    return Result(value=msgs_per_s(timed_n, elapsed), unit="msgs/s", ops=timed_n, seconds=elapsed)


# -- stream consume ----------------------------------------------------------


@register("js_consume", capability=Capability.JETSTREAM, group="jetstream")
async def js_consume(adapter: Adapter, _url: str, config: BenchConfig) -> Result:
    """Pre-fill a stream, then pull-consume and ack every message.

    Consumer creation is a control-plane round-trip, so it is built *before* the
    clock starts; a warmup drain then primes the fetch path. Only the steady-state
    draining of ``timed_n`` messages is timed — never the one-off setup.
    """
    stream = unique("bench_consume")
    subject = f"{stream}.s"
    timed_n = count(config, 50_000, 2_000)
    warm_n = warmup_count(config, timed_n)
    await adapter.js_create_stream(stream, [subject])

    # Pre-fill (untimed) via the fast async window: enough for the warmup drain
    # plus the timed drain.
    for _ in range(warm_n + timed_n):
        await adapter.js_publish_async(subject, JS_PAYLOAD)
    await adapter.js_publish_async_complete()

    # Build the consumer and warm the fetch path — both outside the timed window.
    consumer = await adapter.js_consumer(stream, subject)
    await adapter.js_fetch(consumer, warm_n)

    start = perf_counter()
    consumed = await adapter.js_fetch(consumer, timed_n)
    elapsed = perf_counter() - start
    return Result(value=msgs_per_s(consumed, elapsed), unit="msgs/s", ops=consumed, seconds=elapsed)


# -- key-value ---------------------------------------------------------------


@register("kv_put", capability=Capability.KV, group="jetstream")
async def kv_put(adapter: Adapter, _url: str, config: BenchConfig) -> Result:
    """Repeatedly put one key (history 1); report put ops/s."""
    bucket = unique("benchkv")
    key = "bench"
    timed_n = count(config, 10_000, 1_000)
    warm_n = warmup_count(config, timed_n)
    await adapter.kv_create(bucket)

    for _ in range(warm_n):
        await adapter.kv_put(key, KV_VALUE)
    start = perf_counter()
    for _ in range(timed_n):
        await adapter.kv_put(key, KV_VALUE)
    elapsed = perf_counter() - start
    return Result(value=msgs_per_s(timed_n, elapsed), unit="ops/s", ops=timed_n, seconds=elapsed)


@register("kv_get", capability=Capability.KV, group="jetstream")
async def kv_get(adapter: Adapter, _url: str, config: BenchConfig) -> Result:
    """Direct-get one key repeatedly; report get ops/s."""
    bucket = unique("benchkv")
    key = "bench"
    timed_n = count(config, 10_000, 1_000)
    warm_n = warmup_count(config, timed_n)
    await adapter.kv_create(bucket)
    await adapter.kv_put(key, KV_VALUE)

    for _ in range(warm_n):
        await adapter.kv_get(key)
    start = perf_counter()
    for _ in range(timed_n):
        await adapter.kv_get(key)
    elapsed = perf_counter() - start
    return Result(value=msgs_per_s(timed_n, elapsed), unit="ops/s", ops=timed_n, seconds=elapsed)


# -- object store ------------------------------------------------------------


@register("os_roundtrip", capability=Capability.OBJECT_STORE, group="jetstream")
async def os_roundtrip(adapter: Adapter, _url: str, config: BenchConfig) -> Result:
    """Put then get one large object; report MB/s over both directions."""
    bucket = unique("benchobj")
    size = OS_SIZE_QUICK if config.quick else OS_SIZE_FULL
    data = bytes(size)  # one shared buffer; identical bytes for every client
    await adapter.os_create(bucket)

    # Warmup: a full put+get so chunk subscriptions / consumers are warm.
    await adapter.os_put("warm", data)
    await adapter.os_get("warm")

    start = perf_counter()
    await adapter.os_put("obj", data)
    mid = perf_counter()
    got = await adapter.os_get("obj")
    end = perf_counter()
    if len(got) != size:
        raise RuntimeError(f"object round-trip corrupted: got {len(got)} bytes, expected {size}")

    total = end - start
    return Result(
        value=mb_per_s(2 * size, total),
        unit="MB/s",
        detail={
            "put_MB_per_s": mb_per_s(size, mid - start),
            "get_MB_per_s": mb_per_s(size, end - mid),
        },
        ops=1,
        seconds=total,
    )
