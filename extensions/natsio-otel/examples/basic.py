"""natsio-otel: OpenTelemetry over the instrumentation seam.

`OtelInstrumentation` implements natsio's `Instrumentation` protocol and emits
metrics for connects, bytes, and messages. Pass it to `natsio.connect`; it uses
the global OpenTelemetry providers unless you hand it your own. This example
wires an in-memory metric reader (if the OTel SDK is installed) so you can see
the counters move — with only `opentelemetry-api` present it still runs, just
against the no-op global meter.

Run it (needs a server: `just server`):

    python extensions/natsio-otel/examples/basic.py
"""

import asyncio
import os

from natsio.otel import OtelInstrumentation  # ty: ignore[unresolved-import]

import natsio

try:
    from opentelemetry.metrics import set_meter_provider
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    _reader: "InMemoryMetricReader | None" = InMemoryMetricReader()
    set_meter_provider(MeterProvider(metric_readers=[_reader]))
except ImportError:  # only opentelemetry-api is installed, not the SDK
    _reader = None


async def main() -> None:
    url = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")

    async with await natsio.connect(url, instrumentation=OtelInstrumentation()) as nc:
        sub = await nc.subscribe("telemetry.demo")
        for i in range(100):
            await nc.publish("telemetry.demo", f"msg {i}".encode())
        await nc.flush()
        # Drain what we published so delivery metrics move too.
        seen = 0
        async with asyncio.timeout(5):
            async for _ in sub:
                seen += 1
                if seen >= 100:
                    break
        print(f"published + received {seen} messages")

    metrics = _reader.get_metrics_data() if _reader is not None else None
    if metrics is not None:
        print("\ncollected OpenTelemetry metrics:")
        for rm in metrics.resource_metrics:
            for sm in rm.scope_metrics:
                for metric in sm.metrics:
                    points = list(metric.data.data_points)
                    total = sum(getattr(p, "value", 0) for p in points)
                    print(f"  {metric.name}: {total}")
    else:
        print("\n(install opentelemetry-sdk to see the emitted metrics)")


if __name__ == "__main__":
    asyncio.run(main())
