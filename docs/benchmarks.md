# Benchmarks

The in-repository harness compares **natsio**, **nats-py**, and **nats-core** on
stdlib asyncio. There is no Go adapter or Go measurement; these results make
no claim of nats.go performance parity.

## Method and environment

Each `(scenario, client)` pair uses a fresh JetStream-enabled localhost server.
Five sequential timed repeats follow each scenario’s warm-up; the table shows
the median and the observed `[minimum–maximum]`. Publish timings include the
client’s `flush`; delivery timings end on the expected message count. Adapters
use the respective client APIs. Unsupported adapter capabilities are `n/s`.

This is one machine and one run order, without network latency, TLS, replicated
storage, uvloop, or a randomized cross-run experiment. Ranges describe observed
variability, not confidence intervals; overlapping ranges do not prove a tie.
Other test workloads were kept idle during measurement.

Measured 2026-10-22: macOS-26.4.1-arm64-arm-64bit-Mach-O, 10 logical CPUs,
CPython 3.13.13; nats-server: v2.15.1; natsio 1.0.1,
nats-py 2.16.0, nats-core 0.2.0. The raw JSON also records
the base Git commit and dirty-tree status: this was the prepared 1.0.1 working
tree, before the owner’s release commit.

[Full raw repeats and environment](https://github.com/corruptmane/natsio/blob/main/tools/natsio-bench/results/2026-10-22-current.json).

## Results

Bold marks the best observed **median among all supported clients**, including
nats-core. Larger is better except for `roundtrip_latency`, where smaller wins.

| Scenario | Unit | natsio | nats-py | nats-core |
|---|---|---:|---:|---:|
| `pub_16b` | msgs/s | 1,354,657 [1,339,612–1,365,705] | 828,011 [822,907–845,942] | **2,019,080** [1,959,810–2,056,418] |
| `pub_1k` | msgs/s | 1,052,793 [1,030,513–1,062,969] | 929,869 [909,914–946,654] | **1,517,478** [1,480,253–1,553,491] |
| `pub_64k` | MB/s | 6,706.6 [6,045.2–6,733.6] | **7,318.3** [5,881.1–7,346.6] | 6,137.3 [4,490.7–6,911.8] |
| `pubsub_16b` | msgs/s | 300,910 [300,523–306,526] | 319,275 [299,065–322,124] | **341,225** [337,350–344,010] |
| `pubsub_1k` | msgs/s | 270,353 [252,327–271,367] | **294,252** [291,118–298,423] | 289,635 [245,455–294,520] |
| `roundtrip_latency` | ms | **0.168** [0.167–0.168] | 0.180 [0.178–0.185] | 5.935 [5.921–5.985] |
| `reqrep_throughput` | req/s | **63,767** [63,394–64,059] | 57,159 [51,747–58,356] | 4,882 [4,831–5,246] |
| `js_publish_sync` | msgs/s | **13,620** [13,171–13,746] | 9,225 [9,088–9,311] | n/s |
| `js_publish_async` | msgs/s | **140,870** [138,012–147,327] | 115,448 [107,532–116,255] | n/s |
| `js_consume` | msgs/s | **210,814** [206,354–211,220] | 28,453 [27,736–28,506] | n/s |
| `kv_put` | ops/s | **13,275** [11,851–13,393] | 9,124 [8,588–9,165] | n/s |
| `kv_get` | ops/s | **11,708** [10,880–11,737] | 8,770 [8,167–8,782] | n/s |
| `os_roundtrip` | MB/s | **965.6** [900.3–1,006.6] | 40.0 [39.7–41.0] | n/s |

natsio’s median is ahead of nats-py on 10 of these 13 scenarios. nats-py leads
on large-payload publish bandwidth and both delivery-throughput scenarios.
nats-core leads the two small-payload publish scenarios and 16-byte delivery;
its measured request/reply latency is higher here. These observations do not
isolate causes or predict production behavior. No unsupported workload is
treated as a loss.

## Security-fix hot-path check

A separate five-repeat comparison used the same NATS 2.14.3 binary and Python
3.13.13. The baseline loaded the unchanged core from the starting Git HEAD in
an isolated source directory; the after run loaded the patched checkout. Only
natsio was benchmarked, so the competitor dependency refresh between runs does
not enter these timings. The raw files retain the then-installed 1.0.0 package
metadata; the patch version was prepared afterward.

| Scenario | Before median [min–max] | After median [min–max] | Median change |
|---|---:|---:|---:|
| `pub_16b` | 1,416,841 [1,334,575–1,433,524] | 1,393,295 [1,370,774–1,401,820] | -1.7% |
| `pubsub_16b` | 308,065 [304,957–312,581] | 303,632 [292,318–304,435] | -1.4% |
| `reqrep_throughput` | 63,023 [61,484–64,246] | 62,558 [54,601–62,724] | -0.7% |

The modest measured throughput reductions are retained for the security and
bounded-wait guarantees. This sequential comparison is not a statistical
estimate of regression size. No protocol/parser architecture was changed.

Raw [before](https://github.com/corruptmane/natsio/blob/main/tools/natsio-bench/results/2026-10-22-before.json)
and [after](https://github.com/corruptmane/natsio/blob/main/tools/natsio-bench/results/2026-10-22-after.json) repetitions are retained.

## Reproducing

```bash
just install-server v2.15.1
just bench --repeats 5 --json results.json
just bench --quick
just bench --list
just bench --clients natsio,nats-py --scenarios js_consume,os_roundtrip --repeats 5
```

`just bench-refresh` refreshes the comparator lock entries. Keep the resolved
versions and raw results alongside any performance claim. The harness exits
nonzero if a scenario fails, while still recording successful and unsupported
pairs. Use the full workload for comparisons; quick mode is a smoke test.
