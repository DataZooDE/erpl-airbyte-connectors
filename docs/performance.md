# Replication performance

Hand-written interpretation of [`performance-raw.md`](./performance-raw.md),
which `bin/benchmark.py` generates. Measurements are end to end: the connector
runs as a subprocess, exactly as the Airbyte platform runs it, and RECORD
messages are counted against wall-clock time. The figures therefore include the
SAP round trips, DuckDB, the CDK's serialization and the write to stdout.

Everything below comes from that harness, on the same system, in the same run.
Earlier versions of this document mixed in numbers from ad-hoc probes that used
different queries; three separate conclusions were wrong as a result, so the
rule now is that a figure appears here only if a benchmark case produces it.

## Project your columns

| `DD02L`, 164,673 rows, serial | Records/s |
|---|---:|
| all 55 columns | 9,506 |
| 2 of 55 columns | **36,630** |

**3.9x**, and the two cases differ in the projection alone — same table, same
partition count, same run. This is the single most effective setting the
connector exposes.

## Partitioning does not help, and is off by default

| `DD02L`, all 55 columns | Records/s |
|---:|---:|
| 0 partitions (serial) | 9,450 |
| 2 | 1,152 |
| 4 | 776 |
| 8 | 508 |
| 16 | 516 |

And on a narrow extract, where the SAP side is fastest:

| `DD02L`, 2 of 55 columns | Records/s |
|---:|---:|
| 0 partitions | 36,630 |
| 8 partitions | 27,882 |

Slower in both regimes, so `partitions` defaults to `0`. The setting remains for
anyone whose system behaves differently, but raise it only with a measurement.

:::note
The ERPL extension's own documentation reports a 2.7x speed-up from eight
partitions. That benchmark measures a single-column extract in raw DuckDB —
extraction only, no connector. Both figures can be true: what this table shows
is that the speed-up does not survive to the connector's output. Why it does not
is **not** established here. An earlier version of this document asserted an
explanation from ad-hoc probes; the probes were not measuring comparable work,
and the explanation has been withdrawn rather than restated.
:::

## Other protocols

| Case | Records | Time | Records/s |
|---|---:|---:|---:|
| `SBOOK`, wider transactional rows | 28,782 | 15.7s | 1,833 |
| ODP full, 1 thread | 3,001 | 1.9s | 1,575 |
| ODP full, 4 threads | 3,001 | 1.8s | 1,662 |
| ODP full, 8 threads | 3,001 | 1.9s | 1,579 |
| ODP over OData, initial load | 3,001 | 2.7s | 1,104 |

The ODP source here is far too small for thread scaling to show. Treat those
three rows as evidence the setting is harmless at this size, not as a curve.

## Where to spend tuning effort

On the SAP side of the boundary, in this order:

1. **Columns** — 3.9x, measured, costs nothing.
2. **SAP-side filters** — rows that never cross the wire cost nothing at all.
3. **Protocol** — ODP delta moves only what changed.
4. *Not* partitions, unless you have measured it on your own system.

## Reproducing

```bash
./bin/benchmark.py --list
./bin/benchmark.py --repeat 3          # writes docs/performance-raw.md
```

The harness refuses to run while anything else is talking to the same SAP
system, pairs every figure for a case with the run it came from, and lists any
case that failed rather than omitting it.

## Environment

- Host: Linux 6.18, 32 CPUs, 125 GB RAM
- Python 3.13, DuckDB 1.5.5, erpl v2026.09.04
- SAP: ABAP Platform Trial in a container on the same host

The SAP system shares a laptop with the connector, so these are *relative*
figures for comparing settings, not a capacity statement about production
hardware. Timings are reported, never asserted in tests: a threshold on a number
that moves with someone else's background job is a test that fails for reasons
nobody can act on.
