# Replication performance

Hand-written interpretation of the figures in
[`performance-raw.md`](./performance-raw.md), which `bin/benchmark.py` generates.
Measurements are taken end to end: the connector runs as a subprocess, exactly
as the Airbyte platform runs it, and RECORD messages are counted against
wall-clock time. So the figures include the SAP round trips, DuckDB, the CDK's
serialization and the write to stdout — not just the extraction.

## Project your columns

| `DD02L`, 164,673 rows, serial | Records/s |
|---|---:|
| all 55 columns | 9,506 |
| 2 of 55 columns | **36,630** |

Naming the fields you want is worth **3.9x**, and it is the single most
effective setting this connector exposes. Both rows differ in the projection
alone.

## The connector is emission-bound, not extraction-bound

This is the finding that explains the rest, and it took several wrong turns to
reach.

The SAP side can go far faster than the connector can emit. Measured in raw
DuckDB, with no connector involved, reading one column of the same table:

| | Serial | 8 partitions |
|---|---:|---:|
| raw DuckDB, 1 column | 80,078 rows/s | **345,522 rows/s** |
| through the connector, 2 columns | 36,630 rows/s | 27,882 rows/s |

Raw extraction accelerates 4.3x with partitioning. The connector does not,
because its ceiling is turning rows into protocol messages on stdout — roughly
36,000 records/s on narrow rows and 9,500 on 55-column ones, where each record
carries far more JSON. Extraction headroom above that ceiling cannot be spent.

## So partitioning does not help, and is off by default

Two independent effects, both pointing the same way:

1. **On the SAP side, partitioning helps narrow extracts and hurts wide ones.**
   Raw DuckDB, same table, varying only the columns fetched:

   | Columns fetched | Serial | 8 partitions | |
   |---:|---:|---:|---|
   | 1 of 55 | 2.1s | 0.5s | 4.3x faster |
   | all 55 | 19.3s | 55.9s | 2.9x slower |

   The ERPL extension's published 2.7x speed-up is a single-column benchmark, so
   it measures the regime where partitioning pays. A connector reading whole
   rows is in the other one.

2. **Through the connector it does not pay even when it is fast on the SAP
   side**, because of the emission ceiling above: 2 columns at 8 partitions runs
   at 27,882 records/s against 36,630 serial. The coordination cost is real and
   the speed-up is unusable.

Through the connector, reading all columns, the penalty compounds:

| Partitions | Records/s |
|---:|---:|
| 0 (serial) | 9,450 |
| 2 | 1,152 |
| 4 | 776 |
| 8 | 508 |
| 16 | 516 |

**`partitions` therefore defaults to 0.** The setting remains for anyone whose
SAP system behaves differently, but raise it only with a measurement in hand.

## Other protocols

| Case | Records | Time | Records/s |
|---|---:|---:|---:|
| `SBOOK`, wider transactional rows | 28,782 | 15.7s | 1,833 |
| ODP full, 1 thread | 3,001 | 1.9s | 1,575 |
| ODP full, 4 threads | 3,001 | 1.8s | 1,662 |
| ODP full, 8 threads | 3,001 | 1.9s | 1,579 |
| ODP over OData, initial load | 3,001 | 2.7s | 1,104 |

The ODP source here is too small for thread scaling to show. Treat those three
rows as evidence that the setting is harmless at this size, not as a curve.

## Where to spend tuning effort

On the SAP side of the boundary, in this order:

1. **Columns** — 3.9x, costs nothing.
2. **SAP-side filters** — the rows never cross the wire.
3. **Protocol** — ODP delta moves only what changed.
4. *Not* partitions, unless you have measured it on your own system.

## Reproducing

```bash
./bin/benchmark.py --list
./bin/benchmark.py --repeat 3          # writes docs/performance-raw.md
```

The harness refuses to run while anything else is talking to the same SAP
system: a benchmark that silently measures contention is worse than none.

## Environment

- Host: Linux 6.18, 32 CPUs, 125 GB RAM
- Python 3.13, DuckDB 1.5.5, erpl v2026.09.04
- SAP: ABAP Platform Trial in a container on the same host

The SAP system shares a laptop with the connector, so these are *relative*
figures for comparing settings, not a capacity statement about production
hardware. Timings are reported, never asserted in tests: a threshold on a number
that moves with someone else's background job is a test that fails for reasons
nobody can act on.
