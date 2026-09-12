# Replication performance

Two tools produce the figures here, and every table below names which one:

- **`bin/benchmark.py`** measures the connector end to end. It runs as a
  subprocess, exactly as the Airbyte platform runs it, and counts RECORD
  messages against wall-clock time — so the figures include the SAP round trips,
  DuckDB, the CDK's serialization and the write to stdout. Its raw output is
  [`performance-raw.md`](./performance-raw.md).
- **`bin/trace-round-trips.py`** asks what the SAP side actually did, by counting
  RFC calls in erpl's own trace rather than timing them.

The provenance matters. Four successive conclusions in earlier versions of this
document were wrong, every one because a probe query let DuckDB skip reading the
columns and so was not measuring the connector's workload at all. Counting round
trips is what finally produced an answer that held up.

## Project your columns

*Source: `benchmark.py`, one run, same table and partition count on both rows.*

| `DD02L`, 164,673 rows, serial | Records/s |
|---|---:|
| all 55 columns | 9,422 |
| 2 of 55 columns | **36,209** |

**3.8x**, differing in the projection alone. This is the single most effective
setting the connector exposes, and it costs nothing but naming the fields you
want.

## Partitioning: one cause found and fixed, one still open

*Source: `benchmark.py`, one run per case.*

| `DD02L`, all 55 columns | Before the budget fix | Now |
|---:|---:|---:|
| 0 partitions (serial) | 9,450 | **9,422** |
| 2 | 1,152 | 1,484 |
| 4 | 776 | 1,484 |
| 8 | 508 | 1,491 |
| 16 | 516 | — |

### What was found

*Source: `trace-round-trips.py`.*

erpl's fetch budget is counted in **bytes**, not rows, and it is divided across
partition workers. A wide row therefore starves each worker:

| `DD02L`, all 55 columns | RFC calls | rows per call |
|---|---:|---:|
| serial, default budget | 1,540 | 107 |
| 8 partitions, default budget | 9,680 | **17** |
| 8 partitions, 8x budget | 1,760 | 94 |
| 8 partitions, 32x budget | 880 | 187 |

Seventeen rows per round trip against 107, and 6.3x the round trips. **The
connector now scales the fetch budget with the partition count**, so asking for
partitions no longer silently starves the workers. An explicit `fetch_size`
still wins.

That fix is visible in the table above: the penalty used to *grow* with the
worker count (1,152 → 776 → 508) and is now flat at ~1,485 whatever the count.
The flatness is the evidence that the starvation is gone — if it were still
per-worker, more workers would still be worse.

### What is still open

A flat ~6x penalty against serial remains, independent of partition count, and
this document does not claim to know its cause. Inside DuckDB the same scan with
the same settings is roughly 2x *faster* than serial, so the cost appears when a
partitioned scan's rows are pulled into Python — but that is where the evidence
stops, and four earlier guesses in this space were wrong.

`partitions` therefore defaults to **0**. The setting stays for anyone whose
system behaves differently, but raise it only with a measurement.

:::note
The ERPL extension's own documentation reports a 2.7x speed-up from eight
partitions. That is a single-column extract measured inside DuckDB, which is the
regime where partitioning pays and where the byte budget is not a constraint.
Both figures are right about different workloads.
:::

Partitioning does not pay on a narrow extract either, where the SAP side is
fastest:

| `DD02L`, 2 of 55 columns | Records/s |
|---:|---:|
| 0 partitions | 36,209 |
| 8 partitions | 28,042 |

## Other protocols

*Source: `benchmark.py`.*

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

1. **Columns** — 3.8x, measured, costs nothing.
2. **SAP-side filters** — rows that never cross the wire cost nothing at all.
3. **Protocol** — ODP delta moves only what changed.
4. *Not* partitions, unless you have measured it on your own system.

## Reproducing

```bash
./bin/benchmark.py --list
./bin/benchmark.py --repeat 3          # writes docs/performance-raw.md
./bin/trace-round-trips.py             # RFC call counts behind the above
```

`benchmark.py` refuses to run while anything else is talking to the same SAP
system, pairs every figure for a case with the run it came from, and lists any
case that failed rather than omitting it from a report that would then look
complete.

## Environment

- Host: Linux 6.18, 32 CPUs, 125 GB RAM
- Python 3.13, DuckDB 1.5.5, erpl v2026.09.04
- SAP: ABAP Platform Trial in a container on the same host

The SAP system shares a laptop with the connector, so these are *relative*
figures for comparing settings, not a capacity statement about production
hardware. Timings are reported, never asserted in tests: a threshold on a number
that moves with someone else's background job is a test that fails for reasons
nobody can act on.
