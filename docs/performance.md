# Replication performance

Three tools produce the figures here, and every table names which one:

- **`bin/benchmark.py`** measures the connector end to end. It runs as a
  subprocess, exactly as the Airbyte platform runs it, and counts RECORD
  messages against wall-clock time — so the figures include the SAP round trips,
  DuckDB, the CDK's serialization and the write to stdout. Its raw output is
  [`performance-raw.md`](./performance-raw.md).
- **`bin/probe-layers.py`** runs the driver's own SQL against DuckDB directly, so
  the thread budget, the partition count and ERPL's RFC thread pool can be varied
  without the CDK in the way.
- **`bin/trace-round-trips.py`** asks what the SAP side actually did, by counting
  RFC calls in ERPL's own trace rather than timing them.

The provenance matters. Four successive conclusions in earlier versions of this
document were wrong, every one because a probe query let DuckDB skip reading the
columns and so was not measuring the connector's workload at all. Every figure
below comes from a query built by the driver itself and pulled row by row.

## The one number that governs everything: round trips in flight

An RFC extract is **latency-bound, not CPU-bound**. Each of DuckDB's threads
here is blocked on a SAP round trip, so the thread budget is really "how many
round trips are in flight", and throughput tracks it almost linearly:

*Source: `probe-layers.py`, `DD02L`, all 55 columns, 164,673 rows.*

| DuckDB threads | Records/s |
|---:|---:|
| 1 | 1,479 |
| 2 | 2,806 |
| 4 | 5,135 |
| 8 | 8,740 |
| 16 | **11,720** |
| 32 | 12,919 |

The connector asks for `min(cpu_count, 16)`. The last doubling buys 10% for
twice the SAP-side load, and ERPL itself caches at most 16 RFC connections, so 16
is where this stops.

:::note
Until v0.2 this budget was divided by the `concurrency` setting, on the reasoning
that each Airbyte worker should get a share of the machine. Every stream shares
**one** DuckDB instance, whose scheduler already shares threads across the
queries running on it, so the division did not prevent oversubscription — it
starved the process. Measured end to end on a single-stream sync, before the fix:
9,686 records/s at `concurrency: 1` falling to **2,511** at `concurrency: 16`, for
a setting documented as how many *streams* run at once. It is now flat at ~8,850
whatever the concurrency.
:::

## Project your columns

*Source: `benchmark.py`, one run, same table and partition count on both rows.*

| `DD02L`, 164,673 rows, serial | Records/s |
|---|---:|
| all 55 columns | 9,422 |
| 2 of 55 columns | **36,209** |

**3.8x**, differing in the projection alone. It costs nothing but naming the
fields you want, and it reduces SAP's work as well as the connector's.

Narrow extracts are also the case where the thread budget stops mattering: at 2
columns the byte budget already carries ~8,200 rows per call, so there is little
latency left to hide. *Source: `probe-layers.py --narrow`.*

| `DD02L`, 2 of 55 columns | Records/s |
|---|---:|
| default (one call per column) | 69,499 |
| `threads: 16` | 78,336 |

## Partitioning: measured, and not recommended

`partitions` splits the scan into row windows read by parallel workers. On this
system it is **slower than not partitioning, on both a wide and a narrow
extract**, and the cause is now established.

*Source: `probe-layers.py`, all 55 columns.*

| Configuration | Records/s |
|---|---:|
| serial, 16 threads | **11,720** |
| 8 partitions, 1 DuckDB thread | 1,507 |
| 8 partitions, 8 DuckDB threads | 1,564 |
| 8 partitions, 32 DuckDB threads | 1,543 |
| 8 partitions, `threads: 55` | 1,528 |

Two things stand out. The partitioned figure does not move with the thread budget
— and 1,507 is what a *serial* scan does when restricted to a single thread
(1,479). A partitioned scan behaves like a single-threaded one.

`--connections` says why. ERPL names one trace file per RFC connection, so the
files count the concurrency:

*Source: `probe-layers.py --connections`.*

| Configuration | RFC calls | Connections | Calls per connection |
|---|---:|---:|---|
| serial | 1,540 | **8** | 196, 194, 194, 192, 192, 192, 190, 190 |
| 8 partitions | 1,210 | **3** | 586, 514, 110 |

With partitioning off, ERPL fetches **one RFC call per projected column** and runs
them concurrently — 55 columns, eight connections, evenly loaded. Turning
partitioning on replaces that column-parallel path with a row-window scheduler
that opens three connections and loads them unevenly. Fewer, larger calls, far
less concurrency, and on a latency-bound workload concurrency is the whole game.

This is an ERPL-side property, not a connector one: it reproduces in a plain
DuckDB session with no connector present, and adding the connector's own
`coerce_row` conversion changes nothing (1,500 vs 1,503 records/s).

`partitions` therefore defaults to **0**, and the setting stays only for anyone
whose system behaves differently. Raise it only with a measurement.

:::note
The ERPL extension's own documentation reports a 2.7x speed-up from eight
partitions. That did not reproduce here on either shape — wide (1,564 against
11,720) or narrow (46,671 against 69,499). The figures are not necessarily in
conflict: a system whose RFC latency is much lower, or whose gateway refuses
eight concurrent connections, is a different regime. But on this system, and
through this connector, partitioning costs throughput.
:::

### The fetch budget, and why it is scaled

ERPL's fetch budget is counted in **bytes**, not rows, and a partitioned scan
divides it across workers. A wide row therefore starves each worker:

*Source: `trace-round-trips.py`.*

| `DD02L`, all 55 columns | RFC calls | Rows per call |
|---|---:|---:|
| serial, default budget | 1,540 | 107 |
| 8 partitions, default budget | 9,680 | **17** |
| 8 partitions, 8x budget | 1,760 | 94 |
| 8 partitions, 32x budget | 880 | 187 |

Seventeen rows per round trip against 107. The connector scales the budget with
the partition count so that asking for partitions does not silently starve the
workers — visible in the end-to-end figures below, where the penalty used to
*grow* with the worker count and is now flat:

*Source: `benchmark.py`, one run per case.*

| `DD02L`, all 55 columns | Before the budget fix¹ | Now |
|---:|---:|---:|
| 0 partitions (serial) | 9,450 | **9,422** |
| 2 | 1,152 | 1,484 |
| 4 | 776 | 1,484 |
| 8 | 508 | 1,491 |
| 16 | 516 | — |

¹ From a run against commit `c148066`, before the budget scaling landed. That run
is not in [`performance-raw.md`](./performance-raw.md), which holds only the
current matrix; the column is kept because the *shape* is the evidence.

The flatness is the evidence that the starvation is gone. What remains is the
concurrency collapse described above, which the budget cannot fix.

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
4. **Round trips in flight** — already at the connector's ceiling of 16; lower it
   only if your Basis team says the connector is crowding out users.
5. *Not* partitions, unless you have measured it on your own system.

## Reproducing

```bash
./bin/benchmark.py --list
./bin/benchmark.py --repeat 3               # writes docs/performance-raw.md
uv run ./bin/probe-layers.py                # the thread and partition matrix
uv run ./bin/probe-layers.py --connections  # RFC connections per path
./bin/trace-round-trips.py                  # RFC call counts behind the above
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
hardware. A production SAP system has more work processes and more network
latency, both of which shift where the concurrency ceiling sits. Timings are
reported, never asserted in tests: a threshold on a number that moves with
someone else's background job is a test that fails for reasons nobody can act on.
