# Operations

What running this connector in anger actually involves.

## Resetting a stream

Resetting an **ODP** stream is not only an Airbyte-side action. The subscription
lives on SAP, and a reset should clear both:

1. Reset the stream in Airbyte, so the state is dropped.
2. The next sync performs a fresh DELTAINIT under the same subscriber process,
   which SAP treats as re-initialising the existing subscription.

If you want the SAP-side subscription genuinely gone — decommissioning a
connection, say — clear it in `ODQMON` as well, or it keeps retaining data for a
subscriber that will never read again.

## Finding stranded subscriptions

Anything the connector registered is visible on SAP:

```sql
-- from a DuckDB session with erpl loaded
SELECT * FROM sap_odp_show_subscriptions();      -- ERPL's own, by default
SELECT * FROM sap_odp_show_cursors();            -- open delta cursors
```

Subscriber processes the connector derives start with `AB_`. A subscription
whose Airbyte connection no longer exists is stranded: nothing will ever consume
it, and the queue grows. In transaction `ODQMON`, the same thing is under the
subscriber type `SAP_BW` / name `ERPL`.

A cursor that is open between syncs is not normal. The connector closes its
delta cursor at the end of every run — successful or failed — and reports what
SAP said. A run that failed mid-fetch is the case SAP refuses to close
(`ILLEGAL_REQ_STATE_FOR_CONFIRM`): the log says `could not be closed (REFUSED)`,
the cursor stays reserved until it times out, and
[`sap_odp_drop`](troubleshooting.md) clears it if you need the queue sooner.
Closing is not confirming, so a close never costs the next run its packets.

## What a failed sync leaves behind

| Protocol | After a failure |
|---|---|
| RFC tables | nothing; the next run re-reads |
| Function modules | nothing |
| BICS | nothing on the BW side. Each `sap_bics_*` statement is its own CREATE_DATA_AREA → OPEN → SET_STATE → **CLOSE** cycle inside ERPL, so a provider is closed when the statement ends, exception or not. What the connector calls a "BICS session" is state rows in its own scratch DuckDB file, discarded with it |
| ODP | the subscription stays, the position does **not** advance, and the next run re-reads that delta. The delta cursor is closed on the way out; if the failure was mid-fetch SAP refuses the close, and the log says so |

The connector deliberately does not checkpoint an ODP position for a stream that
did not complete. That means a failure near the end of a long delta costs you
the whole delta again — the alternative is skipping rows permanently, which is
worse.

## Concurrency and SAP work processes

`concurrency` controls how many streams are read at once, and each worker holds
an SAP connection. Raising it consumes dialog work processes on the SAP system;
if your Basis team notices the connector crowding out users, that is the setting
to lower. It is clamped to 32.

It no longer affects how fast a *single* stream reads. Until v0.2 the connector
divided DuckDB's thread budget by this number, which made a one-stream sync
nearly 4x slower at `concurrency: 16` than at 1 — see
[performance](performance.md). The budget is now fixed at `min(cpu_count, 16)`
round trips in flight, whatever the concurrency.

Within one stream, `partitions` is off by default and the measurements say leave
it there — see [performance](performance.md).

## Sizing the connector container

Measured, not derived — `bin/probe-layers.py`'s sibling measurements, peak RSS of
the connector process reading `DD02L` (164,673 rows, 55 columns) on a 32-CPU host:

| Case | Peak RSS |
|---|---:|
| one stream, all 55 columns, default budget | **1,698 MB** |
| one stream, 2 of 55 columns | 450 MB |
| one stream, `fetch_size: 16777216` | 2,936 MB |
| one stream, `fetch_size: 67108864` | 2,938 MB |
| two streams at `concurrency: 2` | 3,310 MB |

Three things follow.

**A wide stream costs about 1.7 GB on its own.** The connector keeps up to 16 SAP
round trips in flight, each carrying a slice of every projected column, so memory
tracks the *width* of the extract and the number of in-flight calls — not the row
count, which is streamed.

**Concurrency multiplies it.** Two concurrent wide streams measured 3,310 MB,
almost exactly twice one. Budget roughly `1.7 GB × concurrency` for wide tables,
and remember `concurrency` defaults to 4.

**Projecting columns is also the memory fix.** Two columns instead of 55 cost 450
MB against 1,698 — the same setting that makes the sync 3.8x faster.

Raising `fetch_size` past 16 MiB buys nothing in either direction: 64 MiB measured
the same 2.9 GB, because ERPL caps the concurrent-row budget (projected columns ×
batch size) independently.

The combination that surprises people is a raised `fetch_size` alongside
`partitions`, because the connector multiplies the budget by the partition count
(that is what stops each worker starving). An explicit `fetch_size` is *not*
multiplied — it is taken as the number you meant — so if you set both, set
`fetch_size` to the total you can afford. The connector warns when an explicit
budget divided by the partition count leaves a worker under 512 KB.

A container with 2 GiB is enough for one wide stream and nothing else. If you run
several streams at once, or raise `fetch_size`, raise the memory limit in step —
otherwise the platform kills the sync, and a killed process looks exactly like a
short read rather than an error you can act on.

## Monitoring a sync

Once rows are flowing, the connector emits a progress line every 60 seconds.
Those are protocol LOG messages, so they also keep the platform's
`maxSecondsBetweenMessages` budget alive.

Before the first row there is nothing to report, and an ODP initial load can
spend a long time in SAP preparing the extraction. That is why the budget is set
to 7200 seconds — a silent two hours at the start of a delta is expected, not a
hang.

## Credentials

The `password` field is marked `airbyte_secret`, so the platform redacts it from
logs and stores it encrypted. The other logon fields — host, client, user — are
not secret and appear in logs, which is deliberate: they are what you need to
read a failure.

Credentials reach SAP as bound parameters of a `CREATE SECRET` statement rather
than being interpolated into any SQL text.

Rotate the password on the SAP side and update the source; there is no cached
copy anywhere else.

## Upgrading the connector

The ERPL extensions are baked into the image at a pinned version, so upgrading
the connector upgrades them together — there is no separate step, and a sync
never downloads anything. Check the [changelog](../source-sap/CHANGELOG.md) for
behaviour changes before bumping a production connection.

## See also

- [Troubleshooting](troubleshooting.md) — specific errors
- [Incremental sync](incremental.md) — what a subscription is and why it matters
