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
delta cursor after a successful run and reports what SAP said; if the close was
refused, the message says so and the next run recovers it.

## What a failed sync leaves behind

| Protocol | After a failure |
|---|---|
| RFC tables | nothing; the next run re-reads |
| Function modules | nothing |
| BICS | the BICS session is server-side and times out on its own |
| ODP | the subscription stays, the position does **not** advance, and the next run re-reads that delta |

The connector deliberately does not checkpoint an ODP position for a stream that
did not complete. That means a failure near the end of a long delta costs you
the whole delta again — the alternative is skipping rows permanently, which is
worse.

## Concurrency and SAP work processes

`concurrency` controls how many streams are read at once, and each worker holds
an SAP connection. Raising it consumes dialog work processes on the SAP system;
if your Basis team notices the connector crowding out users, that is the setting
to lower. It is clamped to 32.

Within one stream, `partitions` is off by default and the measurements say leave
it there — see [performance](performance.md).

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
