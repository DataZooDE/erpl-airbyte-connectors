# Incremental sync

Four mechanisms, depending on protocol. Only the two ODP ones are genuine change
data.

| Protocol | Mechanism | Deletes? |
|---|---|---|
| RFC tables | high-water mark on a column you nominate | no |
| RFC function modules | a cursor parameter you nominate | no |
| **ODP (RFC)** | SAP's delta queue | **yes** |
| **ODP (OData)** | SAP's delta queue, over HTTP | **yes** |
| BICS | a BEx variable used as a watermark | no |

If you need deletes, or you want SAP to decide what changed rather than
inferring it from a timestamp, use ODP.

## ODP — how it behaves

The first incremental run performs SAP's **DELTAINIT**: it returns the whole
current snapshot *and* registers a subscription. Every later run returns only
what changed since, with deletes arriving as `_ab_cdc_deleted_at` tombstones.

Tombstones need the provider to report deletes. ODP over RFC always does. Over
the Gateway it depends on the service: an entity set that exposes no change-mode
column (`ODQ_CHANGEMODE`) syncs new and changed rows but can never mark one
deleted, so the destination keeps deleted rows indefinitely. Discovery warns
when it finds such an entity set, and the stream's schema then has no
`_ab_cdc_deleted_at` — which is the reliable way to tell.

```json
{
  "protocol": {
    "mode": "odp_rfc",
    "context": "ABAP_CDS",
    "objects": [ { "name": "SEPM_IBUPA$P" } ]
  }
}
```

Choose *Incremental | Append + Deduped* in the connection, and make sure the
stream has a primary key so the destination can apply the changes.

Not every provider is delta-capable. Discovery reports it, and a provider
without delta support is full-refresh only.

## The position lives on SAP

This is the thing to understand before running ODP in production.

For **ODP over RFC**, the *position* is on SAP: it remembers how far each
subscriber has read. Airbyte state holds the subscriber's **name**, plus a
last-changed timestamp used to skip an unchanged run — but nothing that says
where in the data it stopped. So:

- Two Airbyte connections reading the same provider **must not share a
  subscriber process**, or they consume each other's changes and each sees a
  fragment. The connector derives a distinct name from the SAP logon and the
  object, refuses two configured objects that would collide, and warns whenever
  it has to derive one. Set it explicitly when in doubt:

  ```json
  { "name": "SEPM_IBUPA$P", "subscriber_process": "AIRBYTE_PROD_BUPA" }
  ```

- **Deleting a connection without resetting the stream strands the
  subscription.** It stays registered on SAP and keeps retaining delta data
  indefinitely. Reset the stream first, or clear it in `ODQMON`.

For **ODP over OData** the position is a delta token, and Airbyte state does
carry it. A fresh container resumes correctly from state alone.

## Skipping a quiet sync

Before a delta read, the connector asks SAP for the provider's last-changed
timestamp — one call, no rows. If nothing has changed, the extraction is skipped
entirely and no cursor is opened. Turn it off with `skip_unchanged: false` if
you would rather always read.

## Cursor-based incremental, for tables

```json
{ "name": "SFLIGHT", "cursor_field": "FLDATE" }
```

A high-water mark. Cheap, needs no subscription, and has two honest limits:
deletes are invisible, and a row whose cursor value moves backwards is missed.
Good for append-only data; wrong for anything edited in place.

## BICS

BW exposes no change tracking, so the watermark is a BEx variable restricted to
everything at or after the highest value seen:

```json
{
  "name": "sales", "cube": "0D_NW_C01", "query": "ZSALES_Q01",
  "cursor_variable": "ZVAR_CALMONTH", "cursor_field": "0CALMONTH",
  "primary_key": ["0CALMONTH", "0MATERIAL"]
}
```

The selection is `>=`, so the boundary period is re-read every run. A primary
key is therefore required — without one the destination cannot deduplicate and
every run appends the same rows again. The connector refuses the configuration
rather than letting that happen.

## When a sync fails

The connector does **not** checkpoint a server-side position for a stream that
did not finish, so a failed run is retried from where it was rather than
skipping ahead. The trade is that a long ODP sync which dies near the end
re-reads from the start of that delta.

## See also

- [Operations](operations.md) — resets, stranded subscriptions, what to watch
- [Authorizations](authorizations.md) — the `RODPS_REPL_*` modules ODP needs
