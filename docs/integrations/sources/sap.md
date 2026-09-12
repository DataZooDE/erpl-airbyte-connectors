# SAP

This source reads data out of SAP systems through the [ERPL](https://erpl.io) DuckDB
extensions. One connector covers four SAP interfaces; you pick one per connection.

| Protocol | Reads |
|---|---|
| SAP Tables and CDS Views (RFC) | Any transparent table, pool/cluster table or CDS view, via `RFC_READ_TABLE` |
| SAP BW Queries (BICS) | BW InfoProviders and BEx queries, including variable binding |
| SAP ODP (RFC) | ODP providers in the BW, ABAP_CDS, SAPI, SLT and HANA contexts, full and delta |
| SAP ODP (OData) | The same ODP data over the SAP Gateway, full and delta |

## Prerequisites

- A SAP NetWeaver or S/4HANA system reachable from where Airbyte runs.
- A SAP user with the **Client** and **User** you will configure, plus a **Password**
  (or an SNC identity).
- Authorization object **`S_RFC`** for the function groups the connector calls:
  `RFC_READ_TABLE` and `DDIF_FIELDINFO_GET` for the RFC protocol, `RODPS_REPL_*`
  for ODP, and the BICS function group for BW.
- For **SAP ODP (OData)**: the **Gateway Base URL** (for example
  `https://sap.example.com:44300`) and an activated ODP OData service.
- If the system sits behind a SAProuter, its **SAProuter String**.

Either a direct logon (**Application Server Host** + **System Number**) or a
load-balanced logon (**Message Server Host** + **System ID** + **Logon Group**) is required.

## Setup guide

1. In Airbyte, create a new source and choose **SAP**.
2. Fill in the logon fields above.
3. Choose a **Protocol** and tell the connector what to read:
   - a **pattern** (`table_pattern`, `query_pattern`, `name_pattern`, `service_pattern`)
     to discover objects in bulk, and/or
   - an explicit **objects** list, which is also where per-object settings live
     (columns, SAP-side filters, cursor field, BEx variables, ODP subscriber name).
4. Run the connection test, then **Set up source** and refresh the schema.

BEx queries with mandatory variables must be listed explicitly with those variables
bound — BW refuses to return a result until they have values.

## Supported sync modes

| Feature | Supported |
|---|---|
| Full Refresh - Overwrite | Yes |
| Full Refresh - Append | Yes |
| Incremental - Append | Yes (RFC with a cursor field; both ODP protocols) |
| Incremental - Append + Deduped | Yes |
| Change Data Capture | Yes, for both ODP protocols |
| Namespaces | No |

**RFC** syncs incrementally when you nominate a **Cursor Field** — a date or
timestamp column. The connector keeps the highest value it has seen and pushes a
`>=` predicate down to SAP on the next run.

**ODP** syncs incrementally through SAP's own delta mechanism. The first
incremental run performs SAP's DELTAINIT: it returns the whole current snapshot
and registers a subscription. Later runs return only what changed, and deletes
arrive as `_ab_cdc_deleted_at` tombstones.

:::caution
The ODP delta position lives **on the SAP system**, keyed by a subscriber process
that the connector derives from the connection. Deleting the Airbyte connection
without resetting the stream leaves that subscription registered on SAP, where it
keeps retaining delta data. Reset the stream before removing a connection, or
clear the subscription in transaction `ODQMON`.
:::

## Supported streams

Streams are whatever your pattern and object list select. Stream names are:

- **RFC** — the SAP table or view name, e.g. `SFLIGHT`
- **BICS** — the name you give the object
- **ODP (RFC)** — `<context>/<provider>`, e.g. `ABAP_CDS/SEPM_IBUPA$P`
- **ODP (OData)** — the entity-set name, e.g. `FactsOfZJRODPVSQL`

## Performance

- **Partitions** (RFC) splits one table scan into row ranges read in parallel.
  Rows then arrive in an unspecified order, which does not affect correctness.
- **Threads** (ODP) parallelises full extractions. Delta extractions always run
  single-threaded: a parallel multi-package delta can under-count.
- **Concurrency** controls how many streams are read at once. Each worker holds a
  SAP connection, so keep it within the system's free work processes.
- **BICS cannot paginate** — BW materialises the entire result set or none of it.
  For a large cube, use **Slice By** to run one BICS session per characteristic
  member and bound memory.

## Limitations

- The connector image is **linux/amd64 only**; ERPL publishes no arm64 build.
- BICS exposes no change tracking, so BW queries are full-refresh only.
- ODP OData catalog discovery depends on the Gateway catalog service, which is not
  reachable on every release. List entity-set URLs explicitly if discovery finds nothing.

## Changelog

| Version | Date | Pull Request | Subject |
|---|---|---|---|
| 1.0.0 | 2026-09-12 | — | Rewrite as a single `source-sap` connector: adds BICS, ODP over RFC and ODP over OData alongside the existing table reads; moves to the Concurrent CDK with incremental sync, parameterised credentials and extensions baked into the image. |
