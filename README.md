<p align="center">
  <img src="assets/erpl_airbyte_rfc_read_table_result.png" alt="ERPL Airbyte connector" width="640">
</p>

# ERPL Airbyte Connectors

An Airbyte source for SAP, built on the [ERPL](https://erpl.io) DuckDB extensions.

One connector — [`source-sap`](./source-sap) — covers four SAP interfaces:

| Protocol | ERPL extension | Reads |
|---|---|---|
| **RFC** | `erpl_rfc` | SAP tables and CDS views, with projection and filter pushdown |
| **BICS** | `erpl_bics` | BW InfoProviders and BEx queries, including variable binding |
| **ODP (RFC)** | `erpl_odp` | ODP providers across BW, ABAP_CDS, SAPI, SLT and HANA — full and delta |
| **ODP (OData)** | `erpl_web` | The same ODP data over the SAP Gateway — full and delta |

Both ODP protocols sync incrementally through SAP's own delta mechanism, and emit
deletes as Airbyte CDC tombstones.

## Getting started

See [source-sap/README.md](./source-sap/README.md) for development, and
[docs/integrations/sources/sap.md](./docs/integrations/sources/sap.md) for the
user-facing setup guide.

```bash
cd source-sap
uv sync
uv run pytest unit_tests -q
./bin/build-image.sh
```

## Repository layout

```
source-sap/                         the connector
  source_sap/protocols/             one driver per SAP interface
  unit_tests/                       no SAP required
  integration_tests/                Airbyte's standard connector tests
  e2e/                              full connector runs against a real SAP system
docs/integrations/sources/sap.md    user-facing documentation
```

## Testing philosophy

The definition of done for every protocol is an **end-to-end test against a real
SAP system, with nothing mocked** — the connector runs as a subprocess exactly as
the Airbyte platform runs it, and the assertions are made on the protocol messages
it writes to stdout. The reference system is the
[ABAP Platform Trial](https://hub.docker.com/r/sapse/abap-platform-trial) container.

## Licence

Business Source License 1.1 (see [LICENSE](./LICENSE)), the same licence as the
ERPL extensions this connector loads. You may use it in production, but not offer
it to third parties on a hosted or embedded basis; the licence converts to MPL 2.0
five years after publication. BICS and ODP replication are ERPL Enterprise Edition
features. For commercial terms, see [erpl.io](https://erpl.io).
