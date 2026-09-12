# Configuration reference

Every field the connector accepts. Generated from `source_sap/spec.yaml`, which
is the authority — if this page and the spec disagree, the spec is right.

## Connection

| Field | Type | Required | Description |
|---|---|---|---|
| `ashost` | string |  | Hostname of the SAP application server, for a direct logon. Leave empty and use the message-server fields instead for a load-balanced logon. |
| `sysnr` | string |  | Two-digit SAP system number. Required for a direct logon. |
| `mshost` | string |  | Message server hostname, for a load-balanced logon. |
| `sysid` | string |  | Three-character SAP system ID. Required for a load-balanced logon. |
| `group` | string |  | Logon group. Required for a load-balanced logon. |
| `client` | string | yes | Three-digit SAP client. |
| `user` | string | yes | SAP user name. Needs authorization object S_RFC for the function groups used. |
| `password` | string |  | SAP password. Not required when logging on with SNC or an SSO2 ticket. |
| `lang` | string |  | Two-letter logon language; controls the language of field texts. |
| `base_url` | string |  | Base URL of the SAP Gateway, used by the "ODP over OData" protocol. Include the scheme and port. |
| `saprouter` | string |  | SAProuter connection string, if the SAP system is reached through one. |
| `snc_mode` | string |  | Set to "1" to secure the connection with SNC. |
| `snc_partnername` | string |  | SNC Partner Name |
| `snc_lib` | string |  | SNC Library Path |
| `snc_qop` | string |  | SNC Quality of Protection |
| `concurrency` | integer |  | How many streams to read in parallel. Each worker holds its own SAP connection, so raise this only as far as the SAP system has free work processes. |
| `saprouter_host` | string |  | Hostname of the SAProuter, without the /H/ /S/ route syntax. Only needed so the platform can allow egress to it. |

The **protocol** field is a choice of one of five modes, each with its own
settings.

## `rfc` — tables and CDS views

| Field | Description |
|---|---|
| `table_pattern` | SAP wildcard pattern selecting tables. Case-sensitive; `*` matches any sequence. |
| `partitions` | Row ranges read in parallel. Default `0`; see [performance](performance.md). |
| `fetch_size` | Bytes per SAP round trip. Scaled with `partitions` automatically when unset. |
| `objects[].name` | Table or CDS view name. |
| `objects[].columns` | Columns to read. Pushed into SAP — the most effective setting here. |
| `objects[].filter` | ABAP `WHERE` fragment evaluated by SAP, e.g. `CARRID = 'LH'`. |
| `objects[].cursor_field` | Date or timestamp column for incremental sync. |
| `objects[].partitions`, `fetch_size`, `max_rows` | Per-table overrides. |

Full guide: [tables and CDS views](tables.md).

## `rfc_invoke` — function modules

| Field | Description |
|---|---|
| `objects[].name` | Stream name. |
| `objects[].function` | Function module to call. Only listed modules are ever called. |
| `objects[].path` | Result parameter whose rows become records, e.g. `/FLIGHT_LIST`. Empty means the scalar exports, one record. |
| `objects[].parameters` | Values for import and table parameters, as JSON. Cast to the SAP-declared type. |
| `objects[].return_parameter` | Override, if the module's return table is not conventionally named. |
| `objects[].primary_key` | Fields forming the record key. |
| `objects[].cursor_field` + `cursor_parameter` | Incremental sync; both are required. |
| `objects[].slice_by` | `{parameter, values}` — one call per value, in parallel. |

Full guide: [calling function modules](function-modules.md).

## `bics` — BW queries

| Field | Description |
|---|---|
| `query_pattern`, `object_type` | Bulk discovery of `QUERY`, `CUBE` or `INFOPROVIDER`. |
| `objects[].cube`, `query` | InfoProvider and BEx query. |
| `objects[].rows`, `columns` | Characteristics and key figures. |
| `objects[].variables` | BEx variables: `{name, low, high, sign, op}`. |
| `objects[].slice_by` | `{characteristic, members}` — one BICS session per member. |
| `objects[].cursor_variable` + `cursor_field` + `primary_key` | Watermark incremental. |

Full guide: [BW queries](bw-queries.md).

## `odp_rfc` — ODP over RFC

| Field | Description |
|---|---|
| `context` | `ABAP_CDS`, `BW`, `SAPI`, `SLT` or `HANA`. |
| `name_pattern` | Provider pattern within the context. |
| `threads` | Parallel workers for full extraction. Delta is always single-threaded. |
| `skip_unchanged` | Probe the last-changed timestamp and skip an unchanged delta. Default `true`. |
| `objects[].name`, `context` | The provider. |
| `objects[].subscriber_process` | The ODQ subscription key. Set it when two connections read the same provider. |
| `objects[].columns` | Projection. |

Full guide: [incremental sync](incremental.md).

## `odp_odata` — ODP over the Gateway

| Field | Description |
|---|---|
| `service_pattern` | Catalog discovery, where the Gateway supports it. |
| `max_page_size` | Records per OData page. |
| `objects[].url` | Entity-set URL, absolute or relative to `base_url`. Confined to the Gateway host. |
| `objects[].entity_set` | Stream name. |
| `objects[].primary_key` | Fields forming the record key. |

Full guide: [incremental sync](incremental.md).

## Limits

Values outside these are clamped rather than rejected:

| Setting | Range |
|---|---|
| `concurrency` | 1–32 |
| `partitions` | 0–64 |
| `threads` (rfc) | 0–32 |
| `threads` (odp_rfc) | 1–32 |
| `fetch_size` | 1–67,108,864 bytes |
| `max_page_size` | 1–100,000 |
