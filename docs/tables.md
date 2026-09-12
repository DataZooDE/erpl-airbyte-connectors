# Reading tables and CDS views

The `rfc` protocol reads any transparent, pool or cluster table, and any CDS
view, through `RFC_READ_TABLE`.

## Selecting what to read

Two mechanisms, and you can use both:

```json
{
  "protocol": {
    "mode": "rfc",
    "table_pattern": "/DMO/*",
    "objects": [
      { "name": "SFLIGHT", "columns": ["CARRID", "CONNID", "FLDATE", "PRICE"] }
    ]
  }
}
```

**`table_pattern`** discovers tables in bulk. It uses SAP wildcards — `*` for any
sequence — and is case-sensitive. Discovery asks SAP for the field list of every
match, so a pattern like `*` on a production system is a very long operation;
narrow it.

**`objects`** names tables explicitly, and is where per-table settings live. A
table listed here is exposed whether or not the pattern matches it.

## Making it fast

### Columns — do this first

```json
{ "name": "DD02L", "columns": ["TABNAME", "TABCLASS"] }
```

The projection is pushed into SAP, so the unread columns never cross the wire.
Measured at **3.8x** on a 55-column table. Nothing else here comes close.

### Filters

```json
{ "name": "SFLIGHT", "filter": "CARRID = 'LH' AND FLDATE >= '20260101'" }
```

An ABAP `WHERE` fragment, evaluated by SAP. Rows that do not match never reach
the connector. Note the ABAP literal syntax — dates are `'20260101'`, not
`'2026-01-01'`.

Airbyte's own filters are applied after the fact, so this is the one that saves
work.

### Partitions

Off by default, and the measurements say leave it that way — see
[performance](performance.md). If you raise it, the connector scales the SAP
fetch budget to match, so you do not have to.

## Incremental sync

Nominate a column whose value only goes up:

```json
{ "name": "SFLIGHT", "cursor_field": "FLDATE" }
```

The connector keeps the highest value it has seen and pushes `FLDATE >= …` into
SAP on the next run. The cursor field must be a real field of the table —
checked at discovery — and its value is converted back to SAP's own format, so
a `DATS` column is filtered as `20260102` even though the state holds
`2026-01-02`.

This is a high-water mark, not change tracking: rows deleted in SAP stay in your
destination, and a row whose cursor value goes *backwards* is missed. For genuine
change data use [ODP](incremental.md).

## Schemas and keys

Field metadata comes from `sap_describe_fields`, which gives names, texts, DDIC
types and which fields are key. That becomes the JSON Schema and the stream's
primary key.

DDIC types map as SAP means them, not as they look. The cases worth knowing —
the full table is in [`source_sap/types.py`](../source-sap/source_sap/types.py):

| SAP | JSON | Why |
|---|---|---|
| `NUMC`, `ACCP` | string | leading zeros are significant |
| `DEC`, `CURR`, `QUAN` | number (`big_number`), or integer when the field has no decimal places | emitted as an exact decimal string, never a float |
| `DATS` | string, `format: date` | |
| `TIMS` | string, `format: time` | |
| `RAW`, `RAWSTRING` | string, base64 | |
| `CLNT` | string | the client, constant for a connection |

Types not in that table — `CHAR`, `INT4`, `FLTP` and the rest — map the obvious
way. One SAP does not have an obvious mapping for degrades to string with a
warning rather than failing discovery.

## Limits

- `RFC_READ_TABLE` returns rows as fixed-width text, so very wide rows can be
  truncated by SAP. If you hit that, project fewer columns.
- Some systems wrap `RFC_READ_TABLE` for authorization reasons, or prefer a
  vendor variant. The ERPL extension supports several, but the connector does
  not yet expose the choice — open an issue if your system needs one.

## See also

- [Authorizations](authorizations.md) — what `S_RFC` needs to allow
- [Performance](performance.md) — the measurements behind the advice above
- [Reference](reference.md) — every field of the `rfc` protocol
