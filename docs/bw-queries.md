# BW queries and cubes

The `bics` protocol reads SAP BW InfoProviders and BEx queries.

```json
{
  "protocol": {
    "mode": "bics",
    "objects": [
      {
        "name": "sales_by_month",
        "cube": "0D_NW_C01",
        "query": "ZSALES_Q01",
        "rows": ["0CALMONTH", "0MATERIAL"],
        "columns": ["0AMOUNT"],
        "variables": [ { "name": "ZVAR_YEAR", "low": "2026" } ]
      }
    ]
  }
}
```

## Choosing objects

`query_pattern` discovers BEx queries or InfoProviders in bulk; `objects` names
them explicitly and carries the per-object settings. A query with **mandatory
BEx variables must be listed explicitly with those variables bound** — BW
refuses to return a result until they have values, so there is nothing sensible
to discover.

## Axes

**`rows`** are the characteristics (customer, month, material) and **`columns`**
the key figures (amount, quantity). Naming `rows` also lets the connector
recognise and drop BW's grand-total row, which it appends to every result and
which is not a fact.

Without `rows`, the total row is still filtered, but by checking every string
column against a small set of labels rather than just the row axis — slightly
blunter, so name your row axis.

## Variables

```json
{ "variables": [
    { "name": "ZVAR_YEAR",  "low": "2026" },
    { "name": "ZVAR_MONTH", "low": "202601", "high": "202612", "op": "BT" }
] }
```

`sign` defaults to `I` (include) and `op` to `EQ`, or `BT` when a `high` is
given. Repeat a name to fill a multi-value variable.

The connector checks that every *mandatory, input-ready* variable of a BEx query
has been given a value, and refuses the configuration otherwise — BW would
return nothing and the reason would not be obvious. It does not police the
reverse: a name the query does not expose is passed to BW, which ignores it.

Two cases skip the check rather than fail it. A `variant` fills the variables on
the BW side, and which ones it fills is not visible from here. And BW does not
enumerate variables for every query — where the introspection call fails, the
connector logs a warning naming the query and continues, because refusing there
would break configurations that work. A BICS sync that returns no rows with such
a warning in its log is an unbound mandatory variable until proven otherwise;
`RSRT` shows the truth.

## The memory problem, and slicing

**BW builds the entire result set or none of it.** There is no pagination in the
protocol, so a large query either fits in memory or is refused.

Slicing is the answer: run one BICS session per member of a characteristic.

```json
{ "slice_by": { "characteristic": "0CALMONTH",
                "members": ["202601", "202602", "202603"] } }
```

Each slice is a separate session and they run in parallel, so this bounds memory
*and* usually finishes sooner.

## Incremental

BW exposes no change tracking. The available approximation is a BEx variable
used as a watermark — see [incremental sync](incremental.md). It requires a
primary key, because the `>=` selection re-reads the boundary period every run.

## Hierarchies and metadata

Not currently exposed. The ERPL extension offers hierarchy extraction, metadata
views and lineage (`sap_bics_hierarchy`, `sap_bics_meta_*`,
`sap_bics_lineage_*`); if you need them through Airbyte, say so.

## See also

- [Authorizations](authorizations.md) — the BICS function group
- [Glossary](glossary.md) — InfoProvider, characteristic, key figure, BEx
