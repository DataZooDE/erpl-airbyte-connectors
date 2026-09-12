# Calling function modules

The `rfc_invoke` protocol turns a call to a remote-enabled function module into
an Airbyte stream. One module, with your parameters, per stream.

```json
{
  "protocol": {
    "mode": "rfc_invoke",
    "objects": [
      {
        "name": "flights",
        "function": "BAPI_FLIGHT_GETLIST",
        "path": "/FLIGHT_LIST",
        "parameters": { "AIRLINE": "LH", "MAX_ROWS": 1000 },
        "primary_key": ["AIRLINEID", "CONNECTID", "FLIGHTDATE"]
      }
    ]
  }
}
```

## Two things to understand first

### Only what you list is ever called

There is no pattern discovery here, deliberately. Finding function modules by
pattern would mean calling them to see what they do, and the connector cannot
tell a module that reads from one that writes. **Choosing read-only modules is
your responsibility.** Scope the `S_RFC` authorization to the function groups
you actually need and no more — see [authorizations](authorizations.md).

### Discovering a module never executes it

Schemas come from `sap_rfc_describe_function`, which reports each parameter's
type. The connector hands that type to DuckDB to parse into columns, so
refreshing the schema of `BAPI_…_CREATE` describes it without calling it.

## Choosing what becomes the rows

**`path`** names the result parameter whose rows become records:

```json
{ "function": "BAPI_FLIGHT_GETLIST", "path": "/FLIGHT_LIST" }
```

Leave `path` empty and the stream is the module's **scalar export parameters** —
exactly one record per sync. That is the right shape for a module that answers a
question rather than returning a list:

```json
{ "name": "ping", "function": "STFC_CONNECTION",
  "parameters": { "REQUTEXT": "hello" } }
```

A `path` naming no result parameter is a configuration error, reported at
discovery with the available names listed.

## Parameters

Written as JSON, and cast to the type the module declares. Dates are accepted in
either SAP's form or ISO:

```json
{ "parameters": { "FLIGHTDATE": "20260102", "MAX_ROWS": 100 } }
```

Nested SAP structures and table parameters are JSON objects and arrays:

```json
{
  "parameters": {
    "DESTINATION_FROM": { "AIRPORTID": "FRA" },
    "DATE_RANGE": [ { "SIGN": "I", "OPTION": "BT",
                      "LOW": "20260101", "HIGH": "20261231" } ]
  }
}
```

Parameter names are validated against the module's interface at discovery and
re-spelled the way SAP spells them, so a lower-case name is accepted rather than
failing later at the call.

## Errors are not silent

A BAPI reports failure by *returning* — `TYPE = 'E'` or `'A'` in its `RETURN`
table — while answering `RFC_OK`. A reader that only looks at the payload turns
that into an empty stream and a green sync.

The connector inspects the return table before emitting anything and fails the
stream with the SAP message attached. It recognises `RETURN`, `E_RETURN`,
`ET_RETURN`, `EX_RETURN`, `T_RETURN` and `RETURN_TAB`, case-insensitively. If
your module uses something else, name it:

```json
{ "return_parameter": "ZZ_MESSAGES" }
```

If a module has a return-shaped table under a name the connector does not
recognise, discovery fails rather than reading it as an empty result.

## Slicing

Call the module once per value of one parameter, in parallel, and union the
results:

```json
{ "slice_by": { "parameter": "AIRLINE", "values": ["LH", "AA", "UA"] } }
```

Useful for a BAPI that accepts only one company code, plant or airline per call.

## Incremental sync

Set both a **cursor field** (a result column) and a **cursor parameter** (the
import parameter the stored value is passed to next run):

```json
{ "cursor_field": "FLIGHTDATE", "cursor_parameter": "DATE_FROM" }
```

Only useful for a module that accepts a lower bound. If yours does not, this is
full refresh.

## Limits

The whole result of a call is materialised — that is how RFC works, not a choice
the connector makes. For large extracts use [tables](tables.md) or
[ODP](incremental.md) instead.

## See also

- [Authorizations](authorizations.md) — scoping `S_RFC` to the modules you call
- [Reference](reference.md) — every field of the `rfc_invoke` protocol
