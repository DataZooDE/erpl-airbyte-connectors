# What your Basis team will ask

The connector logs on as an ordinary SAP user over RFC and calls a fixed,
enumerable set of function modules. Nothing is generated, nothing is installed
on the SAP side, and no transport is required. This page is written to be handed
to whoever grants the authorization.

## The RFC user

A **communication user** (type `C`) is enough and is the right choice: it cannot
log on interactively, so the credentials are useless for anything else.

| Field | Value |
|---|---|
| User type | Communication (`C`) |
| Client | the client you will replicate from |
| Language | any; it only affects the language of field texts |
| Password | no expiry, or the sync fails the day it lapses |

## `S_RFC` — the authorization that matters

Every call the connector makes goes through `S_RFC`, checked per **function
group**. Granting `S_RFC` with `RFC_NAME = *` works and is what most people do
first; the list below is what least privilege looks like instead.

These are the exact modules, per protocol. They come from ERPL's own
`sap_rfc_authorizations()`, which is a static declaration rather than an
observation, so it is complete.

### Tables and CDS views (`rfc`)

```
DDIF_FIELDINFO_GET        field metadata
RFC_READ_TABLE            the read itself
RFC_FUNCTION_SEARCH       discovery
RFC_GROUP_SEARCH          discovery
```

`RFC_READ_TABLE` has vendor variants that some systems prefer. They are used
only if you name one in `read_table_function`:

```
/BODS/RFC_READ_TABLE       /BODS/RFC_READ_TABLE2
/SAPDS/RFC_READ_TABLE      /SAPDS/RFC_READ_TABLE2
```

### Function modules (`rfc_invoke`)

The four above, plus `RPY_FUNCTIONMODULE_READ` to read a module's interface —
**and the modules you configure**. Those are the whole point, so they are
yours to name; the connector calls nothing it was not told to call.

### BW queries (`bics`)

```
BICS_CONS_CREATE_DATA_AREA     BICS_PROV_OPEN
BICS_PROV_GET_DESIGN_TIME_INFO BICS_PROV_GET_INITIAL_STATE
BICS_PROV_GET_RESULT_SET       BICS_PROV_SET_STATE
BICS_PROV_VAR_GET_VARIABLES    BICS_PROV_CLOSE
BAPI_IOBJ_GETDETAIL            RSNDI_SHIE_STRUCTURE_GET3
RSOBJS_GET_NODES               DDIF_FIELDINFO_GET, RFC_READ_TABLE
```

### ODP over RFC (`odp_rfc`)

```
RODPS_REPL_CONTEXT_GET_LIST    RODPS_REPL_ODP_GET_LIST
RODPS_REPL_ODP_GET_DETAIL      RODPS_REPL_ODP_OPEN
RODPS_REPL_ODP_FETCH_XML       RODPS_REPL_ODP_READ_DIRECT_XML
RODPS_REPL_ODP_CLOSE           RODPS_REPL_ODP_RESET
RODPS_REPL_ODP_GET_SUBSCR      RODPS_REPL_CURSOR_GET_LIST
RODPS_REPL_ODP_GET_LAST_MODIF  DDIF_FIELDINFO_GET, RFC_READ_TABLE
```

`RODPS_REPL_ODP_RESET` deletes a subscription. It is only called when you reset
a stream; if your policy forbids it, withhold it and clear subscriptions in
`ODQMON` by hand instead.

### ODP over OData (`odp_odata`)

No RFC at all. This one needs an activated OData service and authorization for
it (`S_SERVICE`), which is a Gateway question rather than an RFC one.

## Table-level authorization

`RFC_READ_TABLE` does not check `S_TABU_DIS` or `S_TABU_NAM` on its own — the
authorization for *which* tables may be read is a separate decision, and on many
systems it is enforced by a wrapper module instead. If your system uses one, set
`read_table_function` to it.

## Network and transport

| | |
|---|---|
| Direct logon | TCP to the application server, port `33<sysnr>` |
| Load-balanced | port `36<sysnr>` to the message server as well |
| SAProuter | one connection to the router port, usually `3299` |
| ODP over OData | HTTPS to the Gateway |

**SNC is off by default**, matching SAP's own default, which means the password
and all extracted data travel in the clear. For anything but a test system, set
`snc_mode` to `1` with an SNC library and partner name. The connector logs a
warning at connection-check time when SNC is off or the Gateway URL is plain
`http`.

## What the connector never does

- No writes, ever — except through a function module *you* configured. The
  connector cannot tell a read-only BAPI from a writing one, which is why the
  module list is yours and never discovered by pattern.
- No transport, no generated ABAP, no changes to SAP objects.
- No calls during schema discovery. Function-module schemas come from the
  interface metadata, so discovering a module never executes it.

## See also

- [Troubleshooting](troubleshooting.md) — what a missing authorization looks like
- [Incremental sync](incremental.md) — what a delta subscription leaves on SAP
