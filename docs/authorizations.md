# What your Basis team will ask

This page is written to be handed to whoever grants the authorization. It should
be enough to build the role without coming back with questions.

The connector logs on as an ordinary SAP user over RFC and calls a fixed,
enumerable set of function modules. Nothing is generated, nothing is installed on
the SAP side, and no transport is required.

## The RFC user

A **communication user** (`SU01`, user type `C` — *Communications Data*) is
enough and is the right choice: it cannot log on interactively, so the
credentials are useless for anything else.

| | |
|---|---|
| User type | Communication (`C`) |
| Client | the client to be replicated |
| Password | no expiry, or the sync fails the day it lapses |
| Language | any; it only affects the language of field texts |

## The role

One role, containing `S_RFC` plus whatever table-level object your system uses.

### `S_RFC` — the one that matters

Checked per **function group**, on every call.

| Field | Value |
|---|---|
| `RFC_TYPE` | `FUGR` (function group) |
| `RFC_NAME` | see the table below, per protocol |
| `ACTVT` | `16` (Execute) |

`RFC_NAME = *` works and is what most people do first. The lists below are what
least privilege looks like instead. They are the modules declared by **ERPL
v2026.09.04**, the version this connector embeds; a test in this repository
fails if a later version declares one this page omits.

`S_RFC` is checked on the *function group*, not the module. If your naming tool
wants groups rather than modules, look each one up in `SE37` — they differ
between releases, which is why this page lists modules.

### Tables and CDS views — protocol `rfc`

```
DDIF_FIELDINFO_GET        field metadata
RFC_READ_TABLE            the read itself
RFC_FUNCTION_SEARCH       discovery
RFC_GROUP_SEARCH          discovery
```

ERPL can also read through vendor variants of `RFC_READ_TABLE`, and declares
them for completeness:

```
/BODS/RFC_READ_TABLE      /BODS/RFC_READ_TABLE2
/SAPDS/RFC_READ_TABLE     /SAPDS/RFC_READ_TABLE2
```

**The connector does not currently let you select one**, so there is no need to
grant them. They are listed here only because they would show up in an
authorization trace of the extension.

### Function modules — protocol `rfc_invoke`

The four above, plus:

```
RPY_FUNCTIONMODULE_READ   reads a module's interface, so schemas need no call
```

**…and the modules the connector is configured to call.** Those are the point of
this protocol, so they are named by whoever configures the source. The connector
calls nothing it was not told to call, and never calls anything during schema
discovery.

### BW queries — protocol `bics`

```
BICS_CONS_CREATE_DATA_AREA      BICS_PROV_OPEN
BICS_PROV_GET_DESIGN_TIME_INFO  BICS_PROV_GET_INITIAL_STATE
BICS_PROV_GET_RESULT_SET        BICS_PROV_SET_STATE
BICS_PROV_VAR_GET_VARIABLES     BICS_PROV_CLOSE
BAPI_IOBJ_GETDETAIL             RSNDI_SHIE_STRUCTURE_GET3
RSOBJS_GET_NODES                DDIF_FIELDINFO_GET
RFC_READ_TABLE
```

BW additionally checks its own analysis authorizations (`S_RS_COMP`,
`S_RS_AUTH`) on the InfoProvider and its characteristics. Those are a BW
question, not an RFC one, and are granted the same way they would be for a human
running the query.

### ODP over RFC — protocol `odp_rfc`

```
RODPS_REPL_CONTEXT_GET_LIST     RODPS_REPL_ODP_GET_LIST
RODPS_REPL_ODP_GET_DETAIL       RODPS_REPL_ODP_OPEN
RODPS_REPL_ODP_FETCH_XML        RODPS_REPL_ODP_READ_DIRECT_XML
RODPS_REPL_ODP_CLOSE            RODPS_REPL_ODP_GET_SUBSCR
RODPS_REPL_CURSOR_GET_LIST      RODPS_REPL_ODP_GET_LAST_MODIF
DDIF_FIELDINFO_GET              RFC_READ_TABLE
```

> **Destructive — grant deliberately.**
> ```
> RODPS_REPL_ODP_RESET            deletes a delta subscription
> ```
> ERPL exposes this as `PRAGMA sap_odp_drop`. **The connector never calls it**;
> it is listed because the extension declares it and it would appear in a trace.
> Withhold it safely — clearing a subscription is then a manual step in `ODQMON`.

ODP also checks `S_RO_OSOA` (DataSource access) on the provider, and for the BW
context the usual BW authorizations.

### ODP over OData — protocol `odp_odata`

No RFC at all. What is needed instead:

| | |
|---|---|
| Service | activated in `/IWFND/MAINT_SERVICE`, with its **service ID** given to whoever configures the source |
| `S_SERVICE` | for the OData service |
| `S_RO_OSOA` | on the underlying ODP provider |
| User | the same communication user works; it authenticates over HTTPS Basic |

## Table-level authorization

`RFC_READ_TABLE` does **not** check `S_TABU_DIS` or `S_TABU_NAM` itself. Which
tables may be read is therefore a separate decision, and if your policy requires
it to be enforced, it has to be enforced by the objects you grant rather than by
the connector. Some systems use a wrapper module for this; the connector does
not yet support selecting one, so tell us if yours does.

## Verifying the role

The reliable loop, rather than guessing:

1. Run the connector's connection test. It fails with the SAP message attached.
2. In SAP, immediately run **`SU53`** as the RFC user — it shows the last failed
   authorization check, which names the object and the value that was missing.
3. For a fuller picture, **`ST01`** (or `STAUTHTRACE`) with the authorization
   trace filtered to the user, then re-run the test.

`SU53` after an RFC failure is the fastest answer and is usually enough.

## Network and transport

| | |
|---|---|
| Direct logon | TCP to the application server, port `33<sysnr>` — `sysnr` `00` is 3300 |
| Load-balanced | plus `36<sysnr>` to the message server |
| SAProuter | one connection to the router port, usually 3299 |
| ODP over OData | HTTPS to the Gateway |

**SNC is off by default**, matching SAP's own default, which means the password
and all extracted data travel in the clear. For anything but a test system, set
`snc_mode` to `1` with an SNC library and partner name. The connector logs a
warning at connection-check time when SNC is off, or when the Gateway URL is
plain `http`.

## What the connector never does

- **No writes** — except through a function module someone configured. The
  connector cannot tell a read-only BAPI from a writing one, which is why the
  module list is explicit and never discovered by pattern.
- **No transport, no generated ABAP, no changes to SAP objects.**
- **No calls during schema discovery.** Function-module schemas come from the
  interface metadata, so refreshing the schema of a writing module describes it
  without executing it.
- **No deletion of ODP subscriptions.** See the note above.

## See also

- [Troubleshooting](troubleshooting.md) — what a missing authorization looks like
- [Incremental sync](incremental.md) — what a delta subscription leaves on SAP
