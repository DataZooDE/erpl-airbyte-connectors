# Troubleshooting

The errors you will actually see, in rough order of how often they happen.

## The connection test fails

### `RFC_LOGON_FAILURE: Name or password is incorrect`

What it says. Check the **client** too — the same user in a different client is
a different user, and this is the commonest version of this error.

### `RFC_COMMUNICATION_FAILURE: Timeout while opening an RFC connection`

The host or port is unreachable. RFC is on `33<sysnr>`, so `sysnr: "00"` means
port 3300. From the machine running Airbyte:

```bash
nc -vz sap.example.com 3300
```

Behind a SAProuter, the route string goes in `saprouter` and the router's own
host in `saprouter_host` — the platform needs a bare hostname for egress and a
`/H/…/S/…` string is not one.

In Docker, `localhost` is the container. Use the host's address.

### `No authorization to call ... / NO_AUTHORITY`

`S_RFC` does not cover a function group the protocol needs.
[Authorizations](authorizations.md) lists exactly which modules each protocol
calls; hand that page to whoever grants it.

### `Could not open the ICU common library`

The SAP shared libraries are not on `LD_LIBRARY_PATH`. Inside the published
image this is set for you; if you are running from a checkout, see
[development](development.md).

## Discovery finds nothing

**`No SAP table matched the pattern …`** — patterns use SAP wildcards (`*`) and
are case-sensitive. `sflight` does not match `SFLIGHT`.

**ODP: `No ODP providers selected`** — set `context` as well as `name_pattern`.
Contexts are listed by the connection test.

**ODP over OData: catalog discovery found nothing** — the Gateway catalog
service is not reachable on every release, and the connector says so rather than
failing. List the entity-set URLs explicitly under `objects`.

**BICS: mandatory variables are not bound** — BW returns nothing until they have
values. The error names them; add them to the object's `variables`.

## A sync succeeds but returns nothing

**A BAPI reported an error.** It should not be silent — the connector inspects
the return table and fails the stream. If yours names that table something
unusual, set `return_parameter`; see [function modules](function-modules.md).

**An ODP delta had nothing to send.** Expected: the second run after a DELTAINIT
returns zero rows if nothing changed. The log says whether the read was skipped
after the last-changed probe.

**A cursor-based incremental has caught up.** Also expected.

## A sync is slow

Project your columns first — it is worth 3.8x on a wide table. Then push filters
into SAP. Do not reach for `partitions`; it is off by default because it
measured slower, and [performance](performance.md) has the numbers.

For ODP, a long silence before the first row is SAP preparing the extraction,
not a hang.

## Errors specific to ODP

**`ILLEGAL_REQ_STATE_FOR_CONFIRM` when closing a cursor** — a previous run was
interrupted mid-fetch. The connector reports it rather than pretending the close
worked; the next run recovers, or `PRAGMA sap_odp_drop` clears it.

**Two connections seeing partial changes** — they share a subscriber process.
Give each an explicit `subscriber_process`. See
[incremental sync](incremental.md).

**A subscription for a connection that no longer exists** — stranded. See
[operations](operations.md).

## Errors specific to BICS

**`No BICS state found for id …`** — the session was not opened before the
result was read. That is a connector bug rather than a configuration problem;
please report it with the query name.

**A result-size refusal** — BW builds the whole result set or none of it. Use
`slice_by` to split the query on a characteristic; see
[BW queries](bw-queries.md).

## Getting help

Include the protocol, the connector version, and the `internal_message` from the
failing trace — it carries the SAP message that the user-facing text summarises.
