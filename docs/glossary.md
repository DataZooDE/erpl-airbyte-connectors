# Glossary

The SAP words this connector's documentation uses, for people who do not use
them daily.

## You need these

**Client** — a self-contained tenant inside one SAP system, identified by three
digits (`100`, `800`). Almost every table carries the client in its key, and a
connection reads exactly one. Two clients in the same system share nothing.

**Application server / `ashost`** — the host that serves RFC connections.
`sysnr` is its two-digit instance number; together they give the port,
`33<sysnr>`.

**Message server / logon group** — a load balancer in front of several
application servers. Use `mshost` + `sysid` + `group` instead of `ashost` +
`sysnr` when your Basis team gives you these.

**SAProuter** — a proxy at the network edge, addressed by a route string like
`/H/router.example.com/S/3299`. It is not a hostname.

**RFC** — Remote Function Call, SAP's own protocol. The connector uses it to
read tables and to call function modules.

**Function module** — a callable routine in ABAP. One flagged *remote-enabled*
can be invoked over RFC. **BAPI** is the name for the subset that forms SAP's
documented business API.

**`S_RFC`** — the authorization object controlling which function groups a user
may call over RFC. The commonest cause of a failed connection test.

**DDIC / data dictionary** — SAP's metadata: which tables exist, their fields
and types. `DD02L` and `DD03L` are its own tables, and are useful for testing
because they are large and present on every system.

**Transparent / pool / cluster table** — storage layouts. Transparent tables map
one-to-one onto database tables; pool and cluster tables do not, which is why
they can only be read through SAP rather than from the database directly.

**CDS view** — a modern view defined in ABAP. Read like a table.

## Business Warehouse

**BW** — SAP's data warehouse. **InfoProvider** is a queryable object in it;
**cube** is the classic kind.

**BEx query** — a saved query over an InfoProvider, with axes and filters
already defined. **BEx variable** is a parameter it expects — some are
*mandatory*, and BW returns nothing until they are given a value.

**Characteristic / key figure** — a dimension (customer, month) and a measure
(revenue, quantity). Characteristics go on the row axis, key figures on the
column axis.

**BICS** — the protocol for reading BW query results.

## Change data

**ODP** — Operational Data Provisioning, SAP's own framework for delta
extraction. It works across several **contexts**: `BW`, `ABAP_CDS`, `SAPI`
(classic extractors), `SLT`, `HANA`.

**ODQ** — the Operational Delta Queue, where ODP keeps the changes a subscriber
has not yet consumed. **`ODQMON`** is the transaction for inspecting it.

**Subscription / subscriber process** — a registered consumer of an ODP
provider. SAP remembers how far that consumer has read, so the connector stores
only the subscription's name, not a position. Deleting a connection without
resetting its stream leaves the subscription behind, still retaining data.

**DELTAINIT** — the first delta run, which returns a full snapshot *and*
registers the subscription.

**Delta token** — the equivalent position for ODP over OData, which the
connector does carry in Airbyte state.

## Gateway

**SAP Gateway** — the HTTP layer that serves OData services. Confusingly, "RFC
gateway" is a different thing entirely: the process that brokers RFC
connections.

**Entity set** — one collection in an OData service, the rough equivalent of a
table.

## This connector's own words

**Protocol** — which of the five ways into SAP a connection uses. Chosen once,
per source.

**Stream** — one Airbyte dataset: a table, a function-module call, a BW query,
an ODP provider or an entity set.

**Projection** — restricting the columns read. Pushed into SAP, and the most
effective tuning setting available.

**Partition** — splitting one table scan into row ranges read in parallel. Off
by default; see [performance](performance.md) for why.
