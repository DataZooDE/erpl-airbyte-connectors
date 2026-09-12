# Phase 0 spike findings (2026-09-12, against local `a4h`)

## 1. Extension delivery — trampoline is build-time only
`LOAD erpl` unpacks `erpl_rfc`, `erpl_bics`, `erpl_odp` + `libsapnwrfc.so`,
`libsapucum.so`, `libicu*.so.50` into `<extension_directory>/v1.5.5/linux_amd64/`.

**After that unpack the 180 MB `erpl.duckdb_extension` trampoline is not needed at
runtime** — loading `erpl_rfc`/`erpl_bics`/`erpl_odp` directly works, provided
`LD_LIBRARY_PATH` points at the extension dir (otherwise: *"Could not open the ICU
common library"*). Dropping it saves ~180 MB of image.

Runtime footprint: erpl_rfc 39 MB, erpl_bics 40 MB, erpl_odp 19 MB, erpl_web 43 MB,
SAP/ICU shared objects ~48 MB.

- `allow_unsigned_extensions=true` is still required.
- `PRAGMA platform` = `linux_amd64`; **erpl publishes no linux_arm64** → image is amd64-only.
- Downloaded from `http://get.erpl.io/v1.5.5/linux_amd64/{erpl,erpl_web}.duckdb_extension.gz`.

## 2. `CREATE SECRET` accepts bound parameters
`con.execute("CREATE SECRET a4h (TYPE sap_rfc, ASHOST $h, ...)", {...})` works — no
string interpolation of credentials anywhere.

**But `sap_show_tables` has no `secret` parameter** (only `TABLENAME`, `TEXT`, `THREADS`).
Several functions rely on "best matching in-scope secret", so the connector must create
**exactly one** `sap_rfc` secret per session.

## 3. ODP over RFC — delta state is just the subscriber_process string
`sap_odp_describe(ctx, name)` returns `supports_full`, `supports_delta`, `delta_modes`
and a `fields` LIST(STRUCT) with `technical_name, abap_type, length, decimals, key,
mandatory` — everything needed for a JSON schema *and* the primary key. Use it instead
of `sap_describe_fields` for ODP.

Verified round trip on `ABAP_CDS` / `ZJRODPVSQL$F`:
- run 1 (auto-DELTAINIT): **3001 rows**, columns include `ODQ_CHANGEMODE`,
  `ODQ_ENTITYCNTR`, `ODQ_TSN`, `ODQ_UNITNO`, `ODQ_RECORDNO`
- run 2, same `subscriber_process`: **0 rows**
- `PRAGMA sap_odp_close_delta_cursor(ctx, proc, name)` → `CLOSED`

→ Airbyte state for odp_rfc = `{"subscriber_process": ..., "initialized": true}`. Nothing else.

`ODQ_CHANGEMODE` on the DELTAINIT rows comes back **NULL** (not `'C'`), so treat
NULL/`''`/`'C'`/`'U'` as upsert and only `'D'` as delete.

## 4. ODP over OData — resume by seeding erpl_web's own table
`odp_odata_read(url)` round trip verified: 3001 rows → 0 rows, delta token advances
(`D20260912091347_000023000` → `D20260912091349_000011000`).

erpl_web keeps the token in `erpl_web.odp_subscriptions` in a **persistent DuckDB file**
(it throws on `:memory:`). Two ways to restore it in a fresh container:

- `import_delta_token := <tok>` — **flaky**: failed with *"Failed to read connection
  error"*. The same URL via curl returns 200 but takes ~40 s, so this is an erpl_web HTTP
  read timeout on the slow first delta request, not a malformed URL.
- **INSERT the row into `erpl_web.odp_subscriptions` ourselves — works, and is what we
  use.** Call `odp_odata_list_subscriptions()` once to force schema creation, then
  `INSERT (subscription_id, service_url, entity_set_name, secret_name, delta_token,
  subscription_status, preference_applied, schema_version)`. A fresh `.db` seeded this
  way resumed with 0 rows and advanced the token correctly.

→ Airbyte state for odp_odata = `{"delta_token": ..., "service_url": ..., "entity_set": ...}`.

## 5. Two erpl_web robustness traps
- **`odp_odata_show` fails against a4h**: the Gateway catalog service returns 403 to
  erpl_web while curl gets 200 on the identical URL (`;v=2` matrix-parameter handling).
  Worse, the failure is an `INTERNAL Error` that **invalidates the whole DuckDB database**
  ("must be restarted prior to being used again"). → run catalog discovery on a
  **throwaway connection**, and treat it as best-effort: explicit entity-set URLs in the
  config are the supported path.
- Dropping an erpl_web connection at interpreter shutdown crashes with a DuckDB
  assertion. **`con.close()` explicitly** — verified clean.

## 6. Types coming out of DuckDB are not orjson-native
`sap_read_table('SFLIGHT')` yields `datetime.date` and `Decimal`; ODP yields `Decimal`
timestamps (`DEC(21,7)` UTC longs). Confirms `types.py` must coerce at the row boundary
or the CDK silently falls back to `json.dumps` (5–10× slower).

## 7. a4h has content for all four protocols
ODP contexts: `ABAP_CDS`, `BW`, `HANA`, `SAPI`. BW cube `0D_FC_C01$F` exists (BICS),
`ZJRODPVSQL$F` is the delta-capable CDS view, `Z_ODP_DL2_SRV/FactsOfZJRODPVSQL` the
OData service, `SFLIGHT` / `/DMO/*` for RFC.
