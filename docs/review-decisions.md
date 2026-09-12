# Review decisions

Findings from the multi-agent review rounds that were considered and **not**
acted on, with the reason. Recorded so later rounds do not re-litigate them.

| Finding | Round | Decision |
|---|---|---|
| `allow_unsigned_extensions=true` is arbitrary native code execution | 1 (security) | **Accepted risk.** ERPL is unsigned and cannot be loaded otherwise; the alternative is not shipping the connector. Mitigated instead: extensions are baked at build time from HTTPS with pinned checksums, and the extension directory is owned by the image user. Documented in the user-facing docs. |
| `snc_mode` defaults to `"0"` (unencrypted RFC) | 1 (security) | **Kept.** This is SAP's own default, and forcing SNC on would break every local/trial setup with no way to opt out. Handled by warning loudly at `check` time and documenting it as production-wrong. |
| A `${base_url}`-derived host cannot work in `allowedHosts` | 1 (security) | **Not a defect.** The platform's `ConfigReplacer` sanitizes each resolved value down to a bare hostname, so a full URL resolves correctly. The separate `gateway_host` field added in response to this was reverted. `saprouter` genuinely does not survive sanitization, so `saprouter_host` stays. |
| `_HEARTBEAT_SECONDS` only writes a log line, which does not reset the platform's `maxSecondsBetweenMessages` budget | 1 (testing) | **Not a defect.** A CDK `logger.info` call *is* a protocol LOG message on stdout, which the platform counts. Comment added at the call site so this is not re-raised. |
| `ErplPartitionGenerator` drops the cursor, so `mark_failed` is a no-op | 1 (all four) | **Already fixed** before the review landed — the crew reviewed a WIP commit. Kept as a finding because the regression test it prompted (at the *generator* boundary, not on a hand-built partition) was the real gap. |

## Round 2

| Finding | Decision |
|---|---|
| `concurrency_group` / `block_simultaneous_read` cannot fire: subscriber processes are uniqueness-enforced, so every group is a singleton | **Deleted.** Behaviourally it was already a no-op, but it read as a safety property that isn't one, which invites someone to rely on it. If two streams ever genuinely need to serialise against a shared SAP-side position, it comes back with a test that proves it fires. |
| `FieldValueCursor` should suppress its checkpoint conditionally on plan count rather than always | **Not taken.** Always deferring to end-of-stream is correct for every plan count; making it conditional buys finer checkpointing for single-plan streams at the cost of a rule with two branches, one of which is only exercised by a driver that does not exist yet. Revisit if an incremental single-plan stream is ever slow enough to care. |
| Volume tests should move off every-push CI | **Not taken here.** They are already `-m slow` and run in a separate CI step, so a push pays for them only on the self-hosted SAP runner that can actually run them. Moving them to a schedule would mean a six-figure regression could sit unnoticed for a day. |

## Round 3

| Finding | Decision |
|---|---|
| Resumable full refresh emits its checkpoint from a worker thread into `InMemoryMessageRepository`, which has no ordering guarantee against the 10,000-item record queue — so a STATE can reach stdout ahead of thousands of the records it covers, and a crash after it skips them permanently | **Feature withdrawn.** Confirmed real by reading `PartitionReader.process_partition`: records go on the queue while `cursor.observe` runs on the same worker thread, and the main thread drains the repository after whatever record it is currently on. The correct fix needs a message repository sharing the record queue; CDK 7.28.3 has none. Without mid-read checkpointing the feature does nothing (round 1's finding), and with it the feature can silently skip rows — which is worse than not having it. `ResumeKeyCursor`, `checkpoint`, `RESUME_FIELD`, the resume predicate and their tests are gone. Revisit if the CDK grows an ordering-safe repository. |
| The resume predicate compared as a string while the cursor tracked the maximum numerically | Moot — withdrawn with the feature. It was real. |
| The resume design rests on an unproven SAP row-ordering contract | Moot — withdrawn with the feature. The e2e test on `SCARR` was weak evidence for a system-wide guarantee. |
| `docs/performance.md` cites figures the harness cannot produce, and quotes numbers that are arithmetically impossible against each other | **Rewritten.** The raw-DuckDB probes used different queries (`count(*)`, a `CAST` to VARCHAR) that were not measuring comparable work, which is how three successive explanations came out wrong. The document now contains only figures a benchmark case produces, and the explanation for why partitioning does not help is withdrawn rather than replaced — it is not established. |

## Documentation review

| Finding | Decision |
|---|---|
| `read_table_function` documented in two places but exists nowhere in the spec or the code | **Removed.** Invented while writing the page, and in the one document meant to be handed to a Basis team. The pages now say the capability exists in ERPL but the connector does not yet expose it. |
| The documented SNC / plain-`http` warning was never emitted | **Fixed in code.** The hook, its `odp_odata` override and its unit tests all existed; nothing called it. Now called from `check_connection`, before the connection is attempted so it appears even when the check then fails. |
| BICS incremental requires a `primary_key` the spec could not express | **Fixed in code.** The feature was unconfigurable from the Airbyte UI. Added to every protocol's object list, with a test. |
| `fetch_size` described as rows; the code budgets bytes | **Fixed in code.** Every reader of the form would have sized it against their row count. |
| `_ab_cdc_deleted_at` promised but not declared in the stream schema | **Fixed in code.** A typed destination may drop a field the schema does not declare, and the tombstone is the reason to choose ODP. Verified present in a real discovered schema. |
| `reference.md` claimed to be generated and complete; it was neither | **Fixed both ways.** The claim is corrected, four supported-but-hidden fields (`return_parameter`, BICS `filters`/`variant`/`properties`, ODP `filters`) are now in the spec, and a test fails if the page and the spec disagree. |
| `RODPS_REPL_ODP_RESET` described as called on stream reset | **Corrected.** The connector never calls it. It is listed in the authorization page as destructive and safely withheld, because it would otherwise appear unexplained in a trace. |
| ERPL-side and SAP-side strings stated flatly, unverifiable from this repo | **Attributed.** The module lists now name ERPL v2026.09.04 as their source, and a test fails if a later version declares something the page omits. |

## Round 4

| Finding | Decision |
|---|---|
| `sap_date` and `sap_timestamp` accept digit-only input of the wrong length: `strptime` reads `2026012` as 2026-01-02 and `202601021030` as 10:03 | **Fixed.** The only confirmed data-corruption finding in the round: a typo in `cursor_start` sent SAP a different selection and the sync stayed green. `sap_time` had the length rule already; it now lives in the shared parser and covers all three, with the reproducing strings as tests. |
| The fetch-budget comment claims ~187 rows per call, but the multiplier shipped (× partitions) measured 94 | **Comment corrected, multiplier kept.** 32× reached 187, but that is four times the memory per worker for 13% more rows per trip, and the end-to-end figure does not move with it. The test docstring also dropped a "2.3x faster than serial" that was a DuckDB-internal measurement, not a connector one. |
| Three of four doc-accuracy test classes need only the filesystem, yet sat in `e2e/` where they run only with credentials | **Moved to `unit_tests/`.** They now run on every commit; the one class that needs a live ERPL to enumerate function modules stays in `e2e/`. |
| `fetch_size: 0` clamps up to a one-byte budget | **Fixed.** Falsy means unset. An explicit budget that leaves a partition worker under 512 KB now warns, naming the stream and the arithmetic. |
| `reference.md` documents `threads` for RFC; only the `odp_rfc` spec branch offered it | **Fixed in the spec,** since the RFC driver honours it. The page-to-spec check now walks each protocol branch separately — pooling field names across the five branches is what hid this, and the first version of the test I wrote had that same hole. |
| `_ab_cdc_deleted_at` is declared by `build_stream` but `discover` publishes the shim's schema, which omits it | **Fixed.** The shim routes through `declare_cdc_column`. Both ODP e2e discover tests assert it and pass against the trial system. |
| `ReadPlan.slice_` carries machinery: BICS put its DuckDB session id in the partition identity | **Fixed, and generalised.** `slice_` is what the slice logger prints, so it holds names a person can act on; the session id moved to `meta`. A conformance test walks every driver and rejects a machinery key, a non-scalar value, or two partitions sharing one identity. |
| The measurement tools are fragile: `trace-round-trips.py` leaks a connection on interrupt, `benchmark.py`'s quiet-system check was narrowed to two pytest paths | **Both fixed.** The narrowing meant it stopped noticing `./bin/test-e2e.sh` — which is exactly the contention that produced this project's first wrong performance table. The check is deliberately broad again; a false positive costs one `--allow-busy`. |
| Leftovers from the withdrawn resumable full refresh; link checker skips `source-sap/**/*.md`; an over-long cursor value reaches SAP; the memory envelope is undocumented | **All fixed.** The cursor bound is 255 characters, against SAP's 72-character `WHERE` lines; `docs/operations.md` now states what `fetch_size` × `concurrency` means for the container. |

## Round 5

| Finding | Decision |
|---|---|
| A failed ODP delta sync leaves the SAP-side cursor open, and `docs/operations.md` claims the opposite in two places | **Fixed.** `on_success` carried both the checkpoint and the cleanup, and a failed stream reaches neither. Drivers now have a `release` hook that runs however the read ended. Safe by ERPL's own construction: `RODPS_REPL_ODP_CLOSE` closes without confirming, and a cursor left mid-fetch is refused rather than consumed. The e2e test asserts the *attempt*, not zero open cursors — what SAP answers depends on where the failure landed, so asserting the count would pass for reasons unrelated to the fix. |
| `sap_cursor_literal` truncates malformed state instead of rejecting it | **Fixed.** `'20260905120000'` on a DATS cursor became `'20260905'`: a selection the state never described, sent with the sync green. Round 4 fixed this class in `sap_values` and left the one path already hardened against untrusted state still slicing. My own round-4 test had pinned the truncation as intended. |
| Catalog-discovered OData URLs bypass the confinement configured URLs go through | **Fixed.** One shared `_resolve_url`. A Gateway's catalogue is input, not authority. |
| BICS opens a BW session per slice and never closes it | **Largely declined, with evidence.** ERPL closes each data provider per statement — its own comment reads "a full CREATE_DATA_AREA + OPEN + SET_STATE + CLOSE cycle on a fresh RFC connection", and `BicsSession`'s destructor closes all three handles under `try`. The connector holds no BW session to leak, which is why no close primitive is exposed in SQL. What *was* wrong: `operations.md` described a server-side timeout. `BICS_PROV_CLOSE` stays in `authorizations.md` — ERPL calls it, and the page now says so. |
| The slice-identity conformance test says "every driver" and covers three of five | **Fixed.** Adding the two missing ones failed immediately, exactly as reported: an unsliced `rfc_invoke` partition had no identity. A test now keeps the case list in step with `DRIVERS`. |
| `odp_odata` advertises incremental unconditionally while the tombstone is conditional | **Fixed by warning, not by refusing.** An entity set without change tracking is a legitimate append-only feed; removing incremental would break working syncs. Discovery says so once, and the docs say where the promise ends. |
| Two RFC volume comparisons pass on two empty results | **Fixed.** Row-count guards on both sides, `errors()` on both runs. Writing the guard found a second defect in my own first attempt: `MIN_BENCH_ROWS` was the wrong bound for a read capped at `max_rows`. |
| State reaches SAP unbounded at the BICS and `rfc_invoke` sinks | **Fixed.** One guard in `sap_values`, applied at all three, with a parameterised test so a fourth sink fails the file. |
| The cursor high-water mark keys on `float`, so large NUMC/DEC cursors stall | **Fixed** with `Decimal`, plus the non-finite values `Decimal` happily parses. |
| `describe_failure` reads BAPIRET keys case-sensitively while `is_bapi_failure` does not | **Fixed.** A classified failure that describes itself as nothing is worse than no check. |
| The BICS mandatory-variable gate fails open against a documented refusal | **Qualified, not closed.** BW does not enumerate variables for every query, and refusing there would break working configurations — but it now warns, naming the query, and the docs say when the check does not run. The test also found the reverse defect: a configured `variant` binds variables server-side and was being refused. |
| Two doc tests match substrings page-wide, so they pass on an incidental mention | **Fixed.** The reference check is per section; the spec/driver check reads the module's AST for an actual config read rather than a mention. The tightened checks then found three real gaps: `primary_key` undocumented for `rfc` and `odp_rfc`, `objects[].name` undocumented for `bics`. |
| `performance.md`'s pre-fix column has no named source; `benchmark.py`'s docstring points `--markdown` at the hand-written page; `DEFAULT_FETCH_SIZE` rests on a comment | **All three fixed:** the column is attributed to the commit it came from, the docstring points at `performance-raw.md`, and the constant names the ERPL version it was read from, with `trace-round-trips.py` as the check. |
| `read_plans` falls back to an unvalidated `cursor_field` | **Fixed at the sink**, and the predicate now uses the *checked* spelling — validating one string and sending another is how a check gets bypassed. |
