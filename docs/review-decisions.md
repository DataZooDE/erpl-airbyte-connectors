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
