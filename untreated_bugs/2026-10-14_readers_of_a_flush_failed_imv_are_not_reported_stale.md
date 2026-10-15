# Readers of an IMV whose DEFERRED flush failed are not reported stale

**Severity:** silent wrong result downstream of a flagged IMV (the failing IMV itself is
`known_stale` with a precise reason; its transitive readers are not).
**Found:** 2026-10-14 (ledger `2026-10-14-1.12.2-bulk-leaf-rebuild/C1-investigation.txt`).

The triggering cause seen there — an empty `reflex.wipe_threshold` after `SET LOCAL`/`RESET`
raising 22P02 in the flush — is fixed in 1.12.2 (branch `fix/v1122-guc-empty-cast`). The residual:
when any flush failure marks an IMV stale under `flush_failure_policy='warn'`, its delta is
discarded and its readers keep stale rows without a flag; `reflex_ivm_status` only propagates
heal-queue reasons downstream.

Remedy shown to converge: `reflex_reconcile('<flagged IMV>')` repairs it and all its readers.
Fix direction: propagate flush-failure staleness through `graph_child` in `reflex_ivm_status`,
naming the upstream reconcile as the remedy (it must clear the readers' finding).

Same residual as `2026-10-10_failed_flush_flags_only_the_failing_imv_stale_not_its_dependents.md` (DEFERRED delta flush part); merge the two when either is worked on.
