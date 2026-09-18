# 2026-09-18 — `reflex_reconcile` repeats the PS-14 convergence advisory on every call

**Status: narrowed.** The original report (prod, pg_reflex 1.11.3, PG 17.8: 66 client lines
for one reconcile of a 49-child partitioned IMV) listed five sources of noise. Items 1–3
are fixed in 1.11.4 and pinned by `src/tests/pg_test_reconcile_log_noise.rs`:

1. one `relation … already exists, skipping` NOTICE per partition child and sync pass —
   the sync now creates only missing children;
2. `__reflex_deferred_reconciled_batch already exists` per flushed source — the marker is
   created once per batch;
3. the false `source table … was altered` WARNING for the sync's own relocation trigger
   toggle (also closed `2026-07-28_sync_trigger_suppression_alter_blocks_reconcile_under_error_policy.md`,
   the same defect under `alter_source_policy = 'error'`).

Item 5 (per-node `INFO reconciled IMV …` lines and every failure/staleness WARNING) is
kept on purpose.

Severity: **low** (no wrong data).

## What remains: item 4

`stamp_targeted_recovery` (`src/reconcile.rs`) WARNs the PS-14 advisory ("reconcile
re-derives only from anchor sources; cannot refill ignore_sources-only partitions …") on
every operator-initiated reconcile of an IMV with `ignored_sources`, whether or not any
partition is actually anchor-empty. This is deliberate ("refuse loudly"). Gate it on the
condition holding (an archive-residue / anchor-empty partition exists) only if that probe
is cheap and exact; otherwise leave it.

## Not yet filed

Observed in the same session and out of scope here; each needs its own report once
reproduced:

- the Sep 1 deferred-flush skip that left `reliability_dp_year_agg` stale;
- `reflex_reconcile` not clearing `last_error`.
