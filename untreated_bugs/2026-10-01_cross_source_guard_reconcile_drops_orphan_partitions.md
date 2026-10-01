# The cross-source guard's COMMIT-time reconcile may drop orphan IMV partitions — S3 (latent, pre-existing)

When a DEFERRED IMV's batch stages deltas on two or more of its sources, the cross-source guard
rebuilds it at COMMIT through `rebuild_for_cross_source_guard` (src/trigger/deferred.rs:507) →
`reconcile_isolated` / `reconcile_for_cross_source_guard` (src/reconcile.rs:1527) →
`reflex_reconcile(view)`, which is `reflex_reconcile_with_orphans(view, true)`
(src/reconcile.rs:924). For a partitioned IMV the pre-rebuild partition sync may therefore drop
"orphan" IMV partitions — destructive DDL run implicitly by an application COMMIT, the case
`reflex_doctor` explicitly refuses without `drop_orphans` (see the doc on
`reflex_reconcile_with_orphans`, reconcile.rs:928).

Unchanged in 1.11.5 (the guard rebuild now runs through the COMMIT-time rebuild path, isolated
and stale-marking on failure, but with the same reconcile). Not reproduced as a data loss: it
needs an IMV partition that the sync classes as an orphan at that moment.

## Fix direction
Call `reflex_reconcile_with_orphans(view, false)` from the guard (both the isolated and the
`flush_failure_policy = error` paths), leaving orphan cleanup to explicit operator reconciles
and `reflex_doctor(fix => TRUE, drop_orphans => TRUE)`. Check that a guard rebuild then still
converges when an orphan is present (it must not fail every COMMIT).
