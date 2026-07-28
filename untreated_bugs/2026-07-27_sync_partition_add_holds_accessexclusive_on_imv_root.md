# 2026-07-27 — NARROWED: adding a source partition no longer freezes the IMV; a pre-existing large-delta escalation still can

**Status: narrowed. The partition-add path is FIXED; one narrow path remains
inside it, and one adjacent pre-existing defect defeats the fix at volume
(filed separately).**

The original report — attaching a source partition freezes ALL reads of the IMV,
including readers pruning to an unrelated partition, for the whole transaction —
is fixed. Three mechanisms were involved and all three are closed:

1. `reflex_sync_partitions` no longer builds a new top-level mirror node with
   `CREATE TABLE … PARTITION OF <live IMV root>` (`AccessExclusive` on the root,
   held to commit). A brand-new node is built standalone, its full sub-partition
   subtree is created into it while detached, and it is added with one
   `ALTER TABLE <root> ATTACH PARTITION` (`ShareUpdateExclusive`).
2. The COMMIT-time reconcile no longer DETACHes a provably EMPTY mirror child
   off its parent — which at mirror depth 1 *is* the root.
3. **(added after adversarial review)** Nor does it DETACH a child that is
   provably FRESH — created by this transaction's sync. Emptiness alone was the
   wrong predicate: a load in the same transaction routes its own IMV
   maintenance delta into the brand-new child *before* the COMMIT-time reconcile
   reaches it, so the child is no longer empty and the swap ran anyway. That is
   the canonical partition-rollover shape (create/attach next period's partition
   and load it in one transaction) and it needs **no DEFAULT partition** — the
   earlier version of this report claimed otherwise and was wrong. A fresh child
   is TRUNCATEd and filled in place; TRUNCATE locks the child only, never the
   parent, and fires no statement-level TRUNCATE trigger of the root.

Pinned by four tests in `src/tests/pg_test_partition_attach_locks.rs` —
`attach_new_partition_never_locks_imv_root_depth1` / `_depth2`,
`create_and_load_partition_never_locks_imv_root_depth1`,
`attach_then_load_partition_never_locks_imv_root_depth2` — each asserting both
that the root never shows `AccessExclusiveLock` in `pg_locks` and that a
concurrent session reading an unrelated partition at `lock_timeout='2s'` never
blocks, plus the bidirectional `EXCEPT ALL` oracle. Each goes RED when the
corresponding half of the fix is reverted.

Measured (PG 17.7, pgrx instance, `.so` from `2f8b786` vs the fix branch,
mirror depth 1, base 300 000 rows, incoming partition created and loaded with
50 000 rows in one transaction): root `AccessExclusive` window **0.082 s → none
observed**; max latency of a reader of an unrelated partition **184 ms → 13 ms**.

## Residual 1 — a pre-existing, non-empty, non-fresh child still swaps

A child that is neither empty nor fresh — i.e. a genuinely pre-existing mirror
child being reconciled — still takes the DETACH/ATTACH swap, and at mirror
depth 1 that DETACHes off the root. This is by design: the swap is always
correct, and the in-place path is licensed only by a proof that nothing worth
preserving is in the child. It is the "reconcile of an EXISTING partition" case
the original report placed out of scope.

The DEFAULT-partition interaction called out in the previous version of this
report is now covered rather than residual: rows that `refill_tree_defaults`
routes into the new child land in a child that IS fresh, so it is TRUNCATEd and
refilled from the authoritative query.
`attach_new_partition_absorbing_default_rows_stays_correct` (T5b) pins the
correctness of that path but deliberately does **not** assert its lock shape.

## Residual 2 — large deltas defeat the fix at volume (filed separately)

Above a delta-size threshold *relative to the IMV's size*, the maintenance path
escalates to a full partitioned reconcile that swaps **every** child, including
unchanged ones, taking `AccessExclusive` on the root at depth 1. This reproduces
with **no partition DDL at all** and is identical on `2f8b786`, so it is
pre-existing and outside this report's scope — but it means a
create-partition-and-bulk-load transaction can still freeze the IMV when the load
is large relative to the IMV. Measured on the fixed branch at depth 1: base
50 000 + load 50 000 escalates (root `AccessExclusive` 0.162 s, reader latency
116 ms), while base 300 000 + load 50 000 does not (no `AccessExclusive`, 13 ms).
Tracked in `2026-07-28_large_delta_full_reconcile_swaps_every_partition.md`.

## Out of scope (unchanged)

* Making the reconcile of an EXISTING, non-empty, non-fresh partition
  reader-free for readers of that same partition. That needs
  `DETACH PARTITION CONCURRENTLY`, which cannot run inside a transaction block.
* The reconcile's absolute cost — tracked in
  `2026-07-24_current_assortment_reconcile_cost.md`.

## Residual cost of the fix's own lock

`ATTACH PARTITION`'s `ShareUpdateExclusive` on the root is held to COMMIT.
Probed at `lock_timeout='1s'` from a second session with the transaction open:
`SELECT` on the root — OK; `INSERT` on the root — OK; `VACUUM` of another child —
OK; `ANALYZE <root>` — blocked; a second `ATTACH PARTITION` — blocked. Readers
and writers are unaffected; blocked ANALYZE/autovacuum on the root and serialized
partition maintenance are the accepted price.
