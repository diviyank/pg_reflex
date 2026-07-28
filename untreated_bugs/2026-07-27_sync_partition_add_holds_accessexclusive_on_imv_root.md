# 2026-07-27 — NARROWED: adding a source partition can still take `AccessExclusive` on the IMV root when the IMV's DEFAULT partition holds rows for the incoming bound

**Status: narrowed. The general case is FIXED; one narrow path remains.**

The original report — attaching a source partition freezes ALL reads of the IMV,
including readers pruning to an unrelated partition, for the whole transaction —
is fixed. Both halves of the mechanism are closed:

* `reflex_sync_partitions` no longer builds a new top-level mirror node with
  `CREATE TABLE … PARTITION OF <live IMV root>` (`AccessExclusive` on the root,
  held to commit). A brand-new node is built standalone, its full sub-partition
  subtree is created into it while it is still detached, and it is added with one
  `ALTER TABLE <root> ATTACH PARTITION` (`ShareUpdateExclusive`, no reader
  conflict).
* The COMMIT-time reconcile no longer DETACHes a provably EMPTY mirror child off
  its parent — which at mirror depth 1 *is* the root. An empty child has nothing
  to preserve, so it is filled in place.

Pinned by `attach_new_partition_never_locks_imv_root_depth1` /
`…_depth2` in `src/tests/pg_test_partition_attach_locks.rs`, which assert both
that the root never shows `AccessExclusiveLock` in `pg_locks` and that a
concurrent session reading an unrelated partition at `lock_timeout='2s'` never
blocks. Both go RED when either half of the fix is reverted.

Measured (PG 17.7, pgrx instance; `.so` from `2f8b786` vs `8963faa`, 300 k-row
incoming branch): root `AccessExclusive` window 0.794 s → none at depth 2 and
0.479 s → none at depth 1; max latency of a reader of an unrelated partition
865 ms → 32 ms and 547 ms → 19 ms.

## The residual

Sync sequences the new node's ATTACH **before** `refill_tree_defaults`, so that
rows drained out of the IMV's DEFAULT partition still route into their new leaf —
preserving the pre-fix semantics. When the IMV's default happens to hold rows
belonging to the incoming bound, the refill therefore lands them in the
brand-new child, and that child is no longer empty by the time the COMMIT-time
reconcile reaches it. `execute_partition_swap_for_child`'s emptiness probe then
(correctly) refuses the in-place fill and takes the full DETACH/ATTACH swap — so
at mirror depth 1 the root goes `AccessExclusive` again for the remainder of the
transaction.

This fails toward doing the full, correct work, which is the required direction:
the swap is always correct, the in-place fill is only valid for a provably empty
child. So this is an availability residual, not a correctness one.

**Reachability.** Requires (a) a DEFAULT partition on the IMV, (b) rows in it
whose partition key falls in the bound of the source partition being attached —
i.e. the source previously routed those rows to *its* default and they are now
being given a real partition. Not the common new-plan/archive-plan shape from the
field report, which has no default partition at all.

**Coverage.** `attach_new_partition_absorbing_default_rows_stays_correct` (T5b)
constructs exactly this shape and pins that the result is correct and that the
absorbed rows appear exactly once. It deliberately does **not** assert the lock
shape for this path — that assertion would be RED today.

## Fix direction

Two candidates, neither obviously worth its complexity yet:

1. Make the in-place path handle a non-empty child by `DELETE`ing it first. This
   is what the swap exists to avoid (bloat, and a full delete of a possibly huge
   partition), so it would need a size guard, and the guard reintroduces a
   fast-but-sometimes-slow branch.
2. Have sync itself drop the drained default rows for the new bound from the
   holding table, once the node is known to be filled authoritatively from the
   IMV query. This keeps the child empty for the reconcile, but makes sync
   responsible for a correctness-critical delete and couples it to the
   reconcile's fill contract.

Given the reachability above, "leave it" is a defensible third option.

## Out of scope (unchanged)

* Making the reconcile of an EXISTING, non-empty partition reader-free for
  readers of that same partition. That needs `DETACH PARTITION CONCURRENTLY`,
  which cannot run inside a transaction block.
* The reconcile's absolute cost — tracked in
  `2026-07-24_current_assortment_reconcile_cost.md`.

## Residual cost of the fix's own lock

`ATTACH PARTITION`'s `ShareUpdateExclusive` on the root is held to COMMIT.
Probed at `lock_timeout='1s'` from a second session with the transaction open:
`SELECT` on the root — OK; `INSERT` on the root — OK; `VACUUM` of another child —
OK; `ANALYZE <root>` — blocked; a second `ATTACH PARTITION` — blocked. Readers
and writers are unaffected; blocked ANALYZE/autovacuum on the root and serialized
partition maintenance are the accepted price.
