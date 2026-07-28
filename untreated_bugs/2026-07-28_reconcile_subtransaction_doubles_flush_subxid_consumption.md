# 2026-07-28 — the reconcile subtransaction doubles the flush's subtransaction-XID consumption, crossing PostgreSQL's 64-entry subxid cache at half the previous root count

**Status: untreated.** Found during adversarial re-review of the
`reflex_reconcile_partition` failure-atomicity fix. The fix itself is correct and should
not be reverted for this; the cost is a separate, measurable regression that belongs on
its own ticket.

## The mechanism

`reflex_reconcile_partition_impl` (`src/partition.rs:1611`) now opens an explicit
`SubTransaction` on **every** call — the `!skip_sync` gate was removed because a plpgsql
`EXCEPTION` block only rolls back on a RAISED error and so never covered the batch path's
RETURNED `ERROR: …`. That is the right correctness call.

But `reflex_flush_partitions_impl` already wraps each root's statements in a plpgsql
`DO … EXCEPTION` block (`src/partition.rs:3113-3128`), which is itself a subtransaction,
and dispatches exactly one `reflex_reconcile_partition(imv, '', nodes, true)` inside it
(`:2963`). So each root now costs **two** subtransaction XIDs instead of one.

PostgreSQL caches at most `PGPROC_MAX_CACHED_SUBXIDS = 64` subtransaction XIDs per backend
in shared memory. Past that the backend's `subxidStatus.overflowed` is set, and every
*other* backend's visibility check against tuples written by this transaction falls back
from the in-memory cache to `pg_subtrans` lookups for the remainder of the transaction.
That is a cliff, not a slope.

## Measurement

pg16.11, `--enable-cassert`, 40 partitioned sources each with one IMV, one new partition
ATTACHed to each (auto-flush trigger disabled so the pending rows accumulate), then a
single `SELECT reflex_flush_partitions()` in one transaction. XIDs consumed are measured
as the gap between that transaction's `pg_current_xact_id()` and the next transaction's.

| build | subtransactions assigned XIDs | over 64? |
|---|---|---|
| pre-branch (`f74fc56`) | **40** (1 per root) | no |
| with the fix (`39905d6`) | **80** (2 per root) | **yes** |

The relationship is linear, so the overflow threshold moves from **65 roots to 33 roots**.

## Why it matters here

A production cluster in this project carries ~190 registered IMVs. Both flush entry points
run in a single transaction:

* `reflex_flush_partitions()` drains **every** pending root in one `Spi::connect_mut`
  (`src/partition.rs:2865-3150`), so one call over N pending roots costs 2N subxids.
* the deferred constraint trigger `__reflex_partition_flush_trigger` (`src/lib.rs:1409-1417`)
  fires `reflex_flush_partition_source(NEW.source_root)` per pending row at COMMIT, inside
  the committing transaction — so a migration attaching partitions across many roots pays
  the same 2N in one transaction.

Anything past 33 roots in one transaction now overflows where it previously did not.

## What was ruled out

* Not a correctness bug. Every subtransaction is properly released or rolled back; the full
  suite is green (1549 passed) and the atomicity properties are mutation-pinned.
* Not fixed by restoring the `skip_sync` gate — that reintroduces the committed-partial-swap
  bug the gate removal closed (pinned by
  `pg_part_failed_skip_sync_reconcile_rolls_back_children_already_swapped`; restoring the
  gate turns it RED).
* Not visible to the current suite: `cargo pgrx test` runs each test in one transaction and
  never observes cross-backend visibility cost.

## Fix direction

The two subtransactions per root are redundant *in the flush path specifically*: the DO
block's `EXCEPTION` subtransaction and the reconcile's `SubTransaction` cover overlapping
scopes. Options, roughly in increasing invasiveness:

1. Have the flush's DO block capture the reconcile's returned text and `RAISE` on a leading
   `ERROR:` — which the report
   `2026-07-27_flush_do_block_commits_destructive_ddl_on_failed_reconcile.md` already
   requires for correctness. Once the block's own `EXCEPTION` subtransaction genuinely
   covers the reconcile, the per-call `SubTransaction` becomes redundant *on that path* and
   could be skipped by an explicit, honestly-named flag (not the old `skip_sync`, which
   conflated prep with isolation).
2. Batch several roots per DO block so the block's subtransaction is amortised.
3. Accept the cost and document the threshold.

Option 1 closes two reports at once and is the only one that removes rather than hides the
cost. Pin whichever is chosen with a test that counts XIDs consumed by an N-root flush
(the `pg_current_xact_id()` gap measurement above works from SQL and needs no new
infrastructure).

Severity: medium — no wrong results, but a cross-backend performance cliff on the automatic
path at a root count this deployment plausibly reaches, and one no existing test can see.
