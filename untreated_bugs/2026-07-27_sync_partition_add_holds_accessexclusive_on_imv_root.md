# 2026-07-27 — adding a source partition freezes all reads of the IMV: `reflex_sync_partitions` takes `AccessExclusive` on the live IMV root and the COMMIT-time reconcile holds it

**Status: untreated, mechanism confirmed by reproduction (PG 17.7, pg_reflex 1.11.2 @ `f74fc56`).**
Field-reported: creating a new plan or archiving the current plan (both attach a source
partition) freezes the serving layer on a passthrough IMV for ~18–25 s. This report supersedes
the incoming field report, whose root-cause attribution and supporting evidence are both wrong
in ways that change the fix — see *What the field report got wrong*.

## The mechanism

Attaching a source partition runs, **inside the caller's single transaction**:

1. `ddl_command_end` → `__reflex_on_ddl_command_end` (`src/lib.rs:1055`) does two things:
   * `INSERT INTO __reflex_partition_pending` — enqueues the source root for a COMMIT-time flush;
   * `PERFORM reflex_sync_partitions(imv, FALSE)` — synchronous, inline.
2. `reflex_sync_partitions_impl` (`src/partition.rs:1007`) creates the missing mirror children via
   `build_partition_node_ddl_pair` (`src/partition.rs:445`), which emits
   `CREATE TABLE IF NOT EXISTS <child> PARTITION OF <parent>` (L493–L500). For the top-level node
   the parent is the **live IMV root** → `AccessExclusiveLock` on the root, and PostgreSQL holds
   every DDL lock **to commit**.
3. At `COMMIT`, the `DEFERRABLE INITIALLY DEFERRED` trigger on `__reflex_partition_pending`
   (`src/lib.rs:1256`ff) fires `reflex_flush_partition_source` → `reflex_reconcile_partition`,
   which does all the heavy fill work — **still inside that same transaction, with step 2's
   `AccessExclusive` still held.**

So the freeze window is not sync's execution time. Sync grabs the root's `AccessExclusive`
cheaply and early; what holds it for 18–25 s is the reconcile that runs afterwards at COMMIT.
Every `SELECT` on the IMV — including ones pruning to a completely unrelated partition — blocks
for that whole window.

## Measured evidence

Fixture: source `plan_data` `LIST(plan_id)` → `RANGE(order_date)`, 12 monthly leaves per plan;
passthrough IMV `pd_imv` mirrored at depth 2; incoming plan partition pre-built detached with
2 M rows, then `ALTER TABLE plan_data ATTACH PARTITION …`.

**Cost split — sync is ~4 % of the transaction:**

| phase | measured |
|---|---|
| `ATTACH` statement (= the entire `ddl_command_end` → `reflex_sync_partitions`) | **0.354 s** |
| `COMMIT` (deferred flush → `reflex_reconcile_partition`) | **~7.8 s** |
| standalone `reflex_reconcile_partition('pd_imv','','plan_data_5')`, same partition | **23.7 s** |

**Lock trace on the IMV root during the attach** (100 ms sampling of `pg_locks`):

```
177.878  pd_imv: AccessExclusiveLock, ShareRowExclusiveLock   <- 0.48 s into the tx
185.372  pd_imv: released at COMMIT                            <- 7.49 s continuous
```

A concurrent reader of a **different** partition (`SELECT count(*) FROM pd_imv WHERE plan_id = 1`,
`lock_timeout='2s'`) took **3 consecutive lock timeouts** across that window; baseline latency
outside it was 0.13 s.

**Bare-PostgreSQL 17.7 lock matrix** (measured in isolation, no extension involved) — this is what
drives every conclusion below:

| statement | lock taken on the parent |
|---|---|
| `CREATE TABLE child PARTITION OF parent` (new child) | **AccessExclusive** |
| `CREATE TABLE IF NOT EXISTS …` (child already exists) | **none at all** |
| `ALTER TABLE parent ATTACH PARTITION` | ShareUpdateExclusive |
| `ALTER TABLE parent DETACH PARTITION` (non-concurrent) | **AccessExclusive** |
| `ALTER TABLE parent DISABLE TRIGGER USER` | ShareRowExclusive (does not block readers) |

## What the field report got wrong

1. **"The sync is slow, ~18–25 s."** No — sync is 0.354 s here (4 % of the transaction).
   It issues DDL and catalog reads only; it never copies IMV data. The only data-moving step is
   `drain_tree_defaults`/`refill_tree_defaults` (`src/partition.rs:3085`, `:3167`), a no-op unless a
   DEFAULT partition actually holds rows. The 18–25 s belongs to the COMMIT-time reconcile.

2. **"Reconcile shows 0 s `AccessExclusive` on the parent, so the swap path already avoids this."**
   The 0 s reading is an artifact: reconcile's internal `reflex_sync_partitions(view, true)`
   (`src/partition.rs:1505`) hits `CREATE TABLE IF NOT EXISTS` on children that already exist, and
   that statement takes **no parent lock at all** (row 2 of the matrix). It proves nothing about
   the swap.

3. **"The swap path never blocks readers."** False in general. Plain `DETACH PARTITION` takes
   `AccessExclusive` on the parent, held to commit. Measured on a 15 s reconcile of an existing
   partition: `pd_imv_plan_data_5` went `AccessExclusive` at t≈1.6 s (the first leaf's DETACH) and
   stayed locked for the remaining 13.7 s while the other 11 leaves filled. A reader pruning to
   `plan_id = 5` took **4 consecutive timeouts**; a reader on `plan_id = 1` never blocked. The
   swap is reader-safe only for readers that prune *away* from the affected parent.

## Severity

**High availability impact, no correctness risk.** No wrong data is produced or lost; the IMV is
correct at commit. The failure is that every reader of the IMV — including readers of partitions
entirely unrelated to the change — is blocked for the full duration of the reconcile, on an
operation the application performs routinely (new plan, plan archive). Frequency is whatever the
application's partition-creation rate is.

## Fix direction

The report's proposal — build the new top-level child detached and `ATTACH` it instead of
`CREATE … PARTITION OF` in place — is the right direction, but its scope has to be set by the
lock matrix, not by the report's reasoning:

* **At mirror depth ≥ 2 the proposal is sufficient.** The subsequent reconcile's DETACH/ATTACH
  land on the *immediate parent of the swapped leaf* — i.e. the new top-level child — not on the
  root (measured: `pd_imv_plan_data_5: AccessExclusiveLock`, root only `ShareRowExclusive`).
  Readers of other partitions stop blocking; readers of the new partition were not reading it
  before, so there is no regression.
* **At mirror depth 1 the proposal is NOT sufficient.** The IMV top-level child *is* the leaf, so
  the reconcile's swap DETACHes it directly from the root → `AccessExclusive` on the root anyway.
  The freeze window shrinks from "whole transaction" to "swap-DETACH → commit" but does not close.

The version that closes both: **a brand-new node has nothing to preserve, so it should not go
through the swap at all.** Build the child (with its full sub-partition subtree) detached, fill
it, add the bound-matching CHECK so `ATTACH` skips its validation scan, then a single
`ALTER TABLE <parent> ATTACH PARTITION` — reusing the existing machinery in
`build_swap_partition_ddl` / `execute_partition_swap_for_child` (`src/partition.rs:1929`) minus
the DETACH/DROP/RENAME half. That keeps the root at `ShareUpdateExclusive` at any depth.

Note this moves the fill from COMMIT-time reconcile into the `ddl_command_end` sync for
new nodes, i.e. sync stops being cheap. That is the point — the work has to happen somewhere in
the transaction, and doing it before the partition is visible is what makes it lock-free. The
flush's existing empty-leaf skip (`fill_node` in `reflex_flush_partitions_impl`,
`src/partition.rs:2787`) must keep the new node out of the COMMIT-time reconcile once sync has
already filled it, or the work is done twice.

## Invariants to preserve

* The two-key `pg_advisory_xact_lock(hashtext(name), hashtext(reverse(name)))` form — a one-key
  lock occupies a different advisory space and would not mutually exclude
  (`src/partition.rs:1107`ff).
* Multi-level trees: the detached child must be built with its **full** sub-partition subtree
  before the single top-level `ATTACH`, so only one brief parent lock is taken.
* The shape-drift heal (`partition_shape_mismatch`, `src/partition.rs:518`), the confirmed
  bound-collision orphan drop (`drop_bound_collision_orphan`, `src/partition.rs:1420`), and the
  empty-source-enumeration refusal to drop orphans (`src/partition.rs:1137`ff).
* `ATTACH PARTITION` takes `AccessExclusive` on the parent's DEFAULT partition, if one exists, and
  scans it for rows belonging to the incoming bound. Sync currently sidesteps this by draining
  defaults; the new path must confirm the interaction on a **large** default, which the
  reproduction above (no default partition) does not cover.

## Acceptance test

Attach a new source partition while a second session polls
`SELECT count(*) FROM <imv> WHERE <partkey> = <other>` with `lock_timeout='2s'`. Today it times
out for the whole reconcile; after the fix it must never block, and the IMV **root** must only
ever show `ShareUpdateExclusive` (never `AccessExclusive`) in `pg_locks` during the add — asserted
at both mirror depth 1 and depth 2, since only depth 2 is covered by the report's own proposal.
Per the methodology, the assertion must be shown to go RED when the fix is reverted.

## Out of scope

* Making the *reconcile of an existing partition* reader-free for readers of that same partition
  (the `DETACH PARTITION` `AccessExclusive` above). That is a separate, harder problem — it needs
  either `DETACH PARTITION CONCURRENTLY` (which cannot run inside a transaction block, so it is
  unavailable on this path) or a redesign that never detaches. File separately if it matters.
* The reconcile's absolute cost (23.7 s for 2 M rows / 12 leaves). Tracked in
  `2026-07-24_current_assortment_reconcile_cost.md` and
  `2026-07-27_partitioned_passthrough_membership_ungated.md`.
