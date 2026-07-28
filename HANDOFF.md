# HANDOFF — `2026-07-27_sync_partition_add_holds_accessexclusive_on_imv_root`

Branch: `worktree-agent-a11e5b03a319a72c9`, based on `main` @ `2f8b786`.
Commits: `4a75c92` (the fix), `0314d9e` (follow-ups — **its message was rewritten
by a commit hook and reads as finished; it is NOT. The test half of that commit
has never been executed.**), `6d5f919` (this file). The bug report is still
in `untreated_bugs/` — deliberately, the work is not finished.

**Environment**: `export CARGO_TARGET_DIR=/private/tmp/tb2` (already warm, 3.5 GB
of build cache). Any deeper path pushes `$CARGO_TARGET_DIR/test-pgdata/.s.PGSQL.<port>`
past PostgreSQL's 103-byte socket limit and `pg_ctl start` fails. **pg17 only.**

**Disk was the binding constraint**: the machine repeatedly hit 100 % full
(`/System/Volumes/Data`), which manifests as `cargo pgrx test` runs that hang for
minutes with no output rather than as an error. Check `df -h /private/tmp` BEFORE
believing any hang or mass failure. `~/.pgrx/data-17` is 8 GB of the user's
benchmark databases — do not delete it.

---

## 1. `src/lib.rs` — NOT a registry column

The one-line `src/lib.rs` change is **only** `include!("tests/pg_test_partition_attach_locks.rs")`
in the `mod tests` block. It adds no registry column, touches no bootstrap DDL,
and needs no `sql/*--*.sql` migration. Nothing here for integration.

---

## 2. Step 0 hypotheses

### H1 — the lock matrix. **RESOLVED, fully confirmed.**
Measured in isolation on the local Homebrew PostgreSQL 17.7, no extension loaded:

| statement | lock on parent | measured |
|---|---|---|
| `CREATE TABLE child PARTITION OF parent` (new child) | `AccessExclusiveLock` | confirmed |
| `CREATE TABLE IF NOT EXISTS …` (child already exists) | none at all | confirmed (0 rows in `pg_locks`) |
| `ALTER TABLE parent ATTACH PARTITION` | `ShareUpdateExclusiveLock` | confirmed |
| `ALTER TABLE parent DETACH PARTITION` (non-concurrent) | `AccessExclusiveLock` | confirmed |
| `ALTER TABLE parent DISABLE TRIGGER USER` | `ShareRowExclusiveLock` | confirmed |

Two additions I measured that the report did not have, and that shaped the fix:
* `CREATE TABLE grandchild PARTITION OF child` (depth 2) takes `AccessExclusive`
  on **`child` only — the root gets no lock at all**. So sub-partition creation
  never touches the root; the root freeze comes exclusively from the *top-level*
  node's create.
* `INSERT INTO <partition child>` does **not** fire a `FOR EACH STATEMENT`
  trigger declared on the partitioned root (verified with a logging trigger:
  1 row logged for `INSERT INTO root`, 0 for `INSERT INTO child`, 0 for
  `INSERT INTO <newly attached child>`). Every pg_reflex IMV-side maintenance
  trigger is `FOR EACH STATEMENT` (`schema_builder.rs:665-838`), so filling a
  child in place is trigger-equivalent to the swap's detached fill. This is what
  makes the in-place fill in §3 safe.

### H3 — bound-matching CHECK skips ATTACH validation for a SUB-PARTITIONED child. **RESOLVED, SURVIVES.**
Fixture: parent `LIST(k)`; incoming child `PARTITION BY RANGE(d)` with 12 monthly
leaves and 2 000 000 rows.

| case | ATTACH time |
|---|---|
| no CHECK (full recursive validation scan) | **456.6 ms** |
| `ALTER TABLE child ADD CONSTRAINT bnd CHECK (k IS NOT NULL AND k = 5)` first | **0.26 ms** |

The CHECK added on the partitioned child propagates to every leaf
(verified in `pg_constraint`: identical `CHECK` row on `q_5_check` and on
`q_5_check_m1..m12`), and `PartConstraintImpliedByRelConstraint` then skips the
scan for the whole subtree. **The approach is not killed by H3.**

**But the CHECK is not needed, and I deliberately did not add one. The reason is
simply that the shipped design attaches the child while it is still EMPTY.**
Measured on bare PG 17.7, LIST-partitioned parent:

| ATTACH | time |
|---|---|
| 2 000 000-row child, WITH the bound-matching CHECK | 4.9 ms |
| 2 000 000-row child, WITHOUT the CHECK | 162 ms |
| **empty child, no CHECK** | **1.7 ms** |

The validation scan has zero rows to scan, so it costs nothing. The 456.6 ms →
0.26 ms speedup in the table above belongs to the **rejected** fill-then-attach
design, where the child is full at attach time. Under the accepted
attach-empty-then-fill shape the CHECK is not a latency/safety trade being
declined — it is a cost that is never paid. It also means no bound predicate has
to be derived for a not-yet-attached child at all, which removes the whole class
of "derived predicate is subtly wrong → silently wrong rows attached" risk.

**Do not re-open this.** Two related facts, measured, that bound how the fix
should be argued:

1. **The ATTACH lock is held to COMMIT.** The parent shows
   `ShareUpdateExclusiveLock, granted` continuously from the ATTACH statement
   until commit (verified over 10 s and 30 s holds); the incoming child shows
   `AccessExclusiveLock`. Shortening the ATTACH statement therefore does **not**
   shorten the lock window — the window is `[ATTACH executes → COMMIT]`
   regardless. No argument of the form "a faster ATTACH reduces the freeze" is
   valid, and none appears in this document or in the benchmark section.
2. **What `ShareUpdateExclusive` on the root actually blocks**, probed from a
   second session at `lock_timeout='1s'` with the ATTACH transaction open:
   `SELECT` on the parent — OK; `INSERT` on the parent — OK; `VACUUM` on a
   different child — OK; `ANALYZE <parent>` — **BLOCKED**; a second
   `ATTACH PARTITION` — **BLOCKED**. So the residual cost of the fix's own lock
   is a blocked ANALYZE/autovacuum on the root and serialized partition
   maintenance. Readers and writers are unaffected, which is what makes the
   end state acceptable.

### H4 — DEFAULT partitions. **RESOLVED. Real, bounded, and not a regression.**
`ATTACH PARTITION` takes `AccessExclusiveLock` on the parent's DEFAULT partition
and scans it. Measured on a 3 000 000-row default with none of the rows belonging
to the incoming bound: ATTACH = **130.6 ms**, locks = `r: ShareUpdateExclusive`,
`r_def: AccessExclusive`. On a drained (empty but bloated) default: **0.57 ms**.

Reader impact, measured with a real second session while the ATTACH transaction
was held open:

| reader | result |
|---|---|
| baseline, nothing in flight | 7.7 ms |
| `SELECT count(*) FROM s WHERE k = 1` (prunes away from the default) | 11.6 / 8.6 / 20.2 ms — **never blocked** |
| `SELECT count(*) FROM s` (unpruned, must touch the default) | **lock timeout at 2 s** |

So the fix does **not** merely trade a root freeze for a default freeze:
* readers that prune away from the default are unaffected;
* a reader that must touch the default blocks — but that reader blocks *today*
  too, on the root's `AccessExclusive`, so there is no regression;
* pg_reflex's existing `drain_tree_defaults` empties every default in the tree
  before the node DDL, so in the real path the scan is over an empty relation.
  The ATTACH is sequenced **before** `refill_tree_defaults` for exactly that
  reason, and so that drained rows belonging to the new bound still route into
  their new leaf (preserving today's semantics — see the T5b test).

### H2 — "the fill can be moved (out of the reconcile, into sync)". **PARTLY RESOLVED — and its premise is FALSE.**
The spec justified moving the fill with: *"doing it BEFORE the partition is
visible is what makes it lock-free."* That is wrong. All of this happens inside
**one transaction**, so visibility to other sessions is not in play at any point;
the only thing that matters is which lock modes are taken. Filling an attached
partition takes `RowExclusive`, which blocks no reader.

I did **not** move the fill. See §3 for what I did instead and §7 for the risk
this deviation carries.

---

## 3. What is implemented, and where

### `4a75c92` — the fix. Two changes in `src/partition.rs`.

**(a) sync builds a new top-level mirror node detached, then attaches it once.**
* New pure helpers next to `build_partition_node_ddl_pair`: `is_top_level_node`,
  `struct DetachedNodeDdl`, `build_detached_node_ddl_pair` (emits
  `CREATE [UNLOGGED] TABLE <child> (LIKE <parent> INCLUDING ALL) [PARTITION BY …]`
  plus the matching `ALTER TABLE <parent> ATTACH PARTITION <child> <bound>`).
* In `reflex_sync_partitions_impl`'s node loop, a top-level node whose mirror
  child does not exist is created standalone and its ATTACH pushed to a
  `pending_attach` list. Its sub-partition subtree is then built into it by the
  ordinary later iterations while it is still detached (H1: those lock the
  detached node, nothing live). The pending attaches run after the loop and
  before `refill_tree_defaults`.
* `LIKE <root> INCLUDING ALL` + ATTACH was verified to reproduce the parent's
  indexes and match them up on attach without duplicates (checked `pg_index`).

**(b) the reconcile does not swap a provably EMPTY child.**
`execute_partition_swap_for_child` probes both mirror children with
`relation_has_rows`; when both are provably empty it fills them in place via the
new pure `build_inplace_partition_fill` (same queries and same partition-constraint
filters as `build_swap_partition_ddl`'s fills, different destination) and returns,
skipping DETACH/ATTACH/DROP/RENAME entirely. `relation_has_rows` already fails
toward "non-empty", so any probe failure still takes the full swap.

This half is what closes **mirror depth 1**, where the top-level child *is* the
leaf and the swap would otherwise DETACH it straight off the root. Mutation M2
below proves it is load-bearing.

### `0314d9e` — follow-ups (test half UNVERIFIED)
* **sync (believed correct, NOT re-run):** the detached `CREATE` is not
  `IF NOT EXISTS`, so it raised 42P07 where the old statement silently no-opped
  on a detached leftover of the same name. It now keys off `existing_children`,
  a set of child names that exist as a relation of *any* relkind, collected in
  the shape-drift heal loop (minus what that loop drops); anything else falls
  back to the original in-place create. **This is the fix for the one genuine
  regression the full suite found** — see §5.
* **tests (UNVERIFIED, never executed):** T1/T2 reworked onto a remote dblink
  worker session. Two bugs addressed, both diagnosed from real failures:
  committed fixtures leaked (they cannot be dropped from a transaction that
  still holds their locks), and reading the IMV from the test session itself
  held `AccessShare` so the worker's cleanup `DROP` deadlocked against it — that
  deadlock is what made the last run hang.

---

## 4. Tests — status

All in `src/tests/pg_test_partition_attach_locks.rs`, included from `src/lib.rs`.

| id | test fn | status |
|---|---|---|
| T1 | `attach_new_partition_never_locks_imv_root_depth2` | written; **passed** in the pre-`0314d9e` shape; **NOT re-run** after the worker rework |
| T2 | `attach_new_partition_never_locks_imv_root_depth1` | same |
| T3 | `attach_new_partition_data_is_correct_depth2` | written, **passing** |
| T4 | `attach_new_partition_is_filled_exactly_once` | written, **passing** |
| T5 | `attach_new_partition_with_non_empty_default_stays_correct` | written, **passing** |
| T5b | `attach_new_partition_absorbing_default_rows_stays_correct` | written, **passing** (default rows that belong to the incoming bound) |
| T6 | regression | **not written as new tests — deliberately.** The three behaviours already have real coverage: `ps16_ddl_sync_heals_target_bound_collision`, `ps16_ddl_sync_heals_intermediate_bound_collision`, `ps16_bound_collision_heal_is_scoped_to_exact_parent_not_whole_subtree`, `pg_part_sync_refuses_mass_drop_on_empty_source_tree`, and `unit_partition.rs`'s `partition_shape_mismatch` cases. They must be confirmed green on the merged tree; duplicating them was judged not worth it. |

T1/T2 cannot be written as an ordinary `#[pg_test]`: the body is one rolled-back
transaction, so the IMV root would be created by the very transaction under test
and would already hold `AccessExclusive` on it from the `CREATE`, masking the
lock under test. Hence the `dblink` worker. `dblink` **is** available in the
pgrx pg17 install and connects fine over the unix socket.

---

## 5. Full pg17 suite

One full run completed, on `4a75c92` (i.e. **before** the `0314d9e` fixes):
**1544 passed, 3 failed of 1547.**

* `cov_reflex_compact_all_imv_empty_registry` and one sibling empty-registry test
  — **test-harness pollution, not a product bug.** The old T1/T2 committed their
  dblink fixtures and could not drop them, leaving registry rows behind.
  `0314d9e` removes the cause.
* `xsu_guard_reconcile_failure_flags_known_stale` — **a genuine regression from
  `4a75c92`**, root-caused: the test DETACHes a mirror child to induce a failure,
  sync then saw the name as "not a partition" and tried the detached `CREATE`,
  which raised 42P07 instead of no-opping. `0314d9e` fixes it.

**Neither fix has been verified.** The suite has not been re-run since.

`cargo fmt`: clean. `cargo clippy --features pg17 --no-default-features --all-targets`:
clean for all changed files (the only 4 warnings are pre-existing `needless_borrow`
in `src/tests/pg_test_audit.rs`, untouched). No `#[pg_extern]` added, so
`cargo pgrx schema` was not required.

---

## 6. Self-mutation — six run, all confirmed

Post-review additions (run on the final tree):

| mutation | result |
|---|---|
| **M5** `is_fresh_partition` forced `false` (revert the F1 half) | T7 `create_and_load_…_depth1` **RED** alone; the other three lock tests stay green |
| **M6** TRUNCATE of a fresh non-empty child removed | T8 `attach_then_load_…_depth2` **RED** on row counts — the TRUNCATE is load-bearing |

M6 leaves T7 green: at depth 1 the duplicate INSERT collides with the IMV's
unique key and the reconcile's error is discarded by its caller, so the child
keeps its delta rows and the count still reads 900. That is the same
error-swallowing the reviewer noted about M4; it is pre-existing and out of scope
here, but it is why T8 (a distinct key shape) is the test that catches M6.

### Original four


Run against `4a75c92`, each mutation applied alone and reverted after.

| # | mutation | expected | observed |
|---|---|---|---|
| M1 | force the in-place `CREATE … PARTITION OF` back on (`build_*_detached = false`) | T1, T2 RED | **T1 RED, T2 RED**; T3/T4/T5 stayed green (they assert data, not locks) |
| M2 | keep the detached build, disable **only** the in-place fill (`if false && int_is_empty && …`) | depth 1 RED, depth 2 green | **T2 (depth 1) RED, T1 (depth 2) green** — empirically confirms the field report's own claim that a detached-skeleton-only fix does not close depth 1, and that the second half of the fix is load-bearing |
| M3 | break the fill (in-place fills replaced by `SELECT 1 WHERE false`) | T3, T4 RED | **T3, T4 and T5 all RED**, caught by the `EXCEPT ALL` oracle ("2 mismatches") and by the exact-count assertion, not by string matching |
| M4 | delete the emptiness guard so the in-place path also runs on a non-empty child | existing reconcile tests RED | **9 existing subpartition tests RED** (`pg_subpart_reconcile_leaf_swaps_only_that_leaf`, `…_skip_sync_still_reconciles`, `…_reconcile_internal_node_swaps_all_leaves`, the shallow-flush ones, and the subpartition fuzz sequence) — the guard is load-bearing and the pre-existing suite already pins it |

The true RED baseline was also captured before any implementation: on unmodified
`2f8b786`, T1 and T2 failed with `canceling statement due to lock timeout` in the
concurrent reader, immediately after the log line
`CREATE TABLE IF NOT EXISTS "public"."la2_imv_la2_src_1" PARTITION OF "public"."la2_imv"`.
RED for exactly the intended reason.

---

## 7. Deviation from the approved design — READ THIS

The spec approved: build the new node detached, **fill it while detached**, add
bound-matching CHECKs, single ATTACH, and make the flush's empty-leaf skip keep
the node out of the COMMIT-time reconcile.

I built: detached node + subtree, **ATTACH it empty**, and let the COMMIT-time
reconcile fill it — in place, because it is provably empty. Same acceptance
outcome (root never `AccessExclusive`, at either depth, proven by T1/T2 and by
mutations M1/M2), materially less machinery, and it avoids three hazards the
approved shape carries:

1. no cross-phase "already filled" signal is needed between the `ddl_command_end`
   sync and the COMMIT-time flush, so no double-fill is structurally possible
   (T4 becomes a guard rather than a mechanism);
2. no partition-constraint predicate has to be derived for a not-yet-attached
   child — the approved design needs one for both the fill and the CHECK, and the
   only sources available while detached (the source child's constraint, a probe
   table) are proxies whose failure mode is silently-wrong rows;
3. no duplicate risk against `refill_tree_defaults` when the IMV's DEFAULT
   partition already holds rows for the incoming bound (T5b covers this).

Hazard 2 is the decisive one, and it is now settled: attaching the child empty
also makes the CHECK unnecessary rather than merely optional (see H3 — an empty
child's validation scan costs 1.7 ms because there is nothing to scan), so the
approved design's only real advantage over this shape evaporates while its
predicate-derivation risk remains. **The deviation was reviewed and accepted.**

M2's result remains the key datum for anyone reconsidering: the sync half alone
leaves depth 1 open, so whatever replaces the in-place fill must still remove the
reconcile's DETACH at depth 1.

Known residual, accepted and deliberate: if `refill_tree_defaults` routes drained
default rows into the brand-new child, that child is no longer empty and the
reconcile falls back to the full DETACH/ATTACH swap — i.e. the root can still go
`AccessExclusive` in that narrow case. This fails toward doing the full, correct
work, which is the required direction. T5b pins the correctness of that path but
**does not** assert the lock shape for it.

---

## 7b. Benchmark — freeze window before / after

pgrx-managed PG 17.7 on port 28817. `.so` rebuilt and reinstalled between runs;
**BEFORE = `src/partition.rs` at `2f8b786`, AFTER = `8963faa`** (both verified by
`cargo pgrx install` immediately preceding the run). Fixture: 300 000 rows in the
existing branch, an incoming pre-populated branch of 300 000 rows (12 monthly
leaves at depth 2). A second session polls `SELECT count(*) … WHERE k = 1` — an
UNRELATED partition — 60 times at `lock_timeout='2s'`; a third samples `pg_locks`
on the IMV root every 50 ms. Two repetitions each.

| | BEFORE (`2f8b786`) | AFTER (`8963faa`) |
|---|---|---|
| **depth 2** — root `AccessExclusive` window | **0.794 s / 0.709 s** | **none observed** |
| depth 2 — root lock modes seen | AccessExclusive + ShareRowExclusive | ShareUpdateExclusive + ShareRowExclusive |
| depth 2 — reader max latency (unrelated partition) | **865 ms / 827 ms** | **31.7 ms / 24.9 ms** |
| depth 2 — ATTACH statement | 121.7 / 106.4 ms | 127.0 / 113.4 ms |
| depth 2 — COMMIT | 857 / 804 ms | 649 / 603 ms |
| **depth 1** — root `AccessExclusive` window | **0.479 s / 0.315 s** | **none observed** |
| depth 1 — reader max latency | **547 ms / 342 ms** | **19.3 ms / 12.7 ms** |
| depth 1 — ATTACH statement | 62.6 / 44.2 ms | 49.5 / 48.0 ms |
| depth 1 — COMMIT | 536 / 364 ms | 354 / 350 ms |
| oracle mismatches (all runs) | 0 | 0 |

Reading these:

* The freeze is gone at both depths: the root never reaches `AccessExclusive`,
  and a reader of an unrelated partition goes from ~0.5–0.9 s of blocking to
  ~13–32 ms of ordinary latency (~28×). At this fixture size the pre-fix block
  stays under the 2 s `lock_timeout`, so it shows as latency rather than as
  timeouts; the in-suite T1/T2 RED baseline, where the transaction is held open
  across the polls, produced hard 2 s lock timeouts instead.
* **The ATTACH statement's own cost is unchanged** (121.7 → 127.0 ms at depth 2;
  62.6 → 49.5 ms at depth 1 — both within run-to-run noise). This design moves
  **no** fill work into the ATTACH, unlike the approved design, which
  deliberately would have.
* Total transaction cost went *down* (857 → 649 ms, 536 → 354 ms): the in-place
  fill of a provably empty child skips the swap's `CREATE … LIKE`, DETACH,
  ATTACH, DROP and RENAME per node.
* Note the lock-window rows, not the statement-cost rows, are the ones that
  matter. `ATTACH`'s `ShareUpdateExclusive` on the parent is held to COMMIT
  regardless of how long the statement itself takes (see H3), so a faster ATTACH
  would not shorten any window.

## 7c. Adversarial review round — F1 confirmed and fixed

The reviewer found (SEVERITY 1) that the fix did not close the commonest field
shape. **Reproduced independently before touching anything:**

```sql
BEGIN;
CREATE TABLE lz_src_5 PARTITION OF lz_src FOR VALUES IN (5);
INSERT INTO lz_src SELECT 5, g FROM generate_series(1, 900) g;
SET CONSTRAINTS ALL IMMEDIATE;
```

| point | lock modes held on the IMV root |
|---|---|
| after the inline sync | AccessShare, ShareRowExclusive, **ShareUpdateExclusive** |
| after the COMMIT-time reconcile | … + **AccessExclusiveLock** |

A second session reading an unrelated partition at `lock_timeout='2s'` blocked
for the full 2 004 ms. **No DEFAULT partition anywhere**, so the previous
report's reachability claim was simply wrong.

**Why the emptiness gate missed it:** the load's own IMV maintenance delta lands
in the brand-new mirror child before the COMMIT-time reconcile reaches it, so
`tgt_empty=false` and the swap runs.

**The predicate that replaced it.** "Is the child empty?" is a proxy; the
property that licenses skipping the swap is "was this child created by this
transaction?" — whatever such a child holds arrived after transaction start, and
the swap would discard it anyway, so TRUNCATE + fill in place is equivalent.

The `pg_class.xmin = pg_current_xact_id()` candidate was **measured and
rejected**, not assumed: a child created inside sync's SPI scope carries the SPI
SUBtransaction's xid, so the probe answers `false` for exactly the children that
must be recognised (`xmin=6474 cur=6472` in the reproduction). Using it would
have produced a fix that never engages. The implementation therefore uses an
explicit hand-off — sync records the OIDs of the children it creates in a
transaction-local GUC (`is_local => true`), the reconcile tests membership.

Both probes fail toward the swap. That asymmetry is the point: a false "fresh"
would TRUNCATE rows predating the transaction (silent data loss), a false "not
fresh" only costs the slower always-correct path.

**TRUNCATE was verified, not assumed** (PG 17.7, `TRUNCATE <partition child>`):
locks are `AccessExclusive` + `Share` on the CHILD and **nothing at all on the
parent**; the parent's statement-level TRUNCATE trigger does **not** fire — the
same isolation the swap's detached fill relies on.

## 7d. Benchmark — corrected, and what the first one was actually measuring

The §7b table below is retained but **its fixture cannot exhibit F1** (it
attaches a pre-populated branch and does no further DML), so its "none observed"
overstated the fix's scope. The reviewer was right about that.

Re-run on an F1-capable fixture (create the partition and load it in one
transaction, mirror depth 1). `.so` rebuilt and reinstalled between runs;
BEFORE = `src/partition.rs` at `2f8b786`, AFTER = the fix branch.

| base / load | | BEFORE | AFTER |
|---|---|---|---|
| 300 000 / 50 000 | root `AccessExclusive` window | **0.082 s** | **none observed** |
| 300 000 / 50 000 | reader max latency (unrelated partition) | **184 ms** | **13 ms** |
| 300 000 / 50 000 | CREATE PARTITION statement | 34.5 ms | 33.0 ms |
| 300 000 / 50 000 | INSERT statement | 98.9 ms | 91.4 ms |
| 300 000 / 50 000 | COMMIT | 104.7 ms | 95.4 ms |
| all runs | oracle mismatches | 0 | 0 |

The statement costs are unchanged — this design still moves no work into the
partition-add statements.

**A discrepancy I chased down rather than reported as a pass.** At base 50 000 /
load 50 000 the AFTER run still showed a 0.162 s `AccessExclusive` window even
though T7 (900 rows) was green. Stepping the transaction statement by statement
showed the lock appears on the **INSERT**, not the flush, with
`INFO: pg_reflex: reconciled IMV 'f_imv' (partitioned, 2 children swapped)` —
a large delta escalates to a full partitioned reconcile that swaps every child,
including unchanged ones. Scoping probe: a large INSERT into an **existing**
partition with **no DDL at all** does the same (300 000 rows → root
`AccessExclusive`; 50 000 → `RowExclusive`), and identically on `2f8b786`. So it
is a pre-existing, independent defect, filed as
`untreated_bugs/2026-07-28_large_delta_full_reconcile_swaps_every_partition.md`.
The threshold is relative to the IMV's size, not absolute.

**Consequence, stated plainly:** the partition-add path is lock-free, but a
create-partition-and-bulk-load transaction can still freeze the IMV when the load
is large relative to the IMV — via that other defect, not this one.

## 8. Status — verification complete (updated after the review round)

Everything the previous session left owed is done and verified on the final tree:

1. `cargo pgrx test pg17 attach_new_partition` — **6 passed, 0 failed**, no hang.
   The worker-session rework holds: the test session never touches
   `la1_imv`/`la2_imv`/`la1_src`/`la2_src` itself, so the worker's cleanup `DROP`
   is never blocked and the fixtures leave nothing committed behind.
2. Full `cargo pgrx test pg17` — **1548 passed, 0 failed, 0 ignored**. The three
   tests `0314d9e` claimed to fix are green:
   `pg_xsu_guard_reconcile_failure_flags_known_stale`,
   `pg_cov_reflex_compact_all_imv_empty_registry`,
   `pg_pg_test_reflex_compact_all_imv_empty_catalog`. That commit's message is
   therefore true as written; no amendment needed.
3. Mutations M1 and M3 re-run on the final tree — see §6.
4. Benchmark — see §7b.
5. `cargo fmt --check` clean; `cargo clippy --features pg17 --all-targets` reports
   nothing for the changed files (the four `needless_borrow` warnings in
   `src/tests/pg_test_audit.rs` predate this branch).

Superseded by the review round: the suite is now **1550 passed, 0 failed** with
the two new F1 tests, and the lock tests each run against their own throwaway
database. That last change was forced: they are the only tests in the suite that
commit global state, and
`drop_deferred_imv_wipes_every_nonmaintenance_table` takes a cluster-wide census
of artifact relations, so it failed once when the two ran concurrently. Isolating
the fixtures into per-test databases removes the interference at the source
rather than by retry.

## 9. `untreated_bugs/` hygiene — narrowed, not deleted

`untreated_bugs/2026-07-27_sync_partition_add_holds_accessexclusive_on_imv_root.md`
now covers exactly the `refill_tree_defaults` residual from §7: when the IMV's
DEFAULT partition holds rows belonging to the incoming bound, the refill lands
them in the brand-new child, the child is no longer empty, and the reconcile
falls back to the full DETACH/ATTACH swap — so the root can still go
`AccessExclusive` in that narrow case. It records the reachability conditions,
two candidate fixes plus "leave it", and states explicitly that T5b pins the
correctness of that path but deliberately does **not** assert its lock shape.

## 10. Dead ends — do not re-explore

* **Deep `CARGO_TARGET_DIR`** (the scratchpad path): breaks `pg_ctl start` via the
  103-byte socket limit. Use `/private/tmp/tb2`.
* **`dblink_exec` with a row-returning statement**: fails with "statement
  returning results not allowed" (`dblink.c:1482`). Wrap `create_reflex_ivm` in
  `DO $mk$ BEGIN PERFORM …; END $mk$`.
* **Letting a `dblink` lock-timeout error propagate into the test**: an SPI error
  aborts the test transaction, so cleanup never runs. Wrap the read in a plpgsql
  function with `EXCEPTION WHEN OTHERS THEN RETURN -1` (a subtransaction).
* **Dropping the committed fixture at the end of the test transaction**: cannot
  work, that transaction still holds the locks. Only the worker, after its own
  COMMIT, can drop them.
* **Asserting `pg_locks` on an IMV root created inside the same `#[pg_test]`**:
  useless — the `CREATE TABLE` already left `AccessExclusive` held, so the
  assertion can never distinguish fixed from broken.
* **Adding a bound-matching CHECK to skip ATTACH validation**: works (H3), but is
  unnecessary here and strictly increases correctness risk. Do not add it back
  without a reason that is not latency.
