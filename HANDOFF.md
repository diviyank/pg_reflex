# HANDOFF — sub-partition swap data loss

Branch `fix/swap-flattens-subpartitioned-child` (worktree branch
`worktree-agent-af6c0dd061ece2667`), based on `integration/s1-batch`.

Rewritten by the author after resuming. The integrator's reconstruction was accurate about
the code but wrong about the evidence: the suite and the M1/M2 mutations *had* been run
before the interruption, they were simply never written down. Everything below has now been
re-run on this branch and is recorded with its numbers.

Fixes `untreated_bugs/2026-07-28_swap_flattens_subpartitioned_child_then_sync_empties_imv.md`
(on branch `worktree-agent-a006921b9bcaad1dd`). Partially closes
`untreated_bugs/2026-07-28_full_reconcile_swaps_every_partition_and_cascades.md`.

---

## 1. Step 0 — falsification results

### 1.1 Does the bug still reproduce on `integration/s1-batch`? **Yes, unchanged.**

Measured by reverting both halves of the fix on this branch (mutation M3 below) and running
the suite: a depth-2 passthrough IMV holding 400 rows goes to **0** after
`reflex_reconcile` + `reflex_sync_partitions`. Neither S1 fix altered the shape or the
reachability of the defect. The `is_fresh_partition` skip added by the S1 lock fix does not
protect it — a populated, non-fresh mirror child still takes the swap path.

### 1.2 Exposure criteria — the report's stated scope is **wrong**

The report says "only depth->=2 mirrors, i.e. IMVs created with a multi-column
`partition_by`". The second half of that is false.

* **`partition_depth >= 2` is NECESSARY.** A depth-1 mirror's children are plain relations,
  so `LIKE ... INCLUDING ALL` reproduces them faithfully. Verified in both directions by
  `pg_subpart_depth1_reconcile_unchanged_by_leaf_resolution`.
* **A depth-1 mirror can NOT have a partitioned child** through any pg_reflex path.
  `create_ivm` builds the mirror from `truncate_partition_tree(source, partition_depth)`,
  which *demotes* nodes sitting at `partition_depth` to leaves (clears `sub_strategy`), and
  the sync's shape-drift heal enforces the same predicate — it would actively DROP a
  partitioned depth-1 child as drift. Only hand-written DDL could produce one, and the next
  sync removes it.
* **`partition_depth >= 2` is NOT SUFFICIENT.** `execute_partition_swap_for_child` early-outs
  to an in-place fill when the child is empty or was created by this transaction's sync, and
  that path does not flatten. So the precise condition is
  **`partition_depth >= 2` AND the top-level mirror child holds rows** — which is every live
  IMV, but it is why an already-repaired mirror survives a second reconcile on an unfixed
  build (measured: `pg_subpart_reconcile_repairs_an_already_flattened_mirror` is GREEN at M3).
* **`partition_columns` arity is NOT the criterion.** An explicit multi-level `partition_by`
  is *impossible* on an aggregate IMV — `resolve_unique_columns` returns early when the plan
  is not passthrough (`src/create_ivm/mod.rs:213-216`), so `resolved_unique_columns` is always
  empty and the level->=2 check at `:632` can never pass. Depth->=2 aggregates therefore exist
  only via the auto-mirror path, which stores **`partition_columns` of length one** with
  `partition_depth = 2`. An exposure query keyed off `array_length(partition_columns,1) >= 2`
  misses every affected aggregate IMV. Filed as
  `untreated_bugs/2026-07-28_explicit_multilevel_partition_by_impossible_on_aggregates.md`.

### 1.3 Does step 1 alone leave data correct? **Yes — flattened IMVs are fine-but-armed.**

Confirmed twice, independently:

* At the RED baseline, `pg_subpart_reconcile_then_sync_keeps_depth2_imv_data` passed its
  post-reconcile row-count and oracle assertions and failed only at the *sync* step.
* `pg_subpart_reconcile_repairs_an_already_flattened_mirror` builds the flattened state by
  replaying the old swap's exact statements and asserts `assert_imv_correct` on it before
  repairing.

The pre-existing test `pg_subpart_global_reconcile_passthrough_multilevel` is green on the
unfixed code for the same reason: it checked the data after a reconcile but never the shape,
and never ran a sync afterwards. That is precisely why this went unnoticed.

**Operational consequence:** an IMV that has already flattened in the field is holding
*correct* data right now. It is not yet wrong — it is armed. The next partition sync (which
any source DDL triggers automatically) empties it.

---

## 2. The fix

Two complementary halves, both in `ead9845`.

**(a) `src/reconcile.rs` — resolve mirror leaves.** The partitioned branch iterated
`list_partition_children(anchor_source)` — *immediate* source children — so on a depth->=2
mirror the derived target child was a partitioned relation every time. It now expands the
source tree to leaves and maps each up to the IMV's mirror depth, exactly as
`reflex_reconcile_partition_impl` already did. At mirror depth 1 this yields the identical
set, so single-level IMVs are untouched.

**(b) `src/partition.rs` — refuse loudly.** `execute_partition_swap_for_child` now probes
`relkind` on the target and intermediate child and returns a clean `Err` naming the child and
the primitive that can rebuild it. It returns rather than raises, so the caller keeps its
transaction. With (a) in place this is a backstop, not a routine path.

The false doc comment at `reconcile.rs:424-431` (called out by the sibling report) was
corrected rather than deleted: it now states what the lock actually does, including the
residual depth-1 case.

**Not chosen:** making the swap build a genuine partitioned subtree. It reintroduces the whole
sub-tree DDL under the root's lock and is strictly more machinery than leaf resolution, which
the sibling report also argued.

---

## 3. Test results — all `cargo pgrx test pg16`, PostgreSQL 16.11

| run | tree | result |
|---|---|---|
| green | `ff8327a` (HEAD) | **1569 passed, 0 failed** |
| green | `c6f15d5` | 1568 passed, 0 failed |
| M1 | leaf resolution reverted, guard kept | **10 failed** |
| M2 | guard disabled, leaf resolution kept | **1 failed** |
| M3 | both reverted (true pre-fix) | **6 failed** |

`cargo fmt --check` clean. `cargo clippy` reports 5 warnings, **all pre-existing** on
`integration/s1-batch` (`src/trigger/mod.rs` dead code, 4 `needless_borrow` in
`src/tests/pg_test_audit.rs`); none from this branch.

### Mutation matrix — which mutation broke which test

**M3 (both halves reverted) — 6 red, every one for the intended reason:**

| test | failure |
|---|---|
| `pg_subpart_reconcile_then_sync_keeps_depth2_imv_data` | `0` vs `400` — the reported data loss |
| `pg_subpart_reconcile_then_source_ddl_keeps_depth2_imv_data` | `0` vs `400` — via routine source DDL |
| `pg_subpart_reconcile_keeps_depth2_mirror_shape` | `0` vs `2` partitioned children — the flattening |
| `pg_subpart_reconcile_then_sync_keeps_depth2_aggregate_imv_data` | `0` vs `2` — aggregate target mirror |
| `pg_subpart_swap_refuses_partitioned_child_and_leaves_state_intact` | swap returned `OK` |
| `full_reconcile_never_locks_imv_root_depth2` | root held `AccessExclusiveLock` |

**M1 (leaf resolution reverted, guard kept) — 10 red.** The guard converts the silent
flattening into `ERROR: partition reconcile failed`, so on top of the M3 set, **three
pre-existing tests** go red: `pg_subpart_global_reconcile_passthrough_multilevel`,
`pg_subpart_cte_passthrough_global_reconcile`,
`pg_subpart_cte_passthrough_sublevel_attach_swap`. Those three were green before this work
*only because the corruption was silent*. This is the decisive evidence that **half (b) alone
stops the data loss**, and that (a) is what makes the operation succeed rather than refuse.

**M2 (guard disabled, leaf resolution kept) — exactly 1 red:**
`pg_subpart_swap_refuses_partitioned_child_and_leaves_state_intact`. The guard is
independently pinned and nothing else depends on it, which is the correct signature for a
backstop.

No test stayed green under its own mutation.

---

## 4. What operators must do

### 4.1 Find affected IMVs

Both queries are asserted verbatim against real IMVs by
`pg_subpart_exposure_detection_queries_are_accurate`, in both directions.

Mirrors deeper than one level — on an unfixed build a `reflex_reconcile` will flatten these:

```sql
SELECT DISTINCT r.name
FROM public.__reflex_ivm_reference r
JOIN pg_inherits i ON i.inhparent = to_regclass(r.name)
JOIN pg_class c ON c.oid = i.inhrelid
WHERE r.enabled AND c.relkind = 'p';
```

Mirrors that have **already flattened** — data still correct, next sync empties them:

```sql
SELECT r.name, r.partition_depth
FROM public.__reflex_ivm_reference r
WHERE r.enabled
  AND COALESCE(r.partition_depth, 0) >= 2
  AND NOT EXISTS (SELECT 1 FROM pg_inherits i
                  JOIN pg_class c ON c.oid = i.inhrelid
                  WHERE i.inhparent = to_regclass(r.name) AND c.relkind = 'p');
```

Caveat on the second query: `partition_depth` NULL means "mirror the full source depth", so a
legacy row with NULL is excluded by the `COALESCE(...,0)` and must be inspected by hand. The
circulated `coalesce(partition_depth,1) >= 2` is a reasonable *want-depth* expression but is
**not** an exposure criterion on its own — it reports IMVs that are supposed to be deep, not
ones currently at risk, and it says nothing about whether the mirror is intact.

### 4.2 Repair an already-flattened IMV

**`SELECT reflex_reconcile('<imv>');` — and nothing else.** Verified by
`pg_subpart_reconcile_repairs_an_already_flattened_mirror`: it restores the depth-2 shape,
restores the leaves, refills the data, and passes the `EXCEPT ALL` oracle. Dropping and
recreating the IMV is not necessary.

Two warnings that matter more than the fix itself:

* **Do NOT run `reflex_sync_partitions` on a flattened IMV.** The shape-drift heal drops the
  flattened children and recreates them **empty**, with no refill. That is the step that
  destroys the data. `drop_orphans => FALSE` does not protect it.
* **`reflex_audit` gives dangerous advice here.** A flattened mirror surfaces as
  `partition-tree-drift` findings (the real leaves read as missing, the flattened parents as
  extra), and the prescribed fix is
  `SELECT reflex_sync_partitions('<imv>', TRUE);` (`src/audit/checks_b_drift.rs:407-426`) —
  exactly the destructive operation. Operators must run `reflex_reconcile` instead. This is
  a live "remedy that cannot clear its own finding" and is **not fixed on this branch**; see
  §5.
* On an **unfixed** build the repair is a stopgap, not a cure: the repaired mirror is
  immediately re-exposed, because the next `reflex_reconcile` sees non-empty children and
  flattens again. Deploy the fix, then repair.

---

## 5. `untreated_bugs/` hygiene — actions owed at integration

Neither report exists on this branch (both live on `worktree-agent-a006921b9bcaad1dd`), so
they cannot be edited here without manufacturing an add/add conflict between two branches
that both touch them. The integrator must apply the following to the **merged** tree:

1. **DELETE** `2026-07-28_swap_flattens_subpartitioned_child_then_sync_empties_imv.md`. Fully
   closed. Its acceptance criteria 1-5 are all covered by
   `src/tests/pg_test_subpartition_dataloss.rs`, and criterion 5's mutation signal is recorded
   as M3 above. Note when closing that its stated scope ("multi-column `partition_by`") was
   wrong — see §1.2.
2. **NARROW** `2026-07-28_full_reconcile_swaps_every_partition_and_cascades.md`. Its H1/H2
   lock finding is closed **for mirror depth >= 2 only**. Suggested residual text:

   > **Partially fixed.** `reflex_reconcile` now resolves mirror leaves, so each swap DETACHes
   > from the leaf's immediate parent. At mirror depth >= 2 that is a branch and the IMV root
   > is never taken `AccessExclusive` — pinned by
   > `full_reconcile_never_locks_imv_root_depth2`, which asserts both directions (no
   > `AccessExclusiveLock` on the root, `AccessExclusiveLock` present on the branch).
   > **Residual:** at mirror depth 1 the leaf's immediate parent IS the root, so a full
   > reconcile still holds `AccessExclusive` on it to commit and every reader blocks. Also
   > unchanged: the reconcile is not reader-free even at depth >= 2 — plan-time expansion locks
   > the branches a query reaches, so a reader still blocks behind whichever branch is
   > mid-swap. The cascade/dependent-staleness half of this report is untouched.
3. **KEEP** the new `2026-07-28_explicit_multilevel_partition_by_impossible_on_aggregates.md`
   (added on this branch, `e446505`). Not fixed here.
4. **FILE** the `reflex_audit` hazard from §4.2 as its own report if it is not already
   tracked: `partition-tree-drift` prescribes `reflex_sync_partitions(..., TRUE)`, which on a
   flattened mirror empties the IMV. Not fixed on this branch — the flattened shape can no
   longer be *created* after this fix, so the hazard only affects mirrors already flattened in
   the field, but that is exactly the population being told to run the audit.

---

## 6. Queued second task, not started

`untreated_bugs/2026-07-28_partitioned_reconcile_destroys_dependent_imvs.md` (on
`worktree-agent-a006921b9bcaad1dd`, commit `23e3909`). Re-establish its shape on top of this
work rather than assuming — leaf resolution changes which relations the swap's `ALTER TABLE`
statements name, and therefore what `ddl_command_end` sees, so its reachability may have moved.

## 7. Constraints honoured

Code + tests only. No version bump, no `CHANGELOG.md`, no `sql/*--*.sql` migration, no new
registry column. All fixtures are real IMVs over real sources; correctness is asserted with
`assert_imv_correct` / bidirectional `EXCEPT ALL`, never string assertions on generated SQL.
No test was weakened after being written.
