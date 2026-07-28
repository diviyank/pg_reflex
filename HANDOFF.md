# HANDOFF — partitioned reconcile destroys its dependents

Branch `fix/swap-ddl-destroys-dependents`, worktree
`.claude/worktrees/agent-a0fa941f00af9d131`, based on
`fix/swap-flattens-subpartitioned-child` @ `689ab95`. **PostgreSQL 17 only**
(a reviewer is on pg16 and `cargo pgrx test` shares `~/.pgrx/<ver>/pgrx-install`),
`CARGO_TARGET_DIR=/private/tmp/rfx-dep`.

Treats
`git show worktree-agent-a006921b9bcaad1dd:untreated_bugs/2026-07-28_partitioned_reconcile_destroys_dependent_imvs.md`.

The predecessor branch's handoff for the sub-partition fix is kept verbatim
below the horizontal rule; it is not mine and should not be edited.

## Step 0 — falsification results

**It still reproduces on this branch.** `c80113b` changed the swap set from the
source's immediate children to the mirror's leaf set, but at mirror depth 1 the
two coincide, so the `ALTER TABLE`s still name the IMV root and the event
trigger still sees the transient child set.

Measured on `689ab95`, pg17, real IMVs over a real LIST(k) source:

| test | before the fix |
|---|---|
| T1 partitioned parent + auto-partitioned dependent | dependent 3 rows → **0** |
| T2 dependent mirror | gains `rdd2d___reflex_swap_tgt_*`, loses the real child |
| T3 chain A→B→C | **both** B and C destroyed |
| T4a unpartitioned dependent of a partitioned parent | **10 oracle mismatches** — stale |
| T4b unpartitioned parent (control) | green, as the report claims |

**Orphan-drop step confirmed** (report §4 was right): the drop is
`drop_bound_collision_orphan`, reached from the *dependent's own*
`reflex_sync_partitions`, not a shape-drift heal —
`NOTICE: dropped confirmed orphan partition 'rdd3b_rdd3a_rdd3s_c' (bounds matched incoming child 'rdd3b___reflex_swap_tgt_rdd3a_rdd3s_c')`.

**Trigger surface.** `reflex_reconcile` is the only entry point that leaves the
damage standing. `reflex_reconcile_partition` and the COMMIT-time flush issue
the same destructive DDL, but their own dependent cascade re-syncs and refills
the dependent afterwards, so the damage is transient there.

**New finding not in the report.** Destruction is only half of it. A dependent
that *cannot* be destroyed (unpartitioned — nothing mirrors the swap tables into
it) is still left silently STALE, because DETACH/ATTACH moves no rows and fires
no data trigger. The report's §3 "the unpartitioned path propagates" holds only
for an unpartitioned **parent** (T4b). Consequence for the fix: the
`__reflex_`-name guard alone is **not sufficient** — on its own it converts data
destruction into silent staleness, the same class of defect.

## Fix — three coupled parts

1. **`src/lib.rs` `__reflex_on_ddl_command_end`** returns immediately when
   `pg_reflex.internal_swap_root` is set. Nothing but pg_reflex's own swap DDL
   can run inside that window and none of it is a source change, so the guard is
   O(1) with no catalog lookup — no name matching to get subtly wrong.
2. **`src/partition.rs` `execute_partition_swap_for_child`** becomes a thin
   wrapper bracketing the (renamed) `swap_partition_child_ddl` with
   `set_internal_swap_root`. Single choke point: covers `reflex_reconcile`,
   `reflex_reconcile_partition` and the flush at once.
3. **`src/reconcile.rs` `cascade_partitioned_rebuild_to_dependents`**, called
   from `reflex_reconcile_with_orphans` after a successful rebuild and **only**
   for partitioned IMVs. Fans out `reflex_reconcile(dep)` over `graph_child`.

Placement of (3) at the public entry point rather than inside `reconcile_one` is
load-bearing: the chain descent rebuilds generated sub-IMVs through
`reconcile_one` with their triggers suppressed *because* they must not
propagate, and a cascade there would send a generated child back up into the
root currently rebuilding it. Restricting it to the partitioned case is equally
load-bearing: cascading after an unpartitioned rebuild would rebuild a consumer
that also has the rebuild's delta staged for COMMIT, double-counting it.

**Rejected:** reusing `build_scoped_cascade_reconcile` and the 80-line 3-way
dispatch from `reflex_reconcile_partition_impl`. For a FULL reconcile every key
is affected, so key-scoping degenerates into the full rebuild `reflex_reconcile`
already performs, while deriving `affected_keys` costs an extra
`SELECT DISTINCT` per partition — more code, more cost, no behaviour change.

**Complexity: O(D) dispatches per reconcile and O(N) `SET LOCAL` statements.**
Not superlinear in either.

## Author fix round (adversarial review F1/F2/F4)

Review verdict was DO-NOT-MERGE on **F1**, which I concede in full.

* **F1 (HIGH, fixed).** The cascade called the one-argument `reflex_reconcile(dep)` =
  `drop_orphans => TRUE`, while the caller's `drop_orphans` was in scope and
  ignored. `reflex_reconcile(parent, FALSE)` therefore destroyed a dependent's
  PRESERVED orphan partition — and the reachable caller is the bad one,
  `reflex_doctor(fix => true, drop_orphans => false)`. Now
  `cascade_partitioned_rebuild_to_dependents(view_name, drop_orphans)` forwarding to
  `reflex_reconcile_with_orphans(dep, drop_orphans)`. **T7** pins it.
* **F2 (MEDIUM, fixed).** The cascade ran *above* the `child_failed` early return, so a
  reconcile deliberately reported as ERROR first laundered known-stale content into every
  dependent and cleared their `known_stale`. Moved below the gate.
* **F4 (re-graded MEDIUM, fixed).** **T9** is the missing negative coverage: the GUC must
  be `''` after a reconcile, and ordinary source DDL in the same transaction must still
  mirror into parent *and* dependent. Filed as LOW on the assumption that a leaked GUC
  causes staleness; M4 shows it also breaks two *oracle* tests on the sub-partition path,
  so the real failure mode is **wrong data**. See the M4 row below.

**F2 is pinned by T8.** My earlier conclusion that it could not be tested was **wrong**,
and the refutation is worth recording. The parts were right — `REBUILDABLE_NODE` and
`is_decomposed_wrapper_row` are mutually exclusive, and `COALESCE(ref.enabled, TRUE)`
closes the disabled route — but I stopped at the first mechanism I tried for the one
remaining route (planting a VIEW on a swap-table name; the `CREATE TABLE` collision
**raises** instead of returning a soft `Err`, so it aborts rather than setting
`child_failed`) and generalised from that single failure to "unreachable".

The reachable fixture needs a CTE that is BOTH an aggregate (→ `REBUILDABLE_NODE`, not a
wrapper) and emits the partition column (→ `compute_cte_partition_subset` partitions the
sub-IMV). DETACHing one of that sub-IMV's intermediate children then makes the
pre-reconcile sync's `CREATE … IF NOT EXISTS … PARTITION OF` skip the still-existing name
rather than re-attach it, so the swap reads an empty bound and returns the soft
`Err("missing intermediate bound")`. No hand-written registry state anywhere.

T8 asserts an **exact** before/after aggregate, not a threshold — the reviewer's first
version of this fixture was a false green precisely because it thresholded on a value the
natural data already exceeded. T8 additionally asserts the root WAS rebuilt, so the test
pins the co-reachable `own = RECONCILED` + `child_failed = true` case rather than a plain
early exit.

## Self-mutation

| mutation | RED | GREEN |
|---|---|---|
| **M1** guard defeated (`IF _swap_root IS NOT NULL AND FALSE`) | T5 only, with `rdd6d___reflex_swap_tgt_rdd6p_rdd6s_c` | T1-T4 |
| **M2** cascade call removed | T1 (6 oracle mismatches), T3, T4a | T2, T5, T4b |
| **M3** `drop_orphans` hardcoded back to `true` | T7 only — `rdd8d_rdd8p_rdd8s_c` destroyed | all others |
| **M4** `set_internal_swap_root(client, None)` deleted | **three** tests — T9 (GUC left as `"rddad"`), `pg_fuzz_subpartition_swap_sequence_matches_recompute`, and its shallow variant. Suite 1577/3. | all others |
| **M5** cascade restored above the `child_failed` gate | T8 only — dependent 45039 (drifted) → 42039 (cascaded), the exact 3×1000 drift | all others |

**M4's blast radius corrects the F4 grade upward.** The two extra tests are *oracle*
tests, so a leaked `internal_swap_root` produces oracle-detectable **wrong data** on the
sub-partition path — not the silent staleness the original LOW grade assumed. While the
GUC is set the event trigger does nothing at all: no dependent auto-sync, no pending
enqueue, no alter-source alarm. The doc comment on
`execute_partition_swap_for_child` now says this explicitly and names the three tests to
re-run.

I originally wrote that each mutation moves exactly one property's tests; **that sentence
was wrong and is withdrawn.** M1, M3 and M5 do isolate to a single test each; M4 does not.

M1 initially left **all** tests green — a false green. The cascade repairs a
mirror the swap corrupted, so a test that goes through `reflex_reconcile` cannot
distinguish "never corrupted" from "corrupted then repaired". **T5 was added for
this**: it drives one swap through `tests.crate_test_partition_swap_for_child`
with no cascade behind it. That is the only assertion pinning the guard.

M2's signature is the design argument in miniature: with the guard but no
cascade, T1 fails as *staleness* (6 mismatches), not emptiness. The name guard
alone converts data destruction into silent staleness.

## Scaling — mechanism confirmed, numbers NOT reproducible from this tree

The reviewer is right to discount the raw numbers: no driver was committed, and they were
taken on pg17 while review ran on pg16. A reusable
`benchmarks/bench_partition_scaling.sh` on another branch will be used to re-measure at
integration. **Read the table below as an illustration of the mechanism, not as a
committed benchmark result.**

The mechanism itself is not in doubt and is confirmed by M1/M2: under the unguarded build
each swap's `ALTER TABLE` re-enters `__reflex_on_ddl_command_end`, which runs a full
`reflex_sync_partitions` over every partitioned dependent — O(N) swaps × O(N) dependent
tree × D dependents. Note also that the comparison below is "fixed" vs "guard defeated",
which isolates the *guard*; the cascade is a cost on top, not part of the saving.

Reconcile of a partitioned IMV with one auto-partitioned dependent:

| | N=10 | N=50 | ratio (linear = 5.0) |
|---|---|---|---|
| **fixed** | 697 ms | 4 652 ms | **6.68** |
| guard defeated (= base behaviour) | 1 378 ms | 18 200 ms | **13.21** |

N = partition count; dependent count D = 1; rows = 40·N spread one partition-key
per partition. The fix's own additions are **O(D) reconcile dispatches + O(N)
`SET LOCAL`** — except on a DAG with fan-in, where the cascade costs one dispatch
per *path* rather than per node (filed, see below).

## State

- Tests: `src/tests/pg_test_reconcile_dependent_dataloss.rs` (9 `#[pg_test]`),
  included from `src/lib.rs`.
- All 9 GREEN, and every one measured RED under a mutation of the property it
  pins (M1-M5).
- Full `cargo pgrx test pg17`: **1581 passed, 0 failed**. `cargo fmt` clean.
  `cargo clippy` — 4 pre-existing `needless_borrow` warnings in
  `src/tests/pg_test_audit.rs`, none from this branch.
- No registry column added. No version bump / CHANGELOG / `sql/*--*.sql`
  migration — integrator owns those. **The migration must replay the
  `__reflex_on_ddl_command_end` body**, or upgraded installs keep the
  destructive one.

## Reports

The parent report lives on `worktree-agent-a006921b9bcaad1dd` (not in this
tree), so it cannot be deleted from here. It is **fully closed** — the
integrator should remove
`untreated_bugs/2026-07-28_partitioned_reconcile_destroys_dependent_imvs.md`.

Its three adjacent defects survived and are filed separately here:

- `2026-07-28_sync_trigger_suppression_alter_blocks_reconcile_under_error_policy.md`
  — the parent report's attribution of the `error`-policy block to the swap is
  **falsified**; it comes from `reflex_sync_partitions`' own `DISABLE TRIGGER USER`.
- `2026-07-28_doctor_mislabels_residue_and_reports_fixed_without_rechecking.md`
- `2026-07-28_alter_source_alarm_suppressed_by_name_shape_not_provenance.md`
  — the parent report's *inferred* suppression hole, now confirmed by code read.

From the review round, filed not folded in:

- `2026-07-28_dependent_cascade_has_no_visited_set_on_dag_fanin.md` (F3) — no
  visited set, so a fan-in node is rebuilt once per path. Correctness unaffected.
- `2026-07-28_scoped_cascade_fallback_escalates_drop_orphans.md` — the same
  one-arg escalation as F1 at `src/partition.rs:2331`, but **pre-existing** and
  not reachable from `reflex_reconcile(x, FALSE)`. Its Step 0 is to establish
  reachability at all; a no-fix-plus-comment is a legitimate outcome.

Not filed, carried as a note: the cascade is not gated by `inside_trigger()`
while the chain descent deliberately is. The reviewer could not turn this into
wrong data and neither could I; it is unbounded extra work at COMMIT time, not a
correctness defect.

## Recovery for operators already hit

`SELECT reflex_rebuild_imv('<dependent>')` — one call, converges, repairs both
the contents and the partition mirror.

---

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

**(a) `src/reconcile.rs` — target the mirror's leaf set.** The partitioned branch iterated
`list_partition_children(anchor_source)` — *immediate* source children — so on a depth->=2
mirror the derived target child was a partitioned relation every time. It now derives the set
from `truncate_partition_tree(source_tree, mirror_depth)` filtered to leaves — the same
function `create_ivm` and `reflex_sync_partitions` use to *build* the mirror, so the swap set
matches the mirror by construction. At mirror depth 1 this yields exactly the source's
immediate children, so single-level IMVs are untouched.

> The first version of (a) filtered the raw source tree to leaves and mapped each up to
> `mirror_depth` via `leaf_ancestor_chain` / `ancestor_bare_at_depth`. Adversarial review
> found that this **silently dropped any source node that is partitioned but currently
> childless** — it is not a leaf and owns no leaf to stand in for it, so its mirror child was
> never rebuilt while the reconcile still returned `RECONCILED`. Reached by a branch
> pre-created ahead of its leaves and by retention dropping a branch's leaves. Deriving from
> `truncate_partition_tree` closes the class: truncation *demotes* nodes at `mirror_depth` to
> leaves, so a childless branch there is included (its mirror child is a plain table that can
> hold rows) and one above `mirror_depth` is still excluded (its mirror child is partitioned
> and holds nothing itself). It also removes the ancestor-mapping helpers from this path
> entirely. Pinned by `pg_subpart_reconcile_rebuilds_childless_branch_mirror_child` and
> `pg_subpart_reconcile_rebuilds_branch_whose_leaves_were_dropped`; isolated by M4.

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
| green | HEAD (post-review round) | **1572 passed, 0 failed** |
| green | `62fe6c7` (pre-review) | 1569 passed, 0 failed |
| M1 | leaf resolution reverted, guard kept | **10 failed** |
| M2 | guard disabled, leaf resolution kept | **1 failed** |
| M3 | both reverted (true pre-fix) | **6 failed** |
| M4 | F1 fix reverted only (leaf-filter restored) | **2 failed** |

`cargo fmt --check` clean. `cargo clippy` reports 5 warnings, **all pre-existing** on
`integration/s1-batch` (`src/trigger/mod.rs` dead code, 4 `needless_borrow` in
`src/tests/pg_test_audit.rs`); none from this branch.

### Mutation matrix — which mutation broke which test

The four mutations are not nested, so the counts are not cumulative. Each isolates one
property.

**M3 (both halves reverted — the true pre-fix tree) — 6 red:**

| test | failure |
|---|---|
| `pg_subpart_reconcile_then_sync_keeps_depth2_imv_data` | `0` vs `400` — the reported data loss |
| `pg_subpart_reconcile_then_source_ddl_keeps_depth2_imv_data` | `0` vs `400` — via routine source DDL |
| `pg_subpart_reconcile_keeps_depth2_mirror_shape` | `0` vs `2` partitioned children — the flattening |
| `pg_subpart_reconcile_then_sync_keeps_depth2_aggregate_imv_data` | `0` vs `2` — aggregate target mirror |
| `pg_subpart_swap_refuses_partitioned_child_and_leaves_state_intact` | swap returned `OK` |
| `full_reconcile_never_locks_imv_root_depth2` | root held `AccessExclusiveLock` |

Both F1 probes are GREEN at M3 — correctly: the pre-fix `list_partition_children` covered
childless branches. F1 was a regression this branch introduced and has now removed.

**M1 (leaf resolution reverted, guard kept) — 10 red. Not a superset of M3.** The refusal
test is correctly GREEN here (the guard is kept), and two of this branch's own tests join:

* 5 shared with M3: the four data-loss/shape tests plus the lock test.
* 3 **pre-existing** tests, which fail with `ERROR: partition reconcile failed`:
  `pg_subpart_global_reconcile_passthrough_multilevel`,
  `pg_subpart_cte_passthrough_global_reconcile`,
  `pg_subpart_cte_passthrough_sublevel_attach_swap`. They were green before this work
  *only because the corruption was silent*.
* 2 more of this branch's: `pg_subpart_exposure_detection_queries_are_accurate`,
  `pg_subpart_reconcile_repairs_an_already_flattened_mirror`.

5 + 3 + 2 = 10. This is the decisive evidence that **the guard alone stops the data loss**,
and that leaf resolution is what makes the operation succeed rather than refuse.

**M2 (guard disabled, leaf resolution kept) — exactly 1 red:**
`pg_subpart_swap_refuses_partitioned_child_and_leaves_state_intact`. Held at 1 across both
review rounds. Nothing else depends on the guard, which is the correct signature for a
backstop rather than a load-bearing check.

**M4 (F1 fix reverted — `truncate_partition_tree` swapped back for the source-leaf filter,
everything else intact) — exactly 2 red:**
`pg_subpart_reconcile_rebuilds_childless_branch_mirror_child` (`1` vs `0` rows of surviving
drift) and `pg_subpart_reconcile_rebuilds_branch_whose_leaves_were_dropped`
(`EXCEPT ALL oracle failed: 25 mismatches`).

No test stayed green under its own mutation.

---

## 4. What operators must do

### 4.1 Find affected IMVs

Both queries are asserted verbatim against real IMVs by
`pg_subpart_exposure_detection_queries_are_accurate`, in both directions, including a
legacy NULL-`partition_depth` fixture.

Both resolve the registry name through `quote_ident` per component. A bare
`to_regclass(r.name)` **down-cases** an unquoted mixed-case name and yields NULL — which
would make query 1 *miss* an affected IMV and make query 2 *falsely report* one (its
`NOT EXISTS` over an empty set is TRUE). The codebase documents the same hazard at
`src/reconcile.rs:1170-1175`.

Query 1 — mirrors deeper than one level. On an unfixed build a `reflex_reconcile` flattens
these, and the next partition sync then empties them:

```sql
SELECT DISTINCT r.name
FROM public.__reflex_ivm_reference r
JOIN pg_inherits i
  ON i.inhparent = to_regclass(CASE WHEN r.name LIKE '%.%'
       THEN quote_ident(split_part(r.name, '.', 1)) || '.'
            || quote_ident(split_part(r.name, '.', 2))
       ELSE quote_ident(r.name) END)
JOIN pg_class c ON c.oid = i.inhrelid
WHERE r.enabled AND c.relkind = 'p';
```

Query 2 — partitioned mirrors that are FLAT but should not be. Data is still correct; the
next sync empties them:

```sql
SELECT r.name, r.partition_depth,
       CASE WHEN r.partition_depth IS NULL
            THEN 'UNKNOWN — mirror depth unrecorded, inspect the source tree'
            ELSE 'ALREADY FLATTENED' END AS status
FROM public.__reflex_ivm_reference r
WHERE r.enabled
  AND r.partition_columns IS NOT NULL
  AND array_length(r.partition_columns, 1) > 0
  AND (r.partition_depth IS NULL OR r.partition_depth >= 2)
  AND NOT EXISTS (
        SELECT 1 FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = to_regclass(CASE WHEN r.name LIKE '%.%'
                 THEN quote_ident(split_part(r.name, '.', 1)) || '.'
                      || quote_ident(split_part(r.name, '.', 2))
                 ELSE quote_ident(r.name) END)
          AND c.relkind = 'p');
```

**NULL `partition_depth` must not be excluded.** It means "mirror the full source depth", so
an already-flattened legacy row has no partitioned child either — excluding it would make it
invisible to *both* queries, which is precisely the dangerous population. It is reported as
UNKNOWN instead. `partition_depth` is always populated by current code
(`src/create_ivm/mod.rs:649`, `:744`), so this only affects rows upgraded from before the
column existed.

The circulated `coalesce(partition_depth,1) >= 2` is a reasonable *want-depth* expression but
is **not** an exposure criterion on its own: it reports IMVs that are supposed to be deep, not
ones currently at risk, it says nothing about whether the mirror is intact, and its
`coalesce(...,1)` silently drops the legacy population.

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

Both halves of that warning are measured, not reasoned:
`pg_subpart_reconcile_repairs_an_already_flattened_mirror` pins that the audit *prescribes*
`reflex_sync_partitions`, and `pg_subpart_sync_on_a_flattened_mirror_empties_it_silently`
pins what running it does — `400` rows to `0`, a **success** return string, `known_stale`
left FALSE, with `drop_orphans => FALSE`.
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
   wrong — see §1.2. **Safe only after the F1 fix (`c80113b`) is in the merged tree**; the
   reviewer held this deletion pending that, and it has since landed.
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
3. **KEEP** the three reports added on this branch, none of which are fixed here:
   * `2026-07-28_explicit_multilevel_partition_by_impossible_on_aggregates.md` (`e446505`)
   * `2026-07-28_audit_partition_drift_remedy_empties_a_flattened_mirror.md` (`62fe6c7`)
   * `2026-07-28_soft_reconcile_error_string_discarded_by_perform_callers.md` (review F2 —
     pre-existing, adjacent to
     `2026-07-27_reconcile_partition_commits_destructive_sync_on_failure.md`)

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
