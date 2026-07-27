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

**But the CHECK turned out not to be needed, and I deliberately did not add one.**
The parent lock during the validation scan is `ShareUpdateExclusiveLock`, which
does not conflict with `AccessShare` — readers are unblocked *whether or not*
validation runs. The CHECK therefore buys latency, not availability, at the price
of disabling PostgreSQL's own proof that the incoming rows match the bound. Per
the asymmetric-correctness rule I let PG validate. In the shipped design the
child is attached while still **empty**, so validation is free anyway and the
question is moot — no predicate has to be derived at all, which also removes the
whole class of "derived predicate is subtly wrong → silently wrong data" risk.

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

## 6. Self-mutation — all four run, all confirmed

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

**This was my call and it should be reviewed.** If the reviewer wants the
approved shape, M2's result is the key datum: the sync half alone leaves depth 1
open, so whatever replaces the in-place fill must still remove the reconcile's
DETACH at depth 1.

Known residual, accepted and deliberate: if `refill_tree_defaults` routes drained
default rows into the brand-new child, that child is no longer empty and the
reconcile falls back to the full DETACH/ATTACH swap — i.e. the root can still go
`AccessExclusive` in that narrow case. This fails toward doing the full, correct
work, which is the required direction. T5b pins the correctness of that path but
**does not** assert the lock shape for it.

---

## 8. What I was doing when I stopped

Mid-verification of `0314d9e`. I had just applied the reader/worker rework to
`src/tests/pg_test_partition_attach_locks.rs`, run `cargo fmt`, and was about to
re-run `cargo pgrx test pg17 attach_new_partition`. That run has **not** happened.

## 9. Next session, in order

1. `df -h /private/tmp` first. Below ~5 GB free, `cargo pgrx test` hangs silently.
2. `export CARGO_TARGET_DIR=/private/tmp/tb2` and run
   `cargo pgrx test pg17 attach_new_partition`. Expect 6 tests. If T1/T2 hang,
   the worker-session rework is still deadlocking — check that **nothing in the
   test session itself** ever touches `la1_imv`/`la2_imv`/`la1_src`/`la2_src`,
   since any such lock blocks the worker's cleanup `DROP`.
3. Re-run the **full** `cargo pgrx test pg17`. Specifically confirm
   `xsu_guard_reconcile_failure_flags_known_stale` and the two empty-registry
   tests are green — those are the three that `0314d9e` claims to fix.
4. Re-run mutation M1 and M3 once more on the final tree (they were run on
   `4a75c92`, not on `0314d9e`).
5. **The benchmark was never run.** `benchmarks/` has no lock/freeze script; I
   wrote one at `/private/tmp/claude-502/.../scratchpad/bench_freeze.sh`
   (may be gone — it is outside the worktree; rewrite if so). It needs
   `cargo pgrx install` + `cargo pgrx start pg17`, a fixture, a reader loop with
   `lock_timeout=2s`, and a `pg_locks` sampler, run once on `2f8b786` and once on
   HEAD. Owed numbers: freeze window before/after, and the cost of the ATTACH
   statement itself before/after (the fix moves no work into it in my design,
   unlike the approved one — worth stating explicitly).
6. Then `untreated_bugs/` hygiene: the report is still present and must be
   removed, or narrowed to the `refill_tree_defaults` residual in §7.

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
