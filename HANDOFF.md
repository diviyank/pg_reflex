# HANDOFF — sub-partition swap data loss (interrupted run)

Written by the integrator, not the author: the authoring session hit an API session
limit mid-run (resets 13:50 Europe/Paris, 2026-07-28) and left no handoff of its own.
Everything below is reconstructed from the committed history and the working tree as
found. **Treat any claim here as unverified until re-checked** — in particular, no
test results were reported by the author.

Branch: `worktree-agent-af6c0dd061ece2667`, based on `integration/s1-batch`
(= `main` + both completed S1 fixes, merged tree green at 1560 passed / 0 failed).

## Commits, oldest first

| commit | what |
|---|---|
| `d3706a3` | test: pin depth-2 mirror survival across reconcile + partition sync |
| `ead9845` | fix: reconcile mirror leaves, refuse to swap a partitioned child |
| `02e2b64` | test: pin the reconcile lock on the branch, not the IMV root |
| `7371dfc` | style: rustfmt the swap refusal guard |
| `9ae9593` | test: pin reflex_reconcile as the repair for an already-flattened mirror |
| `e446505` | docs: file multi-level partition_by rejection on aggregate IMVs (committed by the integrator from the working tree) |

Both halves of the intended fix are present in `ead9845`:

* a refusal guard in `execute_partition_swap_for_child` (`src/partition.rs`) that returns
  `Err` on a `relkind='p'` target/intermediate child rather than silently flattening it
  via `LIKE ... INCLUDING ALL`, with `is_partitioned_relation` as its helper;
* leaf resolution in `src/reconcile.rs` so the swap operates on mirror leaves rather than
  immediate children.

`9ae9593` suggests the author established that `reflex_reconcile` repairs an
already-flattened mirror — which, if it holds, is the answer to "what must operators with
already-flattened IMVs do". **Verify this before repeating it to anyone.**

## State when interrupted

The author's last words were "while the mutation run finishes". The working tree held a
**mutation, not work**: the refusal guard and most of the `reconcile.rs` change were
removed relative to HEAD, i.e. the fix reverted to watch tests go RED. The integrator
saved that diff to `/private/tmp/rfx-dl-mutation-in-progress.patch` and restored the
worktree to HEAD, so the fix is intact and the tree is clean.

**No mutation results were reported.** The mutation matrix is therefore owed in full.

## What the next session must do first

1. `export CARGO_TARGET_DIR=/private/tmp/rfx-dl`; `df -h /private/tmp` (below ~5 GB
   `cargo pgrx test` hangs silently). **pg16 only** — pg17 is used for integration runs.
2. Run the full suite. Nothing on this branch has a reported test result.
3. Re-run the mutations and report them: reverting the refusal guard, and reverting the
   leaf resolution, must each turn a specific test RED. Apply
   `/private/tmp/rfx-dl-mutation-in-progress.patch` to reproduce the author's in-flight
   mutation if useful, but **restore afterwards**.
4. Settle the Step 0 questions the author was working on and that are not yet answered in
   writing: does the bug reproduce on the integration branch (the two S1 fixes changed
   sync and reconcile substantially); what are the **verified** exposure criteria — is
   `partition_depth >= 2` necessary and sufficient, and can a depth-1 mirror ever have a
   partitioned child; and does step 1 alone leave data correct, which decides whether a
   field IMV that already flattened is fine-but-armed or already wrong.
5. Produce an accurate operator detection query. The one circulated so far
   (`coalesce(partition_depth,1) >= 2`) is inference and unverified.
6. Then `untreated_bugs/` hygiene: narrow or remove
   `2026-07-28_swap_flattens_subpartitioned_child_then_sync_empties_imv.md`, and update
   `2026-07-28_full_reconcile_swaps_every_partition_and_cascades.md` if the leaf-resolution
   change also closed its lock finding.

## Queued second task, not started

`untreated_bugs/2026-07-28_partitioned_reconcile_destroys_dependent_imvs.md` (on branch
`worktree-agent-a006921b9bcaad1dd`, commit `23e3909`). `reflex_reconcile` on a partitioned
IMV empties its dependents — each swap's `ALTER TABLE` fires `ddl_command_end`, whose
auto-sync mirrors the parent's transient mid-swap child set and drops the real child as an
orphan. Proposed direction: a `__reflex_`-name guard on the dependent auto-sync (likely
load-bearing on its own), optionally reusing the cascade block at `partition.rs:1805-1828`.
It may be narrowed or closed by the leaf-resolution change in `ead9845` — re-establish its
shape on top of this work rather than assuming.

## Constraints

Code + tests only. No version bump, no `CHANGELOG.md`, no `sql/*--*.sql` migration —
integration owns those. Real IMVs over real sources as fixtures, never hand-inserted
registry rows. `assert_imv_correct` / bidirectional `EXCEPT ALL` for correctness, not
string assertions. Never weaken a test to make it pass.
