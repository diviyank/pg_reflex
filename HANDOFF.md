# HANDOFF — partition-scaling benchmark

## Task

Build a reusable benchmark that measures pg_reflex cost as a function of N =
number of partitions, and use it to check whether the current release batch
introduced a superlinear regression. Code + benchmark only — no version bump,
no CHANGELOG, no migration.

## Environment (important)

Two other agents hold the pgrx-managed pg16 / pg17 installs. **Do not run
`cargo pgrx test` or `cargo pgrx run`.** This work uses the separate homebrew
PostgreSQL 17.7:

```
PGBIN=/opt/homebrew/opt/postgresql@17/bin
export CARGO_TARGET_DIR=/private/tmp/rfx-bench     # short path; ~750 MB
export CARGO_INCREMENTAL=0
cargo pgrx install --pg-config $PGBIN/pg_config --no-default-features --features pg17
```

`$PGBIN/pg_config --pkglibdir` is `/opt/homebrew/lib/postgresql@17`, which is
disjoint from `~/.pgrx/17.7/pgrx-install/lib/postgresql`. Verified before
installing.

Disk on `/private/tmp` is tight. Other agents own `/private/tmp/rfx-dep` and
`/private/tmp/rfx-dl` — do not delete them.

**The filesystem hit 100% early in this session and every timing taken then was
worthless** — coefficients of variation of 50-70% that dropped to 0-4% once
space was freed, with no error raised anywhere. All measurements taken during
that window were discarded. The driver now refuses to start below 5 GB free and
records `df` and load average in the results header. Check the header of any
results file before believing it.

**Persistent change to the maintainer's machine:** `max_locks_per_transaction`
on the homebrew PostgreSQL 17 is now **2048** (was the default 64), set via
`ALTER SYSTEM` + `brew services restart postgresql@17`. It is a prerequisite of
this harness — `reconcile_full` runs its repetitions in one transaction and
each repetition recreates the swap tables with fresh OIDs, so the footprint
accumulates to ~32k lock entries at N=200. Recommend leaving it in place: it
only enlarges the shared lock table and this box has the headroom. Recorded
here and in the header of `bench_partition_scaling.sh` so it is findable.

A *single* reconcile needs no such setting — it holds `42N + 38` locks and
survives to N ≈ 490 on an idle default cluster. That product-side ceiling is
filed as
`untreated_bugs/2026-07-28_full_reconcile_exhausts_max_locks_per_transaction.md`.

## Deliverables

- `benchmarks/bench_partition_scaling.sql` — fixture + metrics for ONE N
- `benchmarks/bench_partition_subxid.sql`  — subtransaction-XID consumption of
  a multi-root flush (the PGPROC_MAX_CACHED_SUBXIDS = 64 cliff)
- `benchmarks/bench_partition_scaling.sh`  — sweeps N, fresh DB per point,
  prints log-log slope + growth + successive-N ratios + ms/N + a noise column
- `benchmarks/build_at_commit.sh`          — attributable per-commit build

Run:

```
./benchmarks/build_at_commit.sh <commit>
PGBIN=/opt/homebrew/opt/postgresql@17/bin \
  ./benchmarks/bench_partition_scaling.sh --label <commit>
./benchmarks/bench_partition_scaling.sh --compare <baseline> <candidate>
```

**`build_at_commit.sh` exists because `cargo pgrx install` silently lied.**
`git archive` stamps extracted files with the commit time, which is older than
the artifact already in `CARGO_TARGET_DIR`; cargo judged the crate fresh and
reinstalled the PREVIOUS commit's `.so` while reporting success. Caught only
because the integration/s1-batch build produced a `.so` byte-identical to the
baseline. Any run made that way would have been attributed to the wrong commit.

## Design decisions worth keeping

- **Total data is held constant while N varies.** Otherwise a rising curve
  cannot be attributed to per-child overhead rather than to more rows.
- **Fresh database per N.** The pending queue, the partition snapshot and the
  IMV registry are cluster state; reusing a database leaks one N into the next.
- **Attach metrics run last.** They add partitions and leave pending rows, so
  they would contaminate the flush metrics, which are the sensitive ones.
- **One warm-up repetition per metric, discarded** (`rep 0` is suppressed in
  `rfx_bench_emit`).
- **Minimum over repetitions** is the headline number; a CV column reports how
  noisy the run was. The machine has other agents compiling on it
  (load average ~6), so the verdicts are deliberately within-run comparisons
  across N, which survive background load.
- `flush_deferred` / `flush_txn` use a **partitioned PASSTHROUGH IMV in
  DEFERRED mode** with a change confined to one leaf. That is the exact shape
  of the 1.11.1 regression (unprunable membership predicate → every leaf
  scanned on every flush), and those two metrics must be FLAT in N.

## Status — complete

- [x] harness written, smoke-tested on homebrew PG17
- [x] sweep on 2f8b786 (main, pre-batch baseline) — `.so` sha `f03056eb23a8b2e8`
- [x] sweep on integration/s1-batch 5f02066 — `.so` sha `ea23909e7b34c36d`
- [x] sweep on fix/swap-flattens-subpartitioned-child 689ab95 — `.so` sha `6169fdf54e7227d2`
- [x] 1.11.1 acceptance check on `b56142a` (= `4e4c825^`, the tree as 1.11.1
      shipped) — `.so` sha `b2645144cb022b13`
- [x] `untreated_bugs/2026-07-25_partition_swap_orphan_probe_quadratic.md`
      updated with the measurement it was missing

Rendered tables for all four builds: `benchmarks/results_partition_scaling_2026-07-28.txt`.
Raw per-rep data: `benchmarks/results_partition_scaling_<label>.tsv`.

## Findings

**No timing regression from the batch.** At every N, `integration/s1-batch` and
the tip are equal to or slightly faster than the 2f8b786 baseline on every
timed metric. None of the suspected per-child additions (the `relkind` probe,
`truncate_partition_tree`, the fresh-partition-OID GUC) is measurable at
N ≤ 200. The GUC membership test is quadratic in principle — an O(N) string
parsed once per child — but the string is ~6 bytes per OID, so at N=200 it is
~1.2 kB and invisible.

**One real regression, already filed elsewhere.** Subtransaction-XID
consumption per multi-root flush doubled:

| build | XIDs per pending root | roots before the 64-subxid overflow |
|---|---:|---:|
| 2f8b786 baseline | 1.04 – 1.26 | ~58 |
| integration/s1-batch | 2.01 – 2.10 | ~31 |
| tip 689ab95 | 2.01 – 2.17 | ~31 |

This independently confirms, on PostgreSQL 17.7, the report filed on the tip
branch as `untreated_bugs/2026-07-28_reconcile_subtransaction_doubles_flush_subxid_consumption.md`
(commit `bb338d1`), which measured the same doubling on pg16 and predicted the
threshold moving from 65 to 33 roots. **Not duplicated here** — one report per
issue. The integrator should fold these numbers into that report.

**Pre-existing, not from this batch:** `reflex_sync_partitions` is quadratic in
N (local slope 1.97 over N=100→200) on all three builds, and every path that
pre-syncs inherits it. Measurement written into the existing report.

**Filed as a product defect:** a full `reflex_reconcile` holds `42N + 38` locks
to end of transaction (baseline `40N + 38`; the batch adds 2 per child) and
dies with `out of shared memory` at N ≈ 490 on a default-configured cluster —
measured by bisection, not extrapolated. `reflex_reconcile_partition` is
`2N + 75` and the COMMIT-time flush is a constant 68, so the automatic path is
not exposed and the partition-scoped primitive is the workaround. See
`untreated_bugs/2026-07-28_full_reconcile_exhausts_max_locks_per_transaction.md`.

## Methodology notes worth keeping

- The headline statistic is the **median**, not the minimum. The 1.11.1
  regression is a shift in the distribution, not in the floor: an unprunable
  predicate still gets a pruning custom plan for the first few executions. At
  N=200 min showed 14.0 vs 4.5 ms while median showed 53.2 vs 4.9 ms.
- A fitted log-log slope alone is not sufficient. A curve flat at small N that
  climbs later fits a low slope (0.59 for that regression). The verdict
  therefore also requires growth at the largest N to stay under 1.6x before
  calling a row FLAT.
- Growth is anchored at the largest N, not at max/min: one noisy mid-sweep
  point otherwise vetoes FLAT on builds that are flat.

Commits under test (build the .so from each, reinstall, re-run):

| ref | sha | what |
|---|---|---|
| main | 2f8b786 | pre-batch baseline |
| integration/s1-batch | 5f02066 | main + the two S1 fixes |
| fix/swap-flattens-subpartitioned-child | 689ab95 | current tip, worktree `.claude/worktrees/agent-af6c0dd061ece2667` |

To measure another commit, export it rather than checking it out, so this
worktree (and the benchmark files, which live only on this branch) stays put:

```
git archive <sha> | tar -x -C /private/tmp/rfxsrc-<label>
cd /private/tmp/rfxsrc-<label>
CARGO_TARGET_DIR=/private/tmp/rfx-bench CARGO_INCREMENTAL=0 \
  cargo pgrx install --pg-config $PGBIN/pg_config --no-default-features --features pg17
```

then run the driver from this worktree with `--label <label>`. The helper
`scratchpad/build_at.sh <sha> <label>` does exactly this and prints the
resulting `.so` sha256, which the driver also records.
