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

Disk on `/private/tmp` is tight (was 3.4 GB free at start). Other agents own
`/private/tmp/rfx-dep` and `/private/tmp/rfx-dl` — do not delete them.

## Deliverables

- `benchmarks/bench_partition_scaling.sql` — fixture + metrics for ONE N
- `benchmarks/bench_partition_subxid.sql`  — subtransaction-XID consumption of
  a multi-root flush (the PGPROC_MAX_CACHED_SUBXIDS = 64 cliff)
- `benchmarks/bench_partition_scaling.sh`  — sweeps N, fresh DB per point,
  prints log-log slope + successive-N ratios + ms/N + a CV noise column

Run:

```
PGBIN=/opt/homebrew/opt/postgresql@17/bin \
  ./benchmarks/bench_partition_scaling.sh --label $(git rev-parse --short HEAD)
```

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

## Status

- [x] harness written and smoke-tested on homebrew PG17
- [ ] sweep on 2f8b786 (main, pre-batch baseline)
- [ ] sweep on integration/s1-batch
- [ ] sweep on fix/swap-flattens-subpartitioned-child (689ab95)
- [ ] 1.11.1 acceptance check (revert the 1.11.2 gate, confirm the benchmark
      turns the flush metrics from FLAT to linear)
- [ ] verdicts + any regression report in untreated_bugs/

Commits under test (build the .so from each, reinstall, re-run):

| ref | sha | what |
|---|---|---|
| main | 2f8b786 | pre-batch baseline |
| integration/s1-batch | 5f02066 | main + the two S1 fixes |
| fix/swap-flattens-subpartitioned-child | 689ab95 | current tip, worktree `.claude/worktrees/agent-af6c0dd061ece2667` |

To measure another commit, `git checkout <sha>` in this worktree, rebuild,
reinstall, and re-run the driver with `--label <sha>`. The benchmark files
live only on this branch, so copy them to `/private/tmp` before checking out a
different commit, or run them by absolute path from a copy.
