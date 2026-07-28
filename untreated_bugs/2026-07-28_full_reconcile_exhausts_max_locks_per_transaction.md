# 2026-07-28 — a full `reflex_reconcile` of a partitioned IMV exhausts the lock table and dies with `out of shared memory`

**Status: untreated. Severity: medium.** No wrong results — the reconcile fails and rolls
back cleanly. What makes it worth filing is that a supported operation on a supported shape
fails outright on a **default-configured PostgreSQL**, and the error the operator sees names
neither pg_reflex nor the setting that would fix it.

Found while building `benchmarks/bench_partition_scaling.sh`, not from a field report.

## The measurement

`reflex_reconcile` on a LIST-partitioned aggregate IMV holds, to end of transaction:

| build | locks held by one full reconcile |
|---|---|
| `2f8b786` (main) | **40N + 38** |
| `integration/s1-batch` `5f02066` and tip `689ab95` | **42N + 38** |

**Measured, not inferred.** Exact fit on five points each (N = 10, 25, 50, 100, 200), no
residual:

| N | 10 | 25 | 50 | 100 | 200 |
|---|---:|---:|---:|---:|---:|
| main | 438 | 1038 | 2038 | 4038 | 8038 |
| batch | 458 | 1088 | 2138 | 4238 | 8438 |

Counted as `SELECT count(*) FROM pg_locks WHERE pid = pg_backend_pid()` immediately after the
reconcile inside a transaction that is then rolled back — locks are held to end of
transaction, so this is the exact peak footprint. It is a pure count, unaffected by machine
load, and it reproduced identically across every run.

The batch adds 2 locks per child. That is linear and not itself a defect; it is recorded so
the coefficient is attributable.

## Where it fails on the default setting

Measured by bisection on PostgreSQL 17.7 (homebrew), `max_locks_per_transaction = 64`
(the default), `max_connections = 100`, on an **otherwise idle** cluster, one client
connection, one `reflex_reconcile` per transaction:

| build | largest N that succeeded | smallest N that failed |
|---|---:|---:|
| main `40N + 38` | **490** (19 638 locks) | **493** (19 758 locks) |
| batch `42N + 38` | **490** (20 618 locks) | **500** (21 038 locks) |

So the practical ceiling is **N ≈ 490** on an idle default-configured cluster.

Two caveats on that number, both important to anyone sizing a cluster:

- **It is not a crisp threshold.** `max_locks_per_transaction * (max_connections +
  max_prepared_transactions)` = 6400 entries is the *nominal* size; the lock table is
  allowed to grow into shared-memory slack, so a lone backend actually reached ~19.6k–20.6k
  entries. main failed at 19 758 while the batch build succeeded at 20 618 — the exact
  ceiling moves between runs.
- **It shrinks with concurrency.** That slack is shared. The measurement above is the
  best case: one idle cluster, one backend. On a cluster where other backends are holding
  locks, the same reconcile fails at a substantially lower N, and non-deterministically. A
  reconcile that works in staging can fail in production at the same N.

### What the operator sees

```
ERROR:  out of shared memory
HINT:  You might need to increase "max_locks_per_transaction".
```

The `HINT` is PostgreSQL's generic one and happens to name the right knob, but the `ERROR`
line names neither pg_reflex, nor the IMV, nor the partition count, nor the fact that the
footprint is a linear function of N that the operator could have projected. Nothing connects
"my nightly reconcile started failing" to "my source table crossed ~490 partitions". The
failure also arrives only after the reconcile has done most of its work.

## Correction to an earlier claim

An earlier note (in `HANDOFF.md` and the benchmark header) said this failed "between N=100
and N=200". That was **wrong**, and the mechanism is worth recording because it is a trap
for anyone re-measuring: the benchmark's `reconcile_full` metric runs its repetitions inside
**one** `DO` block, i.e. one transaction. Each repetition drops and recreates the
`__reflex_swap_*` tables, so each gets fresh OIDs and fresh lock entries, and four
repetitions accumulate roughly 4x the single-reconcile footprint. N=200 x 4 reps ≈ 32k locks
is what exhausted the table, not one reconcile at N=200. A single reconcile at N=200 holds
8038 locks and succeeds on the default setting.

`max_locks_per_transaction >= 2048` therefore remains a genuine prerequisite for running the
benchmark, but for a different reason than first stated.

## Not the same defect as the quadratic probe

`untreated_bugs/2026-07-25_partition_swap_orphan_probe_quadratic.md` covers a *quadratic*
time cost in `reflex_sync_partitions`. This report is a *linear* lock cost in
`reflex_reconcile`. They are independent and need different fixes:

- the lock footprint is **exactly linear** (`40N + 38` / `42N + 38`, zero residual on five
  points), so the reconcile touches a linear number of objects;
- the *time* of the sync it calls is quadratic while its lock count stays at `2N + 25`.

Fixing the probe would not reduce the lock footprint by one entry, and batching the swap set
would not make the sync any less quadratic.

## Exposure of the other partition entry points — the operator's workaround

Measured the same way, on the same builds:

| primitive | locks held | shape |
|---|---|---|
| `reflex_reconcile` (full) | `42N + 38` | linear in N — **this defect** |
| `reflex_reconcile_partition` (one leaf) | `2N + 75` | linear, but **20x smaller** |
| `reflex_sync_partitions` | `2N + 25` | linear, small |
| COMMIT-time flush of a routine change (`reflex_flush_deferred`) | **68, constant** | independent of N |

**The COMMIT-time flush is not exposed at all** — 68 locks at N=10 and 68 at N=200. The
automatic maintenance path a production cluster runs continuously is safe regardless of
partition count.

**`reflex_reconcile_partition` is the workaround.** At `2N + 75` it would need N ≈ 9800
partitions to reach the same ceiling. An operator hitting this defect can reconcile
partition-by-partition instead of calling the full reconcile, at the cost of the O(tree)
pre-sync per call (which is where the separate quadratic-probe defect bites).

## Fix direction

Not attempted here. Two options, roughly in increasing invasiveness:

1. **Refuse loudly instead of dying obscurely.** Before starting, project the footprint
   (`42 * child_count + 38`) against
   `current_setting('max_locks_per_transaction')::int * (current_setting('max_connections')::int + current_setting('max_prepared_transactions')::int)`
   and, if it does not fit with margin, refuse with a message naming the IMV, the child
   count, the projected footprint, the configured limit, and
   `reflex_reconcile_partition` as the partition-scoped alternative. This is the
   "refuse loudly, never fail obscurely" principle and is cheap. It does not raise the
   ceiling, and the projection must be conservative in the safe direction (refuse when in
   doubt) — but note a refusal that fires when the operation would in fact have succeeded is
   itself a regression, so the margin has to be chosen against the *nominal* table size
   rather than against the observed slack.
2. **Batch the swap set.** Commit the swap in chunks of children rather than holding every
   child's locks for the whole reconcile. This genuinely removes the ceiling, but it
   trades away the all-or-nothing atomicity of the current reconcile — a failure midway
   would leave some partitions swapped and some not. Given this package's correctness bias
   that trade needs its own design discussion; it is not obviously worth it when option 1
   plus `reflex_reconcile_partition` already gives operators a working path.

Whichever is chosen, pin it with a test that asserts the lock footprint is what the
projection claims — `SELECT count(*) FROM pg_locks WHERE pid = pg_backend_pid()` inside a
transaction is deterministic and needs no new infrastructure.

## Reproduction

```
psql -d probe -c "CREATE EXTENSION pg_reflex"
# build a LIST-partitioned source with N leaves + a partitioned aggregate IMV over it, then:
BEGIN;
SELECT reflex_reconcile('the_imv');
SELECT count(*) FROM pg_locks WHERE pid = pg_backend_pid();
ROLLBACK;
```

With `max_locks_per_transaction = 64` and `max_connections = 100` on an idle cluster this
succeeds at N = 490 and raises `out of shared memory` at N = 500.
