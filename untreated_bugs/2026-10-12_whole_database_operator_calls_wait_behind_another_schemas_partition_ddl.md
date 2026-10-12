# `reflex_audit()`, `reflex_doctor()`, `reflex_ivm_status()` and `reflex_flush_partitions()` without argument wait behind another schema's partition DDL — perf / limitation (operator calls, not the write path)

Found by the cross-schema lock work of 1.12.0 (`p3-c-report.md` §3, PostgreSQL 15 and 18) and
confirmed by its review (`p3-ac-review.md` C §3, PostgreSQL 18), both in the ledger
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`. Pre-existing: the review's
matrix shows the wait on the base tree (`d0a3e06`) and on the fixed one. 1.11.5 (`6946400`) was
not run. Left as is on purpose ("whole database by design"); filed so that the decision is
recorded.

## Reproduction

The sources give no standalone SQL script. What they give:

Review matrix (`p3-ac-review-blocking-matrix.txt`): two schemas `ta` and `tb` with the same
fixture (a LIST source with a RANGE sub-partitioned branch and a DEFAULT leaf, a dimension, a
plain table; a partition-mirroring aggregate and a keyed passthrough, a join aggregate, a two-CTE
chain, an IMV over the mirroring IMV, a MIN/MAX IMV). 57 operations of `tb`, one statement each,
under `lock_timeout = 2.5s`, while a second session holds something in `ta`. Cells are
base/head, `W` waited:

| Operation of `tb` | leaf | DEFAULT leaf | IMV targets | intermediates | chain sub-IMVs | REINDEX | swap + every table | partition DDL open | reconcile / rebuild open |
|---|---|---|---|---|---|---|---|---|---|
| `reflex_audit()`, `reflex_doctor()`, `reflex_ivm_status()` | W/W | W/W | W/W | W/W | W/W | W/W | W/W | W/W | W/W |

Every other operation of `tb` (writes, partition DDL, reconciles, `reflex_audit('<imv>')`,
creates, drops) runs on the head with only relations of `ta` locked. A wait seen on the head:
`waits AccessShareLock on ta.sim_pc1 <… reflex_audit() …>`. IMMEDIATE and DEFERRED give the same
row, with one exception: `reflex_ivm_status()` waits only when an IMV tree is held.

Author's table (every table of the other schema held `ACCESS EXCLUSIVE`):

| Call | Behind another tenant's DDL |
|---|---|
| `reflex_ivm_status()` | waits (lock timeout): it opens every IMV's partition tree (`pg_partition_tree`, `pg_relation_size` or `COUNT(*)` for every IMV, `src/introspect.rs`) |
| `reflex_audit()` without argument | each check on the other tenant's IMVs waits; under `lock_timeout` it reports them as `check-errored` |
| `reflex_audit('<imv>')` | does not wait |
| `reflex_flush_partitions()` without argument | flushes every pending root it can see, so it opens the trees of other tenants' roots left pending by a failed flush |
| `reflex_doctor()`, `reflex_scheduled_reconcile()`, `reflex_heal_ignored_sources()`, `reflex_repair_dependency_graph()`, `reflex_partition_pending_status()` | did not wait in the author's fixture (nothing to do in it for the first two); not analysed further |

## Expected vs actual

Expected by a tenant who monitors its own schema: a monitoring call is not blocked by, and does
not report errors for, another schema's maintenance. Actual: the three no-argument calls wait for
the other schema's locks, and `reflex_audit()` under `lock_timeout` prints `check-errored` for
IMVs that are fine.

## What is established and what is not

Established: the waits above, on the base and on the head. The two tables disagree on
`reflex_doctor()` (waited in the review's matrix, did not wait in the author's fixture); the
fixtures differ and nobody reconciled the two. Not established: a deadlock. The author states
that "a caller that runs them inside a transaction that holds partition locks can still deadlock
with a tenant's swap"; that is read from the lock order, no deadlock was produced.

## What was ruled out

- Not on the write path: the COMMIT-time trigger uses the per-root flush, and no write, partition
  DDL or per-IMV call of `tb` waited on the head.
- `reflex_audit('<imv>')` is the working per-IMV form.

## Fix direction (hypothesis to falsify)

Give the three calls a schema (or name pattern) argument so that a tenant can restrict them, and
make the no-argument audit report a relation it could not lock as "skipped: locked" instead of
`check-errored`. `reflex_ivm_status()` could report sizes from `pg_class` (`reltuples`, `relpages`), which
needs no relation lock, when asked for an estimate. None of this is tried.
