# The default-drain catalog query takes `AccessShareLock` on every partition in the database: two tenants flushing at once deadlock — S2 (flush fails, root suspended after 5)

Found in the field on 1.11.5 (PostgreSQL 17). Two unrelated schemas, each replacing leaves of its
own partitioned source in its own transaction, deadlock each other at COMMIT inside the partition
flush. After 5 consecutive failures the root is suspended and its IMVs stop following the source.

## Mechanism

`src/partition.rs:4193-4197` enumerates the DEFAULT partitions of one root:

```sql
SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS q, t.level AS lvl
  FROM pg_partition_tree($1) t
  JOIN pg_class c ON c.oid = t.relid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'
```

The predicate only references `pg_class`, so the planner is free to evaluate it on the `pg_class`
scan before the join with `pg_partition_tree`. `pg_get_expr(expr, relid)` opens `relid` with
`AccessShareLock`. Evaluated on the scan, that is a lock on every relation with a partition bound
in the whole database, not on the members of `$1`'s tree. A leaf that another transaction has just
ATTACHed or is about to DROP is held `AccessExclusive` until that transaction ends, so the query
blocks on it.

Two flushes that each hold their own swapped leaves and each run this query wait on each other.

## Evidence (server log, 2026-10-12)

Session A (pid 3741312, schema `omc`) and session B (pid 3738156, schema `yse`), both in `COMMIT`,
both inside `__reflex_partition_flush_fn`, both on the statement above:

```
06:51:37.283 [B] still waiting for AccessShareLock on relation 18638310 … holding the lock: A
06:52:08.863 [A] detected deadlock while waiting for AccessShareLock on relation 9522627
                 DETAIL: Process holding the lock: B.
                 CONTEXT: SQL statement "SELECT quote_ident(n.nspname) … = 'DEFAULT'"
                          … reflex_reconcile_partition('omc.forecast_analysis_view__cte_forecast_bounds', …)
06:52:08.900 [A] WARNING: partition flush for root omc.sales_simulation failed: deadlock detected (SQLSTATE 40P01)
06:52:50.674 [B] acquired AccessShareLock on relation 18638310 after 74390.898 ms
```

`9522627::regclass` = `yse.sales_simulation_p_172`: session A, flushing an `omc` root, waits on a
`yse` relation that no `omc` partition tree contains. A detected the deadlock 9 times in 47 s
(every nested auto-sync and every retry of the flush hits the same lock), its flush is logged as
failed at least 4 times in that one COMMIT, and B waited 74 s for A to finish.

`__reflex_partition_pending` afterwards: `omc.sales_simulation`, attempts 31, failures 5,
`last_error = 'deadlock detected'` — suspended. The two tenants' jobs start a minute apart every
morning, so the collision repeats.

The same log has the pattern on 2026-10-08 14:26 (11 detections in one COMMIT).

## Ruled out

- Client-side ordering: the application serialises its own swaps per schema with an advisory lock
  held to the end of the transaction. It cannot help: the two sessions are in different schemas
  and the conflicting locks are taken by this query, not by the application's DDL.
- A shared relation: the contested relations are each tenant's own source leaves.

## Not established

- The plan actually chosen (no `EXPLAIN` captured); the cross-schema lock is inferred from the
  relation waited on. `18638310` was not resolved (likely a dropped `omc` leaf).
- That `pg_get_expr` takes the lock is from memory of `ruleutils.c` (`try_relation_open(relid,
  AccessShareLock)`), not re-read in the PG 17 source for this report.
- Whether the other six `pg_get_expr(relpartbound …)` call sites in `src/partition.rs` have a
  predicate the planner can evaluate outside the tree in the same way.

## Reproduction sketch

Two schemas, each with a LIST- or RANGE-partitioned source and a partitioned IMV with a DEFAULT
child. Session 1: `BEGIN`, DETACH/ATTACH a leaf of schema 1, hold. Session 2: the same in schema 2,
then `SELECT reflex_flush_partition_source('<schema 2 root>')` — it blocks on schema 1's leaf.
Session 1 then flushes its own root: deadlock.

## Fix direction

Restrict the lock to the tree: make the `pg_get_expr` call depend on `pg_partition_tree`'s output so
it cannot run on the bare `pg_class` scan (e.g. a `MATERIALIZED` CTE over the tree, then the
expression in the outer select), or test the bound without opening the relation
(`pg_inherits` + `c.relpartbound` node text). Audit the other `pg_get_expr(relpartbound …)` sites
the same way. A regression test can assert `pg_locks` holds no lock outside the root's schema after
the call.
