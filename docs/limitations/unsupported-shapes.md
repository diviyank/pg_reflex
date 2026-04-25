# Unsupported shapes

The query patterns below are **not** maintainable as IMVs. Use plain `MATERIALIZED VIEW` for these; the journal entry [`2026-04-22_unsupported_views.md`](https://github.com/diviyank/pg_reflex/blob/main/journal/2026-04-22_unsupported_views.md) tracks specific examples from a real production deployment.

## DISTINCT ON / ROW_NUMBER top-1 picks

`DISTINCT ON (...) ORDER BY ...` and `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) WHERE rn = 1` are *top-1-per-group* selections. On DELETE of the current top, the engine must scan the remaining rows to find the new top — same problem as MIN/MAX retraction, but with an extra ordering constraint.

**Status**: pg_reflex supports `DISTINCT ON` at IMV creation time as a passthrough sub-IMV + read-time VIEW (1.1.1+), but the winner is recomputed on every read. **Future**: top-K with a sort key would unblock this.

## Scalar aggregate without GROUP BY

`SELECT MAX(date) FROM orders` — single-row aggregate, no group key.

**Status**: ✅ Supported (sentinel-row path, 1.0.x+). With `topk=K` (1.3.0), retraction becomes `O(K)`.

## LIMIT 1 / ORDER BY ... LIMIT 1

`SELECT * FROM events ORDER BY ts DESC LIMIT 1` — picks one specific row out of many; arbitrary deletes can change the winner.

**Status**: not supported. Use a `MATERIALIZED VIEW` and refresh on a schedule.

## FULL OUTER JOIN

For `FULL JOIN`, every insert/delete on either side can create or destroy rows on **both** sides of the output (matched ↔ NULL-extended transitions). The current delta logic only reasons about one source at a time.

**Status**: not supported. **Unlock**: generalised delta computation that tracks MATCH ↔ NULL transitions on both sides simultaneously.

## Window functions in SELECT (no GROUP BY)

`SUM(x) OVER (PARTITION BY g)` over a passthrough — a new row can change the partition total for unrelated rows, so the affected set is the entire partition.

**Status**: not supported as a passthrough. Window-on-aggregate (with GROUP BY) is supported because the window applies to the small group-summary table.

## MIN/MAX/BOOL_OR on wide fact tables (without `topk`)

Technically supported, but full-source recompute on retraction makes it operationally unattractive on tables with millions of rows.

**Status**: ✅ With `topk=K` (1.3.0), this becomes `O(K)` per affected group. The 3 stock_chart-style views from `2026-04-22_unsupported_views.md §6` become eligible after opting in.

## UNION ALL inside a CTE

```sql
-- Doesn't decompose:
WITH x AS (SELECT a FROM t1 UNION ALL SELECT a FROM t2)
SELECT a, COUNT(*) FROM x GROUP BY a;
```

The decomposer doesn't recurse into CTE bodies. Workaround: lift the set operation to the top level.

```sql
-- Decomposes:
SELECT a, COUNT(*) FROM (
    SELECT a FROM t1 UNION ALL SELECT a FROM t2
) AS x GROUP BY a;
```

## Passthrough over non-IMV matviews

A passthrough IMV needs trigger coverage on every dependency. When an upstream dependency is a plain `MATERIALIZED VIEW`, it has no DML triggers — pg_reflex never sees deltas on it.

**Status**: not supported. **Workaround**: convert the upstream matview to an IMV, or use `refresh_imv_depending_on` after refreshing the matview.

## ARRAY_AGG / JSON_AGG / catalog-driven views

`ARRAY_AGG` is not algebraically maintainable without storing intermediate arrays keyed by group. `ARRAY_AGG ORDER BY` adds the same top-K problem as `DISTINCT ON`. Schema-introspection views read from `information_schema` / `pg_catalog`, which has no delta signal.

**Status**: not supported. Use `MATERIALIZED VIEW`.

## Non-deterministic functions in WHERE

`WHERE date > NOW()` is rejected — the predicate's truth value changes over time without a corresponding source mutation.

**Workaround**: see [SQL reference / clauses](../sql-reference/clauses.md#deterministic-functions-only-in-where).

## Subqueries with aggregation in FROM

```sql
SELECT ... FROM (SELECT SUM(x) FROM t GROUP BY y) AS sub
```

Rejected at creation time — the trigger's source-replacement logic produces wrong results when the inner SELECT is itself an aggregate.

**Workaround**: rewrite as a CTE, which pg_reflex decomposes into a sub-IMV automatically.
