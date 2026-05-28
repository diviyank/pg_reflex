# Unsupported shapes

This page sorts query patterns into three buckets:

1. **Hard-rejected at IMV creation** — `create_reflex_ivm` returns an `ERROR:` and no triggers/tables are created. The patterns are structurally incompatible with incremental maintenance.
2. **Supported with a fallback path** — the IMV is created and stays correct, but one or more operations bypass the algebraic delta and fall back to a scoped re-aggregation, full refresh, or read-time computation. Operationally these are slower than the hot algebraic path; functionally they're correct.
3. **Operator-side workarounds** — patterns that look unsupported but have a straightforward rewrite that lands on a supported shape.

The journal entry [`2026-04-22_unsupported_views.md`](https://github.com/diviyank/pg_reflex/blob/main/journal/2026-04-22_unsupported_views.md) (1.1.2 era) catalogues a real production deployment's matview inventory. Several of those have since become eligible — auto-on top-K closes the MIN/MAX retraction cliff, FULL OUTER JOIN now flows through the targeted-reconcile fallback, and window functions over passthrough decompose to a sub-IMV plus read-time VIEW. The historical journal is still useful for shape-class analysis, but treat its "supported" verdicts as a snapshot, not the current truth.

---

## 1. Hard-rejected at IMV creation

### `WITH RECURSIVE`

`WITH RECURSIVE` is a fixpoint computation. The fixpoint depends on the entire current relation, not on a delta — there is no incremental update path that doesn't re-run the whole recursion.

**Status**: rejected. Use `MATERIALIZED VIEW`.

### `LIMIT` and `ORDER BY`

`LIMIT` selects a specific tail of an arbitrary order, which mutates unpredictably under inserts. Bare `ORDER BY` (without `DISTINCT ON`) imposes a row order on a relation that has none.

**Status**: rejected. The `DISTINCT ON ... ORDER BY` shape *is* supported (see §2 below) because the ORDER BY scopes a per-group selection rather than a global row order.

### Window functions nested in a subquery or derived table

```sql
SELECT * FROM (
    SELECT id, amount,
           ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
    FROM orders
) s WHERE s.rn = 1;
```

Window functions cannot be decomposed when nested inside a subquery, derived table, or CTE body. The supported pattern is to apply the window in the outermost SELECT.

**Status**: rejected at create time. Workaround: rewrite with the window function in the top-level SELECT, or use `kind: mv` with manual `REFRESH`.

### `LATERAL` joins

`LATERAL` lets the right side of a join reference the left side row-by-row. Delta semantics across LATERAL would require re-evaluating the right-hand-side per left delta row — algebraically tractable for some shapes, but the engine doesn't have the rewrite logic.

**Status**: rejected.

### `GROUPING SETS` / `CUBE` / `ROLLUP`

Each grouping set is morally a separate IMV. Maintaining them collectively in one query would require shared delta processing across N grouping levels.

**Status**: rejected. Workaround: define one IMV per grouping level and `UNION ALL` them at the application boundary.

### `TABLESAMPLE`

Non-deterministic — the same query against the same relation returns different rows on different runs. There is no stable "current state" to maintain incrementally.

**Status**: rejected.

### `WITHIN GROUP` (ordered-set aggregates)

`PERCENTILE_DISC`, `MODE`, etc. require the entire group's sorted distribution — not algebraically maintainable without storing intermediate sorted state per group.

**Status**: rejected.

### Window functions or `DISTINCT ON` inside a referenced CTE

```sql
WITH ranked_data AS (
    SELECT id, amount,
           ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
    FROM orders
)
SELECT id, amount FROM ranked_data WHERE rn = 1;
```

A CTE with a window function or `DISTINCT ON` materializes as a read-time VIEW. An IMV cannot install row-level triggers on a VIEW, so the outer query cannot be incrementally maintained.

**Status**: rejected at create time. Workaround: move the window function or `DISTINCT ON` to the outermost `SELECT`, or define the CTE as a plain `MATERIALIZED VIEW` instead.

### `UNION` / `INTERSECT` / `EXCEPT` (without `ALL`) as a CTE body

```sql
WITH dedup AS (
    SELECT x FROM a
    UNION                  -- or INTERSECT, or EXCEPT
    SELECT x FROM b
)
SELECT x, COUNT(*) FROM dedup GROUP BY x;
```

The deduplicating set operations depend on the entire input multiset, not on a per-operand delta, so they cannot be maintained incrementally as an intermediate sub-IMV. Pre-1.7.0 these silently emitted a VIEW that then failed deep in the consumer's trigger install with `Triggers on views cannot have transition tables`; 1.7.0 rejects them at create time with a clear error.

**Status**: rejected at create time (1.7.0+). Workarounds:
1. Hoist the set operation to the outermost `SELECT` so it stays a VIEW that PostgreSQL evaluates at query time.
2. Define the view with `kind: mv` (plain `MATERIALIZED VIEW`).
3. Rewrite as `UNION ALL` if the operands are guaranteed to produce disjoint rows — the `UNION ALL` form *is* supported as a CTE body (see §2).

### `GROUP BY` key not projected bare in the `SELECT`

```sql
-- rejected: the grouped key `g` reaches the SELECT only wrapped in COALESCE,
-- never as a bare column, so the generated end query cannot resolve it.
SELECT COALESCE(g, 0) AS g0, SUM(v) AS s FROM t GROUP BY g;
```

Each `GROUP BY` key must appear as a bare column somewhere in the `SELECT` list so maintenance can join the delta back to the materialized group. A key that surfaces only inside an expression (`COALESCE(g, 0)`, `g + 1`, a `CASE`, a cast) leaves the end query with no column to match on. Since 1.6.4 this is rejected up front with a clear error instead of failing later in codegen.

**Status**: rejected at create time. Workaround: also project the key bare — `SELECT g, COALESCE(g, 0) AS g0, SUM(v) AS s FROM t GROUP BY g` — and derive the expression in the consuming query, or use `kind: mv`.

### `ARRAY_AGG` / `JSON_AGG` / `STRING_AGG`

Order-sensitive aggregates that depend on the entire group. Maintaining `ARRAY_AGG ORDER BY` brings in the same problem as `DISTINCT ON` retraction; without ordering, the array contents would still need full membership tracking.

**Status**: rejected.

### Non-deterministic functions

`NOW()`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `CURRENT_TIME`, `CLOCK_TIMESTAMP()`, `STATEMENT_TIMESTAMP()`, `TIMEOFDAY()`, `RANDOM()`, `GEN_RANDOM_UUID()` are rejected anywhere in the query — `SELECT`, `WHERE`, `HAVING`, `JOIN ON`, anywhere `pre_visit_expr` reaches. They would cause the IMV to drift over time (or row-by-row, for `RANDOM`) without a corresponding source mutation, and the algebraic delta engine has nothing to react to.

**Status**: rejected at create time. The analyzer flag is named `has_nondeterministic_select` for historical reasons; the rejection is query-wide.

**Workaround**: pin the value at IMV-creation time (`WHERE date > '2026-01-01'`), or pre-compute the cutoff in the application and pass it as a parameter via a CTE that the engine treats as a static input.

### Subqueries with aggregation in `FROM`

```sql
SELECT … FROM (SELECT SUM(x) FROM t GROUP BY y) AS sub
```

The trigger's source-replacement logic substitutes the inner table with the transition table — which means the inner aggregation only sees delta rows, producing wrong results.

**Status**: rejected at create time. Workaround: rewrite as a CTE, which the engine decomposes into a sub-IMV automatically.

---

## 2. Supported with a fallback path

These shapes work correctly. The cost shape differs from the algebraic delta hot path — usually a scoped recompute or a read-time computation — and is documented per case.

### Scalar aggregate (no `GROUP BY`)

```sql
SELECT MAX(date) FROM orders
```

**Status**: ✅ Supported via the sentinel-row path (1.0.x+). Top-K is auto-enabled (K=16) on the MIN/MAX intermediate, so retraction is `O(K)` per call. Append-only workloads can opt out with `topk = 0` if INSERT overhead is a concern.

### `DISTINCT ON` and `ROW_NUMBER` top-1 picks

```sql
SELECT DISTINCT ON (customer_id) customer_id, ts, amount
FROM orders ORDER BY customer_id, ts DESC

-- equivalent
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ts DESC) AS rn
  FROM orders
) WHERE rn = 1
```

**Status**: ✅ Decomposed at IMV creation time (1.1.1+) into a passthrough sub-IMV (incrementally maintained) plus a read-time `VIEW` that applies the `ROW_NUMBER ... WHERE rn = 1` filter. The sub-IMV stays incremental; the per-group winner is recomputed on every read.

**Cost shape**: read latency scales with the sub-IMV size, not the original source. Suitable for tens-of-thousands of groups; for orders-of-magnitude larger sets, consider a `MATERIALIZED VIEW` of the top-1 result.

### Window functions in `SELECT`

```sql
SELECT dept, amount, SUM(amount) OVER (PARTITION BY dept) AS dept_total
FROM employees
```

**Status**: ✅ Decomposed via `window::decompose_window_query`: the base query (without window functions) becomes a sub-IMV — passthrough or aggregate — and the window functions are applied at read time via a `VIEW`. Works for `SUM/COUNT/AVG OVER (PARTITION BY …)`, `RANK`, `DENSE_RANK`, `ROW_NUMBER`, `LAG`, `LEAD`.

**Cost shape**: read latency scales with the sub-IMV size. The window computation itself is fast because PostgreSQL evaluates it over the (typically small) sub-IMV result, not the original source.

### `FULL OUTER JOIN`

`FULL JOIN` produces rows on both sides of the output — matched rows plus NULL-extended rows from each side. The algebraic delta tracks one source at a time, so on every INSERT/DELETE/UPDATE on either side, the engine cannot determine the matched-↔-unmatched transitions for the other side from delta alone.

**Status**: ✅ Supported via fallback. Aggregate `FULL JOIN` IMVs route through the targeted-reconcile path (extract affected groups from the delta, re-aggregate those groups from the full source). Passthrough `FULL JOIN` IMVs fall back to a full `DELETE + INSERT` from `base_query` on retract.

**Cost shape**: every retraction (DELETE/UPDATE on the secondary side) re-aggregates the affected groups from the full source — same cost as the 1.2.0 scoped-recompute path. INSERTs on the primary side use the algebraic delta as normal.

**What would unlock the algebraic path**: generalised delta computation that tracks `MATCH ↔ NULL` transitions on both sides simultaneously. Out of scope for the current release.

### MIN/MAX retraction on wide fact tables

Historical concern from the 1.1.2 audit (R3): MIN/MAX retraction without top-K required a full-source scan per affected group, making large-source IMVs operationally unusable.

**Status**: ✅ Closed. Top-K is auto-enabled (K=16) for every MIN/MAX intermediate column as of 2026-04-26. Retraction is `O(K)` for groups whose heap has surviving elements; `O(group_size)` only for the small fraction of groups whose heap empties (handled by the existing scoped recompute). Append-only operators who want to avoid the heap-maintenance INSERT cost can opt out via `topk = 0`.

The 1.3.0 partial-heap UPDATE-staleness gap was fixed in 1.4.0 — UPDATEs on top-K MIN/MAX IMVs now force a scoped source-scan recompute for affected groups (correctness guarantee), gated by the heap-shrinkage capture table so groups whose heap stayed at K skip the scan.

---

### `UNION ALL` inside a CTE

```sql
WITH x AS (SELECT a FROM t1 UNION ALL SELECT a FROM t2)
SELECT a, COUNT(*) FROM x GROUP BY a;
```

**Status**: ✅ Supported (1.6.5+ via the intermediate-table wrapper path; 1.7.0+ adds a `__reflex_src_idx` discriminator). Each operand of the `UNION ALL` becomes its own sub-IMV (`<view>__union_<i>`), and the CTE wrapper itself materializes as an UNLOGGED TABLE populated per-operand and maintained by per-operand mirror triggers that propagate INSERT / DELETE / UPDATE 1:1 into the wrapper.

**Cost shape**: the wrapper carries one extra `SMALLINT` (`__reflex_src_idx`) column per row and the per-operand DELETE predicate adds one extra equality. Otherwise identical to the algebraic delta cost of the operand IMVs themselves.

**Caveat — intra-operand duplicate over-delete** ([known issue](known-issues.md#intra-operand-duplicate-over-delete-in-union-all-cte-wrappers)). If an operand projects three rows whose every column value is identical and then deletes one of them, the wrapper's mirror DELETE removes all three because the `IS NOT DISTINCT FROM` match predicate matches all three. The `__reflex_src_idx` discriminator fixes cross-operand collisions but does not address intra-operand duplicates. Workaround: include a primary-key / unique column in each operand's projection.

---

## 3. Operator-side workarounds

### Passthrough over a non-IMV `MATERIALIZED VIEW`

A passthrough IMV needs trigger coverage on every dependency. A plain `MATERIALIZED VIEW` is a snapshot — it has no DML and emits no triggers, so pg_reflex never sees deltas on it.

**Workaround**: convert the upstream matview to an IMV, or call `refresh_imv_depending_on('<matview>')` after every `REFRESH MATERIALIZED VIEW` to cascade an explicit reconcile through the chain.
