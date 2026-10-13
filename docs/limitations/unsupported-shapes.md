# Unsupported shapes

This page sorts query patterns into three buckets:

1. **Hard-rejected at IMV creation** — `create_reflex_ivm` returns an `ERROR:` and no triggers/tables are created. The patterns are structurally incompatible with incremental maintenance.
2. **Supported with a fallback path** — the IMV is created and stays correct, but one or more operations bypass the algebraic delta and fall back to a scoped re-aggregation, full refresh, or read-time computation. Operationally these are slower than the hot algebraic path; functionally they're correct.
3. **Operator-side workarounds** — patterns that look unsupported but have a straightforward rewrite that lands on a supported shape.

The journal entry [`2026-04-22_unsupported_views.md`](https://github.com/diviyank/pg_reflex/blob/main/journal/2026-04-22_unsupported_views.md) (1.1.2 era) catalogues a real production deployment's matview inventory. Several of those have since become eligible — auto-on top-K closes the MIN/MAX retraction cliff, FULL OUTER JOIN now flows through a full-rebuild fallback, and window functions over passthrough decompose to a sub-IMV plus read-time VIEW. The historical journal is still useful for shape-class analysis, but treat its "supported" verdicts as a snapshot, not the current truth.

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

Rejected anywhere in the query (`SELECT`, `WHERE`, `HAVING`, `JOIN ON`, `FILTER`, subqueries, CTEs), in three tiers:

- **Clock and session functions, by name**: `NOW()`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `CURRENT_TIME`, `LOCALTIME`, `LOCALTIMESTAMP`, `CLOCK_TIMESTAMP()`, `STATEMENT_TIMESTAMP()`, `TRANSACTION_TIMESTAMP()`, `TIMEOFDAY()`, `RANDOM()`, `GEN_RANDOM_UUID()`, one-argument `age(x)`, `CURRENT_USER`, `SESSION_USER`, `USER`, `CURRENT_ROLE`, `CURRENT_SCHEMA`, `CURRENT_CATALOG`, `SYSTEM_USER`, `current_setting()`, `current_database()`, `current_schemas()`, `pg_backend_pid()`, `inet_client_addr()` / `inet_client_port()` / `inet_server_addr()` / `inet_server_port()`, `txid_current()`, `txid_current_if_assigned()`, `txid_current_snapshot()`, `pg_current_xact_id()`, `pg_current_xact_id_if_assigned()`, `pg_current_snapshot()`, `pg_postmaster_start_time()`, `pg_conf_load_time()`, `pg_my_temp_schema()`, `pg_trigger_depth()`. The list applies to an unqualified call and to one qualified with `pg_catalog`, whatever its case or quoting. A function of the same name in another schema is judged by the second tier.
- **Any function PostgreSQL labels `VOLATILE`**, read from `pg_proc`. `CREATE FUNCTION` labels a function `VOLATILE` unless told otherwise, so a deterministic function without a label is rejected too: the message names it and prints the `ALTER FUNCTION … IMMUTABLE` statement to run. An unqualified call is matched by name in every schema and for every overload; a schema-qualified call in that schema only.
- **A moving date/time literal**: a string holding `now`, `today`, `tomorrow` or `yesterday` given a date/time type, as `'now'::timestamptz` or `DATE 'today'`.

They would make the IMV drift without a source mutation, and the delta engine has nothing to react to.

**Not detected**: a moving value that is not a typed literal, such as `WHERE ts < 'now'` or `daterange('2024-01-01', 'today')`, and a session-dependent function that is neither on the list nor labelled `VOLATILE`.

**Accepted with a WARNING**: two-argument `date_trunc`, `date_part`, `EXTRACT`, `to_char`, `to_date`, two-argument `to_timestamp` and one-argument `date`. On `timestamptz` their result depends on the writer's `TimeZone`; writers with different settings make the IMV diverge. `date_trunc('day', ts, 'UTC')` clears the warning.

**Status**: rejected at create time.

**Workaround**: pin the value at IMV-creation time (`WHERE date > '2026-01-01'`), keep the bound in a table the query reads (see [Clauses](../sql-reference/clauses.md#deterministic-functions-only)), compute the value into a source column, or apply it in the query that reads the IMV. `age(TIMESTAMPTZ '2025-01-01', created_at)`, with a fixed reference, is accepted.

### A function in `FROM`

`FROM t, unnest(arr) AS e`, `FROM generate_series(1, 10) g`: an IMV is maintained from the rows written to its source tables, and a function is not one.

**Status**: rejected at create time. Workaround: store the function's rows in a table and join it. A function of the row's own columns can be called in the SELECT list (`SELECT g, unnest(arr) AS e FROM t`); such a passthrough has no key and is refreshed in full by every write.

### A function in attribute notation

`c.display_name` for a function `display_name(customers)` reads the whole row of the table.

**Status**: rejected at create time. Workaround: store the value in a column of the source (for an immutable function, a generated column) and select that column.

### DISTINCT aggregates other than a top-level `COUNT(DISTINCT)`

Rejected at create time:

- `SUM(DISTINCT x)`, `AVG(DISTINCT x)`, `MIN(DISTINCT x)`, `MAX(DISTINCT x)`, `BOOL_OR(DISTINCT x)`, under any spacing or comment. Workaround: `WITH d AS (SELECT DISTINCT grp, val FROM t) SELECT grp, SUM(val) FROM d GROUP BY grp`.
- `COUNT(DISTINCT x)` inside an expression (`COUNT(DISTINCT x) + 1`, `COALESCE(COUNT(DISTINCT x), 0)`). Workaround: compute it in a CTE and apply the expression in the outer `SELECT`. A bare cast, `COUNT(DISTINCT x)::int`, is accepted.
- More than one `COUNT(DISTINCT …)` in a SELECT list. Workaround: one CTE per distinct count, joined on the group columns with `IS NOT DISTINCT FROM`; the message prints the query.
- `HAVING COUNT(DISTINCT x)` when the SELECT list does not hold the same `COUNT(DISTINCT x)`, with the same argument and `FILTER`. Workaround: the two-CTE form the message prints, shown in [Upgrading to 1.12.0](../getting-started/upgrading.md#definitions-that-are-now-refused).
- `COUNT(DISTINCT x)` filtered by a HAVING clause that holds a subquery, or under `BETWEEN` / `IN` / `IS` with a `FILTER` or a non-column argument. Workaround: compute it in a CTE and filter in the outer query.

### Other aggregates

An aggregate other than `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, `BOOL_OR` (built-in, extension or user-defined) is rejected. One shape is accepted: a single-level query with no `GROUP BY` and no unique key, which is recomputed from its query on every write. Passing `unique_columns`, or naming an output column like a primary key column of the source, gives that IMV a key and it is rejected.

**Status**: rejected at create time, with that exception.

### `SUM` / `AVG` over `REAL`, `money`, intervals and times

`SUM` / `AVG` over a `REAL` (single precision) column or expression is rejected: PostgreSQL adds `REAL` values in single precision in row order, a result that a sum maintained by adding and subtracting deltas does not reproduce. `SUM` over `money`, an interval or a time is rejected as well: pg_reflex maintains integer, numeric and double precision sums only. `MIN` / `MAX` over these types are accepted.

**Status**: rejected at create time. Workarounds: `SUM((x)::numeric)`, or `SUM((x)::numeric)::real` to keep a `REAL` result column; `SUM((x)::numeric)::money`; `SUM(EXTRACT(EPOCH FROM x))` for an interval or a time.

### `SUM` / `AVG` over `DOUBLE PRECISION`

Accepted, and maintained by adding and subtracting, which is not the order PostgreSQL adds rows in. The stored sum can differ from the query's in the last digits. Measured on 20 000 rows of two-decimal values in 5 groups (sums about 4.5 million) through 90 writing statements: largest error of a SUM 1.2e-8. After one outlier is inserted and deleted the error stays while the group has rows: 5.5e-5 for a value of 1e12, 0.055 for 1e15, 10.07 for 1e18. It disappears when the group empties and refills.

**Status**: known limitation. Sum `(x)::numeric` when cancellation matters.

### Column alias lists that rename a source's columns

`FROM customers AS c(cid, nm)`: pg_reflex follows the columns of a source by their own names.

**Status**: rejected at create time. Workaround: drop the alias list and rename in the select list (`c.id AS cid`). When the renamed column is the primary key of a passthrough, pass the new name as the key (3rd argument): no key is inferred from a renamed primary key. A list that repeats the columns' own names is accepted.

### A WHERE clause reading the source as a whole row

`FROM orders o WHERE o IS NOT NULL`, in a one-source query, cannot be evaluated against the changed rows.

**Status**: rejected at create time. Workaround: test the columns themselves.

### A partition, or a table of an inheritance hierarchy, as a source

PostgreSQL fires the statement triggers of the table a statement names. A write addressed to a partitioned table does not fire the triggers of its partitions, and a write to a table of an inheritance hierarchy does not fire those of the other tables of the hierarchy, so an IMV reading a partition, an inheritance child, or a parent that has children would miss writes.

**Status**: rejected at create time, wherever the table is read (FROM, a JOIN, a CTE, a subquery), unless it is listed in `ignore_sources` with `!`. The message prints the remedy:

- for a partition: `FROM <partitioned table> WHERE <the partition's bound>`; below a table partitioned on several columns, read the partitioned table itself and pass `partition_by => ARRAY[]::text[]`; under a partitioned table whose name needs quotes, rename it first. The bound of a HASH partition names the partitioned table by OID, which a dump and restore changes;
- for an inheritance member: `ALTER TABLE <child> NO INHERIT <parent>`. A partitioned table, and a parent whose children were all detached, are accepted.

Not detected: a table that is already a source and is attached as a partition afterwards. Its IMVs miss the writes addressed to the new partitioned table.

### Two sources whose trigger names would be equal

The triggers and trigger functions of a source are named after it with `.` written as `_`, so `s.t` and `s_t`, or `a.b_c` and `a_b.c`, would share them, and the IMVs of one would go unmaintained.

**Status**: rejected at create time, when the second table is read by a new IMV or when one definition reads both. The message prints what to rename: a table no IMV reads yet; or an IMV whose target collides, dropped and created under another name; or the IMV being created, when the table it builds for itself collides. IMVs created before 1.12.0 over two such tables are not detected and stay unmaintained until one table is renamed and they are recreated.

### Expressions pg_reflex writes back with another meaning

pg_reflex writes parts of the query back from its parse tree. Two prefix operators in a row come back without a space: `- -v` as `--v`, the start of a comment, and `@ -v`, `~ -v`, `- @v`, `- ~v` as one operator (`@-`). An integer written `0x1F` comes back as the bit string `X'1F'`, and `0X1F`, `0o17`, `0b101` ending an unaliased item are read as the number 0 aliased `X1F`.

**Status**: rejected at create time wherever pg_reflex writes the expression back: a group key, an aggregate argument or `ORDER BY`, FILTER, HAVING, a CTE body, a set-operation operand, `DISTINCT` / `DISTINCT ON`, a window, a join condition, a WHERE conjunct of an aggregate or of a decomposed query. Workaround: parentheses, `-(-v)`, `@(-v)`, or the integer in decimal, as the message prints. Accepted where PostgreSQL runs your text: the select list, the FROM clause and the WHERE conjuncts kept as written of a plain SELECT (no aggregate, CTE, `DISTINCT ON`, window or HAVING). `- +v` and `+ -v` are accepted; in some clauses `- +v` fails to parse at create.

### A DEFERRED create after a write to its source in the same transaction

A write staged for the DEFERRED IMVs of a source is applied at COMMIT to every DEFERRED IMV of that source, including one created after the write and already filled from it.

**Status**: `create_reflex_ivm(…, 'DEFERRED')` is rejected in a transaction that has staged a write to one of its sources. Workaround: commit the writes, then create the IMV in its own transaction.

### Identifiers

- **Unquoted names are folded to lower case**, as PostgreSQL reads them, in the registry and in the stored query. Folding covers ASCII letters, as in a UTF-8 database. In a single-byte encoding PostgreSQL also folds non-ASCII letters through the locale: write such names in lower case.
- **`U&"…"` identifiers** with an escape sequence or a `UESCAPE` clause are rejected at create time. Write the name itself between double quotes.
- **A partitioned source whose name, or whose schema's name, PostgreSQL writes in double quotes** (upper case, a space, a reserved word, a column-name keyword such as `position`, `time`, `values`) is rejected at create time: the partitions of a source are followed by its unquoted name. Rename the table (`ALTER TABLE … RENAME TO …`), then create the IMV. An unpartitioned source of such a name is accepted.
- **Two columns of one name** in the SELECT list, or two grouping keys sharing a column name, are rejected at create time. Give each its own alias.
- **A bare constant beside `GROUP BY` or `DISTINCT`** is rejected at create time. Cast it: `'EUR'::text`, `NULL::text`.

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

### Shapes recomputed in full on write

Correct, at the cost of re-running the IMV's query on the writes named:

- a query holding a `RIGHT JOIN`: every write to any source. Write it as a `LEFT JOIN` with the tables swapped, and writes to the preserved table keep their delta;
- a table read through a subquery in WHERE, HAVING, the SELECT list, FILTER or a join condition: writes to that table;
- a table read inside a LEFT JOINed derived table or parenthesised join: writes to that table;
- a source read more than once (self-join, two aliases of one dimension, a CTE referenced twice): every write to it;
- a source read as a whole row (`COUNT(t)`, `to_jsonb(t)`): writes to that source;
- a passthrough without a key (a derived table in FROM, a set-returning function in the SELECT list, a primary key not projected under its own name): every write;
- an IMV built on a partitioned IMV whose partitions are rebuilt (a partition swap, `reflex_reconcile_partition`): refreshed whole, once per swap, with a NOTICE that gives the reason, unless it mirrors the rebuilt IMV's partitions, or its query proves that its rows of one partition key come only from rows of that key (one read of the rebuilt IMV as a top-level relation, the column taken from it, from a table equated to it by an inner join, or `COALESCE` over the two columns of the first join's equality) and the rebuilt keys can be named.

The full list, and a query that shows which statements recompute which IMV, are in [Upgrading to 1.12.0](../getting-started/upgrading.md#shapes-that-become-slower).

### `FULL OUTER JOIN`

`FULL JOIN` produces rows on both sides of the output — matched rows plus NULL-extended rows from each side. The algebraic delta tracks one source at a time, so on every INSERT/DELETE/UPDATE on either side, the engine cannot determine the matched-↔-unmatched transitions for the other side from delta alone.

**Status**: ✅ Supported via fallback. Both aggregate and passthrough `FULL JOIN` IMVs fall back to a full intermediate + target rebuild on every mutation to either side (2026-07-25: an earlier targeted-reconcile fast path for aggregate `FULL JOIN` was found to silently drop rows — an unmatched-side insert can surface a brand-new group that the scoped recompute's membership predicate never scopes into — and was removed in favour of the same unconditional rebuild the passthrough case already used).

**Cost shape**: every mutation on either side triggers a full rebuild of the intermediate and target from `base_query` — there is currently no scoped or algebraic path for this shape.

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
