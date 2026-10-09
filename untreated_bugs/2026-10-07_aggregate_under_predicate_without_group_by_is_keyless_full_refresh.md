# An aggregate under IS NULL / BETWEEN / IN / LIKE / ARRAY[...] is not maintained incrementally — limitation (performance without GROUP BY, tagged rejection with GROUP BY; data correct)

Residual of three reports closed on the 1.12.0 branch (aggregate spellings, aggregate inside
IS NULL / BETWEEN / IN, ungrouped aggregate wrapper with a unique key). What is closed:

- `pg_catalog.sum(v)`, `"sum"(v)`, `PG_CATALOG.SUM(v)` are the built-in aggregate and are
  maintained incrementally, with and without GROUP BY.
- An aggregate in an expression kind the aggregate mapping does not rewrite is recognised as
  holding an aggregate (`SelectColumn::holds_undecomposed_aggregate`, `src/sql_analyzer.rs`).
  Beside GROUP BY or DISTINCT it is rejected with a tagged message and a CTE remedy instead of
  the raw `aggregate functions are not allowed in GROUP BY`; without GROUP BY it can no longer be
  given a unique key.

## What remains (pg15, 1.12.0 branch)

```sql
-- table t(id serial primary key, g text, v int)
SELECT SUM(v) IS NULL AS e FROM t
SELECT ARRAY[SUM(v), COUNT(*)] AS a FROM t
SELECT SUM(v) BETWEEN 1 AND 2 AS b FROM t
```

Each is created as a keyless passthrough: oracle-correct through INSERT / UPDATE / DELETE in
IMMEDIATE and DEFERRED mode (`v112_cl_aggregate_under_a_predicate_without_group_by_is_recomputed`,
`v112_cl_ungrouped_aggregate_under_a_predicate_is_refused_a_key`), and fully recomputed on every
write. With `GROUP BY g` the same items are rejected at create; the remedy in the message (the
aggregate in a CTE, the expression in the outer SELECT) is proven by
`v112_cl_aggregate_under_a_predicate_cte_remedy_is_maintained`.

## Expected vs actual

Expected: maintained incrementally like `COALESCE(SUM(v), 0)`. Actual: full recompute per write
(ungrouped) or tagged rejection (grouped).

## Why it was left

Making them maintained was implemented and proven on pg15 (every expression kind, grouped and
ungrouped, both refresh modes): `replace_aggregate_calls` in `src/aggregation.rs` gets a
catch-all arm that visits the whole expression tree and swaps each aggregate call for its
intermediate reading, and `materialize_aggregate` reads the exact output type of the query for a
derived column whose type the type map has no name for (an array was stored as TEXT). It was not
merged because two existing tests pin the present behaviour and may not be edited by a fix:

- `v112_fn_ungrouped_aggregate_derived_item_is_not_called_a_non_aggregate` pins
  `SELECT SUM(v) IS NULL AS e, COUNT(*) AS c FROM t` as rejected with
  `'e' is not an aggregate expression that pg_reflex supports and is not a group key`;
- `v112_fn_ungrouped_column_rejection_prints_the_cte_remedy_first` pins the same rejection for
  `SELECT COUNT(DISTINCT v) IN (1, 2) AS e, COUNT(*) AS n FROM t`.

With the change, the first is created and correct and the second gets the nested COUNT(DISTINCT)
rejection. The same change also makes `HAVING SUM(v) IS NULL` / `BETWEEN` work
(`2026-10-07_having_aggregate_inside_is_null_between_raw_error.md`).

## Fix direction

Decide on the two pinned tests, then apply the catch-all arm and the exact-type read. Check the
type of a derived column over an interval or an array at the same time
(`2026-10-07_aggregate_expression_of_unmapped_type_stored_as_text.md`).
