# A write to a table the query reads through an expression subquery recomputes the IMV in full — perf (correct, slow)

Introduced on purpose by the 1.12.0 bug-fold work (group G4, subquery writes) as the fix for the
silent wrong aggregates and the duplicate-key errors on such a write. Nothing wrong is stored; the
cost is one full recompute of the IMV per statement writing that table (IMMEDIATE) or per flush
(DEFERRED). Writes to the tables of the outer FROM clause keep their scoped delta.

## Reproduction

```sql
CREATE TABLE s(id SERIAL PRIMARY KEY, g TEXT, v INT);
CREATE TABLE t(id SERIAL PRIMARY KEY, g TEXT);
SELECT create_reflex_ivm('v', 'SELECT g, SUM(v) AS sv FROM s WHERE g IN (SELECT g FROM t) GROUP BY g');
INSERT INTO t(g) VALUES ('a');   -- TRUNCATE + refill of the intermediate table
INSERT INTO s(g, v) VALUES ('a', 1);   -- scoped delta, as before
```

`reflex_build_delta_sql` returns the full-refresh statements for every operation on `t`
(`imv_reads_source_by_expression_subquery` in `src/trigger/expression_subquery.rs`, called from
`imv_has_no_scoped_delta`). The shapes covered: `IN` / `NOT IN (SELECT …)`, `EXISTS` / `NOT EXISTS`,
`= ANY` / `> ALL (SELECT …)`, a scalar subquery in the WHERE, in the select list, inside an
aggregate argument, in a join condition, in HAVING or in a select-list expression over an
aggregate, correlated or not, at any nesting depth.

An UPDATE of the subquery's table that changes only columns the subquery does not read
(`UPDATE t SET note = …`) is recomputed in full as well: a table read only through a subquery has
no relevant-column list, so the UPDATE skip never applies to it.

## Workaround

Where the subquery is an uncorrelated `IN` over a column that is unique in the subquery's table,
write it as an inner join: `FROM s JOIN t ON t.g = s.g` is maintained incrementally on writes to
both tables (oracle-tested: `v112_sq_sum_join_and_in` in
`src/tests/pg_test_v112_subquery_writes.rs`). It is the same query only when `t.g` is unique;
with duplicates the join multiplies rows where `IN` does not.

## Why it is not scoped

The delta of a statement is the query with the written table swapped for its transition table.
For a table inside an expression subquery that computes the result over the rows of the OUTER
tables that the changed subquery rows select, and adds or subtracts it. That is the change of the
result only when each outer row is selected by exactly one subquery row and no other condition
lets it pass: a second matching row (`t.g` not unique), `NOT IN` / `NOT EXISTS`, a disjunction
(`… OR w IS NULL`), a scalar subquery over an aggregate, all make it wrong. On the unmodified code the
oracle matrix of `src/tests/pg_test_v112_subquery_writes.rs` found every one of these wrong or
raising on some write to `t`, including plain `SUM … WHERE g IN (SELECT g FROM t)` once `t` holds
a duplicate `g`.

## Fix direction (hypothesis)

A semi-join delta: for `x IN (SELECT y FROM t)` the outer rows whose membership can change are
those with `x` among the `y` of the transition rows; recompute the groups of those rows from the
full query (the affected-groups path the outer-join secondary uses), instead of the whole IMV.
That needs the correlation columns per subquery, which the analyser does not extract today, and
does not apply to a scalar subquery over an aggregate (every outer row can change). A create-time
WARNING naming the cost would make it visible in the meantime.
