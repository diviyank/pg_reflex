# A column or whole-row reference spelled like a source table makes every write to that source recompute the IMV in full — S4 (performance)

Narrowed on 2026-10-08 from `…unqualified_column_named_like_qualified_source_table_raises_on_dml`.
The correctness half is closed in 1.12.0; what is left is the cost.

## What was wrong, and what closed it

The transition-table rewrite (`replace_source_with_transition`, `src/sql_writer/identifier.rs`)
replaces the bare name of a source wherever it stands as a word, so a bare word of an
expression spelled like the source was swapped for the transition table:

```sql
-- a column of another table named like the joined source
CREATE TABLE dim(id SERIAL PRIMARY KEY, season TEXT, v INT);
CREATE TABLE fact(id SERIAL PRIMARY KEY, season TEXT, v INT, dim INT);
SELECT create_reflex_ivm('v', 'SELECT f.season, COUNT(dim) AS c, SUM(a.v) AS s
  FROM fact f JOIN dim a ON a.id = f.id GROUP BY f.season');
INSERT INTO dim(season, v) VALUES ('n',3);
-- before: ERROR:  column "__reflex_new_dim" does not exist

-- a column of the source itself
CREATE TABLE t(id SERIAL PRIMARY KEY, season TEXT, v INT, t INT);
SELECT create_reflex_ivm('w', 'SELECT season, SUM(t) AS s, COUNT(*) AS c FROM t GROUP BY season');
INSERT INTO t(season, v, t) VALUES ('e',10,1);
-- before: ERROR:  function sum(record) does not exist
```

The report this one replaces was understated on three counts: the source did not have to be
schema-qualified (an unquoted `public` source failed the same way, and so did `a.dim`); the
column could belong to the source table itself; and under DEFERRED nothing was raised: the
flush failed, the failure was swallowed as a warning and the IMV stayed stale (silent).

Since 1.12.0 `outer_joins` (`src/trigger/outer_join.rs`) records
`source_name_stands_as_a_value` when an unqualified identifier of an expression is spelled
like the source, and `has_no_scoped_delta` then recomputes the IMV in full on a write to that
source. Pinned by `v112_jr_n3_*` in `src/tests/pg_test_v112_joins_review.rs` (IMMEDIATE and
DEFERRED; table written quoted, bare and schema-qualified), which go RED when the guard is
removed.

## Residual

- Every write to such a source runs the whole query instead of a delta. A whole-row
  reference (`COUNT(t)`, `row_to_json(t)` over `FROM t`) takes the same path, although the
  rewrite would have handled it.
- The qualified forms (`COUNT(f.dim)`, `t.t`) keep their scoped delta.

## Fix direction (hypothesis)

Restrict the bare-name pass of `rewritten_source_spellings` to matches followed by a dot (a
match that is not a qualifier is either the table in a FROM clause, already consumed by the
qualified spellings, or something else), then drop the guard. The legacy-differential unit
tests in `src/sql_writer/tests.rs` pin the current output on generated text and will have to
change on purpose: a first attempt in the G1 work was reverted for that reason.
