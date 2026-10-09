# A HAVING aggregate under IS [NOT] NULL / BETWEEN / IN / LIKE / ANY fails at create with `column "v" does not exist` — S3 (loud, at create)

Pre-existing: same raw error on the branch before and after the 1.12.0 HAVING fold (commit
8ef9743 and the `fix/fold-g2-having` head), pg17. Found while probing shapes next to the HAVING
family.

## Reproduction

```sql
CREATE TABLE t(id SERIAL PRIMARY KEY, g TEXT, v INT);
INSERT INTO t(g, v) VALUES ('a', 1), ('a', 2), ('n', NULL);
SELECT create_reflex_ivm('h1', 'SELECT g, COUNT(*) AS c FROM t GROUP BY g HAVING SUM(v) IS NOT NULL');
SELECT create_reflex_ivm('h2', 'SELECT g, COUNT(*) AS c FROM t GROUP BY g HAVING SUM(v) BETWEEN 1 AND 6');
-- both: ERROR:  column "v" does not exist
```

The adversarial review of the HAVING fold (its P4, pg16, same result on 8ef9743 and on the head)
observed the same raw error at create for an aggregate under `IN (list)`, `IS TRUE`, `LIKE`,
`= ANY (ARRAY[...])`, `IS NOT DISTINCT FROM`, `NOTNULL`, `EXTRACT(... FROM MAX(d))`,
`MAX(d) AT TIME ZONE ...`, a row `IN`, and `> ALL (subquery)`.

Beside a `COUNT(DISTINCT ...)` these shapes are refused with the tagged "cannot be mixed" message
(pinned by `v112_hv_count_distinct_with_other_aggregate_under_any_expression_is_rejected`): there
the unrewritten aggregate would be legal SQL over the regrouped end query and silently wrong.

A selected `COUNT(DISTINCT ...)` repeated in HAVING under one of these nodes is evaluated as
written when its argument is a plain column without FILTER (correct, pinned by
`v112_hv_count_distinct_between`), and refused with a tagged message otherwise
(`COUNT(DISTINCT v) FILTER (...) BETWEEN 1 AND 2`, `COUNT(DISTINCT v + 1) IN (2, 3)`).

## Expected vs actual

Expected: an IMV, or a tagged `[reflex-unsupported]` rejection. Actual: a raw PostgreSQL error
raised at create; nothing is left behind.

## What is established

HAVING is rewritten by the walker the SELECT list uses (`replace_aggregate_calls`,
`src/aggregation.rs`). It descends into binary and unary operators, parentheses, function
arguments, CASE and casts; any other expression kind is printed as written, so the aggregate
call under it reaches the end query unrewritten. This is the HAVING side of
`2026-10-07_aggregate_inside_is_null_between_in_select_item_raw_error.md`: one fix to the walker
closes both.

## What was ruled out

`COALESCE(SUM(v), -1) < 0`, `NOT (SUM(v) > 5)` and CASE over aggregates work in HAVING and are
pinned (`src/tests/pg_test_v112_having.rs`).

## Fix direction (hypothesis to falsify)

Teach the walker the remaining expression kinds (IS NULL family, BETWEEN, IN list, IS DISTINCT
FROM, LIKE), or reject at create any HAVING / SELECT item whose rewritten text still holds an
aggregate call.
