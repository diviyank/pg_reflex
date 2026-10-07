# An aggregate inside IS NULL / BETWEEN / IN as a SELECT item is classified as a group key and raises `aggregate functions are not allowed in GROUP BY` — S3 (loud)

Pre-existing, and wider than DISTINCT. Observed during the 1.12.0 hardening work on the
`release/1.12.0` branch (base commit 6946400). PostgreSQL version: the Task 2 re-review that found
the gap was a static reading with nothing executed and states no version; the one executed
variant (`SUM(v) IS NULL`) was run in the audit that preceded the release, and its PostgreSQL
version is not stated in the sources.

## What was executed and what was reasoned

| Variant | Status |
|---|---|
| `SELECT g, SUM(v) IS NULL AS c FROM t GROUP BY g` | Executed in the audit: raises raw `aggregate functions are not allowed in GROUP BY` at create. (The exact query text was not recorded in the sources; this is the shape named in the ruling.) |
| `COUNT(DISTINCT v) IS NULL` | Reasoned only. Not executed. |
| `COUNT(DISTINCT v) BETWEEN 1 AND 2`, `COUNT(DISTINCT v) IN (1,2)` | Reasoned only. Not executed. |
| other expression kinds around an aggregate (Extract, Like, Substring, Trim, Ceil/Floor, Collate, AtTimeZone, Tuple, Array) | Reasoned only; same code path. |

## Reproduction (the executed variant, shape only)

```sql
CREATE TABLE t(id SERIAL PRIMARY KEY, g TEXT, v INT);
SELECT create_reflex_ivm('x', 'SELECT g, SUM(v) IS NULL AS c FROM t GROUP BY g');
-- ERROR:  aggregate functions are not allowed in GROUP BY
```

## Expected vs actual

Expected: an aggregate nested in a predicate-like expression is classified as aggregate-derived
and either supported or cleanly rejected. Actual (executed variant): the raw error above.
Actual (reasoned variants): the Task 2 re-reviewer's static reading says the same classification
gap applies to `COUNT(DISTINCT v) IS NULL / BETWEEN / IN`, and flagged that the DISTINCT may be
lost or the create may fail depending on base-query generation, which was unverified. The
parked ruling asks the final review to execute it once; the sources do not contain that result.

## What is known about the mechanism

By the re-reviewer's reading of `src/sql_analyzer.rs`: `expr_contains_call` (and
`expr_contains_aggregate`, whose semantics are unchanged) do not descend into `IsNull`,
`IsNotNull`, `Between`, `InList`, `Like`, `Extract`, `Substring`, `Trim`, `Ceil`/`Floor`,
`Collate`, `AtTimeZone`, `Tuple`, `Array`, subqueries, or named function arguments. So such a
select item gets `has_nested_agg = false`, is treated as a passthrough/group-like item, and the
aggregate ends up in a generated `GROUP BY`, where PostgreSQL refuses it. Same hole for `SUM(v)
IS NULL`, hence pre-existing and not specific to DISTINCT.

## Fix direction (hypothesis)

Extend the expression walk to every `Expr` variant that can contain an aggregate, preferably by
using the AST visitor that the 1.12.0 DISTINCT check already uses instead of the hand-written
descent, so a new variant cannot be missed. Then the classification gives either an
aggregate-derived column (supported) or a tagged rejection. Add tests for each variant
including the `COUNT(DISTINCT)` ones, executed rather than reasoned.
