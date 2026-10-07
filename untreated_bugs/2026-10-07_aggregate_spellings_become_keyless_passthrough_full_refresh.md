# Aggregate spellings the analyzer does not recognise become keyless passthrough IMVs that fully refresh on every write — limitation (performance, data correct)

Pre-existing (release branch before 1.12.0 changes). Found by the Task 4 reviewer.

## Reproduction (from the review)

```sql
-- table t(id serial primary key, g text, v int)
SELECT pg_catalog.sum(v) AS s FROM t
SELECT "sum"(v) AS s FROM t
SELECT SUM(v) IS NULL AS e FROM t
SELECT ARRAY[SUM(v), COUNT(*)] AS a FROM t
```

All four are created as keyless passthrough IMVs. The first two are the no-GROUP-BY spelling; the
GROUP BY spelling of the same function raises (see the related report).

## Expected vs actual

Expected: an aggregate IMV maintained incrementally (or a tagged rejection). Actual: created,
"correct under the oracle, but each write is a full refresh". Data is correct; the cost per write
is a full recompute of the source.

## What is established and what is not

Established: creation as a keyless passthrough, oracle-correct, full refresh per write. Not
measured: the cost on a large source. Not established: the cause beyond the analyzer not
recognising these spellings as aggregates (same classification path as the related report).

## Related report

`2026-10-07_qualified_or_quoted_aggregate_name_raw_error.md` (same unrecognised spelling, raw
error with GROUP BY). The 1.12.0 keyless-passthrough create warning covers part of this surface
but its coverage of these four spellings was not checked.

## Fix direction (hypothesis to falsify)

Resolve a function name to its built-in aggregate identity once, before classification (see the
related report), and plan an aggregate wrapped in an expression (`IS NULL`, `ARRAY[...]`) through
the nested-aggregate mapping. Check the create-time warning fires for any shape that stays
keyless.
