# `COUNT(DISTINCT)` beside an aggregate-derived expression escapes the "cannot be mixed" rejection — S3 (LOUD at create)

Pre-existing: same outcome on the 1.11.5 base and on the 1.12.0 branch unless stated. Found by the 1.12.0 whole-branch review; fixture and steps are the probes it ran (pg17).

## Reproduction

```sql
SELECT create_reflex_ivm('v', 'SELECT g, COUNT(DISTINCT v) AS n, SUM(v) + 1 AS s FROM s GROUP BY g');
-- 1.12.0: ERROR: column "__reflex_intermediate_v.__nonnull_count_v" must appear in the GROUP BY clause …
-- 1.11.5: the same raw error naming "__sum_v"
```

## Expected vs actual

Expected: the tagged rejection that `COUNT(DISTINCT)` cannot be mixed with other aggregates.
Actual: a raw PostgreSQL error. Nothing is left behind.

## Fix direction (hypothesis to falsify)

The check in `src/create_ivm/mod.rs` (`has_other_agg`) looks at the `aggregate` field of each
SELECT item and not at `is_aggregate_derived`; include derived items.
