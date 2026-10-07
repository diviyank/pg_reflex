# HAVING ROUND(AVG(v), 2) > 1 fails at create with `column "v" does not exist` — S3 (loud, at create)

Pre-existing: same raw error on the 1.11.5 base and on the 1.12.0 branch. Found by the Task 4b
reviewer (section 6, adjacent A6).

## Reproduction

Shape recorded in the review, no full script: `ROUND(AVG(v), 2) ... HAVING ROUND(AVG(v), 2) > 1`.
Result on base and on head: raw error `column "v" does not exist`.

## Expected vs actual

Expected: an IMV, or a tagged rejection. Actual: raw PostgreSQL error at create; nothing created.

## What was ruled out

Plain `HAVING AVG(v) > 1` and `HAVING MAX(v) > 2` are ok on both; so are nested forms with
`HAVING COUNT(v) > 1`, `HAVING COUNT(*) > 1`, `HAVING MIN(v) > 0 AND SUM(v) > 1`.

## What is established and what is not

Established: the error and parity. Not established: the cause. HAVING has its own aggregate
column list and reader (see the related reports), not touched by the 1.12.0 nested-aggregate
change.

## Related reports

- `2026-10-07_having_sum_over_all_null_group_reads_zero.md`
- `2026-10-07_having_filter_clause_ignored.md`
- `2026-10-07_sentinel_aggregate_with_having_raw_syntax_error.md` (the ungrouped HAVING raw
  `syntax error at or near "AND"`, also listed as A6, is that report)

## Fix direction (hypothesis to falsify)

Plan an aggregate nested in a HAVING expression the way the SELECT list does, so the intermediate
columns it needs exist. Pin with the shape above through INSERT, UPDATE and DELETE.
