# FILTER inside a HAVING aggregate is ignored — S1 (SILENT wrong result)

Pre-existing: identical outcome on the 1.11.5 base and on the 1.12.0 branch. Found by the Task 4b
reviewer (section 6, adjacent A6).

## Reproduction

Shape recorded in the review, no full script: `HAVING COUNT(*) FILTER (WHERE v > 1) > 0`. Result:
"wrong" on base and on head.

## Expected vs actual

Expected: the HAVING counts only rows with `v > 1`. Actual: the FILTER is not applied in HAVING
(the review's wording: "FILTER inside HAVING ignored"). Wrong row membership; magnitudes not
recorded.

## What is established and what is not

Established: the wrong outcome and base/head parity. Not established: the cause. Context from the
Task 4b report: the SELECT-list FILTER handling was unified in 1.12.0 (`aggregate_reading`), while
HAVING keeps its own column list in `plan_aggregation_inner` and its own reader
(`rewrite_aggregate_call`, `src/query_decomposer.rs`); the HAVING branch was not touched.

## Related reports

- `2026-10-07_having_sum_over_all_null_group_reads_zero.md`
- `2026-10-07_having_round_avg_raw_error_at_create.md`

## Fix direction (hypothesis to falsify)

Route HAVING aggregates through the same mapping as the SELECT list so FILTER reaches them, or
reject a FILTER inside HAVING at create. Pin with a group that passes only when the filter is
applied, through INSERT, UPDATE and DELETE.
