# HAVING SUM(...) over an all-NULL group reads 0 — S1 (SILENT wrong result)

Pre-existing: identical outcome on the 1.11.5 base and on the 1.12.0 branch. Found by the Task 4b
reviewer (section 6, adjacent A6) while probing HAVING for ten shapes.

## Reproduction

Shape recorded in the review, no full script: `HAVING SUM(v) >= 0` where one group's `v` values
are all NULL. Result: "wrong" on base and on head. The control `HAVING SUM(v) > 0` (the Task 4b
author's `ctx_having_sum`) hides the group and is green, so the bug appears only when a NULL sum
should fail the predicate while 0 passes it.

## Expected vs actual

Expected: the all-NULL group has `SUM = NULL`, so `NULL >= 0` is not true and the group is absent
(PostgreSQL semantics). Actual: the HAVING reads 0 for the group, so the group is kept.

## What is established and what is not

Established: the wrong outcome and the base/head parity. The review's wording is "`SUM` over an
all-NULL group reads 0". Not established: the code path. Relevant fact from the 4b author: HAVING
aggregates are planned by their own column list in `plan_aggregation_inner` and read by
`rewrite_aggregate_call` (`src/query_decomposer.rs`), separately from the SELECT list, and that
branch was "not measured and not touched" by the fix.

## Related reports

- `2026-10-07_having_filter_clause_ignored.md`
- `2026-10-07_having_round_avg_raw_error_at_create.md`

## Fix direction (hypothesis to falsify)

Give the HAVING reader the same NULL-for-empty-count semantics the SELECT list now has (the
non-null counter guards SUM). Pin with a group whose values are all NULL under `HAVING SUM(v) >= 0`
through INSERT, UPDATE and DELETE.
