# SUM and AVG over a REAL column are wrong after an UPDATE (AVG also at create) — S1 (SILENT wrong result)

Pre-existing: same on the 1.11.5 base and on the 1.12.0 branch. Found by the Task 4b reviewer
(adjacent A4) and listed among "five more pre-existing bugs".

## Reproduction

Reproduction not recorded. The review states: "`SUM(real)` and `AVG(real)`, top level and nested:
wrong data after UPDATE (and at create for AVG)". In the nested-SUM table the cells
`SUM(real) / 2` and `SUM(real) + 1` are marked wrong on both base and head, with the note
"top-level `SUM(real)` is wrong too".

## Expected vs actual

Expected: IMV equals the query through UPDATE. Actual: mismatches after an UPDATE; for AVG also
right after create. Mismatch counts are not recorded.

## What was ruled out

Nested SUM over BIGINT, SMALLINT, NUMERIC and DOUBLE PRECISION are correct on the branch (the
REAL result type is the one that is not re-typed by `read_nested_sum_as`; the Task 4b author
lists "a nested SUM over a REAL column" as not measured and says only a BIGINT result is
re-typed). A plain REAL column in MIN/MAX is correct.

## What is not established

The cause. The sources give none.

## Related report

`2026-10-07_min_max_over_real_expression_wrong_data.md`

## Fix direction (hypothesis to falsify)

Establish with an oracle test which step loses precision (intermediate column type, the delta
expression, or the end-query cast) before choosing between widening the intermediate to DOUBLE
PRECISION and rejecting REAL arguments at create.
