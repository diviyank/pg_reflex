# Two tables whose names differ only by case, each read by an IMV, make a write raise — S2 (loud, blocks writes)

Pre-existing: the same with the 1.12.0 identifier-quote step disabled. Found while testing that
`"T"` and `t` stay two sources (G1 joins, fix round 2), pg17.

## Reproduction (pg17)

```sql
CREATE TABLE "Case_T"(id SERIAL PRIMARY KEY, season TEXT, v INT);
CREATE TABLE case_t(id SERIAL PRIMARY KEY, season TEXT, v INT);
SELECT create_reflex_ivm('v_upper', 'SELECT season, SUM(v) AS s FROM "Case_T" GROUP BY season');
SELECT create_reflex_ivm('v_lower', 'SELECT season, SUM(v) AS s FROM case_t GROUP BY season');
INSERT INTO "Case_T"(season, v) VALUES ('u',100);
-- ERROR:  relation "__reflex_new_case_t" does not exist
```

Run as `"V112_jr_case_T"` and `v112_jr_case_t`; the names above are shortened. Both IMVs are created and correct, and the registry holds two distinct sources
(`"Case_T"` and `case_t`; pinned by `v112_jr_names_differing_by_case_stay_two_sources`). The
first write raises. Not checked: a write to the lower-case table, DEFERRED, a single IMV over
`"Case_T"` with no lower-case homonym (the mixed-case reports below suggest it works).

## Expected vs actual

Expected: two relations, two independent sets of triggers. Actual: the write to one table runs
a statement that names the other table's transition table.

## What is known about the mechanism

Not traced. Candidates: the trigger functions or the `REFERENCING … AS` transition names are
emitted unquoted and fold to one name (`sanitized_source_suffix`,
`src/query_decomposer.rs`, keeps the case of a quoted name; an unquoted use of the result
folds it), so the second create replaces the first one's trigger function; or the trigger body
of one table selects the other table's IMV. Related:
`2026-10-08_unquoted_upper_case_source_beside_lower_case_raises_on_dml.md`,
`2026-10-01_mixed_case_names_rejected_by_create_reflex_ivm.md`.

## Fix direction (hypothesis)

Quote every generated identifier derived from a source name (trigger, trigger function,
transition tables, staging table), or fold the suffix and add a hash when folding changes it.
Add oracle tests for both tables written, IMMEDIATE and DEFERRED.
