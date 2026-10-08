# An unknown aggregate whose output column is named like a primary-key column is rejected although it would be a correct keyless IMV — limitation (loud, a correct definition is refused)

Residual of the alias-name key detection bug. The detection itself is fixed on the 1.12.0
branch: an output column is the source's primary key only when its select item is a bare,
optionally qualified, reference to that column output under the column's own name
(`resolve_unique_columns` / `columns_projected_under_their_own_name`, `src/create_ivm/mod.rs`).
`SELECT v AS id, g FROM s` is now a keyless passthrough, correct through INSERT / UPDATE /
DELETE (`v112_cl_alias_named_like_the_pk_is_not_the_key`).

## Reproduction (pg15, 1.12.0 branch)

```sql
CREATE TABLE s(id SERIAL PRIMARY KEY, g TEXT, v INT);
CREATE AGGREGATE myagg(int) (sfunc = int4larger, stype = int);
SELECT create_reflex_ivm('x', 'SELECT myagg(v) AS id FROM s');
-- ERROR: [reflex-unsupported] Unsupported aggregate(s): MYAGG. ... this one would be keyed on (id).
--        Fix: do not pass unique_columns, and do not name an output column like a primary key
--        column of the source.
SELECT create_reflex_ivm('x', 'SELECT myagg(v) AS m FROM s');   -- created, keyless, correct
```

## Expected vs actual

Expected: the first form is created like the second: `id` is not the key any more, so the IMV is
keyless and recomputed on every write. Actual: it is still rejected, and the message says it
"would be keyed on (id)", which is no longer what would happen.

## Why it was left

`v112_fn_ungrouped_unknown_aggregate_aliased_like_the_primary_key_rejected`
(`src/tests/pg_test_v112_function_contract.rs`) pins the rejection for
`SELECT agg(v) AS id FROM s` and `SELECT agg(id) AS id, agg(v) AS m FROM s`. The fix kept that
outcome through `BuildContext::primary_key_names_reused_by_outputs`, read by
`keyed_keyless_only_aggregate`. The remedy the message prints (rename the alias) still works.

## Fix direction

Decide that the pinned rejection is obsolete, then delete
`primary_key_names_reused_by_outputs` and its use in `keyed_keyless_only_aggregate`, and turn the
two pinned cases of that test into created-and-correct cases.
