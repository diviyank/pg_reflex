# `create_reflex_ivm` raises a raw `syntax error` for a partitioned passthrough that joins its source with a CTE aggregating that same source — S3 (loud, at create)

Pre-existing: seen on pg16 on `d0a3e06` (1.12.0 integration branch) while building the fixture of
`src/tests/pg_test_v112_swap_cascade.rs`. Not reduced; not tried without `partition_by`, without
`ignore_sources`, or in IMMEDIATE.

## Reproduction

Sources and `sc_sfv` as built by `sc_open` in `src/tests/pg_test_v112_swap_cascade.rs`. Then:

```sql
SELECT create_reflex_ivm('sc_share',
  'WITH product_total AS (
     SELECT sf.product, SUM(sf.quantity) AS total FROM sc_sfv AS sf GROUP BY sf.product)
   SELECT sf.plan, sf.product, sf.loc, sf.d, sf.quantity, pt.total
     FROM sc_sfv AS sf JOIN product_total AS pt ON pt.product = sf.product',
  'plan, product, loc, d ; product_total : product', NULL, 'DEFERRED', '!sc_sfv', ARRAY['plan']);
-- ERROR:  syntax error at or near "sc_share__cte_product_total"
```

## Expected vs actual

Expected: the IMV, or a tagged refusal that names the unsupported shape and a remedy. Actual: a raw
PostgreSQL syntax error from SQL pg_reflex generated (evidence:
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/logs/b-sc-probe1.log`). Whether the
generated sub-IMV is left behind was not checked.

## Related

`2026-10-10_table_joined_with_cte_over_itself_blocks_inserts_23505.md` (same family of shape, a
different failure) and `2026-10-10_raw_postgres_errors_at_create_for_five_definitions.md`.

## Fix direction (hypothesis)

Find the statement that splices the generated sub-IMV name unquoted or in the wrong position, then
either generate valid SQL or reject before any DDL with `reflex_reject`.
