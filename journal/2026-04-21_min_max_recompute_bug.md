# 2026-04-21 — `build_min_max_recompute_sql` JOIN-aware recompute

Investigated bug #1 from `journal/2026-04-21_db_clone_benchmark.md`:
`unsent_sop_forecast_reflex` flush aborts with
`ERROR: missing FROM-clause entry for table "caav"`.

## Root cause

`src/trigger.rs::build_min_max_recompute_sql` emits a scalar subquery
`SELECT {AGG}({source_arg}) FROM {source_table} WHERE ...` per MIN/MAX/BOOL_OR
intermediate column. For `unsent_sop_forecast_reflex` the stored
`source_arg = "caav.product_id IS NOT NULL"` where `caav` is a
`LEFT JOIN current_assortment_activity_reflex caav ON …` alias from the
user's SQL. The subquery only has `sales_simulation` in FROM, so `caav`
is unresolvable. The same function also assumes every group key column
is a direct column of `source_table` — for this IMV `canal_id` comes
from the `location` join, which would fail next even after fixing
`caav`.

## Why a one-line fix doesn't work

Tried passing `base_query` (the stored, intermediate-shape query that
already carries the full FROM/JOIN/WHERE structure) into the recompute
and referencing its output columns. Two issues surfaced:

1. **`base_query` at the call site is already delta-substituted.** In
   `reflex_flush_deferred` (trigger.rs:1120–1204) the stored
   `base_query` is rewritten via `replace_source_with_delta` —
   `sales_simulation` becomes `(SELECT * FROM __reflex_delta_…
   WHERE __reflex_op='I|D|U_OLD|U_NEW') AS __dt` — **before** it is
   passed into `reflex_build_delta_sql`. So the value available inside
   `build_min_max_recompute_sql` is the *delta-scoped* query, not the
   full-source one. Using it as the recompute source makes BOOL_OR read
   only the deleted rows — wrong answer.
2. **Intermediate vs output alias mismatch.** `base_query` emits
   intermediate column aliases (`"__bool_or_caav_product_id_is_not_null"`),
   not user-facing aliases (`in_current_assortment`). The initial fix
   looked up `end_query_mappings.output_alias` — correct for the final
   view, wrong for the intermediate-shape `base_query`.

Partial fix reverted (kept git state clean). Reproduction remains
pending a proper design.

## Proposed correct fix

Pass the **original, unsubstituted** `base_query` down the flush
chain, and reference **intermediate column names** (`ic.name`) in the
recompute SET list, not end-query aliases. Concretely:

1. Add `orig_base_query: &str` (or `Option<&str>`) parameter to
   `reflex_build_delta_sql`. Keep the delta-substituted `base_query`
   param for the MERGE path; add the new one solely for recompute.
2. In `reflex_flush_deferred` pass the registry's raw `base_query`
   (before `replace_source_with_delta`) as `orig_base_query`.
3. In `build_min_max_recompute_sql` switch to:
   ```sql
   UPDATE {intermediate} t
   SET "__bool_or_X" = __src."__bool_or_X", ...
   FROM ({orig_base_query}) AS __src
   WHERE __src."gc1" IS NOT DISTINCT FROM t."gc1" AND ...
     AND (t."__bool_or_X" IS NULL OR ...)
   ```
   Reference `ic.name` for the SET column (not `output_alias`).
4. Join cols should use `group_by_aliases.get(c)` to cross the
   potentially-different base_query-side column name (expression →
   user alias) to the intermediate-side normalized name. For plain
   columns these are identical.
5. Keep the existing scalar-subquery fallback for plans where
   `end_query_mappings` is empty (covers existing unit tests).

## Cost implication

Using the full `base_query` as the recompute source means Postgres
scans the whole source (and joins) every time a DELETE/UPDATE on a
BOOL_OR-bearing IMV lands. For `unsent_sop_forecast_reflex` that's a
~2-minute scan per flush. That's unacceptable for any hot delete
workload, but it's **correct**, which is priority #1 per CLAUDE.md.

A follow-on optimisation (separate patch) would make BOOL_OR
algebraically maintainable by adding a `__bool_or_true_count_X`
companion column (increment on TRUE-valued rows add, decrement on
subtract; BOOL_OR = `count > 0`). That removes the need for
recompute entirely — same pattern SUM+COUNT already uses.

## Test fixture for next session

Smallest repro (DEFERRED IMV, LEFT JOIN referenced in BOOL_OR, DELETE
on source):

```sql
CREATE TABLE t_src (g INT, p INT);
CREATE TABLE t_dim (p INT);
INSERT INTO t_src VALUES (1,1),(1,2),(2,3);
INSERT INTO t_dim VALUES (1);

SELECT create_reflex_ivm('v', $q$
  SELECT t_src.g,
         BOOL_OR(d.p IS NOT NULL) AS has_match
  FROM t_src LEFT JOIN t_dim d ON d.p = t_src.p
  GROUP BY t_src.g
$q$, 'g', 'UNLOGGED', 'DEFERRED');

DELETE FROM t_src WHERE p = 1;
SELECT reflex_flush_deferred('t_src');   -- currently errors
SELECT * FROM v;                         -- expected: (1,false),(2,false)
```

## Touched files / status

No code changes retained. Journal only. Memory entry updated to link
here and to surface the cost trade-off for the full-scan fix.

## Resolution — 2026-04-21 (later same day)

Fix applied. `build_min_max_recompute_sql` now takes `orig_base_query`
and emits

```sql
UPDATE intermediate t
SET "__col" = __src."__col", ...
FROM (orig_base_query) AS __src
WHERE t.gc IS NOT DISTINCT FROM __src.gc AND ... AND (t."__col" IS NULL OR ...)
```

Plumbed through `reflex_build_delta_sql` (now takes 7 args; 7th is
`orig_base_query`). IMMEDIATE plpgsql callers in `schema_builder.rs`
pass `_rec.base_query` for both param slots; DEFERRED
`reflex_flush_deferred` passes the stored (un-substituted)
`base_query` as the 7th arg. Regression test
`pg_test_deferred_bool_or_with_join_alias_recompute` covers the
BOOL_OR + LEFT JOIN + DEFERRED DELETE scenario. Full-scan cost trade-off
still stands — follow-on optimisation is the algebraic BOOL_OR with
`__bool_or_true_count_X` companion column.
