# Session 2026-05-28: three fixes landed while migrating `forecast_analysis_view` to IMV

## Summary

Three independent defects were found and patched while migrating
`forecast_analysis_view` (schema `yse`, repo `base-db-anchor-evm`) from
`kind: mv` to `kind: imv` partitioned by `dem_plan_id`. All three were
blocking the migration; one is a previous-fix gap, one is a missing
materialisation path, one is an incomplete cascade in `drop`.

| # | Symptom | Cause | Fix site |
|---|---|---|---|
| 1 | `__max_<col>` typed `numeric` but source is `timestamptz` (even for unqualified `MAX(order_date)`) | Catalog probe in `query_column_types_from_catalog_with_per_source` hard-coded the `public` schema for unqualified source names — ignored the session's `search_path`. | `src/create_ivm.rs:2924` |
| 2 | `"…" is a view: Triggers on views cannot have transition tables` when a CTE body was a top-level UNION ALL | `try_decompose_set_op` always emitted `CREATE VIEW` over operand sub-IMVs. A downstream IMV consuming that view then tried to install transition-table triggers, which PostgreSQL rejects on views. | `src/create_ivm.rs:try_decompose_set_op` |
| 3 | `drop_reflex_ivm(top, true)` left synthetic CTE sub-IMVs (`<top>__cte_*`) in `__reflex_ivm_reference`; re-create then failed with `IMV with this name already exists` | The recursive cascade in `drop_reflex_ivm_impl` did its `<view>__` prefix check against the *current* recursion level, missing sibling sub-IMVs whose name only matches the top-level prefix (e.g. `<top>__cte_date_limits` referenced by both `<top>__cte_forecast_sales` and `<top>__cte_history_sales`). | `src/drop_ivm.rs:drop_reflex_ivm_impl` |

## Environment

- pg_reflex source at HEAD (`07c611f` before this session), Cargo `1.6.5`.
- Installed extension on `db_clone` was `1.6.5`, but built from an older commit
  that pre-dated the b387995 MIN/MAX qualified-column fix — surfaced as Fix 1
  below when the actual underlying bug was a separate one.
- Discovered building `forecast_analysis_view__imv_test` in schema `yse` with
  a CTE chain (`history_bounds`, `target_dp` UNION ALL, `forecast_bounds`,
  `date_limits`, `history_sales`, `forecast_sales`, `active_assortment`) over
  `history_sales_view` (matview), `sop_forecast_view` (IMV),
  `sop_current_view` (matview), `current_assortment_activity_view` (IMV).

---

## Fix 1 — `search_path` is now honoured by the column-types catalog probe

**Symptom.** Building any aggregate IMV that took `MAX`/`MIN` of a non-numeric
column from a source living in a non-`public` schema failed at create-time with
`column "__max_<col>" is of type numeric but expression is of type timestamp
with time zone`. Both **unqualified** (`MAX(order_date) FROM history_sales_view`)
and qualified-but-bare-source (`MAX(hsv.order_date) FROM history_sales_view hsv`)
forms repro'd. Adding the schema (`FROM yse.history_sales_view`) made it work.

**Root cause.** In `query_column_types_from_catalog_with_per_source`
(`src/create_ivm.rs:2924`), the previous logic split each source name and
fell back to a literal `"public"` when no schema prefix was present:

```rust
let (schema, tbl) = if table.contains('.') {
    let parts: Vec<&str> = table.splitn(2, '.').collect();
    (parts[0], parts[1])
} else {
    ("public", table.as_str())
};
// then: SELECT … FROM pg_attribute … WHERE n.nspname = $1 AND c.relname = $2
```

For a session with `search_path = yse, public`, an unqualified
`history_sales_view` referenced from the IMV body resolved to `yse.history_sales_view`
in PostgreSQL — but pg_reflex's catalog probe was searching `public.history_sales_view`,
which doesn't exist. The catalog query returned zero rows, `column_types` ended
up empty for that source, and the MIN/MAX type resolver fell back to its
`NUMERIC` default. The intermediate column then carried `timestamptz` while the
target table declared `numeric`, and PostgreSQL rejected the INSERT.

The previously-merged b387995 fix ("resolve MIN/MAX type-mismatch for qualified
columns") addressed the `source_arg` lookup *inside* the resolver but assumed
the `column_types` map was populated; it doesn't help when the map is empty
because the catalog probe couldn't find the table.

**Why other IMVs didn't hit it.** Every aggregate in the existing IMV set was a
`SUM`/`COUNT` over a `NUMERIC`/`BIGINT` column. An empty `column_types` map +
`NUMERIC` default *happens to be the right answer*. `MIN`/`MAX` over
`timestamptz` is what surfaced the bug.

**Fix.** Resolve the table via `to_regclass($1)`, which respects the session's
`search_path`. Pull the actual `relname` back from `pg_class` so the
`table.column` keys in the returned map stay consistent regardless of whether
the caller passed a qualified or unqualified source name.

```rust
let rows = client.select(
    "SELECT a.attname::text AS col_name, \
            format_type(a.atttypid, a.atttypmod) AS data_type, \
            c.relname::text AS relname, \
            CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable \
     FROM pg_catalog.pg_attribute a \
     JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
     WHERE c.oid = to_regclass($1) \
       AND a.attnum > 0 AND NOT a.attisdropped",
    None, &[…],
);
// … types.insert(format!("{}.{}", relname, col_name), pg_type.clone()); …
```

**Repro (against `db_clone`, schema `yse`):**

```sql
SET search_path = yse, public;
-- BEFORE: fails with the type-mismatch error
SELECT create_reflex_ivm(
  view_name => 't1',
  sql       => 'SELECT MAX(order_date) AS mx FROM history_sales_view',
  storage   => 'UNLOGGED'
);
-- AFTER: succeeds
SELECT mx FROM t1;
```

---

## Fix 2 — aggregation-free CTE sub-IMVs now materialise as TABLEs

**Symptom.** A CTE whose body was a top-level `UNION ALL` (e.g. `target_dp AS
(SELECT … WHERE status = 'archive' UNION ALL SELECT … FROM sop_current_view)`)
caused IMV creation to fail with:

```
"forecast_analysis_view__imv_test__cte_target_dp" is a view
Triggers on views cannot have transition tables.
```

The failure happened when the *next* CTE in the chain (which consumed
`target_dp`) tried to install its transition-table triggers on the view-backed
sub-IMV.

**Root cause.** `try_decompose_set_op` for `UNION ALL` emitted
`CREATE OR REPLACE VIEW` over the operand sub-IMVs:

```rust
if set_op.is_all {
    // UNION ALL: create a VIEW (zero overhead, always up-to-date)
    "CREATE OR REPLACE VIEW {wrapper} AS {op_0} UNION ALL {op_1}"
    …
}
```

That's fine when the wrapper is a *terminal* IMV that the user queries
directly. It's broken when the wrapper is an *intermediate* sub-IMV that
another IMV depends on — PostgreSQL refuses
`CREATE TRIGGER … REFERENCING NEW/OLD TABLE` on any view.

**Fix.** Thread a `materialize_as_table: bool` flag through `DecomposeCtx` and
`create_reflex_ivm_impl`. `try_decompose_ctes` sets it to `true` on every
recursive call that builds a CTE sub-IMV (those are *always* intermediate by
definition). `try_decompose_set_op` honours the flag in its `UNION ALL`
branch: instead of `CREATE OR REPLACE VIEW`, it does

1. `CREATE UNLOGGED TABLE <wrapper> AS <UNION ALL of operand sub-IMVs>` — CTAS,
   populated initially.
2. For each operand sub-IMV, install three trigger functions
   (`__reflex_union_mirror_{ins,del,upd}_<wrapper>_<i>`) plus three statement-
   level triggers (`AFTER INSERT / AFTER DELETE / AFTER UPDATE … REFERENCING
   NEW/OLD TABLE …`) that propagate 1:1 into the wrapper:
   - INS: `INSERT INTO wrapper(cols…) SELECT cols… FROM __reflex_new`
   - DEL: `DELETE FROM wrapper w WHERE EXISTS (SELECT 1 FROM __reflex_old o
     WHERE (w.col1, …) IS NOT DISTINCT FROM (o.col1, …))`
   - UPD: DEL pattern then INS pattern, in one statement

The operand recursion inside `try_decompose_set_op` propagates the flag
through, so nested set-op operands inside a CTE sub-IMV are also materialised
as tables.

**Caveat documented in the code.** The DELETE mirror matches all columns via
`IS NOT DISTINCT FROM`. That assumes operands have **no intra-operand
duplicate rows** — true for typical CTE shapes (each operand projects unique
key columns); duplicates would over-delete in the wrapper. If a use case
appears that needs duplicate handling, the upgrade is to add an internal
`__reflex_src_idx` column + `ROW_NUMBER()`-based exact-N matching.

**Top-level UNION-ALL IMVs (user-facing, no consumer)** still get the
zero-overhead `CREATE VIEW` path — the flag defaults to `false` in the public
`create_reflex_ivm_impl` entry point.

**Files touched.**

- `src/create_ivm.rs`
  - `DecomposeCtx` gets `materialize_as_table: bool` field.
  - New public entry `create_reflex_ivm_impl_with_materialization`; old
    `create_reflex_ivm_impl` is now a `false` shim.
  - `try_decompose_set_op`:
    - operand recursion now uses `create_reflex_ivm_impl_with_materialization`
      and propagates `ctx.materialize_as_table`.
    - `UNION ALL` branch wraps the existing VIEW emission in
      `if ctx.materialize_as_table { CTAS + mirror triggers } else { … }`.
    - Two new helpers: `query_table_column_names` and
      `install_union_mirror_triggers`.
  - `try_decompose_ctes`:
    - CTE sub-IMV recursion uses the new entry with `materialize_as_table=true`.
    - Outer-body recursion propagates `ctx.materialize_as_table`.

---

## Fix 3 — `drop_reflex_ivm` cascade no longer leaks shared synthetic sub-IMVs

**Symptom.** Dropping a CTE-decomposed IMV with `cascade=TRUE` left some
`<top>__cte_*` rows in `public.__reflex_ivm_reference`. A subsequent attempt to
create the same IMV failed with
`ERROR: [reflex-unsupported] IMV with this name already exists`.

In `forecast_analysis_view__imv_test` the leak was `__cte_date_limits`,
`__cte_forecast_bounds`, `__cte_history_bounds` — exactly the sub-IMVs
referenced by **two** sibling sub-IMVs (`__cte_forecast_sales` and
`__cte_history_sales`).

**Root cause.** `drop_reflex_ivm_impl` discovers the synthetic sub-IMVs to
drop by scanning the current view's `depends_on` for entries that match the
prefix `<view>__`:

```rust
let (_, parent_bare) = canonical_source(view_name);
let child_prefix = format!("{parent_bare}__");
…
if !child_bare.starts_with(&child_prefix) { continue; }
```

This works at the top level, where `view_name` *is* the user-facing IMV. But
the function recurses (each discovered sub-IMV is dropped via
`drop_reflex_ivm_impl(child, true)`), and at recursion depth ≥1 `parent_bare`
becomes the **intermediate** sub-IMV's name. Shared sub-IMVs are named
`<top>__cte_<…>` (the top-level IMV's prefix, not the intermediate one's),
so they fail the prefix check at depth ≥1 and are never discovered for drop.

**Fix.** Introduce `drop_reflex_ivm_impl_inner(view_name, cascade, root)`,
threading the **top-level** view name (`root`) through recursion. The prefix
check uses `root` instead of `view_name`:

```rust
let (_, root_bare) = canonical_source(root);
let child_prefix = format!("{root_bare}__");
```

The two recursive call sites are adjusted accordingly:

- The `graph_child` cascade (line 60) starts a new top-level drop for each
  user-facing child IMV — `root` becomes that child.
- The synthetic-sub-IMV drop (end of function) preserves the same `root`.

The public `drop_reflex_ivm_impl(view_name, cascade)` becomes a shim that
calls `_inner(view_name, cascade, view_name)`.

**Repro (verified before/after on `db_clone`):**

```sql
-- Build forecast_analysis_view as IMV (or fav_test as in this session's tests)
-- … 7 registry rows: fav_test + 6 __cte_* …

SELECT drop_reflex_ivm('fav_test', TRUE);

-- BEFORE: 3 orphan rows leak (__cte_date_limits, __cte_forecast_bounds,
--                              __cte_history_bounds)
-- AFTER:  registry is empty
SELECT name FROM public.__reflex_ivm_reference WHERE name LIKE 'fav_test%';
```

---

## Verified outcome

`forecast_analysis_view` was successfully converted to `kind: imv` partitioned
by `dem_plan_id` (partitioning stripped only for the db_clone test because
`db_clone` isn't partitioned yet). Build time 1.9s on `yse`. Row count
**498,253** — delta **0** vs the existing matview. UNION-ALL CTE form
(`target_dp`) used in the final body to exercise Fix 2.

## Known follow-ups not covered

- The mirror trigger **functions** (`public.__reflex_union_mirror_*`) are not
  explicitly dropped by `drop_reflex_ivm`. Operand sub-IMV drop cascades the
  triggers themselves; the functions get orphaned until the next
  `CREATE OR REPLACE FUNCTION` overwrites them. ~15 lines in `drop_ivm.rs` to
  clean them up.
- pg_reflex's own test suite has not been run against this patch.
- Same pattern (intermediate-only-needs-table) would also bite `try_decompose_distinct_on`
  and `try_decompose_window` if a downstream IMV consumed their VIEW output.
  The `materialize_as_table` flag is plumbed through but those branches still
  emit `CREATE VIEW`; the matching `if ctx.materialize_as_table { … }` wrap
  is a straightforward follow-up.
