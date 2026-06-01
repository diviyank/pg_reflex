# Bugs: aggregate IMV projection codegen (two distinct defects)

## Summary

Two independent code-generation defects were found in `create_reflex_ivm` for
**aggregate** IMVs (queries with `GROUP BY`). Both involve how the `SELECT`
projection is rewritten into the intermediate/end tables; neither is a fundamental
limitation of incremental maintenance (the plain materialized-view equivalents work
fine, and minimal-change controls succeed).

- **Bug 1 — expression over a joined GROUP BY column.** A `SELECT` item that wraps a
  column coming from a joined sub-query/CTE which is also a `GROUP BY` key (e.g.
  `COALESCE(a.sx, 0)`) cannot be resolved in the generated end query. The bare
  column succeeds.
- **Bug 2 — carried `EXISTS` with a boolean term in its predicate.** A carried
  `EXISTS(... WHERE ... AND <bool col>)` projection is typed `numeric` instead of
  `boolean` in the intermediate table. Reproduces standalone with plain tables and
  no joins/CTEs — confirmed **distinct** from Bug 1.

Both were hit migrating `appro_summary_view` (schema `yse`) from `kind: mv` to
`kind: imv`, and both have minimal self-contained repros below.

## Environment

- pg_reflex: source at `4443ef0` (Cargo version `1.6.3`); the failure was first hit
  against the installed extension **`1.6.2`** on database `db_clone`.
- Discovered while migrating `appro_summary_view` (schema `yse`) from `kind: mv` to
  `kind: imv` in the `base-db-anchor-evm` view registry.
- The CTE-decomposition path (`try_decompose_ctes`, `src/create_ivm.rs`) is
  involved: each CTE becomes its own sub-IMV and the outer aggregate joins them.

## Bug 1 — expression over a joined GROUP BY column (reproducible, minimal)

### Repro

```sql
CREATE TABLE _rfx_bug_t(g int PRIMARY KEY, v int);
CREATE TABLE _rfx_bug_a(g int, x int);
INSERT INTO _rfx_bug_t VALUES (1,10),(2,20);
INSERT INTO _rfx_bug_a VALUES (1,5),(1,7);

-- FAILS: COALESCE wraps a.sx, which is a joined (sub-IMV) column AND a GROUP BY key
SELECT create_reflex_ivm('_rfx_imv_fail', $body$
  WITH agg AS (SELECT g, SUM(x) AS sx FROM _rfx_bug_a GROUP BY g)
  SELECT t.g, SUM(t.v) AS s, COALESCE(a.sx, 0) AS sx0
  FROM _rfx_bug_t t
  LEFT JOIN agg a ON a.g = t.g
  GROUP BY t.g, a.sx
$body$, unique_columns => 'g');
-- ERROR:  column "sx" does not exist
```

### Control (succeeds)

```sql
-- SUCCEEDS: identical query but the joined group key is projected bare (no COALESCE)
SELECT create_reflex_ivm('_rfx_imv_ok', $body$
  WITH agg AS (SELECT g, SUM(x) AS sx FROM _rfx_bug_a GROUP BY g)
  SELECT t.g, SUM(t.v) AS s, a.sx
  FROM _rfx_bug_t t
  LEFT JOIN agg a ON a.g = t.g
  GROUP BY t.g, a.sx
$body$, unique_columns => 'g');
```

The only difference is `COALESCE(a.sx, 0)` vs bare `a.sx`. Both are valid SQL; a
plain materialized view accepts either.

### Real-world error text

In the actual `appro_summary_view` (a `COALESCE(st.total_stock, 0)` over a
LEFT-joined aggregate sub-IMV that was also in `GROUP BY`) the message was:

```
column "coalesce_st_total_stock__0" does not exist
DETAIL: There is a column named "coalesce_st_total_stock__0" in table
        "appro_summary_view", but it cannot be referenced from this part of the query.
```

i.e. the projection's auto-derived column name (`coalesce_<expr>__0`) is created on
the **target** table, but the generated **end query** references the underlying
joined column (`sx` / `st.total_stock`) in a scope where it is not visible. The
intermediate/end-query rewrite for aggregate IMVs appears to assume `SELECT` items
over group keys are either bare column references or aggregates, and mishandles a
scalar expression applied to a *joined* group-key column.

## Bug 2 — carried `EXISTS` with a boolean term in its predicate is typed `numeric` (reproducible, minimal, DISTINCT from Bug 1)

This is a **separate defect**: it reproduces in a plain single-table aggregate IMV
with **no joins, no CTEs, and no group-key expression** — none of Bug 1's
conditions. The trigger is a **bare boolean column reference inside the carried
`EXISTS` subquery's `WHERE`**.

### Repro (self-contained, plain tables, 2 rows)

```sql
CREATE TABLE _rfx_bug_t(g int PRIMARY KEY, v int);
CREATE TABLE _rfx_pt(product_id int, is_active bool);
INSERT INTO _rfx_bug_t VALUES (1,10),(2,20);
INSERT INTO _rfx_pt VALUES (1,true);

-- FAILS: the EXISTS predicate contains a boolean conjunct (AND c.is_active)
SELECT create_reflex_ivm('_rfx_fail', $body$
  SELECT t.g, SUM(t.v) AS s,
         EXISTS(SELECT 1 FROM _rfx_pt c WHERE c.product_id = t.g AND c.is_active) AS flag
  FROM _rfx_bug_t t GROUP BY t.g
$body$, unique_columns => 'g');
-- ERROR: column "exists__select_1_from__rfx_pt_c_where_c_product_id___t_g_and_c_"
--        is of type numeric but expression is of type boolean
--        HINT: You will need to rewrite or cast the expression.

-- Also FAILS with the explicit form `AND c.is_active = true`.
```

### Control (succeeds)

```sql
-- SUCCEEDS: identical, but the boolean conjunct is removed from the predicate
SELECT create_reflex_ivm('_rfx_ok', $body$
  SELECT t.g, SUM(t.v) AS s,
         EXISTS(SELECT 1 FROM _rfx_pt c WHERE c.product_id = t.g) AS flag
  FROM _rfx_bug_t t GROUP BY t.g
$body$, unique_columns => 'g');
```

The *only* difference is the presence of `AND c.is_active` in the subquery's
`WHERE`. This is exactly the shape of the original `appro_summary_view`
(`EXISTS(... WHERE caav.product_id = pb.product_id AND caav.is_active)`).

### What is and isn't the trigger (verified)

| carried projection | result |
| --- | --- |
| `EXISTS(... WHERE c.k = t.g)` (no boolean term) | SUCCESS |
| `EXISTS(... WHERE c.k = t.g AND c.is_active)` | **FAIL** (numeric vs boolean) |
| `EXISTS(... WHERE c.k = t.g AND c.is_active = true)` | **FAIL** |
| `(t.g > 0) AS flag` (boolean expr, no subquery) | SUCCESS |
| `('x')::text AS lbl` (carried text) | SUCCESS |
| `EXISTS(...) ` over plain table / matview / simple reflex IMV, no bool term | SUCCESS |

So it is **not** "any carried boolean/non-numeric scalar" and **not** about the
subquery source kind — it is specifically a **boolean column reference within the
`EXISTS` predicate** that flips the carried column's inferred type to `numeric`.

### Root-cause hypothesis

An `EXISTS(...)` expression is unconditionally `boolean`, yet the generated
intermediate-table column for the carried scalar is declared `numeric` (the default
measure type) when its predicate contains a boolean term. The type inference for
the carried column appears to be driven by scanning the expression's column
references / text rather than from the top-level expression type — a boolean
conjunct in the predicate steers it to the wrong default. The fix is to type a
carried `EXISTS` (and any carried scalar) from the expression's actual result type:
`EXISTS` ⇒ `boolean`, independent of what its predicate references.

### Anomalous extra data point

The exact failing query against the real `current_assortment_activity_view` also
failed when the predicate was reduced to just `c.product_id = t.g` (no boolean
term), whereas an equivalent locally-built passthrough IMV with the same shape
succeeded with that reduced predicate. This suggests a possible second contributing
factor specific to that view (it is `mode: DEFERRED` and its body has a scalar
subquery `WHERE assortment_id = (SELECT ... FROM sop_current_view)`). The
boolean-conjunct repro above is deterministic and source-agnostic; this extra note
is for the maintainer's awareness, not part of the minimal repro.

## Why this is a bug, not a limitation

- Both shapes are legal SQL and both work as plain materialized views.
- The **bare-column control succeeds**, so incremental maintenance of this join +
  aggregate shape is supported; only the projection rewrite is wrong.
- CTE decomposition into sub-IMVs is an intended, supported feature
  (`try_decompose_ctes`), and the inputs here are exactly that shape.

## Suspected code area

- The aggregate intermediate/end-query generation: `src/query_decomposer.rs`
  (`generate_base_query`, `generate_end_query`, intermediate column naming via
  `sanitize_for_col_name` / `normalized_column_name`) and `src/aggregation.rs`
  (`IntermediateColumn` typing). The end query references the source column name
  while the target carries the derived `coalesce_..._0` / `exists_...` name.
- Likely fix directions: when a `SELECT` item is a scalar expression over group
  keys (including joined group keys), the end query should project the expression
  over the **intermediate's** stored group-key columns (rebinding inner column
  references to the intermediate table), and the intermediate column type should be
  inferred from the expression (so `boolean` stays `boolean`), not defaulted to
  `numeric`.

## Workaround (used in `base-db-anchor-evm`)

Keep the aggregate layer's `SELECT` to **bare group keys + aggregates only**, and
push every wrapping expression into a **passthrough outer layer**:

- Move `SUM(...)` into an `orders` CTE that projects only bare grouping columns and
  the aggregate.
- Compute the boolean flag as `BOOL_OR(is_active) ... GROUP BY product_id` in its
  own CTE (a clean aggregate) instead of a carried `EXISTS`.
- In the outer (non-aggregate, passthrough) `SELECT`, do all the
  `COALESCE(..., 0)` / `COALESCE(..., FALSE)` over the LEFT-joined sub-IMV columns —
  passthrough IMVs accept arbitrary scalar expressions.

This produced a working IMV whose row count and unique key matched the original
materialized view exactly.
