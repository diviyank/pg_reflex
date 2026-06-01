# CTE Unique-Key Propagation for Sub-IMVs — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Automatically infer a sound unique key for CTE/JOIN passthrough sub-IMVs across to-one *and* to-many INNER joins, CROSS joins to single-row relations, and mixed equi+range joins — so they use targeted incremental `DELETE`/`UPDATE` instead of full refresh.

**Architecture:** Sub-IMVs are created in dependency order and each builds a real `__reflex_uk_*` `UNIQUE … NULLS NOT DISTINCT` index on its target table *before* the next CTE is built. So an upstream CTE's key is already visible to a downstream CTE through `pg_index` — the "trickle down" channel is the catalog, not in-memory state. Two gaps remain: (1) the inference's *anchor* probe only accepts a PRIMARY KEY (`source_primary_key_columns`), so union/aggregate/CTE sub-IMVs that have only a `__reflex_uk_*` unique index can never anchor; (2) a `CROSS JOIN` to a single-row relation (ungrouped aggregate like `history_bounds`) has no equi-condition to prove to-one. We fix (1) by widening the anchor probe to any *sound* unique index and (2) with one new `max_one_row` registry column. We then replace the all-or-nothing join gate in `infer_join_passthrough_unique_key` with per-join cardinality classification that composes a to-many key union.

**Tech Stack:** Rust + pgrx; `cargo pgrx test` (PG18); `__reflex_ivm_reference` catalog; `pg_index`/`pg_attribute` introspection.

**Spec:** `docs/specs/2026-05-31-cte-unique-key-propagation-design.md`

**Branch:** `feat/cte-unique-key-propagation` (already created).

---

## File structure / touchpoints

- `src/lib.rs` — add `max_one_row BOOLEAN` to `__reflex_ivm_reference` (CREATE TABLE + `ADD COLUMN IF NOT EXISTS` migration block).
- `src/sql_writer/registry.rs` — add `max_one_row` to `RegistryRow`, default `false` in `decomposed()`, write it in the full INSERT.
- `src/create_ivm.rs`
  - `persist_metadata` — compute and pass `max_one_row`.
  - `source_sound_unique_keys` (new) — sound unique keys from the catalog (PK or NOT-NULL / NULLS NOT DISTINCT unique index).
  - `source_is_max_one_row` (new) — registry lookup.
  - `join_type_for_target` (new) — the join type that introduced a given source.
  - `infer_join_passthrough_unique_key` — rewrite to per-join classification + to-many key union.
- `src/tests/pg_test_cte.rs` — new red tests (one per shape) + the synthetic forecast-shape cascade.
- `sql/pg_reflex--1.7.4--1.7.5.sql` (new) — migration for the new column.
- `Cargo.toml` — version bump `1.7.4` → `1.7.5`.

---

## Task 1: Add `max_one_row` registry column (schema + migration)

**Files:**
- Modify: `src/lib.rs:80-118` (CREATE TABLE) and `src/lib.rs:130-152` (ALTER block)
- Create: `sql/pg_reflex--1.7.4--1.7.5.sql`
- Modify: `Cargo.toml:7`

- [ ] **Step 1: Add the column to the CREATE TABLE body**

In `src/lib.rs`, inside the `CREATE TABLE IF NOT EXISTS public.__reflex_ivm_reference (...)` block, add the column immediately before the closing `target_schema TEXT` line (around line 117). Change:

```
        target_schema TEXT
    );
```
to:
```
        target_schema TEXT,
        -- 1.7.5 — TRUE for an ungrouped aggregate sub-IMV (aggregate with empty
        -- GROUP BY → at most one row). Read by JOIN unique-key inference so a
        -- CROSS JOIN to such a relation (e.g. a single-row history_bounds CTE)
        -- is classified to-one. NULL/FALSE for everything else.
        max_one_row BOOLEAN DEFAULT FALSE
    );
```

- [ ] **Step 2: Add the idempotent ADD COLUMN migration for existing installs**

In `src/lib.rs`, after the `partition_dispatch_cost_cap` ALTER (around line 152), add:

```rust
    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS max_one_row BOOLEAN DEFAULT FALSE;
```

- [ ] **Step 3: Create the version migration file**

Create `sql/pg_reflex--1.7.4--1.7.5.sql`:

```sql
-- Migration: pg_reflex 1.7.4 → 1.7.5
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.7.5';
--
-- 1.7.5 widens CTE/JOIN passthrough unique-key inference (to-one + to-many
-- INNER joins, CROSS-to-single-row, mixed equi+range). One catalog change: a
-- new `max_one_row` flag used to classify a CROSS JOIN to an ungrouped
-- aggregate sub-IMV as to-one. Existing rows default to FALSE (the prior
-- behaviour: such joins simply weren't inferred). No data backfill required —
-- inference re-runs at create time, and existing IMVs keep their stored keys.

ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS max_one_row BOOLEAN DEFAULT FALSE;

DO $migrate$
BEGIN
    RAISE NOTICE 'pg_reflex 1.7.5: added __reflex_ivm_reference.max_one_row; widened JOIN unique-key inference.';
END
$migrate$;
```

- [ ] **Step 4: Bump the crate version**

In `Cargo.toml`, change `version = "1.7.4"` to `version = "1.7.5"`.

- [ ] **Step 5: Compile-check**

Run: `cargo build`
Expected: builds (the new SQL is a string literal; RegistryRow not yet touched, so no field error yet). If `cargo build` is too slow in this environment, defer the check to Task 2 Step 6.

- [ ] **Step 6: Commit**

```bash
git add src/lib.rs sql/pg_reflex--1.7.4--1.7.5.sql Cargo.toml
git commit -m "feat(registry): add max_one_row column + 1.7.5 migration"
```

---

## Task 2: Persist `max_one_row` for ungrouped aggregate IMVs

**Files:**
- Modify: `src/sql_writer/registry.rs:34-54` (struct), `:71-91` (`decomposed`), `:188-219` (full INSERT)
- Modify: `src/create_ivm.rs:2320-2342` (`persist_metadata`)
- Test: `src/tests/pg_test_cte.rs` (new test `test_ungrouped_aggregate_sets_max_one_row`)

- [ ] **Step 1: Write the failing test**

Append to `src/tests/pg_test_cte.rs`:

```rust
// 1.7.5 — an ungrouped aggregate IMV (no GROUP BY → exactly one row) must be
// flagged max_one_row so JOIN inference can treat a CROSS JOIN to it as to-one.
#[pg_test]
fn test_ungrouped_aggregate_sets_max_one_row() {
    Spi::run("CREATE TABLE mor_src (id INT PRIMARY KEY, amt INT NOT NULL)").expect("t");
    Spi::run("INSERT INTO mor_src VALUES (1,10),(2,20)").expect("seed");

    let result = crate::create_reflex_ivm(
        "mor_view",
        "SELECT SUM(amt)::BIGINT AS total FROM mor_src",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let flag = Spi::get_one::<bool>(
        "SELECT max_one_row FROM public.__reflex_ivm_reference WHERE name = 'mor_view'",
    )
    .expect("q")
    .expect("flag");
    assert!(flag, "ungrouped aggregate must set max_one_row = TRUE");

    // A grouped aggregate must NOT be flagged.
    let result2 = crate::create_reflex_ivm(
        "mor_grouped",
        "SELECT id, SUM(amt)::BIGINT AS total FROM mor_src GROUP BY id",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result2, "CREATE REFLEX INCREMENTAL VIEW");
    let flag2 = Spi::get_one::<bool>(
        "SELECT COALESCE(max_one_row, FALSE) FROM public.__reflex_ivm_reference WHERE name = 'mor_grouped'",
    )
    .expect("q")
    .expect("flag2");
    assert!(!flag2, "grouped aggregate must NOT set max_one_row");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo pgrx test pg17 test_ungrouped_aggregate_sets_max_one_row`
(Use whatever pg version the project tests against; the repo targets PG18 — substitute the configured feature, e.g. `cargo pgrx test pg18 ...`.)
Expected: FAIL — column written as default FALSE (or `RegistryRow` has no field), so `flag` is false / compile error.

- [ ] **Step 3: Add the field to `RegistryRow`**

In `src/sql_writer/registry.rs`, add to the struct (after `partition_strategy` at line 53):

```rust
    pub partition_strategy: Option<&'a str>,
    /// TRUE for an ungrouped aggregate IMV (empty GROUP BY → at most one row).
    /// Written by the main create path; the decomposed paths leave it false.
    pub max_one_row: bool,
}
```

In `decomposed()` (the returned `RegistryRow { … }` literal, after `partition_strategy: None,` at line 90):

```rust
            partition_strategy: None,
            max_one_row: false,
        }
```

- [ ] **Step 4: Write `max_one_row` in the full INSERT**

In `src/sql_writer/registry.rs`, in the `else` (full_shape) branch:

Change the column list + VALUES (line 188-194) — append `max_one_row` as the final column and `$20` as its placeholder:

```rust
        let sql = "INSERT INTO public.__reflex_ivm_reference
                     (name, graph_depth, depends_on, depends_on_imv, unlogged_tables,
                      graph_child, sql_query, base_query, end_query,
                      aggregations, index_columns, unique_columns, enabled, last_update_date,
                      storage_mode, refresh_mode, where_predicate, ignored_sources,
                      partition_columns, partition_strategy, target_schema, max_one_row)
                     VALUES ($1, $2, $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::TEXT[], $7, $8, $9, $10::jsonb, $11::TEXT[], $12::TEXT[], TRUE, NOW(), $13, $14, NULLIF($15, ''), $16::TEXT[], NULLIF($17, '{}')::TEXT[], NULLIF($18, ''), COALESCE(NULLIF($19, ''), current_schema()), $20)";
```

Add the bool OID near the other OIDs (after `let oid_int4 = …` at line 122):

```rust
    let oid_bool = PgBuiltInOids::BOOLOID.oid().value();
```

Append the datum as the final element of the full-branch `&[ … ]` (after the `explicit_schema_owned` datum at line 218):

```rust
                    unsafe { DatumWithOid::new(explicit_schema_owned, oid_text) },
                    unsafe { DatumWithOid::new(row.max_one_row, oid_bool) },
                ],
```

(The `!full_shape` short INSERT is unchanged — decomposed rows keep the column default FALSE.)

- [ ] **Step 5: Set `max_one_row` in `persist_metadata`**

In `src/create_ivm.rs`, in `persist_metadata`, just before the `insert_registry_row(` call (after line 2318 `let ignored_sources_vec …`), add:

```rust
    let max_one_row = !ctx.plan.is_passthrough && ctx.plan.group_by_columns.is_empty();
```

Then add the field to the `RegistryRow { … }` literal (after `partition_strategy: Some(&ctx.plan.partition_strategy),` at line 2341):

```rust
            partition_strategy: Some(&ctx.plan.partition_strategy),
            max_one_row,
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cargo pgrx test pg18 test_ungrouped_aggregate_sets_max_one_row`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/sql_writer/registry.rs src/create_ivm.rs src/tests/pg_test_cte.rs
git commit -m "feat(registry): persist max_one_row for ungrouped aggregate IMVs"
```

---

## Task 3: Catalog/registry probe helpers for inference

**Files:**
- Modify: `src/create_ivm.rs` — add three helpers next to the existing key helpers (after `source_equi_join_columns`, around line 3165).
- Test: `src/tests/pg_test_cte.rs` (new `test_sound_unique_keys_includes_reflex_uk`)

- [ ] **Step 1: Write the failing test**

The helpers are private, so test them through behaviour: a JOIN whose anchor's only key is a reflex `__reflex_uk_*` unique index (no PK) must now anchor. Append to `src/tests/pg_test_cte.rs`:

```rust
// 1.7.5 — anchor probe must accept a sound UNIQUE index (not only a PRIMARY
// KEY). A CTE sub-IMV keyed via the explicit per-CTE spec gets a
// `__reflex_uk_*` NULLS NOT DISTINCT unique index; a downstream JOIN must be
// able to anchor on it. Here `j` (no PK on its sources) is keyed via the spec,
// then the outer body joins it to a to-one lookup.
#[pg_test]
fn test_anchor_accepts_reflex_unique_index() {
    Spi::run("CREATE TABLE au_a (k INT NOT NULL, v INT NOT NULL)").expect("a");
    Spi::run("CREATE TABLE au_lk (k INT PRIMARY KEY, label TEXT NOT NULL)").expect("lk");
    Spi::run("INSERT INTO au_a VALUES (1,10),(2,20)").expect("seeda");
    Spi::run("INSERT INTO au_lk VALUES (1,'x'),(2,'y')").expect("seedlk");

    // `j` keyed on k via the explicit per-CTE spec → builds __reflex_uk_*.
    // Outer body: j JOIN au_lk ON k (to-one), anchor must be `j` via its uk.
    let result = crate::create_reflex_ivm(
        "au_view",
        "WITH j AS (SELECT k, v FROM au_a) \
         SELECT j.k, j.v, lk.label FROM j JOIN au_lk lk ON lk.k = j.k",
        Some("k ; j : k"),
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let uk = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'au_view'",
    )
    .expect("q")
    .expect("outer key inferred from reflex unique index anchor");
    assert_eq!(uk, vec!["k".to_string()]);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo pgrx test pg18 test_anchor_accepts_reflex_unique_index`
Expected: FAIL — `au_view`'s `unique_columns` is empty because the current anchor probe (`source_primary_key_columns`) finds no PK on the `j` sub-IMV.

- [ ] **Step 3: Add `source_sound_unique_keys`**

In `src/create_ivm.rs`, after `source_equi_join_columns` (ends ~line 3165), add:

```rust
/// All sound unique keys of `source` discoverable from the catalog: the PRIMARY
/// KEY plus every UNIQUE index that is a *true* key — either all-NOT-NULL or
/// declared `NULLS NOT DISTINCT` (so NULLs cannot duplicate a key). Partial
/// (`indpred`) and expression (attnum 0) indexes are excluded. Each inner Vec is
/// one key's columns, lower-cased, in key order. Empty on none / catalog error.
///
/// This is the anchor counterpart to [`source_cols_cover_unique_key`]: an anchor
/// must EXPOSE a sound key (we project it), whereas a to-one "other" source need
/// only have its equi-join columns COVER some unique index.
fn source_sound_unique_keys(source: &str) -> Vec<Vec<String>> {
    Spi::connect(|client| {
        client
            .select(
                "SELECT array_agg(a.attname::TEXT ORDER BY k.n) AS cols \
                 FROM pg_index ix \
                 JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(col, n) ON true \
                 JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.col \
                 WHERE ix.indrelid = to_regclass($1) AND ix.indisunique \
                   AND ix.indpred IS NULL AND k.col <> 0 \
                 GROUP BY ix.indexrelid, ix.indisprimary, ix.indnullsnotdistinct \
                 HAVING bool_and(a.attnotnull) OR ix.indnullsnotdistinct OR ix.indisprimary",
                None,
                &[unsafe {
                    DatumWithOid::new(source.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .filter_map(|row| row.get_by_name::<Vec<String>, _>("cols").unwrap_or(None))
            .map(|cols| cols.iter().map(|c| c.to_lowercase()).collect())
            .collect()
    })
}

/// TRUE when `source` is a registered IMV flagged `max_one_row` (ungrouped
/// aggregate → at most one row). Base tables / unknown names → FALSE.
fn source_is_max_one_row(source: &str) -> bool {
    Spi::connect(|client| {
        client
            .select(
                "SELECT COALESCE(max_one_row, FALSE) AS m \
                 FROM public.__reflex_ivm_reference \
                 WHERE name = $1 OR name = $2 \
                 ORDER BY (name = $1) DESC LIMIT 1",
                None,
                &[
                    unsafe {
                        DatumWithOid::new(
                            source.to_string(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                    unsafe {
                        DatumWithOid::new(
                            bare_column_name(source).to_string(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                ],
            )
            .unwrap_or_report()
            .filter_map(|row| row.get_by_name::<bool, _>("m").unwrap_or(None))
            .next()
            .unwrap_or(false)
    })
}

/// The join type (`"INNER"`, `"LEFT"`, `"CROSS"`, …) of the join that introduced
/// `source` as its target table, or `None` if `source` is the base relation.
fn join_type_for_target(
    source: &str,
    joins: &[crate::sql_analyzer::JoinInfo],
) -> Option<String> {
    let bare = bare_column_name(source).to_lowercase();
    joins
        .iter()
        .find(|j| bare_column_name(&j.target_table).to_lowercase() == bare)
        .map(|j| j.join_type.clone())
}
```

- [ ] **Step 4: Build only (the rewrite in Task 4 makes the test pass)**

Run: `cargo build`
Expected: builds with `dead_code` warnings for the three new helpers (they're wired up in Task 4). Warnings are acceptable mid-task.

- [ ] **Step 5: Commit**

```bash
git add src/create_ivm.rs src/tests/pg_test_cte.rs
git commit -m "feat(infer): add sound-unique-key / max-one-row / join-type probes"
```

(The `test_anchor_accepts_reflex_unique_index` test stays red until Task 4 — that is expected and noted in the commit body if desired.)

---

## Task 4: Rewrite `infer_join_passthrough_unique_key` (per-join classification)

**Files:**
- Modify: `src/create_ivm.rs:3177-3283` (replace the whole function body)
- Test: the red tests from Task 3 + new shape tests below

- [ ] **Step 1: Write the failing shape tests**

Append to `src/tests/pg_test_cte.rs`:

```rust
// 1.7.5 — CROSS JOIN to a single-row (ungrouped aggregate) relation is to-one;
// the anchor's key survives. Shape mirrors date_limits' history_bounds arm.
#[pg_test]
fn test_infer_cross_join_to_single_row() {
    Spi::run("CREATE TABLE cj_dp (id INT PRIMARY KEY)").expect("dp");
    Spi::run("CREATE TABLE cj_amt (v INT NOT NULL)").expect("amt");
    Spi::run("INSERT INTO cj_dp VALUES (1),(2),(3)").expect("seeddp");
    Spi::run("INSERT INTO cj_amt VALUES (10),(20)").expect("seedamt");

    // hb: ungrouped aggregate (one row, max_one_row). Outer: dp CROSS JOIN hb.
    let result = crate::create_reflex_ivm(
        "cj_view",
        "WITH hb AS (SELECT MAX(v) AS mx FROM cj_amt) \
         SELECT d.id, hb.mx FROM cj_dp d CROSS JOIN hb",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
    let uk = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'cj_view'",
    )
    .expect("q")
    .expect("anchor key survives CROSS-to-single-row");
    assert_eq!(uk, vec!["id".to_string()]);
}

// 1.7.5 — mixed equi + range condition is to-one when the equi alone covers a
// unique key of the joined relation; the range is just an extra filter. Mirrors
// forecast_sales (JOIN ON dpid = dpid AND order_date BETWEEN …).
#[pg_test]
fn test_infer_equi_plus_range_is_to_one() {
    Spi::run("CREATE TABLE er_f (id INT PRIMARY KEY, gid INT NOT NULL, d INT NOT NULL)")
        .expect("f");
    Spi::run("CREATE TABLE er_w (gid INT PRIMARY KEY, lo INT NOT NULL, hi INT NOT NULL)")
        .expect("w");
    Spi::run("INSERT INTO er_w VALUES (1,0,100),(2,0,100)").expect("seedw");
    Spi::run("INSERT INTO er_f VALUES (10,1,50),(11,2,60)").expect("seedf");

    let result = crate::create_reflex_ivm(
        "er_view",
        "SELECT f.id, f.d, w.lo FROM er_f f \
         JOIN er_w w ON w.gid = f.gid AND f.d >= w.lo AND f.d <= w.hi",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
    let uk = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'er_view'",
    )
    .expect("q")
    .expect("equi covers key → to-one → anchor PK inferred");
    assert_eq!(uk, vec!["id".to_string()]);
}

// 1.7.5 — pure-range to-many INNER join: result key = anchor key ∪ joined
// relation's projected key. Mirrors history_sales (hsv range-join date_limits;
// key = hsv key ∪ dem_plan_id). Both sides keyed + projected, so it is provable.
#[pg_test]
fn test_infer_to_many_inner_key_union() {
    Spi::run("CREATE TABLE tm_h (hid INT PRIMARY KEY, d INT NOT NULL)").expect("h");
    Spi::run("CREATE TABLE tm_w (dp INT PRIMARY KEY, lo INT NOT NULL, hi INT NOT NULL)")
        .expect("w");
    Spi::run("INSERT INTO tm_w VALUES (1,0,100),(2,50,200)").expect("seedw");
    Spi::run("INSERT INTO tm_h VALUES (10,60),(11,70)").expect("seedh");

    // Each tm_h row can match multiple tm_w windows → to-many. Project both keys.
    let result = crate::create_reflex_ivm(
        "tm_view",
        "SELECT h.hid, w.dp, h.d FROM tm_h h \
         JOIN tm_w w ON h.d >= w.lo AND h.d <= w.hi",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
    let mut uk = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'tm_view'",
    )
    .expect("q")
    .expect("to-many key union inferred");
    uk.sort();
    assert_eq!(uk, vec!["dp".to_string(), "hid".to_string()]);

    // The result genuinely has the cross of matches; the unique index proves the
    // key (creation would have failed if it were not unique).
    assert_eq!(
        Spi::get_one::<i64>("SELECT COUNT(*) FROM tm_view").unwrap().unwrap(),
        2
    );
}

// 1.7.5 — to-many INNER join where the joined relation has NO projectable sound
// key → still unprovable → no key (keeps full-refresh fallback).
#[pg_test]
fn test_infer_to_many_without_joined_key_stays_none() {
    Spi::run("CREATE TABLE tk_h (hid INT PRIMARY KEY, d INT NOT NULL)").expect("h");
    Spi::run("CREATE TABLE tk_w (lo INT NOT NULL, hi INT NOT NULL, tag TEXT NOT NULL)")
        .expect("w"); // no PK / unique index
    Spi::run("INSERT INTO tk_w VALUES (0,100,'a'),(0,100,'b')").expect("seedw");
    Spi::run("INSERT INTO tk_h VALUES (10,50)").expect("seedh");

    let result = crate::create_reflex_ivm(
        "tk_view",
        "SELECT h.hid, h.d, w.tag FROM tk_h h JOIN tk_w w ON h.d >= w.lo AND h.d <= w.hi",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
    let uk: Option<Vec<String>> = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'tk_view'",
    )
    .expect("q");
    assert!(
        uk.is_none_or(|v| v.is_empty()),
        "to-many with unkeyed joined relation must not infer a key"
    );
}

// 1.7.5 — LEFT to-many must NOT compose a key (NULL-padding); stays None.
#[pg_test]
fn test_infer_left_to_many_stays_none() {
    Spi::run("CREATE TABLE lm_h (hid INT PRIMARY KEY, d INT NOT NULL)").expect("h");
    Spi::run("CREATE TABLE lm_w (dp INT PRIMARY KEY, lo INT NOT NULL, hi INT NOT NULL)")
        .expect("w");
    Spi::run("INSERT INTO lm_w VALUES (1,0,100),(2,0,100)").expect("seedw");
    Spi::run("INSERT INTO lm_h VALUES (10,50)").expect("seedh");

    let result = crate::create_reflex_ivm(
        "lm_view",
        "SELECT h.hid, w.dp, h.d FROM lm_h h \
         LEFT JOIN lm_w w ON h.d >= w.lo AND h.d <= w.hi",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
    let uk: Option<Vec<String>> = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = 'lm_view'",
    )
    .expect("q");
    assert!(
        uk.is_none_or(|v| v.is_empty()),
        "LEFT to-many must not compose a key"
    );
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo pgrx test pg18 test_infer_`
Expected: `test_infer_cross_join_to_single_row`, `test_infer_equi_plus_range_is_to_one`, `test_infer_to_many_inner_key_union`, and `test_anchor_accepts_reflex_unique_index` FAIL (no key inferred). The two negative tests (`test_infer_to_many_without_joined_key_stays_none`, `test_infer_left_to_many_stays_none`) already PASS under the old all-or-nothing gate — that's fine; they guard against regressions in the rewrite.

- [ ] **Step 3: Replace the function body**

In `src/create_ivm.rs`, replace the entire `infer_join_passthrough_unique_key` function (lines 3177-3283, keep the doc comment above it but extend it) with:

```rust
fn infer_join_passthrough_unique_key(ctx: &BuildContext) -> Option<Vec<String>> {
    let analysis = &ctx.analysis;
    if analysis.joins.is_empty() {
        return None;
    }

    // Per-join admissibility. CROSS is now allowed (to-one iff the joined
    // relation is single-row). RIGHT/FULL multiply or NULL-pad the anchor's
    // rows → refuse. OR / USING conditions defeat the equi analysis → refuse.
    let mut has_left = false;
    for join in &analysis.joins {
        match join.join_type.as_str() {
            "INNER" | "CROSS" => {}
            "LEFT" => has_left = true,
            _ => return None,
        }
        if let Some(cond) = &join.condition_sql {
            let lc = cond.to_lowercase();
            if lc.contains(" or ") || lc.trim_start().starts_with("using") {
                return None;
            }
        }
    }

    let real_sources: Vec<&String> = ctx.real_source_names.iter().collect();
    if real_sources.iter().any(|s| s.starts_with('<')) {
        return None;
    }

    // Base source = the one not introduced by any JOIN. A LEFT join only
    // preserves the base table's rows, so it is the only valid anchor then.
    let join_targets: std::collections::HashSet<String> = analysis
        .joins
        .iter()
        .map(|j| bare_column_name(&j.target_table).to_lowercase())
        .collect();
    let is_base = |s: &str| !join_targets.contains(&bare_column_name(s).to_lowercase());

    // output name → its SELECT expression (lower-cased), e.g. "id" → "o.id".
    let mut target_to_expr: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for col in &analysis.select_columns {
        let name = normalized_column_name(col.alias.as_deref().unwrap_or(&col.expr_sql));
        target_to_expr.insert(name, col.expr_sql.to_lowercase());
    }

    // The output name that projects `source.key_col` as a bare column reference,
    // or None when that key column is not passed through unaltered.
    let projected_output = |source: &str, key_col: &str| -> Option<String> {
        let bare = bare_column_name(source).to_lowercase();
        let aliases: Vec<String> = analysis
            .table_aliases
            .iter()
            .filter(|(_, t)| t.to_lowercase() == source.to_lowercase())
            .map(|(a, _)| a.to_lowercase())
            .collect();
        for (out_name, expr) in &target_to_expr {
            if is_from_table(expr, &bare, &aliases) && bare_column_name(expr) == key_col {
                return Some(out_name.clone());
            }
        }
        None
    };

    // The first sound unique key of `source` that is fully projected as bare
    // output columns, mapped to those output names. None when no key qualifies.
    let projected_sound_key = |source: &str| -> Option<Vec<String>> {
        for key in source_sound_unique_keys(source) {
            let mut outs = Vec::with_capacity(key.len());
            let mut all = true;
            for kc in &key {
                match projected_output(source, kc) {
                    Some(o) => outs.push(o),
                    None => {
                        all = false;
                        break;
                    }
                }
            }
            if all {
                return Some(outs);
            }
        }
        None
    };

    for anchor in &real_sources {
        if has_left && !is_base(anchor) {
            continue;
        }
        let Some(mut result_key) = projected_sound_key(anchor) else {
            continue;
        };

        // Classify every other source against this anchor.
        let mut composable = true;
        for other in &real_sources {
            if other.eq_ignore_ascii_case(anchor) {
                continue;
            }

            let (eq_cols, n_eq) =
                source_equi_join_columns(other, &analysis.joins, &analysis.table_aliases);
            let to_one = (n_eq > 0
                && !eq_cols.is_empty()
                && source_cols_cover_unique_key(other, &eq_cols))
                || source_is_max_one_row(other);
            if to_one {
                // Collapses to ≤1 matching row — contributes nothing to the key.
                continue;
            }

            // to-many: sound only for an INNER join whose joined relation has a
            // fully-projected sound key. Each output row is one distinct
            // (anchor-row, other-row) pair, so K_anchor ∪ K_other is unique.
            // LEFT/CROSS to-many can NULL-pad or multiply unbounded → refuse.
            if join_type_for_target(other, &analysis.joins).as_deref() != Some("INNER") {
                composable = false;
                break;
            }
            match projected_sound_key(other) {
                Some(outs) => result_key.extend(outs),
                None => {
                    composable = false;
                    break;
                }
            }
        }

        if composable {
            result_key.sort();
            result_key.dedup();
            if !result_key.is_empty() {
                return Some(result_key);
            }
        }
    }
    None
}
```

Also extend the doc comment directly above the function to reflect the widened rule (replace the existing `/// Sound rule: …` paragraph):

```rust
/// Sound rule: pick an anchor whose sound unique key (PK or NOT-NULL / NULLS
/// NOT DISTINCT unique index — see [`source_sound_unique_keys`]) is fully
/// projected. Then every other source must be either (a) to-one — its equi-join
/// columns cover a unique key, or it is `max_one_row` (single-row aggregate,
/// incl. CROSS joins) — contributing nothing, or (b) a to-many INNER join whose
/// own sound key is fully projected, contributing that key (K_anchor ∪ K_other
/// is unique). INNER/LEFT/CROSS anchors qualify; RIGHT/FULL, OR/USING, LEFT or
/// CROSS to-many, and any source without a projectable key are refused.
```

- [ ] **Step 4: Run the shape tests to verify they pass**

Run: `cargo pgrx test pg18 test_infer_`
Expected: all `test_infer_*` PASS, including the two negatives.

Run: `cargo pgrx test pg18 test_anchor_accepts_reflex_unique_index`
Expected: PASS.

- [ ] **Step 5: Run the existing inference + CTE regression tests**

Run: `cargo pgrx test pg18 test_join_passthrough`
Expected: `test_join_passthrough_infers_key_from_to_one_join`, `test_join_passthrough_no_key_when_to_many` (now has no projectable joined key → still None), `test_join_passthrough_no_key_for_range_join` (joined relation `rj_w` has no key → still None) all PASS.

Run: `cargo pgrx test pg18 test_cte_`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/create_ivm.rs src/tests/pg_test_cte.rs
git commit -m "feat(infer): per-join cardinality classification + to-many key union"
```

---

## Task 5: Synthetic forecast-shape cascade integration test

**Files:**
- Test: `src/tests/pg_test_cte.rs` (new `test_forecast_shape_cte_cascade`)

This reproduces the `forecast_analysis_view` chain with synthetic base TABLES (real PKs/unique indexes) so the cascade is deterministic: `date_limits` is seeded; `forecast_sales` (equi+range to-one) and `history_sales` (range to-many key-union) then auto-resolve.

- [ ] **Step 1: Write the test**

Append to `src/tests/pg_test_cte.rs`:

```rust
// 1.7.5 — end-to-end: a forecast_analysis_view-shaped chain. date_limits is
// seeded (union-anchored, discriminant dropped); forecast_sales and
// history_sales must then auto-resolve their keys via the widened inference.
#[pg_test]
fn test_forecast_shape_cte_cascade() {
    // Base tables (stand-ins for the real MVs, with concrete keys).
    Spi::run("CREATE TABLE fc_dp (dem_plan_id INT PRIMARY KEY, status TEXT NOT NULL)")
        .expect("dp");
    Spi::run(
        "CREATE TABLE fc_fcst (dem_plan_id INT NOT NULL, product_id INT NOT NULL, \
         location_id INT NOT NULL, order_date INT NOT NULL, qty INT NOT NULL, \
         PRIMARY KEY (dem_plan_id, product_id, location_id, order_date))",
    )
    .expect("fcst");
    Spi::run(
        "CREATE TABLE fc_hist (product_id INT NOT NULL, location_id INT NOT NULL, \
         order_date INT NOT NULL, qty INT NOT NULL, \
         PRIMARY KEY (product_id, location_id, order_date))",
    )
    .expect("hist");
    Spi::run("INSERT INTO fc_dp VALUES (1,'archive'),(2,'archive')").expect("seeddp");
    Spi::run("INSERT INTO fc_fcst VALUES (1,100,10,5,7),(2,100,10,6,9)").expect("seedf");
    Spi::run("INSERT INTO fc_hist VALUES (100,10,5,3),(100,10,6,4)").expect("seedh");

    // date_limits: bounds per dem_plan (one row per dpid) → seeded on dem_plan_id.
    // forecast_sales: fc_fcst JOIN date_limits ON dem_plan_id = dem_plan_id AND
    //   order_date range  → equi covers date_limits key (to-one) → key = fcst PK.
    // history_sales: fc_hist JOIN date_limits ON order_date range (to-many) →
    //   key = hist PK ∪ date_limits.dem_plan_id.
    let result = crate::create_reflex_ivm(
        "fc_view",
        "WITH date_limits AS ( \
           SELECT dem_plan_id, MIN(order_date) AS min_date, MAX(order_date) AS max_date \
           FROM fc_fcst GROUP BY dem_plan_id \
         ), forecast_sales AS ( \
           SELECT dl.dem_plan_id, f.product_id, f.location_id, f.order_date, f.qty \
           FROM fc_fcst f JOIN date_limits dl \
             ON f.dem_plan_id = dl.dem_plan_id \
            AND f.order_date >= dl.min_date AND f.order_date <= dl.max_date \
         ), history_sales AS ( \
           SELECT dl.dem_plan_id, h.product_id, h.location_id, h.order_date, h.qty \
           FROM fc_hist h JOIN date_limits dl \
             ON h.order_date >= dl.min_date AND h.order_date <= dl.max_date \
         ) \
         SELECT fs.dem_plan_id, fs.product_id, fs.location_id, fs.order_date, \
                fs.qty AS forecast, hs.qty AS hist \
         FROM forecast_sales fs \
         JOIN history_sales hs \
           ON fs.dem_plan_id = hs.dem_plan_id AND fs.product_id = hs.product_id \
          AND fs.location_id = hs.location_id AND fs.order_date = hs.order_date",
        Some("dem_plan_id,product_id,location_id,order_date ; date_limits : dem_plan_id"),
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // forecast_sales auto-resolves to the fc_fcst PK (equi+range to-one).
    let fs_uk = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference \
         WHERE name = 'fc_view__cte_forecast_sales'",
    )
    .expect("q")
    .expect("forecast_sales key auto-resolved");
    let mut fs_sorted = fs_uk.clone();
    fs_sorted.sort();
    assert_eq!(
        fs_sorted,
        vec![
            "dem_plan_id".to_string(),
            "location_id".to_string(),
            "order_date".to_string(),
            "product_id".to_string()
        ],
        "forecast_sales: equi+range to-one → fc_fcst PK"
    );

    // history_sales auto-resolves to fc_hist PK ∪ date_limits.dem_plan_id.
    let hs_uk = Spi::get_one::<Vec<String>>(
        "SELECT unique_columns FROM public.__reflex_ivm_reference \
         WHERE name = 'fc_view__cte_history_sales'",
    )
    .expect("q")
    .expect("history_sales key auto-resolved");
    let mut hs_sorted = hs_uk.clone();
    hs_sorted.sort();
    assert_eq!(
        hs_sorted,
        vec![
            "dem_plan_id".to_string(),
            "location_id".to_string(),
            "order_date".to_string(),
            "product_id".to_string()
        ],
        "history_sales: to-many key union (hist PK ∪ dem_plan_id)"
    );

    // Targeted incremental DELETE flows through the now-keyed sub-IMVs.
    let before = Spi::get_one::<i64>("SELECT COUNT(*) FROM fc_view").unwrap().unwrap();
    assert!(before >= 1);
    Spi::run("DELETE FROM fc_fcst WHERE dem_plan_id = 2").expect("del");
    let after = Spi::get_one::<i64>("SELECT COUNT(*) FROM fc_view WHERE dem_plan_id = 2")
        .unwrap()
        .unwrap();
    assert_eq!(after, 0, "rows for the deleted dem_plan are gone");
}
```

- [ ] **Step 2: Run the cascade test**

Run: `cargo pgrx test pg18 test_forecast_shape_cte_cascade`
Expected: PASS. If `history_sales` key is empty, re-check that `date_limits` built its `__reflex_uk_(dem_plan_id)` index (the seed) — the to-many `projected_sound_key(date_limits)` depends on it.

- [ ] **Step 3: Commit**

```bash
git add src/tests/pg_test_cte.rs
git commit -m "test(cte): forecast-shape unique-key cascade integration test"
```

---

## Task 6: Full verification + lint

**Files:** none (verification only)

- [ ] **Step 1: Full test suite**

Run: `cargo pgrx test pg18`
Expected: all tests PASS (prior count + the 8 new tests). No regressions in `pg_test_partition*`, `pg_test_set_ops`, `pg_test_drop`, `pg_test_reconcile`.

- [ ] **Step 2: Clippy**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean. (No remaining `dead_code` — all three helpers are wired in Task 4.)

- [ ] **Step 3: Format**

Run: `cargo fmt --check`
Expected: clean. If it reports diffs, run `cargo fmt` and re-commit.

- [ ] **Step 4: Commit any lint fixups**

```bash
git add -A
git commit -m "chore: clippy + fmt for cte unique-key propagation" || echo "nothing to commit"
```

---

## Task 7: Benchmark + worth evaluation (project process)

Per `CLAUDE.md`: benchmark, then evaluate whether the change earns its complexity before considering it done.

- [ ] **Step 1: Reproduce the real view on db_clone**

Recreate `omc.forecast_analysis_view` via `create_reflex_ivm` with the seed key
`unique_columns => '… ; date_limits : dem_plan_id'`. Confirm the
`INFO: … has no unique key` lines for `forecast_sales` / `history_sales` are
gone (they now log `inferred unique key (…)`); `date_limits` logs the explicit
key as before. Note whether the real MV sources expose the unique indexes the
inference needs — if a real source lacks one, that sub-IMV stays keyless (an
honest, documented outcome; add its explicit seed if incremental maintenance is
wanted there).

- [ ] **Step 2: Measure incremental DELETE/UPDATE vs. the prior full-refresh**

On the keyed sub-IMVs, run a representative source mutation (e.g. delete one
`dem_plan_id`'s forecast rows) and compare wall-clock against the pre-change
build (git stash / prior commit) where those sub-IMVs full-refreshed. Use the
existing benchmark harness (see `reference_benchmark_data` memory).

- [ ] **Step 3: Record results and decide**

Append findings to a session journal (`docs/` per project convention). If the
win is real and correctness holds, keep. If marginal, note it and keep anyway
(it removes the warnings and the full-refresh footgun), or open a follow-up to
optimize. Update the memory file for this work.

---

## Self-review notes

- **Spec coverage:** registry channel (Task 1-2), `max_one_row` (Task 1-2), widened anchor probe (Task 3 `source_sound_unique_keys`, Task 4 use), per-join classification to-one/to-many/unprovable (Task 4), conservative refusals (Task 4 gate + negatives in Task 4 Step 1), loud safety net = `__reflex_uk_*` build (unchanged, exercised by Task 4/5 COUNT assertions), explicit seed boundary (Task 5 uses the seed; Task 7 Step 1 documents it). All covered.
- **Type consistency:** `source_sound_unique_keys -> Vec<Vec<String>>`, `source_is_max_one_row -> bool`, `join_type_for_target -> Option<String>`, `RegistryRow.max_one_row: bool` — used consistently across Tasks 2-4.
- **Note on `cargo pgrx test pgNN`:** substitute the project's configured pg feature; the repo targets PG18, and `indnullsnotdistinct` requires PG15+ (satisfied).
