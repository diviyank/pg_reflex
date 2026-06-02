# Sub-partition (multi-level) Source Support — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let pg_reflex mirror a source table's *full* multi-level partition hierarchy (e.g. `sales_simulation`: `LIST(dem_plan_id) → RANGE(order_date)`) onto an IMV, reconcile at any level, and capture `DETACH`/`ATTACH` swaps via the existing DDL event trigger plus an oid-diff snapshot flush.

**Architecture:** Recursively walk the source's `pg_inherits` tree into `PartitionNode`s; generate matching IMV partition children with a sub-`PARTITION BY` suffix for internal nodes; reuse the existing leaf-swap engine (`execute_partition_swap_for_child`) for fills. Capture is reconcile-driven (not DML-trigger-driven, since `ATTACH`/`DETACH` are DDL): the existing `__reflex_on_ddl_command_end` event trigger is extended to enqueue the affected source root into `__reflex_partition_pending`, and `reflex_flush_partitions()` resolves swap/attach/detach via an oid-diff against `__reflex_source_partition_snapshot`. Correctness is the #1 priority; an audit drift-check is the backstop.

**Tech Stack:** Rust + pgrx (`#[pg_extern]`, `extension_sql!`, `#[pg_test]`), PostgreSQL 18, SPI. Approved design: `plans/sub_partitioning.md`.

**Test commands (project standard):**
- Unit (pure Rust): `cargo test --lib <name>` (fast; no Postgres).
- Integration: `cargo pgrx test pg18 <name>`.
- Full gate before each phase commit: `cargo pgrx check && cargo clippy && cargo fmt --check`.

**TDD discipline (CLAUDE.md):** write tests first; do **not** modify a test after it passes. One assertion-focus per test. Commit frequently.

---

## File Structure

| File | Responsibility | This plan |
|---|---|---|
| `src/partition.rs` | partition introspection, DDL codegen, sync, reconcile, swap | recursive tree walk, node DDL pair, oid-diff classifier, recursive sync, level-agnostic reconcile, snapshot helpers |
| `src/create_ivm/mod.rs` | create-time IMV build | replace single-level mirror loop with tree walk; all-levels validation; snapshot seed |
| `src/lib.rs` | `#[pg_extern]`s, catalog DDL, event triggers | `source_partition` arg on `reflex_reconcile_partition`; `reflex_flush_partitions`; two catalog tables; extend event trigger to enqueue |
| `src/audit/checks_b_drift.rs` | drift detection | recursive source-vs-IMV leaf-set + per-leaf row-count check |
| `src/tests/unit_partition.rs` | pure-Rust unit tests | node DDL, oid-diff classifier, naming |
| `src/tests/pg_test_subpartition.rs` (new) | integration | mirror-shape, swap, lifecycle, flush, cascade, drift |
| `src/tests/pg_test_fuzz.rs` | differential fuzz | add attach/detach/swap sequence oracle |
| `benchmarks/bench_partitioned_imv.sql` | perf | 2-level scenario |

New `pg_test_subpartition.rs` must be wired into the test harness the same way the others are. Find the include site:

```bash
rg -n "pg_test_partition" src/lib.rs src/tests/mod.rs 2>/dev/null
```

Add the new file beside it using the identical mechanism (`include!` or `mod`).

---

# Phase 1 — Recursive hierarchy mirroring + codegen + validation

Goal: `create_reflex_ivm` on a multi-level source builds an IMV target (and intermediate, for aggregates) whose partition tree matches the source tree exactly.

## Task 1.1: `PartitionNode` + recursive tree walk

**Files:**
- Modify: `src/partition.rs` (add struct + fn near `list_partition_children`, ~line 154)
- Test: `src/tests/pg_test_subpartition.rs` (new)

- [ ] **Step 1: Write the failing integration test**

Create `src/tests/pg_test_subpartition.rs`:

```rust
// Integration tests for multi-level (sub-partition) source support.
// Plan: plans/sub_partitioning_impl_plan.md. Included from src/lib.rs tests module.

#[pg_test]
fn pg_subpart_tree_walk_lists_all_levels() {
    Spi::run(
        "CREATE TABLE ss (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, qty INT) \
         PARTITION BY LIST (dem_plan_id)",
    )
    .expect("root");
    Spi::run("CREATE TABLE ss_172 PARTITION OF ss FOR VALUES IN (172) PARTITION BY RANGE (order_date)")
        .expect("list child");
    Spi::run(
        "CREATE TABLE ss_172_2025_01 PARTITION OF ss_172 \
         FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')",
    )
    .expect("range leaf 1");
    Spi::run(
        "CREATE TABLE ss_172_2025_02 PARTITION OF ss_172 \
         FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')",
    )
    .expect("range leaf 2");

    // 3 descendant nodes total: ss_172 (internal), and its two leaves.
    let n = Spi::get_one::<i64>(
        "SELECT count(*) FROM crate_test_list_partition_tree('public.ss')",
    );
    // crate_test_list_partition_tree is a thin test-only SQL wrapper defined below.
    assert_eq!(n.unwrap().unwrap(), 3);
}
```

Note: to assert on a Rust-internal function from SQL, expose a **test-only** `#[pg_extern]` wrapper. Add to `src/lib.rs` inside the existing `#[cfg(any(test, feature = "pg_test"))]` tests module (or a dedicated test-helpers section guarded by the same cfg):

```rust
#[cfg(any(test, feature = "pg_test"))]
#[pg_extern]
fn crate_test_list_partition_tree(root: &str) -> i64 {
    Spi::connect(|client| crate::partition::list_partition_tree(&client, root).len() as i64)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo pgrx test pg18 pg_subpart_tree_walk_lists_all_levels`
Expected: FAIL — `list_partition_tree` not found (compile error).

- [ ] **Step 3: Implement `PartitionNode` + `list_partition_tree`**

In `src/partition.rs`, after `PartitionChild` (line ~52), add:

```rust
/// A node in a source table's (possibly multi-level) partition tree.
///
/// `bare_name` is the unqualified relname of this node.  `parent_bare` is the
/// immediate parent's relname (the root's own parent is the anchor root).
/// `bound_expr` is the `FOR VALUES …` fragment relative to the immediate
/// parent.  `sub_strategy`/`sub_columns` are `Some`/non-empty only when this
/// node is *itself* partitioned (an internal node); leaves have `None`/empty.
#[derive(Debug, Clone)]
pub(crate) struct PartitionNode {
    pub bare_name: String,
    pub parent_bare: String,
    pub bound_expr: String,
    pub sub_strategy: Option<String>,
    pub sub_columns: Vec<String>,
}
```

Then add the recursive walk (uses `WITH RECURSIVE` over `pg_inherits`, one query):

```rust
/// Recursively list every descendant partition node of `root`
/// (schema-qualified or bare), ordered top-down (parents before children) so
/// callers can create IMV-side parents before their children.  Each node
/// carries its own sub-partition strategy/columns when it is itself
/// partitioned.  Returns an empty vector when `root` is not partitioned.
pub(crate) fn list_partition_tree(
    client: &pgrx::spi::SpiClient<'_>,
    root: &str,
) -> Vec<PartitionNode> {
    let sql = "\
        WITH RECURSIVE tree AS ( \
            SELECT i.inhrelid AS child_oid, i.inhparent AS parent_oid, 1 AS depth \
            FROM pg_inherits i \
            WHERE i.inhparent = to_regclass($1) \
          UNION ALL \
            SELECT i.inhrelid, i.inhparent, t.depth + 1 \
            FROM pg_inherits i JOIN tree t ON i.inhparent = t.child_oid \
        ) \
        SELECT \
            c.relname::text AS bare_name, \
            pc.relname::text AS parent_bare, \
            pg_get_expr(c.relpartbound, c.oid) AS bound_expr, \
            CASE pt.partstrat WHEN 'l' THEN 'LIST' WHEN 'r' THEN 'RANGE' \
                              WHEN 'h' THEN 'HASH' ELSE NULL END AS sub_strategy, \
            COALESCE(( \
                SELECT array_agg(a.attname::text ORDER BY k.n) \
                FROM unnest(string_to_array(pt.partattrs::text, ' ')::int[]) \
                    WITH ORDINALITY AS k(attnum, n) \
                JOIN pg_attribute a ON a.attrelid = pt.partrelid \
                                   AND a.attnum = k.attnum::smallint \
            ), ARRAY[]::text[]) AS sub_columns \
        FROM tree t \
        JOIN pg_class c  ON c.oid = t.child_oid \
        JOIN pg_class pc ON pc.oid = t.parent_oid \
        LEFT JOIN pg_partitioned_table pt ON pt.partrelid = c.oid \
        ORDER BY t.depth, c.relname";
    match client.select(
        sql,
        None,
        &[unsafe { DatumWithOid::new(root.to_string(), PgBuiltInOids::TEXTOID.oid().value()) }],
    ) {
        Ok(iter) => iter
            .filter_map(|row| {
                let bare = row.get_by_name::<&str, _>("bare_name").ok()??.to_string();
                let parent = row.get_by_name::<&str, _>("parent_bare").ok()??.to_string();
                let bound = row
                    .get_by_name::<&str, _>("bound_expr")
                    .ok()
                    .flatten()
                    .unwrap_or("")
                    .to_string();
                let sub_strategy = row
                    .get_by_name::<&str, _>("sub_strategy")
                    .ok()
                    .flatten()
                    .map(|s| s.to_string());
                let sub_columns: Vec<String> = row
                    .get_by_name::<Vec<String>, _>("sub_columns")
                    .ok()
                    .flatten()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|c| c.to_lowercase())
                    .collect();
                // HASH sub-partitions are unsupported; surface as a leaf-less
                // error path by skipping (validation in Task 1.4 rejects at create).
                Some(PartitionNode {
                    bare_name: bare,
                    parent_bare: parent,
                    bound_expr: bound,
                    sub_strategy: sub_strategy.filter(|s| s == "LIST" || s == "RANGE"),
                    sub_columns,
                })
            })
            .collect(),
        Err(e) => {
            pgrx::warning!("pg_reflex: list_partition_tree('{}') SPI error: {}", root, e);
            Vec::new()
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo pgrx test pg18 pg_subpart_tree_walk_lists_all_levels`
Expected: PASS (count = 3).

- [ ] **Step 5: Commit**

```bash
git add src/partition.rs src/tests/pg_test_subpartition.rs src/lib.rs
git commit -m "feat(partition): recursive list_partition_tree for multi-level sources"
```

## Task 1.2: `build_partition_node_ddl_pair` (tree-aware codegen)

**Files:**
- Modify: `src/partition.rs` (after `build_partition_child_ddl_pair`, ~line 225)
- Test: `src/tests/unit_partition.rs`

- [ ] **Step 1: Write the failing unit tests**

Append to `src/tests/unit_partition.rs`:

```rust
#[test]
fn test_node_ddl_internal_node_has_sub_partition_by() {
    let node = PartitionNode {
        bare_name: "ss_172".to_string(),
        parent_bare: "ss".to_string(), // == anchor root
        bound_expr: "FOR VALUES IN ('172')".to_string(),
        sub_strategy: Some("RANGE".to_string()),
        sub_columns: vec!["order_date".to_string()],
    };
    let (_int, tgt) = build_partition_node_ddl_pair("fcst", &node, "ss", true);
    assert_eq!(
        tgt,
        r#"CREATE TABLE IF NOT EXISTS "public"."fcst_ss_172" PARTITION OF "public"."fcst" FOR VALUES IN ('172') PARTITION BY RANGE ("order_date")"#
    );
}

#[test]
fn test_node_ddl_leaf_under_internal_parent_is_unlogged() {
    let node = PartitionNode {
        bare_name: "ss_172_2025_01".to_string(),
        parent_bare: "ss_172".to_string(), // not the root → parent is an IMV child
        bound_expr: "FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')".to_string(),
        sub_strategy: None,
        sub_columns: vec![],
    };
    let (_int, tgt) = build_partition_node_ddl_pair("fcst", &node, "ss", true);
    assert_eq!(
        tgt,
        r#"CREATE UNLOGGED TABLE IF NOT EXISTS "public"."fcst_ss_172_2025_01" PARTITION OF "public"."fcst_ss_172" FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')"#
    );
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib test_node_ddl_`
Expected: FAIL — `build_partition_node_ddl_pair` not found.

- [ ] **Step 3: Implement**

In `src/partition.rs`, after `build_partition_child_ddl_pair`:

```rust
/// Tree-aware DDL pair builder.  Unlike `build_partition_child_ddl_pair`
/// (which always attaches to the IMV root), this resolves the parent from
/// `node.parent_bare`: when it equals `anchor_root_bare` the parent is the
/// IMV root, otherwise it is the IMV child mirroring that source node.
/// Internal nodes (own partition strategy) get a `PARTITION BY` suffix and
/// are always LOGGED; only leaves honour `unlogged`.
pub(crate) fn build_partition_node_ddl_pair(
    view_name: &str,
    node: &PartitionNode,
    anchor_root_bare: &str,
    unlogged: bool,
) -> (String, String) {
    let int_parent = if node.parent_bare == anchor_root_bare {
        intermediate_table_name(view_name)
    } else {
        schema_prefix(view_name, &intermediate_child_name(view_name, &node.parent_bare))
    };
    let tgt_parent = if node.parent_bare == anchor_root_bare {
        quote_identifier(view_name)
    } else {
        schema_prefix(view_name, &target_child_name(view_name, &node.parent_bare))
    };
    let int_child = schema_prefix(view_name, &intermediate_child_name(view_name, &node.bare_name));
    let tgt_child = schema_prefix(view_name, &target_child_name(view_name, &node.bare_name));

    let sub_clause = match &node.sub_strategy {
        Some(strat) if !node.sub_columns.is_empty() => {
            format!(" {}", build_partition_by_clause(strat, &node.sub_columns))
        }
        _ => String::new(),
    };
    let is_leaf = node.sub_strategy.is_none();
    let create_kw = if unlogged && is_leaf {
        "CREATE UNLOGGED TABLE"
    } else {
        "CREATE TABLE"
    };

    let int_ddl = format!(
        "{} IF NOT EXISTS {} PARTITION OF {} {}{}",
        create_kw, int_child, int_parent, node.bound_expr, sub_clause
    );
    let tgt_ddl = format!(
        "{} IF NOT EXISTS {} PARTITION OF {} {}{}",
        create_kw, tgt_child, tgt_parent, node.bound_expr, sub_clause
    );
    (int_ddl, tgt_ddl)
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib test_node_ddl_`
Expected: PASS (both).

- [ ] **Step 5: Commit**

```bash
git add src/partition.rs src/tests/unit_partition.rs
git commit -m "feat(partition): tree-aware build_partition_node_ddl_pair with sub-PARTITION BY"
```

## Task 1.3: Wire tree walk into create-time mirroring

**Files:**
- Modify: `src/create_ivm/mod.rs` (the two mirror loops at ~796–805 and ~950–966)
- Test: `src/tests/pg_test_subpartition.rs`

- [ ] **Step 1: Write the failing integration test**

Append to `src/tests/pg_test_subpartition.rs`:

```rust
#[pg_test]
fn pg_subpart_create_mirrors_full_tree() {
    Spi::run(
        "CREATE TABLE ss2 (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, \
         product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)",
    )
    .expect("root");
    Spi::run("CREATE TABLE ss2_172 PARTITION OF ss2 FOR VALUES IN (172) PARTITION BY RANGE (order_date)")
        .expect("list child");
    Spi::run("CREATE TABLE ss2_172_2025_01 PARTITION OF ss2_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')")
        .expect("leaf");
    Spi::run(
        "INSERT INTO ss2 (dem_plan_id, order_date, product_id, qty) \
         VALUES (172, '2025-01-15', 5, 10)",
    )
    .expect("seed");

    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst2', \
            'SELECT dem_plan_id, order_date, product_id, qty FROM ss2', \
            NULL, NULL, ARRAY['dem_plan_id','product_id','order_date'], NULL, \
            ARRAY['dem_plan_id'])",
    )
    .expect("create call")
    .expect("create result");
    assert!(!r.starts_with("ERROR"), "create returned: {r}");

    // Target tree: internal fcst2_ss2_172 must itself be partitioned (RANGE).
    let sub_strat = Spi::get_one::<String>(
        "SELECT pt.partstrat::text FROM pg_partitioned_table pt \
         JOIN pg_class c ON c.oid = pt.partrelid WHERE c.relname = 'fcst2_ss2_172'",
    )
    .expect("sub strat query")
    .expect("sub strat");
    assert_eq!(sub_strat, "r", "fcst2_ss2_172 should be RANGE sub-partitioned");

    // Leaf mirror exists and holds the seeded row.
    let leaf_qty = Spi::get_one::<i32>(
        "SELECT qty FROM fcst2_ss2_172_2025_01 WHERE product_id = 5",
    )
    .expect("leaf query")
    .expect("qty");
    assert_eq!(leaf_qty, 10);
}
```

(`create_reflex_ivm` arg order matches `pg_test_partition.rs`: `name, query, ..., unique_key, ..., partition_by`. Verify the exact positional signature with `rg -n "fn create_reflex_ivm" src/lib.rs` and adjust `NULL` placeholders to match arity.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo pgrx test pg18 pg_subpart_create_mirrors_full_tree`
Expected: FAIL — `fcst2_ss2_172` is not sub-partitioned (single-level loop only created flat children), or the leaf table does not exist.

- [ ] **Step 3: Replace both single-level mirror loops with the tree walk**

In `src/create_ivm/mod.rs`, the passthrough loop (~796–805) currently:

```rust
let src_children = crate::partition::list_partition_children(client, &anchor);
for src_child in &src_children {
    let (_, tgt_ddl) = crate::partition::build_partition_child_ddl_pair(
        ctx.view_name, src_child, !ctx.logged,
    );
    client.update(&tgt_ddl, None, &[]).unwrap_or_report();
}
```

Replace with (target-only; tree is already top-down so parents precede children):

```rust
let (_, anchor_root_bare) = crate::query_decomposer::split_qualified_name(&anchor);
let nodes = crate::partition::list_partition_tree(client, &anchor);
for node in &nodes {
    let (_, tgt_ddl) = crate::partition::build_partition_node_ddl_pair(
        ctx.view_name, node, anchor_root_bare, !ctx.logged,
    );
    client.update(&tgt_ddl, None, &[]).unwrap_or_report();
}
```

In the aggregate loop (~950–966) replace the `for src_child` body identically but run **both** DDLs:

```rust
let (_, anchor_root_bare) = crate::query_decomposer::split_qualified_name(&anchor);
let nodes = crate::partition::list_partition_tree(client, &anchor);
info!(
    "pg_reflex: creating {} partition nodes for '{}' (anchor='{}')",
    nodes.len(), ctx.view_name, anchor
);
for node in &nodes {
    let (int_ddl, tgt_ddl) = crate::partition::build_partition_node_ddl_pair(
        ctx.view_name, node, anchor_root_bare, !ctx.logged,
    );
    client.update(&int_ddl, None, &[]).unwrap_or_report();
    client.update(&tgt_ddl, None, &[]).unwrap_or_report();
}
```

(`split_qualified_name` is already imported in `partition.rs`; confirm it is re-exported from `query_decomposer` and import it in `mod.rs` if not: `rg -n "split_qualified_name" src/create_ivm/mod.rs`.)

- [ ] **Step 4: Run to verify it passes**

Run: `cargo pgrx test pg18 pg_subpart_create_mirrors_full_tree`
Expected: PASS. Also run the existing single-level test to confirm no regression:
`cargo pgrx test pg18 pg_part_aggregate_explicit_list_partition` → PASS.

- [ ] **Step 5: Commit**

```bash
git add src/create_ivm/mod.rs src/tests/pg_test_subpartition.rs
git commit -m "feat(create_ivm): mirror full source partition tree (multi-level)"
```

## Task 1.4: All-levels bare-column validation

**Files:**
- Modify: `src/create_ivm/mod.rs` (partition-validation block; find via `rg -n "partitioned but not on|resolved_partition_cols" src/create_ivm/mod.rs`)
- Test: `src/tests/pg_test_subpartition.rs`

- [ ] **Step 1: Write the failing integration test**

```rust
#[pg_test]
fn pg_subpart_rejects_sublevel_column_not_in_unique_key() {
    Spi::run(
        "CREATE TABLE ss3 (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, qty INT) \
         PARTITION BY LIST (dem_plan_id)",
    )
    .expect("root");
    Spi::run("CREATE TABLE ss3_172 PARTITION OF ss3 FOR VALUES IN (172) PARTITION BY RANGE (order_date)")
        .expect("list child");
    Spi::run("CREATE TABLE ss3_172_2025_01 PARTITION OF ss3_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')")
        .expect("leaf");

    // unique_key omits order_date (a sub-level partition key) → must be rejected.
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst3', \
            'SELECT dem_plan_id, qty FROM ss3', \
            NULL, NULL, ARRAY['dem_plan_id'], NULL, ARRAY['dem_plan_id'])",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        r.starts_with("ERROR") && r.contains("order_date"),
        "expected rejection naming order_date, got: {r}"
    );
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo pgrx test pg18 pg_subpart_rejects_sublevel_column_not_in_unique_key`
Expected: FAIL — IMV is created (no validation of sub-level columns yet) so `r` does not start with `ERROR`.

- [ ] **Step 3: Implement the all-levels check**

In the partition-resolution block of `src/create_ivm/mod.rs` (after the anchor is resolved and before mirroring), gather every partition-key column across all levels and require each to be a bare projected output column present in the IMV's unique key / GROUP BY set. Add:

```rust
// All-levels validation: every partition key column at every level must be a
// bare projected output column carried in the IMV's stable key. Required for
// PG's unique-index rule and for swap-fill constraint substitution to resolve.
{
    let nodes = crate::partition::list_partition_tree(client, &anchor);
    let mut all_part_cols: std::collections::BTreeSet<String> =
        ctx.resolved_partition_cols.iter().map(|c| c.to_lowercase()).collect();
    for node in &nodes {
        for c in &node.sub_columns {
            all_part_cols.insert(c.to_lowercase());
        }
    }
    // `stable_key_cols` = the IMV's unique-key columns (aggregate: GROUP BY).
    // Find the existing variable holding these; in the partitioned branch the
    // single-level check already references the projected column set. Reuse it.
    for col in &all_part_cols {
        let present = ctx
            .output_columns // confirm the actual field name via rg; see note
            .iter()
            .any(|oc| oc.eq_ignore_ascii_case(col));
        if !present {
            return Box::leak(
                format!(
                    "ERROR: partition key column '{}' (a partition level of source '{}') \
                     is not a bare projected output column in the IMV's key. Add it to \
                     the SELECT list and unique_key/GROUP BY.",
                    col, anchor
                )
                .into_boxed_str(),
            );
        }
    }
}
```

Implementation note: the exact field names (`ctx.output_columns`, `ctx.resolved_partition_cols`) must be confirmed against the struct. Run `rg -n "resolved_partition_cols|output_columns|stable_key|unique_key" src/create_ivm/mod.rs` and bind to the real fields. The single-level validation already in this file (error string `"partitioned but not on"`) shows the established pattern and the variable that holds the projected/key columns — extend that same comparison set to `all_part_cols`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo pgrx test pg18 pg_subpart_rejects_sublevel_column_not_in_unique_key`
Expected: PASS. Re-run `pg_subpart_create_mirrors_full_tree` (which *includes* `order_date` in unique_key) → still PASS.

- [ ] **Step 5: Commit**

```bash
git add src/create_ivm/mod.rs src/tests/pg_test_subpartition.rs
git commit -m "feat(create_ivm): validate all partition levels are bare key columns"
```

## Task 1.5: Phase 1 gate

- [ ] **Step 1:** Run `cargo fmt`
- [ ] **Step 2:** Run `cargo clippy --all-targets -- -D warnings` → no warnings
- [ ] **Step 3:** Run `cargo pgrx check`
- [ ] **Step 4:** Run `cargo pgrx test pg18 pg_subpart pg_part` (mirror + lifecycle + existing partition tests) → all PASS
- [ ] **Step 5:** Commit any fmt/clippy fixes: `git commit -am "chore: Phase 1 fmt/clippy"`

---

# Phase 2 — Level-agnostic reconcile + recursive sync

Goal: `reflex_reconcile_partition(view, source_partition := '<any-level>')` swaps the right IMV leaves; `reflex_sync_partitions` creates/drops IMV nodes recursively to track source-tree changes.

## Task 2.1: Recursive `reflex_sync_partitions_impl`

**Files:**
- Modify: `src/partition.rs` (`reflex_sync_partitions_impl`, lines 652–826)
- Test: `src/tests/pg_test_subpartition.rs`

- [ ] **Step 1: Write the failing integration test**

```rust
#[pg_test]
fn pg_subpart_sync_creates_new_leaf_and_drops_orphan() {
    Spi::run("CREATE TABLE ss4 (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE ss4_172 PARTITION OF ss4 FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE ss4_172_2025_01 PARTITION OF ss4_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("leaf1");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst4', 'SELECT dem_plan_id, order_date, product_id, qty FROM ss4', \
         NULL, NULL, ARRAY['dem_plan_id','product_id','order_date'], NULL, ARRAY['dem_plan_id'])",
    ).expect("c").expect("c");

    // Attach a brand-new month leaf on the source, then sync.
    Spi::run("CREATE TABLE ss4_172_2025_02 PARTITION OF ss4_172 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("leaf2");
    let _ = Spi::get_one::<String>("SELECT reflex_sync_partitions('fcst4', TRUE)").expect("sync").expect("sync");

    let exists = Spi::get_one::<bool>(
        "SELECT to_regclass('public.fcst4_ss4_172_2025_02') IS NOT NULL",
    ).expect("q").expect("b");
    assert!(exists, "new month leaf should be mirrored after sync");

    // Drop a source leaf, sync with drop_orphans → IMV leaf dropped.
    Spi::run("DROP TABLE ss4_172_2025_01").expect("drop source leaf");
    let _ = Spi::get_one::<String>("SELECT reflex_sync_partitions('fcst4', TRUE)").expect("sync2").expect("sync2");
    let gone = Spi::get_one::<bool>(
        "SELECT to_regclass('public.fcst4_ss4_172_2025_01') IS NULL",
    ).expect("q2").expect("b2");
    assert!(gone, "orphan IMV leaf should be dropped after sync");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo pgrx test pg18 pg_subpart_sync_creates_new_leaf_and_drops_orphan`
Expected: FAIL — current sync walks only `list_partition_children` (one level), so the nested `_2025_02` leaf is never created.

- [ ] **Step 3: Make sync recursive**

In `reflex_sync_partitions_impl` replace the `src_children = list_partition_children(client, &anchor)` and its expected-set construction with the tree walk, and create/drop by node. The create loop:

```rust
let (_, anchor_root_bare) = split_qualified_name(&anchor);
let nodes = list_partition_tree(client, &anchor);

// Expected IMV-side bare names (target + intermediate) across the whole tree.
let src_expected_int: std::collections::HashSet<String> = nodes
    .iter()
    .map(|n| intermediate_child_name(view_name, &n.bare_name))
    .collect();
let src_expected_tgt: std::collections::HashSet<String> = nodes
    .iter()
    .map(|n| target_child_name(view_name, &n.bare_name))
    .collect();

// Create missing nodes top-down (nodes is already top-down ordered).
for node in &nodes {
    let int_name = intermediate_child_name(view_name, &node.bare_name);
    let tgt_name = target_child_name(view_name, &node.bare_name);
    let (int_ddl, tgt_ddl) =
        build_partition_node_ddl_pair(view_name, node, anchor_root_bare, unlogged);
    if has_intermediate && !int_have.contains(&int_name) {
        client.update(&int_ddl, None, &[])
            .map_err(|e| format!("sync: create intermediate node: {}", e))?;
        out.added_intermediate += 1;
    }
    if !tgt_have.contains(&tgt_name) {
        client.update(&tgt_ddl, None, &[])
            .map_err(|e| format!("sync: create target node: {}", e))?;
        out.added_target += 1;
    }
}
```

The existing `int_have`/`tgt_have` are built from single-level `list_partition_children` of the IMV parents — replace those with recursive `list_partition_tree(client, &int_parent)` / `list_partition_tree(client, &tgt_parent)` so they enumerate the whole IMV tree:

```rust
let int_children = if has_intermediate { list_partition_tree(client, &int_parent) } else { Vec::new() };
let tgt_children = list_partition_tree(client, &tgt_parent);
let int_have: std::collections::HashSet<String> =
    int_children.iter().map(|c| c.bare_name.clone()).collect();
let tgt_have: std::collections::HashSet<String> =
    tgt_children.iter().map(|c| c.bare_name.clone()).collect();
```

The orphan-drop loops already iterate `int_children`/`tgt_children` and `DROP … CASCADE` — they now operate over the full tree. Drop **bottom-up**: iterate `int_children`/`tgt_children` in reverse (children before parents) so `CASCADE` is not relied on across levels:

```rust
for c in int_children.iter().rev() { /* existing orphan check + DROP CASCADE */ }
for c in tgt_children.iter().rev() { /* existing orphan check + DROP CASCADE */ }
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo pgrx test pg18 pg_subpart_sync_creates_new_leaf_and_drops_orphan`
Expected: PASS. Re-run `pg_part` single-level sync tests → PASS.

- [ ] **Step 5: Commit**

```bash
git add src/partition.rs src/tests/pg_test_subpartition.rs
git commit -m "feat(partition): recursive reflex_sync_partitions over full tree"
```

## Task 2.2: `source_partition` arg + leaf-expansion resolution

**Files:**
- Modify: `src/lib.rs` (`reflex_reconcile_partition` extern, line 437)
- Modify: `src/partition.rs` (`reflex_reconcile_partition_impl`, add `source_partition` param + leaf expansion)
- Test: `src/tests/pg_test_subpartition.rs`

- [ ] **Step 1: Write the failing integration test**

```rust
#[pg_test]
fn pg_subpart_reconcile_leaf_swaps_only_that_leaf() {
    Spi::run("CREATE TABLE ss5 (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE ss5_172 PARTITION OF ss5 FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE ss5_172_2025_01 PARTITION OF ss5_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("leaf1");
    Spi::run("CREATE TABLE ss5_172_2025_02 PARTITION OF ss5_172 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("leaf2");
    Spi::run("INSERT INTO ss5 VALUES (172,'2025-01-15',5,10),(172,'2025-02-15',5,20)").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst5','SELECT dem_plan_id, order_date, product_id, qty FROM ss5', \
         NULL, NULL, ARRAY['dem_plan_id','product_id','order_date'], NULL, ARRAY['dem_plan_id'])",
    ).expect("c").expect("c");

    // Simulate a swap on the Jan leaf: change Jan data directly on the source
    // leaf (stand-in for detach/attach), then reconcile just that source leaf.
    Spi::run("UPDATE ss5_172_2025_01 SET qty = 999 WHERE product_id = 5").expect("mutate jan");
    let r = Spi::get_one::<String>(
        "SELECT reflex_reconcile_partition('fcst5', '', 'ss5_172_2025_01')",
    ).expect("reconcile").expect("reconcile");
    assert!(!r.starts_with("ERROR"), "reconcile: {r}");

    let jan = Spi::get_one::<i32>("SELECT qty FROM fcst5 WHERE order_date = '2025-01-15'").expect("q").expect("jan");
    let feb = Spi::get_one::<i32>("SELECT qty FROM fcst5 WHERE order_date = '2025-02-15'").expect("q").expect("feb");
    assert_eq!(jan, 999, "Jan leaf reconciled");
    assert_eq!(feb, 20, "Feb leaf untouched");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo pgrx test pg18 pg_subpart_reconcile_leaf_swaps_only_that_leaf`
Expected: FAIL — `reflex_reconcile_partition` currently takes only 2 args; the 3-arg call fails to resolve / function signature mismatch.

- [ ] **Step 3: Add the `source_partition` arg and leaf-expansion**

In `src/lib.rs`, change the extern (line ~437) to add a defaulted third arg:

```rust
#[pg_extern]
fn reflex_reconcile_partition(
    view_name: &str,
    partition_keys: &str,
    source_partition: default!(&str, "''"),
) -> String {
    crate::partition::reflex_reconcile_partition_impl(view_name, partition_keys, source_partition)
}
```

In `src/partition.rs`, change `reflex_reconcile_partition_impl` to accept `source_partition: &str`. Before the existing key-matching block, when `source_partition` is non-empty, resolve it to the set of source **leaf** bare-names under it and map to the IMV child bare-names to process. Add this resolution helper:

```rust
/// Expand a source partition (any level, bare or qualified) to the set of its
/// leaf bare-names. A leaf expands to itself. Empty when the relation is not a
/// partition of a tracked tree.
fn expand_source_partition_to_leaves(
    client: &pgrx::spi::SpiClient<'_>,
    source_partition: &str,
) -> Vec<String> {
    let (_, bare) = split_qualified_name(source_partition);
    let subtree = list_partition_tree(client, source_partition);
    if subtree.is_empty() {
        // Leaf (or non-partitioned): itself.
        vec![bare.to_string()]
    } else {
        subtree
            .into_iter()
            .filter(|n| n.sub_strategy.is_none())
            .map(|n| n.bare_name)
            .collect()
    }
}
```

In `reflex_reconcile_partition_impl`, after loading `part_cols`/`tgt_parent` and before the `keys`-matching loop, branch:

```rust
let to_process: std::collections::HashSet<String> = if !source_partition.trim().is_empty() {
    // Level-agnostic path: map source leaves → IMV target child bare-names.
    expand_source_partition_to_leaves(client, source_partition)
        .into_iter()
        .map(|src_leaf| target_child_name(view_name, &src_leaf))
        .collect()
} else {
    // Existing CSV-of-LIST-keys path, but each matched LIST child must expand
    // to its leaves (the source may be sub-partitioned).
    let mut acc: std::collections::HashSet<String> = std::collections::HashSet::new();
    // ... existing per-key constraint-match loop produces matched source child
    //     bare-names; for each, expand to leaves and map to target child names.
    acc
};
```

For the CSV path leaf-expansion: after the existing loop finds a matching child (currently a target child bare-name `c.bare_name`), recover the source child bare-name (strip the `"<view_bare>_"` prefix as done at line ~1001), expand it to source leaves, and insert `target_child_name(view_name, &leaf)` for each. This makes the legacy `'172'` call swap all 172 month-leaves.

The downstream loop that calls `execute_partition_swap_for_child` already iterates `to_process` (target child bare-names) and recovers the source child bare-name — it now naturally receives leaf names and swaps each leaf. **No change** to `execute_partition_swap_for_child`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo pgrx test pg18 pg_subpart_reconcile_leaf_swaps_only_that_leaf`
Expected: PASS. Re-run the existing `pg_part` reconcile test (the legacy 2-arg form still resolves via the defaulted third arg) → PASS.

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs src/partition.rs src/tests/pg_test_subpartition.rs
git commit -m "feat(partition): level-agnostic reconcile via source_partition arg + leaf expansion"
```

## Task 2.3: Whole-`dem_plan_id` (internal-node) reconcile

**Files:**
- Test only: `src/tests/pg_test_subpartition.rs` (resolution already implemented in 2.2)

- [ ] **Step 1: Write the test**

```rust
#[pg_test]
fn pg_subpart_reconcile_internal_node_swaps_all_leaves() {
    Spi::run("CREATE TABLE ss6 (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE ss6_172 PARTITION OF ss6 FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE ss6_172_2025_01 PARTITION OF ss6_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("l1");
    Spi::run("CREATE TABLE ss6_172_2025_02 PARTITION OF ss6_172 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("l2");
    Spi::run("INSERT INTO ss6 VALUES (172,'2025-01-15',5,10),(172,'2025-02-15',5,20)").expect("seed");
    Spi::get_one::<String>("SELECT create_reflex_ivm('fcst6','SELECT dem_plan_id, order_date, product_id, qty FROM ss6', NULL, NULL, ARRAY['dem_plan_id','product_id','order_date'], NULL, ARRAY['dem_plan_id'])").expect("c").expect("c");

    Spi::run("UPDATE ss6_172_2025_01 SET qty = 111").expect("m1");
    Spi::run("UPDATE ss6_172_2025_02 SET qty = 222").expect("m2");
    // Reconcile the whole dem_plan_id internal node by source name.
    let r = Spi::get_one::<String>("SELECT reflex_reconcile_partition('fcst6', '', 'ss6_172')").expect("rec").expect("rec");
    assert!(!r.starts_with("ERROR"), "{r}");
    let jan = Spi::get_one::<i32>("SELECT qty FROM fcst6 WHERE order_date='2025-01-15'").expect("q").expect("j");
    let feb = Spi::get_one::<i32>("SELECT qty FROM fcst6 WHERE order_date='2025-02-15'").expect("q").expect("f");
    assert_eq!((jan, feb), (111, 222));
}
```

- [ ] **Step 2: Run** → Expected: PASS (resolution from Task 2.2 expands `ss6_172` to both leaves).
- [ ] **Step 3:** No implementation (covered by 2.2). If it fails, the bug is in `expand_source_partition_to_leaves` — fix there, not in a new path.
- [ ] **Step 4: Commit**

```bash
git add src/tests/pg_test_subpartition.rs
git commit -m "test(partition): internal-node reconcile expands to all leaves"
```

## Task 2.4: Phase 2 gate

- [ ] `cargo fmt && cargo clippy --all-targets -- -D warnings`
- [ ] `cargo pgrx check`
- [ ] `cargo pgrx test pg18 pg_subpart pg_part` → all PASS
- [ ] `git commit -am "chore: Phase 2 fmt/clippy"`

---

# Phase 3 — Event-trigger enqueue + flush + snapshot oid-diff

Goal: a source `ATTACH`/`DETACH` automatically enqueues the source root; `reflex_flush_partitions()` resolves swap/attach/detach by oid-diff and reconciles, ignoring pg_reflex's own IMV swaps.

## Task 3.1: Catalog tables (snapshot + pending)

**Files:**
- Modify: `src/lib.rs` (the `extension_sql!` "pg_reflex_init" block with the `ALTER TABLE … ADD COLUMN IF NOT EXISTS` statements, ~line 80–160)
- Test: `src/tests/pg_test_subpartition.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[pg_test]
fn pg_subpart_catalog_tables_exist() {
    let snap = Spi::get_one::<bool>("SELECT to_regclass('public.__reflex_source_partition_snapshot') IS NOT NULL").expect("q").expect("b");
    let pend = Spi::get_one::<bool>("SELECT to_regclass('public.__reflex_partition_pending') IS NOT NULL").expect("q").expect("b");
    assert!(snap && pend, "snapshot={snap} pending={pend}");
}
```

- [ ] **Step 2: Run** → FAIL (tables absent).

- [ ] **Step 3: Add the tables** to the init `extension_sql!` block (after the existing `CREATE TABLE … __reflex_ivm_reference` / index):

```sql
CREATE TABLE IF NOT EXISTS public.__reflex_source_partition_snapshot (
    source_root TEXT NOT NULL,
    child_name  TEXT NOT NULL,
    child_oid   OID  NOT NULL,
    bound       TEXT,
    PRIMARY KEY (source_root, child_name)
);

CREATE TABLE IF NOT EXISTS public.__reflex_partition_pending (
    source_root TEXT NOT NULL,
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_root)
);
```

- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit**

```bash
git add src/lib.rs src/tests/pg_test_subpartition.rs
git commit -m "feat: __reflex_source_partition_snapshot + __reflex_partition_pending catalog tables"
```

## Task 3.2: oid-diff classifier (pure)

**Files:**
- Modify: `src/partition.rs`
- Test: `src/tests/unit_partition.rs`

- [ ] **Step 1: Write the failing unit test**

```rust
#[test]
fn test_classify_partition_diff() {
    let snapshot = vec![
        ("c_jan".to_string(), 100u32),
        ("c_feb".to_string(), 200u32),
        ("c_mar".to_string(), 300u32),
    ];
    let current = vec![
        ("c_jan".to_string(), 100u32),  // unchanged
        ("c_feb".to_string(), 999u32),  // oid changed → swap
        ("c_apr".to_string(), 400u32),  // new → attach
        // c_mar gone → drop
    ];
    let mut got = classify_partition_diff(&snapshot, &current);
    got.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        got,
        vec![
            ("c_apr".to_string(), PartitionDiffAction::AttachNew),
            ("c_feb".to_string(), PartitionDiffAction::SwapFill),
            ("c_mar".to_string(), PartitionDiffAction::Drop),
        ]
    );
}
```

- [ ] **Step 2: Run** → `cargo test --lib test_classify_partition_diff` → FAIL (undefined).

- [ ] **Step 3: Implement** in `src/partition.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PartitionDiffAction {
    SwapFill,
    AttachNew,
    Drop,
}

/// Diff a stored snapshot of (child_name, oid) against the current leaf set.
/// Same name + changed oid = same-bound swap (detach+attach). New name =
/// attach. Missing name = detach/remove. Unchanged names are omitted.
pub(crate) fn classify_partition_diff(
    snapshot: &[(String, u32)],
    current: &[(String, u32)],
) -> Vec<(String, PartitionDiffAction)> {
    use std::collections::HashMap;
    let snap: HashMap<&str, u32> = snapshot.iter().map(|(n, o)| (n.as_str(), *o)).collect();
    let cur: HashMap<&str, u32> = current.iter().map(|(n, o)| (n.as_str(), *o)).collect();
    let mut out = Vec::new();
    for (name, oid) in current {
        match snap.get(name.as_str()) {
            None => out.push((name.clone(), PartitionDiffAction::AttachNew)),
            Some(&snap_oid) if snap_oid != *oid => {
                out.push((name.clone(), PartitionDiffAction::SwapFill))
            }
            Some(_) => {} // unchanged
        }
    }
    for (name, _) in snapshot {
        if !cur.contains_key(name.as_str()) {
            out.push((name.clone(), PartitionDiffAction::Drop));
        }
    }
    out
}
```

- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit**

```bash
git add src/partition.rs src/tests/unit_partition.rs
git commit -m "feat(partition): oid-diff classifier for swap/attach/detach resolution"
```

## Task 3.3: Snapshot read/refresh helpers + seed at create

**Files:**
- Modify: `src/partition.rs` (helpers), `src/create_ivm/mod.rs` (seed after mirroring)
- Test: `src/tests/pg_test_subpartition.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[pg_test]
fn pg_subpart_snapshot_seeded_at_create() {
    Spi::run("CREATE TABLE ss7 (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE ss7_172 PARTITION OF ss7 FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE ss7_172_2025_01 PARTITION OF ss7_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("leaf");
    Spi::get_one::<String>("SELECT create_reflex_ivm('fcst7','SELECT dem_plan_id, order_date, product_id, qty FROM ss7', NULL, NULL, ARRAY['dem_plan_id','product_id','order_date'], NULL, ARRAY['dem_plan_id'])").expect("c").expect("c");

    // Snapshot holds the source's single leaf.
    let cnt = Spi::get_one::<i64>(
        "SELECT count(*) FROM public.__reflex_source_partition_snapshot \
         WHERE child_name = 'ss7_172_2025_01'",
    ).expect("q").expect("c");
    assert_eq!(cnt, 1);
}
```

- [ ] **Step 2: Run** → FAIL (snapshot not seeded).

- [ ] **Step 3: Implement helpers + seed**

In `src/partition.rs`:

```rust
/// Current (child_name, oid) leaf set of `source_root`, for snapshot diffing.
pub(crate) fn current_source_leaf_oids(
    client: &pgrx::spi::SpiClient<'_>,
    source_root: &str,
) -> Vec<(String, u32)> {
    list_partition_tree(client, source_root)
        .into_iter()
        .filter(|n| n.sub_strategy.is_none())
        .filter_map(|n| {
            let q = "SELECT c.oid::oid::int8 AS oid FROM pg_class c WHERE c.relname = $1";
            let oid: Option<i64> = client
                .select(q, Some(1), &[unsafe {
                    DatumWithOid::new(n.bare_name.clone(), PgBuiltInOids::TEXTOID.oid().value())
                }])
                .ok()
                .and_then(|mut it| it.next())
                .and_then(|r| r.get_by_name::<i64, _>("oid").ok().flatten());
            oid.map(|o| (n.bare_name, o as u32))
        })
        .collect()
}

/// Replace the snapshot rows for `source_root` with the current leaf set.
pub(crate) fn refresh_source_snapshot(
    client: &mut pgrx::spi::SpiClient<'_>,
    source_root: &str,
) {
    let _ = client.update(
        "DELETE FROM public.__reflex_source_partition_snapshot WHERE source_root = $1",
        None,
        &[unsafe { DatumWithOid::new(source_root.to_string(), PgBuiltInOids::TEXTOID.oid().value()) }],
    );
    for (name, oid) in current_source_leaf_oids(client, source_root) {
        let _ = client.update(
            "INSERT INTO public.__reflex_source_partition_snapshot (source_root, child_name, child_oid, bound) \
             VALUES ($1, $2, $3, NULL) ON CONFLICT (source_root, child_name) DO UPDATE SET child_oid = EXCLUDED.child_oid",
            None,
            &[
                unsafe { DatumWithOid::new(source_root.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(name, PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(oid as i64, PgBuiltInOids::INT8OID.oid().value()) },
            ],
        );
    }
}
```

(The `child_oid OID` column accepts an int8 bind cast; if pgrx rejects the cast, store as `INT8` — change the column type in Task 3.1 to `BIGINT` and the test accordingly. Confirm during Step 4.)

In `src/create_ivm/mod.rs`, after the mirror loop completes for a partitioned IMV, seed the snapshot:

```rust
crate::partition::refresh_source_snapshot(client, &anchor);
```

(Ensure `client` here is `&mut`; the create path already uses `Spi::connect_mut`. If the surrounding closure holds `client` immutably, hoist the seed to a point where a mutable client is available, mirroring how mirroring DDL is run.)

- [ ] **Step 4: Run** → PASS. If OID-cast errors appear, switch the column to `BIGINT` (Task 3.1) and bind plain `i64`.
- [ ] **Step 5: Commit**

```bash
git add src/partition.rs src/create_ivm/mod.rs src/tests/pg_test_subpartition.rs
git commit -m "feat(partition): source-partition snapshot helpers + seed at create"
```

## Task 3.4: `reflex_flush_partitions` extern

**Files:**
- Modify: `src/lib.rs` (new `#[pg_extern]`), `src/partition.rs` (impl)
- Test: `src/tests/pg_test_subpartition.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[pg_test]
fn pg_subpart_flush_applies_attach_and_detach() {
    Spi::run("CREATE TABLE ss8 (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE ss8_172 PARTITION OF ss8 FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE ss8_172_2025_01 PARTITION OF ss8_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("l1");
    Spi::run("INSERT INTO ss8 VALUES (172,'2025-01-15',5,10)").expect("seed");
    Spi::get_one::<String>("SELECT create_reflex_ivm('fcst8','SELECT dem_plan_id, order_date, product_id, qty FROM ss8', NULL, NULL, ARRAY['dem_plan_id','product_id','order_date'], NULL, ARRAY['dem_plan_id'])").expect("c").expect("c");

    // Build a fresh Feb leaf as a standalone table and ATTACH it (a swap of a new partition).
    Spi::run("CREATE TABLE ss8_172_2025_02 (LIKE ss8 INCLUDING ALL)").expect("staging");
    Spi::run("INSERT INTO ss8_172_2025_02 VALUES (172,'2025-02-15',5,20)").expect("fill staging");
    Spi::run("ALTER TABLE ss8_172 ATTACH PARTITION ss8_172_2025_02 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("attach");

    // The event trigger enqueued ss8; flush should create+fill the Feb IMV leaf.
    let r = Spi::get_one::<String>("SELECT reflex_flush_partitions()").expect("flush").expect("flush");
    assert!(!r.starts_with("ERROR"), "flush: {r}");
    let feb = Spi::get_one::<i32>("SELECT qty FROM fcst8 WHERE order_date='2025-02-15'").expect("q").expect("feb");
    assert_eq!(feb, 20, "attached Feb leaf flushed into IMV");
}
```

- [ ] **Step 2: Run** → FAIL (`reflex_flush_partitions` undefined; even once defined, fails until the event trigger enqueues — Task 3.5. To isolate, this test may first need a manual `INSERT INTO __reflex_partition_pending VALUES ('public.ss8')` before the flush; add that line if Task 3.5 is not yet merged, then remove it once 3.5 lands. Keep the test asserting the flush behavior either way.)

- [ ] **Step 3: Implement the extern + impl**

In `src/lib.rs`:

```rust
/// Resolve all pending source-partition changes: oid-diff each dirty source
/// root against the snapshot, then swap-fill / create / drop the matching IMV
/// partitions and cascade to dependents. Call once after a batch of
/// DETACH/ATTACH swaps. Returns a summary string.
#[pg_extern]
fn reflex_flush_partitions() -> String {
    crate::partition::reflex_flush_partitions_impl(None)
}

/// Flush a single source root (skips the pending queue scan).
#[pg_extern]
fn reflex_flush_partition_source(source_root: &str) -> String {
    crate::partition::reflex_flush_partitions_impl(Some(source_root))
}
```

In `src/partition.rs`:

```rust
/// Flush pending partition changes. When `only` is Some, flush just that
/// source root; otherwise drain __reflex_partition_pending.
pub(crate) fn reflex_flush_partitions_impl(only: Option<&str>) -> String {
    let outcome: Result<String, String> = Spi::connect_mut(|client| {
        let roots: Vec<String> = match only {
            Some(r) => vec![r.to_string()],
            None => client
                .select("SELECT source_root FROM public.__reflex_partition_pending", None, &[])
                .map_err(|e| format!("flush: pending scan failed: {}", e))?
                .filter_map(|row| row.get_by_name::<&str, _>("source_root").ok().flatten().map(|s| s.to_string()))
                .collect(),
        };
        let mut summary: Vec<String> = Vec::new();
        for root in &roots {
            // For each IMV depending on this source root and partitioned,
            // diff + apply.
            let imvs: Vec<String> = client
                .select(
                    "SELECT name FROM public.__reflex_ivm_reference \
                     WHERE partition_columns IS NOT NULL AND array_length(partition_columns,1) > 0 \
                       AND (depends_on @> ARRAY[$1] OR depends_on @> ARRAY[split_part($1,'.',2)])",
                    None,
                    &[unsafe { DatumWithOid::new(root.to_string(), PgBuiltInOids::TEXTOID.oid().value()) }],
                )
                .map_err(|e| format!("flush: imv lookup failed: {}", e))?
                .filter_map(|r| r.get_by_name::<&str, _>("name").ok().flatten().map(|s| s.to_string()))
                .collect();

            // Snapshot diff is computed once per root (snapshot is per source_root).
            let snapshot: Vec<(String, u32)> = client
                .select(
                    "SELECT child_name, child_oid::int8 AS oid FROM public.__reflex_source_partition_snapshot WHERE source_root = $1",
                    None,
                    &[unsafe { DatumWithOid::new(root.to_string(), PgBuiltInOids::TEXTOID.oid().value()) }],
                )
                .map_err(|e| format!("flush: snapshot read failed: {}", e))?
                .filter_map(|r| {
                    let n = r.get_by_name::<&str, _>("child_name").ok().flatten()?.to_string();
                    let o = r.get_by_name::<i64, _>("oid").ok().flatten()? as u32;
                    Some((n, o))
                })
                .collect();
            let current = current_source_leaf_oids(client, root);
            let actions = classify_partition_diff(&snapshot, &current);

            for imv in &imvs {
                for (src_leaf, action) in &actions {
                    match action {
                        PartitionDiffAction::AttachNew | PartitionDiffAction::SwapFill => {
                            // sync (creates the IMV leaf for AttachNew) then swap-fill it.
                            let q = format!(
                                "SELECT public.reflex_reconcile_partition({}, '', {})",
                                sql_literal_text(imv),
                                sql_literal_text(src_leaf)
                            );
                            client.update(&q, None, &[]).map_err(|e| format!("flush reconcile {}: {}", src_leaf, e))?;
                        }
                        PartitionDiffAction::Drop => {
                            // Drop the matching IMV leaf (and intermediate leaf if any).
                            let (schema_opt, _) = split_qualified_name(imv);
                            let schema = schema_opt.unwrap_or("public");
                            let tgt = target_child_name(imv, src_leaf);
                            let int = intermediate_child_name(imv, src_leaf);
                            let _ = client.update(&format!("DROP TABLE IF EXISTS \"{}\".\"{}\" CASCADE", schema, tgt), None, &[]);
                            let _ = client.update(&format!("DROP TABLE IF EXISTS \"{}\".\"{}\" CASCADE", schema, int), None, &[]);
                        }
                    }
                }
                summary.push(format!("{}: {} change(s)", imv, actions.len()));
            }

            refresh_source_snapshot(client, root);
        }

        // Clear processed pending rows.
        match only {
            Some(r) => { let _ = client.update("DELETE FROM public.__reflex_partition_pending WHERE source_root = $1", None, &[unsafe { DatumWithOid::new(r.to_string(), PgBuiltInOids::TEXTOID.oid().value()) }]); }
            None => { let _ = client.update("TRUNCATE public.__reflex_partition_pending", None, &[]); }
        }

        Ok(if summary.is_empty() { "OK — nothing pending".to_string() } else { summary.join("; ") })
    });
    match outcome {
        Ok(s) => s,
        Err(e) => format!("ERROR: {}", e),
    }
}
```

Note: `reflex_reconcile_partition(imv, '', src_leaf)` already calls `reflex_sync_partitions` first (creating the IMV leaf for `AttachNew`) and then swap-fills — so `AttachNew` and `SwapFill` share one code path. Cascade to dependents is handled inside `reflex_reconcile_partition_impl`'s existing `graph_child` loop.

- [ ] **Step 4: Run** → PASS (with the temporary manual pending-insert if 3.5 not yet merged).
- [ ] **Step 5: Commit**

```bash
git add src/lib.rs src/partition.rs src/tests/pg_test_subpartition.rs
git commit -m "feat: reflex_flush_partitions resolves swap/attach/detach via snapshot oid-diff"
```

## Task 3.5: Extend event trigger to enqueue (and ignore reflex-owned swaps)

**Files:**
- Modify: `src/lib.rs` (`__reflex_on_ddl_command_end`, ~line 741–869)
- Test: `src/tests/pg_test_subpartition.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[pg_test]
fn pg_subpart_event_trigger_enqueues_source_not_reflex_owned() {
    Spi::run("CREATE TABLE ss9 (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE ss9_172 PARTITION OF ss9 FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE ss9_172_2025_01 PARTITION OF ss9_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("l1");
    Spi::get_one::<String>("SELECT create_reflex_ivm('fcst9','SELECT dem_plan_id, order_date, product_id, qty FROM ss9', NULL, NULL, ARRAY['dem_plan_id','product_id','order_date'], NULL, ARRAY['dem_plan_id'])").expect("c").expect("c");

    // Clear any enqueue from create-time DDL.
    Spi::run("TRUNCATE public.__reflex_partition_pending").expect("clear");

    // Attach a new source leaf → event trigger must enqueue 'public.ss9'.
    Spi::run("CREATE TABLE ss9_172_2025_02 PARTITION OF ss9_172 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("attach");
    let enq = Spi::get_one::<i64>(
        "SELECT count(*) FROM public.__reflex_partition_pending WHERE source_root = 'public.ss9'",
    ).expect("q").expect("c");
    assert_eq!(enq, 1, "source root should be enqueued");

    // pg_reflex's own IMV partition (fcst9_*) must NOT be enqueued.
    let bad = Spi::get_one::<i64>(
        "SELECT count(*) FROM public.__reflex_partition_pending WHERE source_root LIKE '%fcst9%'",
    ).expect("q").expect("c");
    assert_eq!(bad, 0, "reflex-owned tables must never be enqueued");
}
```

- [ ] **Step 2: Run** → FAIL (event trigger currently calls `reflex_sync_partitions` inline, does not enqueue).

- [ ] **Step 3: Add enqueue logic to `__reflex_on_ddl_command_end`**

Inside the existing `IF _parent IS NOT NULL THEN` block (where it currently loops IMVs and calls `reflex_sync_partitions`), **before** the per-IMV loop, enqueue the parent root once — guarded so reflex-owned tables are never enqueued:

```sql
-- Enqueue the source root for the next flush (data capture for swaps).
-- Guard: never enqueue pg_reflex-owned tables (our own Phase-A swap does
-- ATTACH/DETACH on IMV partitions; reacting to those would race the
-- code-driven graph_child cascade).
IF _parent NOT LIKE '%\_\_reflex\_%'  -- escape underscores
   AND NOT EXISTS (
        SELECT 1 FROM public.__reflex_ivm_reference r
        WHERE r.name = _parent OR r.name = split_part(_parent, '.', 2)
   )
   AND EXISTS (
        SELECT 1 FROM public.__reflex_ivm_reference r
        WHERE r.partition_columns IS NOT NULL
          AND array_length(r.partition_columns, 1) > 0
          AND (r.depends_on @> ARRAY[_parent]
               OR r.depends_on @> ARRAY[split_part(_parent, '.', 2)])
   )
THEN
    INSERT INTO public.__reflex_partition_pending (source_root)
    VALUES (_parent)
    ON CONFLICT (source_root) DO NOTHING;
END IF;
```

Decision: **keep** the existing inline `reflex_sync_partitions(_imv.name, FALSE)` call (it creates structural partitions harmlessly and is idempotent), OR remove it now that flush handles structure. Recommended: **remove** the inline sync loop to make flush the single source of truth and avoid double work — but only after confirming no existing test asserts on the inline-sync side effect. Check: `rg -n "auto-synced|reflex_sync_partitions" src/tests/`. If a test depends on inline sync, keep both; otherwise remove the inline loop body and rely on flush.

- [ ] **Step 4: Run** → `cargo pgrx test pg18 pg_subpart_event_trigger_enqueues_source_not_reflex_owned` → PASS. Then remove the temporary manual pending-insert from Task 3.4's test and re-run `pg_subpart_flush_applies_attach_and_detach` → PASS (end-to-end: attach → enqueue → flush → data).

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs src/tests/pg_test_subpartition.rs
git commit -m "feat: event trigger enqueues source root for partition flush (ignores reflex-owned)"
```

## Task 3.6: DETACH-removal drops the IMV leaf (end-to-end)

**Files:**
- Test only: `src/tests/pg_test_subpartition.rs`

- [ ] **Step 1: Write the test**

```rust
#[pg_test]
fn pg_subpart_detach_remove_drops_imv_leaf_via_flush() {
    Spi::run("CREATE TABLE ssa (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE ssa_172 PARTITION OF ssa FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE ssa_172_2025_01 PARTITION OF ssa_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("l1");
    Spi::run("CREATE TABLE ssa_172_2025_02 PARTITION OF ssa_172 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("l2");
    Spi::run("INSERT INTO ssa VALUES (172,'2025-01-15',5,10),(172,'2025-02-15',5,20)").expect("seed");
    Spi::get_one::<String>("SELECT create_reflex_ivm('fcsta','SELECT dem_plan_id, order_date, product_id, qty FROM ssa', NULL, NULL, ARRAY['dem_plan_id','product_id','order_date'], NULL, ARRAY['dem_plan_id'])").expect("c").expect("c");
    Spi::run("TRUNCATE public.__reflex_partition_pending").expect("clear");

    // Detach + drop the Jan source leaf (removal).
    Spi::run("ALTER TABLE ssa_172 DETACH PARTITION ssa_172_2025_01").expect("detach");
    Spi::run("DROP TABLE ssa_172_2025_01").expect("drop");
    let _ = Spi::get_one::<String>("SELECT reflex_flush_partitions()").expect("flush").expect("flush");

    let jan_gone = Spi::get_one::<bool>("SELECT to_regclass('public.fcsta_ssa_172_2025_01') IS NULL").expect("q").expect("b");
    assert!(jan_gone, "Jan IMV leaf dropped");
    let feb = Spi::get_one::<i32>("SELECT qty FROM fcsta WHERE order_date='2025-02-15'").expect("q").expect("feb");
    assert_eq!(feb, 20, "Feb untouched");
}
```

- [ ] **Step 2: Run** → Expected PASS (DETACH fires the event trigger → enqueue; flush oid-diff sees `ssa_172_2025_01` gone → `Drop`). If the DETACH alone does not enqueue (object_identity nuance), the subsequent `DROP TABLE` is caught by `__reflex_on_sql_drop`; ensure the Drop path is also reachable from flush by having `__reflex_on_sql_drop` enqueue the parent root for dropped *partitions*. If the test fails at enqueue, add to `__reflex_on_sql_drop`: when a dropped table was a partition of a tracked source, `INSERT … __reflex_partition_pending` for its parent root (look up parent via the snapshot, since `pg_inherits` is already gone at sql_drop time — match `child_name` in `__reflex_source_partition_snapshot` to recover `source_root`).
- [ ] **Step 3:** Implement the sql_drop enqueue only if Step 2 reveals it's needed (snapshot-based parent recovery as described).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit**

```bash
git add src/lib.rs src/tests/pg_test_subpartition.rs
git commit -m "feat: detach/drop of source leaf drops IMV leaf via flush"
```

## Task 3.7: Phase 3 gate

- [ ] `cargo fmt && cargo clippy --all-targets -- -D warnings`
- [ ] `cargo pgrx check`
- [ ] `cargo pgrx test pg18 pg_subpart pg_part pg_search_path` (event-trigger + search-path interplay) → PASS
- [ ] `git commit -am "chore: Phase 3 fmt/clippy"`

---

# Phase 4 — Audit drift-check + differential fuzz + bench

Goal: silent drift is always *detectable*; randomized swap sequences stay correct; perf is measured.

## Task 4.1: Recursive partition drift-check

**Files:**
- Modify: `src/audit/checks_b_drift.rs` (add a check fn; wire into the audit runner — find via `rg -n "fn run|push|Check" src/audit/mod.rs`)
- Test: `src/tests/pg_test_audit.rs`

- [ ] **Step 1: Write the failing test**

In `src/tests/pg_test_audit.rs` (follow the existing audit-test shape there; inspect with `sed -n '1,40p' src/tests/pg_test_audit.rs`):

```rust
#[pg_test]
fn pg_audit_detects_partition_tree_drift() {
    Spi::run("CREATE TABLE ssb (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE ssb_172 PARTITION OF ssb FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE ssb_172_2025_01 PARTITION OF ssb_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("l1");
    Spi::get_one::<String>("SELECT create_reflex_ivm('fcstb','SELECT dem_plan_id, order_date, product_id, qty FROM ssb', NULL, NULL, ARRAY['dem_plan_id','product_id','order_date'], NULL, ARRAY['dem_plan_id'])").expect("c").expect("c");

    // Induce drift: attach a new source leaf but deliberately skip the flush.
    Spi::run("CREATE TABLE ssb_172_2025_02 PARTITION OF ssb_172 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("attach");
    Spi::run("TRUNCATE public.__reflex_partition_pending").expect("simulate forgotten flush");

    let report = Spi::get_one::<String>("SELECT reflex_audit('fcstb')").expect("audit").expect("audit");
    assert!(
        report.contains("partition") && report.to_lowercase().contains("drift"),
        "audit should flag partition drift: {report}"
    );
}
```

(Confirm the audit entry-point name with `rg -n "pub fn reflex_audit|#\[pg_extern\].*audit" src/lib.rs src/audit/mod.rs`; adjust the SQL call accordingly.)

- [ ] **Step 2: Run** → FAIL (no partition-drift check yet).

- [ ] **Step 3: Implement the check** in `src/audit/checks_b_drift.rs`, matching the signature/return type of the existing checks in that file (inspect one first). Sketch:

```rust
/// Flags divergence between a partitioned source's recursive leaf set and the
/// IMV's mirrored leaf set (a leaf present on the source but missing on the IMV,
/// or vice-versa). Catches a skipped/forgotten flush or any uncaptured swap.
pub(crate) fn check_partition_tree_drift(
    client: &pgrx::spi::SpiClient<'_>,
    view_name: &str,
    anchor: &str,
) -> Vec<String> {
    let src_leaves: std::collections::HashSet<String> =
        crate::partition::list_partition_tree(client, anchor)
            .into_iter()
            .filter(|n| n.sub_strategy.is_none())
            .map(|n| crate::partition::target_child_name(view_name, &n.bare_name))
            .collect();
    let imv_leaves: std::collections::HashSet<String> =
        crate::partition::list_partition_tree(client, &crate::query_decomposer::quote_identifier(view_name))
            .into_iter()
            .filter(|n| n.sub_strategy.is_none())
            .map(|n| n.bare_name)
            .collect();
    let mut findings = Vec::new();
    for missing in src_leaves.difference(&imv_leaves) {
        findings.push(format!(
            "partition drift: source leaf maps to IMV leaf '{}' which is missing — run SELECT reflex_flush_partitions()",
            missing
        ));
    }
    for extra in imv_leaves.difference(&src_leaves) {
        findings.push(format!(
            "partition drift: IMV leaf '{}' has no source counterpart — run SELECT reflex_sync_partitions('{}', TRUE)",
            extra, view_name
        ));
    }
    findings
}
```

Wire it into the audit runner where the other `checks_b_drift` functions are invoked, but only for IMVs with non-empty `partition_columns` (resolve `anchor` the same way `reflex_sync_partitions_impl` does). `target_child_name` / `intermediate_child_name` must be `pub(crate)` — they already are.

- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit**

```bash
git add src/audit/checks_b_drift.rs src/audit/mod.rs src/tests/pg_test_audit.rs
git commit -m "feat(audit): recursive source-vs-IMV partition-tree drift check"
```

## Task 4.2: Differential fuzz over attach/detach/swap

**Files:**
- Modify: `src/tests/pg_test_fuzz.rs` (add one `#[pg_test]`; reuse the existing oracle pattern — inspect with `rg -n "EXCEPT|fn pg_fuzz|oracle" src/tests/pg_test_fuzz.rs`)

- [ ] **Step 1: Write the test**

```rust
#[pg_test]
fn pg_fuzz_subpartition_swap_sequence_matches_recompute() {
    Spi::run("CREATE TABLE fz (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE fz_1 PARTITION OF fz FOR VALUES IN (1) PARTITION BY RANGE (order_date)").expect("list");
    for m in 1..=3 {
        Spi::run(&format!(
            "CREATE TABLE fz_1_2025_0{m} PARTITION OF fz_1 FOR VALUES FROM ('2025-0{m}-01') TO ('2025-0{n}-01')",
            m = m, n = m + 1
        )).expect("leaf");
    }
    Spi::run("INSERT INTO fz SELECT 1, make_date(2025, g%3+1, 10), g, g*10 FROM generate_series(1,30) g").expect("seed");
    Spi::get_one::<String>("SELECT create_reflex_ivm('fzv','SELECT dem_plan_id, order_date, product_id, qty FROM fz', NULL, NULL, ARRAY['dem_plan_id','product_id','order_date'], NULL, ARRAY['dem_plan_id'])").expect("c").expect("c");

    // Deterministic pseudo-random sequence (no Date/random in tests): swap each
    // month leaf by detach → rebuild standalone with mutated data → attach → flush.
    for m in 1..=3 {
        Spi::run(&format!("ALTER TABLE fz_1 DETACH PARTITION fz_1_2025_0{m}", m = m)).expect("detach");
        Spi::run(&format!("UPDATE fz_1_2025_0{m} SET qty = qty + 1000", m = m)).expect("mutate");
        Spi::run(&format!("ALTER TABLE fz_1 ATTACH PARTITION fz_1_2025_0{m} FOR VALUES FROM ('2025-0{m}-01') TO ('2025-0{n}-01')", m = m, n = m + 1)).expect("attach");
        let _ = Spi::get_one::<String>("SELECT reflex_flush_partitions()").expect("flush").expect("flush");
    }

    // Oracle: IMV must equal a fresh recompute of the base query.
    let drift = Spi::get_one::<i64>(
        "SELECT count(*) FROM ( \
            (SELECT dem_plan_id, order_date, product_id, qty FROM fzv \
             EXCEPT \
             SELECT dem_plan_id, order_date, product_id, qty FROM fz) \
            UNION ALL \
            (SELECT dem_plan_id, order_date, product_id, qty FROM fz \
             EXCEPT \
             SELECT dem_plan_id, order_date, product_id, qty FROM fzv) \
         ) d",
    ).expect("oracle").expect("count");
    assert_eq!(drift, 0, "IMV diverged from source recompute after swap sequence");
}
```

(Note: a same-table detach→in-place-modify→reattach has the **same oid** — the documented limitation. This test exercises it deliberately to confirm the *audit* path / explicit reconcile is what's needed. If the oracle fails *only* for the same-oid reattach, that is expected per the spec; change the test to attach a **freshly-built** table each round — `CREATE TABLE fz_1_2025_0{m}_new (LIKE fz INCLUDING ALL)` + fill + `ATTACH` + `DROP old` — so the oid changes and the oid-diff fires. Use the freshly-built form to assert the supported path is correct.)

- [ ] **Step 2: Run** → use the freshly-built-table form; Expected PASS.
- [ ] **Step 3:** If drift ≠ 0 on the freshly-built form, debug `reflex_flush_partitions_impl` (most likely the snapshot was not refreshed between rounds, or `current_source_leaf_oids` missed a leaf). Fix in `partition.rs`.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit**

```bash
git add src/tests/pg_test_fuzz.rs
git commit -m "test(fuzz): differential oracle over sub-partition swap sequence"
```

## Task 4.3: Bench scenario

**Files:**
- Modify: `benchmarks/bench_partitioned_imv.sql`

- [ ] **Step 1: Add a 2-level scenario** (no test; this is a measurement artifact). Append a section building a `LIST(dem_plan_id) → RANGE(order_date)` source with 4 dem_plan_ids × 6 months × N rows, an IMV with `partition_by => ARRAY['dem_plan_id']` and `order_date` in the unique key, then time three operations with `\timing on`:
  1. leaf swap + `reflex_flush_partitions()` (one month),
  2. whole-`dem_plan_id` reconcile (`reflex_reconcile_partition(view,'', 'src_p_172')`),
  3. `reflex_reconcile(view)` full rebuild.

```sql
\timing on
-- (1) single-leaf swap
ALTER TABLE bench_ss_172 DETACH PARTITION bench_ss_172_2025_03;
CREATE TABLE bench_ss_172_2025_03_new (LIKE bench_ss INCLUDING ALL);
INSERT INTO bench_ss_172_2025_03_new SELECT * FROM /* regenerated month */ ...;
ALTER TABLE bench_ss_172 ATTACH PARTITION bench_ss_172_2025_03_new FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
DROP TABLE bench_ss_172_2025_03;
SELECT reflex_flush_partitions();
-- (2) whole dem_plan_id
SELECT reflex_reconcile_partition('bench_fcst', '', 'bench_ss_172');
-- (3) full
SELECT reflex_reconcile('bench_fcst');
```

- [ ] **Step 2: Run** `psql -f benchmarks/bench_partitioned_imv.sql` against a scratch DB; record timings in `benchmarks/results_partitioned_imv.txt`.
- [ ] **Step 3: Evaluate** per CLAUDE.md: single-leaf swap should be ≈ (1/total_leaves) of full reconcile. If it isn't materially faster, revisit before declaring the feature done (try-measure-revert).
- [ ] **Step 4: Commit**

```bash
git add benchmarks/bench_partitioned_imv.sql benchmarks/results_partitioned_imv.txt
git commit -m "bench: 2-level sub-partition leaf-swap vs reconcile vs full"
```

## Task 4.4: Docs + final gate

**Files:**
- Modify: `docs/concepts/internals.md` (Partitioning section), `CHANGELOG.md`

- [ ] **Step 1:** Add a "Multi-level (sub-partition) sources" subsection to `internals.md`: full write-vector coverage table (from `plans/sub_partitioning.md`), the event-trigger→enqueue→flush model, the oid-diff snapshot, the `detach→in-place→reattach-same-table` limitation, and the `reflex_flush_partitions()` / `reflex_reconcile_partition(view, '', source_partition)` API.
- [ ] **Step 2:** Add an Unreleased `CHANGELOG.md` entry.
- [ ] **Step 3: Final gate:** `cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo pgrx check && cargo pgrx test pg18` → all green.
- [ ] **Step 4: Commit**

```bash
git add docs/concepts/internals.md CHANGELOG.md
git commit -m "docs: multi-level sub-partition source support"
```

---

## Self-review notes (for the implementer)

- **Spec coverage:** Section 1 → Tasks 1.1–1.4; Section 2 capture → Tasks 3.1–3.6; Section 3 reconcile → Tasks 2.2–2.3; Section 4 lifecycle → Task 2.1 + 3.5–3.6; Section 5 tests/bench/backstop → Tasks 4.1–4.3. Out-of-scope items (HASH, multi-hop Tier-2, auto-flush-at-commit, finer cascade, same-oid reattach) are explicitly *not* tasks.
- **Type consistency:** `PartitionNode`, `build_partition_node_ddl_pair`, `classify_partition_diff`/`PartitionDiffAction`, `current_source_leaf_oids`, `refresh_source_snapshot`, `reflex_flush_partitions_impl`, `expand_source_partition_to_leaves` are used with the same signatures throughout. `reflex_reconcile_partition_impl` gains exactly one `source_partition: &str` param (default `''`).
- **Assumptions to confirm at implementation time (do not skip):** (1) exact `create_reflex_ivm` positional arity for the `NULL` placeholders; (2) `ctx` field names in `create_ivm/mod.rs` for projected/key columns; (3) audit entry-point name (`reflex_audit`?); (4) `child_oid` column type (`OID` vs `BIGINT`) per pgrx bind behavior; (5) whether any existing test relies on the inline event-trigger `reflex_sync_partitions` side effect before removing it. Each is called out in the relevant task.
