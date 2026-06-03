# IMV Partition Depth ≤ Source Depth — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a partitioned IMV mirror its source at a depth ≤ the source's — chosen authoritatively by `partition_by`, or auto-pruned to the deepest level whose column is bare-projected — instead of unconditionally mirroring the full source partition hierarchy (which rejects FULL-JOIN-coalesced-key IMVs like `forecast_analysis_view`).

**Architecture:** One truncation primitive (`truncate_partition_tree`) demotes the source tree at the IMV's `mirror_depth` boundary; every site that mirrors/syncs/snapshots the source tree feeds it through that primitive. Depth is persisted in a new nullable `partition_depth INT` column (NULL = full source depth, so existing IMVs are unaffected). Capture stays leaf-granular and maps each changed leaf *up* to its `mirror_depth` ancestor, using a new `ancestors TEXT[]` snapshot column to resolve vanished leaves.

**Tech Stack:** Rust + pgrx (PostgreSQL extension). Tests: pure-Rust `#[test]` units in `src/tests/unit_partition.rs`; `#[pg_test]` integration in `src/tests/pg_test_subpartition.rs`. Gates: `cargo pgrx check`, `cargo pgrx test`, `cargo clippy`, `cargo fmt`.

**Spec:** `plans/2026-06-03-imv-partition-depth.md`

---

## Orientation (read before starting)

Source-of-truth functions you will touch (all line numbers approximate — confirm with `rg`):

- `src/partition.rs`
  - `PartitionNode` struct — `:66`
  - `list_partition_tree(client, root) -> Vec<PartitionNode>` — `:187` (recursive `pg_inherits` walk; CTE already computes a `depth` it does not currently SELECT)
  - `build_partition_node_ddl_pair(view, node, anchor_root_bare, unlogged)` — `:312` (emits `PARTITION BY` suffix only when `node.sub_columns` non-empty; leaf when `sub_strategy.is_none()`)
  - `reflex_sync_partitions_impl(view, drop_orphans)` — `:802` (reads metadata, walks source tree, creates/drops IMV children)
  - `reflex_reconcile_partition_impl(view, keys_csv, source_partition)` — `:1016`
  - `expand_source_partition_to_leaves(client, source_partition) -> Vec<String>` — `:1415`
  - `classify_partition_diff(snapshot, current) -> Vec<(String, PartitionDiffAction)>` — `:582`
  - `current_source_leaf_oids(client, root) -> Vec<(String,u32)>` — `:1591`
  - `refresh_source_snapshot(client, root)` — `:1628`
  - `reflex_flush_partitions_impl(only)` — `:1659`
  - `canonical_root_key` — `:1607`
- `src/create_ivm/mod.rs`
  - `resolve_partitioning(ctx) -> Result<(),String>` — `:444` (explicit branch validates sub-levels at `:550-578`; auto branch at `:592-656`)
  - passthrough create-time mirroring loop — `:829`
  - aggregate create-time mirroring loop — `:985`
  - `BuildContext` struct — `:40` region (fields like `resolved_partition_cols`, `resolved_unique_columns`, `plan`)
  - `persist_metadata(client, ctx)` — `:1328`; RegistryRow built ~`:1381`
- `src/sql_writer/registry.rs` — `RegistryRow` struct `:30`, `decomposed()` `:64`, `insert_registry_row` `:108` (full-shape INSERT at `:193`)
- `src/lib.rs` — `__reflex_ivm_reference` DDL `:80`; `__reflex_source_partition_snapshot` DDL `:166`

Run a single pg_test: `cargo pgrx test pg17 -- <test_name>` (substitute your pg version; check `Cargo.toml` `[features]`). Run all: `cargo pgrx test`. Run a unit test: `cargo test --lib <test_name>`.

---

## File Structure

No new files except possibly test additions. Changes are localized:

| File | Responsibility | Change |
|---|---|---|
| `src/partition.rs` | partition tree introspection, DDL, sync, reconcile, snapshot, flush | `depth` field on `PartitionNode`; `truncate_partition_tree`; depth-aware sync; leaf→ancestor up-mapping in reconcile + flush; `ancestors[]` in snapshot |
| `src/create_ivm/mod.rs` | IMV build pipeline | depth resolution + bounded validation in `resolve_partitioning`; truncate at create-time mirroring; carry `partition_depth` into metadata |
| `src/sql_writer/registry.rs` | catalog row insert | `partition_depth: Option<i32>` field + INSERT column |
| `src/lib.rs` | catalog DDL | `partition_depth INT` column; `ancestors TEXT[]` snapshot column |
| `src/audit/checks_b_drift.rs` | drift backstop | aggregate source leaves to `mirror_depth` before comparing |
| `src/tests/unit_partition.rs` | pure-Rust units | new unit tests |
| `src/tests/pg_test_subpartition.rs` | integration | new `#[pg_test]` cases |

---

# PHASE 1 — Truncation primitive, depth field, create-time mirroring, validation

Phase 1 alone unblocks `forecast_analysis_view` creation: it creates as a single-level `LIST(dem_plan_id)` IMV. Capture maintenance comes in Phases 2–3.

## Task 1.1: Add `depth` to `PartitionNode` and surface it from the tree walk

**Files:**
- Modify: `src/partition.rs:66` (struct), `:187` (`list_partition_tree`)
- Test: `src/tests/unit_partition.rs`

- [ ] **Step 1: Write the failing test**

Add to `src/tests/unit_partition.rs`:

```rust
#[test]
fn test_partition_node_has_depth_field() {
    // A leaf node constructed directly carries an absolute tree-depth.
    let n = PartitionNode {
        bare_name: "ss_172".to_string(),
        oid: 1,
        parent_bare: "ss".to_string(),
        bound_expr: "FOR VALUES IN (172)".to_string(),
        sub_strategy: None,
        sub_columns: vec![],
        depth: 1,
    };
    assert_eq!(n.depth, 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib test_partition_node_has_depth_field`
Expected: FAIL — `struct PartitionNode has no field named depth` (compile error).

- [ ] **Step 3: Add the field and populate it**

In `src/partition.rs`, add to `PartitionNode` (after `sub_columns: Vec<String>,`):

```rust
    /// Absolute tree-depth from the anchor root: the anchor's direct
    /// children are depth 1, their children depth 2, etc. Populated by
    /// `list_partition_tree` from the recursive CTE's `depth` column;
    /// `truncate_partition_tree` keys the level cutoff off it.
    pub depth: usize,
```

In `list_partition_tree` (`:191`), add `t.depth AS node_depth,` to the SELECT list (right after `c.oid::int8 AS node_oid,`):

```rust
        SELECT \
            c.relname::text AS bare_name, \
            c.oid::int8 AS node_oid, \
            t.depth AS node_depth, \
            pc.relname::text AS parent_bare, \
```

In the `filter_map` closure (`:225`), after `let oid = ...`, add:

```rust
                let depth = row.get_by_name::<i32, _>("node_depth").ok()?? as usize;
```

and add `depth,` to the `PartitionNode { ... }` literal.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib test_partition_node_has_depth_field`
Expected: PASS.

- [ ] **Step 5: Fix any other `PartitionNode { ... }` literals**

Run: `rg -n "PartitionNode \{" src/`
For every struct-literal construction (e.g. in `list_partition_tree`, and any test helpers), add `depth: <n>,`. The only non-test literal is in `list_partition_tree`. Build to confirm:

Run: `cargo build`
Expected: compiles (no "missing field depth").

- [ ] **Step 6: Commit**

```bash
git add src/partition.rs src/tests/unit_partition.rs
git commit -m "feat(partition): carry absolute tree-depth on PartitionNode"
```

## Task 1.2: `truncate_partition_tree` primitive

**Files:**
- Modify: `src/partition.rs` (add function near `list_partition_tree`, after `:267`)
- Test: `src/tests/unit_partition.rs`

- [ ] **Step 1: Write the failing tests**

Add to `src/tests/unit_partition.rs`:

```rust
fn node(bare: &str, parent: &str, depth: usize, sub: Option<&str>) -> PartitionNode {
    PartitionNode {
        bare_name: bare.to_string(),
        oid: 0,
        parent_bare: parent.to_string(),
        bound_expr: "FOR VALUES IN (1)".to_string(),
        sub_strategy: sub.map(|s| s.to_string()),
        sub_columns: if sub.is_some() { vec!["order_date".to_string()] } else { vec![] },
        depth,
    }
}

#[test]
fn test_truncate_drops_below_depth_and_demotes_boundary() {
    // ss_172 (internal, depth 1) -> ss_172_2025_01 (leaf, depth 2)
    let tree = vec![
        node("ss_172", "ss", 1, Some("RANGE")),
        node("ss_172_2025_01", "ss_172", 2, None),
    ];
    let out = truncate_partition_tree(tree, 1);
    // Only the depth-1 node remains, demoted to a leaf.
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].bare_name, "ss_172");
    assert!(out[0].sub_strategy.is_none(), "boundary node must be demoted to leaf");
    assert!(out[0].sub_columns.is_empty(), "boundary node must drop sub_columns");
}

#[test]
fn test_truncate_full_depth_is_noop() {
    let tree = vec![
        node("ss_172", "ss", 1, Some("RANGE")),
        node("ss_172_2025_01", "ss_172", 2, None),
    ];
    let out = truncate_partition_tree(tree.clone(), 2);
    assert_eq!(out.len(), 2);
    // Internal node keeps its sub-partitioning.
    assert_eq!(out[0].sub_strategy.as_deref(), Some("RANGE"));
    assert_eq!(out[1].bare_name, "ss_172_2025_01");
}

#[test]
fn test_truncate_depth_beyond_tree_is_noop() {
    let tree = vec![node("ss_172", "ss", 1, None)];
    let out = truncate_partition_tree(tree, 5);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].sub_strategy, None);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib test_truncate`
Expected: FAIL — `cannot find function truncate_partition_tree`.

- [ ] **Step 3: Implement the primitive**

In `src/partition.rs`, immediately after `list_partition_tree` (after its closing `}` at ~`:267`):

```rust
/// Truncate a source partition tree to `mirror_depth` absolute levels.
///
/// Keeps every node at `depth <= mirror_depth`, drops everything deeper, and
/// **demotes** the nodes sitting exactly at `mirror_depth` to leaves (clears
/// `sub_strategy` / `sub_columns`) so `build_partition_node_ddl_pair` emits no
/// `PARTITION BY` suffix for them — a `LIST(a) -> RANGE(b)` source truncated to
/// depth 1 becomes plain `LIST(a)` leaves, each holding all of that key's rows.
///
/// `mirror_depth == 0` is treated as "no truncation" (defensive; callers pass
/// the resolved full source depth for NULL `partition_depth`). A `mirror_depth`
/// at or beyond the tree's max depth is a no-op.
pub(crate) fn truncate_partition_tree(
    nodes: Vec<PartitionNode>,
    mirror_depth: usize,
) -> Vec<PartitionNode> {
    if mirror_depth == 0 {
        return nodes;
    }
    nodes
        .into_iter()
        .filter(|n| n.depth <= mirror_depth)
        .map(|mut n| {
            if n.depth == mirror_depth {
                n.sub_strategy = None;
                n.sub_columns = Vec::new();
            }
            n
        })
        .collect()
}

/// Maximum absolute tree-depth across `nodes` (0 when empty / unpartitioned).
/// Used to resolve a NULL `partition_depth` to "mirror the full source depth".
pub(crate) fn max_tree_depth(nodes: &[PartitionNode]) -> usize {
    nodes.iter().map(|n| n.depth).max().unwrap_or(0)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib test_truncate`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/partition.rs src/tests/unit_partition.rs
git commit -m "feat(partition): truncate_partition_tree + max_tree_depth helpers"
```

## Task 1.3: Depth resolution + bounded validation in `resolve_partitioning`

This replaces the unconditional full-tree sub-level validation (`:550-578`) with a level-bounded one, adds auto-prune to the auto branch, and records the resolved `mirror_depth` on `BuildContext`.

**Files:**
- Modify: `src/create_ivm/mod.rs` — `BuildContext` (add field), `resolve_partitioning` (`:444`)
- Test: `src/tests/pg_test_subpartition.rs` (integration — needs a partitioned source)

- [ ] **Step 1: Add the `resolved_partition_depth` field to `BuildContext`**

In `src/create_ivm/mod.rs`, find the `BuildContext` struct (near `:40-95`) and add after `resolved_partition_cols`:

```rust
    /// Resolved IMV partition mirror-depth (number of source levels to
    /// mirror). `None` until `resolve_partitioning` runs; persisted to
    /// `__reflex_ivm_reference.partition_depth`. `Some(k)` = mirror k levels.
    resolved_partition_depth: Option<i32>,
```

Find where `BuildContext` is constructed (the `BuildContext { ... }` literal around `:1760`, with `resolved_strategy: String::new(),`) and add:

```rust
        resolved_partition_depth: None,
```

- [ ] **Step 2: Write the failing integration tests**

Add to `src/tests/pg_test_subpartition.rs`:

```rust
// Helper: count target-side partition children of an IMV (any depth).
fn imv_child_count(view: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*)::int8 FROM pg_inherits i \
         JOIN pg_class p ON p.oid = i.inhparent \
         WHERE p.relname = '{}'",
        view
    ))
    .unwrap()
    .unwrap()
}

// Helper: is `child` itself partitioned (an internal node)?
fn is_partitioned_rel(child: &str) -> bool {
    Spi::get_one::<bool>(&format!(
        "SELECT EXISTS(SELECT 1 FROM pg_partitioned_table pt \
         JOIN pg_class c ON c.oid = pt.partrelid WHERE c.relname = '{}')",
        child
    ))
    .unwrap()
    .unwrap()
}

#[pg_test]
fn pg_subpart_explicit_shallow_partition_by_creates_single_level() {
    Spi::run(
        "CREATE TABLE ssd (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, \
         product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)",
    )
    .expect("root");
    Spi::run("CREATE TABLE ssd_172 PARTITION OF ssd FOR VALUES IN (172) PARTITION BY RANGE (order_date)")
        .expect("list child");
    Spi::run("CREATE TABLE ssd_172_2025_01 PARTITION OF ssd_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')")
        .expect("leaf");
    Spi::run(
        "INSERT INTO ssd (dem_plan_id, order_date, product_id, qty) VALUES (172, '2025-01-15', 5, 10)",
    )
    .expect("seed");

    // order_date is projected only via a COALESCE-like rename — declare
    // partition_by:[dem_plan_id] so we mirror ONLY the dem_plan_id level.
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst_shallow', \
            'SELECT dem_plan_id, COALESCE(order_date, order_date) AS order_date, product_id, qty FROM ssd', \
            'dem_plan_id,order_date,product_id', NULL, NULL, NULL, ARRAY['dem_plan_id'])",
    )
    .expect("create call")
    .expect("create result");
    assert!(r.contains("fcst_shallow") || r.to_lowercase().contains("created"), "got: {}", r);

    // Exactly ONE target child (the dem_plan_id=172 leaf), and it is NOT
    // itself partitioned (no order_date sub-level mirrored).
    assert_eq!(imv_child_count("fcst_shallow"), 1);
    assert!(!is_partitioned_rel("fcst_shallow_ssd_172"),
        "dem_plan_id leaf must be a plain table, not sub-partitioned");

    // Data is correct.
    let n = Spi::get_one::<i64>("SELECT count(*)::int8 FROM fcst_shallow").unwrap().unwrap();
    assert_eq!(n, 1);
}
```

> Note: the IMV node bare-name format is `<bare_view>_<source_node_bare>` (see `target_child_name`). Confirm the exact child relname with `rg -n "fn target_child_name" src/partition.rs` and adjust `fcst_shallow_ssd_172` if the scheme differs.

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo pgrx test pg17 -- pg_subpart_explicit_shallow_partition_by_creates_single_level`
Expected: FAIL — creation rejected with `partition key column 'order_date' ... is not a bare projected output column` (the current unconditional sub-level check).

- [ ] **Step 4: Replace the explicit-branch sub-level validation with a depth-bounded one**

In `src/create_ivm/mod.rs`, inside `resolve_partitioning`, the explicit branch's `Spi::connect` block (`:529-581`). Replace the section that builds `all_sub_cols` and loops over it (`:550-578`) with a level-bounded resolution. The new logic:

1. Build the source level columns top-down by walking one path of the tree.
2. `mirror_depth = resolved_partition_cols.len()` (declared levels).
3. Validate each declared level `k` (0-based `i`) matches source level column `i` AND is in the unique key; reject otherwise with a per-level message.

Replace `:550-578` (from the `// Validate sub-level partition columns...` comment through the closing of the `for sub_col` loop, up to `Ok(desc.strategy)`) with:

```rust
            // Depth-bounded validation: only the levels the user explicitly
            // declared in `partition_by` are mirrored. Build the source's
            // ordered level columns (top-down) and check each declared level.
            let tree = crate::partition::list_partition_tree(client, &anchor);
            let source_level_cols = crate::partition::source_level_columns(&desc, &tree);

            let unique_key_cols: std::collections::HashSet<String> = ctx
                .resolved_unique_columns
                .iter()
                .map(|c| c.to_lowercase())
                .collect();

            let declared = &ctx.resolved_partition_cols;
            for (i, declared_col) in declared.iter().enumerate() {
                let dl = declared_col.to_lowercase();
                match source_level_cols.get(i) {
                    None => {
                        return Err(format!(
                            "partition_by declares {} level(s) but source '{}' has only {} \
                             partition level(s)",
                            declared.len(), anchor, source_level_cols.len()
                        ));
                    }
                    Some(src_col) if src_col.to_lowercase() != dl => {
                        return Err(format!(
                            "partition_by level {} is '{}' but source '{}' is partitioned on \
                             '{}' at that level; declared levels must match the source's \
                             partition key columns top-down",
                            i + 1, declared_col, anchor, src_col
                        ));
                    }
                    Some(_) => {}
                }
                if !unique_key_cols.contains(&dl) {
                    return Err(format!(
                        "partition key column '{}' (level {} of source '{}') is not a bare \
                         projected output column in the IMV's unique key. Add it to the SELECT \
                         list and unique_columns, or declare a shallower partition_by.",
                        declared_col, i + 1, anchor
                    ));
                }
            }

            Ok(desc.strategy)
```

> The `mirror_depth` for the explicit branch is simply `declared.len()`. Set it on `ctx` right after the `Spi::connect` returns `Ok` (Step 6).

- [ ] **Step 5: Add `source_level_columns` helper to `src/partition.rs`**

After `truncate_partition_tree` (Task 1.2), add:

```rust
/// Ordered partition-key columns per source level, top-down: index 0 is the
/// root's partition column, index 1 the first sub-level's, etc. Derived by
/// following ONE path down the tree (a well-formed hierarchy partitions all
/// siblings at a level by the same key). Level 0 comes from the descriptor;
/// deeper levels from each internal node's `sub_columns`.
pub(crate) fn source_level_columns(
    desc: &PartitionDescriptor,
    tree: &[PartitionNode],
) -> Vec<String> {
    let mut levels: Vec<String> = Vec::new();
    if let Some(c) = desc.column_names.first() {
        levels.push(c.to_lowercase());
    }
    // Walk down: at each depth, find an internal node and take its sub key.
    let mut current_depth = 1usize;
    loop {
        let internal = tree
            .iter()
            .find(|n| n.depth == current_depth && !n.sub_columns.is_empty());
        match internal {
            Some(n) => {
                levels.push(n.sub_columns[0].to_lowercase());
                current_depth += 1;
            }
            None => break,
        }
    }
    levels
}
```

> `PartitionDescriptor` is defined in `src/partition.rs` (`column_names: Vec<String>`, `strategy: String`). Confirm with `rg -n "struct PartitionDescriptor" src/partition.rs`.

- [ ] **Step 6: Set `resolved_partition_depth` after explicit validation succeeds**

In `resolve_partitioning`, the `match validate_result { Ok(s) => ctx.resolved_strategy = s, ... }` block (`:582-591`). Change the `Ok` arm to also set depth:

```rust
        match validate_result {
            Ok(s) => {
                ctx.resolved_strategy = s;
                ctx.resolved_partition_depth = Some(ctx.resolved_partition_cols.len() as i32);
            }
            Err(e) => {
```

- [ ] **Step 7: Run test to verify it passes**

Run: `cargo pgrx test pg17 -- pg_subpart_explicit_shallow_partition_by_creates_single_level`
Expected: PASS.

- [ ] **Step 8: Verify the existing full-mirror explicit test still passes**

Run: `cargo pgrx test pg17 -- pg_subpart_create_mirrors_full_tree`
Expected: PASS — declaring `partition_by:[dem_plan_id]` on `fcst2` projects `order_date` as a bare column and the source is 2-level; but `fcst2` declares only `[dem_plan_id]`, so it now mirrors depth 1. **If this test asserted a 2-level mirror, it must be updated** — see Task 1.5 which adds explicit-2-level coverage. For now confirm it does not regress creation; adjust the assertion to depth-1 if it checked child shape.

- [ ] **Step 9: Commit**

```bash
git add src/create_ivm/mod.rs src/partition.rs src/tests/pg_test_subpartition.rs
git commit -m "feat(partition): depth-bounded explicit partition_by validation"
```

## Task 1.4: Truncate at the two create-time mirroring sites

**Files:**
- Modify: `src/create_ivm/mod.rs:829` (passthrough), `:985` (aggregate)

- [ ] **Step 1: Compute mirror_depth and truncate in the passthrough mirroring loop**

In `materialize_passthrough`, the block at `:823-840`. Replace:

```rust
            let (_, anchor_root_bare) = split_qualified_name(&anchor);
            let nodes = crate::partition::list_partition_tree(client, &anchor);
            for node in &nodes {
```

with:

```rust
            let (_, anchor_root_bare) = split_qualified_name(&anchor);
            let full = crate::partition::list_partition_tree(client, &anchor);
            let mirror_depth = ctx
                .resolved_partition_depth
                .map(|d| d as usize)
                .unwrap_or_else(|| crate::partition::max_tree_depth(&full));
            let nodes = crate::partition::truncate_partition_tree(full, mirror_depth);
            for node in &nodes {
```

- [ ] **Step 2: Same truncation in the aggregate mirroring loop**

In `materialize_aggregate`, the block at `:984-1002`. Replace:

```rust
                let (_, anchor_root_bare) = split_qualified_name(&anchor);
                let nodes = crate::partition::list_partition_tree(client, &anchor);
                info!(
                    "pg_reflex: creating {} partition nodes for '{}' (anchor='{}')",
                    nodes.len(),
```

with:

```rust
                let (_, anchor_root_bare) = split_qualified_name(&anchor);
                let full = crate::partition::list_partition_tree(client, &anchor);
                let mirror_depth = ctx
                    .resolved_partition_depth
                    .map(|d| d as usize)
                    .unwrap_or_else(|| crate::partition::max_tree_depth(&full));
                let nodes = crate::partition::truncate_partition_tree(full, mirror_depth);
                info!(
                    "pg_reflex: creating {} partition nodes for '{}' (anchor='{}')",
                    nodes.len(),
```

- [ ] **Step 3: Run the shallow + full create tests**

Run: `cargo pgrx test pg17 -- pg_subpart_explicit_shallow_partition_by_creates_single_level`
Run: `cargo pgrx test pg17 -- pg_subpart_create_mirrors_full_tree`
Expected: both PASS (shallow makes 1 plain leaf; full still makes the 2-level tree because its `resolved_partition_depth` is `Some(1)`… **see note**).

> **Important consistency note:** `fcst2` in `pg_subpart_create_mirrors_full_tree` declares `partition_by:[dem_plan_id]`, so after Task 1.3 its `resolved_partition_depth = Some(1)` and it will now mirror **depth 1**, not the full 2 levels. This is the intended new behavior (explicit = authoritative). Update that test to declare `partition_by:[dem_plan_id, order_date]` if it must mirror 2 levels, OR assert depth-1 shape. Decide per Task 1.5.

- [ ] **Step 4: Commit**

```bash
git add src/create_ivm/mod.rs
git commit -m "feat(partition): truncate source tree to mirror_depth at create time"
```

## Task 1.5: Auto-prune branch + explicit-2-level coverage

**Files:**
- Modify: `src/create_ivm/mod.rs` auto branch (`:592-656`)
- Test: `src/tests/pg_test_subpartition.rs`

- [ ] **Step 1: Write the failing tests**

Add to `src/tests/pg_test_subpartition.rs`:

```rust
#[pg_test]
fn pg_subpart_explicit_two_level_opts_into_subpartitioning() {
    Spi::run(
        "CREATE TABLE sst (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, qty INT) \
         PARTITION BY LIST (dem_plan_id)",
    ).expect("root");
    Spi::run("CREATE TABLE sst_172 PARTITION OF sst FOR VALUES IN (172) PARTITION BY RANGE (order_date)")
        .expect("list child");
    Spi::run("CREATE TABLE sst_172_2025_01 PARTITION OF sst_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')")
        .expect("leaf");
    Spi::run("INSERT INTO sst VALUES (172, '2025-01-15', 10)").expect("seed");

    // order_date IS a bare projected column here -> can opt into 2 levels.
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst_deep', \
            'SELECT dem_plan_id, order_date, qty FROM sst', \
            'dem_plan_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id','order_date'])",
    ).expect("create call").expect("create result");
    assert!(r.to_lowercase().contains("created") || r.contains("fcst_deep"), "got: {}", r);

    // The dem_plan_id leaf is itself partitioned (order_date sub-level mirrored).
    assert!(is_partitioned_rel("fcst_deep_sst_172"),
        "with explicit 2-level partition_by, the dem_plan_id node must sub-partition");
}

#[pg_test]
fn pg_subpart_auto_prune_stops_at_non_projected_sublevel() {
    Spi::run(
        "CREATE TABLE ssa (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, qty INT) \
         PARTITION BY LIST (dem_plan_id)",
    ).expect("root");
    Spi::run("CREATE TABLE ssa_172 PARTITION OF ssa FOR VALUES IN (172) PARTITION BY RANGE (order_date)")
        .expect("list child");
    Spi::run("CREATE TABLE ssa_172_2025_01 PARTITION OF ssa_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')")
        .expect("leaf");
    Spi::run("INSERT INTO ssa VALUES (172, '2025-01-15', 10)").expect("seed");

    // No partition_by, order_date NOT bare-projected -> auto prunes to depth 1.
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst_auto', \
            'SELECT dem_plan_id, COALESCE(order_date, order_date) AS order_date, qty FROM ssa', \
            'dem_plan_id,order_date', NULL, NULL, NULL, NULL)",
    ).expect("create call").expect("create result");
    assert!(r.to_lowercase().contains("created") || r.contains("fcst_auto"), "got: {}", r);

    assert!(!is_partitioned_rel("fcst_auto_ssa_172"),
        "auto-mirror must prune the order_date sub-level (not bare-projected)");
    assert_eq!(imv_child_count("fcst_auto"), 1);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo pgrx test pg17 -- pg_subpart_explicit_two_level_opts_into_subpartitioning pg_subpart_auto_prune_stops_at_non_projected_sublevel`
Expected: the 2-level test may PASS already (explicit depth 2 mirrors fully); the auto-prune test FAILS — today auto-mirror picks only the root column for depth, but does not set `resolved_partition_depth`, so create-time truncation falls back to `max_tree_depth` (full), leaving `fcst_auto_ssa_172` sub-partitioned.

- [ ] **Step 3: Implement auto-prune in the auto branch**

In `resolve_partitioning`, the auto branch (the `else` at `:592`). Today it returns `(Vec<String>, String)` = (partition cols, strategy) and sets only the root column. Extend it to also compute the pruned depth. Change the closure's return type to include depth and the projected-set check per level.

Replace the auto branch body (`:593-655`, the `let auto: (Vec<String>, String) = Spi::connect(...)` through the `info!`) with:

```rust
        // Phase 5 + depth-prune: auto-mirror when exactly one real source is
        // partitioned. Walk levels top-down; keep a level while its partition
        // column is a bare projected output column; stop at the first that is
        // not. The kept prefix length is the mirror depth.
        let auto: (Vec<String>, String, Option<i32>) = Spi::connect(|client| {
            let mut partitioned_sources: Vec<(String, crate::partition::PartitionDescriptor)> =
                Vec::new();
            for s in &ctx.real_source_names {
                if let Some(desc) = crate::partition::introspect_partition_descriptor(client, s) {
                    partitioned_sources.push((s.clone(), desc));
                }
            }
            if partitioned_sources.len() != 1 {
                return (Vec::new(), String::new(), None);
            }
            let (anchor, desc) = partitioned_sources.into_iter().next().unwrap();
            let part_col = desc.column_names.first().cloned().unwrap_or_default();
            if part_col.is_empty() {
                return (Vec::new(), String::new(), None);
            }

            // Bare projected output columns of the IMV.
            let projected: std::collections::HashSet<String> = if ctx.plan.is_passthrough {
                let mut set: std::collections::HashSet<String> = ctx
                    .plan
                    .passthrough_columns
                    .iter()
                    .map(|c| c.to_lowercase())
                    .collect();
                for c in &ctx.analysis.select_columns {
                    if let Some(alias) = &c.alias {
                        // Only count it as "bare" when the expr is a bare column ref.
                        if crate::sql_analyzer::is_bare_column_reference(&c.expr_sql) {
                            set.insert(bare_column_name(alias).to_lowercase());
                        }
                    } else if crate::sql_analyzer::is_bare_column_reference(&c.expr_sql) {
                        set.insert(bare_column_name(&c.expr_sql).to_lowercase());
                    }
                }
                set
            } else {
                // Aggregate: GROUP BY columns / aliases that are bare refs.
                let mut set: std::collections::HashSet<String> = ctx
                    .plan
                    .group_by_columns
                    .iter()
                    .filter(|c| crate::sql_analyzer::is_bare_column_reference(c))
                    .map(|c| normalized_column_name(c).to_lowercase())
                    .collect();
                for v in ctx.plan.group_by_aliases.values() {
                    set.insert(v.to_lowercase());
                }
                set
            };

            // Root level must be projected for ANY partitioning at all.
            if !projected.contains(&part_col.to_lowercase()) {
                return (Vec::new(), String::new(), None);
            }

            let tree = crate::partition::list_partition_tree(client, &anchor);
            let level_cols = crate::partition::source_level_columns(&desc, &tree);
            // Keep the longest prefix of levels whose column is bare-projected.
            let mut depth = 0usize;
            for col in &level_cols {
                if projected.contains(&col.to_lowercase()) {
                    depth += 1;
                } else {
                    break;
                }
            }
            if depth == 0 {
                return (Vec::new(), String::new(), None);
            }
            if depth < level_cols.len() {
                info!(
                    "pg_reflex: auto-mirror pruning '{}' at depth {} — level {} column '{}' \
                     is not a bare projected output column",
                    anchor, depth, depth + 1, level_cols[depth]
                );
            }
            (vec![part_col], desc.strategy, Some(depth as i32))
        });
        ctx.resolved_partition_cols = auto.0;
        ctx.resolved_strategy = auto.1;
        ctx.resolved_partition_depth = auto.2;
        if !ctx.resolved_partition_cols.is_empty() {
            info!(
                "pg_reflex: auto-mirroring partition column '{}' from source (depth {:?})",
                ctx.resolved_partition_cols[0], ctx.resolved_partition_depth
            );
        }
```

> `is_bare_column_reference` lives in `crate::sql_analyzer` (used already at `:516`). `bare_column_name` and `normalized_column_name` are in-scope in `mod.rs` (used at `:622`, `:463`). `ctx.plan.group_by_aliases` and `passthrough_columns` are existing fields. Confirm `ctx.analysis.select_columns[].expr_sql` / `.alias` field names with `rg -n "struct SelectColumn" src/`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo pgrx test pg17 -- pg_subpart_explicit_two_level_opts_into_subpartitioning pg_subpart_auto_prune_stops_at_non_projected_sublevel`
Expected: both PASS.

- [ ] **Step 5: Reconcile the legacy `fcst2` test**

Open `pg_subpart_create_mirrors_full_tree`. Change its `create_reflex_ivm` call's last argument from `ARRAY['dem_plan_id']` to `ARRAY['dem_plan_id','order_date']` (it projects `order_date` bare, so 2-level is valid) so it still asserts a full 2-level mirror. Run:

Run: `cargo pgrx test pg17 -- pg_subpart_create_mirrors_full_tree`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/create_ivm/mod.rs src/tests/pg_test_subpartition.rs
git commit -m "feat(partition): auto-mirror prunes to deepest bare-projected level"
```

## Task 1.6: Phase 1 gate

- [ ] **Step 1: Full check + clippy + fmt**

Run: `cargo fmt`
Run: `cargo clippy --all-targets -- -D warnings`
Run: `cargo pgrx check`
Run: `cargo pgrx test pg17 -- pg_subpart_`
Expected: all green; all `pg_subpart_*` tests pass.

- [ ] **Step 2: Commit any fmt/clippy fixups**

```bash
git add -A
git commit -m "chore: fmt + clippy for phase 1 partition-depth"
```

---

# PHASE 2 — `partition_depth` column + depth-aware sync

## Task 2.1: Add `partition_depth INT` to the catalog

**Files:**
- Modify: `src/lib.rs:111` region (catalog DDL)

- [ ] **Step 1: Add the column to the `__reflex_ivm_reference` DDL**

In `src/lib.rs`, in the `CREATE TABLE IF NOT EXISTS public.__reflex_ivm_reference (...)` block, add after `wipe_threshold NUMERIC,` (`:111`):

```sql
        -- 1.8.2 — IMV partition mirror depth: how many source partition
        -- levels this IMV mirrors. NULL = mirror the FULL source depth
        -- (legacy/default behavior). Set by resolve_partitioning when the
        -- IMV is shallower than its source (explicit partition_by or
        -- auto-prune). See plans/2026-06-03-imv-partition-depth.md.
        partition_depth INT,
```

> Because the table uses `CREATE TABLE IF NOT EXISTS`, also add an idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for already-installed catalogs. Find where other late-added columns are migrated (search `rg -n "ADD COLUMN IF NOT EXISTS" src/lib.rs`) and add alongside them:

```sql
    ALTER TABLE public.__reflex_ivm_reference ADD COLUMN IF NOT EXISTS partition_depth INT;
```

If no such migration block exists, add one immediately after the `CREATE TABLE` statement.

- [ ] **Step 2: Build**

Run: `cargo build`
Expected: compiles.

- [ ] **Step 3: Commit**

```bash
git add src/lib.rs
git commit -m "feat(catalog): add nullable partition_depth column"
```

## Task 2.2: Persist `partition_depth` through `RegistryRow`

**Files:**
- Modify: `src/sql_writer/registry.rs` (struct `:30`, `decomposed` `:64`, INSERT `:193`), `src/create_ivm/mod.rs` (RegistryRow build `:1381`)

- [ ] **Step 1: Add the field to `RegistryRow`**

In `src/sql_writer/registry.rs`, add to the `RegistryRow<'a>` struct (after `partition_strategy`):

```rust
    /// IMV partition mirror depth; `None` => NULL => full source depth.
    pub partition_depth: Option<i32>,
```

In `decomposed(...)` (the `RegistryRow { ... }` literal at `:74`), add:

```rust
            partition_depth: None,
```

- [ ] **Step 2: Add the column to the full-shape INSERT**

In `insert_registry_row`, the full-shape branch (`:193`). Add `partition_depth` to the column list and a new positional param. Change the SQL column list + VALUES:

```rust
        let sql = "INSERT INTO public.__reflex_ivm_reference
                     (name, graph_depth, depends_on, depends_on_imv, unlogged_tables,
                      graph_child, sql_query, base_query, end_query,
                      aggregations, index_columns, unique_columns, enabled, last_update_date,
                      storage_mode, refresh_mode, where_predicate, ignored_sources,
                      partition_columns, partition_strategy, target_schema, max_one_row,
                      partition_depth)
                     VALUES ($1, $2, $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::TEXT[], $7, $8, $9, $10::jsonb, $11::TEXT[], $12::TEXT[], TRUE, NOW(), $13, $14, NULLIF($15, ''), $16::TEXT[], NULLIF($17, '{}')::TEXT[], NULLIF($18, ''), COALESCE(NULLIF($19, ''), current_schema()), $20, $21)";
```

Add the binding after the `max_one_row` param (`:224`). Because pgrx `DatumWithOid` needs a concrete value, bind NULL via an `Option`:

```rust
                    unsafe { DatumWithOid::new(row.max_one_row, oid_bool) },
                    unsafe { DatumWithOid::new(row.partition_depth, oid_int4) },
```

> `DatumWithOid::new(Option<i32>, INT4OID)` binds NULL for `None`. Confirm pgrx supports `Option<i32>` here (it does for nullable params); if the API needs a different form, use the same pattern other nullable params in this file use. `oid_int4` is already defined at `:126`.

- [ ] **Step 3: Set `partition_depth` where the main path builds its RegistryRow**

In `src/create_ivm/mod.rs`, find the `RegistryRow { ... }` literal in `persist_metadata` (around `:1390`, with `partition_strategy: Some(&ctx.plan.partition_strategy),`). Add:

```rust
            partition_depth: ctx.resolved_partition_depth,
```

- [ ] **Step 4: Build + verify persisted value via a pg_test**

Add to `src/tests/pg_test_subpartition.rs`:

```rust
#[pg_test]
fn pg_subpart_shallow_imv_persists_partition_depth() {
    Spi::run(
        "CREATE TABLE ssp (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, qty INT) \
         PARTITION BY LIST (dem_plan_id)",
    ).expect("root");
    Spi::run("CREATE TABLE ssp_172 PARTITION OF ssp FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("c");
    Spi::run("CREATE TABLE ssp_172_2025_01 PARTITION OF ssp_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("l");
    Spi::run("INSERT INTO ssp VALUES (172, '2025-01-15', 1)").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst_depth', \
            'SELECT dem_plan_id, COALESCE(order_date, order_date) AS order_date, qty FROM ssp', \
            'dem_plan_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id'])",
    ).expect("c").expect("r");

    let d = Spi::get_one::<i32>(
        "SELECT partition_depth FROM public.__reflex_ivm_reference WHERE name = 'fcst_depth'",
    ).unwrap();
    assert_eq!(d, Some(1));
}
```

Run: `cargo pgrx test pg17 -- pg_subpart_shallow_imv_persists_partition_depth`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/sql_writer/registry.rs src/create_ivm/mod.rs src/tests/pg_test_subpartition.rs
git commit -m "feat(partition): persist resolved partition_depth to catalog"
```

## Task 2.3: Depth-aware `reflex_sync_partitions_impl`

**Files:**
- Modify: `src/partition.rs:802` (`reflex_sync_partitions_impl`), specifically the metadata read (`:808`) and the tree walk (`:851`)

- [ ] **Step 1: Write the failing test (no re-deepen on sync)**

Add to `src/tests/pg_test_subpartition.rs`:

```rust
#[pg_test]
fn pg_subpart_sync_does_not_redeepen_shallow_imv() {
    Spi::run(
        "CREATE TABLE ssn (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, qty INT) \
         PARTITION BY LIST (dem_plan_id)",
    ).expect("root");
    Spi::run("CREATE TABLE ssn_172 PARTITION OF ssn FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("c");
    Spi::run("CREATE TABLE ssn_172_2025_01 PARTITION OF ssn_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("l");
    Spi::run("INSERT INTO ssn VALUES (172, '2025-01-15', 1)").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst_sync', \
            'SELECT dem_plan_id, COALESCE(order_date, order_date) AS order_date, qty FROM ssn', \
            'dem_plan_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id'])",
    ).expect("c").expect("r");

    // Sync must NOT add the order_date sub-level back.
    Spi::run("SELECT reflex_sync_partitions('fcst_sync', true)").expect("sync");
    assert!(!is_partitioned_rel("fcst_sync_ssn_172"),
        "sync must respect mirror_depth=1 and not re-deepen");
    assert_eq!(imv_child_count("fcst_sync"), 1);
}

#[pg_test]
fn pg_subpart_null_depth_mirrors_full_source() {
    Spi::run(
        "CREATE TABLE ssf (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, qty INT) \
         PARTITION BY LIST (dem_plan_id)",
    ).expect("root");
    Spi::run("CREATE TABLE ssf_172 PARTITION OF ssf FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("c");
    Spi::run("CREATE TABLE ssf_172_2025_01 PARTITION OF ssf_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("l");
    Spi::run("INSERT INTO ssf VALUES (172, '2025-01-15', 1)").expect("seed");
    // 2-level explicit -> full depth; partition_depth = 2.
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst_full', \
            'SELECT dem_plan_id, order_date, qty FROM ssf', \
            'dem_plan_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id','order_date'])",
    ).expect("c").expect("r");
    // Simulate a legacy row: NULL out partition_depth, then sync must still
    // mirror the full 2 levels (NULL => full depth).
    Spi::run("UPDATE public.__reflex_ivm_reference SET partition_depth = NULL WHERE name = 'fcst_full'").expect("u");
    Spi::run("SELECT reflex_sync_partitions('fcst_full', true)").expect("sync");
    assert!(is_partitioned_rel("fcst_full_ssf_172"),
        "NULL partition_depth must mirror full source depth (no truncation)");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo pgrx test pg17 -- pg_subpart_sync_does_not_redeepen_shallow_imv pg_subpart_null_depth_mirrors_full_source`
Expected: `sync_does_not_redeepen` FAILS — sync currently walks the full source tree and re-adds `fcst_sync_ssn_172` as a sub-partitioned node.

- [ ] **Step 3: Read `partition_depth` in the metadata query**

In `reflex_sync_partitions_impl`, the metadata `SELECT` (`:809-811`). Add `partition_depth`:

```rust
        let meta = client
            .select(
                "SELECT partition_columns, partition_strategy, depends_on, storage_mode, partition_depth \
                 FROM public.__reflex_ivm_reference WHERE name = $1",
```

After reading `unlogged` (`:839-843`), add:

```rust
        let partition_depth: Option<i32> = row
            .get_by_name::<i32, _>("partition_depth")
            .unwrap_or(None);
```

- [ ] **Step 4: Truncate the source `nodes` to mirror_depth**

In `reflex_sync_partitions_impl`, the line `let nodes = list_partition_tree(client, &anchor);` (`:851`). Replace with:

```rust
        let full_nodes = list_partition_tree(client, &anchor);
        let mirror_depth = partition_depth
            .map(|d| d as usize)
            .unwrap_or_else(|| max_tree_depth(&full_nodes));
        let nodes = truncate_partition_tree(full_nodes, mirror_depth);
```

> `nodes` then drives `src_expected_int` / `src_expected_tgt` (`:885-892`), the create loop (`:942-959`), and the drop-orphans loop (which drops any IMV child not in `src_expected_*`). Truncation therefore (a) stops creating deeper nodes and (b) makes any previously-materialized deeper IMV node an orphan that drop_orphans removes — exactly the no-re-deepen behavior. The demotion clears `sub_columns`, so a re-created boundary node is a plain leaf.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo pgrx test pg17 -- pg_subpart_sync_does_not_redeepen_shallow_imv pg_subpart_null_depth_mirrors_full_source`
Expected: both PASS.

- [ ] **Step 6: Commit**

```bash
git add src/partition.rs src/tests/pg_test_subpartition.rs
git commit -m "feat(partition): sync truncates source tree to IMV mirror_depth"
```

## Task 2.4: Phase 2 gate

- [ ] **Step 1: Full gate**

Run: `cargo fmt && cargo clippy --all-targets -- -D warnings && cargo pgrx test pg17 -- pg_subpart_`
Expected: green.

- [ ] **Step 2: Commit fixups**

```bash
git add -A && git commit -m "chore: fmt + clippy for phase 2 partition-depth"
```

---

# PHASE 3 — Shallow-aware capture (snapshot ancestors + flush/reconcile up-mapping)

## Task 3.1: Add `ancestors TEXT[]` to the snapshot and populate it

**Files:**
- Modify: `src/lib.rs:166` (snapshot DDL), `src/partition.rs:1628` (`refresh_source_snapshot`)

- [ ] **Step 1: Add the column**

In `src/lib.rs`, the `__reflex_source_partition_snapshot` DDL (`:166-172`), add `ancestors` before the PK:

```sql
    CREATE TABLE IF NOT EXISTS public.__reflex_source_partition_snapshot (
        source_root TEXT NOT NULL,
        child_name  TEXT NOT NULL,
        child_oid   BIGINT NOT NULL,
        bound       TEXT,
        ancestors   TEXT[],
        PRIMARY KEY (source_root, child_name)
    );
```

Add an idempotent migration near the other `ADD COLUMN IF NOT EXISTS` statements:

```sql
    ALTER TABLE public.__reflex_source_partition_snapshot ADD COLUMN IF NOT EXISTS ancestors TEXT[];
```

- [ ] **Step 2: Write the failing unit test for the ancestor-chain helper**

Add to `src/tests/unit_partition.rs`:

```rust
#[test]
fn test_leaf_ancestor_chain_root_first() {
    // ss_172 (depth1, internal) -> ss_172_2025_01 (depth2, leaf)
    let tree = vec![
        node("ss_172", "ss", 1, Some("RANGE")),
        node("ss_172_2025_01", "ss_172", 2, None),
    ];
    // Ancestor chain of the leaf, root-first, EXCLUDING the leaf itself.
    let chain = leaf_ancestor_chain(&tree, "ss_172_2025_01");
    assert_eq!(chain, vec!["ss_172".to_string()]);
}

#[test]
fn test_leaf_ancestor_chain_of_top_level_leaf_is_empty() {
    let tree = vec![node("ss_a", "ss", 1, None)];
    assert!(leaf_ancestor_chain(&tree, "ss_a").is_empty());
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test --lib test_leaf_ancestor_chain`
Expected: FAIL — `cannot find function leaf_ancestor_chain`.

- [ ] **Step 4: Implement `leaf_ancestor_chain` + populate snapshot**

In `src/partition.rs`, after `source_level_columns` (Task 1.3), add:

```rust
/// Root-first list of a node's ancestor bare-names within `tree` (excluding
/// the node itself). Walks `parent_bare` until the parent is no longer a node
/// in the tree (i.e. it is the anchor root). Used so a swapped-out leaf can
/// still be mapped to its mirror-depth ancestor from the snapshot.
pub(crate) fn leaf_ancestor_chain(tree: &[PartitionNode], leaf_bare: &str) -> Vec<String> {
    use std::collections::HashMap;
    let by_name: HashMap<&str, &PartitionNode> =
        tree.iter().map(|n| (n.bare_name.as_str(), n)).collect();
    let mut chain: Vec<String> = Vec::new();
    let mut cursor = leaf_bare;
    while let Some(node) = by_name.get(cursor) {
        let parent = node.parent_bare.as_str();
        if by_name.contains_key(parent) {
            chain.push(parent.to_string());
            cursor = parent;
        } else {
            break;
        }
    }
    chain.reverse(); // root-first
    chain
}
```

Update `refresh_source_snapshot` (`:1628`) to capture and store the ancestor chain. Replace its body:

```rust
pub(crate) fn refresh_source_snapshot(client: &mut pgrx::spi::SpiClient<'_>, source_root: &str) {
    let key = canonical_root_key(client, source_root);
    let _ = client.update(
        "DELETE FROM public.__reflex_source_partition_snapshot WHERE source_root = $1",
        None,
        &[unsafe { DatumWithOid::new(key.clone(), pgrx::pg_sys::TEXTOID) }],
    );
    let tree = list_partition_tree(client, source_root);
    let leaves: Vec<&PartitionNode> = tree.iter().filter(|n| n.sub_strategy.is_none()).collect();
    for leaf in leaves {
        let ancestors = leaf_ancestor_chain(&tree, &leaf.bare_name);
        let ancestors_arr = format_pg_text_array(&ancestors);
        let _ = client.update(
            "INSERT INTO public.__reflex_source_partition_snapshot \
                 (source_root, child_name, child_oid, bound, ancestors) \
             VALUES ($1, $2, $3, NULL, $4::TEXT[]) \
             ON CONFLICT (source_root, child_name) \
                 DO UPDATE SET child_oid = EXCLUDED.child_oid, ancestors = EXCLUDED.ancestors",
            None,
            &[
                unsafe { DatumWithOid::new(key.clone(), pgrx::pg_sys::TEXTOID) },
                unsafe { DatumWithOid::new(leaf.bare_name.clone(), pgrx::pg_sys::TEXTOID) },
                unsafe { DatumWithOid::new(leaf.oid as i64, pgrx::pg_sys::INT8OID) },
                unsafe { DatumWithOid::new(ancestors_arr, pgrx::pg_sys::TEXTOID) },
            ],
        );
    }
}
```

> `format_pg_text_array` is used elsewhere in `partition.rs`/`registry.rs`; confirm it is in scope (`rg -n "format_pg_text_array" src/partition.rs`). If not, import it: `use crate::sql_writer::registry::format_pg_text_array;` or the canonical path (`rg -n "pub fn format_pg_text_array" src/`).

- [ ] **Step 5: Run unit test to verify it passes**

Run: `cargo test --lib test_leaf_ancestor_chain`
Expected: PASS.

- [ ] **Step 6: Build (snapshot DDL change)**

Run: `cargo build`
Expected: compiles.

- [ ] **Step 7: Commit**

```bash
git add src/lib.rs src/partition.rs src/tests/unit_partition.rs
git commit -m "feat(partition): snapshot carries leaf ancestor chain"
```

## Task 3.2: Up-mapping helper — source leaf → IMV node at mirror_depth

**Files:**
- Modify: `src/partition.rs` (add helper near `expand_source_partition_to_leaves` `:1415`)
- Test: `src/tests/unit_partition.rs`

- [ ] **Step 1: Write the failing unit test**

Add to `src/tests/unit_partition.rs`:

```rust
#[test]
fn test_ancestor_at_depth_picks_correct_level() {
    // chain root-first for a depth-3 leaf: [lvl1, lvl2]; leaf itself is lvl3.
    let chain = vec!["p_172".to_string(), "p_172_2025".to_string()];
    // mirror_depth 1 -> the depth-1 ancestor.
    assert_eq!(ancestor_bare_at_depth(&chain, "p_172_2025_03", 1).as_deref(), Some("p_172"));
    // mirror_depth 2 -> the depth-2 ancestor.
    assert_eq!(ancestor_bare_at_depth(&chain, "p_172_2025_03", 2).as_deref(), Some("p_172_2025"));
    // mirror_depth 3 -> the leaf itself (no climb).
    assert_eq!(ancestor_bare_at_depth(&chain, "p_172_2025_03", 3).as_deref(), Some("p_172_2025_03"));
    // mirror_depth beyond leaf depth -> the leaf itself.
    assert_eq!(ancestor_bare_at_depth(&chain, "p_172_2025_03", 9).as_deref(), Some("p_172_2025_03"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib test_ancestor_at_depth_picks_correct_level`
Expected: FAIL — `cannot find function ancestor_bare_at_depth`.

- [ ] **Step 3: Implement**

In `src/partition.rs`, near the up-mapping helpers (after `target_leaves_under` `:1446`):

```rust
/// Given a leaf's root-first `ancestor_chain` and the leaf's own bare-name,
/// return the bare-name of the node at absolute depth `mirror_depth`. The
/// chain holds depths 1..=(leaf_depth-1); the leaf is at depth
/// `chain.len()+1`. A `mirror_depth` >= the leaf's own depth returns the leaf.
pub(crate) fn ancestor_bare_at_depth(
    ancestor_chain: &[String],
    leaf_bare: &str,
    mirror_depth: usize,
) -> Option<String> {
    if mirror_depth == 0 {
        return None;
    }
    if mirror_depth <= ancestor_chain.len() {
        // depth k -> chain index k-1 (root-first).
        ancestor_chain.get(mirror_depth - 1).cloned()
    } else {
        // mirror_depth >= leaf depth -> the leaf itself.
        Some(leaf_bare.to_string())
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib test_ancestor_at_depth_picks_correct_level`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/partition.rs src/tests/unit_partition.rs
git commit -m "feat(partition): ancestor_bare_at_depth up-mapping helper"
```

## Task 3.3: Shallow-aware reconcile resolution

Make `reflex_reconcile_partition_impl`'s `source_partition` path map source leaves up to the IMV's `mirror_depth` node before forming target child names.

**Files:**
- Modify: `src/partition.rs:1016` (`reflex_reconcile_partition_impl`, the `source_partition` block `:1095-1100`)

- [ ] **Step 1: Read `partition_depth` in the reconcile metadata query**

In `reflex_reconcile_partition_impl`, the catalog `SELECT` (`:1037`). Add `partition_depth`:

```rust
                "SELECT base_query, end_query, partition_columns, partition_strategy, depends_on, graph_child, storage_mode, partition_depth \
                 FROM public.__reflex_ivm_reference WHERE name = $1 AND enabled = TRUE",
```

After reading `storage_mode` (`:1073-1077`), add:

```rust
        let partition_depth: Option<i32> = row
            .get_by_name::<i32, _>("partition_depth")
            .unwrap_or(None);
```

- [ ] **Step 2: Resolve the anchor + mirror_depth, then up-map in the `source_partition` branch**

First, read `depends_on` from the metadata row (the query already selects it). Add this alongside the other field reads, right after the `partition_depth` read from Step 1:

```rust
        let depends_on: Vec<String> = row
            .get_by_name::<Vec<String>, _>("depends_on")
            .unwrap_or(None)
            .unwrap_or_default();
```

Then replace the `source_partition` branch (`:1095-1100`):

```rust
        if !source_partition.trim().is_empty() {
            // Level-agnostic + depth-aware path: expand the named source
            // partition to source leaves, map each UP to the IMV's mirror-depth
            // node, then to its IMV target child name. When the IMV mirrors the
            // full source depth this is the identity (leaf -> leaf).
            let anchor = resolve_anchor_source(client, part_col, &depends_on)
                .unwrap_or_default();
            let full_tree = if anchor.is_empty() {
                Vec::new()
            } else {
                list_partition_tree(client, &anchor)
            };
            let mirror_depth = partition_depth
                .map(|d| d as usize)
                .unwrap_or_else(|| max_tree_depth(&full_tree));
            for src_leaf in expand_source_partition_to_leaves(client, source_partition) {
                let chain = leaf_ancestor_chain(&full_tree, &src_leaf);
                let node = ancestor_bare_at_depth(&chain, &src_leaf, mirror_depth)
                    .unwrap_or_else(|| src_leaf.clone());
                to_process.insert(target_child_name(view_name, &node));
            }
        } else {
```

> `part_col` is already bound at `:1091` (`let part_col = &part_cols[0];`). `resolve_anchor_source` takes `(client, &str, &[String])` and is already used elsewhere in this file.

- [ ] **Step 3: Write the failing integration test (leaf swap refills the shallow IMV node)**

Add to `src/tests/pg_test_subpartition.rs`:

```rust
#[pg_test]
fn pg_subpart_shallow_reconcile_refills_dem_plan_node() {
    Spi::run(
        "CREATE TABLE ssr (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, qty INT) \
         PARTITION BY LIST (dem_plan_id)",
    ).expect("root");
    Spi::run("CREATE TABLE ssr_172 PARTITION OF ssr FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("c");
    Spi::run("CREATE TABLE ssr_172_2025_01 PARTITION OF ssr_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("l1");
    Spi::run("CREATE TABLE ssr_172_2025_02 PARTITION OF ssr_172 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("l2");
    Spi::run("INSERT INTO ssr VALUES (172, '2025-01-15', 10), (172, '2025-02-15', 20)").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst_rec', \
            'SELECT dem_plan_id, COALESCE(order_date, order_date) AS order_date, qty FROM ssr', \
            'dem_plan_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id'])",
    ).expect("c").expect("r");

    // Mutate one month directly, then reconcile via the source SUB-leaf name.
    Spi::run("INSERT INTO ssr VALUES (172, '2025-02-20', 5)").expect("mutate");
    let res = Spi::get_one::<String>(
        "SELECT reflex_reconcile_partition('fcst_rec', '', 'ssr_172_2025_02')",
    ).expect("reconcile").expect("res");
    assert!(!res.starts_with("ERROR"), "reconcile failed: {}", res);

    // The whole dem_plan_id=172 IMV node is refilled (all 3 rows).
    let n = Spi::get_one::<i64>("SELECT count(*)::int8 FROM fcst_rec WHERE dem_plan_id = 172").unwrap().unwrap();
    assert_eq!(n, 3);
}
```

- [ ] **Step 4: Run test (fails before Step 2 wired, passes after)**

Run: `cargo pgrx test pg17 -- pg_subpart_shallow_reconcile_refills_dem_plan_node`
Expected: PASS after Step 2. (If it errors with "no such target child", verify `target_child_name` scheme and that `ssr_172` maps to `fcst_rec_ssr_172`.)

- [ ] **Step 5: Commit**

```bash
git add src/partition.rs src/tests/pg_test_subpartition.rs
git commit -m "feat(partition): reconcile maps source leaves up to mirror_depth node"
```

## Task 3.4: Shallow-aware flush (drop + swap-fill up-mapping)

Make `reflex_flush_partitions_impl` resolve each diff action to the IMV's `mirror_depth` node per-IMV, using the snapshot's `ancestors` for gone leaves.

**Files:**
- Modify: `src/partition.rs:1659` (`reflex_flush_partitions_impl`)

- [ ] **Step 1: Select per-IMV depth + anchor; read snapshot ancestors**

In `reflex_flush_partitions_impl`, the IMV lookup query (`:1685-1691`). Add `partition_depth` and `depends_on`:

```rust
            let imvs: Vec<(String, Option<i32>, Vec<String>)> = client
                .select(
                    "SELECT name, partition_depth, depends_on FROM public.__reflex_ivm_reference \
                     WHERE partition_columns IS NOT NULL AND array_length(partition_columns,1) > 0 \
                       AND (depends_on @> ARRAY[$1] OR depends_on @> ARRAY[split_part($1,'.',2)])",
                    None,
                    &[unsafe { DatumWithOid::new(root.to_string(), pgrx::pg_sys::TEXTOID) }],
                )
                .map_err(|e| format!("flush: imv lookup failed: {}", e))?
                .filter_map(|r| {
                    let name = r.get_by_name::<&str, _>("name").ok().flatten()?.to_string();
                    let depth = r.get_by_name::<i32, _>("partition_depth").ok().flatten();
                    let deps = r.get_by_name::<Vec<String>, _>("depends_on").ok().flatten().unwrap_or_default();
                    Some((name, depth, deps))
                })
                .collect();
```

The snapshot read (`:1703-1715`) currently returns `(String, u32)`. Extend it to also fetch `ancestors`, into a name→ancestors map:

```rust
            let snapshot_ancestors: std::collections::HashMap<String, Vec<String>> = client
                .select(
                    "SELECT child_name, COALESCE(ancestors, ARRAY[]::TEXT[]) AS ancestors \
                     FROM public.__reflex_source_partition_snapshot WHERE source_root = $1",
                    None,
                    &[unsafe { DatumWithOid::new(root_key.clone(), pgrx::pg_sys::TEXTOID) }],
                )
                .map_err(|e| format!("flush: snapshot ancestors read failed: {}", e))?
                .filter_map(|r| {
                    let n = r.get_by_name::<&str, _>("child_name").ok().flatten()?.to_string();
                    let a = r.get_by_name::<Vec<String>, _>("ancestors").ok().flatten().unwrap_or_default();
                    Some((n, a))
                })
                .collect();
```

Keep the existing `snapshot: Vec<(String,u32)>` read and `classify_partition_diff` call (`:1717-1718`) as-is — actions are still leaf-level.

- [ ] **Step 2: Compute the live tree once per root for ancestor climbs**

After `let current = current_source_leaf_oids(client, root);` (`:1717`), add a live tree for climbing survivors:

```rust
            let live_tree = list_partition_tree(client, root);
```

- [ ] **Step 3: Rewrite the per-IMV action loop to up-map**

Replace the `for imv in &imvs { ... }` body (`:1720-1763`) with depth-aware mapping. For each imv, resolve `mirror_depth`, then translate leaf actions into a deduped set of `(imv_node_bare, kind)` where kind is Drop or SwapFill, and emit drops then reconciles:

```rust
            for (imv, depth_opt, _deps) in &imvs {
                let mirror_depth = depth_opt
                    .map(|d| d as usize)
                    .unwrap_or_else(|| max_tree_depth(&live_tree));

                // Map each leaf action up to the IMV's mirror-depth node.
                // - SwapFill / AttachNew (leaf in live tree): climb live tree.
                // - Drop (leaf gone): read ancestor from snapshot; swap-fill if
                //   that ancestor still exists, else drop the IMV node.
                use std::collections::BTreeSet;
                let mut to_swap: BTreeSet<String> = BTreeSet::new();
                let mut to_drop: BTreeSet<String> = BTreeSet::new();

                let live_names: std::collections::HashSet<&str> =
                    live_tree.iter().map(|n| n.bare_name.as_str()).collect();

                for (leaf, action) in &actions {
                    match action {
                        PartitionDiffAction::SwapFill | PartitionDiffAction::AttachNew => {
                            let chain = leaf_ancestor_chain(&live_tree, leaf);
                            let node = ancestor_bare_at_depth(&chain, leaf, mirror_depth)
                                .unwrap_or_else(|| leaf.clone());
                            to_swap.insert(node);
                        }
                        PartitionDiffAction::Drop => {
                            let chain = snapshot_ancestors.get(leaf).cloned().unwrap_or_default();
                            let node = ancestor_bare_at_depth(&chain, leaf, mirror_depth)
                                .unwrap_or_else(|| leaf.clone());
                            if live_names.contains(node.as_str()) {
                                // Sibling removed, ancestor survives -> refill it.
                                to_swap.insert(node);
                            } else {
                                // Whole mirror-depth node gone -> drop IMV node.
                                to_drop.insert(node);
                            }
                        }
                    }
                }
                // A node both dropped and swap-filled: drop wins (it is gone).
                for d in &to_drop {
                    to_swap.remove(d);
                }

                let (schema_opt, _) = split_qualified_name(imv);
                let schema = schema_opt.unwrap_or("public");
                for node in &to_drop {
                    let tgt = target_child_name(imv, node);
                    let int = intermediate_child_name(imv, node);
                    let _ = client.update(
                        &format!("DROP TABLE IF EXISTS \"{}\".\"{}\" CASCADE", schema, tgt),
                        None, &[],
                    );
                    let _ = client.update(
                        &format!("DROP TABLE IF EXISTS \"{}\".\"{}\" CASCADE", schema, int),
                        None, &[],
                    );
                }
                for node in &to_swap {
                    let q = format!(
                        "SELECT public.reflex_reconcile_partition({}, '', {})",
                        sql_literal_text(imv),
                        sql_literal_text(node)
                    );
                    client
                        .update(&q, None, &[])
                        .map_err(|e| format!("flush reconcile {} {}: {}", imv, node, e))?;
                }
                summary.push(format!("{}: {} change(s)", imv, actions.len()));
            }
```

> Note: `reflex_reconcile_partition(imv, '', node)` with `node` being a mirror-depth source node (e.g. `ssr_172`) is itself depth-aware after Task 3.3 — it expands `ssr_172` to its source leaves and maps each back up to `ssr_172` (identity at the boundary), producing one IMV target child to swap. The double resolution is consistent and idempotent.

- [ ] **Step 4: Write the failing integration test (attach-new-month collapses, no new IMV leaf)**

Add to `src/tests/pg_test_subpartition.rs`:

```rust
#[pg_test]
fn pg_subpart_shallow_flush_attach_month_collapses_into_node() {
    Spi::run(
        "CREATE TABLE ssm (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, qty INT) \
         PARTITION BY LIST (dem_plan_id)",
    ).expect("root");
    Spi::run("CREATE TABLE ssm_172 PARTITION OF ssm FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("c");
    Spi::run("CREATE TABLE ssm_172_2025_01 PARTITION OF ssm_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("l1");
    Spi::run("INSERT INTO ssm VALUES (172, '2025-01-15', 10)").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst_flush', \
            'SELECT dem_plan_id, COALESCE(order_date, order_date) AS order_date, qty FROM ssm', \
            'dem_plan_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id'])",
    ).expect("c").expect("r");

    // Attach a brand-new month leaf to the source (DDL — no DML trigger).
    Spi::run("CREATE TABLE ssm_172_2025_02 (LIKE ssm_172 INCLUDING ALL)").expect("staging");
    Spi::run("INSERT INTO ssm_172_2025_02 VALUES (172, '2025-02-15', 20)").expect("fill staging");
    Spi::run("ALTER TABLE ssm_172 ATTACH PARTITION ssm_172_2025_02 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("attach");

    // Flush the source root.
    let res = Spi::get_one::<String>("SELECT reflex_flush_partitions('public.ssm')").expect("flush").expect("res");
    assert!(!res.starts_with("ERROR"), "flush failed: {}", res);

    // IMV still has exactly ONE child (dem_plan_id node), now holding both months.
    assert_eq!(imv_child_count("fcst_flush"), 1);
    let n = Spi::get_one::<i64>("SELECT count(*)::int8 FROM fcst_flush").unwrap().unwrap();
    assert_eq!(n, 2);
}
```

> Confirm the `reflex_flush_partitions` SQL function name/signature with `rg -n "fn reflex_flush_partitions" src/lib.rs` — it may take the root as TEXT. Adjust the call if the registered signature differs (e.g. `reflex_flush_partitions('ssm')` bare).

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo pgrx test pg17 -- pg_subpart_shallow_flush_attach_month_collapses_into_node`
Expected: PASS.

- [ ] **Step 6: Regression — full-depth flush still works**

Run: `cargo pgrx test pg17 -- pg_subpart_`
Confirm pre-existing full-depth flush/reconcile tests (e.g. any `pg_subpart_*flush*` or `pg_test_partition.rs` swap tests) still pass:

Run: `cargo pgrx test pg17 -- pg_partition_`
Expected: PASS (NULL depth path = identity up-mapping).

- [ ] **Step 7: Commit**

```bash
git add src/partition.rs src/tests/pg_test_subpartition.rs
git commit -m "feat(partition): shallow-aware flush up-maps leaves to mirror_depth node"
```

## Task 3.5: Phase 3 gate

- [ ] **Step 1: Full gate**

Run: `cargo fmt && cargo clippy --all-targets -- -D warnings && cargo pgrx test pg17`
Expected: green across the whole suite (not just `pg_subpart_`).

- [ ] **Step 2: Commit fixups**

```bash
git add -A && git commit -m "chore: fmt + clippy for phase 3 partition-depth"
```

---

# PHASE 4 — Audit drift-check + differential fuzz + docs

## Task 4.1: Depth-aware audit drift-check

**Files:**
- Modify: `src/audit/checks_b_drift.rs`

- [ ] **Step 1: Read the current drift check**

Run: `rg -n "fn |list_partition_tree|leaf|row_count|partition_depth" src/audit/checks_b_drift.rs`
Identify where it walks the source recursive leaf set and compares to the IMV's leaves + per-leaf row counts.

- [ ] **Step 2: Write the failing integration test**

Add to `src/tests/pg_test_subpartition.rs`:

```rust
#[pg_test]
fn pg_subpart_shallow_imv_audit_no_drift() {
    Spi::run(
        "CREATE TABLE ssaud (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, qty INT) \
         PARTITION BY LIST (dem_plan_id)",
    ).expect("root");
    Spi::run("CREATE TABLE ssaud_172 PARTITION OF ssaud FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("c");
    Spi::run("CREATE TABLE ssaud_172_2025_01 PARTITION OF ssaud_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("l");
    Spi::run("INSERT INTO ssaud VALUES (172, '2025-01-15', 10)").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst_audit', \
            'SELECT dem_plan_id, COALESCE(order_date, order_date) AS order_date, qty FROM ssaud', \
            'dem_plan_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id'])",
    ).expect("c").expect("r");

    // The drift audit must NOT flag the shallow IMV for "missing" the
    // source's order_date sub-level. Call the audit entrypoint and assert clean.
    let drift = Spi::get_one::<i64>(
        "SELECT count(*)::int8 FROM reflex_audit('fcst_audit') WHERE severity = 'ERROR'",
    ).unwrap().unwrap_or(0);
    assert_eq!(drift, 0, "shallow IMV must not be flagged as drifted");
}
```

> Confirm the audit entrypoint name + output columns with `rg -n "pub fn reflex_audit|fn reflex_audit|severity" src/audit/`. Adjust the query to the actual signature (it may be `SELECT * FROM reflex_audit()` returning a set, or a scalar). If the audit returns a text report, assert it does not contain a partition-tree-mismatch marker instead.

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo pgrx test pg17 -- pg_subpart_shallow_imv_audit_no_drift`
Expected: FAIL — the drift check compares the source's full leaf set (the `order_date` months) against the IMV's depth-1 leaves and reports the months as missing.

- [ ] **Step 4: Aggregate source leaves to `mirror_depth` before comparing**

In `src/audit/checks_b_drift.rs`, read the IMV's `partition_depth` from `__reflex_ivm_reference`, resolve `mirror_depth = partition_depth.unwrap_or(max_tree_depth(source_tree))`, and apply `crate::partition::truncate_partition_tree(source_tree, mirror_depth)` to the source node set before building the expected IMV-leaf set + row-count comparison. Per-leaf row counts then compare the IMV's depth-`mirror_depth` leaf against the SUM of the source leaves beneath it.

Concretely (adapt to the file's existing structure): where it currently does the equivalent of

```rust
let source_leaves = list_partition_tree(client, &anchor).into_iter().filter(|n| n.sub_strategy.is_none());
```

change to:

```rust
let full = crate::partition::list_partition_tree(client, &anchor);
let mirror_depth = partition_depth
    .map(|d| d as usize)
    .unwrap_or_else(|| crate::partition::max_tree_depth(&full));
let source_leaves = crate::partition::truncate_partition_tree(full, mirror_depth)
    .into_iter()
    .filter(|n| n.sub_strategy.is_none());
```

For per-leaf row counts: count source rows grouped by the depth-`mirror_depth` ancestor (i.e. count against the *truncated* leaf's bound predicate, which is the ancestor predicate like `dem_plan_id = 172`), so the SUM of the source months under 172 is compared to the IMV's single 172 leaf. If the existing check counts `SELECT count(*) FROM <source_leaf>`, switch to counting against the truncated node's constraint, e.g. `SELECT count(*) FROM <anchor_root> WHERE <truncated_node_constraintdef>`.

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo pgrx test pg17 -- pg_subpart_shallow_imv_audit_no_drift`
Expected: PASS.

- [ ] **Step 6: Regression — full-depth audit unaffected**

Run: `cargo pgrx test pg17 -- pg_audit_ pg_subpart_`
Expected: PASS (NULL depth = full tree = today's behavior).

- [ ] **Step 7: Commit**

```bash
git add src/audit/checks_b_drift.rs src/tests/pg_test_subpartition.rs
git commit -m "feat(audit): drift check aggregates source leaves to IMV mirror_depth"
```

## Task 4.2: Differential fuzz — shallow IMV on deep source

**Files:**
- Modify: `src/tests/pg_test_fuzz.rs`

- [ ] **Step 1: Locate the existing fuzz harness**

Run: `rg -n "fn |EXCEPT|attach|detach|swap|flush|oracle|recompute" src/tests/pg_test_fuzz.rs | head -40`
Identify the random attach/detach/swap + flush sequence and the `IMV EXCEPT source-recompute = ∅` oracle assertion.

- [ ] **Step 2: Add a shallow-IMV fuzz case**

Add a new `#[pg_test]` that mirrors the existing fuzz body but (a) builds a 2-level `LIST→RANGE` source, (b) creates the IMV with `partition_by:[dem_plan_id]` (depth 1) and a non-bare `order_date` projection, and (c) after each `reflex_flush_partitions`, asserts the oracle. Reuse the harness's sequence generator if it is a free function; otherwise replicate the loop. The oracle for a shallow IMV:

```rust
// IMV must equal a full recompute of the base query (modulo the coalesced
// order_date rename). No EXCEPT rows in either direction.
let drift = Spi::get_one::<i64>(
    "SELECT count(*)::int8 FROM ( \
        (SELECT dem_plan_id, order_date, qty FROM fcst_fuzz \
         EXCEPT SELECT dem_plan_id, order_date, qty FROM <recompute_view>) \
        UNION ALL \
        (SELECT dem_plan_id, order_date, qty FROM <recompute_view> \
         EXCEPT SELECT dem_plan_id, order_date, qty FROM fcst_fuzz) \
     ) d",
).unwrap().unwrap_or(0);
assert_eq!(drift, 0, "shallow IMV drifted from source recompute");
```

> `<recompute_view>` = a fresh `SELECT` over the live source equivalent to the IMV's base query. Follow exactly how the existing fuzz test materializes its oracle (it likely runs the base query directly). Keep the random seed deterministic by varying only by loop index (no `Math.random`/wall-clock — pgrx tests must be reproducible).

- [ ] **Step 3: Run the fuzz test**

Run: `cargo pgrx test pg17 -- pg_fuzz`
Expected: PASS (new + existing fuzz cases green).

- [ ] **Step 4: Commit**

```bash
git add src/tests/pg_test_fuzz.rs
git commit -m "test(fuzz): shallow-IMV-on-deep-source differential oracle"
```

## Task 4.3: Docs + CHANGELOG + version bump

**Files:**
- Modify: `CHANGELOG.md`, `docs/concepts/internals.md` (Partitioning section), `Cargo.toml`

- [ ] **Step 1: CHANGELOG entry**

Add to `CHANGELOG.md` (top, under a new version heading — bump the patch/minor per repo convention; current is 1.8.1):

```markdown
## 1.8.2

### Added
- **Partitioned IMVs can now mirror their source at a shallower depth.** When
  `partition_by` declares fewer levels than the source is partitioned by (or
  when an auto-mirrored sub-level's column is not a bare projected output
  column), the IMV mirrors only the declared/projectable levels instead of
  being rejected. Capture maintenance still works at the coarser (correct,
  heavier) granularity: a source sub-partition change refills the whole
  matching top-level IMV partition. New nullable catalog column
  `__reflex_ivm_reference.partition_depth` (NULL = mirror full source depth,
  so existing IMVs are unaffected). See
  `plans/2026-06-03-imv-partition-depth.md`.
```

- [ ] **Step 2: internals doc**

In `docs/concepts/internals.md`, find the Partitioning section (`rg -n -i "partition" docs/concepts/internals.md`) and add a subsection describing: `partition_depth` semantics (NULL = full), `truncate_partition_tree`, the leaf-granular-diff + up-map capture model, and the user-facing contract ("finer sub-partitioning is opt-in via projection shape + `partition_by`; otherwise the coarsest correct level is used").

- [ ] **Step 3: Version bump**

In `Cargo.toml`, bump `version = "1.8.1"` to `version = "1.8.2"`.

- [ ] **Step 4: Commit**

```bash
git add CHANGELOG.md docs/concepts/internals.md Cargo.toml
git commit -m "docs: partition-depth changelog + internals; bump 1.8.2"
```

## Task 4.4: Final full gate

- [ ] **Step 1: Everything green**

Run: `cargo fmt --check`
Run: `cargo clippy --all-targets -- -D warnings`
Run: `cargo pgrx check`
Run: `cargo pgrx test`
Expected: all green, entire suite.

- [ ] **Step 2: Manually verify the motivating view shape (optional sanity)**

If a representative DB is available, create the `forecast_analysis_view` IMV from the spec and confirm it creates without the `partition key column 'order_date' ... is not a bare projected output column` error and produces a single-level `LIST(dem_plan_id)` target. Otherwise the `pg_subpart_explicit_shallow_*` test is the proxy.

- [ ] **Step 3: Commit any final fixups**

```bash
git add -A && git commit -m "chore: final gate for IMV partition-depth feature"
```

---

## Self-review checklist (for the implementer before opening a PR)

- [ ] `forecast_analysis_view`-shaped IMV (explicit `partition_by:[dem_plan_id]`, `COALESCE`-projected `order_date`) creates as single-level `LIST(dem_plan_id)`.
- [ ] Explicit `partition_by:[dem_plan_id, order_date]` on a bare-projected `order_date` still mirrors 2 levels.
- [ ] Auto-mirror with non-bare sub-level prunes to depth 1; with bare sub-level mirrors 2.
- [ ] `partition_depth IS NULL` ⇒ full-depth mirror (existing IMVs unchanged).
- [ ] Sync does not re-deepen a shallow IMV.
- [ ] Source month swap/attach/detach refills/drops the correct shallow IMV node; `IMV EXCEPT recompute = ∅`.
- [ ] Audit drift-check clean on a shallow IMV.
- [ ] `cargo fmt --check`, `cargo clippy -D warnings`, `cargo pgrx check`, `cargo pgrx test` all green.
- [ ] Removed the leftover `REFLEX-DBG` `notice!` lines in `resolve_anchor_source` only if they are out of scope — do NOT bundle unrelated cleanup; leave them unless they break a test.
