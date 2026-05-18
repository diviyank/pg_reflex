# Pipeline Decomposition Refactor (1.6.2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the 1.6.x refactor by (a) decomposing the 1290-line main body of `create_reflex_ivm_impl` into a sequence of named pipeline phases threading a `BuildContext` struct and `&mut SpiClient<'_>`, and (b) extracting the four remaining branches of `reflex_build_delta_sql` (self-join, outer-join-secondary, passthrough, aggregate-epilogue) into focused helpers — without changing emitted SQL by one byte.

**Architecture:**
- `create_ivm.rs` gains a private `BuildContext<'a>` struct holding the mutable plan, owned analysis, parsed inputs, and resolution outputs (unique cols, partition cols, ivm_froms, depth, unlogged_tables). The post-decomposition body of `create_reflex_ivm_impl` becomes ~12 helper calls; each helper is a free `fn` taking `(&mut SpiClient<'_>, &mut BuildContext)` (or shared-only refs where it doesn't mutate).
- `trigger.rs` gains four private free functions inside `reflex_build_delta_sql`'s outer scope: `self_join_full_refresh_stmts`, `outer_join_secondary_stmts`, `passthrough_op_stmts`, `aggregate_epilogue_stmts`. The epilogue takes `pending_dispatch: Option<PendingDispatch>` by value to make the signaling explicit.
- All extractions are pure code moves: no behavior change, no SQL change. The bar is byte-identical output verified by snapshot tests (Phase A) and the existing pg_test suite + EXCEPT ALL oracle (Phase C).

**Tech Stack:** Rust + pgrx 0.18, pgrx `Spi`/`SpiClient` API, `serde_json` for plan deserialization, `cargo pgrx test` for `#[pg_test]` integration tests, `cargo test` for unit tests in `src/tests/unit_*.rs`.

---

## File Structure

Files modified (no new files):

| File | Change |
|---|---|
| `src/create_ivm.rs` | Add `struct BuildContext<'a>`. Extract ~9 pipeline phases as private free functions. The `create_reflex_ivm_impl` body shrinks from 1290 lines to ~80 lines. |
| `src/trigger.rs` | Extract 4 branches of `reflex_build_delta_sql` into private free functions. `reflex_build_delta_sql` body shrinks from ~708 lines to ~120 lines (a top-level branch switch). |
| `src/tests/unit_trigger.rs` | Add 8 snapshot tests pinning `reflex_build_delta_sql` output for all 4 deferred branches. |

No new files, no `Cargo.toml` change, no public-API change. No SQL/DDL change.

---

## Risk notes for the implementer

These three concerns from the previous cycle drove the deferral. Read them before touching code:

1. **State sharing across `create_reflex_ivm_impl` phases.** Earlier phases compute `froms`, `real_source_names`, `is_join_query`, `resolved_unique_columns`, `resolved_partition_cols`, `resolved_strategy`. The SPI block then derives `ivm_froms`, `depth`, `unlogged_tables`, mutates `plan.partition_columns`/`plan.partition_strategy`/`plan.anchor_source`/`plan.partition_join_paths`/`plan.imv_relevant_columns`, and finally `persist_metadata` consumes all of it. Solution: one `BuildContext<'a>` owns all this state; the helper signatures express what they read vs. mutate.

2. **`&mut SpiClient<'_>` threading.** Today the body uses two `Spi::connect(...)` calls (cycle/existence check, partition resolution) plus one big `Spi::connect_mut(...)` for everything else. Solution: keep the `Spi::connect_mut(...)` closure in the entry function; the closure body becomes a sequence of helper calls, each taking `&mut SpiClient<'_>` borrowed from the closure. The two pre-SPI helpers (`check_existence_and_cycle`, `resolve_partitioning`) keep their own short `Spi::connect(...)` blocks — they each do one read-only probe and inline is cleaner than threading the client through.

3. **`pending_dispatch` signaling from the UPDATE arm to the aggregate epilogue.** Today the local `PendingDispatch` struct lives inside `reflex_build_delta_sql` and `pending_dispatch: Option<PendingDispatch>` is mutated by the UPDATE arm and consumed by the epilogue. Solution: `PendingDispatch` becomes a module-private `struct` (still in `trigger.rs`); the per-op aggregate arm returns `Option<String>` (the merge SQL) which the caller wraps; the epilogue takes `pending_dispatch: Option<PendingDispatch>` as a by-value parameter.

---

## Phase A — Snapshot harness for `reflex_build_delta_sql` (tests first)

**Why:** The four branches we will extract emit non-trivial multi-statement SQL. Byte-identical output is the contract; snapshot tests are the only way to verify it.

### Task 1: Add snapshot helper + first 4 snapshots (self-join + OJS + passthrough)

**Files:**
- Modify: `src/tests/unit_trigger.rs` (append to the file)

- [ ] **Step 1: Write the failing tests**

Append to `src/tests/unit_trigger.rs`:

```rust
#[cfg(test)]
mod delta_sql_snapshots {
    use crate::trigger::reflex_build_delta_sql;

    // Minimal AggregationPlan JSON for an aggregate IMV: GROUP BY `region`, SUM(qty).
    // Two real sources to enable the OJS detection.
    const AGG_JSON_TWO_SOURCES: &str = r#"{
        "is_passthrough": false,
        "group_by_columns": ["region"],
        "group_by_aliases": {},
        "intermediate_columns": [
            {"name":"region","pg_type":"TEXT","source_aggregate":"","source_arg":"region","is_group_key":true},
            {"name":"qty","pg_type":"NUMERIC","source_aggregate":"SUM","source_arg":"qty","is_group_key":false}
        ],
        "needs_ivm_count": true,
        "end_query_mappings": [
            {"output_alias":"region","intermediate_name":"region","aggregate_type":"GROUP_KEY","cast_type":null},
            {"output_alias":"qty","intermediate_name":"qty","aggregate_type":"SUM","cast_type":null}
        ],
        "distinct_columns": [],
        "passthrough_columns": [],
        "passthrough_key_mappings": {},
        "imv_relevant_columns": {},
        "source_join_keys": {},
        "not_null_columns": [],
        "partition_columns": [],
        "partition_strategy": "",
        "anchor_source": "",
        "partition_join_paths": {}
    }"#;

    // Minimal passthrough plan with a per-source unique-key mapping.
    const PASSTHROUGH_JSON_WITH_MAPPING: &str = r#"{
        "is_passthrough": true,
        "group_by_columns": [],
        "group_by_aliases": {},
        "intermediate_columns": [],
        "needs_ivm_count": false,
        "end_query_mappings": [],
        "distinct_columns": [],
        "passthrough_columns": ["id"],
        "passthrough_key_mappings": {"orders":[["id","id"]]},
        "imv_relevant_columns": {},
        "source_join_keys": {},
        "not_null_columns": ["id"],
        "partition_columns": [],
        "partition_strategy": "",
        "anchor_source": "",
        "partition_join_paths": {}
    }"#;

    #[test]
    fn snapshot_self_join_insert_aggregate() {
        // Self-join: source_table appears twice in base_query → full-refresh path
        let base_q = "SELECT a.region, SUM(a.qty) AS qty FROM sales a JOIN sales b ON a.id = b.parent_id GROUP BY a.region";
        let end_q = "SELECT region, qty FROM __reflex_int_v GROUP BY region";
        let sql = reflex_build_delta_sql(
            "v",
            "sales",
            "INSERT",
            base_q,
            end_q,
            Some(AGG_JSON_TWO_SOURCES),
            base_q,
        );
        insta::assert_snapshot!("self_join_insert_aggregate", sql);
    }

    #[test]
    fn snapshot_self_join_delete_passthrough() {
        let base_q = "SELECT a.id, a.qty FROM sales a JOIN sales b ON a.id = b.parent_id";
        let sql = reflex_build_delta_sql(
            "v",
            "sales",
            "DELETE",
            base_q,
            "",
            Some(PASSTHROUGH_JSON_WITH_MAPPING),
            base_q,
        );
        insta::assert_snapshot!("self_join_delete_passthrough", sql);
    }

    #[test]
    fn snapshot_outer_join_secondary_delete_aggregate() {
        // `customers` is secondary in a LEFT JOIN, DELETE on customers
        let base_q = "SELECT o.region, SUM(o.qty) AS qty FROM orders o LEFT JOIN customers c ON c.id = o.customer_id GROUP BY o.region";
        let end_q = "SELECT region, qty FROM __reflex_int_v GROUP BY region";
        let sql = reflex_build_delta_sql(
            "v",
            "customers",
            "DELETE",
            base_q,
            end_q,
            Some(AGG_JSON_TWO_SOURCES),
            base_q,
        );
        insta::assert_snapshot!("outer_join_secondary_delete_aggregate", sql);
    }

    #[test]
    fn snapshot_passthrough_insert() {
        let base_q = "SELECT id, qty FROM orders";
        let sql = reflex_build_delta_sql(
            "v",
            "orders",
            "INSERT",
            base_q,
            "",
            Some(PASSTHROUGH_JSON_WITH_MAPPING),
            base_q,
        );
        insta::assert_snapshot!("passthrough_insert", sql);
    }
}
```

- [ ] **Step 2: Add `insta` as a dev-dependency**

`insta` is not currently in `Cargo.toml`. Add to `[dev-dependencies]` (sits after `proptest = "1"`):

```toml
insta = { version = "1", features = ["yaml"] }
```

Rationale: `insta` is the lightweight idiomatic snapshot crate for Rust. Hand-rolled golden strings would require 5+ manual steps per snapshot to record (run/panic/copy/paste/assert), versus one `INSTA_UPDATE=always` invocation. The dep is dev-only.

- [ ] **Step 3: Run the snapshots to record the baseline**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
INSTA_UPDATE=always cargo test --lib delta_sql_snapshots:: 2>&1 | tail -20
```

Expected: tests pass; new `.snap` files appear under `src/tests/snapshots/`.

- [ ] **Step 4: Commit baseline snapshots**

```bash
git add src/tests/unit_trigger.rs src/tests/snapshots Cargo.toml Cargo.lock
git commit -m "test: snapshot baseline for reflex_build_delta_sql branches (self-join, OJS, passthrough)"
```

---

### Task 2: Add snapshots for aggregate-branch operations + epilogue variants

**Files:**
- Modify: `src/tests/unit_trigger.rs` (extend the `delta_sql_snapshots` module)

- [ ] **Step 1: Write the failing tests**

Inside the `delta_sql_snapshots` module:

```rust
    // Single-source aggregate plan (no OJS, no self-join)
    const AGG_JSON_SINGLE_SOURCE: &str = r#"{
        "is_passthrough": false,
        "group_by_columns": ["region"],
        "group_by_aliases": {},
        "intermediate_columns": [
            {"name":"region","pg_type":"TEXT","source_aggregate":"","source_arg":"region","is_group_key":true},
            {"name":"qty","pg_type":"NUMERIC","source_aggregate":"SUM","source_arg":"qty","is_group_key":false}
        ],
        "needs_ivm_count": true,
        "end_query_mappings": [
            {"output_alias":"region","intermediate_name":"region","aggregate_type":"GROUP_KEY","cast_type":null},
            {"output_alias":"qty","intermediate_name":"qty","aggregate_type":"SUM","cast_type":null}
        ],
        "distinct_columns": [],
        "passthrough_columns": [],
        "passthrough_key_mappings": {},
        "imv_relevant_columns": {},
        "source_join_keys": {},
        "not_null_columns": ["region"],
        "partition_columns": [],
        "partition_strategy": "",
        "anchor_source": "",
        "partition_join_paths": {}
    }"#;

    #[test]
    fn snapshot_aggregate_insert() {
        let base_q = "SELECT region, SUM(qty) AS qty FROM sales GROUP BY region";
        let end_q  = "SELECT region, qty FROM __reflex_int_v GROUP BY region";
        let sql = reflex_build_delta_sql(
            "v", "sales", "INSERT", base_q, end_q,
            Some(AGG_JSON_SINGLE_SOURCE), base_q,
        );
        insta::assert_snapshot!("aggregate_insert", sql);
    }

    #[test]
    fn snapshot_aggregate_delete() {
        let base_q = "SELECT region, SUM(qty) AS qty FROM sales GROUP BY region";
        let end_q  = "SELECT region, qty FROM __reflex_int_v GROUP BY region";
        let sql = reflex_build_delta_sql(
            "v", "sales", "DELETE", base_q, end_q,
            Some(AGG_JSON_SINGLE_SOURCE), base_q,
        );
        insta::assert_snapshot!("aggregate_delete", sql);
    }

    #[test]
    fn snapshot_aggregate_update_with_dispatch() {
        // UPDATE on a grouped aggregate without MIN/MAX → pending_dispatch path
        let base_q = "SELECT region, SUM(qty) AS qty FROM sales GROUP BY region";
        let end_q  = "SELECT region, qty FROM __reflex_int_v GROUP BY region";
        let sql = reflex_build_delta_sql(
            "v", "sales", "UPDATE", base_q, end_q,
            Some(AGG_JSON_SINGLE_SOURCE), base_q,
        );
        insta::assert_snapshot!("aggregate_update_dispatch", sql);
    }

    #[test]
    fn snapshot_aggregate_epilogue_no_group_by() {
        // Aggregate IMV with no GROUP BY (global aggregate) → else branch of epilogue
        let plan = r#"{
            "is_passthrough": false,
            "group_by_columns": [],
            "group_by_aliases": {},
            "intermediate_columns": [
                {"name":"qty","pg_type":"NUMERIC","source_aggregate":"SUM","source_arg":"qty","is_group_key":false}
            ],
            "needs_ivm_count": false,
            "end_query_mappings": [
                {"output_alias":"qty","intermediate_name":"qty","aggregate_type":"SUM","cast_type":null}
            ],
            "distinct_columns": [], "passthrough_columns": [], "passthrough_key_mappings": {},
            "imv_relevant_columns": {}, "source_join_keys": {}, "not_null_columns": [],
            "partition_columns": [], "partition_strategy": "", "anchor_source": "", "partition_join_paths": {}
        }"#;
        let base_q = "SELECT SUM(qty) AS qty FROM sales";
        let end_q  = "SELECT qty FROM __reflex_int_v";
        let sql = reflex_build_delta_sql(
            "v", "sales", "INSERT", base_q, end_q,
            Some(plan), base_q,
        );
        insta::assert_snapshot!("aggregate_epilogue_no_group_by", sql);
    }
```

- [ ] **Step 2: Run snapshots to record baselines**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
INSTA_UPDATE=always cargo test --lib delta_sql_snapshots:: 2>&1 | tail -10
```

Expected: 4 new snapshots recorded under `src/tests/snapshots/`. All tests pass.

- [ ] **Step 3: Sanity-check the snapshots**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
ls src/tests/snapshots/ | wc -l
```

Expected: 8 `.snap` files (4 from Task 1 + 4 from Task 2). Open one and confirm it contains a SQL string with `--<<REFLEX_SEP>>--` separators (the multi-statement marker `reflex_build_delta_sql` uses).

- [ ] **Step 4: Commit**

```bash
git add src/tests/unit_trigger.rs src/tests/snapshots
git commit -m "test: snapshot baseline for reflex_build_delta_sql aggregate branches"
```

---

## Phase B — Extract `reflex_build_delta_sql` branches

Each task in this phase is a pure code move. The Phase A snapshots verify byte-identical output.

### Task 3: Extract `self_join_full_refresh_stmts`

**Files:**
- Modify: `src/trigger.rs:1942-1958` (the `if is_self_join { ... }` body) and add helper above `reflex_build_delta_sql`.

- [ ] **Step 1: Add the helper function**

Insert above `reflex_build_delta_sql` (around line 1820):

```rust
/// Self-join full refresh: source_table appears multiple times in base_query, so
/// the standard delta is wrong (every alias gets replaced with the same transition).
/// Both passthrough and aggregate paths rebuild from base_query.
fn self_join_full_refresh_stmts(
    view_name: &str,
    base_query: &str,
    end_query: &str,
    intermediate_tbl: &str,
    plan: &AggregationPlan,
    stmts: &mut Vec<String>,
) {
    let qv = quote_identifier(view_name);
    if plan.is_passthrough {
        stmts.push(format!("DELETE FROM {}", qv));
        stmts.push(format!("INSERT INTO {} {}", qv, base_query));
    } else {
        stmts.push(format!("TRUNCATE {}", intermediate_tbl));
        stmts.push(format!("INSERT INTO {} {}", intermediate_tbl, base_query));
        if end_query.is_empty() {
            stmts.push(format!("TRUNCATE {}", qv));
            stmts.push(format!("INSERT INTO {} {}", qv, base_query));
        } else {
            stmts.push(format!("TRUNCATE {}", qv));
            stmts.push(format!("INSERT INTO {} {}", qv, end_query));
        }
    }
}
```

- [ ] **Step 2: Replace the inline `if is_self_join` body**

In `reflex_build_delta_sql`, change lines 1942-1958 from:

```rust
    if is_self_join {
        // Self-join: full refresh (delta itself is wrong — both aliases get replaced).
        let qv = quote_identifier(view_name);
        if plan.is_passthrough {
            stmts.push(format!("DELETE FROM {}", qv));
            // ... (17 lines)
        }
    } else if is_outer_join_secondary && plan.is_passthrough {
```

To:

```rust
    if is_self_join {
        self_join_full_refresh_stmts(view_name, base_query, end_query, &intermediate_tbl, &plan, &mut stmts);
    } else if is_outer_join_secondary && plan.is_passthrough {
```

- [ ] **Step 3: Run snapshots — they must match byte-for-byte**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo test --lib delta_sql_snapshots:: 2>&1 | tail -15
```

Expected: PASS. If a snapshot mismatch occurs, the extraction changed the output — fix the helper to match the original exactly. Do NOT `INSTA_UPDATE` the snapshots; they are the contract.

- [ ] **Step 4: Run clippy + fmt**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo fmt --check && cargo clippy --all-targets -- -D warnings 2>&1 | tail -5
```

Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add src/trigger.rs
git commit -m "refactor: extract self_join_full_refresh_stmts from reflex_build_delta_sql"
```

---

### Task 4: Extract `outer_join_secondary_stmts`

**Files:**
- Modify: `src/trigger.rs:1959-2063` (the two `else if is_outer_join_secondary` arms — passthrough + aggregate).

- [ ] **Step 1: Add the helper function**

Insert above `reflex_build_delta_sql`:

```rust
/// Outer-join-secondary handling: when source_table is the secondary side of a
/// LEFT/RIGHT JOIN (or any side of FULL OUTER), the MERGE subtract can't represent
/// the NULL semantics. Passthrough → full refresh. Aggregate → targeted group
/// reconcile via the affected_tbl.
fn outer_join_secondary_stmts(
    view_name: &str,
    source_table: &str,
    operation: &str,
    base_query: &str,
    end_query: &str,
    plan: &AggregationPlan,
    grp_cols: &Option<Vec<String>>,
    intermediate_tbl: &str,
    affected_tbl: &str,
    old_tbl: &str,
    new_tbl: &str,
    stmts: &mut Vec<String>,
) {
    let qv = quote_identifier(view_name);

    if plan.is_passthrough {
        // Passthrough outer-join secondary: full refresh from source
        stmts.push(format!("DELETE FROM {}", qv));
        stmts.push(format!("INSERT INTO {} {}", qv, base_query));
        return;
    }

    if let Some(ref cols) = grp_cols {
        // Aggregate path: extract affected groups, delete + re-insert ONLY those groups.
        let select_expr = affected_groups_select(cols);

        let transition = if operation == "DELETE" { old_tbl } else { new_tbl };
        let delta_q = replace_source_with_transition(base_query, source_table, transition);

        stmts.push(format!("TRUNCATE {}", affected_tbl));
        stmts.push(format!(
            "INSERT INTO {} SELECT DISTINCT {} FROM ({}) AS __d",
            affected_tbl, select_expr, delta_q
        ));

        let ns_in_int = null_safe_in(
            affected_tbl, intermediate_tbl, cols, cols, &plan.not_null_columns,
        );
        stmts.push(format!("DELETE FROM {} WHERE {}", intermediate_tbl, ns_in_int));

        let ns_in_full = null_safe_in(
            affected_tbl, "__full", cols, cols, &plan.not_null_columns,
        );
        stmts.push(format!(
            "INSERT INTO {} SELECT * FROM ({}) AS __full WHERE {}",
            intermediate_tbl, base_query, ns_in_full
        ));

        let target_cols = target_group_columns(plan);
        let ns_in_tgt_delete = null_safe_in(
            affected_tbl, &qv, &target_cols, cols, &plan.not_null_columns,
        );
        stmts.push(format!("DELETE FROM {} WHERE {}", qv, ns_in_tgt_delete));

        let ns_in_tgt_insert = null_safe_in(
            affected_tbl, intermediate_tbl, cols, cols, &plan.not_null_columns,
        );
        stmts.push(format!(
            "INSERT INTO {} {} AND {}",
            qv, end_query, ns_in_tgt_insert
        ));
    } else {
        // No group columns: full refresh
        stmts.push(format!("TRUNCATE {}", intermediate_tbl));
        stmts.push(format!("INSERT INTO {} {}", intermediate_tbl, base_query));
        stmts.push(format!("TRUNCATE {}", qv));
        if end_query.is_empty() {
            stmts.push(format!("INSERT INTO {} {}", qv, base_query));
        } else {
            stmts.push(format!("INSERT INTO {} {}", qv, end_query));
        }
    }
}
```

- [ ] **Step 2: Replace the inline OJS arms**

In `reflex_build_delta_sql`, change lines 1959-2063 (the two `else if is_outer_join_secondary && ...` arms) to a single arm:

```rust
    } else if is_outer_join_secondary {
        outer_join_secondary_stmts(
            view_name,
            source_table,
            operation,
            base_query,
            end_query,
            &plan,
            &grp_cols,
            &intermediate_tbl,
            &affected_tbl,
            &old_tbl,
            &new_tbl,
            &mut stmts,
        );
    } else if plan.is_passthrough {
```

- [ ] **Step 3: Run snapshots**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo test --lib delta_sql_snapshots:: 2>&1 | tail -10
```

Expected: PASS (specifically `snapshot_outer_join_secondary_delete_aggregate`).

- [ ] **Step 4: Run clippy + fmt**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo fmt --check && cargo clippy --all-targets -- -D warnings 2>&1 | tail -5
```

Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add src/trigger.rs
git commit -m "refactor: extract outer_join_secondary_stmts from reflex_build_delta_sql"
```

---

### Task 5: Extract `passthrough_op_stmts`

**Files:**
- Modify: `src/trigger.rs:2064-2168` (the `else if plan.is_passthrough { ... }` body, including scratch fill + per-operation match).

- [ ] **Step 1: Add the helper function**

Insert above `reflex_build_delta_sql`:

```rust
/// Passthrough delta: route through per-(IMV, source) UNLOGGED scratch tables to
/// avoid the transition-table-in-EXECUTE assertion, then run the per-operation
/// targeted DML (mapping-driven DELETE/UPDATE; INSERT splices the scratch into base_query).
fn passthrough_op_stmts(
    view_name: &str,
    source_table: &str,
    operation: &str,
    base_query: &str,
    plan: &AggregationPlan,
    new_tbl: &str,
    old_tbl: &str,
    stmts: &mut Vec<String>,
) {
    let qv = quote_identifier(view_name);
    let pt_new = passthrough_scratch_new_table_name(view_name, source_table);
    let pt_old = passthrough_scratch_old_table_name(view_name, source_table);
    let mappings = plan.passthrough_key_mappings.get(source_table);

    // Scratch fill: see the comment block at lines ~2071-2079 of pre-refactor trigger.rs.
    let needs_new = matches!(operation, "INSERT" | "INSERT_PROMOTED" | "UPDATE");
    let needs_old = matches!(operation, "DELETE" | "DELETE_PROMOTED" | "UPDATE");
    if needs_new {
        stmts.push(format!("TRUNCATE {}", pt_new));
        stmts.push(format!(
            "INSERT INTO {} SELECT * FROM \"{}\"",
            pt_new, new_tbl
        ));
    }
    if needs_old {
        stmts.push(format!("TRUNCATE {}", pt_old));
        stmts.push(format!(
            "INSERT INTO {} SELECT * FROM \"{}\"",
            pt_old, old_tbl
        ));
    }

    match operation {
        "INSERT" | "INSERT_PROMOTED" => {
            let delta_q = replace_source_with_transition(base_query, source_table, &pt_new);
            stmts.push(format!("INSERT INTO {} {}", qv, delta_q));
            if operation == "INSERT_PROMOTED" {
                stmts.push(format!("ANALYZE {}", qv));
            }
        }
        "DELETE" | "DELETE_PROMOTED" => {
            if let Some(mappings) = mappings {
                let target_cols: Vec<String> =
                    mappings.iter().map(|(t, _)| format!("\"{}\"", t)).collect();
                let source_cols: Vec<String> =
                    mappings.iter().map(|(_, s)| format!("\"{}\"", s)).collect();
                let row = row_expr(&target_cols);
                stmts.push(format!(
                    "DELETE FROM {} WHERE {} IN (SELECT {} FROM {})",
                    qv, row, source_cols.join(", "), pt_old
                ));
                if operation == "DELETE_PROMOTED" {
                    stmts.push(format!("ANALYZE {}", qv));
                }
            } else {
                stmts.push(format!("DELETE FROM {}", qv));
                stmts.push(format!("INSERT INTO {} {}", qv, base_query));
            }
        }
        "UPDATE" => {
            if let Some(mappings) = mappings {
                let target_cols: Vec<String> =
                    mappings.iter().map(|(t, _)| format!("\"{}\"", t)).collect();
                let source_cols: Vec<String> =
                    mappings.iter().map(|(_, s)| format!("\"{}\"", s)).collect();
                let row = row_expr(&target_cols);
                stmts.push(format!(
                    "DELETE FROM {} WHERE {} IN (SELECT {} FROM {})",
                    qv, row, source_cols.join(", "), pt_old
                ));
                let delta_new = replace_source_with_transition(base_query, source_table, &pt_new);
                stmts.push(format!("INSERT INTO {} {}", qv, delta_new));
            } else {
                stmts.push(format!("DELETE FROM {}", qv));
                stmts.push(format!("INSERT INTO {} {}", qv, base_query));
            }
        }
        _ => {}
    }
}
```

- [ ] **Step 2: Replace the inline passthrough arm**

In `reflex_build_delta_sql`, change the `else if plan.is_passthrough { ... }` arm (lines 2064-2168) to:

```rust
    } else if plan.is_passthrough {
        passthrough_op_stmts(
            view_name,
            source_table,
            operation,
            base_query,
            &plan,
            &new_tbl,
            &old_tbl,
            &mut stmts,
        );
    } else {
```

- [ ] **Step 3: Run snapshots**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo test --lib delta_sql_snapshots:: 2>&1 | tail -10
```

Expected: PASS (`snapshot_passthrough_insert`, `snapshot_self_join_delete_passthrough`).

- [ ] **Step 4: Run clippy + fmt + full pg_test suite**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo pgrx test --no-default-features --features pg17 2>&1 | tail -20
```

Expected: all green. The `cargo pgrx test` run is the integration gate — it includes `pg_test_passthrough.rs` which exercises every passthrough operation.

- [ ] **Step 5: Commit**

```bash
git add src/trigger.rs
git commit -m "refactor: extract passthrough_op_stmts from reflex_build_delta_sql"
```

---

### Task 6: Extract `aggregate_epilogue_stmts` (with `pending_dispatch` threading)

**Files:**
- Modify: `src/trigger.rs:2240-2505` (the `// Refresh target from intermediate, clean up dead groups...` block through `stmts.push(metadata_sql);` of the `else` branch).
- Modify: `src/trigger.rs:1871-1873` — promote `PendingDispatch` to module-private.
- Modify: `src/trigger.rs:2169-2239` — the aggregate-branch dispatch tracking stays, but the epilogue extraction removes the trailing block.

- [ ] **Step 1: Promote `PendingDispatch` to module scope**

Delete lines 1871-1874 inside `reflex_build_delta_sql`:

```rust
    struct PendingDispatch {
        merge_sql: String,
    }
    let mut pending_dispatch: Option<PendingDispatch> = None;
```

Add at the top of `trigger.rs` near the other module items (after `DeltaOp`, around line 65):

```rust
struct PendingDispatch {
    merge_sql: String,
}
```

In `reflex_build_delta_sql`, keep the local declaration:

```rust
    let mut pending_dispatch: Option<PendingDispatch> = None;
```

- [ ] **Step 2: Add the epilogue helper**

Insert above `reflex_build_delta_sql`:

```rust
/// Aggregate-branch epilogue: refresh target from intermediate, clean up dead groups,
/// stamp metadata. Takes `pending_dispatch` by value because the UPDATE arm may have
/// produced a deferred MERGE that the epilogue must splice into either the
/// high-selectivity dispatch DO block or the partition-aware dispatch.
#[allow(clippy::too_many_arguments)]
fn aggregate_epilogue_stmts(
    view_name: &str,
    source_table: &str,
    operation: &str,
    end_query: &str,
    plan: &AggregationPlan,
    grp_cols: &Option<Vec<String>>,
    intermediate_tbl: &str,
    affected_tbl: &str,
    scratch_tbl: &str,
    pending_dispatch: Option<PendingDispatch>,
    stmts: &mut Vec<String>,
) {
    let end_query_has_group_by = end_query.to_uppercase().contains("GROUP BY");
    let include_dead_cleanup = plan.needs_ivm_count
        && grp_cols.is_some()
        && matches!(operation, "DELETE" | "DELETE_PROMOTED" | "UPDATE");
    let skip_target_delete = operation == "INSERT_PROMOTED"
        && grp_cols.is_some()
        && plan.source_join_keys.contains_key(source_table);
    let metadata_sql = format!(
        "UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() \
         WHERE name = '{}' AND (last_update_date IS NULL OR last_update_date < NOW() - INTERVAL '1 second')",
        view_name.replace("'", "''")
    );

    let mut pending_dispatch = pending_dispatch;

    if end_query_has_group_by {
        let qv = quote_identifier(view_name);
        if plan.group_by_columns.is_empty() {
            let tdel = format!("DELETE FROM {}", qv);
            let tins = format!("INSERT INTO {} {}", qv, end_query);
            if let Some(pd) = pending_dispatch.take() {
                stmts.push(build_high_selectivity_dispatch_sql(
                    view_name, intermediate_tbl, affected_tbl, &pd.merge_sql,
                    None, &tdel, &tins,
                ));
            } else {
                if !skip_target_delete { stmts.push(tdel); }
                stmts.push(tins);
            }
        } else {
            let output_cols: Vec<String> = plan
                .group_by_columns.iter()
                .map(|c| format!("\"{}\"", normalized_column_name(c)))
                .collect();
            let target_cols: Vec<String> = target_group_columns(plan)
                .into_iter()
                .take(plan.group_by_columns.len())
                .collect();
            match inject_affected_filter_before_group_by(
                end_query, &output_cols, affected_tbl, intermediate_tbl, &plan.not_null_columns,
            ) {
                Some(spliced_end_q) => {
                    let ns_in_target = null_safe_in(
                        affected_tbl, &qv, &target_cols, &output_cols, &plan.not_null_columns,
                    );
                    let tdel = format!("DELETE FROM {} WHERE {}", qv, ns_in_target);
                    let tins = format!("INSERT INTO {} {}", qv, spliced_end_q);
                    if let Some(pd) = pending_dispatch.take() {
                        stmts.push(build_high_selectivity_dispatch_sql(
                            view_name, intermediate_tbl, affected_tbl, &pd.merge_sql,
                            None, &tdel, &tins,
                        ));
                    } else {
                        if !skip_target_delete { stmts.push(tdel); }
                        stmts.push(tins);
                    }
                }
                None => {
                    let tdel = format!("DELETE FROM {}", qv);
                    let tins = format!("INSERT INTO {} {}", qv, end_query);
                    if let Some(pd) = pending_dispatch.take() {
                        stmts.push(build_high_selectivity_dispatch_sql(
                            view_name, intermediate_tbl, affected_tbl, &pd.merge_sql,
                            None, &tdel, &tins,
                        ));
                    } else {
                        if !skip_target_delete { stmts.push(tdel); }
                        stmts.push(tins);
                    }
                }
            }
        }
        stmts.push(metadata_sql);
    } else if let Some(ref cols) = grp_cols {
        let qv = quote_identifier(view_name);
        let target_cols = target_group_columns(plan);
        let ns_in_intermediate = null_safe_in(
            affected_tbl, intermediate_tbl, cols, cols, &plan.not_null_columns,
        );
        let ns_in_target_delete = null_safe_in(
            affected_tbl, &qv, &target_cols, cols, &plan.not_null_columns,
        );
        let dead_cleanup_sql = if include_dead_cleanup {
            Some(format!(
                "DELETE FROM {} WHERE __ivm_count <= 0 AND {}",
                intermediate_tbl, ns_in_intermediate
            ))
        } else {
            None
        };
        let target_delete_sql = format!("DELETE FROM {} WHERE {}", qv, ns_in_target_delete);
        let target_insert_sql = format!("INSERT INTO {} {} AND {}", qv, end_query, ns_in_intermediate);

        if let Some(pd) = pending_dispatch.take() {
            let use_partition_dispatch = !plan.partition_columns.is_empty()
                && plan.partition_strategy.eq_ignore_ascii_case("LIST");
            if use_partition_dispatch {
                let part_col = &plan.partition_columns[0];
                let part_col_q = format!("\"{}\"", part_col);
                let filtered_scratch = format!(
                    "(SELECT * FROM {} WHERE {}::text <> ALL($1::TEXT[]))",
                    scratch_tbl, part_col_q
                );
                let merge_filtered = build_merge_from_table_sql(
                    intermediate_tbl, &filtered_scratch, plan, DeltaOp::Add,
                );
                let dead_cleanup_filtered = dead_cleanup_sql.as_ref().map(|s| {
                    format!(
                        "{} AND EXISTS (SELECT 1 FROM {} __ap \
                          WHERE __ap.{} = {}.{} AND __ap.{}::text <> ALL($1::TEXT[]))",
                        s, affected_tbl, part_col_q, intermediate_tbl, part_col_q, part_col_q
                    )
                });
                let tdel_filtered = format!(
                    "{} AND {}.{}::text <> ALL($1::TEXT[])",
                    target_delete_sql, qv, part_col_q
                );
                let tins_filtered = format!(
                    "{} AND {}.{}::text <> ALL($1::TEXT[])",
                    target_insert_sql, intermediate_tbl, part_col_q
                );
                stmts.push(build_partition_aware_dispatch_sql(
                    view_name, intermediate_tbl, intermediate_tbl, affected_tbl,
                    part_col, &merge_filtered, dead_cleanup_filtered.as_deref(),
                    &tdel_filtered, &tins_filtered,
                ));
            } else {
                stmts.push(build_high_selectivity_dispatch_sql(
                    view_name, intermediate_tbl, affected_tbl, &pd.merge_sql,
                    dead_cleanup_sql.as_deref(), &target_delete_sql, &target_insert_sql,
                ));
            }
        } else {
            if let Some(s) = dead_cleanup_sql {
                stmts.push(s);
            }
            if !skip_target_delete {
                stmts.push(target_delete_sql);
            }
            stmts.push(target_insert_sql);
        }
        stmts.push(metadata_sql);
    } else {
        let qv = quote_identifier(view_name);
        stmts.push(format!("TRUNCATE {}", qv));
        stmts.push(format!("INSERT INTO {} {}", qv, end_query));
        stmts.push(metadata_sql);
    }
}
```

- [ ] **Step 2: Replace the inline epilogue in `reflex_build_delta_sql`**

Change lines 2240-2505 (everything from `// Refresh target from intermediate...` through the closing `}` of the `else` branch, but BEFORE the `// Historical note` comment) to a single call:

```rust
        aggregate_epilogue_stmts(
            view_name,
            source_table,
            operation,
            end_query,
            &plan,
            &grp_cols,
            &intermediate_tbl,
            &affected_tbl,
            &scratch_tbl,
            pending_dispatch.take(),
            &mut stmts,
        );
    }
```

(The final `}` closes the outer `else` branch of the `is_self_join`/`is_outer_join_secondary`/`is_passthrough`/else chain.)

- [ ] **Step 3: Run snapshots**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo test --lib delta_sql_snapshots:: 2>&1 | tail -15
```

Expected: all 8 snapshots PASS, with particular attention to `snapshot_aggregate_insert`, `snapshot_aggregate_delete`, `snapshot_aggregate_update_dispatch`, `snapshot_aggregate_epilogue_no_group_by`.

- [ ] **Step 4: Run the EXCEPT ALL correctness oracle**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo pgrx test --no-default-features --features pg17 pg_test_correctness 2>&1 | tail -30
```

Expected: every `assert_imv_correct` passes. This is the gold-standard gate for the trigger SQL.

- [ ] **Step 5: Run clippy + fmt**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo fmt --check && cargo clippy --all-targets -- -D warnings 2>&1 | tail -5
```

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/trigger.rs
git commit -m "refactor: extract aggregate_epilogue_stmts (with pending_dispatch threading)"
```

---

## Phase C — Extract `create_reflex_ivm_impl` pipeline phases

This phase is correctness-preserving but extensive. The bar is `cargo pgrx test` green plus the EXCEPT ALL oracle in `pg_test_correctness.rs`. There are no string snapshots here — the function emits SPI side effects, not strings.

### Task 7: Introduce `BuildContext` struct + thread it through the post-decompose body

**Files:**
- Modify: `src/create_ivm.rs` (add the struct, restructure `create_reflex_ivm_impl:586-1875` to construct and pass it).

- [ ] **Step 1: Add the `BuildContext` struct**

Insert near the top of `create_ivm.rs` after `ParsedInputs` (line 39):

```rust
/// Pipeline state shared across phases of `create_reflex_ivm_impl`. Owns the
/// mutable `plan` and `analysis`; helpers mutate fields in-place. Input refs
/// (`view_name`, `sql`, etc.) borrow from the caller's arg slots.
struct BuildContext<'a> {
    // Inputs
    view_name: &'a str,
    sql: &'a str,
    unique_columns_str: &'a str,
    if_not_exists: bool,
    topk_k: Option<usize>,
    ignore_sources: &'a [String],
    partition_by: &'a [String],

    // Parsed
    logged: bool,
    deferred: bool,
    storage_upper: String,
    mode_upper: String,
    analysis: crate::sql_analyzer::SqlAnalysis,

    // Plan (mutated through pipeline)
    plan: crate::aggregation::AggregationPlan,

    // Source decomposition (set immediately after plan construction)
    froms: Vec<String>,
    real_source_names: Vec<String>,
    is_join_query: bool,

    // Pre-SPI resolution outputs
    resolved_unique_columns: Vec<String>,
    resolved_partition_cols: Vec<String>,
    resolved_strategy: String,

    // SPI-phase outputs
    ivm_froms: Vec<String>,
    depth: i32,
    unlogged_tables: Vec<String>,
}
```

- [ ] **Step 2: Restructure the entry function to build the context**

In `create_reflex_ivm_impl` (line 586), keep the existing 4 `try_decompose_*` calls (lines 597-671). After they fall through, replace the body that begins at line 673 with:

```rust
    let ParsedInputs {
        logged,
        deferred,
        storage_upper,
        mode_upper,
        parsed_sql: _,
        analysis,
    } = parsed;

    let froms = analysis.sources.clone();
    let real_source_names: Vec<String> =
        froms.iter().filter(|s| !s.starts_with('<')).cloned().collect();
    let is_join_query = real_source_names.len() > 1;

    let plan = if topk_k.is_some() {
        plan_aggregation_with_topk(&analysis, topk_k)
    } else {
        plan_aggregation(&analysis)
    };

    // Reject COUNT(DISTINCT) mixed with other aggregates.
    let has_cd = analysis.select_columns.iter().any(|c| {
        matches!(c.aggregate, Some(crate::sql_analyzer::AggregateKind::CountDistinct))
    });
    let has_other_agg = analysis.select_columns.iter().any(|c| {
        matches!(c.aggregate, Some(ref k) if !matches!(k,
            crate::sql_analyzer::AggregateKind::CountDistinct))
    });
    if has_cd && has_other_agg {
        return "ERROR: COUNT(DISTINCT col) cannot be mixed with other aggregates in the same query. \
                Use a CTE to separate them: WITH cd AS (SELECT grp, COUNT(DISTINCT col) ...) SELECT ...";
    }

    let mut ctx = BuildContext {
        view_name, sql, unique_columns_str, if_not_exists,
        topk_k, ignore_sources, partition_by,
        logged, deferred, storage_upper, mode_upper, analysis, plan,
        froms, real_source_names, is_join_query,
        resolved_unique_columns: Vec::new(),
        resolved_partition_cols: Vec::new(),
        resolved_strategy: String::new(),
        ivm_froms: Vec::new(),
        depth: 0,
        unlogged_tables: Vec::new(),
    };
```

Below this, the rest of the body (lines ~711-1872, currently inline) will be replaced one phase at a time by Tasks 8-16. For this task, just call them as inline blocks (move them under `ctx` and rename local references), so the cargo build still passes. After Task 7, the body of `create_reflex_ivm_impl` still works end-to-end but uses `ctx.plan`, `ctx.analysis`, `ctx.froms`, etc. everywhere.

The mechanical rewrite to make this compile:
- Every reference to `plan` becomes `ctx.plan` (and `&mut ctx.plan` where mut-borrowed)
- `analysis` → `ctx.analysis`
- `froms` → `ctx.froms`
- `real_source_names` → `ctx.real_source_names`
- `is_join_query` → `ctx.is_join_query`
- `resolved_unique_columns` → `ctx.resolved_unique_columns`
- `resolved_partition_cols` → `ctx.resolved_partition_cols`
- `resolved_strategy` → `ctx.resolved_strategy`
- `logged`, `deferred`, `storage_upper`, `mode_upper` → `ctx.logged`, etc.

Use your editor's project-wide rename within the function body. Test the rename succeeded by running `cargo check`.

- [ ] **Step 3: Verify cargo build**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo check --no-default-features --features pg17 2>&1 | tail -20
```

Expected: clean compile. No new warnings.

- [ ] **Step 4: Run the full test suite**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo pgrx test --no-default-features --features pg17 2>&1 | tail -30
```

Expected: all green. This is the bar Phase C must continue to pass.

- [ ] **Step 5: Commit**

```bash
git add src/create_ivm.rs
git commit -m "refactor: introduce BuildContext for create_reflex_ivm_impl pipeline state"
```

---

### Task 8: Extract `resolve_unique_columns`

**Files:**
- Modify: `src/create_ivm.rs:716-823` (the passthrough unique-column resolution block).

- [ ] **Step 1: Add the helper function**

Insert above `create_reflex_ivm_impl`:

```rust
/// For passthrough IMVs, resolve the unique-key column set: explicit
/// `unique_columns_str` if non-empty, else probe single-source PKs from
/// pg_index. Multi-source JOINs without an explicit key fall back to
/// full refresh on DELETE/UPDATE (warned). Populates
/// `ctx.resolved_unique_columns` and `ctx.plan.passthrough_columns`/
/// `ctx.plan.passthrough_key_mappings`.
fn resolve_unique_columns(ctx: &mut BuildContext) {
    if !ctx.plan.is_passthrough {
        return;
    }
    let real_sources: Vec<&String> =
        ctx.froms.iter().filter(|s| !s.starts_with('<')).collect();

    if !ctx.unique_columns_str.is_empty() {
        ctx.resolved_unique_columns = ctx.unique_columns_str
            .split(',')
            .map(|s| normalized_column_name(s.trim()))
            .filter(|s| !s.is_empty())
            .collect();
        ctx.plan.passthrough_columns = ctx.resolved_unique_columns.clone();
        info!(
            "pg_reflex: using explicit unique key ({}) for '{}'",
            ctx.resolved_unique_columns.join(", "),
            ctx.view_name
        );
        build_passthrough_key_mappings(
            &mut ctx.plan,
            &ctx.resolved_unique_columns,
            &real_sources,
            &ctx.analysis,
        );
    } else if !ctx.is_join_query {
        // Move lines 738-813 of the original function verbatim here (the PK auto-detect loop).
        // It reads `real_sources`, `ctx.analysis`, `ctx.view_name`; mutates `ctx.resolved_unique_columns`
        // and (via `build_passthrough_key_mappings`) `ctx.plan`.
        // ... (auto-detect body) ...
    } else {
        info!(
            "pg_reflex: JOIN passthrough '{}' has no unique key. \
             Provide 3rd argument to create_reflex_ivm for incremental DELETE/UPDATE. \
             Example: SELECT create_reflex_ivm('{}', '...', 'col1,col2')",
            ctx.view_name, ctx.view_name
        );
    }
}
```

Lift the existing PK auto-detect body verbatim (lines 740-812 of the pre-refactor file) into the `else if !ctx.is_join_query` branch, replacing the placeholder comment. Rename locals as needed:
- `view_name` → `ctx.view_name`
- `analysis` → `ctx.analysis`
- `resolved_unique_columns` → `ctx.resolved_unique_columns`
- `plan` → `ctx.plan` / `&mut ctx.plan`

- [ ] **Step 2: Replace the inline block with the call**

In `create_reflex_ivm_impl`, replace lines 712-823 (everything from `let mut resolved_unique_columns: Vec<String> = Vec::new();` through the end of the JOIN-no-key `else`) with:

```rust
    resolve_unique_columns(&mut ctx);
```

- [ ] **Step 3: Run tests**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo pgrx test --no-default-features --features pg17 pg_test_passthrough 2>&1 | tail -15
```

Expected: PASS. `pg_test_passthrough.rs` exercises explicit key, auto-detect single-source, and JOIN-no-key fallback.

- [ ] **Step 4: Run clippy + fmt + commit**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo fmt --check && cargo clippy --all-targets -- -D warnings
git add src/create_ivm.rs
git commit -m "refactor: extract resolve_unique_columns into a pipeline helper"
```

---

### Task 9: Extract `populate_source_join_keys` + `validate_select_columns`

**Files:**
- Modify: `src/create_ivm.rs:829-872` (join-key population + warning loop).

- [ ] **Step 1: Add the helpers**

```rust
fn populate_source_join_keys(ctx: &mut BuildContext) {
    if ctx.plan.is_passthrough || !ctx.is_join_query {
        return;
    }
    let real_sources: Vec<&String> =
        ctx.froms.iter().filter(|s| !s.starts_with('<')).collect();
    build_source_join_keys(&mut ctx.plan, &real_sources, &ctx.analysis);
}

/// Warn on SELECT entries that are neither GROUP BY nor recognized aggregates.
/// These columns will be missing from the IMV (silent data loss is worse than a warning).
fn validate_select_columns(ctx: &BuildContext) {
    if ctx.plan.is_passthrough {
        return;
    }
    let group_by_set: std::collections::HashSet<&str> =
        ctx.plan.group_by_columns.iter().map(|s| s.as_str()).collect();
    for col in &ctx.analysis.select_columns {
        if !col.is_passthrough && col.aggregate.is_none() && !col.is_aggregate_derived {
            warning!(
                "pg_reflex: unsupported expression '{}' in SELECT — column will be missing from IMV '{}'",
                col.alias.as_deref().unwrap_or(&col.expr_sql),
                ctx.view_name
            );
        } else if col.is_passthrough
            && !group_by_set.contains(col.expr_sql.as_str())
            && !ctx.analysis.has_distinct
        {
            let bare = bare_column_name(&col.expr_sql);
            let in_gb = group_by_set.iter().any(|gb| bare_column_name(gb) == bare);
            if !in_gb {
                warning!(
                    "pg_reflex: expression '{}' not in GROUP BY and not a recognized aggregate — column will be missing from IMV '{}'",
                    col.expr_sql,
                    ctx.view_name
                );
            }
        }
    }
}
```

- [ ] **Step 2: Replace the inline blocks**

Replace lines 825-831 (build_source_join_keys) and lines 841-872 (validate select columns) with:

```rust
    populate_source_join_keys(&mut ctx);
    validate_select_columns(&ctx);
```

- [ ] **Step 3: Run tests + commit**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo pgrx test --no-default-features --features pg17 2>&1 | tail -10
cargo fmt --check && cargo clippy --all-targets -- -D warnings
git add src/create_ivm.rs
git commit -m "refactor: extract populate_source_join_keys + validate_select_columns"
```

Expected: green.

---

### Task 10: Extract `check_existence_and_cycle`

**Files:**
- Modify: `src/create_ivm.rs:874-941` (the duplicate-name + cycle-detection block, two `Spi::connect`s).

- [ ] **Step 1: Add the helper**

```rust
/// Pre-flight checks: reject duplicate view name (or skip-noop on
/// `if_not_exists`), detect cycles in the IMV dependency DAG. Returns the
/// short-circuit string when the create should stop, `None` to continue.
fn check_existence_and_cycle(ctx: &BuildContext) -> Option<&'static str> {
    let already_exists = Spi::connect(|client| {
        !client
            .select(
                "SELECT 1 FROM public.__reflex_ivm_reference WHERE name = $1",
                None,
                &[unsafe {
                    DatumWithOid::new(ctx.view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .collect::<Vec<_>>()
            .is_empty()
    });
    if already_exists {
        if ctx.if_not_exists {
            return Some("REFLEX INCREMENTAL VIEW ALREADY EXISTS (skipped)");
        }
        return Some("ERROR: IMV with this name already exists");
    }

    let cycle_detected = if ctx.froms.is_empty() {
        false
    } else {
        Spi::connect(|client| {
            let args = [
                unsafe {
                    DatumWithOid::new(
                        format_pg_text_array_literal(&ctx.froms),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                },
                unsafe {
                    DatumWithOid::new(ctx.view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                },
            ];
            !client
                .select(
                    "WITH RECURSIVE dep_graph(dep) AS (\
                        SELECT unnest(depends_on) \
                        FROM public.__reflex_ivm_reference \
                        WHERE name = ANY($1::TEXT[]) \
                        UNION \
                        SELECT unnest(r.depends_on) \
                        FROM dep_graph dg \
                        JOIN public.__reflex_ivm_reference r ON r.name = dg.dep \
                    ) \
                    SELECT 1 FROM dep_graph WHERE dep = $2 LIMIT 1",
                    Some(1),
                    &args,
                )
                .unwrap_or_report()
                .collect::<Vec<_>>()
                .is_empty()
        })
    };
    if cycle_detected {
        return Some("ERROR: circular dependency detected — this IMV would form a cycle in the dependency graph");
    }
    None
}
```

- [ ] **Step 2: Replace the inline block**

Replace lines 874-941 with:

```rust
    if let Some(msg) = check_existence_and_cycle(&ctx) {
        return msg;
    }
```

- [ ] **Step 3: Run tests + commit**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo pgrx test --no-default-features --features pg17 pg_test_error 2>&1 | tail -10
cargo fmt --check && cargo clippy --all-targets -- -D warnings
git add src/create_ivm.rs
git commit -m "refactor: extract check_existence_and_cycle"
```

`pg_test_error.rs` exercises the cycle + duplicate paths. Expected: green.

---

### Task 11: Extract `resolve_partitioning`

**Files:**
- Modify: `src/create_ivm.rs:943-1129` (entire partitioning resolution block).

- [ ] **Step 1: Add the helper**

```rust
/// Resolve `partition_by`: validate explicit columns against plan + anchor, or
/// auto-mirror from the single partitioned source. Populates
/// `ctx.resolved_partition_cols` and `ctx.resolved_strategy`. Returns
/// `Err(message)` for validation failures; the caller passes the message
/// through (with the same `Box::leak` semantics as the original).
fn resolve_partitioning(ctx: &mut BuildContext) -> Result<(), String> {
    let mut resolved_partition_cols: Vec<String> = ctx.partition_by.to_vec();
    let resolved_strategy: String;

    if !resolved_partition_cols.is_empty() {
        // (Move lines 961-1037 of original — explicit partition_by validation against plan shape)
        // (Move lines 1039-1068 — anchor resolution + strategy fetch via Spi::connect)
    } else {
        // (Move lines 1071-1129 — auto-mirror from single partitioned source)
    }

    ctx.resolved_partition_cols = resolved_partition_cols;
    ctx.resolved_strategy = resolved_strategy;
    Ok(())
}
```

Lift the existing block (lines 943-1129) verbatim. Rename locals:
- `plan` → `ctx.plan`
- `real_source_names` → `ctx.real_source_names`
- `partition_by` → `ctx.partition_by`
- `resolved_partition_cols` stays local until end, then assigned to `ctx`
- `resolved_strategy` same

Replace the two `return Box::leak(...)` lines with `return Err(<the formatted string>)` — the caller will re-leak. Replace the explicit-validation `return` (line 994 and 1025) and the anchor-error path (line 1063) with `return Err(message)`.

- [ ] **Step 2: Replace the inline block**

Replace lines 943-1129 with:

```rust
    if let Err(msg) = resolve_partitioning(&mut ctx) {
        return Box::leak(msg.into_boxed_str());
    }
```

- [ ] **Step 3: Run tests + commit**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo pgrx test --no-default-features --features pg17 pg_test_partition 2>&1 | tail -15
cargo fmt --check && cargo clippy --all-targets -- -D warnings
git add src/create_ivm.rs
git commit -m "refactor: extract resolve_partitioning"
```

Expected: green. `pg_test_partition.rs` covers both explicit and auto-mirror paths.

---

### Task 12: Extract `materialize_storage` (and its passthrough + aggregate halves)

**Files:**
- Modify: `src/create_ivm.rs:1131-1520` (the start of the `Spi::connect_mut` block up through the end of the passthrough/aggregate branch).

This is the largest extraction in Phase C. Split into 3 helpers: the lookup/setup, the passthrough materialization, and the aggregate materialization.

- [ ] **Step 1: Add the helpers**

```rust
/// Resolve existing IMV dependencies among `ctx.froms` and compute graph_depth.
/// Populates `ctx.ivm_froms` and `ctx.depth`.
fn resolve_existing_imv_deps(client: &SpiClient<'_>, ctx: &mut BuildContext) {
    let args = [unsafe {
        DatumWithOid::new(
            format_pg_text_array_literal(&ctx.froms),
            PgBuiltInOids::TEXTOID.oid().value(),
        )
    }];

    let matching_froms = client
        .select(
            "SELECT name, graph_depth FROM public.__reflex_ivm_reference WHERE name = ANY($1::TEXT[])",
            None,
            &args,
        )
        .unwrap_or_report()
        .collect::<Vec<_>>();

    ctx.ivm_froms = matching_froms
        .iter()
        .filter_map(|row| row.get_by_name::<&str, _>("name").unwrap_or(None))
        .map(|s| s.to_string())
        .collect();

    ctx.depth = matching_froms
        .iter()
        .filter_map(|row| row.get_by_name::<i32, _>("graph_depth").unwrap_or(None))
        .max()
        .unwrap_or(0)
        + 1;
}

/// Stage partition + anchor + partition_join_paths fields onto `ctx.plan`.
/// Mutates: `ctx.plan.partition_columns`, `partition_strategy`, `anchor_source`,
/// `partition_join_paths`.
fn apply_partition_plan(client: &SpiClient<'_>, ctx: &mut BuildContext) {
    ctx.plan.partition_columns = ctx.resolved_partition_cols.clone();
    ctx.plan.partition_strategy = ctx.resolved_strategy.clone();
    ctx.plan.anchor_source = if ctx.resolved_partition_cols.is_empty() {
        String::new()
    } else {
        crate::partition::resolve_anchor_source(
            client,
            &ctx.resolved_partition_cols[0],
            &ctx.real_source_names,
        )
        .unwrap_or_default()
    };

    if !ctx.plan.partition_columns.is_empty() && !ctx.plan.anchor_source.is_empty() {
        // (Move lines 1192-1222 verbatim — the JOIN-path fragment computation)
    }
}

/// Drop `imv_relevant_columns` entries that don't exist on the source table.
/// Mutates: `ctx.plan.imv_relevant_columns`.
fn filter_imv_relevant_columns(client: &SpiClient<'_>, ctx: &mut BuildContext) {
    let (_t, _nn, per_source_cols_for_filter) =
        query_column_types_from_catalog_with_per_source(client, &ctx.froms);
    for (source, cols) in ctx.plan.imv_relevant_columns.iter_mut() {
        if let Some(actual) = per_source_cols_for_filter.get(source) {
            cols.retain(|c| actual.contains(c.as_str()));
        } else if source.starts_with('<') {
            cols.clear();
        }
    }
    ctx.plan.imv_relevant_columns.retain(|_, v| !v.is_empty());
}

/// Passthrough materialization: CREATE TABLE AS (or partitioned variant),
/// ANALYZE, unique-index, scratch tables.
fn materialize_passthrough(client: &mut SpiClient<'_>, ctx: &mut BuildContext) {
    // (Move lines 1254-1389 verbatim. Locals: `view_name` → `ctx.view_name`,
    //  `plan` → `ctx.plan`, `logged` → `ctx.logged`, `sql` → `ctx.sql`,
    //  `real_source_names` → `ctx.real_source_names`,
    //  `resolved_unique_columns` → `ctx.resolved_unique_columns`,
    //  `froms` → `ctx.froms`.)
}

/// Aggregate materialization: catalog type discovery, intermediate + target + delta-scratch
/// DDL, partition children. Pushes intermediate table name onto `ctx.unlogged_tables`.
fn materialize_aggregate(client: &mut SpiClient<'_>, ctx: &mut BuildContext) {
    // (Move lines 1391-1519 verbatim. Locals → ctx fields as above.
    //  Note: this is where `unlogged_tables.push(tbl)` happens — push to
    //  `ctx.unlogged_tables` instead.)
}
```

- [ ] **Step 2: Replace the inline blocks (still inside `Spi::connect_mut`)**

The body of the `Spi::connect_mut(|client| { ... })` block in `create_reflex_ivm_impl` becomes (for lines 1132-1520):

```rust
    Spi::connect_mut(|client| {
        resolve_existing_imv_deps(client, &mut ctx);
        apply_partition_plan(client, &mut ctx);
        filter_imv_relevant_columns(client, &mut ctx);
        if ctx.plan.is_passthrough {
            materialize_passthrough(client, &mut ctx);
        } else {
            materialize_aggregate(client, &mut ctx);
        }
        // ... (remaining inline phases — extracted in Tasks 13-16)
    });
```

- [ ] **Step 3: Verify the build**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo check --no-default-features --features pg17 2>&1 | tail -10
```

Expected: clean. Borrow-checker may complain — `ctx.plan` mut-borrow during a helper call vs. ctx field reads. If so, the fix is to bind fields to locals before the call (e.g. `let view_name = ctx.view_name;` then pass them in explicitly). Prefer to thread ctx mutably to a single helper at a time; don't share `&mut ctx` and `&ctx.field` simultaneously.

- [ ] **Step 4: Run tests + commit**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo pgrx test --no-default-features --features pg17 2>&1 | tail -20
cargo fmt --check && cargo clippy --all-targets -- -D warnings
git add src/create_ivm.rs
git commit -m "refactor: extract materialize_storage helpers (passthrough + aggregate)"
```

Expected: green.

---

### Task 13: Extract `install_source_triggers` + `install_deferred_flush_if_needed`

**Files:**
- Modify: `src/create_ivm.rs:1522-1643` (the trigger install loop + deferred flush).

- [ ] **Step 1: Add the helpers**

```rust
/// Install consolidated triggers on every real source of the IMV. Skips
/// `<subquery:...>`/`<function:...>` placeholders, ignored sources, and
/// materialized-view sources. Upgrades existing triggers to deferred when
/// any deferred IMV depends on the source.
fn install_source_triggers(client: &mut SpiClient<'_>, ctx: &BuildContext) {
    for source in &ctx.froms {
        // (Move lines 1524-1635 verbatim. Locals:
        //  `view_name` → `ctx.view_name`,
        //  `ignore_sources` → `ctx.ignore_sources`,
        //  `deferred` → `ctx.deferred`.)
    }
}

/// When the IMV uses deferred refresh, ensure the deferred-flush
/// infrastructure (function + per-source helpers) exists.
fn install_deferred_flush_if_needed(client: &mut SpiClient<'_>, ctx: &BuildContext) {
    if ctx.deferred {
        for ddl in build_deferred_flush_ddl() {
            client.update(&ddl, None, &[]).unwrap_or_report();
        }
    }
}
```

- [ ] **Step 2: Replace inline blocks**

Inside the `Spi::connect_mut` body, replace lines 1522-1643 with:

```rust
        install_source_triggers(client, &ctx);
        install_deferred_flush_if_needed(client, &ctx);
```

- [ ] **Step 3: Run tests + commit**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo pgrx test --no-default-features --features pg17 pg_test_deferred pg_test_trigger 2>&1 | tail -15
cargo fmt --check && cargo clippy --all-targets -- -D warnings
git add src/create_ivm.rs
git commit -m "refactor: extract install_source_triggers + install_deferred_flush_if_needed"
```

Expected: green.

---

### Task 14: Extract `install_min_max_indexes`

**Files:**
- Modify: `src/create_ivm.rs:1645-1693`.

- [ ] **Step 1: Add the helper**

```rust
/// Source-side indexes on GROUP BY columns for MIN/MAX recompute performance.
/// Skips IMV sources (the IMV's intermediate has its own indexes) and
/// `<subquery>` placeholders. Only emits indexes for columns that exist on
/// the source table.
fn install_min_max_indexes(client: &mut SpiClient<'_>, ctx: &BuildContext) {
    let has_min_max = ctx.plan
        .intermediate_columns
        .iter()
        .any(|ic| ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX");
    if !has_min_max || ctx.plan.group_by_columns.is_empty() {
        return;
    }
    for source in &ctx.froms {
        if source.starts_with('<') || ctx.ivm_froms.contains(source) {
            continue;
        }
        // (Move lines 1656-1691 verbatim — the per-source index DDL emission.)
    }
}
```

- [ ] **Step 2: Replace inline block**

Replace lines 1645-1693 with:

```rust
        install_min_max_indexes(client, &ctx);
```

- [ ] **Step 3: Run tests + commit**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo pgrx test --no-default-features --features pg17 2>&1 | tail -15
cargo fmt --check && cargo clippy --all-targets -- -D warnings
git add src/create_ivm.rs
git commit -m "refactor: extract install_min_max_indexes"
```

Expected: green.

---

### Task 15: Extract `persist_metadata`

**Files:**
- Modify: `src/create_ivm.rs:1695-1769` (metadata derivation + RegistryRow + graph_child update).

- [ ] **Step 1: Add the helper**

```rust
/// Compute base_query / end_query / aggregations_json / index_columns /
/// where_predicate, INSERT a RegistryRow into `__reflex_ivm_reference`, and
/// add this IMV's name to the `graph_child` array of each parent IMV.
fn persist_metadata(client: &mut SpiClient<'_>, ctx: &BuildContext) {
    let base_query = if ctx.plan.is_passthrough {
        ctx.sql.to_string()
    } else {
        generate_base_query(&ctx.analysis, &ctx.plan)
    };
    let end_query = if ctx.plan.is_passthrough {
        String::new()
    } else {
        generate_end_query(ctx.view_name, &ctx.plan)
    };
    let aggregations_json = generate_aggregations_json(&ctx.plan);
    let index_columns: Vec<String> = ctx.plan
        .group_by_columns
        .iter()
        .chain(ctx.plan.distinct_columns.iter())
        .map(|c| {
            if let Some(alias) = ctx.plan.group_by_aliases.get(c) {
                normalized_column_name(alias)
            } else {
                normalized_column_name(c)
            }
        })
        .collect();
    let real_sources: Vec<&String> =
        ctx.froms.iter().filter(|s| !s.starts_with('<')).collect();
    let where_predicate: String = if real_sources.len() <= 1 {
        ctx.analysis.where_clause.clone().unwrap_or_default()
    } else {
        String::new()
    };
    let ignored_sources_vec: Vec<String> = ctx.ignore_sources.to_vec();

    insert_registry_row(
        client,
        &RegistryRow {
            view_name: ctx.view_name,
            graph_depth: ctx.depth,
            depends_on: &ctx.froms,
            depends_on_imv: &ctx.ivm_froms,
            unlogged_tables: &ctx.unlogged_tables,
            graph_child: &[],
            sql_query: ctx.sql,
            base_query: &base_query,
            end_query: &end_query,
            aggregations_json: &aggregations_json,
            aggregations_cast: AggregationsCast::Jsonb,
            index_columns: &index_columns,
            unique_columns: &ctx.resolved_unique_columns,
            storage_mode: &ctx.storage_upper,
            refresh_mode: &ctx.mode_upper,
            where_predicate: Some(&where_predicate),
            ignored_sources: Some(&ignored_sources_vec),
            partition_columns: Some(&ctx.plan.partition_columns),
            partition_strategy: Some(&ctx.plan.partition_strategy),
        },
    )
    .unwrap_or_report();

    add_graph_child_links(client, ctx.view_name, &ctx.ivm_froms).unwrap_or_report();
}
```

- [ ] **Step 2: Replace inline block**

Replace lines 1695-1769 with:

```rust
        persist_metadata(client, &ctx);
```

- [ ] **Step 3: Run tests + commit**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo pgrx test --no-default-features --features pg17 2>&1 | tail -15
cargo fmt --check && cargo clippy --all-targets -- -D warnings
git add src/create_ivm.rs
git commit -m "refactor: extract persist_metadata"
```

Expected: green.

---

### Task 16: Extract `initial_aggregate_materialization`

**Files:**
- Modify: `src/create_ivm.rs:1772-1870`.

- [ ] **Step 1: Add the helper**

```rust
/// For aggregate IMVs (skipped for passthrough — CREATE TABLE AS already
/// populated): execute the initial INSERT into intermediate + target, build
/// indexes, provision affected-groups + shrunk-groups tables, ANALYZE, and
/// data-probe NOT-NULL columns. Mutates `ctx.plan.not_null_columns` with
/// the probed additions.
fn initial_aggregate_materialization(client: &mut SpiClient<'_>, ctx: &mut BuildContext) {
    if ctx.plan.is_passthrough {
        return;
    }
    let intermediate_tbl = intermediate_table_name(ctx.view_name);
    let base_q = generate_base_query(&ctx.analysis, &ctx.plan);
    let initial_insert = format!("INSERT INTO {} {}", intermediate_tbl, base_q);
    client.update(&initial_insert, None, &[]).unwrap_or_report();

    let end_q = generate_end_query(ctx.view_name, &ctx.plan);
    let target_insert =
        format!("INSERT INTO {} {}", quote_identifier(ctx.view_name), end_q);
    client.update(&target_insert, None, &[]).unwrap_or_report();

    for index_ddl in build_indexes_ddl(ctx.view_name, &ctx.plan) {
        client.update(&index_ddl, None, &[]).unwrap_or_report();
    }

    // (Move lines 1787-1831 verbatim — affected-groups + shrunk-groups tables.
    //  `plan` → `ctx.plan`, `view_name` → `ctx.view_name`.)

    client.update(&format!("ANALYZE {}", intermediate_tbl), None, &[]).unwrap_or_report();
    client.update(
        &format!("ANALYZE {}", quote_identifier(ctx.view_name)),
        None, &[],
    ).unwrap_or_report();

    let probed_nn = probe_not_null_columns_from_data(client, &intermediate_tbl, &ctx.plan);
    let new_cols: Vec<String> = probed_nn
        .into_iter()
        .filter(|c| !ctx.plan.not_null_columns.contains(c))
        .collect();
    if !new_cols.is_empty() {
        for c in &new_cols {
            ctx.plan.not_null_columns.insert(c.clone());
        }
        persist_probed_not_null_columns(client, ctx.view_name, &new_cols);
        info!(
            "pg_reflex: data-probe added {} effectively-NOT-NULL column(s) to '{}': {:?}",
            new_cols.len(),
            ctx.view_name,
            new_cols
        );
    }
}
```

- [ ] **Step 2: Replace the inline block**

Replace lines 1772-1870 with:

```rust
        initial_aggregate_materialization(client, &mut ctx);
```

After this, the body of `create_reflex_ivm_impl` (after the 4 `try_decompose_*` calls and the ctx construction) reads as:

```rust
    resolve_unique_columns(&mut ctx);
    populate_source_join_keys(&mut ctx);
    validate_select_columns(&ctx);
    if let Some(msg) = check_existence_and_cycle(&ctx) {
        return msg;
    }
    if let Err(msg) = resolve_partitioning(&mut ctx) {
        return Box::leak(msg.into_boxed_str());
    }

    Spi::connect_mut(|client| {
        resolve_existing_imv_deps(client, &mut ctx);
        apply_partition_plan(client, &mut ctx);
        filter_imv_relevant_columns(client, &mut ctx);
        if ctx.plan.is_passthrough {
            materialize_passthrough(client, &mut ctx);
        } else {
            materialize_aggregate(client, &mut ctx);
        }
        install_source_triggers(client, &ctx);
        install_deferred_flush_if_needed(client, &ctx);
        install_min_max_indexes(client, &ctx);
        persist_metadata(client, &ctx);
        initial_aggregate_materialization(client, &mut ctx);
    });

    info!("pg_reflex: created IMV '{}'", view_name);
    "CREATE REFLEX INCREMENTAL VIEW"
```

- [ ] **Step 3: Run the full test suite (this is the Phase C completion gate)**

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo pgrx test --no-default-features --features pg17 2>&1 | tail -30
```

Expected: every `#[pg_test]` passes. Pay particular attention to:
- `pg_test_correctness` — the EXCEPT ALL oracle. Every IMV's contents must equal the source SQL re-evaluated.
- `pg_test_e2e` — full lifecycle.
- `pg_test_partition`, `pg_test_partition_dispatch` — partitioning paths.

- [ ] **Step 4: Commit**

```bash
git add src/create_ivm.rs
git commit -m "refactor: extract initial_aggregate_materialization (completes pipeline decomposition)"
```

---

## Verification (end of plan)

After all 16 tasks, run the full bar one more time:

```bash
cd /Users/diviyan/fentech/tools/pg_reflex
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo pgrx check
cargo pgrx test --no-default-features --features pg17
```

All four commands must succeed. The `cargo pgrx test` run is the gold standard — `pg_test_correctness.rs` invokes the EXCEPT ALL oracle on every IMV it creates; the snapshot tests in `unit_trigger.rs` pin `reflex_build_delta_sql` byte-for-byte.

If any phase requires a benchmark sanity check (per CLAUDE.md's development cycle), pick 2-3 of the materialized views in `/home/diviyan/fentech/algorithm/api/base-db-anchor-evm/base_db/sql` and re-create them as IMVs against `db_clone`; the cold-path `create_reflex_ivm` runtime should be within 5% of pre-refactor (this refactor is structural, no hot-path change expected).

---

## Out of scope (do not attempt in this plan)

- Splitting `reflex_build_delta_sql`'s remaining aggregate-branch operation match further (the 3 helpers from Phase B are already at the right granularity).
- Renaming any public symbol or `#[pg_extern]`.
- Changing the `__reflex_ivm_reference` schema, the trigger plpgsql body, or any DDL emitter.
- Promoting `BuildContext` to a `pub` API or splitting it into multiple structs.
- Touching `query_decomposer.rs`, `partition.rs`, `schema_builder.rs`, `aggregation.rs`, or any other file beyond the three listed in the File Structure table.
