//! Partitioning support for pg_reflex IMVs.
//!
//! This module contains:
//!   * Introspection helpers for source-table partition descriptors.
//!   * DDL builders for partition children (intermediate + target).
//!   * Validation helpers used by `create_reflex_ivm_impl`.
//!   * Implementation of `reflex_sync_partitions` (with `drop_orphans` flag).
//!   * Implementation of `reflex_reconcile_partition`.
//!
//! Design notes (see `plans/partitioning_2.md` and `plans/partition_plan.md`):
//!   * Only LIST and RANGE are supported in v1 (no HASH).
//!   * For aggregate IMVs the partition columns MUST be a subset of GROUP BY
//!     (Postgres requires unique indexes on partitioned tables to include the
//!     partition key, and our intermediate has a `UNIQUE NULLS NOT DISTINCT`
//!     index on the group-by columns).
//!   * The "anchor source" is the single source table that physically owns
//!     the partition column; that source must itself be partitioned on the
//!     same column with the same strategy.
//!   * Bounds are NEVER stored on our side — `pg_get_partition_constraintdef`
//!     (for the WHERE-clause path) and `pg_get_expr(relpartbound, oid)` (for
//!     the FOR VALUES path) are queried live from `pg_inherits` so we cannot
//!     drift from the source.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;

use crate::query_decomposer::{
    canonical_source, intermediate_table_name, quote_identifier, safe_identifier,
    split_qualified_name,
};
use crate::sql_writer::identifier::format_pg_text_array;

/// A root that has failed this many flushes stops being retried. Its pending row
/// and `last_error` survive for `reflex_doctor`, and dependents stay marked
/// `known_stale`, so the condition is reported rather than silently retried.
pub(crate) const PARTITION_FLUSH_FAILURE_CAP: i32 = 5;

/// An explicit PostgreSQL subtransaction, the same primitive a plpgsql
/// `BEGIN … EXCEPTION` block opens.
///
/// `Spi::connect_mut` does NOT open one — it is only `SPI_connect` plus a
/// scratch memory context — so a primitive that reports failure by RETURNING a
/// string (rather than raising) commits everything it did before failing.
/// Wrapping such a primitive in one of these makes "it returned ERROR" mean
/// "nothing happened".
///
/// Bookkeeping mirrors plpgsql's `exec_stmt_block`: `BeginInternalSubTransaction`
/// switches the current memory context and resource owner, both of which must be
/// restored around the release/rollback.
///
/// A PostgreSQL error RAISED inside the subtransaction (as opposed to one
/// reported by returning `ERROR: …`) does not reach `release`/`rollback`: pgrx
/// turns it into a Rust panic that unwinds through this frame. `Drop` closes the
/// subtransaction on that path, which is not optional — leaving it open lets a
/// plpgsql `EXCEPTION` handler upstack roll back OUR subtransaction instead of
/// its own and abort the backend.
///
/// Unwinding is a safe place to do this: `pg_guard_ffi_boundary` has already run
/// `FlushErrorState` and restored `PG_exception_stack` before panicking, so
/// PostgreSQL is no longer in its error state, and the `SpiClient` opened inside
/// this subtransaction is an inner scope whose own `Drop` has already run
/// `SPI_finish`. That is the same state plpgsql's `PG_CATCH` is in when it calls
/// `RollbackAndReleaseCurrentSubTransaction`.
struct SubTransaction {
    memory_context: pgrx::pg_sys::MemoryContext,
    resource_owner: pgrx::pg_sys::ResourceOwner,
    /// Whether `Drop` still owes a rollback. Cleared BEFORE the FFI call, not
    /// after — see `close`.
    close_owed: bool,
}

impl SubTransaction {
    fn begin() -> Self {
        unsafe {
            let memory_context = pgrx::pg_sys::CurrentMemoryContext;
            let resource_owner = pgrx::pg_sys::CurrentResourceOwner;
            pgrx::pg_sys::BeginInternalSubTransaction(std::ptr::null());
            pgrx::pg_sys::MemoryContextSwitchTo(memory_context);
            Self {
                memory_context,
                resource_owner,
                close_owed: true,
            }
        }
    }

    /// Commit: everything done inside becomes part of the enclosing
    /// transaction, and locks taken inside are reassigned to it.
    fn release(mut self) {
        self.close(true);
    }

    /// Undo everything done inside, including DDL.
    fn rollback(mut self) {
        self.close(false);
    }

    /// `close_owed` is cleared BEFORE the FFI call, and moving it after would be
    /// a bug, not a fix. If the call were ever to raise part-way, `Drop` would
    /// then re-enter a half-run close: `RollbackAndReleaseCurrentSubTransaction`
    /// on a subtransaction already past `TBLOCK_SUBINPROGRESS` takes its
    /// `elog(FATAL)` arm (`xact.c`), turning a hypothetical leak into a certain
    /// backend kill. A close that begins must never be attempted twice.
    ///
    /// It is also unreachable as written. `Drop` only ever takes the rollback
    /// direction, and `RollbackAndReleaseCurrentSubTransaction` cannot raise:
    /// its only non-returning failure is that `elog(FATAL)`, which terminates
    /// rather than unwinding, and `AbortSubTransaction` / `CleanupSubTransaction`
    /// run under `HOLD_INTERRUPTS` and report state problems at WARNING. So the
    /// "raise inside `Drop` while unwinding → abort()" hazard has no trigger.
    /// The release direction guards on parallel mode (impossible here — the
    /// reconcile does DDL, and `BeginInternalSubTransaction` would have refused
    /// first) and on block state (balanced by construction now that `Drop`
    /// closes every path), and `CommitSubTransaction` itself raises nothing.
    fn close(&mut self, commit: bool) {
        if !self.close_owed {
            return;
        }
        self.close_owed = false;
        unsafe {
            if commit {
                pgrx::pg_sys::ReleaseCurrentSubTransaction();
            } else {
                pgrx::pg_sys::RollbackAndReleaseCurrentSubTransaction();
            }
            pgrx::pg_sys::MemoryContextSwitchTo(self.memory_context);
            pgrx::pg_sys::CurrentResourceOwner = self.resource_owner;
        }
    }
}

impl Drop for SubTransaction {
    fn drop(&mut self) {
        self.close(false);
    }
}

/// Take the IMV-name advisory lock in the CALLER's transaction, before any
/// subtransaction is opened.
///
/// PostgreSQL releases a lock first acquired inside a subtransaction when that
/// subtransaction rolls back (measured, not assumed). The sync takes this lock
/// deep inside the reconcile's subtransaction, so a failed reconcile would hand
/// the caller back an `ERROR:` string AND silently drop the mutual exclusion the
/// caller still relies on — `trigger/dispatch.rs` discards that string and goes
/// on to run MERGE/DELETE/INSERT against the same IMV. Acquiring it out here
/// first means the outer transaction owns it either way; the sync's later
/// acquisition of the same key is then a no-op refcount bump.
///
/// INVARIANT: the two-key `(hashtext(name), hashtext(reverse(name)))` form, which
/// every IMV-name advisory lock in pg_reflex shares. A one-key `bigint` lock
/// occupies a different advisory space and would never mutually exclude.
fn acquire_imv_advisory_lock(view_name: &str) {
    let _ = Spi::connect_mut(|client| -> Result<(), ()> {
        let _ = client.update(
            "SELECT pg_advisory_xact_lock(hashtext($1), hashtext(reverse($1)))",
            None,
            &[unsafe {
                DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        );
        Ok(())
    });
}

/// A description of how a source table is partitioned.
///
/// `column_names` are the OUTPUT names (lowercased, unquoted) of the
/// partition-key columns on the source.  `strategy` is "LIST" or "RANGE"
/// (uppercased).
#[derive(Debug, Clone)]
pub(crate) struct PartitionDescriptor {
    pub strategy: String,
    pub column_names: Vec<String>,
}

/// A single existing child partition of a partitioned table.
///
/// `bare_name` is the unqualified relname of the child (e.g. `orders_p1`).
/// `bound_expr` is the SQL fragment usable after `FOR VALUES`
/// (e.g. `FOR VALUES IN ('a', 'b')` → `IN ('a', 'b')`).
#[derive(Debug, Clone)]
pub(crate) struct PartitionChild {
    pub bare_name: String,
    // Faithful introspection of the child's `FOR VALUES …` bound. Current
    // consumers (reconcile's CSV-key probe, the swap executor) key off
    // `bare_name` only and read bounds live via `read_partition_bound`, but the
    // DTO carries the bound so callers needn't re-query it.
    #[allow(dead_code)]
    pub bound_expr: String,
}

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
    /// The node's `pg_class.oid`. Captured during the tree walk so snapshot
    /// diffing keys off the actual relation (not a bare-name re-lookup, which
    /// could match a homonym in another schema — partition children are not
    /// required to share the parent's schema).
    pub oid: u32,
    pub parent_bare: String,
    pub bound_expr: String,
    pub sub_strategy: Option<String>,
    pub sub_columns: Vec<String>,
    /// Absolute tree-depth from the anchor root: the anchor's direct
    /// children are depth 1, their children depth 2, etc. Populated by
    /// `list_partition_tree` from the recursive CTE's `depth` column;
    /// `truncate_partition_tree` keys the level cutoff off it.
    pub depth: usize,
}

/// Read the partition descriptor of `source` (schema-qualified or bare).
/// Returns None if the table is not partitioned, or if the strategy is
/// not LIST/RANGE.
pub(crate) fn introspect_partition_descriptor(
    client: &pgrx::spi::SpiClient<'_>,
    source: &str,
) -> Option<PartitionDescriptor> {
    // `partattrs` is `int2vector` — not directly castable to int2[].  We
    // serialize to text (space-separated) then split + cast to int[] for
    // iteration via `unnest WITH ORDINALITY`.
    let row = client
        .select(
            "SELECT \
                CASE pt.partstrat WHEN 'l' THEN 'LIST' WHEN 'r' THEN 'RANGE' \
                                  WHEN 'h' THEN 'HASH' ELSE 'OTHER' END AS strategy, \
                ARRAY( \
                    SELECT a.attname::text \
                    FROM unnest(string_to_array(pt.partattrs::text, ' ')::int[]) \
                        WITH ORDINALITY AS k(attnum, n) \
                    JOIN pg_attribute a ON a.attrelid = pt.partrelid \
                                       AND a.attnum = k.attnum::smallint \
                    ORDER BY k.n \
                ) AS cols \
             FROM pg_partitioned_table pt \
             WHERE pt.partrelid = to_regclass($1)",
            None,
            &[unsafe {
                DatumWithOid::new(source.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        )
        .ok()?
        .next()?;

    let strategy: String = row.get_by_name::<&str, _>("strategy").ok()??.to_string();
    if strategy != "LIST" && strategy != "RANGE" {
        return None;
    }
    let cols: Vec<String> = row.get_by_name::<Vec<String>, _>("cols").ok()??;
    let normalized: Vec<String> = cols.iter().map(|c| c.to_lowercase()).collect();
    Some(PartitionDescriptor {
        strategy,
        column_names: normalized,
    })
}

/// True when `source` is itself a partitioned table whose partition key
/// includes `col`. Such a source is "co-partitioned" on the column: its own
/// rows already carry the partition key, so it never needs an anchor JOIN to
/// recover it. Accepts schema-qualified or bare names.
pub(crate) fn source_partitioned_on(
    client: &pgrx::spi::SpiClient<'_>,
    source: &str,
    col: &str,
) -> bool {
    let (schema_opt, bare) = canonical_source(source);
    let canonical_name = match schema_opt {
        Some(schema) => format!("{}.{}", schema, bare),
        None => bare,
    };
    introspect_partition_descriptor(client, &canonical_name)
        .map(|d| d.column_names.iter().any(|c| c.eq_ignore_ascii_case(col)))
        .unwrap_or(false)
}

/// List existing child partitions of `parent` (schema-qualified or bare).
/// Returns an empty vector if the parent is not partitioned or has no
/// children. `bound_expr` is the post-FOR-VALUES fragment.
pub(crate) fn list_partition_children(
    client: &pgrx::spi::SpiClient<'_>,
    parent: &str,
) -> Vec<PartitionChild> {
    match client.select(
        "SELECT c.relname::text AS bare_name, \
                pg_get_expr(c.relpartbound, c.oid) AS bound_expr \
         FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = to_regclass($1) \
         ORDER BY c.relname",
        None,
        &[unsafe { DatumWithOid::new(parent.to_string(), PgBuiltInOids::TEXTOID.oid().value()) }],
    ) {
        Ok(iter) => iter
            .filter_map(|row| {
                let bare = row.get_by_name::<&str, _>("bare_name").ok()??.to_string();
                let bound = row.get_by_name::<&str, _>("bound_expr").ok()??.to_string();
                Some(PartitionChild {
                    bare_name: bare,
                    bound_expr: bound,
                })
            })
            .collect(),
        Err(e) => {
            pgrx::warning!(
                "pg_reflex: list_partition_children('{}') SPI error: {}",
                parent,
                e
            );
            Vec::new()
        }
    }
}

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
            c.oid::int8 AS node_oid, \
            t.depth AS node_depth, \
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
                let oid = row.get_by_name::<i64, _>("node_oid").ok()?? as u32;
                let depth = row.get_by_name::<i32, _>("node_depth").ok()?? as usize;
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
                Some(PartitionNode {
                    bare_name: bare,
                    oid,
                    parent_bare: parent,
                    bound_expr: bound,
                    sub_strategy: sub_strategy.filter(|s| s == "LIST" || s == "RANGE"),
                    sub_columns,
                    depth,
                })
            })
            .collect(),
        Err(e) => {
            pgrx::warning!(
                "pg_reflex: list_partition_tree('{}') SPI error: {}",
                root,
                e
            );
            Vec::new()
        }
    }
}

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
        ancestor_chain.get(mirror_depth - 1).cloned()
    } else {
        Some(leaf_bare.to_string())
    }
}

/// Build the `PARTITION BY <strategy> (<cols>)` suffix used in the
/// `CREATE TABLE` DDL for the intermediate and target.
pub(crate) fn build_partition_by_clause(strategy: &str, columns: &[String]) -> String {
    let cols_csv = columns
        .iter()
        .map(|c| format!("\"{}\"", c.to_lowercase()))
        .collect::<Vec<_>>()
        .join(", ");
    format!("PARTITION BY {} ({})", strategy.to_uppercase(), cols_csv)
}

/// Generate the bare child-partition name used for an IMV's intermediate or
/// target child.  We derive it from the source child's bare relname so the
/// IMV-side names stay readable and 1:1 with the source.
pub(crate) fn intermediate_child_name(view_name: &str, source_child_bare: &str) -> String {
    let (_, bare_view) = split_qualified_name(view_name);
    safe_identifier(&format!(
        "__reflex_intermediate_{}_{}",
        bare_view, source_child_bare
    ))
}

pub(crate) fn target_child_name(view_name: &str, source_child_bare: &str) -> String {
    let (_, bare_view) = split_qualified_name(view_name);
    safe_identifier(&format!("{}_{}", bare_view, source_child_bare))
}

/// Schema-prefix the child name with the IMV's schema (defaults to "public").
fn schema_prefix(view_name: &str, child: &str) -> String {
    let (schema, _) = split_qualified_name(view_name);
    let schema = schema.unwrap_or("public");
    format!("\"{}\".\"{}\"", schema, child)
}

/// Build the `CREATE [UNLOGGED] TABLE child PARTITION OF parent FOR VALUES …
/// [PARTITION BY …]` DDL pair for one node of a source partition tree.
///
/// Resolves the parent from `node.parent_bare`: when it equals
pub(crate) struct PartitionNodeDdl {
    pub int_ddl: String,
    pub tgt_ddl: String,
    /// Resolved immediate-parent qualified names the DDL above attaches
    /// into — the exact scope a bound-collision check must search (direct
    /// children of THIS parent only, never the whole multi-level subtree).
    pub int_parent_qual: String,
    pub tgt_parent_qual: String,
}

/// `anchor_root_bare` (quote-insensitively) the parent is the IMV root,
/// otherwise it is the IMV child mirroring that source node. Internal nodes
/// (own partition strategy) get a `PARTITION BY` suffix and are always LOGGED;
/// only leaves honour `unlogged`. Partitioned parents are always LOGGED, so
/// the UNLOGGED keyword must be set per-leaf here.
pub(crate) fn build_partition_node_ddl_pair(
    view_name: &str,
    node: &PartitionNode,
    anchor_root_bare: &str,
    unlogged: bool,
) -> PartitionNodeDdl {
    let (schema_opt, bare_view) = split_qualified_name(view_name);
    let schema = schema_opt.unwrap_or("public");

    // Root detection must be quote-insensitive: `parent_bare` comes from
    // `pg_class.relname` (always unquoted) while `anchor_root_bare` may carry
    // surrounding quotes when the anchor is a quoted/decomposed source (e.g. a
    // CTE sub-IMV passed as `"view__cte_x"`). Comparing the raw strings would
    // treat a top-level child as nested and resolve a non-existent parent.
    let is_top_level = is_top_level_node(node, anchor_root_bare);

    let int_parent = if is_top_level {
        intermediate_table_name(view_name)
    } else {
        schema_prefix(
            view_name,
            &intermediate_child_name(view_name, &node.parent_bare),
        )
    };
    let tgt_parent = if is_top_level {
        format!("\"{}\".\"{}", schema, bare_view) + "\""
    } else {
        schema_prefix(view_name, &target_child_name(view_name, &node.parent_bare))
    };
    let int_child = schema_prefix(
        view_name,
        &intermediate_child_name(view_name, &node.bare_name),
    );
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
    PartitionNodeDdl {
        int_ddl,
        tgt_ddl,
        int_parent_qual: int_parent,
        tgt_parent_qual: tgt_parent,
    }
}

/// True when `node` mirrors a DIRECT child of the anchor source root, i.e. its
/// IMV counterpart attaches straight to the live IMV root.
///
/// Quote-insensitive: `parent_bare` comes from `pg_class.relname` (never
/// quoted) while `anchor_root_bare` may carry quotes when the anchor is a
/// quoted/decomposed source.
pub(crate) fn is_top_level_node(node: &PartitionNode, anchor_root_bare: &str) -> bool {
    node.parent_bare.trim_matches('"') == anchor_root_bare.trim_matches('"')
}

/// DDL that builds one mirror node **detached** and then adds it with a single
/// `ALTER TABLE … ATTACH PARTITION`.
///
/// `CREATE TABLE … PARTITION OF <parent>` takes an `AccessExclusiveLock` on the
/// parent, and PostgreSQL holds every DDL lock to commit. For a top-level node
/// that parent is the live IMV root, so the whole rest of the transaction —
/// including the COMMIT-time reconcile that does the heavy fill — runs with
/// every reader of the IMV blocked, even readers pruning to an unrelated
/// partition. `ATTACH PARTITION` takes only `ShareUpdateExclusiveLock`, which
/// does not conflict with `AccessShare`.
///
/// A brand-new node has nothing to preserve, so it is created standalone; the
/// caller builds its whole sub-partition subtree into it while it is still
/// detached (those `CREATE … PARTITION OF` calls then lock the detached node,
/// never anything live) and issues the single ATTACH afterwards.
#[derive(Debug, Clone)]
pub(crate) struct DetachedNodeDdl {
    pub int_create: String,
    pub tgt_create: String,
    pub int_attach: String,
    pub tgt_attach: String,
}

pub(crate) fn build_detached_node_ddl_pair(
    view_name: &str,
    node: &PartitionNode,
    anchor_root_bare: &str,
    unlogged: bool,
) -> DetachedNodeDdl {
    let attached = build_partition_node_ddl_pair(view_name, node, anchor_root_bare, unlogged);
    let int_child = schema_prefix(
        view_name,
        &intermediate_child_name(view_name, &node.bare_name),
    );
    let tgt_child = schema_prefix(view_name, &target_child_name(view_name, &node.bare_name));
    let is_leaf = node.sub_strategy.is_none();
    let sub_clause = match &node.sub_strategy {
        Some(strat) if !node.sub_columns.is_empty() => {
            format!(" {}", build_partition_by_clause(strat, &node.sub_columns))
        }
        _ => String::new(),
    };
    let create_kw = if unlogged && is_leaf {
        "CREATE UNLOGGED TABLE"
    } else {
        "CREATE TABLE"
    };
    DetachedNodeDdl {
        int_create: format!(
            "{} {} (LIKE {} INCLUDING ALL){}",
            create_kw, int_child, attached.int_parent_qual, sub_clause
        ),
        tgt_create: format!(
            "{} {} (LIKE {} INCLUDING ALL){}",
            create_kw, tgt_child, attached.tgt_parent_qual, sub_clause
        ),
        int_attach: format!(
            "ALTER TABLE {} ATTACH PARTITION {} {}",
            attached.int_parent_qual, int_child, node.bound_expr
        ),
        tgt_attach: format!(
            "ALTER TABLE {} ATTACH PARTITION {} {}",
            attached.tgt_parent_qual, tgt_child, node.bound_expr
        ),
    }
}

/// True when an existing IMV partition child's actual shape disagrees with the
/// shape its mirror node requires, i.e. it must be dropped and recreated.
///
/// `expect_partitioned` is `node.sub_strategy.is_some()` (post-truncation).
/// `actual_relkind` is the existing child's `pg_class.relkind` (`Some('p')` for
/// a partitioned table, `Some('r')`/etc. for a leaf, `None` when no such child
/// exists yet — nothing to heal). `CREATE TABLE IF NOT EXISTS … PARTITION OF`
/// cannot convert one shape into the other in place, so a mismatch is fatal to
/// reconcile until the child is rebuilt.
pub(crate) fn partition_shape_mismatch(
    expect_partitioned: bool,
    actual_relkind: Option<char>,
) -> bool {
    match actual_relkind {
        None => false,
        Some(rk) => {
            let actual_partitioned = rk == 'p';
            actual_partitioned != expect_partitioned
        }
    }
}

/// Build the canonical swap-table name for one source-child of a view.
/// `kind` is "int" (intermediate) or "tgt" (target).
pub(crate) fn swap_partition_name(view_name: &str, kind: &str, source_child_bare: &str) -> String {
    let (_, bare_view) = split_qualified_name(view_name);
    safe_identifier(&format!(
        "__reflex_swap_{}_{}_{}",
        kind, bare_view, source_child_bare
    ))
}

/// Bundle of SQL statements used by the atomic DETACH/ATTACH swap path of
/// `reflex_reconcile_partition_impl`.
///
/// The statements are produced as a deterministic, side-effect-free list so
/// they can be unit-tested without an SPI connection; the runtime simply
/// executes them in order.  The order is significant: build the orphan
/// swap-children, fill them, ATTACH new + DETACH old (atomic under PG's
/// catalog snapshot), drop the old children, then rename the swap children
/// to the canonical partition-child names.
#[derive(Debug, Clone)]
pub(crate) struct SwapPartitionDdl {
    /// Fully-qualified ("schema"."name") name of the new intermediate swap
    /// child.  Used by tests + by the runtime to add CHECK constraints
    /// and ANALYZE.
    pub swap_int_qual: String,
    /// Fully-qualified name of the new target swap child.
    pub swap_tgt_qual: String,
    /// `CREATE TABLE swap_int (LIKE old_int INCLUDING ALL)` (with
    /// `UNLOGGED` when the parent IMV is UNLOGGED).
    pub create_swap_int: String,
    /// Likewise for target.
    pub create_swap_tgt: String,
    /// `INSERT INTO swap_int SELECT * FROM (<base_query>) __src WHERE (<constraint>)`
    /// for aggregate IMVs; None when the IMV is passthrough (no intermediate).
    pub fill_swap_int: Option<String>,
    /// `INSERT INTO swap_tgt SELECT * FROM (<end_query OR base_query>) __end WHERE (<constraint>)`
    pub fill_swap_tgt: String,
    /// `ALTER TABLE swap_int ADD CONSTRAINT … CHECK (<constraint_def>)` —
    /// adding the check BEFORE ATTACH causes PG to skip its own validation
    /// scan, shortening the ACCESS EXCLUSIVE window on the parent.  None
    /// when the constraint def is empty.
    pub check_int: Option<String>,
    /// Likewise for target.
    pub check_tgt: Option<String>,
    // NOTE: the DETACH/ATTACH statements are intentionally NOT produced here.
    // A multi-level leaf's immediate parent is an internal node, not the IMV
    // root, and resolving it requires SPI (pg_inherits). They are therefore
    // built parent-aware in `execute_partition_swap_for_child`.
    /// `ALTER TABLE swap_int DROP CONSTRAINT __reflex_swap_check` — the
    /// CHECK is redundant once attached (the partition bound enforces it),
    /// and leaving it forces PG to re-validate it on every future ATTACH.
    pub drop_check_int: Option<String>,
    pub drop_check_tgt: Option<String>,
    /// `DROP TABLE old_int_child` — orphaned, no readers reach it after
    /// DETACH.
    pub drop_old_int: String,
    pub drop_old_tgt: String,
    /// `ALTER TABLE swap_int RENAME TO <canonical int child name>`.
    pub rename_int: String,
    pub rename_tgt: String,
}

/// Build the full list of swap-partition DDL statements for one (view, source_child)
/// mapping.  Pure function — does not touch SPI or pg_catalog.  Tested directly
/// via `partition::tests::test_build_swap_partition_ddl_shape`.
///
/// Parameters:
///   * `view_name` — the IMV's possibly-schema-qualified name.
///   * `source_child` — the source's partition child (bare name + `bound_expr`
///     in `FOR VALUES …` form).
///   * `int_constraint_def` / `tgt_constraint_def` — output of
///     `pg_get_partition_constraintdef` on the OLD intermediate/target
///     children (used as CHECK constraints so ATTACH skips validation).
///   * `unlogged` — whether the new tables should be UNLOGGED (mirrors the
///     parent IMV's storage mode).
///   * `base_query`, `end_query` — IMV's stored queries; `end_query` empty
///     when the IMV is passthrough.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_swap_partition_ddl(
    view_name: &str,
    source_child: &PartitionChild,
    int_constraint_def: &str,
    tgt_constraint_def: &str,
    unlogged: bool,
    base_query: &str,
    end_query: &str,
) -> SwapPartitionDdl {
    let int_child_bare = intermediate_child_name(view_name, &source_child.bare_name);
    let tgt_child_bare = target_child_name(view_name, &source_child.bare_name);
    let int_child_qual = schema_prefix(view_name, &int_child_bare);
    let tgt_child_qual = schema_prefix(view_name, &tgt_child_bare);

    let swap_int_bare = swap_partition_name(view_name, "int", &source_child.bare_name);
    let swap_tgt_bare = swap_partition_name(view_name, "tgt", &source_child.bare_name);
    let swap_int_qual = schema_prefix(view_name, &swap_int_bare);
    let swap_tgt_qual = schema_prefix(view_name, &swap_tgt_bare);

    let unlogged_kw = if unlogged { "UNLOGGED " } else { "" };

    let create_swap_int = format!(
        "CREATE {kw}TABLE {swap} (LIKE {old} INCLUDING ALL)",
        kw = unlogged_kw,
        swap = swap_int_qual,
        old = int_child_qual
    );
    let create_swap_tgt = format!(
        "CREATE {kw}TABLE {swap} (LIKE {old} INCLUDING ALL)",
        kw = unlogged_kw,
        swap = swap_tgt_qual,
        old = tgt_child_qual
    );

    let fill_swap_int = if end_query.is_empty() {
        None
    } else {
        Some(format!(
            "INSERT INTO {swap} SELECT * FROM ({bq}) __src WHERE ({con})",
            swap = swap_int_qual,
            bq = base_query,
            con = int_constraint_def
        ))
    };

    let fill_swap_tgt = if end_query.is_empty() {
        format!(
            "INSERT INTO {swap} SELECT * FROM ({bq}) __src WHERE ({con})",
            swap = swap_tgt_qual,
            bq = base_query,
            con = tgt_constraint_def
        )
    } else {
        format!(
            "INSERT INTO {swap} SELECT * FROM ({eq}) __end WHERE ({con})",
            swap = swap_tgt_qual,
            eq = end_query,
            con = tgt_constraint_def
        )
    };

    let check_int = if int_constraint_def.is_empty() {
        None
    } else {
        Some(format!(
            "ALTER TABLE {swap} ADD CONSTRAINT __reflex_swap_check CHECK ({con})",
            swap = swap_int_qual,
            con = int_constraint_def
        ))
    };
    let check_tgt = if tgt_constraint_def.is_empty() {
        None
    } else {
        Some(format!(
            "ALTER TABLE {swap} ADD CONSTRAINT __reflex_swap_check CHECK ({con})",
            swap = swap_tgt_qual,
            con = tgt_constraint_def
        ))
    };

    let drop_check_int = check_int.as_ref().map(|_| {
        format!(
            "ALTER TABLE {} DROP CONSTRAINT __reflex_swap_check",
            swap_int_qual
        )
    });
    let drop_check_tgt = check_tgt.as_ref().map(|_| {
        format!(
            "ALTER TABLE {} DROP CONSTRAINT __reflex_swap_check",
            swap_tgt_qual
        )
    });

    let drop_old_int = format!("DROP TABLE {}", int_child_qual);
    let drop_old_tgt = format!("DROP TABLE {}", tgt_child_qual);

    let rename_int = format!(
        "ALTER TABLE {} RENAME TO \"{}\"",
        swap_int_qual, int_child_bare
    );
    let rename_tgt = format!(
        "ALTER TABLE {} RENAME TO \"{}\"",
        swap_tgt_qual, tgt_child_bare
    );

    SwapPartitionDdl {
        swap_int_qual,
        swap_tgt_qual,
        create_swap_int,
        create_swap_tgt,
        fill_swap_int,
        fill_swap_tgt,
        check_int,
        check_tgt,
        drop_check_int,
        drop_check_tgt,
        drop_old_int,
        drop_old_tgt,
        rename_int,
        rename_tgt,
    }
}

/// Transaction-local set of mirror children that `reflex_sync_partitions`
/// created in THIS transaction, recorded as `pg_class` OIDs.
///
/// The COMMIT-time reconcile runs later in the same transaction and needs to
/// know which children are brand new. It cannot infer it: a child created
/// inside sync's SPI scope carries the SPI SUBtransaction's xid in
/// `pg_class.xmin`, not `pg_current_xact_id()`, so the obvious
/// "was this relation created by my transaction" probe answers `false` for
/// exactly the children we must recognise (measured on PG 17.7).
///
/// OIDs rather than names because a GUC value is flat text and a quoted
/// identifier may legally contain the separator; an OID cannot. A child that
/// was dropped and recreated between sync and reconcile gets a new OID and so
/// reads as NOT fresh — the safe direction.
///
/// `set_config(..., is_local => true)` scopes the value to the transaction and
/// discards it on rollback, including rollback of the SPI subtransaction that
/// wrote it.
const FRESH_PARTITIONS_GUC: &str = "pg_reflex.fresh_partition_oids";

fn record_fresh_partitions(client: &mut pgrx::spi::SpiClient<'_>, child_quals: &[String]) {
    for qual in child_quals {
        let _ = client.update(
            &format!(
                "SELECT set_config('{guc}', \
                   concat_ws(',', NULLIF(current_setting('{guc}', true), ''), \
                             to_regclass($1)::oid::text), true)",
                guc = FRESH_PARTITIONS_GUC
            ),
            None,
            &[unsafe { DatumWithOid::new(qual.clone(), PgBuiltInOids::TEXTOID.oid().value()) }],
        );
    }
}

/// True only when `child_qual` is one of the children this transaction's sync
/// created. Any doubt — GUC unset, probe failure, unresolvable name, OID not
/// listed — answers `false`, which routes the caller to the full DETACH/ATTACH
/// swap.
///
/// What that asymmetry buys, stated precisely: a wrong `true` costs the LOCK
/// SHAPE, not data. TRUNCATE-then-fill-in-place is semantically identical to the
/// swap — `build_swap_partition_ddl` also discards the old child wholesale and
/// refills from the same authoritative `base_query`/`end_query` — so misjudging
/// a child as fresh cannot by itself produce wrong rows; it takes
/// `AccessExclusive` on the child where the swap would have taken it on the
/// parent, which is the worse trade for readers of that one partition. A wrong
/// `false` costs only the slower, always-correct path. Failing toward `false`
/// stays the right default, but this predicate is not a data-loss guard.
fn is_fresh_partition(client: &pgrx::spi::SpiClient<'_>, child_qual: &str) -> bool {
    client
        .select(
            &format!(
                "SELECT to_regclass($1)::oid::text = ANY(string_to_array( \
                   COALESCE(current_setting('{guc}', true), ''), ',')) AS fresh",
                guc = FRESH_PARTITIONS_GUC
            ),
            Some(1),
            &[unsafe {
                DatumWithOid::new(child_qual.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        )
        .ok()
        .and_then(|mut it| it.next())
        .and_then(|r| r.get_by_name::<bool, _>("fresh").ok().flatten())
        .unwrap_or(false)
}

/// The two fill statements of the in-place path used when a mirror child is
/// provably empty. Same row-producing queries and same partition-constraint
/// filters as `build_swap_partition_ddl`'s fills — only the destination differs
/// (the live child rather than a detached swap table).
pub(crate) fn build_inplace_partition_fill(
    int_child_qual: &str,
    tgt_child_qual: &str,
    int_constraint_def: &str,
    tgt_constraint_def: &str,
    base_query: &str,
    end_query: &str,
) -> (Option<String>, String) {
    let fill_int = if end_query.is_empty() {
        None
    } else {
        Some(format!(
            "INSERT INTO {child} SELECT * FROM ({bq}) __src WHERE ({con})",
            child = int_child_qual,
            bq = base_query,
            con = int_constraint_def
        ))
    };
    let fill_tgt = if end_query.is_empty() {
        format!(
            "INSERT INTO {child} SELECT * FROM ({bq}) __src WHERE ({con})",
            child = tgt_child_qual,
            bq = base_query,
            con = tgt_constraint_def
        )
    } else {
        format!(
            "INSERT INTO {child} SELECT * FROM ({eq}) __end WHERE ({con})",
            child = tgt_child_qual,
            eq = end_query,
            con = tgt_constraint_def
        )
    };
    (fill_int, fill_tgt)
}

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
            Some(_) => {}
        }
    }
    for (name, _) in snapshot {
        if !cur.contains_key(name.as_str()) {
            out.push((name.clone(), PartitionDiffAction::Drop));
        }
    }
    out
}

/// Detect snapshot divergence from the live tree: missing leaves, new leaves, or oid changes.
/// Returns the bare-names of divergent leaves (empty if perfectly in sync).
pub(crate) fn detect_snapshot_live_divergence(
    snapshot: &[(String, u32)],
    current: &[(String, u32)],
) -> Vec<String> {
    use std::collections::HashMap;
    let snap: HashMap<&str, u32> = snapshot.iter().map(|(n, o)| (n.as_str(), *o)).collect();
    let cur: HashMap<&str, u32> = current.iter().map(|(n, o)| (n.as_str(), *o)).collect();
    let mut out = Vec::new();
    for (n, o) in current {
        match snap.get(n.as_str()) {
            None => out.push(n.clone()),
            Some(&so) if so != *o => out.push(n.clone()),
            _ => {}
        }
    }
    for (n, _) in snapshot {
        if !cur.contains_key(n.as_str()) {
            out.push(n.clone());
        }
    }
    out
}

/// Parse a TEXT[] partition-column input.  Returns None for NULL/empty.
///
/// The user passes column names as a Postgres TEXT[]; pgrx forwards it as
/// `Option<Vec<Option<String>>>` (TEXT[] permits NULL elements).  We strip
/// NULLs, lowercase, and reject empty strings.
pub(crate) fn parse_partition_by_input(input: Option<Vec<Option<String>>>) -> Vec<String> {
    let Some(v) = input else { return Vec::new() };
    v.into_iter()
        .flatten()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Resolve the "anchor source" for a given partition column.
///
/// The anchor is the source table that physically owns the column.  Returns
/// Err(message) when zero or multiple sources claim it, since both are
/// ambiguous.  `sources` are schema-qualified (or bare) source names from
/// the IMV's `depends_on` list.
pub(crate) fn resolve_anchor_source(
    client: &pgrx::spi::SpiClient<'_>,
    partition_col: &str,
    sources: &[String],
) -> Result<String, String> {
    let col = partition_col.to_lowercase();
    let debug_resolve = client
        .select(
            "SELECT current_setting('pg_reflex.debug_resolve_anchor', true)::bool AS v",
            Some(1),
            &[],
        )
        .ok()
        .and_then(|mut it| it.next())
        .and_then(|r| r.get_by_name::<bool, _>("v").ok().flatten())
        .unwrap_or(false);
    // TEMP reflex-debug — remove after diagnosis. Shows the exact source names
    // anchor resolution probes (bare vs schema-qualified) + current search_path.
    if debug_resolve {
        let dbg_path = client
            .select("SELECT current_setting('search_path')", Some(1), &[])
            .ok()
            .and_then(|mut it| it.next())
            .and_then(|r| r.get_by_name::<&str, _>("current_setting").ok().flatten())
            .map(|s| s.to_string())
            .unwrap_or_default();
        pgrx::notice!(
            "REFLEX-DBG resolve_anchor col={:?} search_path={:?} sources={:?}",
            partition_col,
            dbg_path,
            sources
        );
    }
    let mut owners: Vec<String> = Vec::new();
    for s in sources {
        if s.starts_with('<') {
            continue;
        }
        let has = client
            .select(
                "SELECT 1 FROM pg_attribute a \
                 JOIN pg_class c ON c.oid = a.attrelid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE a.attrelid = to_regclass($1) \
                   AND a.attnum > 0 AND NOT a.attisdropped \
                   AND lower(a.attname) = $2 \
                 LIMIT 1",
                Some(1),
                &[
                    unsafe {
                        DatumWithOid::new(s.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    },
                    unsafe { DatumWithOid::new(col.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                ],
            )
            .map(|mut it| it.next().is_some())
            .unwrap_or(false);
        // TEMP reflex-debug — per-source ownership + regclass resolution.
        if debug_resolve {
            let dbg_regclass = client
                .select(
                    "SELECT to_regclass($1)::text AS rc",
                    Some(1),
                    &[unsafe {
                        DatumWithOid::new(s.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    }],
                )
                .ok()
                .and_then(|mut it| it.next())
                .and_then(|r| r.get_by_name::<&str, _>("rc").ok().flatten())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "NULL".to_string());
            pgrx::notice!(
                "REFLEX-DBG   source={:?} to_regclass={:?} owns_col={}",
                s,
                dbg_regclass,
                has
            );
        }
        if has {
            owners.push(s.clone());
        }
    }
    match owners.len() {
        0 => Err(format!(
            "no source table owns partition column '{}'",
            partition_col
        )),
        1 => Ok(owners.into_iter().next().unwrap()),
        _ => {
            // The partition column is commonly the join key, so it appears on
            // several sources. The anchor — whose partition children we mirror
            // — must itself be partitioned ON that column. A bare column on a
            // non-partitioned source (e.g. a sibling sub-IMV) cannot be the
            // anchor, and a source partitioned on a *different* column is not a
            // candidate either.
            let mut owners_on_col: Vec<String> = owners
                .iter()
                .filter(|s| source_partitioned_on(client, s, &col))
                .cloned()
                .collect();

            // A reflex-generated intermediate (`__cte_`/`__union_`/`__base`) can
            // inherit the partition column and descriptor from a base source it
            // reads, so a decomposed query ends up with several partitioned
            // owners. The anchor whose partition children we physically mirror
            // must be a real base table, not a derived intermediate, so prefer
            // base sources when any exist.
            let is_intermediate = |s: &String| {
                let (_, bare) = canonical_source(s);
                bare.contains("__cte_") || bare.contains("__union_") || bare.contains("__base")
            };
            let mut base_on_col: Vec<String> = owners_on_col
                .iter()
                .filter(|s| !is_intermediate(s))
                .cloned()
                .collect();

            // When several sources are co-partitioned on the SAME column (e.g. a
            // FULL/INNER JOIN whose key IS the partition column, as in
            // forecast_analysis_view), their partition layouts align, so ANY of
            // them is a sound anchor for child DDL — this is no longer ambiguous.
            // Pick deterministically (lexicographically) for a stable choice
            // across rebuilds. Non-anchor co-owners are handled in
            // apply_partition_plan: they own the column natively, so they get NO
            // anchor JOIN-path and fall through to Path B (sound for outer rows).
            let pool = if !base_on_col.is_empty() {
                &mut base_on_col
            } else {
                &mut owners_on_col
            };
            if pool.is_empty() {
                Err(format!(
                    "multiple sources own partition column '{}' but none is partitioned on it — ambiguous: {:?}",
                    partition_col, owners
                ))
            } else {
                pool.sort();
                Ok(pool[0].clone())
            }
        }
    }
}

/// Result of `reflex_sync_partitions`.
#[derive(Debug, Default)]
pub(crate) struct SyncResult {
    pub added_intermediate: usize,
    pub added_target: usize,
    pub dropped_intermediate: usize,
    pub dropped_target: usize,
    /// Names of source children present on the IMV but absent from source.
    /// Populated only when drop_orphans = false.
    pub preserved_orphans: Vec<String>,
    /// Set when `drop_orphans` was requested but the live source tree
    /// enumerated empty, so no child could be *confirmed* an orphan.
    pub refused_orphan_drop: bool,
}

impl SyncResult {
    pub fn into_message(self) -> String {
        let mut msg = format!(
            "sync: +{} intermediate, +{} target",
            self.added_intermediate, self.added_target
        );
        if self.dropped_intermediate > 0 || self.dropped_target > 0 {
            msg.push_str(&format!(
                ", -{} intermediate, -{} target",
                self.dropped_intermediate, self.dropped_target
            ));
        }
        if !self.preserved_orphans.is_empty() {
            msg.push_str(&format!(
                ", preserved orphans: {}",
                self.preserved_orphans.join(", ")
            ));
        }
        if self.refused_orphan_drop {
            msg.push_str(", refused orphan drop (source partition set enumerated empty)");
        }
        msg
    }
}

/// Implementation of `reflex_sync_partitions(view_name, drop_orphans)`.
///
/// 1. Reads (partition_columns, partition_strategy) from the IMV reference
///    table.  Returns "OK — not partitioned" if not set.
/// 2. Resolves the anchor source via `depends_on`.
/// 3. Diffs source partitions against current IMV partitions and creates
///    missing children on both intermediate and target.
/// 4. When `drop_orphans` is true (default), drops IMV-side children whose
///    source counterpart no longer exists (CASCADE — but only touches
///    pg_reflex-owned objects below the IMV partition).  When false, emits
///    a NOTICE and preserves them.
pub(crate) fn reflex_sync_partitions_impl(view_name: &str, drop_orphans: bool) -> String {
    if let Err(msg) = crate::validate_view_name(view_name) {
        return msg.to_string();
    }
    let outcome: Result<String, String> = Spi::connect_mut(|client| {
        // Load partition metadata.
        let meta = client
            .select(
                "SELECT partition_columns, partition_strategy, depends_on, storage_mode, partition_depth \
                 FROM public.__reflex_ivm_reference WHERE name = $1",
                Some(1),
                &[unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .map_err(|e| format!("sync: catalog query failed: {}", e))?
            .next();
        let row = match meta {
            Some(r) => r,
            None => return Err(format!("IMV '{}' not found", view_name)),
        };
        let part_cols: Vec<String> = row
            .get_by_name::<Vec<String>, _>("partition_columns")
            .unwrap_or(None)
            .unwrap_or_default();
        let strategy: String = row
            .get_by_name::<&str, _>("partition_strategy")
            .unwrap_or(None)
            .unwrap_or("")
            .to_string();
        if part_cols.is_empty() || strategy.is_empty() {
            return Ok("OK — not partitioned".to_string());
        }
        let sources: Vec<String> = row
            .get_by_name::<Vec<String>, _>("depends_on")
            .unwrap_or(None)
            .unwrap_or_default();
        let unlogged = row
            .get_by_name::<&str, _>("storage_mode")
            .unwrap_or(None)
            .unwrap_or("UNLOGGED")
            .eq_ignore_ascii_case("UNLOGGED");
        let partition_depth: Option<i32> =
            row.get_by_name::<i32, _>("partition_depth").unwrap_or(None);

        // Resolve anchor source.
        let anchor = resolve_anchor_source(client, &part_cols[0], &sources)
            .map_err(|e| format!("sync: {}", e))?;

        // Read source partition tree (all descendants, not just one level).
        let (_, anchor_root_bare) = split_qualified_name(&anchor);
        let full_nodes = list_partition_tree(client, &anchor);
        let mirror_depth = partition_depth
            .map(|d| d as usize)
            .unwrap_or_else(|| max_tree_depth(&full_nodes));
        let nodes = truncate_partition_tree(full_nodes, mirror_depth);
        let int_parent = intermediate_table_name(view_name);
        let tgt_parent = quote_identifier(view_name);
        // Passthrough IMVs (no aggregation / ivm-count) have no intermediate
        // table — `intermediate_column_spec` returns None at create time, so
        // `__reflex_intermediate_<view>` never exists. Creating intermediate
        // partition children of an absent parent raises 42P01; skip all
        // intermediate-child management when the relation is absent, mirroring
        // the `end_query.is_empty()` guard in execute_partition_swap_for_child.
        let has_intermediate: bool = client
            .select(
                "SELECT to_regclass($1) IS NOT NULL AS present",
                Some(1),
                &[unsafe {
                    DatumWithOid::new(int_parent.clone(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .map_err(|e| format!("sync: intermediate existence probe failed: {}", e))?
            .next()
            .and_then(|r| r.get_by_name::<bool, _>("present").ok().flatten())
            .unwrap_or(false);
        // Read IMV partition tree (all descendants).
        let int_children = if has_intermediate {
            list_partition_tree(client, &int_parent)
        } else {
            Vec::new()
        };
        let tgt_children = list_partition_tree(client, &tgt_parent);

        // Build name-keyed views (using the IMV-side bare name format).
        let int_have: std::collections::HashSet<String> =
            int_children.iter().map(|c| c.bare_name.clone()).collect();
        let tgt_have: std::collections::HashSet<String> =
            tgt_children.iter().map(|c| c.bare_name.clone()).collect();
        let src_expected_int: std::collections::HashSet<String> = nodes
            .iter()
            .map(|n| intermediate_child_name(view_name, &n.bare_name))
            .collect();
        let src_expected_tgt: std::collections::HashSet<String> = nodes
            .iter()
            .map(|n| target_child_name(view_name, &n.bare_name))
            .collect();

        let mut out = SyncResult::default();

        // Advisory lock keyed by IMV name so concurrent callers serialize their
        // DDL on this view. INVARIANT: every IMV-name advisory lock in pg_reflex
        // uses the two-key `(hashtext(name), hashtext(reverse(name)))` form
        // (immediate/deferred trigger bodies, deferred flush at
        // trigger/deferred.rs, and partition flush at lib.rs). A one-key `bigint`
        // lock and a two-key lock occupy different advisory-lock spaces in
        // PostgreSQL and never mutually exclude, so sync MUST take the same
        // two-key form to share that space rather than opening a parallel one.
        let _ = client.update(
            "SELECT pg_advisory_xact_lock(hashtext($1), hashtext(reverse($1)))",
            None,
            &[unsafe {
                DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        );

        // When dropping orphans, drop BEFORE adding. A source leaf that was
        // swap-renamed (detach old / attach a freshly-built table with the same
        // bounds) leaves an orphan IMV child whose bounds equal the incoming
        // child's; adding first would raise "would overlap partition". This is
        // the multi-level reconcile path: a sub-level swap renames the sub-IMV
        // leaf, and reconcile(parent) must heal its mirror without overlap.
        // Fail-safe (mirrors the F3 guard in `execute_partition_swap_for_child`):
        // an empty source enumeration carries NO information about orphanhood.
        // `list_partition_tree` returns an empty Vec both when the anchor has no
        // children AND when it could not be read at all — an unqualified anchor
        // name `to_regclass` cannot resolve under the caller's search_path, an
        // anchor that is not partitioned, or a failed catalog query. Treating
        // that as "every child is an orphan" drops EVERY partition of the IMV
        // and empties it. Nothing can be confirmed an orphan here, so refuse.
        let source_enumeration_empty = nodes.is_empty();
        if drop_orphans && source_enumeration_empty {
            out.refused_orphan_drop = true;
            pgrx::warning!(
                "pg_reflex: refused to drop orphan partitions of IMV '{}' — the partition set of \
                 anchor source '{}' enumerated empty, so no child can be confirmed an orphan. \
                 Existing partitions were left intact. Verify the anchor resolves \
                 (SELECT to_regclass('{}')) and is partitioned, then re-run.",
                view_name,
                anchor,
                anchor
            );
        }
        if drop_orphans && !source_enumeration_empty {
            let (schema_opt, _) = split_qualified_name(view_name);
            let schema = schema_opt.unwrap_or("public");
            // Drop bottom-up (children before parents) to avoid depending on CASCADE.
            for c in int_children.iter().rev() {
                if !src_expected_int.contains(&c.bare_name) {
                    let q = format!(
                        "DROP TABLE IF EXISTS \"{}\".\"{}\" CASCADE",
                        schema, c.bare_name
                    );
                    client
                        .update(&q, None, &[])
                        .map_err(|e| format!("sync: drop intermediate child failed: {}", e))?;
                    out.dropped_intermediate += 1;
                }
            }
            for c in tgt_children.iter().rev() {
                if !src_expected_tgt.contains(&c.bare_name) {
                    let q = format!(
                        "DROP TABLE IF EXISTS \"{}\".\"{}\" CASCADE",
                        schema, c.bare_name
                    );
                    client
                        .update(&q, None, &[])
                        .map_err(|e| format!("sync: drop target child failed: {}", e))?;
                    out.dropped_target += 1;
                }
            }
        }

        // Fix #3: heal shape drift. An IMV child created as a leaf whose source
        // counterpart later became partitioned (or vice versa) was previously skipped
        // by `CREATE TABLE IF NOT EXISTS`, leaving a plain table where reconcile
        // expects a partitioned one ("… is not partitioned"). Drop such mismatched
        // children top-down so the create loop below rebuilds them with the right
        // shape. Nodes are depth-ordered, so a dropped internal node's CASCADE removes
        // stale descendants that the create loop then re-creates.
        // Names of mirror children that already exist as a RELATION of any kind
        // — a live partition, or a detached/orphaned leftover carrying the same
        // name. Only a name that exists nowhere may be built detached: reusing
        // the name would raise 42P07 where the old
        // `CREATE TABLE IF NOT EXISTS ... PARTITION OF` silently no-opped.
        let mut existing_children: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        {
            let (schema_opt, _) = split_qualified_name(view_name);
            let schema = schema_opt.unwrap_or("public");
            for node in &nodes {
                let expect_partitioned = node.sub_strategy.is_some();
                for child_bare in [
                    intermediate_child_name(view_name, &node.bare_name),
                    target_child_name(view_name, &node.bare_name),
                ] {
                    let relkind: Option<char> = client
                        .select(
                            "SELECT relkind::text AS rk FROM pg_class c \
                             JOIN pg_namespace n ON n.oid = c.relnamespace \
                             WHERE n.nspname = $1 AND c.relname = $2",
                            Some(1),
                            &[
                                unsafe {
                                    DatumWithOid::new(
                                        schema.to_string(),
                                        PgBuiltInOids::TEXTOID.oid().value(),
                                    )
                                },
                                unsafe {
                                    DatumWithOid::new(
                                        child_bare.clone(),
                                        PgBuiltInOids::TEXTOID.oid().value(),
                                    )
                                },
                            ],
                        )
                        .ok()
                        .and_then(|mut it| it.next())
                        .and_then(|r| r.get_by_name::<&str, _>("rk").ok().flatten())
                        .and_then(|s| s.chars().next());

                    if relkind.is_some() {
                        existing_children.insert(child_bare.clone());
                    }
                    if partition_shape_mismatch(expect_partitioned, relkind) {
                        let q = format!(
                            "DROP TABLE IF EXISTS \"{}\".\"{}\" CASCADE",
                            schema, child_bare
                        );
                        client.update(&q, None, &[]).map_err(|e| {
                            format!("sync: heal drop of mismatched child {}: {}", child_bare, e)
                        })?;
                        existing_children.remove(&child_bare);
                        pgrx::notice!(
                            "pg_reflex: rebuilt partition child '{}' (shape drift: expected {})",
                            child_bare,
                            if expect_partitioned {
                                "partitioned"
                            } else {
                                "leaf"
                            }
                        );
                    }
                }
            }
        }

        let mut drain_roots: Vec<String> = vec![tgt_parent.clone()];
        if has_intermediate {
            drain_roots.push(int_parent.clone());
        }

        // The drain -> build -> refill sequence below is a pure PHYSICAL
        // relocation of rows already present in the IMV — it moves
        // default-resident rows into a holding table and routes them back
        // through `INSERT INTO <root>` once the correct leaf exists. When
        // this IMV is itself a maintenance SOURCE for a downstream chained
        // IMV, its target table carries an `AFTER INSERT ... FOR EACH
        // STATEMENT ... REFERENCING NEW TABLE` trigger (schema_builder.rs)
        // that the refill's `INSERT INTO <root>` would otherwise fire,
        // re-counting merely-relocated rows into the downstream IMV. Suppress
        // those triggers on the relocation roots only — tuple routing is not a
        // trigger and still works — then re-enable them unconditionally so the
        // caller's transaction is unaffected. `ALTER TABLE ... DISABLE TRIGGER
        // USER` is scoped to these tables and requires only their ownership
        // (which the IMV owner holds), unlike the session-wide
        // `session_replication_role` GUC which is superuser-only and would abort
        // reconcile for non-superuser roles.
        // Track which roots we actually disabled so every one is re-enabled on
        // every exit path — including a failure partway through this loop, which
        // must not leave an already-disabled root (e.g. the target table that
        // carries a downstream chained-IMV trigger) disabled in the committing
        // transaction.
        let mut disabled_roots: Vec<&String> = Vec::new();
        let mut disable_err: Option<String> = None;
        for root in &drain_roots {
            match client.update(
                &format!("ALTER TABLE {} DISABLE TRIGGER USER", root),
                None,
                &[],
            ) {
                Ok(_) => disabled_roots.push(root),
                Err(e) => {
                    disable_err = Some(format!(
                        "sync: suppress triggers for relocation on {}: {}",
                        root, e
                    ));
                    break;
                }
            }
        }
        if let Some(e) = disable_err {
            for root in &disabled_roots {
                let _ = client.update(
                    &format!("ALTER TABLE {} ENABLE TRIGGER USER", root),
                    None,
                    &[],
                );
            }
            return Err(e);
        }

        let (schema_opt, _) = split_qualified_name(view_name);
        let schema = schema_opt.unwrap_or("public");

        let reloc_result: Result<(), String> = (|| {
            let drain_entries = drain_tree_defaults(client, &drain_roots)?;

            // Top-level nodes that do not exist yet are built DETACHED and
            // attached with one `ALTER TABLE … ATTACH PARTITION` after the
            // whole tree (including their sub-partition subtree) is created —
            // see `build_detached_node_ddl_pair`. Deferring the attach keeps
            // the subtree build off any live relation and holds the root at
            // ShareUpdateExclusive instead of AccessExclusive.
            let mut pending_attach: Vec<String> = Vec::new();
            // Mirror children this sync run actually creates. They did not exist
            // before this transaction, so nothing in them is worth preserving —
            // see `record_fresh_partitions`.
            let mut created_children: Vec<String> = Vec::new();

            for node in &nodes {
                let int_name = intermediate_child_name(view_name, &node.bare_name);
                let tgt_name = target_child_name(view_name, &node.bare_name);
                let int_is_new = !existing_children.contains(&int_name);
                let tgt_is_new = !existing_children.contains(&tgt_name);
                let ddl =
                    build_partition_node_ddl_pair(view_name, node, anchor_root_bare, unlogged);
                let detached =
                    build_detached_node_ddl_pair(view_name, node, anchor_root_bare, unlogged);
                let top_level = is_top_level_node(node, anchor_root_bare);
                let build_int_detached = top_level && int_is_new;
                let build_tgt_detached = top_level && tgt_is_new;
                if has_intermediate {
                    // Bound-collision heal (untreated_bugs/
                    // 2026-07-25_nightly_swap_target_overlap_restale.md): a
                    // source-side repartition that DETACHes a leaf and
                    // ATTACHes a freshly-named replacement at the SAME bound
                    // leaves the old leaf's mirror child behind as an orphan
                    // (drop_orphans=false — the auto-sync default, since
                    // orphan deletion is never automatic). The CREATE below
                    // would then raise "would overlap partition" against that
                    // orphan. Drop only a CONFIRMED orphan — bounds identical
                    // to the incoming child AND mapped to no live source leaf
                    // — never a broader drop_orphans-style sweep. Mirrors the
                    // F3 heal in `execute_partition_swap_for_child`.
                    drop_bound_collision_orphan(
                        client,
                        schema,
                        &ddl.int_parent_qual,
                        &src_expected_int,
                        &int_name,
                        &node.bound_expr,
                    )?;
                    if build_int_detached {
                        client
                            .update(&detached.int_create, None, &[])
                            .map_err(|e| {
                                format!("sync: create detached intermediate node: {}", e)
                            })?;
                        pending_attach.push(detached.int_attach.clone());
                    } else {
                        client
                            .update(&ddl.int_ddl, None, &[])
                            .map_err(|e| format!("sync: create intermediate node: {}", e))?;
                    }
                    if int_is_new {
                        created_children.push(schema_prefix(view_name, &int_name));
                    }
                    if !int_have.contains(&int_name) {
                        out.added_intermediate += 1;
                    }
                }
                drop_bound_collision_orphan(
                    client,
                    schema,
                    &ddl.tgt_parent_qual,
                    &src_expected_tgt,
                    &tgt_name,
                    &node.bound_expr,
                )?;
                if build_tgt_detached {
                    client
                        .update(&detached.tgt_create, None, &[])
                        .map_err(|e| format!("sync: create detached target node: {}", e))?;
                    pending_attach.push(detached.tgt_attach.clone());
                } else {
                    client
                        .update(&ddl.tgt_ddl, None, &[])
                        .map_err(|e| format!("sync: create target node: {}", e))?;
                }
                if tgt_is_new {
                    created_children.push(schema_prefix(view_name, &tgt_name));
                }
                if !tgt_have.contains(&tgt_name) {
                    out.added_target += 1;
                }
            }
            // Attach the detached nodes AFTER the whole tree is built, so each
            // one goes in complete with its sub-partition subtree and the
            // parent is locked exactly once, at ShareUpdateExclusive. Runs
            // before `refill_tree_defaults` so drained default rows belonging
            // to a new bound still route into their new leaf, exactly as they
            // did when the node was created in place.
            for stmt in &pending_attach {
                client
                    .update(stmt, None, &[])
                    .map_err(|e| format!("sync: attach new node: {}", e))?;
            }
            record_fresh_partitions(client, &created_children);
            refill_tree_defaults(client, drain_entries)?;
            Ok(())
        })();

        // Restore before propagating either result — never leave a relocation
        // root with its maintenance triggers disabled for the caller's
        // continuing transaction, even when the relocation itself failed.
        let mut restore_err: Option<String> = None;
        for root in &disabled_roots {
            if let Err(e) = client.update(
                &format!("ALTER TABLE {} ENABLE TRIGGER USER", root),
                None,
                &[],
            ) {
                if restore_err.is_none() {
                    restore_err = Some(format!("sync: restore triggers on {}: {}", root, e));
                }
            }
        }
        reloc_result?;
        if let Some(e) = restore_err {
            return Err(e);
        }

        if !drop_orphans {
            for c in &int_children {
                if !src_expected_int.contains(&c.bare_name) {
                    out.preserved_orphans.push(c.bare_name.clone());
                    pgrx::notice!(
                        "pg_reflex: orphan intermediate partition '{}' preserved (drop_orphans=false)",
                        c.bare_name
                    );
                }
            }
            for c in &tgt_children {
                if !src_expected_tgt.contains(&c.bare_name) {
                    out.preserved_orphans.push(c.bare_name.clone());
                    pgrx::notice!(
                        "pg_reflex: orphan target partition '{}' preserved (drop_orphans=false)",
                        c.bare_name
                    );
                }
            }
        }

        Ok(out.into_message())
    });
    match outcome {
        Ok(s) => s,
        Err(e) => format!("ERROR: {}", e),
    }
}

/// Drop a CONFIRMED orphan DIRECT CHILD of `parent_qual` whose `FOR VALUES`
/// bound is byte-identical to `about_to_attach`'s: a `CREATE TABLE ...
/// PARTITION OF <parent_qual> ... FOR VALUES <bound>` would otherwise raise
/// "would overlap partition" against it. Scoped to `list_partition_children`
/// (direct children of this ONE parent) rather than the whole multi-level
/// subtree — a flat whole-tree scan would treat unrelated leaves under
/// DIFFERENT branches that happen to share a repeated sub-partition bound
/// literal (e.g. LIST(region) -> LIST(quarter) with 'Q1'..'Q4' under every
/// region) as colliding siblings and drop live, unrelated data. Narrower than
/// a `drop_orphans` sweep too — `expected` gates it to a child that maps to
/// NO live source leaf, so this never touches a partition a live source still
/// backs, regardless of the caller's own `drop_orphans` flag. Same intent as
/// the F3 heal in `execute_partition_swap_for_child`, but NOT the same scoping:
/// that one passes the IMV ROOT rather than the swapped leaf's immediate
/// parent, so on a multi-level tree it compares the root's direct children
/// against a leaf's bound, never matches, and silently heals nothing. Tracked
/// in `untreated_bugs/2026-07-25_swap_f3_heal_wrong_parent_scope_multilevel.md`
/// — do not copy that scoping here.
fn drop_bound_collision_orphan(
    client: &mut pgrx::spi::SpiClient<'_>,
    schema: &str,
    parent_qual: &str,
    expected: &std::collections::HashSet<String>,
    about_to_attach: &str,
    bound_expr: &str,
) -> Result<(), String> {
    if bound_expr.is_empty() {
        return Ok(());
    }
    for child in list_partition_children(client, parent_qual) {
        if child.bare_name == about_to_attach || expected.contains(&child.bare_name) {
            continue;
        }
        if !child.bound_expr.is_empty() && child.bound_expr == bound_expr {
            let q = format!(
                "DROP TABLE IF EXISTS \"{}\".\"{}\" CASCADE",
                schema, child.bare_name
            );
            client.update(&q, None, &[]).map_err(|e| {
                format!(
                    "sync: drop bound-collision orphan '{}': {}",
                    child.bare_name, e
                )
            })?;
            pgrx::notice!(
                "pg_reflex: dropped confirmed orphan partition '{}' (bounds matched incoming child '{}')",
                child.bare_name,
                about_to_attach
            );
        }
    }
    Ok(())
}

/// Implementation of `reflex_reconcile_partition(view_name, partition_keys)`.
///
/// Atomic DETACH/ATTACH swap (Phase A of `plans/partitioning_3.md`).
///
/// 1. Idempotent cleanup pass: drop any leftover `__reflex_swap_*` tables
///    for this view from a prior failed swap.  Detected purely by name
///    prefix — no catalog state to maintain.
/// 2. Sync source partitions (`reflex_sync_partitions_impl(.., drop_orphans=true)`).
/// 3. For each provided key, locate the matching child via
///    `pg_get_partition_constraintdef` and gather (intermediate child,
///    target child, source child, both constraints, both `FOR VALUES`
///    bounds, persistence).
/// 4. For each matching child, build the new partition outside the
///    partition tree, fill it, add a CHECK constraint matching the
///    bound (so ATTACH skips its validation scan), DETACH the old
///    children + ATTACH the new ones, drop the old children, rename the
///    swap children to the canonical names.  Lock hold on the parent
///    drops from "full rebuild duration" to "the ALTER TABLE swap
///    itself" (µs).
/// 5. Cascade to dependents:
///    * Same partition column → partition-scoped reconcile.
///    * Non-partitioned but GROUP BY this column → key-scoped reconcile.
///    * Anything else → full reconcile.
///
/// Atomicity: failures are REPORTED (an `ERROR: …` return value), not raised,
/// so the calling statement commits. Steps 1-4 therefore run inside an explicit
/// `SubTransaction` that is rolled back on any reported failure — otherwise the
/// destructive pre-sync (`DROP TABLE … CASCADE` on orphan children) and any
/// children already swapped would stay committed while the caller is told the
/// call failed. `Spi::connect_mut` gives no such isolation on its own.
///
/// `skip_sync` skips only the O(tree) PREP, never the isolation: the batch
/// flush's plpgsql `EXCEPTION` block is a subtransaction, but one that only
/// rolls back on a RAISED error — a returned `ERROR: …` lets the block complete
/// normally and RELEASE, committing every child already swapped. So the
/// subtransaction is opened on both paths.
pub(crate) fn reflex_reconcile_partition_impl(
    view_name: &str,
    partition_keys_csv: &str,
    source_partition: &str,
    skip_sync: bool,
) -> String {
    if let Err(msg) = crate::validate_view_name(view_name) {
        return msg.to_string();
    }
    // Held by the CALLER's transaction, so a rolled-back reconcile still leaves
    // it in place for whatever the caller does next with the returned string.
    acquire_imv_advisory_lock(view_name);

    // Opened BEFORE the destructive pre-sync so a reported failure anywhere
    // below undoes it, on both the standalone and the batch path.
    let subxact = SubTransaction::begin();

    // The batch flush path (`reflex_flush_partitions_impl`) syncs the tree and
    // cleans orphan swaps once up front, then drives many per-leaf reconciles
    // with `skip_sync = true` so this O(tree) prep isn't repeated per leaf.
    // The standalone / cascade entry points pass `false` and stay self-contained.
    let presync: Result<(), String> = if skip_sync {
        Ok(())
    } else {
        // Idempotent recovery: drop any orphan __reflex_swap_* tables left over
        // from a prior aborted swap.  Names are deterministic from view +
        // source-child bare name (see `swap_partition_name`); we identify them
        // by the `__reflex_swap_int_<bare_view>_` / `__reflex_swap_tgt_<bare_view>_`
        // prefix, scoped to the IMV's schema.
        cleanup_orphan_swap_tables(view_name);

        // First, ensure the partition set is in sync with the source. A sync
        // that FAILS leaves the partition set neither known-current nor
        // necessarily unchanged; swapping children against it would rebuild
        // slices from a tree we could not verify, so refuse instead of
        // discarding the failure. (A refused orphan drop is not a failure —
        // it is the sync declining to destroy, and reports no ERROR.)
        let sync = reflex_sync_partitions_impl(view_name, true);
        if sync.starts_with("ERROR") {
            Err(format!("reconcile_partition: pre-sync failed: {}", sync))
        } else {
            Ok(())
        }
    };

    let outcome: Result<String, String> = presync.and_then(|()| Spi::connect_mut(|client| {
        let row = client
            .select(
                "SELECT base_query, end_query, partition_columns, partition_strategy, depends_on, graph_child, storage_mode, partition_depth \
                 FROM public.__reflex_ivm_reference WHERE name = $1 AND enabled = TRUE",
                Some(1),
                &[unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .map_err(|e| format!("reconcile_partition: catalog query failed: {}", e))?
            .next();
        let row = match row {
            Some(r) => r,
            None => return Err(format!("IMV '{}' not found or disabled", view_name)),
        };
        let base_query: String = row
            .get_by_name::<&str, _>("base_query")
            .unwrap_or(None)
            .unwrap_or("")
            .to_string();
        let end_query: String = row
            .get_by_name::<&str, _>("end_query")
            .unwrap_or(None)
            .unwrap_or("")
            .to_string();
        let part_cols: Vec<String> = row
            .get_by_name::<Vec<String>, _>("partition_columns")
            .unwrap_or(None)
            .unwrap_or_default();
        let strategy: String = row
            .get_by_name::<&str, _>("partition_strategy")
            .unwrap_or(None)
            .unwrap_or("")
            .to_string();
        let children: Vec<String> = row
            .get_by_name::<Vec<String>, _>("graph_child")
            .unwrap_or(None)
            .unwrap_or_default();
        let storage_mode: String = row
            .get_by_name::<&str, _>("storage_mode")
            .unwrap_or(None)
            .unwrap_or("UNLOGGED")
            .to_string();
        let partition_depth: Option<i32> =
            row.get_by_name::<i32, _>("partition_depth").unwrap_or(None);
        let depends_on: Vec<String> = row
            .get_by_name::<Vec<String>, _>("depends_on")
            .unwrap_or(None)
            .unwrap_or_default();
        if part_cols.is_empty() || strategy.is_empty() {
            return Err(format!(
                "IMV '{}' is not partitioned — use reflex_reconcile",
                view_name
            ));
        }

        let tgt_parent = quote_identifier(view_name);
        let schema = split_qualified_name(view_name)
            .0
            .unwrap_or("public")
            .to_string();
        let unlogged = storage_mode.eq_ignore_ascii_case("UNLOGGED");
        let part_col = &part_cols[0];

        let mut to_process: std::collections::HashSet<String> = std::collections::HashSet::new();

        if !source_partition.trim().is_empty() {
            // Level-agnostic + depth-aware path: expand each named source
            // partition to source leaves, map each UP to the IMV's mirror-depth
            // node, then to its IMV target child name. When the IMV mirrors the
            // full source depth this is the identity (leaf -> leaf).
            //
            // `source_partition` accepts a comma-separated list so the batch
            // flush can reconcile every changed leaf of one IMV in a SINGLE
            // call — the affected-key derivation and dependent cascade below
            // then run ONCE over the union, instead of once per leaf (which
            // redundantly rebuilds the same dependent slice N times).
            let anchor = resolve_anchor_source(client, part_col, &depends_on).unwrap_or_default();
            let full_tree = if anchor.is_empty() {
                Vec::new()
            } else {
                list_partition_tree(client, &anchor)
            };
            let mirror_depth = partition_depth
                .map(|d| d as usize)
                .unwrap_or_else(|| max_tree_depth(&full_tree));
            for sp in source_partition
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            {
                for src_leaf in expand_source_partition_to_leaves(client, sp) {
                    let chain = leaf_ancestor_chain(&full_tree, &src_leaf);
                    let node = ancestor_bare_at_depth(&chain, &src_leaf, mirror_depth)
                        .unwrap_or_else(|| src_leaf.clone());
                    to_process.insert(target_child_name(view_name, &node));
                }
            }
        } else {
            let keys: Vec<String> = partition_keys_csv
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if keys.is_empty() {
                return Err(
                    "reconcile_partition: empty partition_keys (or pass source_partition)"
                        .to_string(),
                );
            }
            let tgt_children = list_partition_children(client, &tgt_parent);
            for key in &keys {
                let child_match = tgt_children.iter().find(|c| {
                    let oid_q =
                        "SELECT pg_get_partition_constraintdef(to_regclass($1)::oid) AS def";
                    let qname = format!(
                        "{}.{}",
                        split_qualified_name(view_name).0.unwrap_or("public"),
                        c.bare_name
                    );
                    let def: Option<String> = client
                        .select(
                            oid_q,
                            Some(1),
                            &[unsafe {
                                DatumWithOid::new(
                                    qname.clone(),
                                    PgBuiltInOids::TEXTOID.oid().value(),
                                )
                            }],
                        )
                        .ok()
                        .and_then(|mut it| it.next())
                        .and_then(|r| {
                            r.get_by_name::<&str, _>("def")
                                .ok()
                                .flatten()
                                .map(|s| s.to_string())
                        });
                    let def = match def {
                        Some(d) if !d.is_empty() => d,
                        _ => return false,
                    };
                    let lit = sql_literal_text(key);
                    let substituted = substitute_identifier(&def, part_col, &lit);
                    let probe = format!("SELECT ({})::boolean AS match", substituted);
                    let matched: Option<bool> = client
                        .select(&probe, Some(1), &[])
                        .ok()
                        .and_then(|mut it| it.next())
                        .and_then(|r| r.get_by_name::<bool, _>("match").ok().flatten());
                    matched.unwrap_or(false)
                });
                if let Some(c) = child_match {
                    // A matched top-level child may itself be partitioned (internal
                    // node) on a multi-level IMV: expand to its leaves (or itself).
                    for leaf in target_leaves_under(client, &schema, &c.bare_name) {
                        to_process.insert(leaf);
                    }
                }
            }
        }

        if to_process.is_empty() {
            return Ok(format!(
                "reconcile_partition: no children matched (keys={:?}, source_partition={:?})",
                partition_keys_csv, source_partition
            ));
        }

        // Deterministic order. `to_process` is a set, and iterating it in hash
        // order made both the NOTICE stream and — when one child fails midway —
        // WHICH children had already been swapped vary run to run, so a partial
        // failure was not reproducible. It also makes the returned
        // "RECONCILED partitions: …" list stable.
        let mut to_process: Vec<String> = to_process.into_iter().collect();
        to_process.sort();

        // Process each child via the shared atomic DETACH/ATTACH swap
        // helper.  The intermediate is swapped before the target so the
        // target's `end_query` fill reads the fresh intermediate.
        for child_bare in &to_process {
            // Recover the source bare-child name from the target bare-
            // child name (target = "<bare_view>_<src_child>" per
            // `target_child_name`).
            let (_, view_bare) = split_qualified_name(view_name);
            let src_child_bare = child_bare
                .strip_prefix(&format!("{}_", view_bare))
                .unwrap_or(child_bare)
                .to_string();
            execute_partition_swap_for_child(
                client,
                view_name,
                &schema,
                &src_child_bare,
                &base_query,
                &end_query,
                unlogged,
            )
            .map_err(|e| format!("reconcile_partition: {}", e))?;
        }

        // Update last_update_date.
        let _ = client.update(
            "UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() WHERE name = $1",
            None,
            &[unsafe {
                DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        );

        // Affected partition-key values driving this reconcile — used to scope
        // the cascade into non-partitioned aggregate dependents. Taken from the
        // keys when called by key; otherwise derived from the reconciled
        // parent's target children (the swap-fill / flush path passes a source
        // partition, not keys).
        let affected_keys: Vec<String> = if !partition_keys_csv.trim().is_empty() {
            partition_keys_csv
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        } else {
            let mut set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
            for child_bare in &to_process {
                let q = format!(
                    "SELECT DISTINCT \"{}\"::text AS k FROM \"{}\".\"{}\"",
                    part_col, schema, child_bare
                );
                if let Ok(it) = client.select(&q, None, &[]) {
                    for row in it {
                        if let Some(k) = row.get_by_name::<&str, _>("k").ok().flatten() {
                            set.insert(k.to_string());
                        }
                    }
                }
            }
            set.into_iter().collect()
        };

        // Cascade to dependents.  Resolve each dependent's own metadata:
        //   * partitioned on the SAME column        -> partition-scoped reconcile.
        //   * non-partitioned, GROUP BY this column  -> key-scoped reconcile.
        //   * anything else                          -> full reconcile.
        for child in &children {
            let dep = client
                .select(
                    "SELECT partition_columns, base_query, end_query, aggregations::text AS agg_json \
                     FROM public.__reflex_ivm_reference WHERE name = $1",
                    Some(1),
                    &[unsafe {
                        DatumWithOid::new(child.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    }],
                )
                .ok()
                .and_then(|mut it| it.next());
            let (dep_cols, dep_base, dep_end, dep_group_by): (
                Vec<String>,
                String,
                String,
                Vec<String>,
            ) = match dep {
                Some(r) => {
                    let group_by = r
                        .get_by_name::<&str, _>("agg_json")
                        .ok()
                        .flatten()
                        .and_then(|j| {
                            serde_json::from_str::<crate::aggregation::AggregationPlan>(j).ok()
                        })
                        .map(|p| p.group_by_columns)
                        .unwrap_or_default();
                    (
                        r.get_by_name::<Vec<String>, _>("partition_columns")
                            .ok()
                            .flatten()
                            .unwrap_or_default(),
                        r.get_by_name::<&str, _>("base_query")
                            .ok()
                            .flatten()
                            .unwrap_or("")
                            .to_string(),
                        r.get_by_name::<&str, _>("end_query")
                            .ok()
                            .flatten()
                            .unwrap_or("")
                            .to_string(),
                        group_by,
                    )
                }
                None => (Vec::new(), String::new(), String::new(), Vec::new()),
            };
            let same_part = !dep_cols.is_empty()
                && dep_cols
                    .first()
                    .map(|c| c.eq_ignore_ascii_case(part_col))
                    .unwrap_or(false);
            // We can't reflex_reconcile_partition / reflex_reconcile from
            // inside this SPI scope directly — call the inner impls in a
            // fresh SPI session by deferring via PERFORM at SQL level.
            if same_part {
                let q = format!(
                    "SELECT public.reflex_reconcile_partition({}, {})",
                    sql_literal_text(child),
                    sql_literal_text(partition_keys_csv)
                );
                let _ = client.update(&q, None, &[]);
            } else if let Some(scoped) = build_scoped_cascade_reconcile(
                child,
                part_col,
                &affected_keys,
                &dep_cols,
                &dep_base,
                &dep_end,
                &dep_group_by,
            ) {
                let _ = client.update(&scoped, None, &[]);
            } else {
                let q = format!(
                    "SELECT public.reflex_reconcile({})",
                    sql_literal_text(child)
                );
                let _ = client.update(&q, None, &[]);
            }
        }

        Ok(format!(
            "RECONCILED partitions: {}",
            to_process.join(", ")
        ))
    }));

    if outcome.is_ok() {
        subxact.release();
    } else {
        subxact.rollback();
    }

    match outcome {
        Ok(s) => s,
        Err(e) => format!("ERROR: {}", e),
    }
}

/// Build a key-scoped cascade reconcile of a NON-partitioned aggregate
/// dependent whose GROUP BY includes the parent's partition key `part_col`.
///
/// A full `reflex_reconcile` of such a dependent TRUNCATEs and rebuilds it by
/// rescanning EVERY partition of the source — even when a single parent key was
/// reconciled. Instead, rebuild only the affected `part_col` groups with a
/// literal-pruned `DELETE`+`INSERT` of the intermediate and target slices for
/// `part_col IN (<keys>)`. The literal `IN` list is what lets PostgreSQL prune
/// the source partitions (a subquery/array form does not — verified).
///
/// Returns `None` (caller falls back to full reconcile) when the dependent is
/// partitioned, is a passthrough (no intermediate to scope), does not group by
/// `part_col`, has no affected keys, or its base query has no spliceable
/// `GROUP BY`. The emitted DO block self-heals: any runtime error in the scoped
/// path runs a full `reflex_reconcile` in its EXCEPTION branch, so the
/// optimization can never leave the dependent incorrect.
fn build_scoped_cascade_reconcile(
    child: &str,
    part_col: &str,
    affected_keys: &[String],
    dep_partition_cols: &[String],
    base_query: &str,
    end_query: &str,
    group_by_columns: &[String],
) -> Option<String> {
    if !dep_partition_cols.is_empty() {
        return None; // partitioned dependents use the co-partitioned path
    }
    if end_query.trim().is_empty() {
        return None; // passthrough: no intermediate table to scope
    }
    if affected_keys.is_empty() {
        return None;
    }
    if !group_by_columns
        .iter()
        .any(|c| c.eq_ignore_ascii_case(part_col))
    {
        return None; // dependent does not group by the parent's partition key
    }

    let lits = affected_keys
        .iter()
        .map(|k| sql_literal_text(k))
        .collect::<Vec<_>>()
        .join(", ");
    let key_pred = format!("\"{}\" IN ({})", part_col, lits);

    // Inject the literal filter BEFORE the source GROUP BY so it prunes the
    // source scan (wrapping post-aggregation would not push through GROUP BY).
    let spliced_base = crate::trigger::splice_before_group_by(
        base_query,
        &format!(" AND \"{}\" IN ({})", part_col, lits),
    )?;

    let intermediate = intermediate_table_name(child);
    let target = quote_identifier(child);
    let child_lit = sql_literal_text(child);

    Some(format!(
        "DO $reflex_scoped_cascade$ \
         BEGIN \
           DELETE FROM {int} WHERE {pred}; \
           INSERT INTO {int} {spliced_base}; \
           DELETE FROM {tgt} WHERE {pred}; \
           INSERT INTO {tgt} SELECT * FROM ({end_query}) __reflex_scoped WHERE {pred}; \
           UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() WHERE name = {child_lit}; \
         EXCEPTION WHEN OTHERS THEN \
           PERFORM public.reflex_reconcile({child_lit}); \
         END \
         $reflex_scoped_cascade$",
        int = intermediate,
        tgt = target,
        pred = key_pred,
        spliced_base = spliced_base,
        end_query = end_query,
        child_lit = child_lit,
    ))
}

/// Execute the per-child atomic DETACH/ATTACH swap.  Shared between
/// `reflex_reconcile_partition_impl` (partition-scoped) and the global
/// partition-aware path in `reconcile::reflex_reconcile` (rebuilds every
/// partition via swap instead of TRUNCATE-on-parent).
///
/// `src_child_bare` is the source-child's bare relname; the intermediate
/// and target child names are derived from it.  Reads each child's
/// bound + constraint def live from `pg_class` / `pg_get_partition_*`.
///
/// Returns Err(message) on any DDL failure. `Spi::connect_mut` is not a
/// sub-transaction, so it is the CALLER that owns rolling those partial swaps
/// back: `reflex_reconcile_partition_impl` does it with an explicit
/// `SubTransaction`, and the batch flush with the plpgsql `EXCEPTION` block it
/// dispatches every root's statements through.
///
/// The swap's `ALTER TABLE`s are bracketed by [`set_internal_swap_root`], which
/// tells the `ddl_command_end` event trigger that the partition-tree churn it is
/// about to observe is pg_reflex's own and TRANSIENT. Without it a dependent IMV
/// re-mirrors the mid-swap child set — adopting a `<dep>___reflex_swap_tgt_*`
/// child and dropping its real one as a bound-collision orphan — and is left
/// EMPTY once the closing RENAME puts the parent back. The bracket is cleared on
/// the error path too; a hard error longjmps past it, but `SET LOCAL` unwinds
/// with the aborting (sub)transaction, so the GUC cannot outlive the swap.
pub(crate) fn execute_partition_swap_for_child(
    client: &mut pgrx::spi::SpiClient<'_>,
    view_name: &str,
    schema: &str,
    src_child_bare: &str,
    base_query: &str,
    end_query: &str,
    unlogged: bool,
) -> Result<(), String> {
    set_internal_swap_root(client, Some(view_name));
    let result = swap_partition_child_ddl(
        client,
        view_name,
        schema,
        src_child_bare,
        base_query,
        end_query,
        unlogged,
    );
    set_internal_swap_root(client, None);
    result
}

/// Publish (or clear, with `None`) the IMV whose partition tree is mid-swap, in
/// a transaction-scoped GUC `__reflex_on_ddl_command_end` reads. `SET LOCAL` so
/// it reverts at (sub)transaction end even when a hard error skips the clear;
/// the placeholder GUC name (contains a dot) needs no prior definition.
fn set_internal_swap_root(client: &mut pgrx::spi::SpiClient<'_>, root: Option<&str>) {
    let sql = match root {
        Some(name) => format!(
            "SET LOCAL pg_reflex.internal_swap_root = '{}'",
            name.replace('\'', "''")
        ),
        None => "SET LOCAL pg_reflex.internal_swap_root = ''".to_string(),
    };
    let _ = client.update(&sql, None, &[]);
}

#[allow(clippy::too_many_arguments)]
fn swap_partition_child_ddl(
    client: &mut pgrx::spi::SpiClient<'_>,
    view_name: &str,
    schema: &str,
    src_child_bare: &str,
    base_query: &str,
    end_query: &str,
    unlogged: bool,
) -> Result<(), String> {
    let int_parent = intermediate_table_name(view_name);
    let tgt_parent = quote_identifier(view_name);
    let int_child_bare = intermediate_child_name(view_name, src_child_bare);
    let tgt_child_bare = target_child_name(view_name, src_child_bare);

    // The swap replaces the child with `CREATE TABLE swap (LIKE old INCLUDING
    // ALL)`. `LIKE` NEVER copies partitioning — PostgreSQL has no
    // `INCLUDING PARTITIONING` — so on a PARTITIONED child the replacement is a
    // plain table, and the DETACH/ATTACH/DROP that follows silently flattens the
    // mirror and discards its whole sub-partition subtree. The data is still
    // correct at that instant, so nothing complains; the next partition sync
    // sees the shape drift, drops the flattened children and recreates them
    // EMPTY, taking the IMV to zero rows.
    //
    // Both operator entry points resolve mirror LEAVES before calling here, so
    // this guard is a backstop rather than a routine path. It refuses instead of
    // raising: the caller keeps its transaction and decides what to report.
    for (bare, what) in [
        (&tgt_child_bare, "target"),
        (&int_child_bare, "intermediate"),
    ] {
        if what == "intermediate" && end_query.is_empty() {
            continue;
        }
        if is_partitioned_relation(client, schema, bare) {
            return Err(format!(
                "cannot swap sub-partitioned {} child '{}' — the swap's \
                 'LIKE ... INCLUDING ALL' replacement cannot carry partitioning and would \
                 flatten the mirror; reconcile its leaves instead (reflex_reconcile_partition \
                 with source_partition => '{}')",
                what, bare, src_child_bare
            ));
        }
    }

    let int_bound = read_partition_bound(client, schema, &int_child_bare);
    let tgt_bound = read_partition_bound(client, schema, &tgt_child_bare);
    let int_def = read_partition_constraint_def(client, schema, &int_child_bare);
    let tgt_def = read_partition_constraint_def(client, schema, &tgt_child_bare);

    if int_bound.is_empty() && !end_query.is_empty() {
        return Err(format!(
            "missing intermediate bound for child '{}'",
            int_child_bare
        ));
    }
    if tgt_bound.is_empty() {
        return Err(format!(
            "missing target bound for child '{}'",
            tgt_child_bare
        ));
    }

    // A mirror child with nothing worth preserving does not need the
    // DETACH/ATTACH swap, and the swap costs an `AccessExclusiveLock` on the
    // child's immediate parent — the IMV ROOT at mirror depth 1 — held to
    // commit, which freezes every reader of the IMV including readers pruning
    // to an unrelated partition. Two disjoint proofs qualify a child:
    //
    //   * it is EMPTY, so the in-place fill trivially reproduces what the swap
    //     would have built; or
    //   * it is FRESH — created by this transaction's sync (`is_fresh_partition`)
    //     — in which case whatever it holds arrived after transaction start and
    //     the swap would discard it anyway, so TRUNCATE + fill is equivalent.
    //     The load's own IMV maintenance delta lands in a brand-new child before
    //     the COMMIT-time reconcile reaches it, so the emptiness proof alone
    //     misses the commonest field shape (create/attach a partition and load it
    //     in one transaction).
    //
    // TRUNCATE takes `AccessExclusive` on the CHILD only — never on the parent —
    // and fires no statement-level TRUNCATE trigger of the root, so it inherits
    // the swap's isolation without the swap's lock. Both probes fail toward
    // "not qualified", so any doubt still takes the swap.
    let int_child_qual_probe = schema_prefix(view_name, &int_child_bare);
    let tgt_child_qual_probe = schema_prefix(view_name, &tgt_child_bare);
    let int_is_empty = end_query.is_empty()
        || (!int_def.is_empty() && !relation_has_rows(client, &int_child_qual_probe));
    let tgt_is_empty = !tgt_def.is_empty() && !relation_has_rows(client, &tgt_child_qual_probe);
    let int_is_fresh = end_query.is_empty()
        || (!int_def.is_empty() && is_fresh_partition(client, &int_child_qual_probe));
    let tgt_is_fresh = !tgt_def.is_empty() && is_fresh_partition(client, &tgt_child_qual_probe);
    if (int_is_empty || int_is_fresh) && (tgt_is_empty || tgt_is_fresh) {
        let (fill_int, fill_tgt) = build_inplace_partition_fill(
            &int_child_qual_probe,
            &tgt_child_qual_probe,
            &int_def,
            &tgt_def,
            base_query,
            end_query,
        );
        if let Some(ref fill) = fill_int {
            if !int_is_empty {
                client
                    .update(&format!("TRUNCATE {}", int_child_qual_probe), None, &[])
                    .map_err(|e| format!("truncate fresh int child: {}", e))?;
            }
            client
                .update(fill, None, &[])
                .map_err(|e| format!("fill empty int child in place: {}", e))?;
            let _ = client.update(&format!("ANALYZE {}", int_child_qual_probe), None, &[]);
        }
        if !tgt_is_empty {
            client
                .update(&format!("TRUNCATE {}", tgt_child_qual_probe), None, &[])
                .map_err(|e| format!("truncate fresh tgt child: {}", e))?;
        }
        client
            .update(&fill_tgt, None, &[])
            .map_err(|e| format!("fill empty tgt child in place: {}", e))?;
        let _ = client.update(&format!("ANALYZE {}", tgt_child_qual_probe), None, &[]);
        return Ok(());
    }

    let src_child = PartitionChild {
        bare_name: src_child_bare.to_string(),
        bound_expr: tgt_bound.clone(),
    };
    let ddl = build_swap_partition_ddl(
        view_name, &src_child, &int_def, &tgt_def, unlogged, base_query, end_query,
    );

    // Compute immediate parents for multi-level support. For single-level partitions,
    // these will match the root (int_parent / tgt_parent).
    let int_child_qual = schema_prefix(view_name, &int_child_bare);
    let tgt_child_qual = schema_prefix(view_name, &tgt_child_bare);
    let int_immediate_parent = read_immediate_parent_qual(client, schema, &int_child_bare)
        .unwrap_or_else(|| int_parent.clone());
    let tgt_immediate_parent = read_immediate_parent_qual(client, schema, &tgt_child_bare)
        .unwrap_or_else(|| tgt_parent.clone());

    // Build parent-aware DDL statements for all four operations.
    let int_attach_bound: &str = if !int_bound.is_empty() && int_bound != tgt_bound {
        int_bound.as_str()
    } else {
        tgt_bound.as_str()
    };
    let detach_old_int = format!(
        "ALTER TABLE {} DETACH PARTITION {}",
        int_immediate_parent, int_child_qual
    );
    let attach_new_int = format!(
        "ALTER TABLE {} ATTACH PARTITION {} {}",
        int_immediate_parent, ddl.swap_int_qual, int_attach_bound
    );
    let detach_old_tgt = format!(
        "ALTER TABLE {} DETACH PARTITION {}",
        tgt_immediate_parent, tgt_child_qual
    );
    let attach_new_tgt = format!(
        "ALTER TABLE {} ATTACH PARTITION {} {}",
        tgt_immediate_parent, ddl.swap_tgt_qual, tgt_bound
    );

    // ============ INTERMEDIATE SWAP ============
    if !end_query.is_empty() {
        client
            .update(&ddl.create_swap_int, None, &[])
            .map_err(|e| format!("create swap int: {}", e))?;
        if let Some(ref fill) = ddl.fill_swap_int {
            client
                .update(fill, None, &[])
                .map_err(|e| format!("fill swap int: {}", e))?;
        }
        let _ = client.update(&format!("ANALYZE {}", ddl.swap_int_qual), None, &[]);
        if let Some(ref c) = ddl.check_int {
            client
                .update(c, None, &[])
                .map_err(|e| format!("add check int: {}", e))?;
        }
        client
            .update(&detach_old_int, None, &[])
            .map_err(|e| format!("detach old int: {}", e))?;
        client
            .update(&attach_new_int, None, &[])
            .map_err(|e| format!("attach new int: {}", e))?;
        if let Some(ref c) = ddl.drop_check_int {
            let _ = client.update(c, None, &[]);
        }
        client
            .update(&ddl.drop_old_int, None, &[])
            .map_err(|e| format!("drop old int: {}", e))?;
        client
            .update(&ddl.rename_int, None, &[])
            .map_err(|e| format!("rename int: {}", e))?;
    }

    // ============ TARGET SWAP ============
    client
        .update(&ddl.create_swap_tgt, None, &[])
        .map_err(|e| format!("create swap tgt: {}", e))?;
    client
        .update(&ddl.fill_swap_tgt, None, &[])
        .map_err(|e| format!("fill swap tgt: {}", e))?;
    let _ = client.update(&format!("ANALYZE {}", ddl.swap_tgt_qual), None, &[]);
    if let Some(ref c) = ddl.check_tgt {
        client
            .update(c, None, &[])
            .map_err(|e| format!("add check tgt: {}", e))?;
    }
    client
        .update(&detach_old_tgt, None, &[])
        .map_err(|e| format!("detach old tgt: {}", e))?;

    // F3 heal: before attaching swap_tgt, check if a confirmed orphan with the same
    // bounds exists and drop it. A confirmed orphan is an existing target child whose
    // bounds match the incoming swap target's bounds but which maps to NO live source
    // partition (i.e. not in the expected target set).
    {
        // Query registry to get partition columns and source list
        let reg_row = client
            .select(
                "SELECT partition_columns, depends_on FROM public.__reflex_ivm_reference WHERE name = $1",
                Some(1),
                &[unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .ok()
            .and_then(|mut it| it.next());

        if let Some(row) = reg_row {
            let part_cols: Vec<String> = row
                .get_by_name::<Vec<String>, _>("partition_columns")
                .unwrap_or(None)
                .unwrap_or_default();
            let sources: Vec<String> = row
                .get_by_name::<Vec<String>, _>("depends_on")
                .unwrap_or(None)
                .unwrap_or_default();

            // Only attempt orphan drop if this is a partitioned IMV
            if !part_cols.is_empty() {
                // Resolve anchor source
                match resolve_anchor_source(client, &part_cols[0], &sources) {
                    Ok(anchor) => {
                        // Build expected target set from live source tree
                        let full_nodes = list_partition_tree(client, &anchor);
                        let src_expected_tgt: std::collections::HashSet<String> = full_nodes
                            .iter()
                            .filter(|n| n.sub_strategy.is_none()) // Only leaf sources
                            .map(|n| target_child_name(view_name, &n.bare_name))
                            .collect();

                        // F3 fail-safe: only attempt orphan drop if live-source set was successfully
                        // and non-emptily enumerated. If src_expected_tgt is empty, it means either:
                        // (a) list_partition_tree encountered a query error and returned an empty Vec, or
                        // (b) there are no leaf sources. Either way, we cannot trust the orphan determination.
                        // Skip the drop and emit a notice.
                        if src_expected_tgt.is_empty() {
                            pgrx::notice!(
                                "pg_reflex: orphan-overlap heal skipped for IMV '{}' — \
                                 live source partition set could not be determined (empty enumeration)",
                                view_name
                            );
                        } else {
                            // Live-source set was successfully enumerated and non-empty; safe to drop confirmed orphans.
                            // List target parent's children and find any with matching bounds
                            let tgt_children = list_partition_children(client, &tgt_parent);
                            for child in &tgt_children {
                                // Skip the expected target we're about to attach
                                if child.bare_name == tgt_child_bare {
                                    continue;
                                }
                                // Check if this child has the same bounds as swap target
                                let child_bound =
                                    read_partition_bound(client, schema, &child.bare_name);
                                if child_bound == tgt_bound && !child_bound.is_empty() {
                                    // Found a partition with same bounds — is it a confirmed orphan?
                                    if !src_expected_tgt.contains(&child.bare_name) {
                                        // Confirmed orphan: no live source backing it. Drop it before attach.
                                        let drop_orphan = format!(
                                            "DROP TABLE IF EXISTS \"{}\".\"{}\"",
                                            schema, child.bare_name
                                        );
                                        client
                                            .update(&drop_orphan, None, &[])
                                            .map_err(|e| format!("drop orphan: {}", e))?;
                                        pgrx::notice!(
                                            "pg_reflex: dropped confirmed orphan target partition '{}' \
                                             (bounds matched incoming swap target)",
                                            child.bare_name
                                        );
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // Anchor source could not be resolved; orphan check is skipped
                        pgrx::notice!(
                            "pg_reflex: orphan-overlap check skipped for IMV '{}' — anchor source resolution failed: {}",
                            view_name, e
                        );
                    }
                }
            }
        }
    }

    client
        .update(&attach_new_tgt, None, &[])
        .map_err(|e| format!("attach new tgt: {}", e))?;
    if let Some(ref c) = ddl.drop_check_tgt {
        let _ = client.update(c, None, &[]);
    }
    client
        .update(&ddl.drop_old_tgt, None, &[])
        .map_err(|e| format!("drop old tgt: {}", e))?;
    client
        .update(&ddl.rename_tgt, None, &[])
        .map_err(|e| format!("rename tgt: {}", e))?;

    Ok(())
}

/// SQL-quote a text literal — doubles embedded single quotes.
/// Expand a source partition (any level, schema-qualified or bare) to the set
/// of its source LEAF bare-names. A leaf (or non-partitioned relation) expands
/// to itself.
fn expand_source_partition_to_leaves(
    client: &pgrx::spi::SpiClient<'_>,
    source_partition: &str,
) -> Vec<String> {
    let (_, bare) = canonical_source(source_partition);
    let subtree = list_partition_tree(client, source_partition);
    if subtree.is_empty() {
        vec![bare]
    } else {
        subtree
            .into_iter()
            .filter(|n| n.sub_strategy.is_none())
            .map(|n| n.bare_name)
            .collect()
    }
}

/// Target-side LEAF bare-names under an IMV child `bare` (or `[bare]` if it is
/// already a leaf). Used to expand a matched top-level child (which on a
/// multi-level IMV is an internal node) to the leaves the swap operates on.
fn target_leaves_under(client: &pgrx::spi::SpiClient<'_>, schema: &str, bare: &str) -> Vec<String> {
    let qual = format!("{}.{}", schema, bare);
    let sub = list_partition_tree(client, &qual);
    if sub.is_empty() {
        vec![bare.to_string()]
    } else {
        sub.into_iter()
            .filter(|n| n.sub_strategy.is_none())
            .map(|n| n.bare_name)
            .collect()
    }
}

/// Resolve the CURRENT immediate parent of partition child `<schema>.<bare>`,
/// returned as a quoted `"schema"."parent"` reference, or None if it is not a
/// partition.
fn read_immediate_parent_qual(
    client: &pgrx::spi::SpiClient<'_>,
    schema: &str,
    child_bare: &str,
) -> Option<String> {
    client
        .select(
            "SELECT pn.nspname::text AS s, pc.relname::text AS r \
             FROM pg_inherits i \
             JOIN pg_class child ON child.oid = i.inhrelid \
             JOIN pg_namespace cn ON cn.oid = child.relnamespace \
             JOIN pg_class pc ON pc.oid = i.inhparent \
             JOIN pg_namespace pn ON pn.oid = pc.relnamespace \
             WHERE cn.nspname = $1 AND child.relname = $2",
            Some(1),
            &[
                unsafe {
                    DatumWithOid::new(schema.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                },
                unsafe {
                    DatumWithOid::new(child_bare.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                },
            ],
        )
        .ok()
        .and_then(|mut it| it.next())
        .and_then(|r| {
            let s = r.get_by_name::<&str, _>("s").ok().flatten()?.to_string();
            let p = r.get_by_name::<&str, _>("r").ok().flatten()?.to_string();
            Some(format!("\"{}\".\"{}\"", s, p))
        })
}

fn sql_literal_text(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Return `pg_get_expr(c.relpartbound, c.oid)` for a child, e.g.
/// `FOR VALUES IN ('N', 'S')`.  Returns the empty string when the child is
/// not a partition or the lookup fails.
fn read_partition_bound(client: &pgrx::spi::SpiClient<'_>, schema: &str, bare: &str) -> String {
    client
        .select(
            "SELECT pg_get_expr(c.relpartbound, c.oid) AS bound \
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2",
            Some(1),
            &[
                unsafe {
                    DatumWithOid::new(schema.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                },
                unsafe {
                    DatumWithOid::new(bare.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                },
            ],
        )
        .ok()
        .and_then(|mut it| it.next())
        .and_then(|r| {
            r.get_by_name::<&str, _>("bound")
                .ok()
                .flatten()
                .map(|s| s.to_string())
        })
        .unwrap_or_default()
}

/// True when `<schema>.<bare>` exists and is a PARTITIONED relation
/// (`relkind = 'p'`). Absent or plain relations answer false, so callers that
/// use this as a refusal guard fail toward "proceed" only for shapes the swap
/// can actually rebuild.
fn is_partitioned_relation(client: &pgrx::spi::SpiClient<'_>, schema: &str, bare: &str) -> bool {
    client
        .select(
            "SELECT (c.relkind = 'p') AS is_part FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2",
            Some(1),
            &[
                unsafe {
                    DatumWithOid::new(schema.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                },
                unsafe {
                    DatumWithOid::new(bare.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                },
            ],
        )
        .ok()
        .and_then(|mut it| it.next())
        .and_then(|r| r.get_by_name::<bool, _>("is_part").ok().flatten())
        .unwrap_or(false)
}

/// Return `pg_get_partition_constraintdef(child_oid)` — the boolean expression
/// describing the partition bound (e.g. `(region IS NOT NULL) AND (region = 'N')`).
/// Returns the empty string on lookup failure.
fn read_partition_constraint_def(
    client: &pgrx::spi::SpiClient<'_>,
    schema: &str,
    bare: &str,
) -> String {
    let qname = format!("{}.{}", schema, bare);
    client
        .select(
            "SELECT pg_get_partition_constraintdef(to_regclass($1)::oid) AS def",
            Some(1),
            &[unsafe { DatumWithOid::new(qname, PgBuiltInOids::TEXTOID.oid().value()) }],
        )
        .ok()
        .and_then(|mut it| it.next())
        .and_then(|r| {
            r.get_by_name::<&str, _>("def")
                .ok()
                .flatten()
                .map(|s| s.to_string())
        })
        .unwrap_or_default()
}

/// Drop any `__reflex_swap_*_<bare_view>_*` orphan TABLES left behind by a
/// prior failed swap.  Filters `relkind = 'r'` so leftover indexes
/// (which keep their pre-rename names when their owning swap table is
/// renamed to the canonical child name) are not touched — they're
/// functional and dropping them would break the canonical child's
/// UNIQUE index attachment.
fn cleanup_orphan_swap_tables(view_name: &str) {
    let (schema_opt, bare_view) = split_qualified_name(view_name);
    let schema = schema_opt.unwrap_or("public").to_string();
    let prefix_int = format!("__reflex_swap_int_{}_", bare_view);
    let prefix_tgt = format!("__reflex_swap_tgt_{}_", bare_view);

    let _ = Spi::connect_mut(|client| -> Result<(), ()> {
        let q = "SELECT c.relname::text AS r \
                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 \
                   AND c.relkind = 'r' \
                   AND (c.relname LIKE $2 || '%' OR c.relname LIKE $3 || '%')";
        let mut targets: Vec<String> = Vec::new();
        if let Ok(rows) = client.select(
            q,
            None,
            &[
                unsafe { DatumWithOid::new(schema.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe {
                    DatumWithOid::new(prefix_int.clone(), PgBuiltInOids::TEXTOID.oid().value())
                },
                unsafe {
                    DatumWithOid::new(prefix_tgt.clone(), PgBuiltInOids::TEXTOID.oid().value())
                },
            ],
        ) {
            for row in rows {
                if let Ok(Some(name)) = row.get_by_name::<&str, _>("r") {
                    targets.push(name.to_string());
                }
            }
        }
        for name in targets {
            let drop_sql = format!("DROP TABLE IF EXISTS \"{}\".\"{}\" CASCADE", schema, name);
            let _ = client.update(&drop_sql, None, &[]);
        }
        Ok(())
    });
}

/// Current (child_name, oid) LEAF set of `source_root`, for snapshot diffing.
pub(crate) fn current_source_leaf_oids(
    client: &pgrx::spi::SpiClient<'_>,
    source_root: &str,
) -> Vec<(String, u32)> {
    list_partition_tree(client, source_root)
        .into_iter()
        .filter(|n| n.sub_strategy.is_none())
        .map(|n| (n.bare_name, n.oid))
        .collect()
}

/// Read snapshot (child_name, child_oid) pairs for a source root.
pub(crate) fn read_snapshot_pairs(
    client: &pgrx::spi::SpiClient<'_>,
    source_root: &str,
) -> Vec<(String, u32)> {
    client
        .select(
            "SELECT child_name, child_oid FROM public.__reflex_source_partition_snapshot WHERE source_root = $1",
            None,
            &[unsafe { DatumWithOid::new(source_root.to_string(), PgBuiltInOids::TEXTOID.oid().value()) }],
        )
        .ok()
        .map(|rows| {
            rows.filter_map(|r| {
                let n = r.get_by_name::<&str, _>("child_name").ok().flatten()?.to_string();
                let o = r.get_by_name::<i64, _>("child_oid").ok().flatten()? as u32;
                Some((n, o))
            })
            .collect()
        })
        .unwrap_or_default()
}

/// Canonical, schema-qualified `"schema.relname"` key for a source root, so the
/// snapshot is keyed identically whether the caller passes a bare name (the
/// create-time anchor, e.g. `fz`) or the `pg_partition_root` form the event
/// trigger enqueues (e.g. `public.fz`). Falls back to the input on lookup
/// failure.
pub(crate) fn canonical_root_key(client: &pgrx::spi::SpiClient<'_>, name: &str) -> String {
    client
        .select(
            "SELECT n.nspname || '.' || c.relname AS k \
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.oid = to_regclass($1)",
            Some(1),
            &[unsafe { DatumWithOid::new(name.to_string(), pgrx::pg_sys::TEXTOID) }],
        )
        .ok()
        .and_then(|mut it| it.next())
        .and_then(|r| {
            r.get_by_name::<&str, _>("k")
                .ok()
                .flatten()
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| name.to_string())
}

/// Replace the snapshot rows for `source_root` with the current leaf set.
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
        // Capture the leaf's `FOR VALUES …` bound. The detach-then-drop flush
        // path proves an irrelevant dropped partition was a no-op by probing the
        // IMV filter against this bound (the child itself is gone by flush time).
        let _ = client.update(
            "INSERT INTO public.__reflex_source_partition_snapshot \
                 (source_root, child_name, child_oid, bound, ancestors) \
             SELECT $1, $2, $3, pg_get_expr(c.relpartbound, c.oid), $4::TEXT[] \
             FROM pg_class c WHERE c.oid = $3::oid \
             ON CONFLICT (source_root, child_name) \
                 DO UPDATE SET child_oid = EXCLUDED.child_oid, \
                              bound = EXCLUDED.bound, \
                              ancestors = EXCLUDED.ancestors",
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

/// Resolve a relation `oid` to a quoted, schema-qualified name (`"schema"."rel"`),
/// or None if the relation no longer exists (e.g. a DETACHed-then-DROPped child).
fn oid_to_qualified_name(client: &pgrx::spi::SpiClient<'_>, oid: u32) -> Option<String> {
    client
        .select(
            &format!(
                "SELECT format('%I.%I', n.nspname, c.relname) AS q \
                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.oid = {}",
                oid
            ),
            Some(1),
            &[],
        )
        .ok()
        .and_then(|mut it| it.next())
        .and_then(|r| r.get_by_name::<&str, _>("q").ok().flatten())
        .map(|s| s.to_string())
}

/// Cheap emptiness probe: does the relation named by `qualified` (a quoted,
/// regclass-resolvable name from `oid_to_qualified_name`) hold at least one row?
/// On probe failure we assume non-empty so the caller still does the swap — the
/// skip is an optimization and must never trade correctness for speed.
fn relation_has_rows(client: &pgrx::spi::SpiClient<'_>, qualified: &str) -> bool {
    client
        .select(
            &format!("SELECT EXISTS(SELECT 1 FROM {}) AS has_rows", qualified),
            Some(1),
            &[],
        )
        .ok()
        .and_then(|mut it| it.next())
        .and_then(|r| r.get_by_name::<bool, _>("has_rows").ok().flatten())
        .unwrap_or(true)
}

/// Extract the inner value list of a single-key LIST partition bound, i.e. the
/// `…` of `FOR VALUES IN (…)`. Returns None for RANGE/HASH/DEFAULT bounds (whose
/// text does not match the prefix) — those callers fall back to reconcile. The
/// prefix is fixed and the closing `)` is always the last character, so stripping
/// is robust regardless of the value literals' content (no comma-splitting).
fn list_bound_inner(bound: &str) -> Option<String> {
    bound
        .trim()
        .strip_prefix("FOR VALUES IN (")
        .and_then(|s| s.strip_suffix(')'))
        .map(|s| s.to_string())
}

/// Pick the `depends_on` entry that names `root` (qualified or bare). This is the
/// exact source string the IMV's INSERT/DELETE triggers were built with, so the
/// synthesized transition table and `reflex_build_delta_sql`'s source-rewrite stay
/// mutually consistent. Falls back to `root` when no entry matches.
fn source_matching_root(deps: &[String], root: &str) -> String {
    let bare_root = split_qualified_name(root).1;
    deps.iter()
        .find(|d| d.as_str() == root || split_qualified_name(d).1 == bare_root)
        .cloned()
        .unwrap_or_else(|| root.to_string())
}

/// Flush pending partition changes. When `only` is Some, flush just that
/// source root; otherwise drain `__reflex_partition_pending`. For each dirty
/// root, oid-diff the live leaf set against the snapshot and apply: AttachNew /
/// SwapFill -> partition-scoped reconcile of the matching IMV leaf (creates it
/// via sync if new, then swap-fills); Drop -> DROP the orphaned IMV leaf.
/// Refreshes the snapshot and clears the pending row(s) afterward.
/// A pending root that has exceeded the failure cap: (source_root, failures, last_error).
type CappedRoot = (String, i32, Option<String>);

/// Set the consecutive-failure counter on pending roots. `None` targets every
/// root carrying failures; `Some(root)` targets just that one. Returns the number
/// of rows changed.
///
/// `last_error` is deliberately left in place: it is the only record of why the
/// root broke, the next attempt overwrites it, and a successful drain deletes the
/// row outright.
fn set_partition_failures(source_root: Option<&str>, value: i32) -> i64 {
    Spi::connect_mut(|client| {
        let updated = match source_root {
            Some(root) => client.update(
                &format!(
                    "UPDATE public.__reflex_partition_pending SET failures = {value} \
                      WHERE source_root = $1 AND failures <> {value}"
                ),
                None,
                &[unsafe {
                    DatumWithOid::new(root.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            ),
            None => client.update(
                &format!(
                    "UPDATE public.__reflex_partition_pending SET failures = {value} \
                      WHERE failures <> {value}"
                ),
                None,
                &[],
            ),
        };
        match updated {
            Ok(table) => table.len() as i64,
            Err(e) => {
                pgrx::warning!("pg_reflex: setting partition failure count failed: {}", e);
                0
            }
        }
    })
}

/// Re-arm pending roots by zeroing their consecutive-failure counter — the
/// operator-facing `reflex_reset_partition_failures` contract.
///
/// `PARTITION_FLUSH_FAILURE_CAP` makes both flush entry points decline a root that
/// has failed that many times in a row, so a capped root is skipped by
/// `reflex_flush_partitions()` *and* by `reflex_flush_partition_source(root)`.
/// Before this primitive existed the only way out was a manual UPDATE against an
/// extension-owned table — which the skip warning asked for without providing.
///
/// An explicit call is a FULL reset: the operator is asserting the cause is fixed,
/// so the root gets its whole retry budget back. `reflex_doctor` deliberately does
/// not use this — see `rearm_capped_partition_root`.
pub(crate) fn reflex_reset_partition_failures_impl(source_root: Option<&str>) -> i64 {
    set_partition_failures(source_root, 0)
}

/// Grant a capped root exactly ONE more flush attempt, for `reflex_doctor`.
///
/// Setting the counter to `CAP - 1` rather than 0 is what makes "one attempt"
/// true: the flush that follows runs (it is below the cap), and if it fails the
/// drain's own `failures + 1` puts the root straight back at the cap. Zeroing
/// instead would hand the *commit-time* drain a full fresh budget, so a cron
/// running `reflex_doctor(fix => TRUE)` would cycle a poison root
/// `CAP -> 0 -> 1 -> … -> CAP` forever and it would never be permanently skipped —
/// the one guarantee the cap exists to provide. It would also drop the row below
/// the doctor's own reporting gates, hiding a root the doctor just failed to
/// repair from the operator's next run.
pub(crate) fn rearm_capped_partition_root(source_root: &str) -> i64 {
    set_partition_failures(Some(source_root), PARTITION_FLUSH_FAILURE_CAP - 1)
}

pub(crate) fn reflex_flush_partitions_impl(only: Option<&str>) -> String {
    let outcome: Result<String, String> = Spi::connect_mut(|client| {
        let (roots, capped_roots): (Vec<String>, Vec<CappedRoot>) = match only {
            // The deferred commit-time trigger reaches the flush through this
            // single-root path, so the failure cap must be honoured here too — a
            // poison root would otherwise be retried on every triggering commit,
            // never permanently skipped and never warned about. A root with no
            // pending row (e.g. a direct manual call) has no failure history and
            // is processed normally.
            Some(r) => {
                let root = canonical_root_key(client, r);
                let failures: i32 = client
                    .select(
                        "SELECT failures FROM public.__reflex_partition_pending WHERE source_root = $1",
                        Some(1),
                        &[unsafe {
                            DatumWithOid::new(root.clone(), PgBuiltInOids::TEXTOID.oid().value())
                        }],
                    )
                    .ok()
                    .and_then(|mut it| it.next())
                    .and_then(|row| row.get_by_name::<i32, _>("failures").ok().flatten())
                    .unwrap_or(0);
                if failures >= PARTITION_FLUSH_FAILURE_CAP {
                    let last_error: Option<String> = client
                        .select(
                            "SELECT last_error FROM public.__reflex_partition_pending WHERE source_root = $1",
                            Some(1),
                            &[unsafe {
                                DatumWithOid::new(root.clone(), PgBuiltInOids::TEXTOID.oid().value())
                            }],
                        )
                        .ok()
                        .and_then(|mut it| it.next())
                        .and_then(|row| row.get_by_name::<&str, _>("last_error").ok().flatten())
                        .map(|s| s.to_string());
                    (Vec::new(), vec![(root, failures, last_error)])
                } else {
                    (vec![root], Vec::new())
                }
            }
            None => {
                let pending: Vec<CappedRoot> = client
                    .select(
                        "SELECT source_root, failures, last_error FROM public.__reflex_partition_pending",
                        None,
                        &[],
                    )
                    .map_err(|e| format!("flush: pending scan failed: {}", e))?
                    .filter_map(|row| {
                        let root = row.get_by_name::<&str, _>("source_root")
                            .ok()
                            .flatten()?
                            .to_string();
                        let failures = row.get_by_name::<i32, _>("failures")
                            .ok()
                            .flatten()
                            .unwrap_or(0);
                        let last_error = row.get_by_name::<&str, _>("last_error")
                            .ok()
                            .flatten()
                            .map(|s| s.to_string());
                        Some((root, failures, last_error))
                    })
                    .collect();

                let (active, capped): (Vec<_>, Vec<_>) = pending
                    .into_iter()
                    .partition(|(_, failures, _)| *failures < PARTITION_FLUSH_FAILURE_CAP);

                let active_roots: Vec<String> = active.into_iter().map(|(r, _, _)| r).collect();
                (active_roots, capped)
            }
        };

        // Emit warnings for capped roots so operators know why they're not being retried
        for (root, failures, last_error) in &capped_roots {
            pgrx::warning!(
                "pg_reflex: partition flush for root {} skipped — {} consecutive failures (last error: {}); \
                 run reflex_doctor and reset failures to retry",
                root,
                failures,
                last_error.as_deref().unwrap_or("unknown")
            );
        }

        let mut summary: Vec<String> = Vec::new();
        for root in &roots {
            // Canonical key so the snapshot read matches the key written at
            // create-time seed / prior flush, regardless of bare vs qualified.
            let root_key = canonical_root_key(client, root);
            // Partitioned IMVs depending on this source root.
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
                    let deps = r
                        .get_by_name::<Vec<String>, _>("depends_on")
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                    Some((name, depth, deps))
                })
                .collect();

            // Snapshot (child_name, oid) for this root.
            let snapshot: Vec<(String, u32)> = client
                .select(
                    "SELECT child_name, child_oid FROM public.__reflex_source_partition_snapshot WHERE source_root = $1",
                    None,
                    &[unsafe { DatumWithOid::new(root_key.clone(), pgrx::pg_sys::TEXTOID) }],
                )
                .map_err(|e| format!("flush: snapshot read failed: {}", e))?
                .filter_map(|r| {
                    let n = r.get_by_name::<&str, _>("child_name").ok().flatten()?.to_string();
                    let o = r.get_by_name::<i64, _>("child_oid").ok().flatten()? as u32;
                    Some((n, o))
                })
                .collect();

            // Snapshot ancestors (per child_name, for up-mapping drops in shallow IMVs).
            let snapshot_ancestors: std::collections::HashMap<String, Vec<String>> = client
                .select(
                    "SELECT child_name, COALESCE(ancestors, ARRAY[]::TEXT[]) AS ancestors \
                     FROM public.__reflex_source_partition_snapshot WHERE source_root = $1",
                    None,
                    &[unsafe { DatumWithOid::new(root_key.clone(), pgrx::pg_sys::TEXTOID) }],
                )
                .map_err(|e| format!("flush: snapshot ancestors read failed: {}", e))?
                .filter_map(|r| {
                    let n = r
                        .get_by_name::<&str, _>("child_name")
                        .ok()
                        .flatten()?
                        .to_string();
                    let a = r
                        .get_by_name::<Vec<String>, _>("ancestors")
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                    Some((n, a))
                })
                .collect();

            // Captured `FOR VALUES …` bound per snapshot leaf, keyed by child_name.
            // Used to prove a detached-then-dropped partition was irrelevant to an
            // unpartitioned IMV's filter once the child itself is gone.
            let snapshot_bounds: std::collections::HashMap<String, Option<String>> = client
                .select(
                    "SELECT child_name, bound FROM public.__reflex_source_partition_snapshot \
                     WHERE source_root = $1",
                    None,
                    &[unsafe { DatumWithOid::new(root_key.clone(), pgrx::pg_sys::TEXTOID) }],
                )
                .map_err(|e| format!("flush: snapshot bounds read failed: {}", e))?
                .filter_map(|r| {
                    let n = r
                        .get_by_name::<&str, _>("child_name")
                        .ok()
                        .flatten()?
                        .to_string();
                    let b = r
                        .get_by_name::<&str, _>("bound")
                        .ok()
                        .flatten()
                        .map(|s| s.to_string());
                    Some((n, b))
                })
                .collect();

            // Single partition-key column (lowercased) for the bound probe; only a
            // single-key LIST source is eligible (multi-key/RANGE → reconcile).
            let part_key_col: Option<String> = introspect_partition_descriptor(client, root)
                .filter(|d| d.column_names.len() == 1)
                .map(|d| d.column_names[0].clone());

            let current = current_source_leaf_oids(client, root);
            let actions = classify_partition_diff(&snapshot, &current);
            let live_tree = list_partition_tree(client, root);

            // Collect all mutating statements for this root to run in one atomically-isolated
            // subtransaction. This ensures one failing root doesn't abort the whole flush or block
            // other roots from draining.
            let mut root_stmts: Vec<String> = Vec::new();

            for (imv, depth_opt, _deps) in &imvs {
                let mirror_depth = depth_opt
                    .map(|d| d as usize)
                    .unwrap_or_else(|| max_tree_depth(&live_tree));

                use std::collections::{BTreeMap, BTreeSet};
                // node -> must actually fill it. A brand-new (`AttachNew`) leaf
                // whose source is EMPTY is a provable empty->empty no-op: the
                // up-front sync below creates its mirror partition empty and
                // there is no prior target data to clear, so its per-leaf swap
                // is pure waste (the dominant cost when a demand-plan partition
                // attaches dozens of empty monthly leaves). A `SwapFill`
                // (oid changed) or a surviving-ancestor refill may need to clear
                // stale target rows, so those always fill regardless of source
                // emptiness.
                let mut fill_node: BTreeMap<String, bool> = BTreeMap::new();
                let mut to_drop: BTreeSet<String> = BTreeSet::new();

                let live_names: std::collections::HashSet<&str> =
                    live_tree.iter().map(|n| n.bare_name.as_str()).collect();
                let cur_oid: std::collections::HashMap<&str, u32> =
                    current.iter().map(|(n, o)| (n.as_str(), *o)).collect();

                for (leaf, action) in &actions {
                    match action {
                        PartitionDiffAction::AttachNew => {
                            let chain = leaf_ancestor_chain(&live_tree, leaf);
                            let node = ancestor_bare_at_depth(&chain, leaf, mirror_depth)
                                .unwrap_or_else(|| leaf.clone());
                            let source_nonempty = cur_oid
                                .get(leaf.as_str())
                                .and_then(|o| oid_to_qualified_name(client, *o))
                                .map(|q| relation_has_rows(client, &q))
                                .unwrap_or(true);
                            *fill_node.entry(node).or_insert(false) |= source_nonempty;
                        }
                        PartitionDiffAction::SwapFill => {
                            let chain = leaf_ancestor_chain(&live_tree, leaf);
                            let node = ancestor_bare_at_depth(&chain, leaf, mirror_depth)
                                .unwrap_or_else(|| leaf.clone());
                            *fill_node.entry(node).or_insert(false) = true;
                        }
                        PartitionDiffAction::Drop => {
                            let chain = snapshot_ancestors.get(leaf).cloned().unwrap_or_default();
                            let node = ancestor_bare_at_depth(&chain, leaf, mirror_depth)
                                .unwrap_or_else(|| leaf.clone());
                            if live_names.contains(node.as_str()) {
                                // Sibling removed, ancestor survives -> refill it.
                                *fill_node.entry(node).or_insert(false) = true;
                            } else {
                                // Whole mirror-depth node gone -> drop IMV node.
                                to_drop.insert(node);
                            }
                        }
                    }
                }
                // A node both dropped and refilled: drop wins (it is gone).
                for d in &to_drop {
                    fill_node.remove(d);
                }

                let (schema_opt, _) = split_qualified_name(imv);
                let schema = schema_opt.unwrap_or("public");

                // Sync the IMV's partition tree against the source ONCE per flush
                // here, then drive the per-leaf reconciles with skip_sync => true
                // so they don't each re-walk the whole tree. This also creates the
                // (empty) mirror partitions for the skipped empty leaves above.
                if !fill_node.is_empty() || !to_drop.is_empty() {
                    root_stmts.push(format!(
                        "PERFORM public.reflex_sync_partitions({}, true)",
                        sql_literal_text(imv)
                    ));
                }
                for node in &to_drop {
                    let tgt = target_child_name(imv, node);
                    let int = intermediate_child_name(imv, node);
                    root_stmts.push(format!(
                        "DROP TABLE IF EXISTS \"{}\".\"{}\" CASCADE",
                        schema, tgt
                    ));
                    root_stmts.push(format!(
                        "DROP TABLE IF EXISTS \"{}\".\"{}\" CASCADE",
                        schema, int
                    ));
                }
                // Reconcile ALL changed leaves of this IMV in one call: the
                // dependent cascade then fires ONCE over the union of affected
                // keys, instead of once per leaf (a 12-month single-plan push
                // would otherwise rebuild that plan's aggregate slice 12×).
                let fill_nodes: Vec<&str> = fill_node
                    .iter()
                    .filter(|(_, fill)| **fill)
                    .map(|(node, _)| node.as_str())
                    .collect();
                if !fill_nodes.is_empty() {
                    root_stmts.push(format!(
                        "PERFORM public.reflex_reconcile_partition({}, '', {}, true)",
                        sql_literal_text(imv),
                        sql_literal_text(&fill_nodes.join(","))
                    ));
                }
                summary.push(format!("{}: {} change(s)", imv, actions.len()));
            }

            // Unpartitioned IMVs depending on this root can't capture a swap via
            // per-partition reconcile. Instead of a blunt full reconcile on every
            // partition change (which TRUNCATE+rebuilds the IMV and full-cascades
            // to every dependent), apply each attached/detached child as the bulk
            // INSERT/DELETE it semantically is (plans/2026-06-11) — the same
            // pipeline the INSERT/DELETE triggers use, so propagation is
            // write-driven and dies wherever the delta nets to zero. Falls back to
            // full reconcile when a baseline is missing or a child can't be
            // resolved (always correct).
            let unpartitioned_imvs: Vec<(String, Vec<String>)> = client
                .select(
                    "SELECT name, depends_on FROM public.__reflex_ivm_reference \
                     WHERE enabled = TRUE \
                       AND COALESCE(array_length(partition_columns, 1), 0) = 0 \
                       AND (depends_on @> ARRAY[$1] OR depends_on @> ARRAY[split_part($1,'.',2)])",
                    None,
                    &[unsafe { DatumWithOid::new(root.to_string(), pgrx::pg_sys::TEXTOID) }],
                )
                .map_err(|e| format!("flush: unpartitioned imv lookup failed: {}", e))?
                .filter_map(|r| {
                    let name = r.get_by_name::<&str, _>("name").ok().flatten()?.to_string();
                    let deps = r
                        .get_by_name::<Vec<String>, _>("depends_on")
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                    Some((name, deps))
                })
                .collect();

            for (imv, deps) in &unpartitioned_imvs {
                let source = source_matching_root(deps, root);
                let trans_new = crate::query_decomposer::transition_new_table_name(&source);
                let trans_old = crate::query_decomposer::transition_old_table_name(&source);

                // A missing baseline (IMV predating snapshot seeding) or a SwapFill
                // (needs DELETE-old + INSERT-new, old child often already gone) is
                // handled by a single correct full reconcile.
                let mut force_reconcile = snapshot.is_empty()
                    || actions
                        .iter()
                        .any(|(_, a)| matches!(a, PartitionDiffAction::SwapFill));

                let mut delta_stmts: Vec<String> = Vec::new();
                if !force_reconcile {
                    for (leaf, action) in &actions {
                        let (op, trans, oid) = match action {
                            PartitionDiffAction::AttachNew => (
                                "INSERT",
                                &trans_new,
                                current.iter().find(|(n, _)| n == leaf).map(|(_, o)| *o),
                            ),
                            PartitionDiffAction::Drop => (
                                "DELETE",
                                &trans_old,
                                snapshot.iter().find(|(n, _)| n == leaf).map(|(_, o)| *o),
                            ),
                            PartitionDiffAction::SwapFill => {
                                force_reconcile = true;
                                break;
                            }
                        };
                        match oid.and_then(|o| oid_to_qualified_name(client, o)) {
                            Some(child) => delta_stmts.push(format!(
                                "PERFORM public.reflex_apply_partition_delta({}, {}, '{}', {}, {})",
                                sql_literal_text(imv),
                                sql_literal_text(&source),
                                op,
                                sql_literal_text(&child),
                                sql_literal_text(trans),
                            )),
                            // Child gone (detached then dropped before this flush).
                            // For a DROP we can still prove the partition was
                            // irrelevant — and thus a guaranteed no-op — by probing
                            // the IMV filter against the captured LIST bound. The
                            // SQL helper reconciles for any inconclusive case, so
                            // an unprovable drop stays correct.
                            None => {
                                let bound_inner = matches!(action, PartitionDiffAction::Drop)
                                    .then(|| snapshot_bounds.get(leaf))
                                    .flatten()
                                    .and_then(|b| b.as_deref())
                                    .and_then(list_bound_inner);
                                match (&part_key_col, bound_inner) {
                                    (Some(keycol), Some(inner)) => delta_stmts.push(format!(
                                        "PERFORM public.reflex_partition_drop_maybe_skip({}, {}, {})",
                                        sql_literal_text(imv),
                                        sql_literal_text(keycol),
                                        sql_literal_text(&inner),
                                    )),
                                    _ => {
                                        force_reconcile = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                if force_reconcile {
                    root_stmts.push(format!(
                        "PERFORM public.reflex_reconcile({})",
                        sql_literal_text(imv)
                    ));
                    summary.push(format!("{}: full reconcile (unpartitioned)", imv));
                } else if !delta_stmts.is_empty() {
                    root_stmts.extend(delta_stmts);
                    summary.push(format!(
                        "{}: incremental partition delta ({} change(s))",
                        imv,
                        actions.len()
                    ));
                }
            }

            // Append the snapshot refresh and pending drain to the same root's statement list
            // so they commit/rollback atomically with the reconciles.
            root_stmts.push(format!(
                "PERFORM public.__reflex_refresh_partition_snapshot({})",
                sql_literal_text(root)
            ));
            root_stmts.push(format!(
                "DELETE FROM public.__reflex_partition_pending WHERE source_root = {}",
                sql_literal_text(root)
            ));

            // Emit the per-root DO block. The EXCEPTION branch leaves the snapshot + pending row
            // intact (rolled back) so the root retries on the next flush, and logs a WARNING
            // instead of aborting the batch. The pending drain is a scoped DELETE (RowExclusive) per root,
            // never a blanket TRUNCATE: two concurrent flushes each holding RowExclusive on the
            // globally-shared __reflex_partition_pending table (from event trigger INSERTs) would
            // both try to upgrade to AccessExclusive and deadlock. Also, TRUNCATE would silently
            // wipe pending rows a concurrent backend enqueued after `roots` was scanned but before
            // this point, losing that flush. The per-root DELETE is correct for both entry points.
            let root_esc = root.replace('\'', "''");
            let body = root_stmts
                .into_iter()
                .map(|s| format!("{};", s))
                .collect::<Vec<_>>()
                .join("\n");
            let do_block = format!(
                "DO $_reflex_part_sp$ \
                 BEGIN \
                   \n{body}\n \
                 EXCEPTION WHEN OTHERS THEN \
                   UPDATE public.__reflex_partition_pending \
                      SET last_error = left(SQLERRM, 2000), \
                          failures   = failures + 1 \
                    WHERE source_root = '{root_esc}'; \
                   UPDATE public.__reflex_ivm_reference \
                      SET known_stale = TRUE, stale_reason = left(SQLERRM, 2000), stale_since = now() \
                    WHERE depends_on @> ARRAY['{root_esc}'] OR depends_on @> ARRAY[split_part('{root_esc}', '.', 2)]; \
                   RAISE WARNING 'pg_reflex: partition flush for root % failed: % (SQLSTATE %) — left pending for retry', \
                     '{root_esc}', SQLERRM, SQLSTATE; \
                 END \
                 $_reflex_part_sp$"
            );
            // Stamped OUTSIDE the DO block: the block's EXCEPTION branch rolls its
            // own body back, and a failed attempt is exactly the one that must stay
            // dated. A successful drain deletes the row, so any row still visible to
            // an operator carries the timestamp of the attempt that failed. Without
            // this, neither `enqueued_at` (reset on every re-enqueue) nor `attempts`
            // (an enqueue counter) could date a drain failure.
            client
                .update(
                    "UPDATE public.__reflex_partition_pending \
                        SET last_attempt_at = statement_timestamp() WHERE source_root = $1",
                    None,
                    &[unsafe {
                        DatumWithOid::new(root.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    }],
                )
                .map_err(|e| {
                    format!("flush: stamping last_attempt_at for {} failed: {}", root, e)
                })?;
            client
                .update(&do_block, None, &[])
                .map_err(|e| format!("flush: DO block dispatch for root {} failed: {}", root, e))?;
        }

        Ok(if summary.is_empty() {
            "OK — nothing pending".to_string()
        } else {
            summary.join("; ")
        })
    });
    match outcome {
        Ok(s) => s,
        Err(e) => format!("ERROR: {}", e),
    }
}

/// One default partition emptied into a holding table during a tree drain.
pub(crate) struct DrainEntry {
    pub parent_qual: String,
    pub holding_qual: String,
    /// The drained default's own depth in the tree (not its parent's). Refill
    /// always inserts at `parent_qual` (the tree root) regardless of this
    /// value — tuple routing carries each row to the right depth — so it is
    /// diagnostic only.
    pub default_level: i32,
}

/// Phase 1 of the multi-level default drain: for every DEFAULT partition in each
/// root's tree that holds rows, move those rows into a fresh holding table and
/// leave the default attached but empty. Emptied defaults let the subsequent
/// `CREATE ... PARTITION OF` calls proceed without SQLSTATE 23514.
pub(crate) fn drain_tree_defaults(
    client: &mut pgrx::spi::SpiClient<'_>,
    roots: &[String],
) -> Result<Vec<DrainEntry>, String> {
    let mut entries = Vec::new();
    for root in roots {
        let defaults: Vec<(String, i32)> = client
            .select(
                "SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS q, t.level AS lvl \
                   FROM pg_partition_tree($1) t \
                   JOIN pg_class c ON c.oid = t.relid \
                   JOIN pg_namespace n ON n.oid = c.relnamespace \
                  WHERE pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'",
                None,
                &[unsafe { DatumWithOid::new(root.clone(), PgBuiltInOids::TEXTOID.oid().value()) }],
            )
            .map_err(|e| format!("drain: enumerate defaults of {}: {}", root, e))?
            .filter_map(|r| {
                let q = r.get_by_name::<&str, _>("q").ok().flatten()?.to_string();
                let lvl = r.get_by_name::<i32, _>("lvl").ok().flatten().unwrap_or(0);
                Some((q, lvl))
            })
            .collect();

        for (default_qual, default_level) in defaults {
            let (schema, bare) = split_qualified_name(&default_qual);
            let holding_bare =
                safe_identifier(&format!("__reflex_drain_{}", bare.trim_matches('"')));
            let holding_qual = match schema {
                Some(s) => format!("{}.{}", s, quote_identifier(&holding_bare)),
                None => quote_identifier(&holding_bare),
            };
            // Skip empty defaults: nothing to move, no holding table.
            let has_rows: bool = client
                .select(
                    &format!("SELECT EXISTS (SELECT 1 FROM {})", default_qual),
                    Some(1),
                    &[],
                )
                .ok()
                .and_then(|mut it| it.next())
                .and_then(|r| r.get_by_name::<bool, _>("exists").ok().flatten())
                .unwrap_or(false);
            if !has_rows {
                continue;
            }
            client
                .update(
                    &format!(
                        "CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS)",
                        holding_qual, default_qual
                    ),
                    None,
                    &[],
                )
                .map_err(|e| format!("drain: create holding {}: {}", holding_qual, e))?;
            client
                .update(
                    &format!(
                        "WITH d AS (DELETE FROM {} RETURNING *) INSERT INTO {} SELECT * FROM d",
                        default_qual, holding_qual
                    ),
                    None,
                    &[],
                )
                .map_err(|e| format!("drain: move rows from {}: {}", default_qual, e))?;
            entries.push(DrainEntry {
                parent_qual: root.clone(),
                holding_qual,
                default_level,
            });
        }
    }
    Ok(entries)
}

/// Phase 3: refill each emptied default's rows back into its tree root by
/// tuple routing, which carries each row to its now-existing leaf — or back
/// into a still-unmatched default — regardless of the drained default's
/// original depth. Sorted by `default_level` for deterministic, readable
/// ordering only; every entry inserts at the same root, so order does not
/// affect correctness. Drops each holding table.
pub(crate) fn refill_tree_defaults(
    client: &mut pgrx::spi::SpiClient<'_>,
    mut entries: Vec<DrainEntry>,
) -> Result<(), String> {
    entries.sort_by_key(|e| e.default_level);
    for e in &entries {
        client
            .update(
                &format!(
                    "INSERT INTO {} SELECT * FROM {}",
                    e.parent_qual, e.holding_qual
                ),
                None,
                &[],
            )
            .map_err(|err| format!("refill: reinsert into {}: {}", e.parent_qual, err))?;
        client
            .update(&format!("DROP TABLE {}", e.holding_qual), None, &[])
            .map_err(|err| format!("refill: drop holding {}: {}", e.holding_qual, err))?;
    }
    Ok(())
}

// Phase 1 (plans/1_6_1_refacto.md) — `substitute_identifier` is a re-export
// of the canonical implementation in
// [`crate::sql_writer::identifier::substitute_identifier_ci`]. Existing call
// sites and `unit_partition.rs` tests continue to compile against the
// original name.
use crate::sql_writer::identifier::substitute_identifier_ci as substitute_identifier;

#[cfg(test)]
#[path = "tests/unit_partition.rs"]
mod tests;
