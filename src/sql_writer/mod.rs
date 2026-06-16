//! `sql_writer` — internal module that owns SQL emission for pg_reflex.
//!
//! Pre-1.6.1, SQL string construction was spread across `create_ivm.rs`
//! (4× duplicate registry INSERTs), `schema_builder.rs` (six `format!()`-
//! based DDL emitters + a 400-line plpgsql trigger body), and three subtly
//! different identifier rewriters in `query_decomposer.rs`, `partition.rs`,
//! and `trigger.rs`. The 1.5.2 `mixed_case_identifier` bug class lived in
//! that rewriter family.
//!
//! This module is intentionally non-generic and lives in-tree. It knows
//! about [`AggregationPlan`], `IntermediateColumn`, and
//! `__reflex_ivm_reference`. Promoting it to a crate is explicitly not a
//! goal (see `plans/1_6_1_refacto.md`).

pub mod ddl;
pub mod identifier;
pub mod registry;
pub mod slot;

// Re-export the public surface so external callers can import everything via
// `crate::sql_writer::...` without reaching into the sub-modules.  Direct
// uses inside the crate still go through the sub-modules; the
// `#[allow(unused_imports)]` keeps the re-exports compiling when nothing
// outside this module references them.
#[allow(unused_imports)]
pub use ddl::{CreateIndex, CreateTable};
#[allow(unused_imports)]
pub use identifier::{
    format_pg_text_array, quote, replace_identifier, replace_source_with_transition, safe,
    split_qualified, strip_redundant_bare_alias, substitute_identifier_ci,
};
#[allow(unused_imports)]
pub use registry::{
    add_graph_child_links, insert_registry_row, read_imv, remove_graph_child,
    set_partition_dispatch_cost_cap, set_wipe_floor_rows, set_wipe_threshold, AggregationsCast,
    ImvRecord, RegistryRow,
};
#[allow(unused_imports)]
pub use slot::slot_replace;

#[cfg(test)]
mod tests;
