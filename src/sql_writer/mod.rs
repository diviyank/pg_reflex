//! `sql_writer` — internal module that owns SQL emission for pg_reflex.
//!
//! SQL string construction is centralised here: registry INSERTs, DDL emitters,
//! and identifier rewriters live in one place rather than being duplicated across
//! callers.
//!
//! This module is intentionally non-generic and lives in-tree. It knows
//! about [`AggregationPlan`], `IntermediateColumn`, and
//! `__reflex_ivm_reference`. Promoting it to a crate is explicitly not a
//! goal.

pub mod ddl;
pub mod identifier;
pub(crate) mod keyword_scan;
pub mod registry;
pub mod slot;

// Re-export the public surface so external callers can import everything via
// `crate::sql_writer::...` without reaching into the sub-modules.  Direct
// uses inside the crate still go through the sub-modules.
pub use ddl::{CreateIndex, CreateTable};
pub use registry::{add_graph_child_links, insert_registry_row, AggregationsCast, RegistryRow};
pub use slot::slot_replace;

#[cfg(test)]
mod tests;
