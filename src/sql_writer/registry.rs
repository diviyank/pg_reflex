//! Single source of truth for the `public.__reflex_ivm_reference` INSERT row
//! shape. Pre-1.6.1 the four create paths (set-op, DISTINCT ON, window, main)
//! each repeated a 14-column INSERT block with hand-laddered
//! `DatumWithOid::new(...)` calls. Adding a column meant editing four sites.
//!
//! [`RegistryRow`] is the typed row; [`insert_registry_row`] is the one
//! INSERT site. Optional columns default to safe empties so the four
//! historical INSERT shapes are preserved exactly: paths that pre-1.6.1 did
//! not set `where_predicate`, `ignored_sources`, `partition_columns`,
//! `partition_strategy` simply leave them at their defaults here and the
//! catalog row ends up identical.

use crate::sql_writer::identifier::format_pg_text_array;
use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi;

/// JSON-blob hint: the `aggregations` column was historically inserted as
/// `::json` for the decomposed paths and `::jsonb` for the main path. Both
/// shapes survive a round-trip, but byte-identical contract is preserved by
/// having callers select the cast they used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregationsCast {
    Json,
    Jsonb,
}

/// Typed row written to `public.__reflex_ivm_reference` by every IMV
/// creation path. Fields that historically defaulted to empty arrays /
/// empty strings carry sensible defaults so set-op / DISTINCT ON / window
/// paths can leave them at default and continue to produce the pre-1.6.1
/// catalog row byte-for-byte.
#[derive(Clone, Debug)]
pub struct RegistryRow<'a> {
    pub view_name: &'a str,
    pub graph_depth: i32,
    pub depends_on: &'a [String],
    pub depends_on_imv: &'a [String],
    pub unlogged_tables: &'a [String],
    pub graph_child: &'a [String],
    pub sql_query: &'a str,
    pub base_query: &'a str,
    pub end_query: &'a str,
    pub aggregations_json: &'a str,
    pub aggregations_cast: AggregationsCast,
    pub index_columns: &'a [String],
    pub unique_columns: &'a [String],
    pub storage_mode: &'a str,
    pub refresh_mode: &'a str,
    pub where_predicate: Option<&'a str>,
    pub ignored_sources: Option<&'a [String]>,
    pub partition_columns: Option<&'a [String]>,
    pub partition_strategy: Option<&'a str>,
    /// IMV partition mirror depth; `None` => NULL => full source depth.
    pub partition_depth: Option<i32>,
    /// TRUE for an ungrouped aggregate IMV (empty GROUP BY → at most one row).
    /// Written by the main create path; the decomposed paths leave it false.
    pub max_one_row: bool,
}

impl<'a> RegistryRow<'a> {
    /// Shape used by the decomposed paths (set-op, DISTINCT ON, window).
    /// Aggregations cast is `::json`; partitioning / ignored_sources /
    /// where_predicate columns stay NULL/default.
    #[allow(clippy::too_many_arguments)]
    pub fn decomposed(
        view_name: &'a str,
        graph_depth: i32,
        depends_on: &'a [String],
        depends_on_imv: &'a [String],
        sql_query: &'a str,
        base_query: &'a str,
        storage_mode: &'a str,
        refresh_mode: &'a str,
    ) -> Self {
        RegistryRow {
            view_name,
            graph_depth,
            depends_on,
            depends_on_imv,
            unlogged_tables: &[],
            graph_child: &[],
            sql_query,
            base_query,
            end_query: "",
            aggregations_json: "{}",
            aggregations_cast: AggregationsCast::Json,
            index_columns: &[],
            unique_columns: &[],
            storage_mode,
            refresh_mode,
            where_predicate: None,
            ignored_sources: None,
            partition_columns: None,
            partition_strategy: None,
            partition_depth: None,
            max_one_row: false,
        }
    }
}

/// Insert one row into `public.__reflex_ivm_reference`.
///
/// Uses two distinct INSERT statements: a short one for the decomposed
/// paths (matches the pre-1.6.1 16-column shape with `::json` aggregations)
/// and the full one for the main path (20 columns, `::jsonb` aggregations,
/// where_predicate / partitioning).
///
/// Returns the SPI client unchanged so the caller can chain follow-up
/// `UPDATE graph_child` work in the same connection scope.
pub fn insert_registry_row(
    client: &mut pgrx::spi::SpiClient<'_>,
    row: &RegistryRow<'_>,
) -> Result<(), spi::Error> {
    // Choose SQL based on whether any optional columns are set or whether
    // the caller asked for ::jsonb. The two shapes preserve historical
    // byte-for-byte catalog rows:
    //   * decomposed (json, 16 cols, no where/partition/ignored) → set-op,
    //     DISTINCT ON, window paths.
    //   * full (jsonb, 20 cols, where + ignored + partition) → main path.
    let full_shape = row.aggregations_cast == AggregationsCast::Jsonb
        || row.where_predicate.is_some()
        || row.ignored_sources.is_some()
        || row.partition_columns.is_some()
        || row.partition_strategy.is_some()
        || !row.unlogged_tables.is_empty();

    let oid_text = PgBuiltInOids::TEXTOID.oid().value();
    let oid_int4 = PgBuiltInOids::INT4OID.oid().value();
    let oid_bool = PgBuiltInOids::BOOLOID.oid().value();

    // Schema the IMV's objects land in: the explicit schema when the name is
    // qualified, else the empty string so the INSERT's COALESCE falls back to
    // current_schema() at create time. Persisted so drop teardown can qualify
    // its DDL independently of the session search_path at drop time.
    let explicit_schema_owned = crate::query_decomposer::canonical_source(row.view_name)
        .0
        .unwrap_or_default();

    let view_name_owned = row.view_name.to_string();
    let sql_query_owned = row.sql_query.to_string();
    let base_query_owned = row.base_query.to_string();
    let end_query_owned = row.end_query.to_string();
    let aggregations_owned = row.aggregations_json.to_string();
    let storage_owned = row.storage_mode.to_string();
    let refresh_owned = row.refresh_mode.to_string();
    let depends_on_owned = format_pg_text_array(row.depends_on);
    let depends_on_imv_owned = format_pg_text_array(row.depends_on_imv);
    let unlogged_owned = format_pg_text_array(row.unlogged_tables);
    let graph_child_owned = format_pg_text_array(row.graph_child);
    let index_cols_owned = format_pg_text_array(row.index_columns);
    let unique_cols_owned = format_pg_text_array(row.unique_columns);

    if !full_shape {
        let sql = "INSERT INTO public.__reflex_ivm_reference
                     (name, graph_depth, depends_on, depends_on_imv, unlogged_tables,
                      graph_child, sql_query, base_query, end_query,
                      aggregations, index_columns, unique_columns, enabled, last_update_date,
                      storage_mode, refresh_mode, target_schema)
                     VALUES ($1, $2, $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::TEXT[], $7, $8, $9, $10::json, $11::TEXT[], $12::TEXT[], TRUE, NOW(), $13, $14, COALESCE(NULLIF($15, ''), current_schema()))";
        client
            .update(
                sql,
                None,
                &[
                    unsafe { DatumWithOid::new(view_name_owned, oid_text) },
                    unsafe { DatumWithOid::new(row.graph_depth, oid_int4) },
                    unsafe { DatumWithOid::new(depends_on_owned, oid_text) },
                    unsafe { DatumWithOid::new(depends_on_imv_owned, oid_text) },
                    unsafe { DatumWithOid::new(unlogged_owned, oid_text) },
                    unsafe { DatumWithOid::new(graph_child_owned, oid_text) },
                    unsafe { DatumWithOid::new(sql_query_owned, oid_text) },
                    unsafe { DatumWithOid::new(base_query_owned, oid_text) },
                    unsafe { DatumWithOid::new(end_query_owned, oid_text) },
                    unsafe { DatumWithOid::new(aggregations_owned, oid_text) },
                    unsafe { DatumWithOid::new(index_cols_owned, oid_text) },
                    unsafe { DatumWithOid::new(unique_cols_owned, oid_text) },
                    unsafe { DatumWithOid::new(storage_owned, oid_text) },
                    unsafe { DatumWithOid::new(refresh_owned, oid_text) },
                    unsafe { DatumWithOid::new(explicit_schema_owned, oid_text) },
                ],
            )
            .map(|_| ())
    } else {
        let where_predicate_owned = row.where_predicate.unwrap_or("").to_string();
        let ignored_sources_owned = match row.ignored_sources {
            Some(s) => format_pg_text_array(s),
            None => format_pg_text_array(&[] as &[String]),
        };
        let part_cols_owned = match row.partition_columns {
            Some(s) => format_pg_text_array(s),
            None => format_pg_text_array(&[] as &[String]),
        };
        let part_strat_owned = row.partition_strategy.unwrap_or("").to_string();

        let sql = "INSERT INTO public.__reflex_ivm_reference
                     (name, graph_depth, depends_on, depends_on_imv, unlogged_tables,
                      graph_child, sql_query, base_query, end_query,
                      aggregations, index_columns, unique_columns, enabled, last_update_date,
                      storage_mode, refresh_mode, where_predicate, ignored_sources,
                      partition_columns, partition_strategy, target_schema, max_one_row, partition_depth)
                     VALUES ($1, $2, $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::TEXT[], $7, $8, $9, $10::jsonb, $11::TEXT[], $12::TEXT[], TRUE, NOW(), $13, $14, NULLIF($15, ''), $16::TEXT[], NULLIF($17, '{}')::TEXT[], NULLIF($18, ''), COALESCE(NULLIF($19, ''), current_schema()), $20, $21)";
        client
            .update(
                sql,
                None,
                &[
                    unsafe { DatumWithOid::new(view_name_owned, oid_text) },
                    unsafe { DatumWithOid::new(row.graph_depth, oid_int4) },
                    unsafe { DatumWithOid::new(depends_on_owned, oid_text) },
                    unsafe { DatumWithOid::new(depends_on_imv_owned, oid_text) },
                    unsafe { DatumWithOid::new(unlogged_owned, oid_text) },
                    unsafe { DatumWithOid::new(graph_child_owned, oid_text) },
                    unsafe { DatumWithOid::new(sql_query_owned, oid_text) },
                    unsafe { DatumWithOid::new(base_query_owned, oid_text) },
                    unsafe { DatumWithOid::new(end_query_owned, oid_text) },
                    unsafe { DatumWithOid::new(aggregations_owned, oid_text) },
                    unsafe { DatumWithOid::new(index_cols_owned, oid_text) },
                    unsafe { DatumWithOid::new(unique_cols_owned, oid_text) },
                    unsafe { DatumWithOid::new(storage_owned, oid_text) },
                    unsafe { DatumWithOid::new(refresh_owned, oid_text) },
                    unsafe { DatumWithOid::new(where_predicate_owned, oid_text) },
                    unsafe { DatumWithOid::new(ignored_sources_owned, oid_text) },
                    unsafe { DatumWithOid::new(part_cols_owned, oid_text) },
                    unsafe { DatumWithOid::new(part_strat_owned, oid_text) },
                    unsafe { DatumWithOid::new(explicit_schema_owned, oid_text) },
                    unsafe { DatumWithOid::new(row.max_one_row, oid_bool) },
                    unsafe { DatumWithOid::new(row.partition_depth, oid_int4) },
                ],
            )
            .map(|_| ())
    }
}

/// Helper: append this IMV as a `graph_child` of each `parent_imv`.
///
/// Consolidates the 4 duplicate `for imv_name in &depends_on_imv` loops in
/// the pre-1.6.1 create paths. Each loop did `UPDATE … SET graph_child =
/// array_append(...) WHERE name = $2` for the parent IMV; this helper does
/// the same thing in one call site.
pub fn add_graph_child_links(
    client: &mut pgrx::spi::SpiClient<'_>,
    child_view: &str,
    parents: &[String],
) -> Result<(), spi::Error> {
    let oid_text = PgBuiltInOids::TEXTOID.oid().value();
    for parent in parents {
        client.update(
            "UPDATE public.__reflex_ivm_reference
                         SET graph_child = array_append(COALESCE(graph_child, ARRAY[]::TEXT[]), $1)
                         WHERE name = $2",
            None,
            &[
                unsafe { DatumWithOid::new(child_view.to_string(), oid_text) },
                unsafe { DatumWithOid::new(parent.clone(), oid_text) },
            ],
        )?;
    }
    Ok(())
}
