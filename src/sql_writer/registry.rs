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

use crate::aggregation::AggregationPlan;
use crate::sql_writer::identifier::format_pg_text_array;
use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
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
    /// JSON object with creation-time parameters for chain rebuild (1.11.0).
    /// Populated at create time; `None` → NULL in catalog → `{}` on rebuild.
    pub create_args: Option<&'a str>,
    /// TRUE when every real source of this node is a materialized view (PS-3,
    /// 1.11.0). Such a node cannot self-maintain — PG fires no trigger on a
    /// matview — so it is a snapshot frozen at create time and needs an explicit
    /// `refresh_imv_depending_on('<mv>')` after each `REFRESH MATERIALIZED VIEW`.
    /// PERMANENT and structural: distinct from `known_stale`, never cleared by
    /// reconcile or `verify_stale_cleared`. The decomposed paths leave it false
    /// (their real sources are triggerable sub-IMV tables).
    pub requires_explicit_refresh: bool,
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
            create_args: None,
            requires_explicit_refresh: false,
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
    let create_args_owned = row.create_args.unwrap_or("{}").to_string();

    if !full_shape {
        let sql = "INSERT INTO public.__reflex_ivm_reference
                     (name, graph_depth, depends_on, depends_on_imv, unlogged_tables,
                      graph_child, sql_query, base_query, end_query,
                      aggregations, index_columns, unique_columns, enabled, last_update_date,
                      storage_mode, refresh_mode, target_schema, create_args)
                     VALUES ($1, $2, $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::TEXT[], $7, $8, $9, $10::json, $11::TEXT[], $12::TEXT[], TRUE, NOW(), $13, $14, COALESCE(NULLIF($15, ''), current_schema()), $16)";
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
                    unsafe { DatumWithOid::new(create_args_owned, oid_text) },
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
                      partition_columns, partition_strategy, target_schema, max_one_row, partition_depth, create_args,
                      requires_explicit_refresh)
                     VALUES ($1, $2, $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::TEXT[], $7, $8, $9, $10::jsonb, $11::TEXT[], $12::TEXT[], TRUE, NOW(), $13, $14, NULLIF($15, ''), $16::TEXT[], NULLIF($17, '{}')::TEXT[], NULLIF($18, ''), COALESCE(NULLIF($19, ''), current_schema()), $20, $21, $22, $23)";
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
                    unsafe { DatumWithOid::new(create_args_owned, oid_text) },
                    unsafe { DatumWithOid::new(row.requires_explicit_refresh, oid_bool) },
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

/// Owned, typed view of one `public.__reflex_ivm_reference` row — the read
/// counterpart to [`RegistryRow`]. Carries the single-row fields the catalog
/// readers consume (reconcile, drop, audit) and parses the `aggregations`
/// JSONB into an [`AggregationPlan`] exactly once. Reporting projections
/// (introspect's status query, audit's all-rows scan) are deliberately NOT
/// served by this struct — they read column subsets across many rows.
// `ImvRecord` is a faithful typed mirror of the catalog row: most fields have
// a production reader (reconcile/drop/audit), `view_name` is asserted by the
// read seam's round-trip test, and a field like `refresh_mode` is carried for
// completeness ahead of a reader. The allow keeps a complete read DTO clean
// without trimming fields the cdylib build would otherwise flag.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct ImvRecord {
    pub view_name: String,
    pub base_query: String,
    pub end_query: String,
    /// Parsed once from the `aggregations` JSONB column. `None` when the
    /// stored JSON is absent or unparseable (legacy/empty rows).
    pub plan: Option<AggregationPlan>,
    pub storage_mode: String,
    pub refresh_mode: String,
    pub enabled: bool,
    pub depends_on: Vec<String>,
    pub depends_on_imv: Vec<String>,
    pub graph_child: Vec<String>,
    /// Schema the IMV's objects were created in (1.7.1). `None` for legacy rows.
    pub target_schema: Option<String>,
    /// IMV partition mirror depth (1.8.2). `None` => mirror full source depth.
    pub partition_depth: Option<i32>,
}

impl ImvRecord {
    /// See [`owns_intermediate`].
    pub fn owns_intermediate(&self) -> bool {
        owns_intermediate(&self.end_query)
    }
}

/// Whether an intermediate table (`__reflex_intermediate_<view>`) participates in
/// this IMV's maintenance, decided from its `end_query`.
///
/// `end_query` is non-empty exactly for the aggregate shape whose target is filled
/// FROM the intermediate; it is `''` for a passthrough IMV and for every decomposed
/// wrapper row. This is the branch the runtime itself takes (`src/partition.rs:633`,
/// `:644`, `:1054`, `src/trigger/ops.rs:436`, `:653`, `:783`,
/// `src/reconcile.rs:139`), which is why the audit reads the same signal rather
/// than `aggregations->>'is_passthrough'`: `RegistryRow::decomposed` writes
/// `aggregations = '{}'`, so the JSON signal claims "not passthrough" — and
/// therefore "has an intermediate" — for wrapper rows that own nothing. One
/// function, so the audit's classification and reconcile's heal gate cannot drift
/// apart (see `PartitionMirror::run` in `src/audit/checks_b_drift.rs` for the full
/// rationale, and `ImvRow::owns_intermediate` in `src/audit/mod.rs`).
pub fn owns_intermediate(end_query: &str) -> bool {
    !end_query.is_empty()
}

/// Read one IMV row by name. Returns `None` when no row matches (the IMV is
/// not registered). Disabled rows are still returned — callers that require
/// `enabled = TRUE` check [`ImvRecord::enabled`] themselves (matching the
/// historical per-caller `WHERE enabled = TRUE` filters).
pub fn read_imv(client: &pgrx::spi::SpiClient<'_>, name: &str) -> Option<ImvRecord> {
    let rows =
        client
            .select(
                "SELECT name, base_query, end_query, aggregations::text AS aggregations, \
                    storage_mode, refresh_mode, enabled, depends_on, depends_on_imv, \
                    graph_child, target_schema, partition_depth \
             FROM public.__reflex_ivm_reference WHERE name = $1",
                Some(1),
                &[unsafe {
                    DatumWithOid::new(name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .collect::<Vec<_>>();

    let row = rows.first()?;

    let agg_json: String = row
        .get_by_name::<&str, _>("aggregations")
        .unwrap_or(None)
        .unwrap_or("{}")
        .to_string();

    Some(ImvRecord {
        view_name: row
            .get_by_name::<&str, _>("name")
            .unwrap_or(None)
            .unwrap_or("")
            .to_string(),
        base_query: row
            .get_by_name::<&str, _>("base_query")
            .unwrap_or(None)
            .unwrap_or("")
            .to_string(),
        end_query: row
            .get_by_name::<&str, _>("end_query")
            .unwrap_or(None)
            .unwrap_or("")
            .to_string(),
        plan: serde_json::from_str::<AggregationPlan>(&agg_json).ok(),
        storage_mode: row
            .get_by_name::<&str, _>("storage_mode")
            .unwrap_or(None)
            .unwrap_or("UNLOGGED")
            .to_string(),
        refresh_mode: row
            .get_by_name::<&str, _>("refresh_mode")
            .unwrap_or(None)
            .unwrap_or("IMMEDIATE")
            .to_string(),
        enabled: row
            .get_by_name::<bool, _>("enabled")
            .unwrap_or(None)
            .unwrap_or(true),
        depends_on: row
            .get_by_name::<Vec<String>, _>("depends_on")
            .unwrap_or(None)
            .unwrap_or_default(),
        depends_on_imv: row
            .get_by_name::<Vec<String>, _>("depends_on_imv")
            .unwrap_or(None)
            .unwrap_or_default(),
        graph_child: row
            .get_by_name::<Vec<String>, _>("graph_child")
            .unwrap_or(None)
            .unwrap_or_default(),
        target_schema: row
            .get_by_name::<&str, _>("target_schema")
            .unwrap_or(None)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string()),
        partition_depth: row.get_by_name::<i32, _>("partition_depth").unwrap_or(None),
    })
}

/// Read IMV row with create_args for chain rebuild. Returns the fields needed
/// to faithfully recreate the IMV via reflex_rebuild_chain.
pub fn read_imv_for_rebuild(
    client: &pgrx::spi::SpiClient<'_>,
    name: &str,
) -> Option<(String, String)> {
    // Returns (sql_query, create_args)
    let rows =
        client
            .select(
                "SELECT sql_query, COALESCE(create_args, '{}') AS create_args \
             FROM public.__reflex_ivm_reference WHERE name = $1",
                Some(1),
                &[unsafe {
                    DatumWithOid::new(name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .collect::<Vec<_>>();

    let row = rows.first()?;

    let sql_query = row
        .get_by_name::<&str, _>("sql_query")
        .unwrap_or(None)
        .unwrap_or("")
        .to_string();
    let create_args = row
        .get_by_name::<&str, _>("create_args")
        .unwrap_or(None)
        .unwrap_or("{}")
        .to_string();

    Some((sql_query, create_args))
}

/// The subset of a registry row that decides whether an anchor-scoped rebuild
/// (`reflex_rebuild_imv` / `reflex_reconcile`) can converge the IMV. Read as one
/// projection so the caller-side advisory (PS-14) reuses PS-3's stored
/// `requires_explicit_refresh` verdict and the existing `ignored_sources` /
/// `partition_strategy` metadata rather than re-deriving matview-ness.
#[derive(Clone, Copy, Debug, Default)]
pub struct RebuildRisk {
    /// PS-3: every real source is a materialized view — the node cannot
    /// self-maintain and the canonical recovery is `refresh_imv_depending_on`.
    pub requires_explicit_refresh: bool,
    /// At least one `ignore_sources` entry is declared. Combined with
    /// `is_partitioned`, this is the archive-residue shape: a partition whose
    /// rows arrive only via an authoritative ignored source stays empty under an
    /// anchor-scoped rebuild.
    pub has_ignored_sources: bool,
    /// The IMV is declaratively partitioned.
    pub is_partitioned: bool,
}

/// Read the [`RebuildRisk`] projection for one IMV. `None` when the IMV is not
/// registered.
pub fn read_rebuild_risk(client: &pgrx::spi::SpiClient<'_>, name: &str) -> Option<RebuildRisk> {
    let rows = client
        .select(
            "SELECT COALESCE(requires_explicit_refresh, FALSE) AS req, \
                    COALESCE(array_length(ignored_sources, 1), 0) > 0 AS has_ignored, \
                    (partition_strategy IS NOT NULL AND partition_strategy <> '') AS is_part \
             FROM public.__reflex_ivm_reference WHERE name = $1",
            Some(1),
            &[unsafe {
                DatumWithOid::new(name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        )
        .unwrap_or_report()
        .collect::<Vec<_>>();

    let row = rows.first()?;
    Some(RebuildRisk {
        requires_explicit_refresh: row
            .get_by_name::<bool, _>("req")
            .unwrap_or(None)
            .unwrap_or(false),
        has_ignored_sources: row
            .get_by_name::<bool, _>("has_ignored")
            .unwrap_or(None)
            .unwrap_or(false),
        is_partitioned: row
            .get_by_name::<bool, _>("is_part")
            .unwrap_or(None)
            .unwrap_or(false),
    })
}

/// Record one targeted-recovery call against an IMV: bump `rebuild_count` and
/// stamp `last_rebuild_at`. Returns the number of catalog rows updated (0 = IMV
/// not found). Called only from the targeted-recovery entry points, never from
/// the internal cascade descent (PS-14).
pub fn bump_rebuild_count(
    client: &mut pgrx::spi::SpiClient<'_>,
    name: &str,
) -> Result<u64, spi::Error> {
    let n = client
        .update(
            "UPDATE public.__reflex_ivm_reference \
             SET rebuild_count = COALESCE(rebuild_count, 0) + 1, last_rebuild_at = now() \
             WHERE name = $1",
            None,
            &[unsafe {
                DatumWithOid::new(name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        )?
        .len();
    Ok(n as u64)
}

/// Set or clear (`value = None`) the per-IMV `wipe_threshold` override.
/// Returns the number of catalog rows updated (0 = IMV not found).
pub fn set_wipe_threshold(
    client: &mut pgrx::spi::SpiClient<'_>,
    name: &str,
    value: Option<pgrx::AnyNumeric>,
) -> Result<u64, spi::Error> {
    let oid_text = PgBuiltInOids::TEXTOID.oid().value();
    let n = client
        .update(
            "UPDATE public.__reflex_ivm_reference SET wipe_threshold = $1 WHERE name = $2",
            None,
            &[
                unsafe {
                    DatumWithOid::new(value.clone(), PgBuiltInOids::NUMERICOID.oid().value())
                },
                unsafe { DatumWithOid::new(name.to_string(), oid_text) },
            ],
        )?
        .len();
    Ok(n as u64)
}

/// Internal: set or clear a single `BIGINT` config column by name. The
/// `column` argument is always a compile-time `&'static str` literal from
/// this module — never user input — so interpolating it is injection-safe.
fn set_int8_column(
    client: &mut pgrx::spi::SpiClient<'_>,
    column: &'static str,
    name: &str,
    value: Option<i64>,
) -> Result<u64, spi::Error> {
    let oid_text = PgBuiltInOids::TEXTOID.oid().value();
    let sql = format!("UPDATE public.__reflex_ivm_reference SET {column} = $1 WHERE name = $2");
    let n = client
        .update(
            &sql,
            None,
            &[
                unsafe { DatumWithOid::new(value, PgBuiltInOids::INT8OID.oid().value()) },
                unsafe { DatumWithOid::new(name.to_string(), oid_text) },
            ],
        )?
        .len();
    Ok(n as u64)
}

/// Set or clear (`value = None`) the per-IMV `wipe_floor_rows` override.
pub fn set_wipe_floor_rows(
    client: &mut pgrx::spi::SpiClient<'_>,
    name: &str,
    value: Option<i64>,
) -> Result<u64, spi::Error> {
    set_int8_column(client, "wipe_floor_rows", name, value)
}

/// Set or clear (`value = None`) the per-IMV `partition_dispatch_cost_cap`.
pub fn set_partition_dispatch_cost_cap(
    client: &mut pgrx::spi::SpiClient<'_>,
    name: &str,
    value: Option<i64>,
) -> Result<u64, spi::Error> {
    set_int8_column(client, "partition_dispatch_cost_cap", name, value)
}

/// Remove `child_view` from `parent_view`'s `graph_child` array. The read
/// counterpart of one iteration of [`add_graph_child_links`]; used by
/// `drop_reflex_ivm` to unlink a dropped IMV from each parent.
pub fn remove_graph_child(
    client: &mut pgrx::spi::SpiClient<'_>,
    parent_view: &str,
    child_view: &str,
) -> Result<(), spi::Error> {
    let oid_text = PgBuiltInOids::TEXTOID.oid().value();
    client.update(
        "UPDATE public.__reflex_ivm_reference \
         SET graph_child = array_remove(graph_child, $1) WHERE name = $2",
        None,
        &[
            unsafe { DatumWithOid::new(child_view.to_string(), oid_text) },
            unsafe { DatumWithOid::new(parent_view.to_string(), oid_text) },
        ],
    )?;
    Ok(())
}
