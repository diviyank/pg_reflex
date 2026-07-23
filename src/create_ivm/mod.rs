use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::aggregation::{plan_aggregation, plan_aggregation_with_topk};
use crate::query_decomposer::{
    affected_groups_table_name, bare_column_name, canonical_source, format_pg_text_array_literal,
    generate_aggregations_json, generate_base_query, generate_end_query, intermediate_table_name,
    normalized_column_name, quote_identifier, safe_identifier, shrunk_groups_table_name,
    split_qualified_name,
};
use crate::schema_builder::{
    build_deferred_flush_ddl, build_deferred_trigger_ddls, build_delta_scratch_table_ddl,
    build_indexes_ddl, build_intermediate_table_ddl, build_passthrough_scratch_ddls,
    build_staging_table_ddl, build_target_table_ddl, build_trigger_ddls, resolve_column_type,
};
use crate::sql_analyzer::{analyze, SqlAnalysisError};
use crate::sql_writer::{
    add_graph_child_links, insert_registry_row, AggregationsCast, RegistryRow,
};
use crate::validate_view_name;

/// Outputs of the parse-and-validate prelude of [`create_reflex_ivm_impl`].
///
/// Bundles the normalized storage/refresh flags, the parsed sqlparser AST and
/// the [`crate::sql_analyzer::SqlAnalysis`] so the decomposition helpers and
/// the main pipeline can read them without re-parsing.
pub(crate) struct ParsedInputs {
    logged: bool,
    deferred: bool,
    storage_upper: String,
    mode_upper: String,
    parsed_sql: Vec<sqlparser::ast::Statement>,
    analysis: crate::sql_analyzer::SqlAnalysis,
}

/// Pipeline state shared across phases of `create_reflex_ivm_impl`. Owns the
/// mutable `plan`; helpers mutate fields in-place. Input refs (`view_name`,
/// `sql`, etc.) borrow from the caller's arg slots.
pub(crate) struct BuildContext<'a> {
    // Inputs
    view_name: &'a str,
    sql: &'a str,
    unique_columns_str: &'a str,
    if_not_exists: bool,
    #[allow(dead_code)]
    topk_k: Option<usize>,
    ignore_sources: &'a [String],
    partition_by: &'a [String],
    /// True when the user explicitly requested an UNPARTITIONED IMV on a
    /// partitioned source (empty `partition_by` array via the partitioned
    /// overload). Suppresses auto-mirror so the target stays a plain table.
    /// Distinct from omitting `partition_by` (which auto-mirrors).
    explicit_unpartitioned: bool,

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
    /// Resolved IMV partition mirror-depth (number of source levels to
    /// mirror). `None` until `resolve_partitioning` runs; persisted to
    /// `__reflex_ivm_reference.partition_depth`. `Some(k)` = mirror k levels.
    resolved_partition_depth: Option<i32>,

    // SPI-phase outputs
    ivm_froms: Vec<String>,
    depth: i32,
    unlogged_tables: Vec<String>,
}

/// Shared, by-reference parameter bundle for the decomposition entry points
/// (`try_decompose_set_op`, `try_decompose_ctes`, `try_decompose_distinct_on`).
///
/// One struct so threading a new field reaches every path uniformly — a path
/// cannot silently lack one (this is exactly how Bug 2 happened).
pub(crate) struct DecomposeCtx<'a> {
    view_name: &'a str,
    sql: &'a str,
    unique_columns_str: &'a str,
    cte_unique_columns: &'a std::collections::HashMap<String, String>,
    storage_mode: &'a str,
    refresh_mode: &'a str,
    topk_k: Option<usize>,
    ignore_sources: &'a [String],
    partition_by: &'a [String],
    parsed: &'a ParsedInputs,
    /// When true, decompositions that would otherwise emit a `CREATE VIEW`
    /// wrapper (UNION ALL today; DISTINCT ON / window in future) MUST instead
    /// materialise the wrapper as an UNLOGGED TABLE maintained by per-operand
    /// mirror triggers. Set on every recursive `create_reflex_ivm_impl` call
    /// that builds an intermediate sub-IMV (e.g. a CTE sub-IMV), because
    /// downstream consumers will try to install transition-table triggers on
    /// the wrapper, which PostgreSQL rejects for views.
    materialize_as_table: bool,
}

/// Phase 1 of the create-IMV pipeline.
///
/// Normalizes `storage`/`refresh`, validates the view name, parses the SQL
/// and runs [`crate::sql_analyzer::analyze`] on it. Also rejects DISTINCT on
/// aggregates other than COUNT (only COUNT(DISTINCT) is incrementally
/// maintainable).
///
/// Returns the error string verbatim on the `Err` arm so the caller can
/// `return` it unchanged — preserves byte-identical error reporting.
fn validate_and_parse_inputs(
    view_name: &str,
    sql: &str,
    storage_mode: &str,
    refresh_mode: &str,
) -> Result<ParsedInputs, &'static str> {
    let storage_upper = storage_mode.to_uppercase();
    if storage_upper != "LOGGED" && storage_upper != "UNLOGGED" {
        return Err(crate::reflex_reject(
            "storage must be 'LOGGED' or 'UNLOGGED'",
        ));
    }
    let logged = storage_upper == "LOGGED";
    let mode_upper = refresh_mode.to_uppercase();
    if mode_upper != "IMMEDIATE" && mode_upper != "DEFERRED" {
        return Err(crate::reflex_reject(
            "mode must be 'IMMEDIATE' or 'DEFERRED'",
        ));
    }
    let deferred = mode_upper == "DEFERRED";
    validate_view_name(view_name)?;
    let dialect = PostgreSqlDialect {};
    let parsed_sql = match Parser::parse_sql(&dialect, sql) {
        Ok(stmts) => stmts,
        Err(e) => {
            warning!("pg_reflex: failed to parse SQL for '{}': {}", view_name, e);
            return Err(crate::reflex_reject(&format!("Failed to parse SQL: {}", e)));
        }
    };
    let analysis = match analyze(&parsed_sql) {
        Err(SqlAnalysisError::MultipleQueries(_)) => {
            return Err(crate::reflex_reject("Expected 1 query, got multiple"));
        }
        Err(SqlAnalysisError::NotASelectQuery) => {
            return Err(crate::reflex_reject("Query is not a SELECT"));
        }
        Ok(a) => {
            if let Some(reason) = a.unsupported_reason() {
                return Err(crate::reflex_reject(&reason));
            }
            // Reject SUM(DISTINCT), AVG(DISTINCT), etc. — DISTINCT modifier is only
            // supported on COUNT. Check the original SQL for the pattern.
            let sql_upper = sql.to_uppercase();
            let has_distinct_agg = sql_upper.contains("SUM(DISTINCT")
                || sql_upper.contains("SUM (DISTINCT")
                || sql_upper.contains("AVG(DISTINCT")
                || sql_upper.contains("AVG (DISTINCT")
                || sql_upper.contains("MIN(DISTINCT")
                || sql_upper.contains("MIN (DISTINCT")
                || sql_upper.contains("MAX(DISTINCT")
                || sql_upper.contains("MAX (DISTINCT")
                || sql_upper.contains("BOOL_OR(DISTINCT")
                || sql_upper.contains("BOOL_OR (DISTINCT");
            if has_distinct_agg {
                return Err(crate::reflex_reject(
                    "DISTINCT modifier on SUM/AVG/MIN/MAX/BOOL_OR is not supported. \
                     Only COUNT(DISTINCT col) is supported. Use a CTE with SELECT DISTINCT \
                     to pre-deduplicate: WITH d AS (SELECT DISTINCT grp, val FROM t) SELECT grp, SUM(val) FROM d GROUP BY grp"
                ));
            }
            a
        }
    };
    Ok(ParsedInputs {
        logged,
        deferred,
        storage_upper,
        mode_upper,
        parsed_sql,
        analysis,
    })
}

/// For passthrough IMVs, resolve the unique-key column set: explicit
/// `unique_columns_str` if non-empty, else probe single-source PKs from
/// pg_index. Multi-source JOINs without an explicit key fall back to
/// full refresh on DELETE/UPDATE (warned). Populates
/// `ctx.resolved_unique_columns` and `ctx.plan.passthrough_columns` /
/// `ctx.plan.passthrough_key_mappings`.
fn resolve_unique_columns(ctx: &mut BuildContext) {
    if !ctx.plan.is_passthrough {
        return;
    }

    if !ctx.unique_columns_str.is_empty() {
        // Explicit unique columns from 3rd parameter
        ctx.resolved_unique_columns = ctx
            .unique_columns_str
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

        // Build per-source-table column mappings
        let real_sources: Vec<&String> = ctx.real_source_names.iter().collect();
        build_passthrough_key_mappings(
            &mut ctx.plan,
            &ctx.resolved_unique_columns,
            &real_sources,
            &ctx.analysis,
        );
    } else if !ctx.is_join_query {
        // Auto-detect: only for single-source queries (JOINs need explicit key)
        let select_bare_names: std::collections::HashSet<String> = ctx
            .analysis
            .select_columns
            .iter()
            .map(|c| {
                let name = c.alias.as_deref().unwrap_or(&c.expr_sql);
                bare_column_name(name).to_lowercase()
            })
            .collect();

        let real_sources: Vec<&String> = ctx.real_source_names.iter().collect();
        for source in &real_sources {
            let pk_cols: Vec<String> = Spi::connect(|client| {
                client
                    .select(
                        "SELECT array_agg(a.attname::text ORDER BY k.n) as cols \
                         FROM pg_index ix \
                         JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(col, n) ON true \
                         JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.col \
                         WHERE ix.indrelid = to_regclass($1) \
                           AND ix.indisunique AND ix.indisprimary \
                         GROUP BY ix.indexrelid \
                         ORDER BY count(*) \
                         LIMIT 1",
                        None,
                        &[unsafe {
                            DatumWithOid::new(
                                source.to_string(),
                                PgBuiltInOids::TEXTOID.oid().value(),
                            )
                        }],
                    )
                    .unwrap_or_report()
                    .filter_map(|row| row.get_by_name::<Vec<String>, _>("cols").unwrap_or(None))
                    .next()
                    .unwrap_or_default()
            });

            if !pk_cols.is_empty() {
                let pk_lower: Vec<String> = pk_cols.iter().map(|c| c.to_lowercase()).collect();
                let all_in_select = pk_lower.iter().all(|c| select_bare_names.contains(c));
                if all_in_select {
                    ctx.resolved_unique_columns = pk_lower;
                    ctx.plan.passthrough_columns = ctx.resolved_unique_columns.clone();
                    // Single source: 1:1 mapping (target col == source col)
                    ctx.plan.passthrough_key_mappings.insert(
                        source.to_string(),
                        ctx.resolved_unique_columns
                            .iter()
                            .map(|c| (c.clone(), c.clone()))
                            .collect(),
                    );
                    info!(
                        "pg_reflex: auto-detected PK ({}) from '{}' for '{}'",
                        ctx.resolved_unique_columns.join(", "),
                        source,
                        ctx.view_name
                    );
                    break;
                } else {
                    info!(
                        "pg_reflex: source '{}' has PK ({}) but the SELECT list does not include all PK columns — \
                         passthrough '{}' will fall back to row-matching for DELETE/UPDATE. \
                         Add the PK columns to the SELECT list, or pass them as the 3rd argument to create_reflex_ivm.",
                        source,
                        pk_lower.join(", "),
                        ctx.view_name
                    );
                }
            }
        }
    } else if let Some(inferred) = infer_join_passthrough_unique_key(ctx) {
        // JOIN passthrough with a structurally-sound unique key (anchor PK
        // preserved through to-one equi-joins).
        ctx.resolved_unique_columns = inferred;
        ctx.plan.passthrough_columns = ctx.resolved_unique_columns.clone();
        let real_sources: Vec<&String> = ctx.real_source_names.iter().collect();
        build_passthrough_key_mappings(
            &mut ctx.plan,
            &ctx.resolved_unique_columns,
            &real_sources,
            &ctx.analysis,
        );
        info!(
            "pg_reflex: inferred unique key ({}) for JOIN passthrough '{}'",
            ctx.resolved_unique_columns.join(", "),
            ctx.view_name
        );
    } else {
        // JOIN query without a provable key: fall back to full refresh on DELETE/UPDATE
        info!(
            "pg_reflex: JOIN passthrough '{}' has no unique key. \
             Provide 3rd argument to create_reflex_ivm for incremental DELETE/UPDATE. \
             Example: SELECT create_reflex_ivm('{}', '...', 'col1,col2')",
            ctx.view_name, ctx.view_name
        );
    }
}

fn populate_source_join_keys(ctx: &mut BuildContext) {
    if ctx.plan.is_passthrough || !ctx.is_join_query {
        return;
    }
    let real_sources: Vec<&String> = ctx.real_source_names.iter().collect();
    build_source_join_keys(&mut ctx.plan, &real_sources, &ctx.analysis);
}

/// Warn on SELECT entries that are neither GROUP BY nor recognized aggregates.
/// These columns will be missing from the IMV (silent data loss is worse than a warning).
fn validate_select_columns(ctx: &BuildContext) {
    if ctx.plan.is_passthrough {
        return;
    }
    let group_by_set: std::collections::HashSet<&str> = ctx
        .plan
        .group_by_columns
        .iter()
        .map(|s| s.as_str())
        .collect();
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
                    DatumWithOid::new(
                        ctx.view_name.to_string(),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
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
        return Some(crate::reflex_reject("IMV with this name already exists"));
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
                    DatumWithOid::new(
                        ctx.view_name.to_string(),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
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
        return Some(crate::reflex_reject(
            "circular dependency detected — this IMV would form a cycle in the dependency graph",
        ));
    }
    None
}

/// Resolve `partition_by`: validate explicit columns against plan + anchor, or
/// auto-mirror from the single partitioned source. Populates
/// `ctx.resolved_partition_cols` and `ctx.resolved_strategy`. Returns error
/// message string on validation failure (caller re-leaks).
fn resolve_partitioning(ctx: &mut BuildContext) -> Result<(), String> {
    // Explicit opt-out: the user asked for an unpartitioned IMV (empty
    // `partition_by`) even though a source may be partitioned. Skip both the
    // explicit-validation and auto-mirror paths — leave the IMV a plain table.
    if ctx.explicit_unpartitioned {
        ctx.resolved_partition_cols = Vec::new();
        ctx.resolved_partition_depth = None;
        info!(
            "pg_reflex: '{}' created UNPARTITIONED by request (partition_by => []); \
             source partition swaps will trigger a full reconcile via flush",
            ctx.view_name
        );
        return Ok(());
    }

    ctx.resolved_partition_cols = ctx.partition_by.to_vec();

    if !ctx.resolved_partition_cols.is_empty() {
        // Explicit partition_by — validate against plan shape.
        if !ctx.plan.is_passthrough {
            let gb_lower: std::collections::HashSet<String> = ctx
                .plan
                .group_by_columns
                .iter()
                .map(|c| c.to_lowercase())
                .collect();
            // Also normalize qualified GROUP BY columns ("d.region" →
            // "region") so partition_by can name the bare column even
            // when the GROUP BY clause uses a `<table>.col` form.
            let gb_normalized: std::collections::HashSet<String> = ctx
                .plan
                .group_by_columns
                .iter()
                .map(|c| normalized_column_name(c).to_lowercase())
                .collect();
            let projected_aliases: std::collections::HashSet<String> = ctx
                .plan
                .group_by_aliases
                .values()
                .map(|v| v.to_lowercase())
                .collect();
            // Reverse map from alias → GROUP BY expression (the AST string
            // before aliasing).  Used to recover the underlying GROUP BY
            // expression when the user passes the alias in `partition_by`.
            let alias_to_gb_expr: std::collections::HashMap<String, String> = ctx
                .plan
                .group_by_aliases
                .iter()
                .map(|(k, v)| (v.to_lowercase(), k.clone()))
                .collect();
            for col in &ctx.resolved_partition_cols {
                let col_l = col.to_lowercase();
                if !gb_lower.contains(&col_l)
                    && !gb_normalized.contains(&col_l)
                    && !projected_aliases.contains(&col_l)
                {
                    return Err(crate::reflex_reject(&format!(
                        "partition_by column '{}' is not in GROUP BY; \
                         partition columns must be a subset of GROUP BY for aggregate IMVs",
                        col
                    ))
                    .to_string());
                }
                // Phase B (plans/partitioning_3.md §2): reject when the
                // matching GROUP BY entry is a computed expression rather
                // than a bare column reference.  We look up the AST string
                // that produced this `partition_by` column — either the
                // group_by_columns entry itself (when partition_by matches
                // the GROUP BY's lexical form) or, when partition_by names
                // an alias, the GROUP BY entry whose alias is `col`.
                let gb_expr: Option<String> = if gb_lower.contains(&col_l) {
                    ctx.plan
                        .group_by_columns
                        .iter()
                        .find(|gb| gb.to_lowercase() == col_l)
                        .cloned()
                } else if gb_normalized.contains(&col_l) {
                    ctx.plan
                        .group_by_columns
                        .iter()
                        .find(|gb| normalized_column_name(gb).to_lowercase() == col_l)
                        .cloned()
                } else {
                    alias_to_gb_expr.get(&col_l).cloned()
                };
                if let Some(ref gb) = gb_expr {
                    if !crate::sql_analyzer::is_bare_column_reference(gb) {
                        return Err(crate::reflex_reject(&format!(
                            "partition_by column '{}' corresponds to a computed \
                             GROUP BY expression ('{}'). Partition columns must be bare \
                             column references on the source. Workaround: add a generated \
                             / computed column to the source and partition on that.",
                            col, gb
                        ))
                        .to_string());
                    }
                }
            }
        }
        let validate_result: Result<String, String> = Spi::connect(|client| {
            let anchor = crate::partition::resolve_anchor_source(
                client,
                &ctx.resolved_partition_cols[0],
                &ctx.real_source_names,
            )?;
            let desc = crate::partition::introspect_partition_descriptor(client, &anchor)
                .ok_or_else(|| {
                    format!(
                        "anchor source '{}' for column '{}' is not partitioned LIST/RANGE",
                        anchor, ctx.resolved_partition_cols[0]
                    )
                })?;
            let part_col_l = ctx.resolved_partition_cols[0].to_lowercase();
            if !desc.column_names.iter().any(|c| c == &part_col_l) {
                return Err(format!(
                    "anchor source '{}' is partitioned but not on '{}' (partitioned on: {:?})",
                    anchor, ctx.resolved_partition_cols[0], desc.column_names
                ));
            }

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
                            declared.len(),
                            anchor,
                            source_level_cols.len()
                        ));
                    }
                    Some(src_col) if src_col.to_lowercase() != dl => {
                        return Err(format!(
                            "partition_by level {} is '{}' but source '{}' is partitioned on \
                             '{}' at that level; declared levels must match the source's \
                             partition key columns top-down",
                            i + 1,
                            declared_col,
                            anchor,
                            src_col
                        ));
                    }
                    Some(_) => {}
                }
                // Only validate sub-levels (i >= 1) for unique key presence.
                // Level 0 (root) is already validated above in the aggregate/passthrough
                // check that ensured it's in GROUP BY or passthrough columns.
                if i > 0 && !unique_key_cols.contains(&dl) {
                    return Err(format!(
                        "partition key column '{}' (level {} of source '{}') is not a bare \
                         projected output column in the IMV's unique key. Add it to the SELECT \
                         list and unique_columns, or declare a shallower partition_by.",
                        declared_col,
                        i + 1,
                        anchor
                    ));
                }
            }

            Ok(desc.strategy)
        });
        match validate_result {
            Ok(s) => {
                ctx.resolved_strategy = s;
                ctx.resolved_partition_depth = Some(ctx.resolved_partition_cols.len() as i32);
            }
            Err(e) => {
                return Err(crate::reflex_reject(&format!(
                    "partition_by validation failed — {}",
                    e
                ))
                .to_string());
            }
        }
    } else {
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

            // Bare projected output columns of the IMV. A level is mirrorable
            // only when its partition column appears as a BARE column reference
            // in the SELECT (not a computed expression like COALESCE). We must
            // NOT seed from passthrough_columns / the unique key — a column can
            // be in the unique key yet projected via COALESCE (e.g. a FULL JOIN
            // coalesced key), which is exactly the case that must prune.
            let projected: std::collections::HashSet<String> = if ctx.plan.is_passthrough {
                let mut set: std::collections::HashSet<String> = std::collections::HashSet::new();
                for c in &ctx.analysis.select_columns {
                    if crate::sql_analyzer::is_bare_column_reference(&c.expr_sql) {
                        let name = c.alias.as_deref().unwrap_or(&c.expr_sql);
                        set.insert(bare_column_name(name).to_lowercase());
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
                    anchor,
                    depth,
                    depth + 1,
                    level_cols[depth]
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
    }

    Ok(())
}

/// Resolve existing IMV dependencies among `ctx.froms` and compute graph_depth.
/// Populates `ctx.ivm_froms` and `ctx.depth`.
fn resolve_existing_imv_deps(client: &mut pgrx::spi::SpiClient<'_>, ctx: &mut BuildContext) {
    // The probe must compare canonical names, not raw ones. A CTE-decomposed
    // sub-IMV source is persisted double-quoted (`"schema"."view__cte_x"`) to
    // preserve identifier case, while the registry `name` is whatever the caller
    // passed `create_reflex_ivm` — never quoted. Comparing raw strings therefore
    // never matched a generated child, which left `depends_on_imv` and the
    // child's `graph_child` empty and collapsed `graph_depth` (PS-1 / N1).
    //
    // Only this side needs canonicalising: `canonical_source` is the identity on
    // an unquoted bare or schema-qualified name, so routing through it strictly
    // widens the match and cannot un-match a source that matched before.
    //
    // `ctx.froms` itself is deliberately left alone — the quoted spelling stored
    // in `depends_on` is load-bearing for the drop-time prefix scan, for the
    // `sanitized_source_suffix`-derived trigger and staging-table names, and for
    // the `$1 = ANY(depends_on)` lookups in `refresh_imv_depending_on` and
    // `reflex_flush_deferred`.
    let canonical_froms: Vec<String> = ctx
        .froms
        .iter()
        .map(|source| match canonical_source(source) {
            (Some(schema), bare) => format!("{schema}.{bare}"),
            (None, bare) => bare,
        })
        .collect();

    let args = [unsafe {
        DatumWithOid::new(
            format_pg_text_array_literal(&canonical_froms),
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

    // Registry names, not the raw `froms` spelling: `depends_on_imv`,
    // `add_graph_child_links` and `remove_graph_child` all join on `name`.
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
fn apply_partition_plan(client: &mut pgrx::spi::SpiClient<'_>, ctx: &mut BuildContext) {
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
        let partition_col = ctx.plan.partition_columns[0].to_lowercase();
        let anchor = ctx.plan.anchor_source.clone();
        let anchor_quoted = quote_identifier(&anchor);
        for (source, mappings) in &ctx.plan.source_join_keys.clone() {
            if source.eq_ignore_ascii_case(&anchor)
                || split_qualified_name(source)
                    .1
                    .eq_ignore_ascii_case(split_qualified_name(&anchor).1)
            {
                continue;
            }
            let matched_source_col = mappings
                .iter()
                .find(|(ic, _)| ic.eq_ignore_ascii_case(&partition_col))
                .map(|(_, sc)| sc.clone());
            if let Some(source_col) = matched_source_col {
                let fragment = format!(
                    "SELECT a.\"{pc}\" AS pkey, t.* \
                     FROM {{transition_alias}} t \
                     JOIN {anchor} a ON a.\"{pc}\" = t.\"{sc}\"",
                    pc = partition_col,
                    anchor = anchor_quoted,
                    sc = source_col,
                );
                ctx.plan
                    .partition_join_paths
                    .insert(source.clone(), fragment);
            }
        }
    }
}

/// Drop `imv_relevant_columns` entries that don't exist on the source table.
/// Mutates: `ctx.plan.imv_relevant_columns`.
fn filter_imv_relevant_columns(client: &mut pgrx::spi::SpiClient<'_>, ctx: &mut BuildContext) {
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
fn materialize_passthrough(client: &mut pgrx::spi::SpiClient<'_>, ctx: &mut BuildContext) {
    let is_partitioned = !ctx.plan.partition_columns.is_empty();
    // Partitioned parents are never UNLOGGED — PG18 rejects it and pre-PG18
    // ignored it (children carry UNLOGGED via `build_partition_child_ddl_pair`).
    let create_kw = if ctx.logged || is_partitioned {
        "CREATE TABLE"
    } else {
        "CREATE UNLOGGED TABLE"
    };
    if is_partitioned {
        let scratch_name = format!(
            "__reflex_pt_shape_{}",
            safe_identifier(split_qualified_name(ctx.view_name).1)
        );
        client
            .update(
                &format!(
                    "CREATE TEMP TABLE {} AS {} WITH NO DATA",
                    scratch_name, ctx.sql
                ),
                None,
                &[],
            )
            .unwrap_or_report();
        let col_defs: Vec<String> = client
            .select(
                "SELECT attname::text AS name, \
                        format_type(atttypid, atttypmod) AS pg_type \
                 FROM pg_attribute \
                 WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped \
                 ORDER BY attnum",
                None,
                &[unsafe {
                    DatumWithOid::new(scratch_name.clone(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .filter_map(|r| {
                let n = r.get_by_name::<&str, _>("name").ok()??.to_string();
                let t = r.get_by_name::<&str, _>("pg_type").ok()??.to_string();
                Some(format!("\"{}\" {}", n, t))
            })
            .collect();
        client
            .update(&format!("DROP TABLE {}", scratch_name), None, &[])
            .unwrap_or_report();
        // Root table partitions only on the first column; deeper levels are
        // handled by building child nodes below (lines 884-896).
        let root_partition_cols = if ctx.plan.partition_columns.len() > 1 {
            vec![ctx.plan.partition_columns[0].clone()]
        } else {
            ctx.plan.partition_columns.clone()
        };
        let part_clause = crate::partition::build_partition_by_clause(
            &ctx.plan.partition_strategy,
            &root_partition_cols,
        );
        client
            .update(
                &format!(
                    "{} IF NOT EXISTS {} ({}) {}",
                    create_kw,
                    quote_identifier(ctx.view_name),
                    col_defs.join(", "),
                    part_clause
                ),
                None,
                &[],
            )
            .unwrap_or_report();
        if let Ok(anchor) = crate::partition::resolve_anchor_source(
            client,
            &ctx.plan.partition_columns[0],
            &ctx.real_source_names,
        ) {
            let (_, anchor_root_bare) = split_qualified_name(&anchor);
            let mut nodes = crate::partition::list_partition_tree(client, &anchor);
            if let Some(depth) = ctx.resolved_partition_depth {
                nodes = crate::partition::truncate_partition_tree(nodes, depth as usize);
            }
            for node in &nodes {
                let ddl = crate::partition::build_partition_node_ddl_pair(
                    ctx.view_name,
                    node,
                    anchor_root_bare,
                    !ctx.logged,
                );
                client.update(&ddl.tgt_ddl, None, &[]).unwrap_or_report();
            }
            crate::partition::refresh_source_snapshot(client, &anchor);
        }
        client
            .update(
                &format!(
                    "INSERT INTO {} {}",
                    quote_identifier(ctx.view_name),
                    ctx.sql
                ),
                None,
                &[],
            )
            .unwrap_or_report();
    } else {
        client
            .update(
                &format!(
                    "{} {} AS {}",
                    create_kw,
                    quote_identifier(ctx.view_name),
                    ctx.sql
                ),
                None,
                &[],
            )
            .unwrap_or_report();
    }
    client
        .update(
            &format!("ANALYZE {}", quote_identifier(ctx.view_name)),
            None,
            &[],
        )
        .unwrap_or_report();

    if !ctx.resolved_unique_columns.is_empty() {
        let bare_view = split_qualified_name(ctx.view_name).1;
        let uk_cols: Vec<String> = ctx
            .resolved_unique_columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect();
        client
            .update(
                &format!(
                    "CREATE UNIQUE INDEX IF NOT EXISTS \"__reflex_uk_{}\" ON {} ({}) NULLS NOT DISTINCT",
                    bare_view,
                    quote_identifier(ctx.view_name),
                    uk_cols.join(", ")
                ),
                None,
                &[],
            )
            .unwrap_or_report();
    }

    for source in &ctx.froms {
        if source.starts_with('<') {
            continue;
        }
        for ddl in build_passthrough_scratch_ddls(ctx.view_name, source) {
            client.update(&ddl, None, &[]).unwrap_or_report();
        }
    }
}

/// Aggregate materialization: catalog type discovery, intermediate + target + delta-scratch
/// DDL, partition children. Pushes intermediate table name onto `ctx.unlogged_tables`.
fn materialize_aggregate(client: &mut pgrx::spi::SpiClient<'_>, ctx: &mut BuildContext) {
    let (mut column_types, not_null_cols, per_source_cols) =
        query_column_types_from_catalog_with_per_source(client, &ctx.froms);
    ctx.plan.optimize_not_null_sums(&not_null_cols);
    for (source, cols) in ctx.plan.imv_relevant_columns.iter_mut() {
        if let Some(actual) = per_source_cols.get(source) {
            cols.retain(|c| actual.contains(c.as_str()));
        } else if source.starts_with('<') {
            cols.clear();
        }
    }
    ctx.plan.imv_relevant_columns.retain(|_, v| !v.is_empty());

    let base_q_for_types = generate_base_query(&ctx.analysis, &ctx.plan);
    augment_column_types_from_query(&base_q_for_types, &mut column_types);
    augment_column_types_from_query(ctx.sql, &mut column_types);

    for ic in &mut ctx.plan.intermediate_columns {
        if ic.source_aggregate == "SUM" {
            let base_type = resolve_column_type(&ic.name, &column_types, "").to_uppercase();
            if base_type == "DOUBLE PRECISION" {
                ic.pg_type = "DOUBLE PRECISION".to_string();
            }
        }
    }

    for ic in &mut ctx.plan.intermediate_columns {
        if (ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX")
            && ic.pg_type.eq_ignore_ascii_case("NUMERIC")
        {
            let resolved = resolve_column_type(&ic.source_arg, &column_types, "");
            if !resolved.is_empty() && !resolved.eq_ignore_ascii_case("NUMERIC") {
                ic.pg_type = resolved;
            }
        }
    }

    for mapping in &mut ctx.plan.end_query_mappings {
        if mapping.cast_type.is_none() {
            let discovered = resolve_column_type(&mapping.output_alias, &column_types, "");
            if !discovered.is_empty() {
                let default_type = match mapping.aggregate_type.as_str() {
                    "SUM" | "AVG" | "DERIVED" => "NUMERIC",
                    "COUNT" => "BIGINT",
                    "BOOL_OR" => "BOOLEAN",
                    _ => "",
                };
                if !default_type.is_empty() && discovered.to_uppercase() != default_type {
                    mapping.cast_type = Some(discovered);
                }
            }
        }
    }

    if let Some(ddl) =
        build_intermediate_table_ddl(ctx.view_name, &ctx.plan, &column_types, ctx.logged)
    {
        let tbl = intermediate_table_name(ctx.view_name);
        client.update(&ddl, None, &[]).unwrap_or_report();
        ctx.unlogged_tables.push(tbl.clone());
        if let Some(scratch_ddl) =
            build_delta_scratch_table_ddl(ctx.view_name, &ctx.plan, &column_types)
        {
            client.update(&scratch_ddl, None, &[]).unwrap_or_report();
        }
    }

    let target_ddl = build_target_table_ddl(ctx.view_name, &ctx.plan, &column_types, ctx.logged);
    client.update(&target_ddl, None, &[]).unwrap_or_report();

    if !ctx.plan.partition_columns.is_empty() {
        match crate::partition::resolve_anchor_source(
            client,
            &ctx.plan.partition_columns[0],
            &ctx.real_source_names,
        ) {
            Ok(anchor) => {
                let (_, anchor_root_bare) = split_qualified_name(&anchor);
                let mut nodes = crate::partition::list_partition_tree(client, &anchor);
                if let Some(depth) = ctx.resolved_partition_depth {
                    nodes = crate::partition::truncate_partition_tree(nodes, depth as usize);
                }
                info!(
                    "pg_reflex: creating {} partition nodes for '{}' (anchor='{}')",
                    nodes.len(),
                    ctx.view_name,
                    anchor
                );
                for node in &nodes {
                    let ddl = crate::partition::build_partition_node_ddl_pair(
                        ctx.view_name,
                        node,
                        anchor_root_bare,
                        !ctx.logged,
                    );
                    client.update(&ddl.int_ddl, None, &[]).unwrap_or_report();
                    client.update(&ddl.tgt_ddl, None, &[]).unwrap_or_report();
                }
                crate::partition::refresh_source_snapshot(client, &anchor);
            }
            Err(e) => {
                warning!(
                    "pg_reflex: could not resolve anchor for '{}' partition children: {}",
                    ctx.view_name,
                    e
                );
            }
        }
    }
}

/// Install consolidated triggers on every real source of the IMV. Skips
/// `<subquery:...>`/`<function:...>` placeholders, ignored sources, and
/// materialized-view sources. Upgrades existing triggers to deferred when
/// any deferred IMV depends on the source.
fn install_source_triggers(client: &mut pgrx::spi::SpiClient<'_>, ctx: &BuildContext) {
    for source in &ctx.froms {
        if source.starts_with("<subquery:") || source.starts_with("<function:") {
            warning!(
                "pg_reflex: source '{}' for '{}' is a subquery — \
                 triggers are created on the underlying tables inside the subquery, \
                 but the subquery itself is re-executed on each delta",
                source,
                ctx.view_name
            );
            continue;
        }
        if source.starts_with('<') {
            continue;
        }

        let (_, source_bare) = split_qualified_name(source);
        if ctx
            .ignore_sources
            .iter()
            .any(|s| s == source || s == source_bare)
        {
            info!(
                "pg_reflex: skipping trigger install on source '{}' for IMV '{}' (ignored)",
                source, ctx.view_name
            );
            continue;
        }

        let is_matview = client
            .select(
                "SELECT 1 FROM pg_class WHERE oid = to_regclass($1) AND relkind = 'm'",
                None,
                &[unsafe {
                    DatumWithOid::new(source.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .next()
            .is_some();

        if is_matview {
            warning!(
                "pg_reflex: source '{}' is a materialized view — triggers skipped. \
                 Use SELECT refresh_imv_depending_on('{}') after REFRESH MATERIALIZED VIEW.",
                source,
                source
            );
            continue;
        }

        let safe_source = crate::query_decomposer::sanitized_source_suffix(source);
        let trig_exists = client
            .select(
                &format!(
                    "SELECT 1 FROM pg_trigger WHERE tgname = '__reflex_trigger_ins_on_{}'",
                    safe_source
                ),
                None,
                &[],
            )
            .unwrap_or_report()
            .next()
            .is_some();

        if ctx.deferred {
            ensure_staging_matches_source(client, source);
            let staging_ddl = build_staging_table_ddl(source);
            client.update(&staging_ddl, None, &[]).unwrap_or_report();
        }

        if !trig_exists {
            let has_any_deferred = ctx.deferred
                || {
                    let check = client
                    .select(
                        &format!(
                            "SELECT 1 FROM public.__reflex_ivm_reference \
                             WHERE '{}' = ANY(depends_on) AND refresh_mode = 'DEFERRED' AND enabled = TRUE",
                            source.replace("'", "''")
                        ),
                        None,
                        &[],
                    )
                    .unwrap_or_report()
                    .next()
                    .is_some();
                    check
                };

            if has_any_deferred {
                let cols = fetch_source_columns(client, source);
                for ddl in build_deferred_trigger_ddls(source, &cols) {
                    client.update(&ddl, None, &[]).unwrap_or_report();
                }
            } else {
                for ddl in build_trigger_ddls(source) {
                    client.update(&ddl, None, &[]).unwrap_or_report();
                }
            }
        } else if ctx.deferred {
            let cols = fetch_source_columns(client, source);
            for ddl in build_deferred_trigger_ddls(source, &cols) {
                client.update(&ddl, None, &[]).unwrap_or_report();
            }
        }
    }
}

/// Read the ordered list of live column names for `source_table` from
/// `pg_attribute`.  Used at deferred-trigger DDL time so the trigger body
/// can name the columns it stages instead of relying on `SELECT *`'s
/// positional binding (which broke when a per-source staging delta
/// outlived a source DROP+CREATE that reordered columns — see the 1.6.2
/// regression in `pg_test_deferred.rs`).
fn fetch_source_columns(client: &pgrx::spi::SpiClient<'_>, source_table: &str) -> Vec<String> {
    client
        .select(
            "SELECT attname::text AS name \
             FROM pg_attribute \
             WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped \
             ORDER BY attnum",
            None,
            &[unsafe {
                DatumWithOid::new(
                    source_table.to_string(),
                    PgBuiltInOids::TEXTOID.oid().value(),
                )
            }],
        )
        .unwrap_or_report()
        .filter_map(|row| {
            row.get_by_name::<&str, _>("name")
                .ok()
                .flatten()
                .map(|s| s.to_string())
        })
        .collect()
}

/// Guard the create_ivm path against a pre-existing staging delta whose
/// column NAMES no longer agree with the source's live shape.  Two cases:
///
///   * staging column set == source column set (any order):
///     `LIKE source` would produce the same column NAMES, so the named-
///     column INSERT emitted by `build_deferred_trigger_ddls` will route
///     rows correctly.  Leave staging in place (it may carry pending
///     deferred deltas for other DEFERRED IMVs on this source).
///
///   * staging column set differs from source:
///     - if staging is empty → drop + let `build_staging_table_ddl`
///       recreate it from the current source shape.
///     - if staging holds rows → refuse with a clear error directing the
///       user to flush.  Dropping silently would lose other IMVs' staged
///       work; quietly proceeding would mean the new named-column INSERT
///       references columns that don't exist on staging.
fn ensure_staging_matches_source(client: &mut pgrx::spi::SpiClient<'_>, source_table: &str) {
    let staging_qual = crate::query_decomposer::staging_delta_table_name(source_table);
    let staging_exists = client
        .select(
            "SELECT 1 FROM pg_class WHERE oid = to_regclass($1)",
            None,
            &[unsafe {
                DatumWithOid::new(
                    staging_qual.replace('"', ""),
                    PgBuiltInOids::TEXTOID.oid().value(),
                )
            }],
        )
        .unwrap_or_report()
        .next()
        .is_some();
    if !staging_exists {
        return;
    }

    let source_cols: std::collections::HashSet<String> = fetch_source_columns(client, source_table)
        .into_iter()
        .collect();
    let staging_cols: std::collections::HashSet<String> = client
        .select(
            "SELECT attname::text AS name \
             FROM pg_attribute \
             WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped \
               AND attname <> '__reflex_op' \
             ORDER BY attnum",
            None,
            &[unsafe {
                DatumWithOid::new(
                    staging_qual.replace('"', ""),
                    PgBuiltInOids::TEXTOID.oid().value(),
                )
            }],
        )
        .unwrap_or_report()
        .filter_map(|row| {
            row.get_by_name::<&str, _>("name")
                .ok()
                .flatten()
                .map(|s| s.to_string())
        })
        .collect();

    if source_cols == staging_cols {
        return;
    }

    let has_rows = client
        .select(
            &format!("SELECT 1 FROM {} LIMIT 1", staging_qual),
            None,
            &[],
        )
        .unwrap_or_report()
        .next()
        .is_some();
    if has_rows {
        let added: Vec<&String> = source_cols.difference(&staging_cols).collect();
        let dropped: Vec<&String> = staging_cols.difference(&source_cols).collect();
        pgrx::error!(
            "pg_reflex: staging delta table {} is out of sync with source '{}' \
             (added columns: {:?}, removed columns: {:?}) and has pending deferred rows. \
             Run SELECT reflex_flush_deferred('{}') first (or DROP it manually if the rows \
             are no longer needed), then retry create_reflex_ivm.",
            staging_qual,
            source_table,
            added,
            dropped,
            source_table
        );
    }
    // CASCADE because reflex_flush_deferred installs per-session TEMP VIEWs
    // (`__reflex_new_<src>`, `__reflex_old_<src>`) against the staging delta;
    // they outlive the flush call and would otherwise block the DROP.
    client
        .update(&format!("DROP TABLE {} CASCADE", staging_qual), None, &[])
        .unwrap_or_report();
}

/// When the IMV uses deferred refresh, ensure the deferred-flush
/// infrastructure (function + per-source helpers) exists.
fn install_deferred_flush_if_needed(client: &mut pgrx::spi::SpiClient<'_>, ctx: &BuildContext) {
    if ctx.deferred {
        for ddl in build_deferred_flush_ddl() {
            client.update(&ddl, None, &[]).unwrap_or_report();
        }
    }
}

/// Source-side indexes on GROUP BY columns for MIN/MAX recompute performance.
/// Skips IMV sources (the IMV's intermediate has its own indexes) and
/// `<subquery>` placeholders. Only emits indexes for columns that exist on
/// the source table.
fn install_min_max_indexes(client: &mut pgrx::spi::SpiClient<'_>, ctx: &BuildContext) {
    let has_min_max = ctx
        .plan
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
        let (src_schema, src_name) = split_qualified_name(source);
        let src_schema_str = src_schema.unwrap_or("public");
        let source_cols: Vec<String> = client
            .select(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
                None,
                &[
                    unsafe { DatumWithOid::new(src_schema_str.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(src_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                ],
            )
            .unwrap_or_report()
            .filter_map(|row| row.get_by_name::<&str, _>("column_name").unwrap_or(None).map(|s| s.to_lowercase()))
            .collect();

        let idx_cols: Vec<String> = ctx
            .plan
            .group_by_columns
            .iter()
            .map(|c| normalized_column_name(c))
            .filter(|c| source_cols.contains(c))
            .map(|c| format!("\"{}\"", c))
            .collect();

        if idx_cols.is_empty() {
            continue;
        }
        let safe_src = source.replace('.', "_");
        let bare_view = split_qualified_name(ctx.view_name).1;
        let idx_name = safe_identifier(&format!("__reflex_idx_{}_{}", bare_view, safe_src));
        let ddl = format!(
            "CREATE INDEX IF NOT EXISTS \"{}\" ON {} ({})",
            idx_name,
            source,
            idx_cols.join(", ")
        );
        client.update(&ddl, None, &[]).unwrap_or_report();
    }
}

/// Returns true when `view` already has an index whose leading key columns are
/// exactly `cols` (in order) — i.e. an existing index already serves a keyed
/// lookup on `cols`. `cols` are bare (unquoted, normalized) column names.
///
/// `indkey` is a 0-indexed `int2vector`; comparing `indkey[ord-1]` (per desired
/// column at 1-based ordinal `ord`) against the column's `attnum` avoids the
/// fragile `int2vector::int2[]` cast and any array lower-bound mismatch.
fn index_covers_prefix(client: &mut SpiClient<'_>, view: &str, cols: &[String]) -> bool {
    if cols.is_empty() {
        return true;
    }
    let cols_literal = cols
        .iter()
        .map(|c| format!("'{}'", c.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    let n = cols.len();
    let v = view.replace('\'', "''");
    let sql = format!(
        "SELECT EXISTS ( \
            SELECT 1 FROM pg_index i \
            WHERE i.indrelid = '{v}'::regclass \
              AND i.indnkeyatts >= {n} \
              AND NOT EXISTS ( \
                  SELECT 1 FROM unnest(ARRAY[{cols_literal}]::text[]) \
                       WITH ORDINALITY t(cname, ord) \
                  WHERE i.indkey[ord - 1] IS DISTINCT FROM ( \
                      SELECT a.attnum FROM pg_attribute a \
                      WHERE a.attrelid = i.indrelid AND a.attname = t.cname \
                            AND NOT a.attisdropped ) ) ) AS covered",
        v = v,
        n = n,
        cols_literal = cols_literal,
    );
    client
        .select(&sql, None, &[])
        .unwrap_or_report()
        .next()
        .and_then(|r| r.get_by_name::<bool, _>("covered").unwrap_or(None))
        .unwrap_or(false)
}

/// Auto-create an index on the IMV for each passthrough secondary's join-key
/// columns, so the keyed secondary DELETE (audit #3) is index-served (per-leaf
/// on a partitioned IMV). Skipped when an existing index already covers the
/// columns as a leading prefix — including the `__reflex_uk_*` unique key, which
/// covers the primary source's full key, so the key-owner mapping is naturally
/// skipped and only genuine secondaries get a new index.
fn install_secondary_key_indexes(client: &mut SpiClient<'_>, ctx: &BuildContext) {
    if !ctx.plan.is_passthrough {
        return;
    }
    let bare_view = split_qualified_name(ctx.view_name).1;
    for (source, mappings) in &ctx.plan.passthrough_key_mappings {
        if mappings.is_empty() {
            continue;
        }
        // Mapping target names are ALREADY normalized (case-preserved, unquoted)
        // — use them as-is. Re-normalizing an unquoted mixed-case name would
        // lowercase it (e.g. "Id" -> id) and break both the coverage probe and
        // the CREATE INDEX against a case-preserved column.
        let cols: Vec<String> = mappings.iter().map(|(t, _)| t.clone()).collect();
        if index_covers_prefix(client, ctx.view_name, &cols) {
            continue;
        }
        let cols_q = cols
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");
        let safe_src = source.replace('.', "_");
        let idx_name = safe_identifier(&format!("__reflex_skidx_{}_{}", bare_view, safe_src));
        let ddl = format!(
            "CREATE INDEX IF NOT EXISTS \"{}\" ON {} ({})",
            idx_name, ctx.view_name, cols_q
        );
        client.update(&ddl, None, &[]).unwrap_or_report();
    }
}

/// PS-3 per-node verdict: TRUE iff the node has at least one real source and
/// *every* real source is a materialized view. A real source is an entry of
/// `ctx.froms` that is neither a `<subquery:>` / `<function:>` placeholder nor an
/// ignored source. Such a node cannot self-maintain — PG fires no trigger on a
/// matview — so it is a snapshot frozen at create time. A node with even one
/// triggerable real source (a plain table or a sub-IMV table) is maintainable
/// via it and returns false. This aggregates the per-source matview probe in
/// `install_source_triggers` to a single per-node decision.
fn all_real_sources_are_matviews(client: &SpiClient<'_>, ctx: &BuildContext) -> bool {
    let mut saw_real_source = false;
    for source in &ctx.froms {
        if source.starts_with('<') {
            continue;
        }
        let (_, source_bare) = split_qualified_name(source);
        if ctx
            .ignore_sources
            .iter()
            .any(|s| s == source || s == source_bare)
        {
            continue;
        }
        saw_real_source = true;
        let is_matview = client
            .select(
                "SELECT 1 FROM pg_class WHERE oid = to_regclass($1) AND relkind = 'm'",
                None,
                &[unsafe {
                    DatumWithOid::new(source.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .next()
            .is_some();
        if !is_matview {
            return false;
        }
    }
    saw_real_source
}

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
    let index_columns: Vec<String> = ctx
        .plan
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
    let real_sources: Vec<&String> = ctx.real_source_names.iter().collect();
    // The per-op early-skip evaluates `where_predicate` against the flat
    // transition table (sql/trigger_pred_check_*.plpgsql.in and the deferred
    // flush run `SELECT 1 FROM <transition> WHERE <where_predicate>`). The
    // transition table is NOT aliased as the source, so the predicate must use
    // bare column names — `imv_relevant_where` is exactly that alias-stripped
    // form. Storing the raw qualified `where_clause` (e.g. `src.amt > 50`)
    // instead errors at maintenance time with "missing FROM-clause entry for
    // table src". Only single-source IMVs carry a predicate, so the lone
    // entry is the full (possibly conservative) stripped predicate; an empty
    // string is coerced to NULL by the registry insert.
    let where_predicate: String = if real_sources.len() <= 1 {
        ctx.analysis
            .imv_relevant_where
            .values()
            .next()
            .cloned()
            .unwrap_or_default()
    } else {
        String::new()
    };
    let ignored_sources_vec: Vec<String> = ctx.ignore_sources.to_vec();
    let max_one_row = !ctx.plan.is_passthrough && ctx.plan.group_by_columns.is_empty();

    // Build create_args JSON for faithful chain rebuild: capture all parameters
    // passed to this create call so reflex_rebuild_chain can reproduce them exactly.
    let mut create_args_parts = Vec::new();

    // unique_columns_str
    let unique_cols_escaped = ctx
        .unique_columns_str
        .replace('\\', "\\\\")
        .replace('"', "\\\"");
    create_args_parts.push(format!(
        r#""unique_columns_str": "{}""#,
        unique_cols_escaped
    ));

    // storage_mode
    let storage_escaped = ctx.storage_upper.replace('\\', "\\\\").replace('"', "\\\"");
    create_args_parts.push(format!(r#""storage_mode": "{}""#, storage_escaped));

    // refresh_mode
    let refresh_escaped = ctx.mode_upper.replace('\\', "\\\\").replace('"', "\\\"");
    create_args_parts.push(format!(r#""refresh_mode": "{}""#, refresh_escaped));

    // topk_k
    if let Some(k) = ctx.topk_k {
        create_args_parts.push(format!(r#""topk_k": {}"#, k));
    }

    // ignore_sources (as array)
    let ignore_json = ignored_sources_vec
        .iter()
        .map(|s| {
            let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
            format!(r#""{}""#, escaped)
        })
        .collect::<Vec<_>>()
        .join(", ");
    create_args_parts.push(format!(r#""ignore_sources": [{}]"#, ignore_json));

    // partition_by (as array)
    let partition_json = ctx
        .partition_by
        .iter()
        .map(|s| {
            let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
            format!(r#""{}""#, escaped)
        })
        .collect::<Vec<_>>()
        .join(", ");
    create_args_parts.push(format!(r#""partition_by": [{}]"#, partition_json));

    // explicit_unpartitioned
    create_args_parts.push(format!(
        r#""explicit_unpartitioned": {}"#,
        ctx.explicit_unpartitioned
    ));

    let create_args_json = format!("{{ {} }}", create_args_parts.join(", "));

    let requires_explicit_refresh = all_real_sources_are_matviews(client, ctx);

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
            partition_depth: ctx.resolved_partition_depth,
            max_one_row,
            create_args: Some(&create_args_json),
            requires_explicit_refresh,
        },
    )
    .unwrap_or_report();

    add_graph_child_links(client, ctx.view_name, &ctx.ivm_froms).unwrap_or_report();

    // Seed the source-partition snapshot for every partitioned source this IMV
    // depends on, capturing the leaf set the IMV was just built over. The
    // partition-attach flush diffs against this baseline; without it an
    // unpartitioned IMV's first flush would see every existing leaf as newly
    // attached and re-apply already-present partitions. The partitioned-IMV path
    // seeds this via its mirror build; this covers unpartitioned IMVs. Idempotent
    // (refresh does DELETE+INSERT).
    for source in &ctx.real_source_names {
        if crate::partition::introspect_partition_descriptor(client, source).is_some() {
            crate::partition::refresh_source_snapshot(client, source);
        }
    }
}

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
    let target_insert = format!("INSERT INTO {} {}", quote_identifier(ctx.view_name), end_q);
    client.update(&target_insert, None, &[]).unwrap_or_report();

    for index_ddl in build_indexes_ddl(ctx.view_name, &ctx.plan) {
        client.update(&index_ddl, None, &[]).unwrap_or_report();
    }

    // Create persistent affected-groups table (avoids DROP+CREATE per trigger fire).
    // Uses UNLOGGED for speed; lost on crash but rebuilt by reflex_reconcile.
    // Co-located in the IMV's schema (1.4.1) so SQL works under any `search_path`.
    if !ctx.plan.group_by_columns.is_empty() || !ctx.plan.distinct_columns.is_empty() {
        let group_cols_csv = ctx
            .plan
            .group_by_columns
            .iter()
            .chain(ctx.plan.distinct_columns.iter())
            .map(|c| format!("\"{}\"", normalized_column_name(c)))
            .collect::<Vec<_>>()
            .join(", ");
        let affected_ref = affected_groups_table_name(ctx.view_name);
        client
            .update(
                &format!(
                    "CREATE UNLOGGED TABLE IF NOT EXISTS {} AS SELECT {} FROM {} WHERE FALSE",
                    affected_ref, group_cols_csv, intermediate_tbl
                ),
                None,
                &[],
            )
            .unwrap_or_report();

        // N1: per-IMV "shrunk groups" capture table — populated post-Sub
        // on UPDATE for top-K MIN/MAX IMVs to scope the forced recompute
        // to groups whose heap actually shrank below K. Provisioned only
        // when the plan has any top-K column; non-top-K IMVs leave it
        // unallocated.
        let has_topk = ctx.plan.intermediate_columns.iter().any(|ic| ic.has_topk());
        if has_topk {
            let shrunk_ref = shrunk_groups_table_name(ctx.view_name);
            client
                .update(
                    &format!(
                        "CREATE UNLOGGED TABLE IF NOT EXISTS {} AS SELECT {} FROM {} WHERE FALSE",
                        shrunk_ref, group_cols_csv, intermediate_tbl
                    ),
                    None,
                    &[],
                )
                .unwrap_or_report();
        }
    }

    // ANALYZE so the query planner has accurate statistics
    client
        .update(&format!("ANALYZE {}", intermediate_tbl), None, &[])
        .unwrap_or_report();
    client
        .update(
            &format!("ANALYZE {}", quote_identifier(ctx.view_name)),
            None,
            &[],
        )
        .unwrap_or_report();

    // Structural NOT-NULL inference. Promote group-by / distinct columns that
    // are *provably* non-NULL from the query (INNER-join equi-keys or catalog
    // NOT-NULL base columns on a non-nullable side), so MERGE maintenance can
    // match keys with `=` instead of `IS NOT DISTINCT FROM` — the index-defeating
    // 405 s yse.ivm_sop_forecast_view regression in 1.4.4. Unlike the former
    // data-probe this never trusts transient null-freeness, so a later NULL on a
    // genuinely nullable / outer-join column cannot silently drop rows
    // (docs/fuzz-findings.md findings #1 and #3).
    let probed_nn = infer_not_null_columns(client, &ctx.plan, Some(&ctx.analysis));
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

/// Returns the first GROUP BY key that is not projected *bare* in the SELECT
/// list (it appears only inside an expression, e.g. `GROUP BY a.sx` with
/// `SELECT COALESCE(a.sx, 0)`, or is omitted from SELECT entirely).
///
/// Such a key becomes an intermediate-table column but has NO column in the
/// result (target) table, since the target carries only the projected outputs.
/// Every target-side operation — the target composite index and the
/// target-sync row matching (`target_group_columns`) — then references a column
/// that does not exist, surfacing as a cryptic `column "<key>" does not exist`
/// at create or a crash on the first incremental maintenance. Reject up front
/// with an actionable message instead. (A query that groups *by the expression*
/// — `GROUP BY DATE_TRUNC('month', ts)` projecting `DATE_TRUNC('month', ts)` —
/// is fine: the key is the expression and it is projected.)
fn first_unprojected_group_key(analysis: &crate::sql_analyzer::SqlAnalysis) -> Option<String> {
    for gb in &analysis.group_by_columns {
        let gb_norm = crate::query_decomposer::normalized_column_name(gb);
        let projected = analysis.select_columns.iter().any(|sc| {
            sc.is_passthrough
                && crate::query_decomposer::normalized_column_name(&sc.expr_sql) == gb_norm
        });
        if !projected {
            return Some(gb.clone());
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn create_reflex_ivm_impl(
    view_name: &str,
    sql: &str,
    unique_columns_str: &str,
    if_not_exists: bool,
    storage_mode: &str,
    refresh_mode: &str,
    topk_k: Option<usize>,
    ignore_sources: &[String],
    partition_by: &[String],
    explicit_unpartitioned: bool,
) -> &'static str {
    create_reflex_ivm_impl_with_materialization(
        view_name,
        sql,
        unique_columns_str,
        if_not_exists,
        storage_mode,
        refresh_mode,
        topk_k,
        ignore_sources,
        partition_by,
        false, // top-level call: zero-overhead VIEW wrappers are still fine
        explicit_unpartitioned,
    )
}

/// Guardrail for the search_path footgun behind "intermediate tables sometimes
/// land in public". A *bare* IMV name created under a non-`public` search_path
/// puts the IMV's maintenance tables (intermediate / scratch / affected) in that
/// head schema, but the generated maintenance SQL references them by bare name —
/// so any DML that fires the source triggers from a session whose search_path
/// excludes that schema cannot resolve them (it errors, or silently hits a
/// `public` homonym). Schema-qualified IMV names are immune. We can't safely
/// auto-qualify (it would change the catalog key every other entry point looks
/// up by) nor reject (a consistent create+maintain search_path works fine), so
/// we surface the risk with the concrete fix at creation time.
fn warn_on_bare_name_under_nonpublic_search_path(view_name: &str) {
    if canonical_source(view_name).0.is_some() {
        return; // already schema-qualified — search_path-independent
    }
    let current_schema: Option<String> = Spi::get_one::<String>("SELECT current_schema()::text")
        .ok()
        .flatten();
    if let Some(schema) = current_schema {
        if schema != "public" {
            warning!(
                "pg_reflex: IMV '{view_name}' created with a bare name under search_path schema \
                 '{schema}'. Its maintenance tables (intermediate/scratch/affected) live in \
                 '{schema}', but the generated maintenance SQL references them unqualified — DML \
                 firing the source triggers from a session whose search_path excludes '{schema}' \
                 will fail to find them. Create with a schema-qualified name \
                 ('{schema}.{view_name}') to make maintenance search_path-independent."
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn create_reflex_ivm_impl_with_materialization(
    view_name: &str,
    sql: &str,
    unique_columns_str: &str,
    if_not_exists: bool,
    storage_mode: &str,
    refresh_mode: &str,
    topk_k: Option<usize>,
    ignore_sources: &[String],
    partition_by: &[String],
    materialize_as_table: bool,
    explicit_unpartitioned: bool,
) -> &'static str {
    let parsed = match validate_and_parse_inputs(view_name, sql, storage_mode, refresh_mode) {
        Ok(p) => p,
        Err(msg) => return msg,
    };

    warn_on_bare_name_under_nonpublic_search_path(view_name);

    // The `unique_columns` argument may carry per-CTE keys after the outer key
    // (`'<outer> ; <cte alias> : <cols> ; ...'`). Strip them so only the outer
    // key reaches the non-CTE paths; the map is consumed by CTE decomposition.
    let (outer_unique_columns, cte_unique_columns) = parse_unique_columns_spec(unique_columns_str);
    let unique_columns_str = outer_unique_columns.as_str();

    if let Some(gb) = first_unprojected_group_key(&parsed.analysis) {
        return crate::reflex_reject(&format!(
            "GROUP BY key '{gb}' is not projected in the SELECT list. \
             An aggregate reflex IMV requires every GROUP BY column to appear bare in \
             SELECT — a key used only inside an expression (e.g. COALESCE({gb}, 0)) or \
             omitted from SELECT has no column in the result table, which the target \
             index and incremental refresh both rely on. Fix: add '{gb}' to the SELECT \
             list, or move the wrapping expression into a passthrough outer layer \
             (an outer SELECT over a CTE that projects the bare key + aggregates)."
        ));
    }

    // Build the decomposition context once, covering all fields needed by the three
    // decomposition functions. This ensures every path has access to every field
    // (including sql and cte_unique_columns, even if some functions don't use them).
    let dctx = DecomposeCtx {
        view_name,
        sql,
        unique_columns_str,
        cte_unique_columns: &cte_unique_columns,
        storage_mode,
        refresh_mode,
        topk_k,
        ignore_sources,
        partition_by,
        parsed: &parsed,
        materialize_as_table,
    };

    if let Some(result) = try_decompose_set_op(&dctx) {
        return result;
    }

    if let Some(result) = try_decompose_ctes(&dctx) {
        return result;
    }

    if let Some(result) = try_decompose_distinct_on(&dctx) {
        return result;
    }

    if let Some(result) = try_decompose_window(
        view_name,
        sql,
        unique_columns_str,
        storage_mode,
        refresh_mode,
        topk_k,
        ignore_sources,
        partition_by,
        &parsed,
    ) {
        return result;
    }

    // Reject subqueries with aggregation in FROM — the trigger replaces the inner table
    // with the transition table, so inner aggregations would only see delta rows.
    let has_subquery_with_agg = parsed
        .analysis
        .sources
        .iter()
        .any(|s| s.starts_with("<subquery:"))
        && parsed
            .analysis
            .from_clause_sql
            .to_uppercase()
            .contains("GROUP BY");
    if has_subquery_with_agg {
        return crate::reflex_reject("Subqueries with aggregation in FROM are not supported. \
                Use a CTE (WITH clause) instead — pg_reflex decomposes CTEs into sub-IMVs automatically.");
    }

    let ParsedInputs {
        logged,
        deferred,
        storage_upper,
        mode_upper,
        parsed_sql: _,
        mut analysis,
    } = parsed;

    // Resolve every bare source against the current `search_path` so the
    // identifiers stored in `__reflex_ivm_reference.depends_on`, baked into the
    // generated trigger bodies, and pushed into `__reflex_deferred_pending`
    // all carry the qualified `schema.table` form of the relation the trigger
    // is actually attached to. Without this, a bare `FROM foo` whose
    // search_path-resolved schema is e.g. `alp.foo` flows through unqualified;
    // `reflex_flush_deferred` (trigger.rs ~2885) later splits the schemaless
    // name, falls back to `nspname='public'`, and projects columns from a
    // public-side homonym, crashing the COMMIT-time flush.
    canonicalize_analysis_sources(&mut analysis);

    // COUNT(DISTINCT) mixed-with-other-aggregates check — must reject before
    // building the ctx.
    let has_cd = analysis.select_columns.iter().any(|c| {
        matches!(
            c.aggregate,
            Some(crate::sql_analyzer::AggregateKind::CountDistinct)
        )
    });
    let has_other_agg = analysis.select_columns.iter().any(|c| {
        matches!(c.aggregate, Some(ref k) if !matches!(k,
            crate::sql_analyzer::AggregateKind::CountDistinct))
    });
    if has_cd && has_other_agg {
        return crate::reflex_reject("COUNT(DISTINCT col) cannot be mixed with other aggregates in the same query. \
                Use a CTE to separate them: WITH cd AS (SELECT grp, COUNT(DISTINCT col) ...) SELECT ...");
    }

    let froms = analysis.sources.clone();
    let real_source_names: Vec<String> = froms
        .iter()
        .filter(|s| !s.starts_with('<'))
        .cloned()
        .collect();
    let is_join_query = real_source_names.len() > 1;

    let plan = if topk_k.is_some() {
        plan_aggregation_with_topk(&analysis, topk_k)
    } else {
        plan_aggregation(&analysis)
    };

    let mut ctx = BuildContext {
        view_name,
        sql,
        unique_columns_str,
        if_not_exists,
        topk_k,
        ignore_sources,
        partition_by,
        explicit_unpartitioned,
        logged,
        deferred,
        storage_upper,
        mode_upper,
        analysis,
        plan,
        froms,
        real_source_names,
        is_join_query,
        resolved_unique_columns: Vec::new(),
        resolved_partition_cols: Vec::new(),
        resolved_strategy: String::new(),
        resolved_partition_depth: None,
        ivm_froms: Vec::new(),
        depth: 0,
        unlogged_tables: Vec::new(),
    };

    resolve_unique_columns(&mut ctx);

    // 1.5.3 (plans/partitioning_3.md §4) — populate per-source
    // partition_join_paths fragments.  Skipped here for the unpartitioned
    // / passthrough cases; the real computation runs below once
    // `resolved_partition_cols` is known.  (We can't run it earlier than
    // build_source_join_keys, and we can't run it later than the catalog
    // insert that persists `plan.aggregations`.)  This call is a no-op
    // until the partition path resolves the partition column.
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
        install_secondary_key_indexes(client, &ctx);

        persist_metadata(client, &ctx);

        initial_aggregate_materialization(client, &mut ctx);
    });

    info!("pg_reflex: created IMV '{}'", view_name);
    "CREATE REFLEX INCREMENTAL VIEW"
}

mod admin;
mod decompose;
mod soundness;

pub(crate) use admin::*;
pub(crate) use decompose::*;
pub(crate) use soundness::*;

#[cfg(test)]
#[path = "../tests/unit_create_ivm.rs"]
mod tests;
