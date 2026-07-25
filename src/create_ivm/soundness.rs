use super::*;
use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

use crate::query_decomposer::{
    bare_column_name, canonical_source, format_pg_text_array_literal, normalized_column_name,
};
use crate::validate_view_name;

/// Resolve every real-table entry in `analysis.sources` (and the matching
/// `analysis.table_aliases` values) to its `schema.table` form via the
/// current session's `search_path`. Synthetic markers (`<subquery:…>`,
/// `<function:…>`, `<…>`) are left untouched. Entries that do not resolve
/// (typo, race against a DROP, or already passing through as a CTE label) are
/// kept verbatim — downstream catalog lookups will surface a clear error in
/// that case rather than this helper guessing.
///
/// Sources that resolve to the `public` schema are intentionally left bare:
/// the legacy code path's `unwrap_or("public")` fallback (e.g. trigger.rs's
/// `nspname=$1` lookup) lands on the same relation, so there is nothing for
/// canonicalization to fix and rewriting would churn identifier suffixes
/// across already-deployed installs. The bug class only manifests when a
/// non-`public` source shares a bare name with a `public` homonym — there
/// the canonical form keeps DDL-time and trigger-fire-time in agreement.
///
/// The IMV's persistent identity for a source (depends_on, trigger-body slot,
/// pending-table value) is the canonical form returned here.
pub(crate) fn canonicalize_analysis_sources(analysis: &mut crate::sql_analyzer::SqlAnalysis) {
    use std::collections::HashMap;
    let mut resolutions: HashMap<String, String> = HashMap::new();

    Spi::connect(|client| {
        for raw in &analysis.sources {
            if raw.starts_with('<') || resolutions.contains_key(raw) {
                continue;
            }
            let qualified: Option<String> = client
                .select(
                    "SELECT n.nspname::TEXT || '.' || c.relname::TEXT AS q \
                     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE c.oid = to_regclass($1)",
                    None,
                    &[unsafe {
                        DatumWithOid::new(raw.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    }],
                )
                .ok()
                .and_then(|mut it| it.next())
                .and_then(|row| {
                    row.get_by_name::<&str, _>("q")
                        .ok()
                        .flatten()
                        .map(|s| s.to_string())
                });
            if let Some(q) = qualified {
                if q.starts_with("public.") {
                    continue;
                }
                if q != *raw {
                    resolutions.insert(raw.clone(), q);
                }
            }
        }
    });

    if resolutions.is_empty() {
        return;
    }

    for source in analysis.sources.iter_mut() {
        if let Some(q) = resolutions.get(source) {
            *source = q.clone();
        }
    }
    for table in analysis.table_aliases.values_mut() {
        if let Some(q) = resolutions.get(table) {
            *table = q.clone();
        }
    }
}

/// Build per-source-table column mappings for passthrough DELETE/UPDATE.
///
/// For the "key owner" table (whose columns directly match the key), mapping is 1:1.
/// For secondary (joined) tables, the mapping is derived from JOIN conditions:
/// e.g., `ON s.product_id = p.id` maps target "product_id" → source "id" for the products table.
pub(crate) fn build_passthrough_key_mappings(
    plan: &mut crate::aggregation::AggregationPlan,
    key_columns: &[String],
    sources: &[&String],
    analysis: &crate::sql_analyzer::SqlAnalysis,
) {
    use std::collections::HashMap;

    // Build reverse alias map: real table name → alias
    let reverse_aliases: HashMap<&str, &str> = analysis
        .table_aliases
        .iter()
        .map(|(alias, table)| (table.as_str(), alias.as_str()))
        .collect();

    // Build a map from target column name → expr_sql (e.g., "product_id" → "s.product_id")
    let mut target_col_to_expr: HashMap<String, String> = HashMap::new();
    for col in &analysis.select_columns {
        let target_name = col.alias.as_deref().unwrap_or(&col.expr_sql);
        let target_name = normalized_column_name(target_name);
        target_col_to_expr.insert(target_name, col.expr_sql.clone());
    }

    // For each source table, determine if it's the key owner or a secondary table
    for source in sources {
        let source_str = source.as_str();
        let alias = reverse_aliases.get(source_str).copied();

        // Check if this source owns all key columns directly
        // (i.e., for each key column, the SELECT expr references this table)
        let mut is_key_owner = true;
        for kc in key_columns {
            if let Some(expr) = target_col_to_expr.get(kc.as_str()) {
                // expr is like "s.product_id" — check if the table qualifier matches this source
                if let Some(dot_pos) = expr.rfind('.') {
                    let qualifier = &expr[..dot_pos];
                    let matches_alias = alias.is_some_and(|a| a.to_lowercase() == qualifier);
                    let matches_table = bare_column_name(source_str).to_lowercase() == qualifier;
                    if !matches_alias && !matches_table {
                        is_key_owner = false;
                        break;
                    }
                }
                // No qualifier (e.g., single table) — assume it belongs to this source if single source
            } else {
                is_key_owner = false;
                break;
            }
        }

        if is_key_owner {
            // Key owner: target_col == source_col (columns exist directly in this table)
            let mappings: Vec<(String, String)> = key_columns
                .iter()
                .map(|kc| {
                    // Extract the bare source column name from the expression
                    let source_col = target_col_to_expr
                        .get(kc.as_str())
                        .map(|expr| normalized_column_name(expr))
                        .unwrap_or_else(|| kc.clone());
                    (kc.clone(), source_col)
                })
                .collect();
            plan.passthrough_key_mappings
                .insert(source_str.to_string(), mappings);
        } else {
            // Secondary table: derive mapping from JOIN conditions
            let mut mappings: Vec<(String, String)> = Vec::new();
            for join in &analysis.joins {
                if let Some(ref cond) = join.condition_sql {
                    let join_mappings = parse_join_condition_mappings(
                        cond,
                        source_str,
                        &analysis.table_aliases,
                        key_columns,
                        &target_col_to_expr,
                    );
                    mappings.extend(join_mappings);
                }
            }
            if !mappings.is_empty() {
                plan.passthrough_key_mappings
                    .insert(source_str.to_string(), mappings);
            }
            // If no mappings found, this source has no entry → triggers fall back to full refresh
        }
    }
}

/// 1.4.6 — Build the per-source JOIN-key mapping for aggregate plans.
///
/// For each source in the IMV, find JOIN equalities where the source's
/// column equals another table's column AND the other column projects to
/// a GROUP BY column of the intermediate. Then, gate on:
///   * **All** equalities involving the source map to a GROUP BY column
///     (any partial mapping → unsafe, falls back to MERGE).
///   * The mapped source columns cover a UNIQUE key of the source table
///     (PK or unique index). Without a unique key, multiple source rows
///     could share the JOIN-col values and collide in bulk-INSERT.
///
/// Sets `plan.source_join_keys[source] = Vec<(intermediate_col, source_col)>`
/// only when both gates pass. Empty / missing entry → standard MERGE path.
///
/// Skipped for passthrough plans (they use `passthrough_key_mappings`).
pub(crate) fn build_source_join_keys(
    plan: &mut crate::aggregation::AggregationPlan,
    sources: &[&String],
    analysis: &crate::sql_analyzer::SqlAnalysis,
) {
    use std::collections::{HashMap, HashSet};

    if plan.is_passthrough {
        return;
    }

    // 1) Build the lowered set of GROUP BY column names and a target→expr
    //    map so `parse_join_condition_mappings` can recognize "other side"
    //    references whether they're bare (`dem_plan_id`) or qualified
    //    (`sales_simulation.dem_plan_id`).
    let group_by_lowercased: Vec<String> = plan
        .group_by_columns
        .iter()
        .map(|c| normalized_column_name(c).to_lowercase())
        .collect();
    let group_by_set: HashSet<String> = group_by_lowercased.iter().cloned().collect();

    let mut target_col_to_expr: HashMap<String, String> = HashMap::new();
    for col in &analysis.select_columns {
        let target_name = col.alias.as_deref().unwrap_or(&col.expr_sql);
        let target_name = bare_column_name(target_name).to_lowercase();
        if group_by_set.contains(&target_name) {
            target_col_to_expr.insert(target_name, col.expr_sql.to_lowercase());
        }
    }

    // 2) For each source, look only at the JOIN that *introduces* this
    //    source into the FROM clause — i.e. `analysis.joins[i]` with
    //    target_table matching `source`. Other JOINs may reference this
    //    source's columns incidentally (e.g. `pricing` joining on
    //    `demand_planning.assortment_id`), but those say nothing about
    //    THIS source's identity. Counting them inflates the
    //    "all-equalities-mapped" denominator and spuriously refuses the
    //    bulk path.
    let source_bare_lc: std::collections::HashMap<&String, String> = sources
        .iter()
        .map(|s| (*s, bare_column_name(s).to_lowercase()))
        .collect();
    for source in sources {
        let source_str = source.as_str();
        let source_lc = source_str.to_lowercase();
        let source_bare = source_bare_lc.get(source).cloned().unwrap_or_default();

        let mut all_mappings: Vec<(String, String)> = Vec::new();
        let mut all_equalities_count: usize = 0;
        for join in &analysis.joins {
            let target_lc = join.target_table.to_lowercase();
            let target_bare = bare_column_name(&join.target_table).to_lowercase();
            let target_matches = target_lc == source_lc
                || target_bare == source_bare
                || target_lc == source_bare
                || target_bare == source_lc;
            if !target_matches {
                continue;
            }
            if let Some(ref cond) = join.condition_sql {
                let n_eq =
                    count_equalities_involving_source(cond, source_str, &analysis.table_aliases);
                if n_eq == 0 {
                    continue;
                }
                all_equalities_count += n_eq;
                let join_mappings = parse_join_condition_mappings(
                    cond,
                    source_str,
                    &analysis.table_aliases,
                    &group_by_lowercased,
                    &target_col_to_expr,
                );
                all_mappings.extend(join_mappings);
            }
        }

        if all_mappings.is_empty() || all_equalities_count == 0 {
            continue;
        }

        // Dedup mappings before counting coverage.
        all_mappings.sort();
        all_mappings.dedup();

        // Gate A: every JOIN equality involving the source must map to a
        // GROUP BY column. Partial mappings (some equality components map,
        // others don't) leave un-pinned dimensions on the source side, so
        // a single source row's identity doesn't fully determine its
        // intermediate group keys → bulk path unsafe.
        if all_mappings.len() < all_equalities_count {
            continue;
        }

        // Gate B: the source-side columns of the mapping must cover a
        // UNIQUE key on the source table. Otherwise multiple source rows
        // can produce the same mapped values, breaking the "transition row
        // uniquely identifies a slice of intermediate" assumption.
        let source_cols: Vec<String> = all_mappings.iter().map(|(_, sc)| sc.clone()).collect();
        if !source_cols_cover_unique_key(source_str, &source_cols) {
            continue;
        }

        plan.source_join_keys
            .insert(source_str.to_string(), all_mappings);
    }
}

/// Count how many "x = y" equalities in a JOIN ON clause reference
/// `source_table` on one side. Conservative — only counts top-level
/// AND-joined equality predicates (the same shape
/// `parse_join_condition_mappings` understands). Stand-alone helper so
/// the safety gate ("did all equalities map?") can compare against a
/// concrete denominator.
pub(crate) fn count_equalities_involving_source(
    condition: &str,
    source_table: &str,
    table_aliases: &std::collections::HashMap<String, String>,
) -> usize {
    let source_lower = source_table.to_lowercase();
    let source_bare = bare_column_name(source_table).to_lowercase();
    let source_aliases: Vec<String> = table_aliases
        .iter()
        .filter(|(_, table)| table.to_lowercase() == source_lower)
        .map(|(alias, _)| alias.to_lowercase())
        .collect();

    let mut count = 0;
    // Case-insensitive AND split: lowercase first, split once. Avoids the
    // double-iteration footgun that existed in the legacy
    // `parse_join_condition_mappings` (now fixed).
    let condition_lower = condition.to_lowercase();
    for part in condition_lower.split(" and ") {
        let part = part.trim();
        let sides: Vec<&str> = part.splitn(2, '=').collect();
        if sides.len() != 2 {
            continue;
        }
        let left = sides[0].trim();
        let right = sides[1].trim();
        if is_from_table(left, &source_bare, &source_aliases)
            || is_from_table(right, &source_bare, &source_aliases)
        {
            count += 1;
        }
    }
    count
}

/// Query pg_catalog to confirm `cols` cover at least one UNIQUE/PK index
/// on `source`. Returns `true` if any unique index has all of its columns
/// contained in `cols` (case-insensitive). Falls back to `false` on any
/// catalog access error — safe to refuse the bulk path on doubt.
pub(crate) fn source_cols_cover_unique_key(source: &str, cols: &[String]) -> bool {
    let cols_lower: std::collections::HashSet<String> =
        cols.iter().map(|c| c.to_lowercase()).collect();

    let unique_indexes: Vec<Vec<String>> = Spi::connect(|client| {
        client
            .select(
                "SELECT array_agg(a.attname::TEXT ORDER BY k.n) AS cols \
                 FROM pg_index ix \
                 JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(col, n) ON true \
                 JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.col \
                 WHERE ix.indrelid = to_regclass($1) AND ix.indisunique \
                 GROUP BY ix.indexrelid",
                None,
                &[unsafe {
                    DatumWithOid::new(source.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .filter_map(|row| row.get_by_name::<Vec<String>, _>("cols").unwrap_or(None))
            .collect()
    });

    // Check catalog unique indexes first
    if unique_indexes.iter().any(|idx_cols| {
        idx_cols
            .iter()
            .all(|c| cols_lower.contains(&c.to_lowercase()))
    }) {
        return true;
    }

    // Fall back to registered group-by key: equi-join cols must cover the group-by key
    if let Some(group_key) = source_registered_group_key(source) {
        return group_key
            .iter()
            .all(|c| cols_lower.contains(&c.to_lowercase()));
    }

    false
}

/// PRIMARY KEY columns of `source` (lower-cased, in key order). Empty when the
/// table has no primary key or on any catalog error. PK columns are NOT NULL,
/// so they are a true unique key (unlike a nullable UNIQUE index).
pub(crate) fn source_primary_key_columns(source: &str) -> Vec<String> {
    Spi::connect(|client| {
        client
            .select(
                "SELECT a.attname::TEXT AS col \
                 FROM pg_index ix \
                 JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(col, ord) ON true \
                 JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.col \
                 WHERE ix.indrelid = to_regclass($1) AND ix.indisprimary \
                 ORDER BY k.ord",
                None,
                &[unsafe {
                    DatumWithOid::new(source.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .filter_map(|row| {
                row.get_by_name::<&str, _>("col")
                    .unwrap_or(None)
                    .map(|c| c.to_lowercase())
            })
            .collect()
    })
}

/// Collect `source`'s own bare column names that appear on one side of a
/// top-level `x = y` equality in any JOIN ON clause, plus the total count of
/// equalities involving the source. Used to decide whether the source is
/// reached by a to-one join (its equi-join columns cover a unique key).
pub(crate) fn source_equi_join_columns(
    source: &str,
    joins: &[crate::sql_analyzer::JoinInfo],
    table_aliases: &std::collections::HashMap<String, String>,
) -> (Vec<String>, usize) {
    let source_lower = source.to_lowercase();
    let source_bare = bare_column_name(source).to_lowercase();
    let source_aliases: Vec<String> = table_aliases
        .iter()
        .filter(|(_, table)| table.to_lowercase() == source_lower)
        .map(|(alias, _)| alias.to_lowercase())
        .collect();

    let mut cols: Vec<String> = Vec::new();
    let mut n_eq = 0usize;
    for join in joins {
        let Some(ref cond) = join.condition_sql else {
            continue;
        };
        let cond_lower = cond.to_lowercase();
        for part in cond_lower.split(" and ") {
            let part = part.trim();
            let sides: Vec<&str> = part.splitn(2, '=').collect();
            if sides.len() != 2 {
                continue;
            }
            let left = sides[0].trim();
            let right = sides[1].trim();
            let source_side = if is_from_table(left, &source_bare, &source_aliases) {
                Some(left)
            } else if is_from_table(right, &source_bare, &source_aliases) {
                Some(right)
            } else {
                None
            };
            if let Some(side) = source_side {
                n_eq += 1;
                cols.push(bare_column_name(side).to_lowercase());
            }
        }
    }
    cols.sort();
    cols.dedup();
    (cols, n_eq)
}

/// Fetch all sound unique keys (PRIMARY KEY + NOT-NULL UNIQUE indexes) of
/// a table. Each key is a Vec of column names in definition order.
/// Includes __reflex_uk_* indexes created for keyed CTE sub-IMVs.
#[allow(dead_code)]
pub(crate) fn source_sound_unique_keys(source: &str) -> Vec<Vec<String>> {
    let mut result = Vec::new();

    // PRIMARY KEY
    let pk = source_primary_key_columns(source);
    if !pk.is_empty() {
        result.push(pk);
    }

    // UNIQUE indexes: sound keys are those with NULLS NOT DISTINCT, or all columns NOT NULL
    // Excludes partial indexes (indpred IS NOT NULL) and expression indexes (indkey contains 0)
    let uks: Vec<Vec<String>> = Spi::connect(|client| {
        client
            .select(
                "SELECT array_agg(a.attname::TEXT ORDER BY k.n) AS cols \
                 FROM pg_index ix \
                 JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(col, n) ON true \
                 JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.col \
                 WHERE ix.indrelid = to_regclass($1) AND ix.indisunique \
                   AND NOT ix.indisprimary \
                   AND ix.indpred IS NULL AND k.col <> 0 \
                 GROUP BY ix.indexrelid, ix.indnullsnotdistinct \
                 HAVING ix.indnullsnotdistinct OR bool_and(a.attnotnull)",
                None,
                &[unsafe {
                    DatumWithOid::new(source.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .filter_map(|row| row.get_by_name::<Vec<String>, _>("cols").unwrap_or(None))
            .collect()
    });
    result.extend(uks);
    result
}

/// TRUE when `source` is a registered IMV flagged `max_one_row` (ungrouped
/// aggregate → at most one row). Base tables / unknown names → FALSE.
/// Normalizes the source name to unquoted canonical form to match registry storage.
#[allow(dead_code)]
pub(crate) fn source_is_max_one_row(source: &str) -> bool {
    let (schema_part, bare_name) = canonical_source(source);
    let canonical = match schema_part {
        Some(s) => format!("{}.{}", s, bare_name),
        None => bare_name.clone(),
    };

    Spi::connect(|client| {
        client
            .select(
                "SELECT COALESCE(max_one_row, FALSE) AS m \
                 FROM public.__reflex_ivm_reference \
                 WHERE name = $1 OR name = $2 \
                 ORDER BY (name = $1) DESC LIMIT 1",
                None,
                &[
                    unsafe { DatumWithOid::new(canonical, PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(bare_name, PgBuiltInOids::TEXTOID.oid().value()) },
                ],
            )
            .unwrap_or_report()
            .filter_map(|row| row.get_by_name::<bool, _>("m").unwrap_or(None))
            .next()
            .unwrap_or(false)
    })
}

/// Return the GROUP BY columns of an aggregate IMV (from the registry), or None
/// if not a grouped aggregate. Group-by keys are sound unique keys because
/// GROUP BY collapses duplicates (one row per group). The source name (already
/// the full `<parent>__cte_<alias>` sub-IMV name after the CTE body rewrite, or a
/// plain table name) is matched exactly on its canonical or bare form — never by
/// wildcard, which could match a same-named CTE sub-IMV of a different parent.
#[allow(dead_code)]
pub(crate) fn source_registered_group_key(source: &str) -> Option<Vec<String>> {
    let (schema_part, bare_name) = canonical_source(source);
    let canonical = match schema_part {
        Some(s) => format!("{}.{}", s, bare_name),
        None => bare_name.clone(),
    };

    Spi::connect(|client| {
        client
            .select(
                "SELECT index_columns \
                 FROM public.__reflex_ivm_reference \
                 WHERE (name = $1 OR name = $2) AND index_columns IS NOT NULL \
                 ORDER BY (name = $1) DESC LIMIT 1",
                None,
                &[
                    unsafe { DatumWithOid::new(canonical, PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(bare_name, PgBuiltInOids::TEXTOID.oid().value()) },
                ],
            )
            .unwrap_or_report()
            .filter_map(|row| {
                row.get_by_name::<Vec<String>, _>("index_columns")
                    .unwrap_or(None)
            })
            .next()
            .and_then(|cols| if cols.is_empty() { None } else { Some(cols) })
    })
}

/// Find the join type (INNER, LEFT, CROSS, etc.) that reaches a particular
/// target source. Returns the first match or None.
#[allow(dead_code)]
pub(crate) fn join_type_for_target(
    target: &str,
    joins: &[crate::sql_analyzer::JoinInfo],
) -> Option<String> {
    let target_bare = bare_column_name(target).to_lowercase();
    joins
        .iter()
        .find(|j| bare_column_name(&j.target_table).to_lowercase() == target_bare)
        .map(|j| j.join_type.clone())
}

/// Structural unique-key inference for a JOIN passthrough that was given no
/// explicit key. Returns the OUTPUT column names forming a sound unique key, or
/// `None` when none can be proven.
///
/// Sound rule: pick an anchor whose sound unique key (PK or NOT-NULL / NULLS
/// NOT DISTINCT unique index — see [`source_sound_unique_keys`]) is fully
/// projected. Then every other source must be either (a) to-one — its equi-join
/// columns cover a unique key, or it is `max_one_row` (single-row aggregate,
/// incl. CROSS joins) — contributing nothing, or (b) a to-many INNER join whose
/// own sound key is fully projected, contributing that key (K_anchor ∪ K_other
/// is unique). INNER/LEFT/CROSS anchors qualify; RIGHT/FULL, OR/USING, LEFT or
/// CROSS to-many, and any source without a projectable key are refused.
///
/// Enhancements:
/// - Equi-join equivalence (FIX 1): Two qualified column refs are equivalent if
///   connected by = equalities in top-level join ON clauses. projected_output
///   succeeds if an output column is equi-equivalent (not just identical) to a
///   key column of the anchor.
/// - Registered group-by key (FIX 2): If a source is an aggregate IMV, its
///   GROUP BY columns are a sound unique key and will be checked by
///   source_cols_cover_unique_key / projected_sound_key.
pub(crate) fn infer_join_passthrough_unique_key(ctx: &BuildContext) -> Option<Vec<String>> {
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

    // Build equi-join equivalence relation: map each qualified column to its
    // equivalence class representative. Two columns are equi-equivalent if
    // connected by = in top-level join ON clauses. Use canonical (lex-minimal)
    // representative to ensure transitivity. Normalize aliases to bare table names
    // so that "f.col" and "fc_fcst.col" are treated as equivalent.
    let mut equiv: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    // Helper to normalize a qualified column ref: expand alias to bare table name
    let normalize_col = |col: &str| -> String {
        if let Some(dot_pos) = col.rfind('.') {
            let qualifier = &col[..dot_pos];
            let col_name = &col[dot_pos + 1..];
            // Check if qualifier is an alias, and if so, use the bare table name
            if let Some(table_name) = analysis.table_aliases.get(qualifier) {
                format!(
                    "{}.{}",
                    bare_column_name(table_name).to_lowercase(),
                    col_name
                )
            } else {
                col.to_lowercase()
            }
        } else {
            col.to_lowercase()
        }
    };

    for join in &analysis.joins {
        if let Some(cond) = &join.condition_sql {
            for conj in split_top_level_and(cond) {
                if let Some((left, right)) = split_top_level_eq(&conj) {
                    let left_col = left.trim();
                    let right_col = right.trim();
                    if is_simple_col_ref(left_col) && is_simple_col_ref(right_col) {
                        let l_norm = normalize_col(left_col);
                        let r_norm = normalize_col(right_col);
                        // Canonical rep = lexicographically smaller
                        let canon = if l_norm <= r_norm {
                            l_norm.clone()
                        } else {
                            r_norm.clone()
                        };
                        equiv.insert(l_norm, canon.clone());
                        equiv.insert(r_norm, canon);
                    }
                }
            }
        }
    }

    // Return the equivalence class representative of a qualified column ref.
    // Normalize the input first to expand any aliases.
    let equiv_rep = |col: &str| -> String {
        let col_norm = if let Some(dot_pos) = col.rfind('.') {
            let qualifier = &col[..dot_pos];
            let col_name = &col[dot_pos + 1..];
            if let Some(table_name) = analysis.table_aliases.get(qualifier) {
                format!(
                    "{}.{}",
                    bare_column_name(table_name).to_lowercase(),
                    col_name
                )
            } else {
                col.to_lowercase()
            }
        } else {
            col.to_lowercase()
        };
        equiv.get(&col_norm).cloned().unwrap_or(col_norm)
    };

    // The output name that projects a column equi-equivalent to source.key_col,
    // or None when that key column is not projected (directly or via equi-join).
    let projected_output = |source: &str, key_col: &str| -> Option<String> {
        let bare = bare_column_name(source).to_lowercase();
        let aliases: Vec<String> = analysis
            .table_aliases
            .iter()
            .filter(|(_, t)| t.to_lowercase() == source.to_lowercase())
            .map(|(a, _)| a.to_lowercase())
            .collect();
        let key_qualified = format!("{}.{}", bare, key_col);
        let key_rep = equiv_rep(&key_qualified);
        for (out_name, expr) in &target_to_expr {
            // Direct projection: bare column ref from source
            if is_from_table(expr, &bare, &aliases) && bare_column_name(expr) == key_col {
                return Some(out_name.clone());
            }
            // Equi-equivalent projection: output expr is equi-equivalent to the key
            if is_simple_col_ref(expr) {
                let expr_rep = equiv_rep(expr);
                if expr_rep == key_rep {
                    return Some(out_name.clone());
                }
            }
        }
        None
    };

    // The first sound unique key of `source` that is fully projected as bare
    // output columns, mapped to those output names. Also tries registered
    // group-by key for aggregate IMVs. None when no key qualifies.
    let projected_sound_key = |source: &str| -> Option<Vec<String>> {
        // Catalog keys (PK, UNIQUE indexes)
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
        // Registered group-by key (for aggregate IMVs)
        if let Some(group_key) = source_registered_group_key(source) {
            let mut outs = Vec::with_capacity(group_key.len());
            let mut all = true;
            for kc in &group_key {
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
            let to_one =
                (n_eq > 0 && !eq_cols.is_empty() && source_cols_cover_unique_key(other, &eq_cols))
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

/// Parse a JOIN condition to extract column mappings between the key-owner table and a secondary table.
///
/// For `s.product_id = p.id AND s.version = p.version`:
/// - Splits by AND
/// - For each equality, identifies which side belongs to the secondary table
/// - Maps the key-owner side's target column name to the secondary side's source column name
pub(crate) fn parse_join_condition_mappings(
    condition: &str,
    secondary_table: &str,
    table_aliases: &std::collections::HashMap<String, String>,
    key_columns: &[String],
    target_col_to_expr: &std::collections::HashMap<String, String>,
) -> Vec<(String, String)> {
    let mut mappings = Vec::new();

    // Build a set for fast lookup: which aliases/names refer to the secondary table?
    let secondary_lower = secondary_table.to_lowercase();
    let secondary_bare = bare_column_name(secondary_table).to_lowercase();
    let secondary_aliases: Vec<String> = table_aliases
        .iter()
        .filter(|(_, table)| table.to_lowercase() == secondary_lower)
        .map(|(alias, _)| alias.to_lowercase())
        .collect();

    // Build reverse: for each key column, which expr_sql does it correspond to?
    // e.g., "product_id" → "s.product_id"

    // Case-insensitive split on AND: lowercase the condition first, then split
    // only on the lowercase form. The previous chain-of-two-splits formulation
    // produced spurious whole-condition iterations when only one of "AND" /
    // "and" was present in the source string (caught by
    // pg_test_source_join_keys_skipped_when_composite_join_partial_map).
    let condition_lower = condition.to_lowercase();
    for part in condition_lower.split(" and ") {
        let part = part.trim();
        let sides: Vec<&str> = part.splitn(2, '=').collect();
        if sides.len() != 2 {
            continue;
        }
        let left = sides[0].trim().to_string();
        let right = sides[1].trim().to_string();

        // Determine which side belongs to the secondary table
        let (secondary_side, other_side) =
            if is_from_table(&left, &secondary_bare, &secondary_aliases) {
                (left, right)
            } else if is_from_table(&right, &secondary_bare, &secondary_aliases) {
                (right, left)
            } else {
                continue;
            };

        let secondary_col = bare_column_name(&secondary_side).to_string();
        let other_col = bare_column_name(&other_side).to_string();

        // Find which key column the other side maps to
        // The other side's bare column might be a key column directly,
        // or the other side's full expression might match a key column's expr_sql
        for kc in key_columns {
            if *kc == other_col {
                // Direct match: key column "product_id" and other side bare name is "product_id"
                mappings.push((kc.clone(), secondary_col.clone()));
                break;
            }
            // Check via expr_sql: key column "product_id" has expr "s.product_id",
            // and other_side is "s.product_id"
            if let Some(expr) = target_col_to_expr.get(kc.as_str()) {
                if *expr == other_side {
                    mappings.push((kc.clone(), secondary_col.clone()));
                    break;
                }
            }
        }
    }

    mappings
}

/// Check if a qualified column reference (e.g., "p.id") belongs to a given table.
pub(crate) fn is_from_table(
    qualified_col: &str,
    table_bare_name: &str,
    table_aliases: &[String],
) -> bool {
    if let Some(dot_pos) = qualified_col.rfind('.') {
        let qualifier = &qualified_col[..dot_pos];
        qualifier == table_bare_name || table_aliases.iter().any(|a| a == qualifier)
    } else {
        false
    }
}

/// Return type of [`query_column_types_from_catalog_with_per_source`]:
/// `(types, not_null_cols, per_source_columns)`.
type CatalogColumnInfo = (
    HashMap<String, String>,
    std::collections::HashSet<String>,
    HashMap<String, std::collections::HashSet<String>>,
);

/// Per-source-aware column-type catalog lookup. Returns the cross-source
/// type map and not-null set used elsewhere, plus a per-source map
/// `source-table-name → set of catalog-known column names`.
///
/// Used by the 1.4.5 filter-aware spurious-skip metadata to ground the
/// analyzer's over-inclusive `imv_relevant_columns` (which attributes
/// bare idents to every real source) in the actual catalog shape, so the
/// persisted JSON only ever names columns that exist on the named source.
pub(crate) fn query_column_types_from_catalog_with_per_source(
    client: &pgrx::spi::SpiClient<'_>,
    table_names: &[String],
) -> CatalogColumnInfo {
    let mut types = HashMap::new();
    let mut not_null_cols = std::collections::HashSet::new();
    let mut per_source: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
    for table in table_names {
        // Skip non-real tables (subqueries, functions)
        if table.starts_with('<') {
            continue;
        }
        // Resolve the table via `to_regclass($1)` so we honour the session's
        // `search_path` instead of hard-coding the `public` schema. This is the
        // only way an unqualified source name from the IMV body (e.g.
        // `FROM history_sales_view`) finds the tenant-schema relation when the
        // caller has done `SET search_path = <tenant>, public`. We pull the
        // actual `relname` back out of pg_class so the `table.column` keys in
        // the returned map are consistent regardless of whether the caller
        // passed a qualified or unqualified name.
        //
        // pg_catalog (not information_schema.columns) because the latter omits
        // materialized views — a MIN/MAX over a matview column would then get
        // no type and default to NUMERIC. `format_type` covers every relkind.
        let rows = client
            .select(
                "SELECT a.attname::text AS col_name, \
                        format_type(a.atttypid, a.atttypmod) AS data_type, \
                        c.relname::text AS relname, \
                        CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable \
                 FROM pg_catalog.pg_attribute a \
                 JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
                 WHERE c.oid = to_regclass($1) \
                   AND a.attnum > 0 AND NOT a.attisdropped",
                None,
                &[unsafe {
                    DatumWithOid::new(table.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report();
        for row in rows {
            if let (Some(col_name), Some(data_type), Some(relname)) = (
                row.get_by_name::<String, _>("col_name").unwrap_or(None),
                row.get_by_name::<String, _>("data_type").unwrap_or(None),
                row.get_by_name::<String, _>("relname").unwrap_or(None),
            ) {
                let pg_type = map_information_schema_type(&data_type);
                types.insert(format!("{}.{}", relname, col_name), pg_type.clone());
                // Also insert bare column name for simpler lookups
                types.entry(col_name.to_string()).or_insert(pg_type);

                // Track NOT NULL columns for SUM optimization
                let is_nullable = row
                    .get_by_name::<String, _>("is_nullable")
                    .unwrap_or(None)
                    .unwrap_or_default();
                if is_nullable == "NO" {
                    not_null_cols.insert(col_name.to_string());
                }
                per_source
                    .entry(table.clone())
                    .or_default()
                    .insert(col_name.clone());
            }
        }
    }
    (types, not_null_cols, per_source)
}

/// Canonicalize a column reference for comparison: lower-case, trim, and strip
/// the double-quotes around each dotted part. `"ss"."Dem_Plan_Id"` → `ss.dem_plan_id`.
pub(crate) fn canon_col_ref(s: &str) -> String {
    s.trim()
        .split('.')
        .map(|p| p.trim().trim_matches('"').to_lowercase())
        .collect::<Vec<_>>()
        .join(".")
}

/// True if `s` is a bare column reference (`ident` or `qualifier.ident`) rather
/// than a function call, literal, or compound expression. Only such refs can be
/// safely treated as join-key operands.
///
/// Quoted identifiers are rejected: a quoted name may contain a literal `.`
/// (`"weird.name"`), which the dot-splitting in [`canon_col_ref`] /
/// [`resolve_column_source`] would mis-read as a `qualifier.column`, possibly
/// resolving to the wrong table. Treating any quoted ref as non-simple means it
/// is never promoted to NOT NULL — correct (it falls back to `IS NOT DISTINCT
/// FROM`), just not index-optimized.
pub(crate) fn is_simple_col_ref(s: &str) -> bool {
    let t = s.trim();
    if t.is_empty() || t.contains('"') {
        return false;
    }
    t.split('.').all(|p| {
        let p = p.trim();
        !p.is_empty()
            && p.chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '$')
    })
}

/// Split a boolean expression on top-level ` AND `, respecting parentheses and
/// single-quoted string literals.
pub(crate) fn split_top_level_and(cond: &str) -> Vec<String> {
    let bytes = cond.as_bytes();
    let upper = cond.to_uppercase();
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if in_str {
            if c == '\'' {
                in_str = false;
            }
            i += 1;
            continue;
        }
        match c {
            '\'' => in_str = true,
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {
                if depth == 0 && upper[i..].starts_with(" AND ") {
                    parts.push(cond[start..i].to_string());
                    i += 5;
                    start = i;
                    continue;
                }
            }
        }
        i += 1;
    }
    parts.push(cond[start..].to_string());
    parts
}

/// If `conj` is a top-level equality `lhs = rhs` (not `<=`, `>=`, `<>`, `!=`),
/// return the two operands. Respects parentheses and string literals.
pub(crate) fn split_top_level_eq(conj: &str) -> Option<(&str, &str)> {
    let bytes = conj.as_bytes();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if in_str {
            if c == '\'' {
                in_str = false;
            }
            i += 1;
            continue;
        }
        match c {
            '\'' => in_str = true,
            '(' => depth += 1,
            ')' => depth -= 1,
            '=' if depth == 0 => {
                let prev = if i > 0 { bytes[i - 1] as char } else { ' ' };
                let next = if i + 1 < bytes.len() {
                    bytes[i + 1] as char
                } else {
                    ' '
                };
                if !matches!(prev, '<' | '>' | '!') && next != '=' {
                    return Some((&conj[..i], &conj[i + 1..]));
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Collect the canonical column references that are guaranteed non-NULL because
/// they appear as an operand of a conjunctive equality in an INNER-join ON
/// condition. An equi-join `x = y` cannot match a NULL on either side, so both
/// operands are non-NULL in the join output. Conditions containing a top-level
/// `OR` are skipped (a disjunct could re-admit a NULL).
pub(crate) fn inner_join_equi_non_null_refs(
    analysis: &crate::sql_analyzer::SqlAnalysis,
) -> std::collections::HashSet<String> {
    let mut refs = std::collections::HashSet::new();
    for j in &analysis.joins {
        if !j.join_type.to_uppercase().contains("INNER") {
            continue;
        }
        let Some(cond) = &j.condition_sql else {
            continue;
        };
        if cond.to_uppercase().contains(" OR ") {
            continue;
        }
        for conj in split_top_level_and(cond) {
            if let Some((l, r)) = split_top_level_eq(&conj) {
                if is_simple_col_ref(l) {
                    refs.insert(canon_col_ref(l));
                }
                if is_simple_col_ref(r) {
                    refs.insert(canon_col_ref(r));
                }
            }
        }
    }
    refs
}

/// Resolve a (possibly aliased) column reference to its real source table.
/// Returns `(real_table, bare_column)` when the reference is a simple
/// `qualifier.col` or a bare `col` with a single source; `None` otherwise.
pub(crate) fn resolve_column_source(
    analysis: &crate::sql_analyzer::SqlAnalysis,
    col_expr: &str,
) -> Option<(String, String)> {
    if !is_simple_col_ref(col_expr) {
        return None;
    }
    let canon = canon_col_ref(col_expr);
    if let Some((qual, col)) = canon.split_once('.') {
        let real = analysis
            .table_aliases
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(qual))
            .map(|(_, v)| v.trim_matches('"').to_lowercase())
            .unwrap_or_else(|| qual.to_string());
        Some((real, col.to_string()))
    } else if analysis.sources.len() == 1 {
        Some((analysis.sources[0].trim_matches('"').to_lowercase(), canon))
    } else {
        None
    }
}

/// True if `col_expr` resolves to a base-table column declared NOT NULL in the
/// catalog AND that table is not on the nullable side of an outer join. Such a
/// column can never be NULL in the IMV.
pub(crate) fn column_base_not_null(
    client: &mut pgrx::spi::SpiClient<'_>,
    analysis: &crate::sql_analyzer::SqlAnalysis,
    col_expr: &str,
    left_target_tables: &std::collections::HashSet<String>,
) -> bool {
    let Some((table, col)) = resolve_column_source(analysis, col_expr) else {
        return false;
    };
    if left_target_tables.contains(&table) {
        return false;
    }
    client
        .select(
            "SELECT a.attnotnull AS nn FROM pg_attribute a \
             WHERE a.attrelid = to_regclass($1) AND a.attname = $2 \
               AND a.attnum > 0 AND NOT a.attisdropped",
            Some(1),
            &[
                unsafe { DatumWithOid::new(table, PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(col, PgBuiltInOids::TEXTOID.oid().value()) },
            ],
        )
        .ok()
        .and_then(|mut t| {
            t.next()
                .and_then(|r| r.get_by_name::<bool, _>("nn").ok().flatten())
        })
        == Some(true)
}

/// For a passthrough IMV's unique key (auto-detected *or* explicit), determine
/// which key columns are provably NOT NULL by the same structural proof
/// `infer_not_null_columns` uses for aggregate GROUP BY keys: an equi-join
/// operand of an INNER join, or a catalog NOT-NULL base column not on the
/// nullable side of an outer join.
///
/// The result feeds `plan.not_null_columns`, which the keyed DELETE/UPDATE
/// codegen (`trigger::merge::null_safe_in`, `trigger::scope::
/// build_null_safe_membership_predicate`) reads to choose a sargable `=`/`IN`
/// match for a column proven safe, or the NULL-safe `IS NOT DISTINCT FROM`
/// match otherwise. Left unproven, a column defaults to the safe form — never
/// the reverse — because `(key) IN (SELECT ...)` is never TRUE for a NULL key:
/// treating an actually-nullable key as safe would leave a phantom target row
/// on source DELETE and abort the source UPDATE with a unique-index violation
/// (2026-07-25 untreated_bugs report).
///
/// An explicit key can name any column, so it is the branch that actually
/// needs this; the auto-detect paths (`resolve_unique_columns`'s PK probe,
/// `infer_join_passthrough_unique_key`) only ever resolve to a PRIMARY KEY,
/// which is always NOT NULL — calling this on them is a cheap confirmation,
/// not a new failure mode.
pub(crate) fn provably_not_null_key_columns(
    client: &mut pgrx::spi::SpiClient<'_>,
    analysis: &crate::sql_analyzer::SqlAnalysis,
    key_columns: &[String],
) -> std::collections::HashSet<String> {
    let mut left_target_tables: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    for j in &analysis.joins {
        let jt = j.join_type.to_uppercase();
        if jt.contains("RIGHT") || jt.contains("FULL") {
            // Nullable side is ambiguous for RIGHT/FULL joins — prove nothing
            // rather than guess which key column could go NULL.
            return std::collections::HashSet::new();
        }
        if jt.contains("LEFT") {
            left_target_tables.insert(j.target_table.trim_matches('"').to_lowercase());
        }
    }

    let equi_refs = inner_join_equi_non_null_refs(analysis);
    let target_col_to_expr: HashMap<String, String> = analysis
        .select_columns
        .iter()
        .map(|c| {
            let target_name = normalized_column_name(c.alias.as_deref().unwrap_or(&c.expr_sql));
            (target_name, c.expr_sql.clone())
        })
        .collect();

    key_columns
        .iter()
        .filter(|kc| {
            target_col_to_expr.get(kc.as_str()).is_some_and(|expr| {
                is_simple_col_ref(expr)
                    && (equi_refs.contains(&canon_col_ref(expr))
                        || column_base_not_null(client, analysis, expr, &left_target_tables))
            })
        })
        .cloned()
        .collect()
}

/// Infer the group-by / distinct columns that are *provably* NOT NULL from the
/// query's structure (NOT from transient create-time data). A column is promoted
/// only when it can never be NULL in the IMV:
///
///   * it is an equi-join operand of an INNER join (`dp.id = ss.dem_plan_id`
///     keeps `ss.dem_plan_id` non-NULL — the yse.ivm_sop_forecast_view 405 s
///     case), or
///   * its base column is declared NOT NULL in the catalog and its table is not
///     on the nullable side of an outer join.
///
/// Promoting a column lets MERGE maintenance match keys with `=` (index-friendly)
/// instead of `IS NOT DISTINCT FROM`. Doing so on a column that can later become
/// NULL silently drops rows (docs/fuzz-findings.md findings #1 and #3), so the
/// inference is deliberately conservative: anything not provably non-NULL (outer-
/// join columns, unconstrained nullable columns, computed expressions, output
/// aliases) is left alone and matched with the always-correct `IS NOT DISTINCT
/// FROM`. With no analysis available, nothing is promoted.
pub(crate) fn infer_not_null_columns(
    client: &mut pgrx::spi::SpiClient<'_>,
    plan: &crate::aggregation::AggregationPlan,
    analysis: Option<&crate::sql_analyzer::SqlAnalysis>,
) -> std::collections::HashSet<String> {
    let mut proven = std::collections::HashSet::new();
    let Some(a) = analysis else {
        return proven;
    };

    // Map the LEFT-join target tables (the nullable side). RIGHT/FULL joins make
    // the nullable side ambiguous, so we promote nothing for those queries.
    let mut left_target_tables: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    for j in &a.joins {
        let jt = j.join_type.to_uppercase();
        if jt.contains("RIGHT") || jt.contains("FULL") {
            return proven;
        }
        if jt.contains("LEFT") {
            left_target_tables.insert(j.target_table.trim_matches('"').to_lowercase());
        }
    }

    let from_outer_join = |col_expr: &str| -> bool {
        if left_target_tables.is_empty() || !col_expr.contains('.') {
            return false;
        }
        let alias = col_expr.split('.').next().unwrap_or("").trim_matches('"');
        if left_target_tables.contains(&alias.to_lowercase()) {
            return true;
        }
        a.table_aliases.iter().any(|(k, real)| {
            k.eq_ignore_ascii_case(alias)
                && left_target_tables.contains(&real.trim_matches('"').to_lowercase())
        })
    };

    let equi_refs = inner_join_equi_non_null_refs(a);

    for col_expr in plan
        .group_by_columns
        .iter()
        .chain(plan.distinct_columns.iter())
    {
        let norm = normalized_column_name(col_expr);
        if plan.not_null_columns.contains(&norm) {
            continue;
        }
        if from_outer_join(col_expr) {
            continue;
        }
        // Only reason about simple (unquoted) column references; `canon_col_ref`
        // dot-splitting is unsafe for quoted names that may contain a literal dot.
        let proven_not_null = is_simple_col_ref(col_expr)
            && (equi_refs.contains(&canon_col_ref(col_expr))
                || column_base_not_null(client, a, col_expr, &left_target_tables));
        if proven_not_null {
            proven.insert(norm);
        }
    }
    proven
}

/// Persist a delta of newly-discovered NOT NULL columns to
/// `__reflex_ivm_reference.aggregations`. Performs a JSON-level merge with
/// the existing array (no overwrite of catalog-derived entries).
pub(crate) fn persist_probed_not_null_columns(
    client: &mut pgrx::spi::SpiClient<'_>,
    view_name: &str,
    new_cols: &[String],
) {
    if new_cols.is_empty() {
        return;
    }
    let arr_literal = format_pg_text_array_literal(new_cols);
    client
        .update(
            "UPDATE public.__reflex_ivm_reference \
             SET aggregations = jsonb_set( \
                 aggregations::jsonb, \
                 '{not_null_columns}', \
                 ( \
                     SELECT jsonb_agg(DISTINCT col) \
                     FROM ( \
                         SELECT jsonb_array_elements_text( \
                             COALESCE((aggregations::jsonb)->'not_null_columns', '[]'::jsonb) \
                         ) AS col \
                         UNION \
                         SELECT unnest($1::text[]) AS col \
                     ) s \
                 ) \
             )::json \
             WHERE name = $2",
            None,
            &[
                unsafe { DatumWithOid::new(arr_literal, PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                },
            ],
        )
        .unwrap_or_report();
}

/// SQL-callable wrapper: re-probe an existing IMV from data and update its
/// stored aggregations. Used by the 1.4.4→1.4.5 migration to backfill
/// effectively-NOT-NULL columns the catalog missed on existing IMVs, and by
/// operators after a data shape change. Returns a human-readable status.
pub(crate) fn reflex_probe_not_null_columns_impl(view_name: &str) -> String {
    if let Err(msg) = validate_view_name(view_name) {
        return msg.to_string();
    }
    let result: Result<Vec<String>, String> = Spi::connect_mut(|client| {
        let rows = client
            .select(
                "SELECT aggregations::text AS aggregations, sql_query \
                 FROM public.__reflex_ivm_reference \
                 WHERE name = $1 AND enabled = TRUE",
                None,
                &[unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .map_err(|e| format!("lookup failed: {}", e))?
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Err(format!("IMV '{}' not found or disabled", view_name));
        }
        let agg_json: String = rows[0]
            .get_by_name::<&str, _>("aggregations")
            .map_err(|e| format!("read aggregations: {}", e))?
            .unwrap_or("{}")
            .to_string();
        let plan: crate::aggregation::AggregationPlan = serde_json::from_str(&agg_json)
            .map_err(|e| format!("aggregations JSON parse: {}", e))?;
        if plan.is_passthrough {
            return Ok(Vec::new());
        }
        // Re-derive the join analysis from the stored query so the same
        // structural NOT-NULL inference used at create time applies here. If the
        // query is missing or unparseable, `analysis` stays None and nothing is
        // promoted — conservative, never unsound.
        let analysis = rows[0]
            .get_by_name::<&str, _>("sql_query")
            .ok()
            .flatten()
            .and_then(|q| {
                Parser::parse_sql(&PostgreSqlDialect {}, q)
                    .ok()
                    .and_then(|stmts| crate::sql_analyzer::analyze(&stmts).ok())
            });
        let probed = infer_not_null_columns(client, &plan, analysis.as_ref());
        let new_cols: Vec<String> = probed
            .into_iter()
            .filter(|c| !plan.not_null_columns.contains(c))
            .collect();
        persist_probed_not_null_columns(client, view_name, &new_cols);
        Ok(new_cols)
    });
    match result {
        Ok(cols) if cols.is_empty() => {
            format!(
                "pg_reflex: probe found no additional NOT NULL columns on '{}'",
                view_name
            )
        }
        Ok(cols) => format!(
            "pg_reflex: probe added {} NOT NULL column(s) on '{}': {:?}",
            cols.len(),
            view_name,
            cols
        ),
        Err(e) => format!("ERROR: {}", e),
    }
}
