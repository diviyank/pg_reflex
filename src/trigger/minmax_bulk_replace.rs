use super::*;
use crate::aggregation::AggregationPlan;

/// Staged rows (old and new side) at or above this fraction of the source rows
/// of the groups they touch: the DEFERRED top-K MIN/MAX flush replaces those
/// groups from the base query instead of merging the delta into them. Below it,
/// aggregating the old and new delta is cheaper than reading the groups again
/// (measured crossover ~0.35 when no heap shrinks, lower when one does). Both
/// paths are exact, so the ratio only decides speed.
pub(crate) const MIN_MAX_BULK_REPLACE_RATIO: f64 = 0.5;

/// Whether the top-K MIN/MAX UPDATE of `plan` may replace its affected groups
/// wholesale. Only the DEFERRED flush's netted delta (`netted_delta`): a
/// statement trigger never rebuilds, since another trigger of the same statement
/// would apply its delta on top of a source state the replacement already read.
/// Only grouped plans whose group key is the GROUP BY alone, over an
/// unpartitioned target. An IMV ignoring a source is excluded at run time.
pub(crate) fn may_replace_large_deltas(plan: &AggregationPlan, netted_delta: bool) -> bool {
    netted_delta
        && plan.needs_ivm_count
        && !plan.group_by_columns.is_empty()
        && plan.distinct_columns.is_empty()
        && plan.partition_columns.is_empty()
        && plan.intermediate_columns.iter().any(|ic| ic.has_topk())
}

/// A DO block that, for the groups the netted delta touches, either replaces
/// their intermediate rows from the base query or runs `algebraic_stmts` (the
/// Sub / shrunk capture / Add / forced recompute sequence) unchanged. An entry
/// of `algebraic_stmts` may be a gated pair joined by the statement separator:
/// each part gets its own EXECUTE, since every caller splits the returned SQL
/// on that separator.
///
/// Both fill `affected_tbl` with those groups, so the epilogue that follows
/// brings their target rows in line either way. The replacement deletes the
/// groups' intermediate rows and inserts the base query scoped to them: every
/// aggregate column and `__ivm_count` come from the source, a group the source
/// no longer holds is not reinserted, and a new one is. It runs when the staged
/// rows reach [`MIN_MAX_BULK_REPLACE_RATIO`] of `Σ __ivm_count` over those
/// groups, and the IMV ignores no source (a replacement would read the current
/// state of a source whose changes the IMV defers).
///
/// The source at a DEFERRED flush reflects every delta staged for it, and the
/// cross-source guard keeps an IMV with two pending sources off this path, so
/// the replaced groups equal what the delta would have produced; a delta of
/// another source flushed later describes a change made after this read.
///
/// `None` when the base query has no GROUP BY to scope.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_min_max_bulk_replace_sql(
    view_name: &str,
    plan: &AggregationPlan,
    orig_base_query: &str,
    delta_old: &str,
    delta_new: &str,
    intermediate_tbl: &str,
    affected_tbl: &str,
    group_cols: &[String],
    algebraic_stmts: &[String],
) -> Option<String> {
    let scoped_insert = |eq: bool| -> Option<String> {
        let scoped = affected_scoped_base_query(
            plan,
            orig_base_query,
            affected_tbl,
            eq,
            LITERAL_KEYS_MARKER,
        )?;
        Some(literal_keyed_plpgsql(
            &format!("INSERT INTO {intermediate_tbl} {scoped}"),
            affected_tbl,
            plan,
        ))
    };
    let reinsert = match affected_null_key_gate(affected_tbl, group_cols, &plan.not_null_columns) {
        None => scoped_insert(true)?,
        Some(null_key) => format!(
            "IF {null_key} THEN {} ELSE {} END IF;",
            scoped_insert(false)?,
            scoped_insert(true)?
        ),
    };
    let in_groups = null_safe_in_gated(
        affected_tbl,
        intermediate_tbl,
        group_cols,
        group_cols,
        &plan.not_null_columns,
    );
    let group_rows = in_groups
        .stmts(|pred| format!("SELECT __ivm_count FROM {intermediate_tbl} WHERE {pred}"))
        .join(" UNION ALL ");
    let delete = in_groups
        .stmts(|pred| format!("DELETE FROM {intermediate_tbl} WHERE {pred};"))
        .join("\n");
    let algebraic = algebraic_stmts
        .iter()
        .flat_map(|stmts| split_reflex_statements(stmts))
        .map(|stmt| format!("EXECUTE {};", dollar_quoted(&stmt)))
        .collect::<Vec<_>>()
        .join("\n");
    let cols = group_cols.join(", ");
    let body = format!(
        "DECLARE\n\
             __reflex_replace BOOLEAN;\n\
             __reflex_staged NUMERIC;\n\
             __reflex_group_rows NUMERIC;\n\
             __reflex_keys RECORD;\n\
         BEGIN\n\
             SELECT COALESCE(cardinality(ignored_sources), 0) = 0 INTO __reflex_replace\n\
                 FROM public.__reflex_ivm_reference WHERE name = '{view}';\n\
             IF __reflex_replace THEN\n\
                 TRUNCATE {affected};\n\
                 WITH __reflex_delta AS MATERIALIZED (\n\
                          SELECT {cols}, __ivm_count FROM ({delta_old}) __reflex_old\n\
                          UNION ALL\n\
                          SELECT {cols}, __ivm_count FROM ({delta_new}) __reflex_new),\n\
                      __reflex_groups AS (\n\
                          INSERT INTO {affected} SELECT DISTINCT {cols} FROM __reflex_delta)\n\
                 SELECT COALESCE(sum(__ivm_count), 0) INTO __reflex_staged FROM __reflex_delta;\n\
                 ANALYZE {affected};\n\
                 SELECT COALESCE(sum(__ivm_count), 0) INTO __reflex_group_rows\n\
                     FROM ({group_rows}) __reflex_rows;\n\
                 __reflex_replace := __reflex_staged >= {ratio} * __reflex_group_rows;\n\
             END IF;\n\
             IF __reflex_replace THEN\n\
                 {delete}\n\
                 {reinsert}\n\
                 ANALYZE {intermediate_tbl};\n\
             ELSE\n\
                 {algebraic}\n\
             END IF;\n\
         END",
        view = view_name.replace('\'', "''"),
        affected = affected_tbl,
        ratio = MIN_MAX_BULK_REPLACE_RATIO,
    );
    Some(format!("DO {}", dollar_quoted(&body)))
}
