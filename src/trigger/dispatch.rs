use super::*;
use crate::aggregation::AggregationPlan;
use crate::query_decomposer::normalized_column_name;

/// 2026-05-15 — high-selectivity dispatch threshold. When the number of
/// affected groups exceeds this fraction of the intermediate's row count, the
/// trigger switches from MERGE-based incremental to TRUNCATE+rebuild (full
/// IMV-body refresh via `reflex_reconcile`).
///
/// History: 1.4.5 introduced this at 0.3 because reconcile (~14 s on
/// SOP-forecast shape at the time) was faster than the broken incremental
/// path (~17 s at 100K-affected dead-cleanup pathology, exposed in 2026-05-15).
///
/// 1.4.6 (Item α + ANALYZE fix) makes incremental fast at ALL reachable
/// selectivities on the SOP-forecast shape (1 M source × 50 dem_plan, ~200 K
/// intermediate, 20 K rows per plan = 10 % per flip):
///
/// | Selectivity | Incremental | reconcile (1.4.5) |
/// |---:|---:|---:|
/// | 11 % (20K) | ~620 ms  | ~17 s |
/// | 33 % (60K) | ~1.3 s   | ~17 s |
/// | 50 % (100K)| ~1.9 s   | ~17 s |
/// | 78 % (140K)| ~2.9 s   | ~17 s |
///
/// On that synthetic shape reconcile never wins. **But real db_clone shapes
/// invert the proportions** (76 M source × 28 dem_plan, 7.7 M intermediate,
/// 0.9–8.9 M rows per plan = 12–115 % per flip). On those shapes the
/// per-row MERGE + target double-rewrite hits O(|affected| · log
/// |intermediate|) and loses 2–8× to reconcile at high selectivity.
///
/// 1.4.6 lowers the default to 0.50, a compromise that:
///   * Keeps small-mutation workloads (< 50 % of intermediate) on the
///     incremental path — preserves the 60×-faster pure-data UPDATE case.
///   * Dispatches catastrophic bulk flips (>= 50 % of intermediate) to
///     reconcile, which on db_clone alp is ~2× faster than the bulk
///     incremental path.
///
/// Per-IMV override via the `wipe_threshold` column in
/// `__reflex_ivm_reference` (write via `reflex_set_wipe_threshold(name,
/// value)`) is the right tool when this global default doesn't fit a
/// specific IMV's shape — e.g. yse-style 419 K intermediate where the
/// crossover is closer to 0.15.
pub(crate) const WIPE_THRESHOLD_DEFAULT: f64 = 0.5;

/// Default for the `reflex.assert_inplace_update` GUC (off): when on, the
/// in-place upsert cold section re-derives the affected key set and RAISEs on
/// any mismatch. On in CI/fuzz; off in production by default.
#[allow(dead_code)]
pub(crate) const ASSERT_INPLACE_UPDATE_DEFAULT: bool = false;

/// Specification for the in-place upsert + delete-gone cold path in
/// partitioned passthrough UPDATE. When Some, the cold section replaces
/// DELETE+INSERT with an atomic upsert + delete-gone. When None, the
/// standard DELETE+INSERT behavior is preserved.
pub(crate) struct InplaceSpec<'a> {
    pub delta_new: &'a str, // base_query rewritten to read pt_new (the recompute)
    pub key_target_cols: &'a [String], // quoted unique-key cols in the TARGET (e.g. "\"id\"", "\"region\"")
    pub key_source_cols: &'a [String], // quoted unique-key cols in the SOURCE scratch (pt_old)
    pub non_key_cols: &'a [String],    // quoted NON-key target cols (resolved at codegen time)
    pub pt_old: &'a str,               // OLD-image scratch table (quoted, schema-qualified)
}

/// 1.4.5 — emit a DO block that dispatches between MERGE-incremental and
/// TRUNCATE-rebuild based on runtime selectivity.
///
/// Replaces these statements in the standard flow:
///   1. MERGE intermediate USING scratch
///   2. DELETE intermediate WHERE __ivm_count<=0 AND in_affected (optional)
///   3. DELETE target WHERE in_affected
///   4. INSERT target SELECT end_query WHERE in_affected
///
/// At high selectivity, instead:
///   1. TRUNCATE intermediate
///   2. INSERT INTO intermediate <base_query> -- full re-aggregation
///   3. TRUNCATE target
///   4. INSERT INTO target <end_query>
///
/// Scratch and affected are populated BEFORE this block (steps 1-3 of the
/// standard flow remain unchanged). The DO block reads the affected table's
/// row count and compares to pg_class.reltuples on the intermediate.
///
/// Threshold: `current_setting('reflex.wipe_threshold', true)::numeric` if
/// set, else `WIPE_THRESHOLD_DEFAULT`. Operators can `SET LOCAL` per-session
/// or per-statement.
#[allow(clippy::too_many_arguments)]
/// Render a list of statements as PL/pgSQL `EXECUTE` lines, one per statement,
/// each dollar-quoted with `$reflex_inner$` (any occurrence of that tag inside
/// the statement is rewritten so it cannot terminate the quote early).
///
/// PS-5 — the target DELETE / INSERT / dead-cleanup slots each hold a *list*
/// rather than a single statement, because a nullable group key makes the
/// affected-groups match expand into a self-gated sargable/NULL-safe pair (see
/// `merge.rs::AffectedMatch`). Each statement carries its own gate, so emitting
/// all of them unconditionally is correct: exactly one does work and PostgreSQL
/// skips the other's plan with a `One-Time Filter`.
///
/// `using` is the PL/pgSQL `USING` argument list appended to every `EXECUTE`
/// (the partitioned cold path binds hot-partition keys as `$1`/`$2`). Both
/// variants of a gated pair derive from the same filtered string, so they
/// reference the same placeholders.
fn execute_each(stmts: &[String], using: Option<&str>) -> String {
    let suffix = match using {
        Some(u) => format!(" USING {}", u),
        None => String::new(),
    };
    stmts
        .iter()
        .map(|s| {
            format!(
                "EXECUTE $reflex_inner${}$reflex_inner${};\n",
                s.replace("$reflex_inner$", "$reflex_inner_alt$"),
                suffix
            )
        })
        .collect()
}

pub(crate) fn build_high_selectivity_dispatch_sql(
    view_name: &str,
    intermediate_tbl: &str,
    affected_tbl: &str,
    merge_sql: &str,
    dead_cleanup_sql: &[String],
    target_delete_sql: &[String],
    target_insert_sql: &[String],
) -> String {
    let dead_cleanup = execute_each(dead_cleanup_sql, None);
    let safe_merge = merge_sql.replace("$reflex_inner$", "$reflex_inner_alt$");
    let safe_tdel = execute_each(target_delete_sql, None);
    let safe_tins = execute_each(target_insert_sql, None);
    let safe_view = view_name.replace('\'', "''");

    format!(
        "DO $reflex_dispatch$\n\
         DECLARE\n\
             _aff BIGINT;\n\
             _imm NUMERIC;\n\
             _thr NUMERIC;\n\
             _per_imv NUMERIC;\n\
             _ratio NUMERIC;\n\
         BEGIN\n\
             SELECT count(*) INTO _aff FROM {affected};\n\
             SELECT GREATEST(reltuples::NUMERIC, 1.0) INTO _imm\n\
                 FROM pg_class WHERE oid = '{intermediate}'::regclass;\n\
             -- 1.4.6 precedence: per-IMV wipe_threshold column (operator\n\
             -- override) → session GUC reflex.wipe_threshold → compiled\n\
             -- default. Per-IMV is the right granularity when one extension\n\
             -- instance serves IMVs with shape-divergent crossovers.\n\
             SELECT wipe_threshold INTO _per_imv\n\
                 FROM public.__reflex_ivm_reference WHERE name = '{view}';\n\
             _thr := COALESCE(_per_imv, current_setting('reflex.wipe_threshold', true)::NUMERIC, {default_thr});\n\
             _ratio := _aff::NUMERIC / _imm;\n\
             IF _ratio >= _thr THEN\n\
                 -- High-selectivity path — delegate to reflex_reconcile,\n\
                 -- which implements the drop-index/bulk-INSERT/recreate-\n\
                 -- index pattern. At >= threshold selectivity the cost of\n\
                 -- the standard MERGE + target double-rewrite exceeds the\n\
                 -- cost of a full IMV rebuild.\n\
                 RAISE DEBUG 'pg_reflex wipe: ratio=% thr=% — reconcile', _ratio, _thr;\n\
                 PERFORM public.reflex_reconcile('{view}');\n\
             ELSE\n\
                 RAISE DEBUG 'pg_reflex wipe: ratio=% thr=% — incremental', _ratio, _thr;\n\
                 EXECUTE $reflex_inner${merge}$reflex_inner$;\n\
                 -- ANALYZE intermediate after MERGE so the planner has\n\
                 -- accurate stats for downstream dead-cleanup, target_delete,\n\
                 -- and target_insert. The MERGE modified ~|scratch| rows in\n\
                 -- the intermediate (UPDATE/INSERT), and the algebraic count\n\
                 -- column (__ivm_count) just shifted distribution; without\n\
                 -- fresh stats the planner can pick pathological NestedLoop+\n\
                 -- SeqScan plans (12+ min on 100K groups). ~150 ms cost on\n\
                 -- the SOP-forecast shape.\n\
                 EXECUTE 'ANALYZE {intermediate}';\n\
{dead_cleanup}\
{tdel}\
{tins}\
             END IF;\n\
         END\n\
         $reflex_dispatch$",
        affected = affected_tbl,
        intermediate = intermediate_tbl,
        view = safe_view,
        default_thr = WIPE_THRESHOLD_DEFAULT,
        merge = safe_merge,
        dead_cleanup = dead_cleanup,
        tdel = safe_tdel,
        tins = safe_tins,
    )
}

/// Compiled default for the per-partition denominator floor.  A partition
/// with `reltuples = 0` (brand-new or never-ANALYZE'd) would otherwise
/// trip the dispatch on any non-zero |dirty|; the floor caps that to a
/// meaningful "small partition" size.
pub(crate) const WIPE_FLOOR_ROWS_DEFAULT: i64 = 1000;

/// 1.5.3 (plans/partitioning_3.md §3) — partition-aware sibling of
/// `build_high_selectivity_dispatch_sql`.
///
/// Emitted only when the plan is partitioned AND the firing source is the
/// anchor (Tier 1).  The DO block:
///   1. GROUPs the populated `affected_tbl` by the partition column to get
///      per-partition dirty counts.
///   2. For each partition, looks up the actual child via the
///      `__reflex_partition_child_for_key` SQL helper and reads its
///      `reltuples` to compute a per-partition ratio
///      `dirty / GREATEST(reltuples, wipe_floor_rows)`.
///   3. Classifies partitions as hot / cold using the IMV's
///      `wipe_threshold` (per-IMV override → GUC → compiled default).
///   4. Trip-cap: if `hot_count > total_partitions / 2`, falls back to
///      `reflex_reconcile(view)` (global) since DETACHing many partitions
///      sequentially is worse than one rebuild.
///   5. Hot → `PERFORM reflex_reconcile_partition(view, csv_of_hot_keys)`
///      (uses the atomic swap from Phase A).
///   6. Cold → runs the standard MERGE / dead-cleanup / target DELETE /
///      target INSERT with a `<partition_col> <> ALL($1::TEXT[])` filter
///      added.  `$1` is `_hot_keys` passed via EXECUTE USING.
///
/// The cold-path SQL strings MUST already contain the filter splice — the
/// caller is responsible for wrapping the scratch / WHERE clauses with
/// the `$1::TEXT[]` parameter binding before passing in.
/// Aggregate-path hot/cold partition dispatch (audit #2 / Component 4).
///
/// Classification groups dirty counts by RESOLVED CHILD (see the in-SQL note),
/// emitting both `_hot_keys` (one representative value per hot child, for
/// `reflex_reconcile_partition`) and `_hot_child_names` (the hot child names).
/// The cold body's hot-exclusion filter is strategy-specific and lives in the
/// caller's SQL strings:
///   * **LIST**  → `<part_col> <> ALL($1::TEXT[])` (hot VALUES) — bind `_hot_keys`.
///   * **RANGE** → `__reflex_partition_child_for_key(...)::text <> ALL($2::text[])`
///     (hot CHILD names; a value-array filter is wrong because many values map to
///     one range child, and an OID filter is wrong because the swap changes the
///     child OID) — bind `_hot_keys, _hot_child_names`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_partition_aware_dispatch_sql_strategy(
    view_name: &str,
    intermediate_tbl: &str,
    intermediate_parent_qual: &str,
    affected_tbl: &str,
    partition_col: &str,
    strategy: &str,
    merge_sql_with_filter: &str,
    dead_cleanup_sql: &[String],
    target_delete_sql_with_filter: &[String],
    target_insert_sql_with_filter: &[String],
) -> String {
    // RANGE cold filters reference $2 (hot child NAMES); LIST references only $1.
    // Child NAMES (not OIDs) because the hot swap DETACHes/re-ATTACHes the child,
    // changing its OID — the name is preserved by the swap's final RENAME, so a
    // name captured before the swap still matches the rebuilt child after it.
    let using = if strategy.eq_ignore_ascii_case("RANGE") {
        "_hot_keys, _hot_child_names"
    } else {
        "_hot_keys"
    };
    let dead_cleanup = execute_each(dead_cleanup_sql, Some(using));
    let safe_merge = merge_sql_with_filter.replace("$reflex_inner$", "$reflex_inner_alt$");
    let safe_tdel = execute_each(target_delete_sql_with_filter, Some(using));
    let safe_tins = execute_each(target_insert_sql_with_filter, Some(using));
    let safe_view = view_name.replace('\'', "''");
    let safe_part_col = partition_col.replace('"', "");
    let safe_part_col_lit = safe_part_col.replace('\'', "''");

    format!(
        "DO $reflex_dispatch$\n\
         DECLARE\n\
             _thr NUMERIC;\n\
             _per_imv NUMERIC;\n\
             _floor BIGINT;\n\
             _per_imv_floor BIGINT;\n\
             _hot_keys TEXT[] := ARRAY[]::TEXT[];\n\
             _hot_child_names TEXT[] := ARRAY[]::TEXT[];\n\
             _hot_count INT;\n\
             _partition_total INT;\n\
         BEGIN\n\
             SELECT wipe_threshold, wipe_floor_rows INTO _per_imv, _per_imv_floor\n\
                 FROM public.__reflex_ivm_reference WHERE name = '{view}';\n\
             _thr   := COALESCE(_per_imv, current_setting('reflex.wipe_threshold', true)::NUMERIC, {default_thr});\n\
             _floor := COALESCE(_per_imv_floor, NULLIF(current_setting('reflex.wipe_floor_rows', true), '')::BIGINT, {default_floor});\n\
             -- Per-partition dirty counts from the already-populated\n\
             -- affected table (GROUP BY partition_col).\n\
             SELECT count(*) INTO _partition_total\n\
                 FROM pg_inherits WHERE inhparent = '{int_parent}'::regclass;\n\
             IF _partition_total IS NULL OR _partition_total = 0 THEN\n\
                 RAISE DEBUG 'pg_reflex partition dispatch: % has no children — fallback to global', '{view}';\n\
                 EXECUTE $reflex_inner${merge}$reflex_inner$ USING {using};\n\
                 EXECUTE 'ANALYZE {intermediate}';\n\
{dead_cleanup}\
{tdel}\
{tins}\
                 RETURN;\n\
             END IF;\n\
             -- Classify by RESOLVED CHILD, not by raw partition value: sum the\n\
             -- dirty counts of every value that maps to the same child, then\n\
             -- compare the child's total dirty against its reltuples. For LIST\n\
             -- (1 value = 1 child) this is identical to per-value grouping; for\n\
             -- RANGE (many values per child) it is the only correct grouping.\n\
             -- _hot_keys carries one REPRESENTATIVE value per hot child (for\n\
             -- reflex_reconcile_partition); _hot_child_names carries the hot child\n\
             -- names (for the RANGE cold-exclusion filter, $2 — names survive the\n\
             -- swap's DETACH/ATTACH+RENAME, OIDs do not).\n\
             -- per_val is MATERIALIZED as an optimization barrier: it forces the\n\
             -- dedup-to-distinct-values to run BEFORE the LATERAL child resolution\n\
             -- below. Without it the planner flattens the CTEs and pushes the\n\
             -- VOLATILE __reflex_partition_child_for_key down into the row scan,\n\
             -- calling it once PER CHANGED ROW (O(delta), ~seconds at 10-100k rows)\n\
             -- instead of once per DISTINCT touched partition (O(partitions), ms).\n\
             WITH per_val AS MATERIALIZED (\n\
                 SELECT \"{part_col}\"::text AS pkey, count(*) AS dirty\n\
                 FROM {affected}\n\
                 GROUP BY \"{part_col}\"\n\
             ),\n\
             per_child AS (\n\
                 SELECT cfk.child AS child_oid,\n\
                        sum(pv.dirty) AS dirty,\n\
                        min(pv.pkey)  AS rep_key\n\
                 FROM per_val pv\n\
                 CROSS JOIN LATERAL (\n\
                     SELECT public.__reflex_partition_child_for_key(\n\
                                '{int_parent}'::regclass, '{part_col_lit}', pv.pkey) AS child\n\
                 ) cfk\n\
                 WHERE cfk.child IS NOT NULL\n\
                 GROUP BY cfk.child\n\
             ),\n\
             classified AS (\n\
                 SELECT pc.rep_key, pc.child_oid::text AS child_name,\n\
                        (pc.dirty::NUMERIC / GREATEST(c.reltuples::NUMERIC, _floor::NUMERIC) >= _thr) AS hot\n\
                 FROM per_child pc JOIN pg_class c ON c.oid = pc.child_oid\n\
             )\n\
             SELECT COALESCE(array_agg(rep_key::text) FILTER (WHERE hot), ARRAY[]::TEXT[]),\n\
                    COALESCE(array_agg(child_name)    FILTER (WHERE hot), ARRAY[]::TEXT[])\n\
                 INTO _hot_keys, _hot_child_names\n\
                 FROM classified;\n\
             _hot_count := COALESCE(array_length(_hot_keys, 1), 0);\n\
             RAISE DEBUG 'pg_reflex partition dispatch: hot=% total=% thr=% floor=%', _hot_count, _partition_total, _thr, _floor;\n\
             -- Trip-cap: sequential DETACH/ATTACH on > half of partitions\n\
             -- is worse than one full reconcile.\n\
             IF _hot_count > _partition_total / 2 THEN\n\
                 RAISE DEBUG 'pg_reflex partition dispatch: % hot of % partitions for %, fallback global', _hot_count, _partition_total, '{view}';\n\
                 PERFORM public.reflex_reconcile('{view}');\n\
                 RETURN;\n\
             END IF;\n\
             -- Hot partitions: atomic-swap reconcile.\n\
             IF _hot_count > 0 THEN\n\
                 RAISE DEBUG 'pg_reflex partition dispatch: % hot partitions for % → reflex_reconcile_partition', _hot_count, '{view}';\n\
                 PERFORM public.reflex_reconcile_partition('{view}', array_to_string(_hot_keys, ','));\n\
             END IF;\n\
             -- Cold partitions: standard MERGE + target sync, restricted\n\
             -- by the strategy-specific partition filter the caller spliced in.\n\
             EXECUTE $reflex_inner${merge}$reflex_inner$ USING {using};\n\
             EXECUTE 'ANALYZE {intermediate}';\n\
{dead_cleanup}\
{tdel}\
{tins}\
         END\n\
         $reflex_dispatch$",
        view = safe_view,
        int_parent = intermediate_parent_qual.replace('"', "").replace('\'', "''"),
        intermediate = intermediate_tbl,
        affected = affected_tbl,
        part_col = safe_part_col,
        part_col_lit = safe_part_col_lit,
        default_thr = WIPE_THRESHOLD_DEFAULT,
        default_floor = WIPE_FLOOR_ROWS_DEFAULT,
        merge = safe_merge,
        dead_cleanup = dead_cleanup,
        tdel = safe_tdel,
        tins = safe_tins,
        using = using,
    )
}

/// Helper: build the in-place upsert + delete-gone cold section for LIST partitioning.
fn build_inplace_cold_list_block(spec: &InplaceSpec, qv: &str, pcol: &str) -> String {
    let k_csv = spec.key_target_cols.join(", ");
    let ksrc_csv = spec.key_source_cols.join(", ");
    let nk_csv = spec.non_key_cols.join(", ");
    let all_cols = if nk_csv.is_empty() {
        k_csv.clone()
    } else {
        format!("{}, {}", k_csv, nk_csv)
    };
    let on_conflict = if spec.non_key_cols.is_empty() {
        "DO NOTHING".to_string()
    } else {
        let set_csv = spec
            .non_key_cols
            .iter()
            .map(|c| format!("{} = EXCLUDED.{}", c, c))
            .collect::<Vec<_>>()
            .join(", ");
        format!("DO UPDATE SET {}", set_csv)
    };
    let key_join = spec
        .key_target_cols
        .iter()
        .map(|c| format!("__t.{} IS NOT DISTINCT FROM __r.{}", c, c))
        .collect::<Vec<_>>()
        .join(" AND ");
    let dn = spec.delta_new;
    let pt_old = spec.pt_old;
    let assert_default = ASSERT_INPLACE_UPDATE_DEFAULT;
    let qvmsg = qv.replace('\'', "''");

    // delete-gone: rows still in pt_old (cold) but dropped by the recompute (source-deleted OR left the WHERE filter).
    let block = format!(
        "             DROP TABLE IF EXISTS __reflex_pt_proj;\n\
         \x20            EXECUTE format($reflex_dn$CREATE TEMP TABLE __reflex_pt_proj ON COMMIT DROP AS SELECT * FROM ({dn}) __r WHERE __r.{pcol}::text <> ALL($1::text[]) AND __r.{pcol} = ANY($2::text[]::%s[])$reflex_dn$, _part_type) USING _hot_keys, _cold_keys;\n\
         \x20            EXECUTE $reflex_up$INSERT INTO {qv} ({all_cols}) SELECT {all_cols} FROM __reflex_pt_proj ON CONFLICT ({k_csv}) {on_conflict}$reflex_up$;\n\
         \x20            EXECUTE format($reflex_del$DELETE FROM {qv} __t WHERE ({k_csv}) IN (SELECT {ksrc_csv} FROM {pt_old}) AND __t.{pcol}::text <> ALL($1::text[]) AND __t.{pcol} = ANY($2::text[]::%s[]) AND NOT EXISTS (SELECT 1 FROM __reflex_pt_proj __r WHERE {key_join})$reflex_del$, _part_type) USING _hot_keys, _cold_keys;\n\
         \x20            IF COALESCE(current_setting('reflex.assert_inplace_update', true)::bool, {assert_default}) THEN\n\
         \x20                EXECUTE format($reflex_as$SELECT 1 WHERE EXISTS ((SELECT {k_csv} FROM {qv} __t WHERE ({k_csv}) IN (SELECT {ksrc_csv} FROM {pt_old}) AND __t.{pcol} = ANY($1::text[]::%1$s[]) EXCEPT ALL SELECT {k_csv} FROM __reflex_pt_proj) UNION ALL (SELECT {k_csv} FROM __reflex_pt_proj EXCEPT ALL SELECT {k_csv} FROM {qv} __t WHERE ({k_csv}) IN (SELECT {ksrc_csv} FROM {pt_old}) AND __t.{pcol} = ANY($1::text[]::%1$s[])))$reflex_as$, _part_type) INTO _assert_hit USING _cold_keys;\n\
         \x20                IF _assert_hit IS NOT NULL THEN RAISE EXCEPTION 'pg_reflex in-place assertion failed for {qvmsg}: affected key set diverged'; END IF;\n\
         \x20            END IF;\n",
        dn=dn, pcol=pcol, qv=qv, k_csv=k_csv, all_cols=all_cols, on_conflict=on_conflict,
        ksrc_csv=ksrc_csv, pt_old=pt_old, key_join=key_join,
        assert_default=assert_default, qvmsg=qvmsg
    );
    block
}

/// Helper: build the in-place upsert + delete-gone cold section for RANGE partitioning.
fn build_inplace_cold_range_block(
    spec: &InplaceSpec,
    qv: &str,
    parent_lit: &str,
    part_col_lit: &str,
    part_col_qualified: &str,
) -> String {
    let k_csv = spec.key_target_cols.join(", ");
    let ksrc_csv = spec.key_source_cols.join(", ");
    let nk_csv = spec.non_key_cols.join(", ");
    let all_cols = if nk_csv.is_empty() {
        k_csv.clone()
    } else {
        format!("{}, {}", k_csv, nk_csv)
    };
    let on_conflict = if spec.non_key_cols.is_empty() {
        "DO NOTHING".to_string()
    } else {
        let set_csv = spec
            .non_key_cols
            .iter()
            .map(|c| format!("{} = EXCLUDED.{}", c, c))
            .collect::<Vec<_>>()
            .join(", ");
        format!("DO UPDATE SET {}", set_csv)
    };
    let key_join = spec
        .key_target_cols
        .iter()
        .map(|c| format!("__t.{} IS NOT DISTINCT FROM __r.{}", c, c))
        .collect::<Vec<_>>()
        .join(" AND ");
    let dn = spec.delta_new;
    let pt_old = spec.pt_old;
    let assert_default = ASSERT_INPLACE_UPDATE_DEFAULT;
    let qvmsg = qv.replace('\'', "''");
    // For RANGE, the partition column reference is needed in three places (recompute projection, target delete, assert)
    let pt_col_ref = format!("__r.{}", part_col_qualified);
    let tgt_col_ref = format!("__t.{}", part_col_qualified);

    // delete-gone: rows still in pt_old (cold) but dropped by the recompute (source-deleted OR left the WHERE filter).
    let block = format!(
        "             DROP TABLE IF EXISTS __reflex_pt_proj;\n\
         \x20            CREATE TEMP TABLE __reflex_pt_proj ON COMMIT DROP AS SELECT * FROM ({dn}) __r WHERE public.__reflex_partition_child_for_key('{parent_lit}'::regclass, '{part_col_lit}', {pt_col_ref}::text)::text <> ALL($1::text[]);\n\
         \x20            EXECUTE $reflex_up$INSERT INTO {qv} ({all_cols}) SELECT {all_cols} FROM __reflex_pt_proj ON CONFLICT ({k_csv}) {on_conflict}$reflex_up$;\n\
         \x20            DELETE FROM {qv} __t WHERE ({k_csv}) IN (SELECT {ksrc_csv} FROM {pt_old}) AND public.__reflex_partition_child_for_key('{parent_lit}'::regclass, '{part_col_lit}', {tgt_col_ref}::text)::text <> ALL($1::text[]) AND NOT EXISTS (SELECT 1 FROM __reflex_pt_proj __r WHERE {key_join}) USING _hot_child_names;\n\
         \x20            IF COALESCE(current_setting('reflex.assert_inplace_update', true)::bool, {assert_default}) THEN\n\
         \x20                SELECT 1 WHERE EXISTS ((SELECT {k_csv} FROM {qv} __t WHERE ({k_csv}) IN (SELECT {ksrc_csv} FROM {pt_old}) AND public.__reflex_partition_child_for_key('{parent_lit}'::regclass, '{part_col_lit}', {tgt_col_ref}::text)::text <> ALL($1::text[]) EXCEPT ALL SELECT {k_csv} FROM __reflex_pt_proj) UNION ALL (SELECT {k_csv} FROM __reflex_pt_proj EXCEPT ALL SELECT {k_csv} FROM {qv} __t WHERE ({k_csv}) IN (SELECT {ksrc_csv} FROM {pt_old}) AND public.__reflex_partition_child_for_key('{parent_lit}'::regclass, '{part_col_lit}', {tgt_col_ref}::text)::text <> ALL($1::text[]))) INTO _assert_hit USING _hot_child_names;\n\
         \x20                IF _assert_hit IS NOT NULL THEN RAISE EXCEPTION 'pg_reflex in-place assertion failed for {qvmsg}: affected key set diverged'; END IF;\n\
         \x20            END IF;\n",
        dn=dn, parent_lit=parent_lit, part_col_lit=part_col_lit, pt_col_ref=pt_col_ref,
        qv=qv, all_cols=all_cols, on_conflict=on_conflict, k_csv=k_csv,
        ksrc_csv=ksrc_csv, pt_old=pt_old, tgt_col_ref=tgt_col_ref, key_join=key_join,
        assert_default=assert_default, qvmsg=qvmsg
    );
    block
}

/// Passthrough sibling of [`build_partition_aware_dispatch_sql`] (audit #2). Same
/// group-dirty-by-child classification, trip-cap, and atomic-swap hot path, but
/// the cold body is the passthrough keyed delete + delta insert (no intermediate,
/// no MERGE, no dead-cleanup). Hot children are rebuilt by `reflex_reconcile_partition`;
/// the cold body maintains the rest, each statement restricted to COLD partitions
/// via the `$1::TEXT[]` (_hot_keys) filter the caller already spliced in.
///
/// `affected_select` must be shaped as `SELECT <expr> AS pkey FROM <delta>` — the
/// touched partition values (read from the scratch tables). `cold_insert_with_filter`
/// may be empty (DELETE-only operations); the INSERT EXECUTE is then omitted.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_passthrough_partition_dispatch_sql(
    view_name: &str,
    target_parent_qual: &str,
    affected_select: &str,
    partition_col: &str,
    target_part_ref: &str,
    strategy: &str,
    cold_delete_with_filter: &str,
    cold_insert_with_filter: &str,
    _inplace: Option<&InplaceSpec>,
) -> String {
    let safe_view = view_name.replace('\'', "''");
    let safe_part_col_lit = partition_col.replace('"', "").replace('\'', "''");
    let parent = target_parent_qual.replace('"', "").replace('\'', "''");
    let safe_del = cold_delete_with_filter.replace("$reflex_inner$", "$reflex_inner_alt$");
    let safe_ins = cold_insert_with_filter.replace("$reflex_inner$", "$reflex_inner_alt$");
    // RANGE cold filters reference $2 (hot child NAMES — stable across the swap's
    // DETACH/ATTACH+RENAME, unlike the OID); LIST references only $1 (hot values).
    let is_list = strategy.eq_ignore_ascii_case("LIST");

    let using = if strategy.eq_ignore_ascii_case("RANGE") {
        "_hot_keys, _hot_child_names"
    } else {
        "_hot_keys"
    };

    // For use in SQL format() strings, extract just the column name from target_part_ref
    // (target_part_ref is typically "table"."column", we need just the "column" part).
    let target_col_only = if target_part_ref.contains('.') {
        target_part_ref
            .rsplit('.')
            .next()
            .unwrap_or(target_part_ref)
    } else {
        target_part_ref
    };

    // Build the cold-body execution block for the main partition dispatch (post-trip-cap).
    // For in-place upsert (when spec is Some), emit the specialized block.
    // Otherwise, fall back to the standard DELETE+INSERT path.
    let cold_dispatch_block = if let Some(spec) = _inplace {
        if is_list {
            build_inplace_cold_list_block(spec, target_parent_qual, target_col_only)
        } else {
            // RANGE: need the qualified partition column reference for the helper
            build_inplace_cold_range_block(
                spec,
                target_parent_qual,
                &parent,
                &safe_part_col_lit,
                target_part_ref,
            )
        }
    } else {
        // Standard cold DELETE+INSERT path (no in-place optimization)
        if is_list {
            // LIST: cold DELETE with pruning
            let del_part = format!(
                "             IF array_length(_cold_keys,1) IS NOT NULL THEN\n\
                 EXECUTE format($reflex_cold_list${sql} AND {col} = ANY($2::text[]::%s[])$reflex_cold_list$, _part_type)\n\
                     USING _hot_keys, _cold_keys;\n",
                sql = safe_del,
                col = target_col_only
            );
            // LIST: cold INSERT with pruning (if present)
            let ins_part = if cold_insert_with_filter.is_empty() {
                "             END IF;\n".to_string()
            } else {
                format!(
                    "             EXECUTE format($reflex_cold_list${sql} AND {col} = ANY($2::text[]::%s[])$reflex_cold_list$, _part_type)\n\
                     USING _hot_keys, _cold_keys;\n\
                 END IF;\n",
                    sql = safe_ins,
                    col = target_col_only
                )
            };
            format!("{}{}", del_part, ins_part)
        } else {
            // RANGE: cold DELETE (no pruning needed — hot-exclusion by child name suffices)
            // Note: literal USING clause here (not a placeholder), since this block is embedded into the template.
            let del_part = format!(
                "             EXECUTE $reflex_inner${del}$reflex_inner$ USING _hot_keys, _hot_child_names;\n",
                del = safe_del
            );
            // RANGE: cold INSERT (if present)
            let ins_part = if cold_insert_with_filter.is_empty() {
                String::new()
            } else {
                format!(
                    "             EXECUTE $reflex_inner${ins}$reflex_inner$ USING _hot_keys, _hot_child_names;\n",
                    ins = safe_ins
                )
            };
            format!("{}{}", del_part, ins_part)
        }
    };

    // For the defensive branch (unpartitioned target), use the original simple behavior.
    let ins_block = if cold_insert_with_filter.is_empty() {
        String::new()
    } else {
        format!(
            "             EXECUTE $reflex_inner${ins}$reflex_inner$ USING {using};\n",
            ins = safe_ins,
            using = using
        )
    };
    format!(
        "DO $reflex_pt_dispatch$\n\
         DECLARE\n\
             _thr NUMERIC;\n\
             _per_imv NUMERIC;\n\
             _floor BIGINT;\n\
             _per_imv_floor BIGINT;\n\
             _hot_keys TEXT[] := ARRAY[]::TEXT[];\n\
             _hot_child_names TEXT[] := ARRAY[]::TEXT[];\n\
             _cold_keys TEXT[] := ARRAY[]::TEXT[];\n\
             _part_type TEXT;\n\
             _hot_count INT;\n\
             _partition_total INT;\n\
             _assert_hit INT;\n\
         BEGIN\n\
             SELECT wipe_threshold, wipe_floor_rows INTO _per_imv, _per_imv_floor\n\
                 FROM public.__reflex_ivm_reference WHERE name = '{view}';\n\
             _thr   := COALESCE(_per_imv, current_setting('reflex.wipe_threshold', true)::NUMERIC, {default_thr});\n\
             _floor := COALESCE(_per_imv_floor, NULLIF(current_setting('reflex.wipe_floor_rows', true), '')::BIGINT, {default_floor});\n\
             SELECT count(*) INTO _partition_total\n\
                 FROM pg_inherits WHERE inhparent = '{parent}'::regclass;\n\
             IF _partition_total IS NULL OR _partition_total = 0 THEN\n\
                 -- Target not partitioned (defensive) — run the cold body over\n\
                 -- everything (empty _hot_keys / _hot_child_names excludes nothing).\n\
                 EXECUTE $reflex_inner${del}$reflex_inner$ USING {using};\n\
{ins_block}\
                 RETURN;\n\
             END IF;\n\
             -- Classify dirty partition values by RESOLVED CHILD (see\n\
             -- build_partition_aware_dispatch_sql_strategy): one representative key\n\
             -- per hot child, plus the hot child NAMES for the RANGE cold filter.\n\
             -- per_val is MATERIALIZED as an optimization barrier: it forces the\n\
             -- dedup-to-distinct-values to run BEFORE the LATERAL child resolution\n\
             -- below. Without it the planner flattens the CTEs and pushes the\n\
             -- VOLATILE __reflex_partition_child_for_key down into the row scan,\n\
             -- calling it once PER CHANGED ROW (O(delta), ~seconds at 10-100k rows)\n\
             -- instead of once per DISTINCT touched partition (O(partitions), ms).\n\
             WITH per_val AS MATERIALIZED (\n\
                 SELECT pkey::text AS pkey, count(*) AS dirty FROM ({aff}) __pv GROUP BY pkey\n\
             ),\n\
             per_child AS (\n\
                 SELECT cfk.child AS child_oid, sum(pv.dirty) AS dirty, min(pv.pkey) AS rep_key\n\
                 FROM per_val pv\n\
                 CROSS JOIN LATERAL (\n\
                     SELECT public.__reflex_partition_child_for_key('{parent}'::regclass, '{part_col_lit}', pv.pkey) AS child\n\
                 ) cfk\n\
                 WHERE cfk.child IS NOT NULL\n\
                 GROUP BY cfk.child\n\
             ),\n\
             classified AS (\n\
                 SELECT pc.rep_key, pc.child_oid::text AS child_name,\n\
                        (pc.dirty::NUMERIC / GREATEST(c.reltuples::NUMERIC, _floor::NUMERIC) >= _thr) AS hot\n\
                 FROM per_child pc JOIN pg_class c ON c.oid = pc.child_oid\n\
             )\n\
             SELECT COALESCE(array_agg(rep_key::text) FILTER (WHERE hot), ARRAY[]::TEXT[]),\n\
                    COALESCE(array_agg(child_name)    FILTER (WHERE hot), ARRAY[]::TEXT[]),\n\
                    COALESCE(array_agg(rep_key::text) FILTER (WHERE NOT hot), ARRAY[]::TEXT[])\n\
                 INTO _hot_keys, _hot_child_names, _cold_keys\n\
                 FROM classified;\n\
             _hot_count := COALESCE(array_length(_hot_keys, 1), 0);\n\
             SELECT atttypid::regtype::text INTO _part_type\n\
                 FROM pg_attribute\n\
                 WHERE attrelid = '{parent}'::regclass AND attname = '{part_col_lit}' AND attnum > 0;\n\
             -- Trip-cap: swapping > half the partitions costs more than one full reconcile.\n\
             IF _hot_count > _partition_total / 2 THEN\n\
                 PERFORM public.reflex_reconcile('{view}');\n\
                 RETURN;\n\
             END IF;\n\
             -- Hot children: atomic-swap rebuild.\n\
             IF _hot_count > 0 THEN\n\
                 PERFORM public.reflex_reconcile_partition('{view}', array_to_string(_hot_keys, ','));\n\
             END IF;\n\
             -- Cold children: keyed delete + delta insert, hot children excluded\n\
             -- via the strategy-specific filter the caller spliced into the SQL.\n\
             -- For LIST: append partition-key pruning when _cold_keys is non-empty.\n\
{cold_dispatch_block}\
         END\n\
         $reflex_pt_dispatch$",
        view = safe_view,
        parent = parent,
        aff = affected_select,
        part_col_lit = safe_part_col_lit,
        default_thr = WIPE_THRESHOLD_DEFAULT,
        default_floor = WIPE_FLOOR_ROWS_DEFAULT,
        del = safe_del,
        ins_block = ins_block,
        using = using,
        cold_dispatch_block = cold_dispatch_block,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn push_materialized_merge_and_affected(
    stmts: &mut Vec<String>,
    scratch_tbl: &str,
    delta_query: &str,
    intermediate_tbl: &str,
    plan: &AggregationPlan,
    op: DeltaOp,
    affected_tbl: &str,
    select_expr: &str,
    include_cleanup: bool,
) {
    if include_cleanup {
        stmts.push(format!("TRUNCATE {}", affected_tbl));
    }
    stmts.push(format!("TRUNCATE {}", scratch_tbl));
    stmts.push(format!("INSERT INTO {} {}", scratch_tbl, delta_query));
    stmts.push(build_merge_from_table_sql(
        intermediate_tbl,
        scratch_tbl,
        plan,
        op,
    ));
    // 2026-05-15 (Item α surfaced this): the MERGE just modified
    // ~|scratch| rows in the intermediate. `pg_class.reltuples` is stale
    // (last set at IMV creation = 0). Downstream statements that JOIN on
    // the intermediate via the composite UNIQUE index (target sync's INSERT
    // and dead-cleanup DELETE) need accurate stats so the planner picks
    // Index Scan via the composite key instead of a pathological SeqScan
    // or NestedLoop. A full ANALYZE on a 180K-row intermediate is ~150 ms.
    stmts.push(format!("ANALYZE {}", intermediate_tbl));
    // Scratch is the result of GROUP BY in build_merge_from_table_sql's delta,
    // so it already contains one row per group key. DISTINCT here would add a
    // redundant hash/sort pass for the same output.
    stmts.push(format!(
        "INSERT INTO {} SELECT {} FROM {} AS __d",
        affected_tbl, select_expr, scratch_tbl
    ));
    // ANALYZE affected after INSERT — TRUNCATE clobbers reltuples; without
    // fresh stats the planner estimates 1 row and picks NL+SeqScan for the
    // downstream dead-cleanup DELETE and target sync EXISTS lookups
    // (pathological at high affected counts — 12+ minutes on 100K groups).
    // ~50 ms cost.
    stmts.push(format!("ANALYZE {}", affected_tbl));
}

// 1.4.6 — placeholder for a future revisit of dispatch on the INSERT and
// DELETE codegen paths (currently UPDATE-only). The blocker is that any
// dispatch decision needs |affected|, which requires scratch + affected
// fill, which on the bulk-flip shapes that motivate dispatch is the
// dominant cost being paid (50–100 s on alp 8.9 M rows). A future
// implementation must estimate |affected| from cheaper signals (pg_stats
// most_common_freqs on the JOIN key, or per-IMV calibrated cost) so the
// decision can fire before scratch is built. See
// journal/2026-05-15_dispatch_wiring_revert.md for the decision log.
//
// Signature sketch:
//     fn push_scratch_and_affected_for_dispatch(
//         stmts: &mut Vec<String>, scratch_tbl: &str, delta_query: &str,
//         intermediate_tbl: &str, plan: &AggregationPlan, op: DeltaOp,
//         affected_tbl: &str, select_expr: &str,
//     ) -> String { /* returns merge SQL; pushes scratch+affected fill */ }

/// 1.4.6 — bulk-DELETE path for Item α IN→OUT (and regular DELETE) on
/// sources that have passed the `plan.source_join_keys` safety gate.
///
/// Pre-conditions (enforced by the caller):
///   * Source has a mapping in `plan.source_join_keys` (every JOIN
///     equality maps to a GROUP BY col AND the source side covers a
///     UNIQUE key on the source). This guarantees the transition row's
///     identity uniquely identifies a slice of intermediate.
///   * end_query has no further GROUP BY — target rows are 1:1 with
///     intermediate after filtering `__ivm_count > 0`. Without this,
///     bulk DELETE on intermediate could leave target rows that should
///     be recomputed instead of removed.
///   * Source is not an outer-join secondary / not self-join (handled
///     earlier in the function).
///
/// Emits two indexed DELETEs (intermediate + target) that scan only the
/// transition rows against the leading column of the intermediate's
/// composite unique index. Bypasses the scratch fill — which is the
/// dominant cost (50-100 s) on bulk dim-flips like db_clone alp A4b.
pub(crate) fn push_bulk_delete_via_transition(
    stmts: &mut Vec<String>,
    intermediate_tbl: &str,
    target_qv: &str,
    transition_tbl: &str,
    mappings: &[(String, String)],
    plan: &AggregationPlan,
) {
    let int_cols: Vec<String> = mappings
        .iter()
        .map(|(ic, _)| format!("\"{}\"", ic))
        .collect();
    let src_cols: Vec<String> = mappings
        .iter()
        .map(|(_, sc)| format!("\"{}\"", sc))
        .collect();
    let src_select = src_cols.join(", ");
    let int_tuple = if int_cols.len() == 1 {
        int_cols[0].clone()
    } else {
        format!("({})", int_cols.join(", "))
    };

    // 1) DELETE from intermediate — uses the leading-col index because the
    // mapping always covers a unique key on the source which corresponds to
    // a prefix of the intermediate's composite UNIQUE index.
    stmts.push(format!(
        "DELETE FROM {} WHERE {} IN (SELECT {} FROM \"{}\")",
        intermediate_tbl, int_tuple, src_select, transition_tbl
    ));

    // 2) DELETE from target — translate intermediate cols to target cols via
    // plan.group_by_columns position (target_group_columns mirrors that
    // ordering with alias resolution applied).
    let target_cols_all = target_group_columns(plan);
    let mut target_tuple_cols: Vec<String> = Vec::with_capacity(mappings.len());
    for (ic, _) in mappings {
        if let Some(idx) = plan
            .group_by_columns
            .iter()
            .position(|gb| normalized_column_name(gb) == *ic)
        {
            if let Some(tc) = target_cols_all.get(idx) {
                target_tuple_cols.push(tc.clone());
            }
        }
    }
    if target_tuple_cols.len() == mappings.len() {
        let tgt_tuple = if target_tuple_cols.len() == 1 {
            target_tuple_cols[0].clone()
        } else {
            format!("({})", target_tuple_cols.join(", "))
        };
        stmts.push(format!(
            "DELETE FROM {} WHERE {} IN (SELECT {} FROM \"{}\")",
            target_qv, tgt_tuple, src_select, transition_tbl
        ));
    }

    // 3) ANALYZE intermediate — like the MERGE path. Future trigger fires
    // on this IMV want fresh stats on the row count.
    stmts.push(format!("ANALYZE {}", intermediate_tbl));
}

/// 1.4.6 — bulk-INSERT path for Item α OUT→IN flips on sources that have
/// passed the `plan.source_join_keys` safety gate.
///
/// Pre-condition (enforced by the caller via `plan.source_join_keys.contains`):
/// the source is JOIN-secondary with all JOIN equalities mapping to GROUP
/// BY columns AND those source columns cover a UNIQUE key on the source.
/// Together with Item α's OUT→IN guarantee (OLD post-filter empty), this
/// means the intermediate has zero rows for the group keys the scratch
/// will produce — MERGE's per-row probe is wasted, a plain INSERT is
/// correct.
///
/// Companion change in the target-sync block: the target DELETE on the
/// affected keys is dropped (target had zero rows for those keys by the
/// same reasoning).
pub(crate) fn push_bulk_insert_and_affected(
    stmts: &mut Vec<String>,
    scratch_tbl: &str,
    delta_query: &str,
    intermediate_tbl: &str,
    affected_tbl: &str,
    select_expr: &str,
) {
    stmts.push(format!("TRUNCATE {}", affected_tbl));
    stmts.push(format!("TRUNCATE {}", scratch_tbl));
    stmts.push(format!("INSERT INTO {} {}", scratch_tbl, delta_query));
    stmts.push(format!(
        "INSERT INTO {} SELECT * FROM {}",
        intermediate_tbl, scratch_tbl
    ));
    // Same reasoning as push_materialized_merge_and_affected: the
    // intermediate just grew by ~|scratch| rows. The downstream target-sync
    // INSERT joins on the composite UNIQUE index and needs accurate stats.
    stmts.push(format!("ANALYZE {}", intermediate_tbl));
    stmts.push(format!(
        "INSERT INTO {} SELECT {} FROM {} AS __d",
        affected_tbl, select_expr, scratch_tbl
    ));
    stmts.push(format!("ANALYZE {}", affected_tbl));
}
